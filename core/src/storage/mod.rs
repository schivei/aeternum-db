//! Public API for AeternumDB's page-level storage engine.
//!
//! [`StorageEngine`] is the single entry point for all page I/O.  It combines
//! a [`FileManager`] (disk) and a [`BufferPool`] (memory) behind a clean async
//! interface and a shared-state [`Arc`] so clones are cheap.
//!
//! # Sharding
//! Each [`StorageEngine`] manages exactly one database file (one shard).
//! Horizontal sharding is achieved by running multiple engines in parallel —
//! one per [`ShardId`] — and routing each [`PageId`] to the correct engine
//! at the application layer.  [`StorageEngine`] is `Clone + Send + Sync`, so
//! multiple shard handles can be stored in a `HashMap<ShardId, StorageEngine>`
//! and shared across async tasks without additional synchronisation.
//!
//! # Replication
//! Read replicas can open the same file path with their own [`StorageEngine`]
//! instance and serve reads independently.  Write replication (streaming
//! mutations from a primary to replicas) requires a Write-Ahead Log, which is
//! planned for PR 1.8.
//!
//! # Example
//! ```no_run
//! # use aeternumdb_core::storage::{StorageConfig, StorageEngine};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = StorageEngine::new(StorageConfig {
//!     data_path: "/tmp/mydb.db".into(),
//!     buffer_pool_size: 1000,
//!     page_size: 8192,
//! }).await?;
//!
//! let page_id = engine.allocate_page().await?;
//! engine.write_page_data(page_id, 0, b"hello world").await?;
//! let buf = engine.read_page_data(page_id, 0, 11).await?;
//! assert_eq!(&buf, b"hello world");
//! engine.deallocate_page(page_id).await?;
//! # Ok(())
//! # }
//! ```

pub mod buffer_pool;
pub mod file_manager;
pub mod page;

use buffer_pool::{BufferPool, BufferPoolConfig, BufferPoolError};
use file_manager::{FileManager, FileManagerError};
use page::{Page, PageId, HEADER_SIZE};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Logical shard identifier used to route page operations in a cluster.
///
/// In a sharded deployment each shard corresponds to an independent
/// [`StorageEngine`] instance backed by its own database file.  A router maps
/// `(ShardId, PageId)` tuples to the appropriate engine handle.
pub type ShardId = u16;

/// Error type returned by [`StorageEngine`] operations.
#[derive(Debug)]
pub enum StorageError {
    /// An error originating in the buffer pool layer.
    BufferPool(BufferPoolError),
    /// An error originating in the file manager layer.
    FileManager(FileManagerError),
    /// A read or write operation would exceed the page data bounds.
    OutOfBounds,
    /// The stored page checksum did not match the data payload.
    ChecksumMismatch(PageId),
    /// Attempted to deallocate a page that still has active pins.
    PagePinned(PageId),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::BufferPool(e) => write!(f, "buffer pool error: {e}"),
            StorageError::FileManager(e) => write!(f, "file manager error: {e}"),
            StorageError::OutOfBounds => write!(f, "page data access out of bounds"),
            StorageError::ChecksumMismatch(id) => write!(f, "checksum mismatch on page {id}"),
            StorageError::PagePinned(id) => {
                write!(f, "page {id} is pinned and cannot be deallocated")
            }
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::BufferPool(e) => Some(e),
            StorageError::FileManager(e) => Some(e),
            _ => None,
        }
    }
}

impl From<BufferPoolError> for StorageError {
    fn from(e: BufferPoolError) -> Self {
        StorageError::BufferPool(e)
    }
}

impl From<FileManagerError> for StorageError {
    fn from(e: FileManagerError) -> Self {
        StorageError::FileManager(e)
    }
}

/// Configuration for a [`StorageEngine`] instance.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Path to the database file on disk (created if it does not exist).
    pub data_path: PathBuf,
    /// Number of pages to keep in the in-memory buffer pool simultaneously.
    pub buffer_pool_size: usize,
    /// Size of each page in bytes.  Must be greater than `HEADER_SIZE` (16).
    ///
    /// Common values: `4096`, `8192`, `16384`.
    pub page_size: usize,
}

/// Main entry point for page-level I/O in AeternumDB.
///
/// `StorageEngine` is cheaply [`Clone`]-able — all clones share the same
/// underlying state through an [`Arc`].  It is `Send + Sync` and safe to use
/// from multiple async tasks concurrently.
#[derive(Clone)]
pub struct StorageEngine {
    /// Shared interior state protected by an async mutex.
    inner: Arc<Mutex<EngineInner>>,
    /// Cached page size (bytes), duplicated here so `page_size()` is sync.
    page_size: usize,
}

/// Interior state of a [`StorageEngine`], protected by a [`Mutex`].
struct EngineInner {
    /// Disk I/O layer.
    file_manager: FileManager,
    /// Memory caching layer.
    buffer_pool: BufferPool,
}

impl EngineInner {
    /// Pin page `id` in the buffer pool, loading it from disk when not cached.
    ///
    /// Validates the checksum on every disk load to detect silent corruption.
    /// If the page is already in the pool its pin count is incremented.
    async fn load_and_pin(&mut self, id: PageId) -> Result<(), StorageError> {
        if self.buffer_pool.contains(id) {
            self.buffer_pool.pin(id)?;
        } else {
            let page = self.file_manager.read_page(id).await?;
            if !page.validate_checksum() {
                return Err(StorageError::ChecksumMismatch(id));
            }
            self.buffer_pool.insert_and_pin(page)?;
        }
        Ok(())
    }

    /// Apply `data` bytes at `offset` to the in-pool copy of page `id`.
    ///
    /// The pool automatically marks the page dirty.
    fn write_to_page(
        &mut self,
        id: PageId,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageError> {
        let page = self
            .buffer_pool
            .get_mut(id)
            .ok_or(BufferPoolError::PageNotFound(id))?;
        page.write_data(offset, data)
            .map_err(|_| StorageError::OutOfBounds)
    }

    /// Write the in-pool copy of page `id` to disk, mark it clean, and unpin it.
    async fn flush_to_disk(&mut self, id: PageId) -> Result<(), StorageError> {
        let page = self.buffer_pool.get(id).unwrap().clone();
        self.file_manager.write_page(&page).await?;
        self.buffer_pool.mark_clean(id);
        self.buffer_pool.unpin(id, false)?;
        Ok(())
    }

    /// Evict page `id` from the buffer pool if it is currently cached.
    fn evict_if_cached(&mut self, id: PageId) -> Result<(), StorageError> {
        if self.buffer_pool.contains(id) {
            self.buffer_pool.evict(id)?;
        }
        Ok(())
    }
}

impl StorageEngine {
    /// Open (or create) a storage engine using `config`.
    ///
    /// # Panics
    /// Panics if `config.page_size <= HEADER_SIZE`.
    pub async fn new(config: StorageConfig) -> Result<Self, StorageError> {
        assert!(
            config.page_size > HEADER_SIZE,
            "page_size must be > HEADER_SIZE ({HEADER_SIZE})"
        );
        let fm = FileManager::open(&config.data_path, config.page_size).await?;
        let bp = BufferPool::new(BufferPoolConfig::new(
            config.buffer_pool_size,
            config.page_size,
        ));
        let page_size = config.page_size;
        Ok(StorageEngine {
            inner: Arc::new(Mutex::new(EngineInner {
                file_manager: fm,
                buffer_pool: bp,
            })),
            page_size,
        })
    }

    /// Allocate a new page slot and return its identifier.
    pub async fn allocate_page(&self) -> Result<PageId, StorageError> {
        let id = self.inner.lock().await.file_manager.allocate_page().await?;
        Ok(id)
    }

    /// Release page `id`, removing it from the buffer pool and marking its
    /// disk slot as free so it can be reused.
    ///
    /// Returns [`StorageError::PagePinned`] when the page still has active
    /// pins; the caller must unpin it before deallocating.
    pub async fn deallocate_page(&self, id: PageId) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        if inner.buffer_pool.pin_count(id) > 0 {
            return Err(StorageError::PagePinned(id));
        }
        inner.evict_if_cached(id)?;
        inner.file_manager.deallocate_page(id).await?;
        Ok(())
    }

    /// Pin page `id` in the buffer pool and return a snapshot of its data.
    ///
    /// The page remains pinned until [`unpin_page`](Self::unpin_page) is
    /// called with the same `id`.
    pub async fn pin_page(&self, id: PageId) -> Result<Page, StorageError> {
        let mut inner = self.inner.lock().await;
        inner.load_and_pin(id).await?;
        Ok(inner.buffer_pool.get(id).unwrap().clone())
    }

    /// Unpin page `id`.
    ///
    /// Set `dirty = true` only if you modified the page's data through a
    /// direct buffer pool reference; for writes via [`write_page_data`] this
    /// is handled automatically.
    pub async fn unpin_page(&self, id: PageId, dirty: bool) -> Result<(), StorageError> {
        self.inner.lock().await.buffer_pool.unpin(id, dirty)?;
        Ok(())
    }

    /// Write `data` into page `id` at byte `offset` and flush to disk.
    ///
    /// The page is loaded from disk if not already in the buffer pool.
    /// After the write the page is persisted immediately and unpinned.
    pub async fn write_page_data(
        &self,
        id: PageId,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        inner.load_and_pin(id).await?;
        inner.write_to_page(id, offset, data)?;
        inner.flush_to_disk(id).await
    }

    /// Read `len` bytes from page `id` starting at byte `offset`.
    ///
    /// The page is loaded from disk (and checksum-verified) if not already in
    /// the buffer pool.
    pub async fn read_page_data(
        &self,
        id: PageId,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, StorageError> {
        let mut inner = self.inner.lock().await;
        inner.load_and_pin(id).await?;
        let result = inner
            .buffer_pool
            .get(id)
            .unwrap()
            .read_data(offset, len)
            .map(|s: &[u8]| s.to_vec())
            .map_err(|_| StorageError::OutOfBounds);
        inner.buffer_pool.unpin(id, false)?;
        result
    }

    /// Flush all dirty unpinned pages in the buffer pool to disk.
    ///
    /// Holds the engine lock for the entire duration so that concurrent writes
    /// cannot overwrite an in-flight flush snapshot (no lost-update race).
    pub async fn flush(&self) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        let dirty_pages = inner.buffer_pool.flush_dirty_pages();
        for page in dirty_pages {
            inner.file_manager.write_page(&page).await?;
        }
        Ok(())
    }

    /// Return the configured page size in bytes.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Return the number of pages currently held in the buffer pool.
    pub async fn buffer_pool_len(&self) -> usize {
        self.inner.lock().await.buffer_pool.len()
    }

    /// Return the maximum number of pages the buffer pool can hold.
    pub async fn buffer_pool_capacity(&self) -> usize {
        self.inner.lock().await.buffer_pool.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    const PAGE_SIZE: usize = 512;

    async fn make_engine() -> (StorageEngine, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let engine = StorageEngine::new(StorageConfig {
            data_path: tmp.path().to_path_buf(),
            buffer_pool_size: 64,
            page_size: PAGE_SIZE,
        })
        .await
        .unwrap();
        (engine, tmp)
    }

    #[tokio::test]
    async fn test_allocate_page() {
        let (engine, _tmp) = make_engine().await;
        let id = engine.allocate_page().await.unwrap();
        assert_eq!(id, 0);
    }

    #[tokio::test]
    async fn test_write_and_read_page_data() {
        let (engine, _tmp) = make_engine().await;
        let id = engine.allocate_page().await.unwrap();
        engine
            .write_page_data(id, 0, b"hello storage")
            .await
            .unwrap();
        let data = engine.read_page_data(id, 0, 13).await.unwrap();
        assert_eq!(&data, b"hello storage");
    }

    #[tokio::test]
    async fn test_deallocate_page() {
        let (engine, _tmp) = make_engine().await;
        let id = engine.allocate_page().await.unwrap();
        engine.deallocate_page(id).await.unwrap();
    }

    #[tokio::test]
    async fn test_pin_unpin_page() {
        let (engine, _tmp) = make_engine().await;
        let id = engine.allocate_page().await.unwrap();
        let _page = engine.pin_page(id).await.unwrap();
        engine.unpin_page(id, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_dirty_pages() {
        let (engine, _tmp) = make_engine().await;
        let id = engine.allocate_page().await.unwrap();
        engine.write_page_data(id, 0, b"dirty data").await.unwrap();
        engine.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_data_persists_across_pin_unpin() {
        let (engine, _tmp) = make_engine().await;
        let id = engine.allocate_page().await.unwrap();
        engine.write_page_data(id, 5, b"persistent").await.unwrap();
        let data = engine.read_page_data(id, 5, 10).await.unwrap();
        assert_eq!(&data, b"persistent");
    }

    #[tokio::test]
    async fn test_page_size_accessor() {
        let (engine, _tmp) = make_engine().await;
        assert_eq!(engine.page_size(), PAGE_SIZE);
    }

    #[tokio::test]
    async fn test_buffer_pool_capacity() {
        let (engine, _tmp) = make_engine().await;
        assert_eq!(engine.buffer_pool_capacity().await, 64);
    }

    #[tokio::test]
    async fn test_multiple_pages() {
        let (engine, _tmp) = make_engine().await;
        let n = 5;
        let mut ids = Vec::new();
        for i in 0..n {
            let id = engine.allocate_page().await.unwrap();
            engine
                .write_page_data(id, 0, &(i as u8).to_le_bytes())
                .await
                .unwrap();
            ids.push(id);
        }
        for (i, &id) in ids.iter().enumerate() {
            let data = engine.read_page_data(id, 0, 1).await.unwrap();
            assert_eq!(data[0], i as u8);
        }
    }

    #[tokio::test]
    async fn test_out_of_bounds_write_returns_error() {
        let (engine, _tmp) = make_engine().await;
        let id = engine.allocate_page().await.unwrap();
        let big = vec![0u8; PAGE_SIZE + 1];
        assert!(matches!(
            engine.write_page_data(id, 0, &big).await,
            Err(StorageError::OutOfBounds)
        ));
    }

    #[tokio::test]
    async fn test_out_of_bounds_read_returns_error() {
        let (engine, _tmp) = make_engine().await;
        let id = engine.allocate_page().await.unwrap();
        let data_cap = PAGE_SIZE - HEADER_SIZE;
        assert!(matches!(
            engine.read_page_data(id, data_cap - 5, 10).await,
            Err(StorageError::OutOfBounds)
        ));
    }

    #[tokio::test]
    async fn test_buffer_pool_eviction_under_pressure() {
        let tmp = NamedTempFile::new().unwrap();
        let engine = StorageEngine::new(StorageConfig {
            data_path: tmp.path().to_path_buf(),
            buffer_pool_size: 4,
            page_size: PAGE_SIZE,
        })
        .await
        .unwrap();

        let mut ids = Vec::new();
        for i in 0u8..8 {
            let id = engine.allocate_page().await.unwrap();
            engine.write_page_data(id, 0, &[i]).await.unwrap();
            ids.push(id);
        }

        for (i, &id) in ids.iter().enumerate() {
            let data = engine.read_page_data(id, 0, 1).await.unwrap();
            assert_eq!(data[0], i as u8);
        }
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        use tokio::task;

        let tmp = NamedTempFile::new().unwrap();
        let engine = StorageEngine::new(StorageConfig {
            data_path: tmp.path().to_path_buf(),
            buffer_pool_size: 100,
            page_size: PAGE_SIZE,
        })
        .await
        .unwrap();

        let mut ids = Vec::new();
        for _ in 0..10 {
            ids.push(engine.allocate_page().await.unwrap());
        }

        let mut handles = Vec::new();
        for id in ids {
            let e = engine.clone();
            handles.push(task::spawn(async move {
                e.write_page_data(id, 0, &[id as u8]).await.unwrap();
                let data = e.read_page_data(id, 0, 1).await.unwrap();
                assert_eq!(data[0], id as u8);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
}
