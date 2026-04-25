// Storage Engine — Public API
// Licensed under AGPLv3.0

//! Public API for AeternumDB's storage engine.
//!
//! [`StorageEngine`] combines a [`FileManager`] and a [`BufferPool`] behind a
//! single, ergonomic async interface.  Callers interact with pages through the
//! pin / unpin mechanism: a pinned page is guaranteed to stay in memory until
//! explicitly unpinned.
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
//! // Allocate a new page and write some bytes.
//! let page_id = engine.allocate_page().await?;
//! engine.write_page_data(page_id, 0, b"hello world").await?;
//!
//! // Read back the bytes.
//! let buf = engine.read_page_data(page_id, 0, 11).await?;
//! assert_eq!(&buf, b"hello world");
//!
//! // Release the page.
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

/// Error type for the [`StorageEngine`].
#[derive(Debug)]
pub enum StorageError {
    /// An error from the buffer pool layer.
    BufferPool(BufferPoolError),
    /// An error from the file manager layer.
    FileManager(FileManagerError),
    /// Attempted to read / write beyond page data bounds.
    OutOfBounds,
    /// The page checksum did not match the stored data.
    ChecksumMismatch(PageId),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::BufferPool(e) => write!(f, "buffer pool error: {e}"),
            StorageError::FileManager(e) => write!(f, "file manager error: {e}"),
            StorageError::OutOfBounds => write!(f, "page data access out of bounds"),
            StorageError::ChecksumMismatch(id) => write!(f, "checksum mismatch on page {id}"),
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

// ── Configuration ────────────────────────────────────────────────────────────

/// Configuration for [`StorageEngine`].
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Path to the database file on disk.
    pub data_path: PathBuf,
    /// Number of pages to keep in the in-memory buffer pool.
    pub buffer_pool_size: usize,
    /// Size of each page in bytes (must be > `HEADER_SIZE`).
    ///
    /// Common values: 4096, 8192, 16384.
    pub page_size: usize,
}

// ── StorageEngine ─────────────────────────────────────────────────────────────

/// Main entry point for page-level I/O in AeternumDB.
///
/// The engine is [`Clone`]-able (cheap) — clones share the same underlying
/// state through `Arc`.
#[derive(Clone)]
pub struct StorageEngine {
    inner: Arc<Mutex<EngineInner>>,
    page_size: usize,
}

struct EngineInner {
    file_manager: FileManager,
    buffer_pool: BufferPool,
    page_size: usize,
}

impl StorageEngine {
    /// Open (or create) the storage engine using `config`.
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
                page_size,
            })),
            page_size,
        })
    }

    /// Allocate a new page and return its identifier.
    pub async fn allocate_page(&self) -> Result<PageId, StorageError> {
        let id = self.inner.lock().await.file_manager.allocate_page().await?;
        Ok(id)
    }

    /// Deallocate the page at `id`.
    ///
    /// The page is evicted from the buffer pool (if present) and its slot is
    /// marked free on disk.
    pub async fn deallocate_page(&self, id: PageId) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        // Remove from buffer pool if cached.
        if inner.buffer_pool.contains(id) {
            inner.buffer_pool.evict(id)?;
        }
        inner.file_manager.deallocate_page(id).await?;
        Ok(())
    }

    /// Pin page `id`, loading it from disk if necessary.
    ///
    /// The returned clone of the page is a snapshot; use [`write_page_data`]
    /// to modify its contents durably.
    pub async fn pin_page(&self, id: PageId) -> Result<Page, StorageError> {
        let mut inner = self.inner.lock().await;
        if !inner.buffer_pool.contains(id) {
            let page = inner.file_manager.read_page(id).await?;
            inner.buffer_pool.insert_and_pin(page)?;
        } else {
            inner.buffer_pool.pin(id)?;
        }
        let page = inner.buffer_pool.get(id).unwrap().clone();
        Ok(page)
    }

    /// Unpin page `id`.  Set `dirty = true` if the page was modified.
    pub async fn unpin_page(&self, id: PageId, dirty: bool) -> Result<(), StorageError> {
        self.inner.lock().await.buffer_pool.unpin(id, dirty)?;
        Ok(())
    }

    /// Write `data` into page `id` at byte `offset`, then flush to disk.
    pub async fn write_page_data(
        &self,
        id: PageId,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;

        // Load page into buffer pool if not already present.
        if !inner.buffer_pool.contains(id) {
            let page = inner.file_manager.read_page(id).await?;
            inner.buffer_pool.insert_and_pin(page)?;
        } else {
            inner.buffer_pool.pin(id)?;
        }

        // Mutate in-pool.
        {
            let page = inner
                .buffer_pool
                .get_mut(id)
                .ok_or(BufferPoolError::PageNotFound(id))?;
            page.write_data(offset, data)
                .map_err(|_| StorageError::OutOfBounds)?;
        }

        // Flush to disk immediately.
        let page = inner.buffer_pool.get(id).unwrap().clone();
        inner.file_manager.write_page(&page).await?;
        inner.buffer_pool.unpin(id, false)?;
        Ok(())
    }

    /// Read `len` bytes from page `id` starting at byte `offset`.
    pub async fn read_page_data(
        &self,
        id: PageId,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, StorageError> {
        let mut inner = self.inner.lock().await;

        // Load if not cached.
        if !inner.buffer_pool.contains(id) {
            let page: Page = inner.file_manager.read_page(id).await?;
            if !page.validate_checksum() {
                return Err(StorageError::ChecksumMismatch(id));
            }
            inner.buffer_pool.insert_and_pin(page)?;
        } else {
            inner.buffer_pool.pin(id)?;
        }

        let result = {
            let page = inner.buffer_pool.get(id).unwrap();
            page.read_data(offset, len)
                .map(|s: &[u8]| s.to_vec())
                .map_err(|_| StorageError::OutOfBounds)
        };
        inner.buffer_pool.unpin(id, false)?;
        result
    }

    /// Flush all dirty, unpinned pages in the buffer pool to disk.
    pub async fn flush(&self) -> Result<(), StorageError> {
        let dirty_pages: Vec<Page> = self.inner.lock().await.buffer_pool.flush_dirty_pages();
        for page in dirty_pages {
            self.inner
                .lock()
                .await
                .file_manager
                .write_page(&page)
                .await?;
        }
        Ok(())
    }

    /// Return the configured page size.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Return the number of pages currently in the buffer pool.
    pub async fn buffer_pool_len(&self) -> usize {
        self.inner.lock().await.buffer_pool.len()
    }

    /// Return the buffer pool capacity.
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
        // write_page_data already handles pin/unpin; just read back the data.
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
        // Very small buffer pool of 4 pages.
        let engine = StorageEngine::new(StorageConfig {
            data_path: tmp.path().to_path_buf(),
            buffer_pool_size: 4,
            page_size: PAGE_SIZE,
        })
        .await
        .unwrap();

        // Allocate and write 8 pages — forces eviction.
        let mut ids = Vec::new();
        for i in 0u8..8 {
            let id = engine.allocate_page().await.unwrap();
            engine.write_page_data(id, 0, &[i]).await.unwrap();
            ids.push(id);
        }

        // All values should still be readable (data is on disk).
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

        // Pre-allocate pages.
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
