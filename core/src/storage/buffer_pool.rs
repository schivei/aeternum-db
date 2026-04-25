//! LRU buffer pool with page pinning and dirty-page tracking.
//!
//! [`BufferPool`] caches [`Page`]s in memory so that repeated accesses avoid
//! disk I/O.  When the pool is full and a new page must be inserted, the
//! Least-Recently-Used **unpinned** page is evicted to make room.
//!
//! Pinned pages are never evicted.  Each call to [`pin`](BufferPool::pin) or
//! [`insert_and_pin`](BufferPool::insert_and_pin) increments a reference
//! count; each call to [`unpin`](BufferPool::unpin) decrements it.  A page
//! becomes evictable only when its pin count reaches zero.
//!
//! # Thread safety
//! [`BufferPool`] is not `Sync`.  Use the [`SharedBufferPool`] type alias
//! (backed by [`parking_lot::RwLock`]) when you need concurrent access from
//! multiple threads.

use crate::storage::page::{Page, PageId};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Errors returned by [`BufferPool`] operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BufferPoolError {
    /// The pool is full and every resident page is pinned; no eviction is possible.
    PoolFull,
    /// The requested page is not present in the pool.
    PageNotFound(PageId),
    /// Attempted to unpin a page whose pin count is already zero.
    NotPinned(PageId),
}

impl std::fmt::Display for BufferPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferPoolError::PoolFull => write!(f, "buffer pool is full; all pages are pinned"),
            BufferPoolError::PageNotFound(id) => write!(f, "page {id} not in buffer pool"),
            BufferPoolError::NotPinned(id) => write!(f, "page {id} is not pinned"),
        }
    }
}

impl std::error::Error for BufferPoolError {}

/// Page eviction policy selector.
///
/// Currently only LRU is implemented.  The enum is non-exhaustive to allow
/// future policies (LRU-K, Clock, etc.) without breaking changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// Least-Recently-Used: evict the page accessed furthest in the past.
    #[default]
    Lru,
}

/// Configuration for a [`BufferPool`] instance.
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Maximum number of pages held in memory simultaneously.
    pub capacity: usize,
    /// Page size in bytes.  Stored for validation; the pool keeps full
    /// [`Page`] objects as returned by the [`FileManager`](super::file_manager::FileManager).
    pub page_size: usize,
    /// Which eviction algorithm to use when the pool is full.
    pub eviction_policy: EvictionPolicy,
}

impl BufferPoolConfig {
    /// Create a default LRU configuration with the given capacity and page size.
    pub fn new(capacity: usize, page_size: usize) -> Self {
        BufferPoolConfig {
            capacity,
            page_size,
            eviction_policy: EvictionPolicy::Lru,
        }
    }
}

/// A single slot in the buffer pool frames array.
#[derive(Debug)]
struct Frame {
    /// The cached page data.
    page: Page,
    /// Number of active pins on this page.  A page with `pin_count > 0`
    /// cannot be evicted.
    pin_count: u32,
    /// `true` when the page has been modified since it was loaded from disk.
    /// A dirty page must be written to disk before its frame can be reused.
    dirty: bool,
}

/// LRU buffer pool for caching database pages in memory.
///
/// Wrap in [`SharedBufferPool`] to share across threads.
pub struct BufferPool {
    /// Configuration (capacity, page size, eviction policy).
    config: BufferPoolConfig,
    /// Maps a page id to its frame index in [`frames`](Self::frames).
    page_table: HashMap<PageId, usize>,
    /// Fixed-size array of frame slots; `None` means the slot is unused.
    frames: Vec<Option<Frame>>,
    /// LRU access order: front is least-recently-used (eviction candidate).
    lru_order: VecDeque<usize>,
    /// Frame indices that have never been used (available without eviction).
    free_frames: VecDeque<usize>,
}

impl BufferPool {
    /// Create a new buffer pool with the given configuration.
    pub fn new(config: BufferPoolConfig) -> Self {
        let cap = config.capacity;
        let frames = (0..cap).map(|_| None).collect();
        let free_frames = (0..cap).collect();
        BufferPool {
            config,
            page_table: HashMap::new(),
            frames,
            lru_order: VecDeque::new(),
            free_frames,
        }
    }

    /// Insert `page` into the pool and pin it.
    ///
    /// If the page is already present its pin count is incremented.  If the
    /// pool is full an LRU unpinned victim is evicted.  Returns an error when
    /// the pool is full and every page is pinned.
    pub fn insert_and_pin(&mut self, page: Page) -> Result<(), BufferPoolError> {
        let id = page.id();
        if self.repin_existing(id) {
            return Ok(());
        }
        let fi = self.get_free_frame()?;
        self.frames[fi] = Some(Frame {
            page,
            pin_count: 1,
            dirty: false,
        });
        self.page_table.insert(id, fi);
        self.lru_order.push_back(fi);
        Ok(())
    }

    /// Increment the pin count for page `id`.
    ///
    /// Returns an error if the page is not in the pool.
    pub fn pin(&mut self, id: PageId) -> Result<&Page, BufferPoolError> {
        let fi = *self
            .page_table
            .get(&id)
            .ok_or(BufferPoolError::PageNotFound(id))?;
        let frame = self.frames[fi].as_mut().unwrap();
        frame.pin_count += 1;
        self.touch_lru(fi);
        Ok(&self.frames[fi].as_ref().unwrap().page)
    }

    /// Decrement the pin count for page `id`.
    ///
    /// Pass `dirty = true` if the caller modified the page's data, so it will
    /// be written to disk before eviction.  Returns an error when the pin
    /// count is already zero.
    pub fn unpin(&mut self, id: PageId, dirty: bool) -> Result<(), BufferPoolError> {
        let fi = *self
            .page_table
            .get(&id)
            .ok_or(BufferPoolError::PageNotFound(id))?;
        let frame = self.frames[fi].as_mut().unwrap();
        if frame.pin_count == 0 {
            return Err(BufferPoolError::NotPinned(id));
        }
        frame.pin_count -= 1;
        if dirty {
            frame.dirty = true;
        }
        Ok(())
    }

    /// Return an immutable reference to the page if it resides in the pool.
    pub fn get(&self, id: PageId) -> Option<&Page> {
        let fi = *self.page_table.get(&id)?;
        Some(&self.frames[fi].as_ref()?.page)
    }

    /// Return a mutable reference to the page if it resides in the pool.
    ///
    /// Automatically marks the page dirty.
    pub fn get_mut(&mut self, id: PageId) -> Option<&mut Page> {
        let fi = *self.page_table.get(&id)?;
        let frame = self.frames[fi].as_mut()?;
        frame.dirty = true;
        Some(&mut frame.page)
    }

    /// Returns `true` if the page is currently in the pool.
    pub fn contains(&self, id: PageId) -> bool {
        self.page_table.contains_key(&id)
    }

    /// Returns `true` if the page is marked dirty.
    pub fn is_dirty(&self, id: PageId) -> bool {
        self.page_table
            .get(&id)
            .and_then(|&fi| self.frames[fi].as_ref())
            .map(|f| f.dirty)
            .unwrap_or(false)
    }

    /// Returns the current pin count for page `id`, or `0` if not present.
    pub fn pin_count(&self, id: PageId) -> u32 {
        self.page_table
            .get(&id)
            .and_then(|&fi| self.frames[fi].as_ref())
            .map(|f| f.pin_count)
            .unwrap_or(0)
    }

    /// Return the number of pages currently in the pool.
    pub fn len(&self) -> usize {
        self.page_table.len()
    }

    /// Returns `true` if the pool holds no pages.
    pub fn is_empty(&self) -> bool {
        self.page_table.is_empty()
    }

    /// Return the maximum number of pages the pool can hold simultaneously.
    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    /// Collect all dirty **unpinned** pages and return them without altering
    /// the dirty flag.
    ///
    /// The caller is responsible for writing the returned pages to disk and
    /// calling [`mark_clean`](Self::mark_clean) for each page after a
    /// successful write.  Keeping pages dirty until the write succeeds ensures
    /// they are retried on the next flush if an error occurs.
    pub fn flush_dirty_pages(&self) -> Vec<Page> {
        self.frames
            .iter()
            .flatten()
            .filter(|f| f.dirty && f.pin_count == 0)
            .map(|f| f.page.clone())
            .collect()
    }

    /// Remove page `id` from the pool immediately, regardless of pin count.
    ///
    /// Returns the evicted [`Page`] and its dirty flag.  Returns an error if
    /// the page is not in the pool.
    pub fn evict(&mut self, id: PageId) -> Result<(Page, bool), BufferPoolError> {
        let fi = *self
            .page_table
            .get(&id)
            .ok_or(BufferPoolError::PageNotFound(id))?;
        let frame = self.frames[fi].take().unwrap();
        self.page_table.remove(&id);
        self.lru_order.retain(|&x| x != fi);
        self.free_frames.push_back(fi);
        Ok((frame.page, frame.dirty))
    }

    /// If page `id` is already in the pool, increment its pin count, move it
    /// to the MRU position, and return `true`.  Returns `false` otherwise.
    fn repin_existing(&mut self, id: PageId) -> bool {
        let Some(&fi) = self.page_table.get(&id) else {
            return false;
        };
        let frame = self.frames[fi].as_mut().unwrap();
        frame.pin_count += 1;
        self.touch_lru(fi);
        true
    }

    /// Return a free frame index, evicting the LRU unpinned page when no
    /// unused frames remain.
    fn get_free_frame(&mut self) -> Result<usize, BufferPoolError> {
        if let Some(fi) = self.free_frames.pop_front() {
            return Ok(fi);
        }
        self.evict_lru_victim()
    }

    /// Scan [`lru_order`](Self::lru_order) from front to back and evict the
    /// first frame whose pin count is zero **and** is not dirty.
    ///
    /// Dirty pages must be flushed to disk before they can be evicted; callers
    /// should use [`flush_dirty_pages`](Self::flush_dirty_pages) and then
    /// retry.  Returns the freed frame index, or
    /// [`BufferPoolError::PoolFull`] when every page is pinned or dirty.
    fn evict_lru_victim(&mut self) -> Result<usize, BufferPoolError> {
        let victim_fi = self
            .lru_order
            .iter()
            .copied()
            .find(|&fi| {
                self.frames[fi]
                    .as_ref()
                    .map(|f| f.pin_count == 0 && !f.dirty)
                    .unwrap_or(false)
            })
            .ok_or(BufferPoolError::PoolFull)?;

        let victim_id = self.frames[victim_fi].as_ref().unwrap().page.id();
        self.page_table.remove(&victim_id);
        self.frames[victim_fi] = None;
        self.lru_order.retain(|&x| x != victim_fi);
        Ok(victim_fi)
    }

    /// Clear the dirty flag for page `id` without changing its pin count.
    ///
    /// Called after a successful disk write so the page is not written again
    /// unnecessarily and becomes eligible for LRU eviction.  No-op when `id`
    /// is not in the pool.
    pub fn mark_clean(&mut self, id: PageId) {
        if let Some(&fi) = self.page_table.get(&id) {
            if let Some(frame) = self.frames[fi].as_mut() {
                frame.dirty = false;
            }
        }
    }

    /// Move frame `fi` to the back of [`lru_order`](Self::lru_order)
    /// (most-recently-used position).
    fn touch_lru(&mut self, fi: usize) {
        self.lru_order.retain(|&x| x != fi);
        self.lru_order.push_back(fi);
    }
}

/// Thread-safe shared buffer pool backed by a [`parking_lot::RwLock`].
///
/// Use [`new_shared_pool`] to construct one.
pub type SharedBufferPool = Arc<RwLock<BufferPool>>;

/// Construct a new [`SharedBufferPool`] with the given configuration.
pub fn new_shared_pool(config: BufferPoolConfig) -> SharedBufferPool {
    Arc::new(RwLock::new(BufferPool::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::{PageType, HEADER_SIZE};

    const PAGE_SIZE: usize = 256;
    const DATA_SIZE: usize = PAGE_SIZE - HEADER_SIZE;

    fn make_page(id: PageId) -> Page {
        Page::new(id, PageType::Data, DATA_SIZE)
    }

    fn make_pool(cap: usize) -> BufferPool {
        BufferPool::new(BufferPoolConfig::new(cap, PAGE_SIZE))
    }

    #[test]
    fn test_insert_and_contains() {
        let mut pool = make_pool(4);
        pool.insert_and_pin(make_page(1)).unwrap();
        assert!(pool.contains(1));
        assert!(!pool.contains(2));
    }

    #[test]
    fn test_pin_existing_page() {
        let mut pool = make_pool(4);
        pool.insert_and_pin(make_page(1)).unwrap();
        pool.unpin(1, false).unwrap();
        pool.pin(1).unwrap();
        assert_eq!(pool.pin_count(1), 1);
    }

    #[test]
    fn test_pin_missing_page_returns_error() {
        let mut pool = make_pool(4);
        assert!(matches!(
            pool.pin(99),
            Err(BufferPoolError::PageNotFound(99))
        ));
    }

    #[test]
    fn test_unpin_marks_dirty() {
        let mut pool = make_pool(4);
        pool.insert_and_pin(make_page(1)).unwrap();
        pool.unpin(1, true).unwrap();
        assert!(pool.is_dirty(1));
    }

    #[test]
    fn test_unpin_not_pinned_returns_error() {
        let mut pool = make_pool(4);
        pool.insert_and_pin(make_page(1)).unwrap();
        pool.unpin(1, false).unwrap();
        assert!(matches!(
            pool.unpin(1, false),
            Err(BufferPoolError::NotPinned(1))
        ));
    }

    #[test]
    fn test_lru_eviction_when_full() {
        let mut pool = make_pool(2);
        pool.insert_and_pin(make_page(1)).unwrap();
        pool.unpin(1, false).unwrap();
        pool.insert_and_pin(make_page(2)).unwrap();
        pool.unpin(2, false).unwrap();
        pool.insert_and_pin(make_page(3)).unwrap();
        assert!(!pool.contains(1));
        assert!(pool.contains(2));
        assert!(pool.contains(3));
    }

    #[test]
    fn test_pool_full_when_all_pinned() {
        let mut pool = make_pool(2);
        pool.insert_and_pin(make_page(1)).unwrap();
        pool.insert_and_pin(make_page(2)).unwrap();
        assert!(matches!(
            pool.insert_and_pin(make_page(3)),
            Err(BufferPoolError::PoolFull)
        ));
    }

    #[test]
    fn test_flush_dirty_pages() {
        let mut pool = make_pool(4);
        pool.insert_and_pin(make_page(1)).unwrap();
        pool.unpin(1, true).unwrap();
        pool.insert_and_pin(make_page(2)).unwrap();
        pool.unpin(2, false).unwrap();

        let dirty = pool.flush_dirty_pages();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].id(), 1);
        // Pages remain dirty until the caller confirms a successful disk write.
        assert!(pool.is_dirty(1));
        pool.mark_clean(1);
        assert!(!pool.is_dirty(1));
    }

    #[test]
    fn test_evict_page() {
        let mut pool = make_pool(4);
        pool.insert_and_pin(make_page(5)).unwrap();
        pool.unpin(5, true).unwrap();
        let (page, dirty) = pool.evict(5).unwrap();
        assert_eq!(page.id(), 5);
        assert!(dirty);
        assert!(!pool.contains(5));
    }

    #[test]
    fn test_len_and_capacity() {
        let mut pool = make_pool(4);
        assert_eq!(pool.len(), 0);
        pool.insert_and_pin(make_page(1)).unwrap();
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.capacity(), 4);
    }

    #[test]
    fn test_get_mut_marks_dirty() {
        let mut pool = make_pool(4);
        pool.insert_and_pin(make_page(10)).unwrap();
        pool.get_mut(10).unwrap();
        assert!(pool.is_dirty(10));
    }

    #[test]
    fn test_concurrent_shared_pool() {
        use std::thread;

        let shared = new_shared_pool(BufferPoolConfig::new(100, PAGE_SIZE));
        let mut handles = vec![];

        for i in 0..10u64 {
            let pool = Arc::clone(&shared);
            handles.push(thread::spawn(move || {
                pool.write().insert_and_pin(make_page(i)).unwrap();
                pool.write().unpin(i, false).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let pool = shared.read();
        assert_eq!(pool.len(), 10);
    }

    #[test]
    fn test_insert_same_page_twice_increments_pin() {
        let mut pool = make_pool(4);
        pool.insert_and_pin(make_page(1)).unwrap();
        pool.insert_and_pin(make_page(1)).unwrap();
        assert_eq!(pool.pin_count(1), 2);
        assert_eq!(pool.len(), 1);
    }
}
