//! Async file-based page storage with free-space bitmap tracking.
//!
//! [`FileManager`] owns a single database file and exposes page-level I/O.
//! Pages are stored contiguously; page `id` occupies bytes
//! `id × page_size .. (id+1) × page_size`.  An in-memory bitmap
//! tracks which slots are allocated, and a LIFO free-list enables O(1) reuse
//! of deallocated slots.
//!
//! # File growth
//! When all slots are occupied the file is extended by [`GROWTH_CHUNK_PAGES`]
//! pages at a time, amortising the cost of each `ftruncate`/`SetEndOfFile`
//! system call.

use crate::storage::page::{Page, PageHeader, PageId, PageType, HEADER_SIZE};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

/// Number of page slots added to the file each time it must grow.
const GROWTH_CHUNK_PAGES: u64 = 64;

/// Errors returned by [`FileManager`] operations.
#[derive(Debug)]
pub enum FileManagerError {
    /// The given page id is beyond the current page count.
    InvalidPageId(PageId),
    /// Attempted to free a page that is already free (double-free).
    PageAlreadyFree(PageId),
    /// Attempted to allocate a page that is already allocated.
    PageAlreadyAllocated(PageId),
    /// Attempted to read or write a page slot that is not allocated.
    PageNotAllocated(PageId),
    /// The page's total size does not match this manager's configured page size.
    PageSizeMismatch {
        /// Page that caused the mismatch.
        page_id: PageId,
        /// Size this manager expects for each page.
        expected: usize,
        /// Actual size of the provided page.
        actual: usize,
    },
    /// An underlying I/O error.
    Io(std::io::Error),
    /// The on-disk page layout could not be parsed (corrupt data).
    CorruptPage(PageId),
}

impl std::fmt::Display for FileManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileManagerError::InvalidPageId(id) => write!(f, "invalid page id {id}"),
            FileManagerError::PageAlreadyFree(id) => write!(f, "page {id} is already free"),
            FileManagerError::PageAlreadyAllocated(id) => {
                write!(f, "page {id} is already allocated")
            }
            FileManagerError::PageNotAllocated(id) => write!(f, "page {id} is not allocated"),
            FileManagerError::PageSizeMismatch {
                page_id,
                expected,
                actual,
            } => write!(
                f,
                "page {page_id} size mismatch: expected {expected} bytes, got {actual}"
            ),
            FileManagerError::Io(e) => write!(f, "I/O error: {e}"),
            FileManagerError::CorruptPage(id) => write!(f, "page {id} is corrupt"),
        }
    }
}

impl std::error::Error for FileManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let FileManagerError::Io(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

impl From<std::io::Error> for FileManagerError {
    fn from(e: std::io::Error) -> Self {
        FileManagerError::Io(e)
    }
}

/// Returns `true` when the raw header bytes identify a free or uninitialised page.
///
/// Reads byte 8 (the `page_type` field in the serialized header) and checks
/// it against [`PageType::Free`], avoiding a full deserialization during the
/// startup scan.  An all-zero buffer indicates a slot that was never written
/// (e.g. from an old file that pre-dates the free-marker convention) and is
/// also treated as free.
fn is_page_free_header(buf: &[u8; HEADER_SIZE]) -> bool {
    buf[8] == PageType::Free.as_u8() || buf.iter().all(|&b| b == 0)
}

/// Scan every page header in `file` and reconstruct the allocation bitmap and
/// free-list.
///
/// Pages whose header cannot be fully read (e.g. truncated) are treated as
/// free.  Called once by [`FileManager::open`] to restore in-memory state
/// from the on-disk file.
async fn scan_allocation_state(
    file: &File,
    page_count: u64,
    page_size: usize,
) -> Result<(Vec<bool>, VecDeque<PageId>), FileManagerError> {
    let mut bitmap = vec![false; page_count as usize];
    let mut free_list = VecDeque::new();

    let mut reader = file.try_clone().await?;
    for id in 0..page_count {
        reader.seek(SeekFrom::Start(id * page_size as u64)).await?;
        let mut header_buf = [0u8; HEADER_SIZE];
        match reader.read_exact(&mut header_buf).await {
            Ok(_) => {
                if is_page_free_header(&header_buf) {
                    free_list.push_back(id);
                } else {
                    bitmap[id as usize] = true;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                free_list.push_back(id);
            }
            Err(e) => return Err(FileManagerError::Io(e)),
        }
    }
    Ok((bitmap, free_list))
}

/// Manages reading and writing pages to a single database file.
///
/// # Thread safety
/// [`FileManager`] is not `Sync`.  Wrap it in a `Mutex` when sharing across
/// async tasks.
pub struct FileManager {
    /// Filesystem path of the managed database file.
    path: PathBuf,
    /// Open file handle used for all I/O.
    file: File,
    /// Fixed total size of each page in bytes (header + data).
    page_size: usize,
    /// `bitmap[i]` is `true` while page `i` is allocated.
    bitmap: Vec<bool>,
    /// LIFO queue of page ids available for immediate reuse.
    free_list: VecDeque<PageId>,
    /// Total number of page slots currently in the file (allocated + free).
    page_count: u64,
}

impl FileManager {
    /// Open (or create) the database file at `path`.
    ///
    /// All existing page headers are read to reconstruct the allocation bitmap
    /// and free-list so the engine can continue exactly where it left off.
    ///
    /// # Panics
    /// Panics if `page_size <= HEADER_SIZE` or if `page_size - HEADER_SIZE` exceeds
    /// [`u16::MAX`] (the maximum value storable in the `free_space` header field).
    pub async fn open(path: impl AsRef<Path>, page_size: usize) -> Result<Self, FileManagerError> {
        assert!(
            page_size > HEADER_SIZE,
            "page_size must be larger than HEADER_SIZE ({HEADER_SIZE})"
        );
        assert!(
            page_size - HEADER_SIZE <= u16::MAX as usize,
            "page data capacity {} exceeds u16::MAX; use a smaller page_size or widen free_space",
            page_size - HEADER_SIZE
        );

        let path = path.as_ref().to_owned();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await?;

        let file_len = file.metadata().await?.len();
        let page_count = file_len.div_ceil(page_size as u64);
        let (bitmap, free_list) = scan_allocation_state(&file, page_count, page_size).await?;

        Ok(FileManager {
            path,
            file,
            page_size,
            bitmap,
            free_list,
            page_count,
        })
    }

    /// Return the path of the managed file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the total number of page slots (allocated + free) in the file.
    pub fn page_count(&self) -> u64 {
        self.page_count
    }

    /// Return the number of currently allocated (non-free) pages.
    pub fn allocated_count(&self) -> u64 {
        self.bitmap.iter().filter(|&&b| b).count() as u64
    }

    /// Allocate a new page slot and return its [`PageId`].
    ///
    /// Reuses a previously freed slot when one is available; otherwise grows
    /// the file by [`GROWTH_CHUNK_PAGES`] new slots and allocates the first.
    pub async fn allocate_page(&mut self) -> Result<PageId, FileManagerError> {
        if let Some(id) = self.try_reuse_free_slot().await? {
            return Ok(id);
        }
        self.grow_and_allocate().await
    }

    /// Mark page `id` as free so its slot can be reused by a future
    /// [`allocate_page`](Self::allocate_page) call.
    ///
    /// Overwrites the on-disk header with a [`PageType::Free`] marker so that
    /// reopening the file correctly reconstructs allocation state.
    pub async fn deallocate_page(&mut self, id: PageId) -> Result<(), FileManagerError> {
        self.check_valid_id(id)?;
        if !self.bitmap[id as usize] {
            return Err(FileManagerError::PageAlreadyFree(id));
        }
        self.bitmap[id as usize] = false;
        self.free_list.push_front(id);
        self.write_free_marker(id).await
    }

    /// Write `page` to disk at the offset corresponding to its page id.
    ///
    /// Returns [`FileManagerError::PageNotAllocated`] when the page slot is
    /// not currently allocated in the bitmap.  Returns
    /// [`FileManagerError::PageSizeMismatch`] when the page's total byte
    /// length does not match this manager's configured `page_size`.
    pub async fn write_page(&mut self, page: &Page) -> Result<(), FileManagerError> {
        self.check_valid_id(page.id())?;
        self.check_allocated(page.id())?;
        self.check_page_size(page)?;
        self.write_page_to_disk(page).await
    }

    /// Read and return the page stored at `id`.
    ///
    /// Returns [`FileManagerError::PageNotAllocated`] when `id` is not
    /// currently allocated in the bitmap.
    pub async fn read_page(&mut self, id: PageId) -> Result<Page, FileManagerError> {
        self.check_valid_id(id)?;
        self.check_allocated(id)?;
        let offset = id * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; self.page_size];
        self.file.read_exact(&mut buf).await?;
        Page::deserialize(&buf).ok_or(FileManagerError::CorruptPage(id))
    }

    /// Returns `true` if page `id` is currently allocated.
    pub fn is_allocated(&self, id: PageId) -> bool {
        (id as usize) < self.bitmap.len() && self.bitmap[id as usize]
    }

    /// Pop the front of the free list, mark the slot allocated, write an
    /// empty page header to disk, and return the reclaimed [`PageId`].
    ///
    /// Returns `Ok(None)` when the free list is empty.
    async fn try_reuse_free_slot(&mut self) -> Result<Option<PageId>, FileManagerError> {
        let Some(id) = self.free_list.pop_front() else {
            return Ok(None);
        };
        self.bitmap[id as usize] = true;
        let page = Page::new(id, PageType::Data, self.page_size - HEADER_SIZE);
        self.write_page_to_disk(&page).await?;
        Ok(Some(id))
    }

    /// Grow the file by [`GROWTH_CHUNK_PAGES`] slots, enqueue the new free
    /// slots, then allocate and return the first new slot.
    async fn grow_and_allocate(&mut self) -> Result<PageId, FileManagerError> {
        let id = self.page_count;
        let new_count = self.page_count + GROWTH_CHUNK_PAGES;
        self.extend_file(new_count).await?;
        self.enqueue_free_slots(id + 1, new_count);
        self.bitmap[id as usize] = true;
        let page = Page::new(id, PageType::Data, self.page_size - HEADER_SIZE);
        self.write_page_to_disk(&page).await?;
        Ok(id)
    }

    /// Seek to the last byte of the target size and write a zero, causing the
    /// OS to fill the gap.  Then writes a [`PageType::Free`] header at the
    /// start of every newly added slot so that the allocation state is
    /// self-describing on disk.  Updates [`bitmap`](Self::bitmap) and
    /// [`page_count`](Self::page_count).
    async fn extend_file(&mut self, new_count: u64) -> Result<(), FileManagerError> {
        let old_count = self.page_count;
        let new_size = new_count * self.page_size as u64;
        self.file.seek(SeekFrom::Start(new_size - 1)).await?;
        self.file.write_all(&[0u8]).await?;
        self.file.flush().await?;
        self.bitmap.resize(new_count as usize, false);
        self.page_count = new_count;
        self.write_free_headers_for_range(old_count, new_count)
            .await
    }

    /// Write a [`PageType::Free`] header at the on-disk offset of every slot
    /// in the range `from..to`.
    ///
    /// Only the 16-byte header is written; the remaining page-data bytes are
    /// already zero-filled by the OS after sparse file growth.  The checksum
    /// is computed over the all-zero data payload so that any future
    /// validation of these slots does not produce spurious corruption errors.
    async fn write_free_headers_for_range(
        &mut self,
        from: PageId,
        to: PageId,
    ) -> Result<(), FileManagerError> {
        let data_size = self.page_size - HEADER_SIZE;
        let checksum = Page::compute_checksum(&vec![0u8; data_size]);
        for id in from..to {
            let offset = id * self.page_size as u64;
            self.file.seek(SeekFrom::Start(offset)).await?;
            let header = PageHeader {
                page_id: id,
                page_type: PageType::Free,
                free_space: data_size as u16,
                checksum,
            };
            self.file.write_all(&header.serialize()).await?;
        }
        self.file.flush().await?;
        Ok(())
    }

    /// Push page ids `from..to` onto the tail of the free list.
    fn enqueue_free_slots(&mut self, from: PageId, to: PageId) {
        for id in from..to {
            self.free_list.push_back(id);
        }
    }

    /// Overwrite the on-disk header for page `id` with a [`PageType::Free`]
    /// marker so the slot is recognised as free after a restart.
    async fn write_free_marker(&mut self, id: PageId) -> Result<(), FileManagerError> {
        let free_page = Page::new(id, PageType::Free, self.page_size - HEADER_SIZE);
        self.write_page_to_disk(&free_page).await
    }

    /// Return an error if `id` is not a valid page slot in the current file.
    fn check_valid_id(&self, id: PageId) -> Result<(), FileManagerError> {
        if id >= self.page_count {
            Err(FileManagerError::InvalidPageId(id))
        } else {
            Ok(())
        }
    }

    /// Return an error if page `id` is not currently allocated in the bitmap.
    fn check_allocated(&self, id: PageId) -> Result<(), FileManagerError> {
        if !self.bitmap[id as usize] {
            Err(FileManagerError::PageNotAllocated(id))
        } else {
            Ok(())
        }
    }

    /// Return an error if `page`'s total byte size does not match `self.page_size`.
    ///
    /// Prevents writing a page constructed with a different `page_size` from
    /// overwriting only part of a disk slot and corrupting adjacent pages.
    fn check_page_size(&self, page: &Page) -> Result<(), FileManagerError> {
        let actual = page.total_size();
        if actual != self.page_size {
            Err(FileManagerError::PageSizeMismatch {
                page_id: page.id(),
                expected: self.page_size,
                actual,
            })
        } else {
            Ok(())
        }
    }

    /// Seek to `page.id() × page_size` and write the full serialized page.
    async fn write_page_to_disk(&mut self, page: &Page) -> Result<(), FileManagerError> {
        let offset = page.id() * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset)).await?;
        let bytes = page.serialize();
        self.file.write_all(&bytes).await?;
        self.file.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    const PAGE_SIZE: usize = 256;

    async fn open_temp() -> (FileManager, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let fm = FileManager::open(tmp.path(), PAGE_SIZE).await.unwrap();
        (fm, tmp)
    }

    #[tokio::test]
    async fn test_allocate_page() {
        let (mut fm, _tmp) = open_temp().await;
        let id = fm.allocate_page().await.unwrap();
        assert_eq!(id, 0);
        assert!(fm.is_allocated(id));
    }

    #[tokio::test]
    async fn test_multiple_allocations() {
        let (mut fm, _tmp) = open_temp().await;
        let id0 = fm.allocate_page().await.unwrap();
        let id1 = fm.allocate_page().await.unwrap();
        assert_ne!(id0, id1);
        assert!(fm.is_allocated(id0));
        assert!(fm.is_allocated(id1));
    }

    #[tokio::test]
    async fn test_deallocate_page() {
        let (mut fm, _tmp) = open_temp().await;
        let id = fm.allocate_page().await.unwrap();
        fm.deallocate_page(id).await.unwrap();
        assert!(!fm.is_allocated(id));
    }

    #[tokio::test]
    async fn test_double_free_returns_error() {
        let (mut fm, _tmp) = open_temp().await;
        let id = fm.allocate_page().await.unwrap();
        fm.deallocate_page(id).await.unwrap();
        assert!(matches!(
            fm.deallocate_page(id).await,
            Err(FileManagerError::PageAlreadyFree(_))
        ));
    }

    #[tokio::test]
    async fn test_write_and_read_page() {
        let (mut fm, _tmp) = open_temp().await;
        let id = fm.allocate_page().await.unwrap();
        let mut page = Page::new(id, PageType::Data, PAGE_SIZE - HEADER_SIZE);
        page.write_data(0, b"hello").unwrap();

        fm.write_page(&page).await.unwrap();
        let read_back = fm.read_page(id).await.unwrap();
        assert_eq!(read_back.read_data(0, 5).unwrap(), b"hello");
    }

    #[tokio::test]
    async fn test_free_slot_reuse() {
        let (mut fm, _tmp) = open_temp().await;
        let id0 = fm.allocate_page().await.unwrap();
        fm.deallocate_page(id0).await.unwrap();
        let id_reused = fm.allocate_page().await.unwrap();
        assert_eq!(id_reused, id0);
    }

    #[tokio::test]
    async fn test_invalid_page_id_returns_error() {
        let (mut fm, _tmp) = open_temp().await;
        assert!(matches!(
            fm.read_page(999).await,
            Err(FileManagerError::InvalidPageId(_))
        ));
    }

    #[tokio::test]
    async fn test_file_grows_on_demand() {
        let (mut fm, _tmp) = open_temp().await;
        assert_eq!(fm.page_count(), 0);
        fm.allocate_page().await.unwrap();
        assert_eq!(fm.page_count(), GROWTH_CHUNK_PAGES);
    }

    #[tokio::test]
    async fn test_allocated_count() {
        let (mut fm, _tmp) = open_temp().await;
        let id0 = fm.allocate_page().await.unwrap();
        let _id1 = fm.allocate_page().await.unwrap();
        assert_eq!(fm.allocated_count(), 2);
        fm.deallocate_page(id0).await.unwrap();
        assert_eq!(fm.allocated_count(), 1);
    }

    /// Reading a freed page slot must return `PageNotAllocated`, not silently
    /// return stale data.  This mirrors what happens when a caller tries to
    /// access a deleted database record through a stale page reference.
    #[tokio::test]
    async fn test_read_freed_page_returns_not_allocated() {
        let (mut fm, _tmp) = open_temp().await;
        let id = fm.allocate_page().await.unwrap();
        fm.deallocate_page(id).await.unwrap();
        assert!(matches!(
            fm.read_page(id).await,
            Err(FileManagerError::PageNotAllocated(_))
        ));
    }

    /// Writing to a freed page slot must return `PageNotAllocated`, preventing
    /// callers from accidentally overwriting a slot that may have been reused.
    #[tokio::test]
    async fn test_write_freed_page_returns_not_allocated() {
        let (mut fm, _tmp) = open_temp().await;
        let id = fm.allocate_page().await.unwrap();
        let page = Page::new(id, PageType::Data, PAGE_SIZE - HEADER_SIZE);
        fm.deallocate_page(id).await.unwrap();
        assert!(matches!(
            fm.write_page(&page).await,
            Err(FileManagerError::PageNotAllocated(_))
        ));
    }
}
