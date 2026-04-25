// Storage Engine — File Manager
// Licensed under AGPLv3.0

//! Async file-based page storage with free-space bitmap tracking.
//!
//! The [`FileManager`] owns a single database file on disk.  Pages are stored
//! contiguously; each page occupies exactly `page_size` bytes at offset
//! `page_id * page_size`.  A compact in-memory bitmap tracks which page slots
//! are allocated.
//!
//! # File growth
//! When all existing slots are occupied the file is extended by
//! `GROWTH_CHUNK_PAGES` pages at a time, which amortises the cost of
//! `ftruncate` / `SetEndOfFile` calls.

use crate::storage::page::{Page, PageId, PageType, HEADER_SIZE};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

/// Number of pages added when the file must grow.
const GROWTH_CHUNK_PAGES: u64 = 64;

/// Error type returned by [`FileManager`].
#[derive(Debug)]
pub enum FileManagerError {
    /// A page id is beyond the currently known page count.
    InvalidPageId(PageId),
    /// The page slot is already free (double-free detected).
    PageAlreadyFree(PageId),
    /// The page slot is still allocated (cannot reallocate without freeing).
    PageAlreadyAllocated(PageId),
    /// An I/O error occurred.
    Io(std::io::Error),
    /// The page byte layout could not be parsed.
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

/// Manages reading and writing pages to a single database file.
///
/// # Thread safety
/// The struct itself is *not* `Sync`; callers are responsible for wrapping it
/// in a `Mutex` or similar guard when sharing across tasks.
pub struct FileManager {
    path: PathBuf,
    file: File,
    page_size: usize,
    /// `true` at index `i` means page `i` is allocated.
    bitmap: Vec<bool>,
    /// LIFO queue of free page ids available for reuse.
    free_list: VecDeque<PageId>,
    /// Total number of page slots in the file (allocated + free).
    page_count: u64,
}

impl FileManager {
    /// Open (or create) the database file at `path`.
    ///
    /// `page_size` must be at least `HEADER_SIZE + 1`.
    pub async fn open(path: impl AsRef<Path>, page_size: usize) -> Result<Self, FileManagerError> {
        assert!(
            page_size > HEADER_SIZE,
            "page_size must be larger than HEADER_SIZE ({HEADER_SIZE})"
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
        let page_count = file_len / (page_size as u64);

        // Scan existing pages to rebuild the free bitmap.
        let mut bitmap = vec![false; page_count as usize];
        let mut free_list = VecDeque::new();

        if page_count > 0 {
            let mut reader = file.try_clone().await?;
            let data_size = page_size - HEADER_SIZE;
            for id in 0..page_count {
                reader.seek(SeekFrom::Start(id * page_size as u64)).await?;
                let mut header_buf = [0u8; HEADER_SIZE];
                match reader.read_exact(&mut header_buf).await {
                    Ok(_) => {
                        // byte 8 is page_type; Free = 3
                        if header_buf[8] == PageType::Free.as_u8() {
                            free_list.push_back(id);
                        } else {
                            bitmap[id as usize] = true;
                        }
                    }
                    Err(_) => {
                        // Partial page at end — mark as free.
                        free_list.push_back(id);
                    }
                }
                // skip data bytes
                let _ = data_size; // data bytes not read during scan
            }
        }

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
    /// Reuses a previously freed slot when available; otherwise grows the file
    /// by [`GROWTH_CHUNK_PAGES`] pages.
    pub async fn allocate_page(&mut self) -> Result<PageId, FileManagerError> {
        // Prefer reusing a free slot.
        if let Some(id) = self.free_list.pop_front() {
            self.bitmap[id as usize] = true;
            // Write an empty (zeroed) page header to claim the slot.
            let page = Page::new(id, PageType::Data, self.page_size - HEADER_SIZE);
            self.write_page_to_disk(&page).await?;
            return Ok(id);
        }

        // Grow the file.
        let new_start = self.page_count;
        let new_count = self.page_count + GROWTH_CHUNK_PAGES;
        let new_file_size = new_count * self.page_size as u64;

        // Pre-allocate by seeking to the new end and writing a zero byte.
        self.file.seek(SeekFrom::Start(new_file_size - 1)).await?;
        self.file.write_all(&[0u8]).await?;
        self.file.flush().await?;

        // Mark new slots as free except the first which we allocate now.
        self.bitmap.resize(new_count as usize, false);
        for id in (new_start + 1)..new_count {
            self.free_list.push_back(id);
        }
        self.page_count = new_count;

        let id = new_start;
        self.bitmap[id as usize] = true;
        let page = Page::new(id, PageType::Data, self.page_size - HEADER_SIZE);
        self.write_page_to_disk(&page).await?;

        Ok(id)
    }

    /// Mark page `id` as free so its slot can be reused.
    ///
    /// The on-disk header is overwritten with a [`PageType::Free`] marker.
    pub async fn deallocate_page(&mut self, id: PageId) -> Result<(), FileManagerError> {
        self.check_id(id)?;
        if !self.bitmap[id as usize] {
            return Err(FileManagerError::PageAlreadyFree(id));
        }
        self.bitmap[id as usize] = false;
        self.free_list.push_front(id);

        // Write a Free-type page header to disk.
        let free_page = Page::new(id, PageType::Free, self.page_size - HEADER_SIZE);
        self.write_page_to_disk(&free_page).await?;
        Ok(())
    }

    /// Write `page` to disk at the position corresponding to its page id.
    ///
    /// The page is serialized and written as a single contiguous block.
    pub async fn write_page(&mut self, page: &Page) -> Result<(), FileManagerError> {
        self.check_id(page.id())?;
        self.write_page_to_disk(page).await
    }

    /// Read and return the page at `id` from disk.
    pub async fn read_page(&mut self, id: PageId) -> Result<Page, FileManagerError> {
        self.check_id(id)?;
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

    // ── Internal helpers ────────────────────────────────────────────────────

    fn check_id(&self, id: PageId) -> Result<(), FileManagerError> {
        if id >= self.page_count {
            Err(FileManagerError::InvalidPageId(id))
        } else {
            Ok(())
        }
    }

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

    const PAGE_SIZE: usize = 256; // small pages for tests

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
        // Next allocation should reuse the freed slot.
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
}
