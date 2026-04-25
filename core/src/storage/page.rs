//! Page structure, serialization, and checksum utilities.
//!
//! A page is the fundamental unit of storage in AeternumDB.  Every page has a
//! fixed-size header followed by a data payload whose size is determined by the
//! configured `page_size`.
//!
//! # Layout
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │ PageHeader (16 bytes)                               │
//! │   page_id   : u64  (8 bytes)                        │
//! │   page_type : u8   (1 byte, padded to 2)            │
//! │   free_space: u16  (2 bytes)                        │
//! │   checksum  : u32  (4 bytes)                        │
//! ├─────────────────────────────────────────────────────┤
//! │ Data payload (page_size − HEADER_SIZE bytes)        │
//! └─────────────────────────────────────────────────────┘
//! ```

use crc32fast::Hasher as Crc32Hasher;
use std::fmt;

/// Zero-value byte written into the header padding slot.
const HEADER_RESERVED_BYTE: u8 = 0;

/// Size of the serialized page header in bytes.
pub const HEADER_SIZE: usize = 16;

/// Identifier for a page within a database file.
pub type PageId = u64;

/// Type tag stored inside a page header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Page holds table row data.
    Data = 0,
    /// Page holds index entries.
    Index = 1,
    /// Page holds overflow data for large values.
    Overflow = 2,
    /// Page is unallocated / free.
    Free = 3,
}

impl PageType {
    /// Convert raw byte to [`PageType`].
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(PageType::Data),
            1 => Some(PageType::Index),
            2 => Some(PageType::Overflow),
            3 => Some(PageType::Free),
            _ => None,
        }
    }

    /// Convert to raw byte.
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for PageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PageType::Data => write!(f, "Data"),
            PageType::Index => write!(f, "Index"),
            PageType::Overflow => write!(f, "Overflow"),
            PageType::Free => write!(f, "Free"),
        }
    }
}

/// Fixed-size page header (16 bytes on disk).
///
/// Layout (little-endian):
/// - bytes 0..8  : `page_id`
/// - byte  8     : `page_type`
/// - byte  9     : reserved (padding)
/// - bytes 10..12: `free_space`
/// - bytes 12..16: `checksum`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageHeader {
    /// Unique identifier of this page.
    pub page_id: PageId,
    /// Kind of data stored in this page.
    pub page_type: PageType,
    /// Number of free bytes remaining in the data section.
    pub free_space: u16,
    /// CRC-32 checksum of the data payload.
    pub checksum: u32,
}

impl PageHeader {
    /// Serialize the header into exactly [`HEADER_SIZE`] bytes.
    pub fn serialize(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.page_id.to_le_bytes());
        buf[8] = self.page_type.as_u8();
        buf[9] = HEADER_RESERVED_BYTE;
        buf[10..12].copy_from_slice(&self.free_space.to_le_bytes());
        buf[12..16].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserialize a header from exactly [`HEADER_SIZE`] bytes.
    ///
    /// Returns `None` if the `page_type` byte is unrecognised.
    pub fn deserialize(buf: &[u8; HEADER_SIZE]) -> Option<Self> {
        let page_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let page_type = PageType::from_u8(buf[8])?;
        let free_space = u16::from_le_bytes(buf[10..12].try_into().unwrap());
        let checksum = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        Some(PageHeader {
            page_id,
            page_type,
            free_space,
            checksum,
        })
    }
}

/// A single database page consisting of a header and a data payload.
#[derive(Debug, Clone)]
pub struct Page {
    /// Page header metadata.
    pub header: PageHeader,
    /// Raw data bytes (length == `page_size − HEADER_SIZE`).
    pub data: Vec<u8>,
}

impl Page {
    /// Create a new, empty page with the given identifier, type, and data capacity.
    ///
    /// `data_capacity` is the number of bytes available for data (i.e.
    /// `page_size − HEADER_SIZE`).
    ///
    /// # Panics
    /// Panics if `data_capacity` is 0.
    pub fn new(page_id: PageId, page_type: PageType, data_capacity: usize) -> Self {
        assert!(data_capacity > 0, "data_capacity must be > 0");
        let data = vec![0u8; data_capacity];
        let checksum = Self::compute_checksum(&data);
        Page {
            header: PageHeader {
                page_id,
                page_type,
                free_space: data_capacity as u16,
                checksum,
            },
            data,
        }
    }

    /// Compute CRC-32 checksum over the provided data slice.
    pub fn compute_checksum(data: &[u8]) -> u32 {
        let mut hasher = Crc32Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    /// Recompute and update the header checksum from current data contents.
    pub fn update_checksum(&mut self) {
        self.header.checksum = Self::compute_checksum(&self.data);
    }

    /// Validate the header checksum against the current data payload.
    ///
    /// Returns `true` when the checksum matches.
    pub fn validate_checksum(&self) -> bool {
        Self::compute_checksum(&self.data) == self.header.checksum
    }

    /// Write `src` bytes into the data section starting at `offset`.
    ///
    /// Returns an error if the write would exceed the data buffer.
    ///
    /// # Note on `free_space`
    /// `free_space` is updated to reflect remaining capacity *after* the end of
    /// this write (`data.len() - (offset + src.len())`).  For non-sequential or
    /// overlapping writes the reported value is approximate — it does **not**
    /// account for gaps between earlier writes.  Callers that need precise free
    /// space accounting should track used ranges at a higher layer.
    pub fn write_data(&mut self, offset: usize, src: &[u8]) -> Result<(), PageError> {
        let end = offset
            .checked_add(src.len())
            .ok_or(PageError::WriteOutOfBounds)?;
        if end > self.data.len() {
            return Err(PageError::WriteOutOfBounds);
        }
        self.data[offset..end].copy_from_slice(src);
        self.update_checksum();
        // Track remaining bytes after the highest byte written in this call.
        self.header.free_space = (self.data.len().saturating_sub(end)) as u16;
        Ok(())
    }

    /// Read `len` bytes from the data section starting at `offset`.
    ///
    /// Returns an error if the read would exceed the data buffer.
    pub fn read_data(&self, offset: usize, len: usize) -> Result<&[u8], PageError> {
        let end = offset.checked_add(len).ok_or(PageError::ReadOutOfBounds)?;
        if end > self.data.len() {
            return Err(PageError::ReadOutOfBounds);
        }
        Ok(&self.data[offset..end])
    }

    /// Serialize the entire page (header + data) into a byte vector.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.data.len());
        buf.extend_from_slice(&self.header.serialize());
        buf.extend_from_slice(&self.data);
        buf
    }

    /// Deserialize a page from raw bytes.
    ///
    /// The first [`HEADER_SIZE`] bytes are the header; the remainder is data.
    ///
    /// Returns `None` if the buffer is too short or the header is corrupt.
    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        let header_bytes: &[u8; HEADER_SIZE] = buf[..HEADER_SIZE].try_into().ok()?;
        let header = PageHeader::deserialize(header_bytes)?;
        let data = buf[HEADER_SIZE..].to_vec();
        Some(Page { header, data })
    }

    /// Return the page identifier.
    #[inline]
    pub fn id(&self) -> PageId {
        self.header.page_id
    }

    /// Return the total serialized size of this page in bytes.
    #[inline]
    pub fn size(&self) -> usize {
        HEADER_SIZE + self.data.len()
    }
}

/// Errors that can occur when reading or writing page data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    /// The write operation would exceed the page data buffer.
    WriteOutOfBounds,
    /// The read operation would exceed the page data buffer.
    ReadOutOfBounds,
    /// The page checksum does not match.
    ChecksumMismatch,
}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PageError::WriteOutOfBounds => write!(f, "write out of bounds"),
            PageError::ReadOutOfBounds => write!(f, "read out of bounds"),
            PageError::ChecksumMismatch => write!(f, "checksum mismatch"),
        }
    }
}

impl std::error::Error for PageError {}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA_SIZE: usize = 100;

    fn make_page(id: PageId) -> Page {
        Page::new(id, PageType::Data, DATA_SIZE)
    }

    #[test]
    fn test_page_creation() {
        let page = make_page(1);
        assert_eq!(page.id(), 1);
        assert_eq!(page.header.page_type, PageType::Data);
        assert_eq!(page.header.free_space, DATA_SIZE as u16);
        assert_eq!(page.data.len(), DATA_SIZE);
    }

    #[test]
    fn test_checksum_initial_valid() {
        let page = make_page(42);
        assert!(page.validate_checksum());
    }

    #[test]
    fn test_checksum_after_write() {
        let mut page = make_page(1);
        page.write_data(0, &[1, 2, 3, 4]).unwrap();
        assert!(page.validate_checksum());
    }

    fn corrupt_data_at(page: &mut Page, index: usize, value: u8) {
        page.data[index] = value;
    }

    #[test]
    fn test_checksum_detects_corruption() {
        let mut page = make_page(1);
        page.write_data(0, &[10, 20, 30]).unwrap();
        corrupt_data_at(&mut page, 0, 0xFF);
        assert!(!page.validate_checksum());
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let mut page = make_page(7);
        page.write_data(0, b"hello world").unwrap();
        let bytes = page.serialize();
        let restored = Page::deserialize(&bytes).expect("deserialize must succeed");
        assert_eq!(restored.id(), page.id());
        assert_eq!(restored.header.page_type, page.header.page_type);
        assert_eq!(restored.data, page.data);
        assert_eq!(restored.header.checksum, page.header.checksum);
    }

    #[test]
    fn test_deserialize_too_short_returns_none() {
        assert!(Page::deserialize(&[0u8; 4]).is_none());
    }

    #[test]
    fn test_page_type_roundtrip() {
        for &pt in &[
            PageType::Data,
            PageType::Index,
            PageType::Overflow,
            PageType::Free,
        ] {
            assert_eq!(PageType::from_u8(pt.as_u8()), Some(pt));
        }
        assert!(PageType::from_u8(99).is_none());
    }

    #[test]
    fn test_write_read_data() {
        let mut page = make_page(1);
        let payload = b"test payload";
        page.write_data(10, payload).unwrap();
        let read_back = page.read_data(10, payload.len()).unwrap();
        assert_eq!(read_back, payload);
    }

    #[test]
    fn test_write_out_of_bounds() {
        let mut page = make_page(1);
        let big = vec![0u8; DATA_SIZE + 1];
        assert_eq!(page.write_data(0, &big), Err(PageError::WriteOutOfBounds));
    }

    #[test]
    fn test_read_out_of_bounds() {
        let page = make_page(1);
        assert_eq!(
            page.read_data(DATA_SIZE - 5, 10),
            Err(PageError::ReadOutOfBounds)
        );
    }

    #[test]
    fn test_page_size() {
        let page = make_page(1);
        assert_eq!(page.size(), HEADER_SIZE + DATA_SIZE);
    }

    #[test]
    fn test_header_serialize_deserialize() {
        let header = PageHeader {
            page_id: 123,
            page_type: PageType::Index,
            free_space: 4080,
            checksum: 0xDEAD_BEEF,
        };
        let bytes = header.serialize();
        let restored = PageHeader::deserialize(&bytes).unwrap();
        assert_eq!(restored, header);
    }

    #[test]
    fn test_free_space_updated_after_write() {
        let mut page = make_page(1);
        assert_eq!(page.header.free_space, DATA_SIZE as u16);
        page.write_data(0, &[1u8; 50]).unwrap();
        assert_eq!(page.header.free_space, (DATA_SIZE - 50) as u16);
    }
}
