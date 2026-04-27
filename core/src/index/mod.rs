//! Index subsystem for AeternumDB.
//!
//! This module provides a production-ready B-tree index that integrates with
//! the storage engine from the storage subsystem.  Indexes are the primary
//! mechanism for fast key-value lookups and range queries.
//!
//! # Architecture
//!
//! Each index is backed by a set of pages in the storage engine.  The B-tree
//! structure maps keys to values (or to page locations in a heap file for
//! secondary indexes).  Every B-tree node occupies exactly one storage page.
//!
//! # Example
//! ```no_run
//! # use std::sync::Arc;
//! # use aeternumdb_core::storage::{StorageConfig, StorageEngine};
//! # use aeternumdb_core::index::btree::{BTree, BTreeConfig};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let engine = Arc::new(StorageEngine::new(StorageConfig {
//! #     data_path: "/tmp/mydb.db".into(),
//! #     buffer_pool_size: 1000,
//! #     page_size: 8192,
//! # }).await?);
//! // Build a B-tree index on top of the storage engine.
//! let tree = BTree::<i64, String>::new(Arc::clone(&engine), BTreeConfig::default()).await?;
//! tree.insert(42, "hello".to_string()).await?;
//! let v = tree.search(&42).await?;
//! assert_eq!(v.as_deref(), Some("hello"));
//! # Ok(())
//! # }
//! ```

pub mod btree;

/// Errors returned by index operations.
#[derive(Debug)]
pub enum IndexError {
    /// The underlying storage engine returned an error.
    Storage(crate::storage::StorageError),
    /// A node could not be serialized or deserialized.
    Serialization(String),
    /// The tree is corrupt (e.g. an expected page was missing or had wrong type).
    Corrupt(String),
    /// The supplied fanout is outside the allowed range [4, 1000].
    InvalidFanout(usize),
    /// A duplicate key was inserted and the index does not allow duplicates.
    DuplicateKey,
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::Storage(e) => write!(f, "storage error: {e}"),
            IndexError::Serialization(msg) => write!(f, "serialization error: {msg}"),
            IndexError::Corrupt(msg) => write!(f, "index corrupt: {msg}"),
            IndexError::InvalidFanout(n) => {
                write!(f, "invalid fanout {n}: must be in range [4, 1000]")
            }
            IndexError::DuplicateKey => write!(f, "duplicate key"),
        }
    }
}

impl std::error::Error for IndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IndexError::Storage(e) => Some(e),
            _ => None,
        }
    }
}

impl From<crate::storage::StorageError> for IndexError {
    fn from(e: crate::storage::StorageError) -> Self {
        IndexError::Storage(e)
    }
}
