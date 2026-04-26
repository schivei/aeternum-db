# B-Tree Index Usage Guide

## Overview

The `BTree<K, V>` type in `aeternumdb_core::index::btree` provides a
persistent, concurrent B+ tree index backed by the AeternumDB storage engine.

## Creating an Index

```rust
use aeternumdb_core::index::btree::{BTree, BTreeConfig};
use aeternumdb_core::storage::{StorageConfig, StorageEngine};
use std::sync::Arc;

// 1. Open a storage engine.
let engine = StorageEngine::new(StorageConfig {
    data_path: "/var/lib/mydb/index.db".into(),
    buffer_pool_size: 1000,   // pages to keep in memory
    page_size: 8192,          // 8 KiB pages
}).await?;

// 2. Create a new B-tree with the default fanout (100).
let tree = BTree::<i64, String>::new(
    Arc::new(engine),
    BTreeConfig::default(),
).await?;

// Save the metadata page ID so you can reopen the index later.
let meta_page_id = tree.meta_page_id().await;
```

## Configuration

```rust
BTreeConfig {
    fanout: 100,   // Valid range: 4..=1000.  Default: 100.
}
```

**Choosing a fanout:**

- A larger fanout means fewer levels (lower I/O for searches) but each node
  takes more space and splits more data.
- For most workloads, the default of `100` is a good starting point.
- For very large values, reduce the fanout to keep each page under the page
  size limit.

## Key and Value Types

Any type that implements `BTreeKey` or `BTreeValue` can be used.  Built-in
implementations are provided for:

| Type      | As key | As value |
|-----------|--------|----------|
| `i64`     | ✅      | ✅        |
| `u64`     | ✅      | ✅        |
| `String`  | ✅      | ✅        |
| `Vec<u8>` | ✅      | ✅        |

To use a custom type, implement the trait:

```rust
use aeternumdb_core::index::btree::{BTreeKey};
use aeternumdb_core::index::IndexError;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UserId(u64);

impl BTreeKey for UserId {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 8 {
            return Err(IndexError::Serialization("UserId must be 8 bytes".into()));
        }
        Ok(UserId(u64::from_be_bytes(bytes.try_into().unwrap())))
    }
}
```

> **Important for custom key types:** `to_bytes()` must produce a
> lexicographically ordered byte sequence that mirrors the type's `Ord`
> implementation.  Use big-endian encoding for numeric types.

## CRUD Operations

### Insert

```rust
tree.insert(42i64, "hello".to_string()).await?;
```

Returns `Err(IndexError::DuplicateKey)` if the key already exists.

### Upsert (insert or update)

```rust
tree.upsert(42i64, "world".to_string()).await?;
```

Inserts the entry if the key is new; replaces the value if it already exists.

### Search

```rust
match tree.search(&42i64).await? {
    Some(value) => println!("Found: {value}"),
    None => println!("Not found"),
}
```

### Delete

```rust
let deleted = tree.delete(&42i64).await?;
if deleted {
    println!("Key removed");
} else {
    println!("Key was not present");
}
```

## Range Queries

Range queries accept any `RangeBounds<K>`:

```rust
use std::ops::Bound;

// Inclusive range: keys 10 to 20 (inclusive).
let iter = tree.range(10i64..=20i64).await?;
for (key_bytes, val_bytes) in iter {
    let key = i64::from_bytes(&key_bytes)?;
    let val = String::from_bytes(&val_bytes)?;
    println!("{key}: {val}");
}

// Exclusive range: keys 10 to 20 (exclusive upper bound).
let iter = tree.range(10i64..20i64).await?;

// From a key to the end.
let iter = tree.range(10i64..).await?;

// Full scan.
let iter = tree.range(..).await?;
```

The iterator is synchronous — all required leaf pages are loaded during the
`range()` call.

## Bulk Loading

When you have a large pre-sorted dataset, use `bulk_load` instead of individual
inserts:

```rust
let entries: Vec<(i64, String)> = (0..1_000_000)
    .map(|i: i64| (i, format!("value-{i}")))
    .collect();

tree.bulk_load(entries).await?;
```

> **Note:** Input must be sorted in ascending key order.  Duplicate keys will
> return `IndexError::DuplicateKey`.

## Reopening an Existing Index

To persist the index across process restarts, save the metadata page ID and
pass it to `BTree::open`:

```rust
// On startup, store the meta page ID somewhere durable (e.g. a config file).
let meta_page_id: u64 = tree.meta_page_id().await;

// On the next startup:
let tree = BTree::<i64, String>::open(Arc::new(engine), meta_page_id).await?;
```

## Concurrent Access

`BTree<K, V>` is `Clone + Send + Sync`.  Clones share the same underlying
storage handle and metadata lock:

```rust
let tree = Arc::new(tree);

let handles: Vec<_> = (0..10).map(|t| {
    let tree = tree.clone();
    tokio::spawn(async move {
        for i in 0..100i64 {
            let key = t * 100 + i;
            tree.insert(key, key.to_string()).await.unwrap();
        }
    })
}).collect();

for h in handles { h.await.unwrap(); }
```

## Utility Methods

```rust
// Number of key-value pairs.
let count: u64 = tree.len().await;

// True when the tree has no entries.
let empty: bool = tree.is_empty().await;

// Current tree height (1 = root is a leaf).
let height: usize = tree.height().await;

// Page ID of the metadata page (needed to reopen the index).
let meta_id: u64 = tree.meta_page_id().await;
```

## Error Handling

All operations return `Result<_, IndexError>`:

| Variant                    | Meaning                                         |
|----------------------------|-------------------------------------------------|
| `IndexError::Storage`      | Underlying storage engine error                 |
| `IndexError::Serialization`| Node serialization / deserialization failure    |
| `IndexError::Corrupt`      | Tree structure is inconsistent                  |
| `IndexError::InvalidFanout`| Fanout outside [4, 1000]                        |
| `IndexError::DuplicateKey` | `insert` was called with an existing key        |

## Performance Tips

1. **Buffer pool size:** A larger buffer pool (more pages in memory) reduces
   disk I/O significantly for hot workloads.  Aim for the entire working set
   to fit in the pool when possible.

2. **Page size:** Larger pages (e.g. 16 KiB) improve sequential read throughput
   but increase write amplification on splits.  8 KiB is a good default.

3. **Fanout:** Keep `fanout × value_size` well below `page_size − HEADER_SIZE`
   to avoid serialization errors.

4. **Batch writes:** Use `bulk_load` for initial population rather than
   inserting one key at a time.

5. **Async context:** All operations are `async`.  Run them inside a
   `tokio::runtime::Runtime` or an `#[tokio::main]` entry point.
