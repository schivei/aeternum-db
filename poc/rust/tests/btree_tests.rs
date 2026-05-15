//! Integration tests for the B-tree index.
//!
//! These tests exercise the full stack: B-tree operations backed by the real
//! storage engine, reading and writing to temporary files.

use aeternumdb_core::index::btree::{BTree, BTreeConfig, BTreeKey};
use aeternumdb_core::index::IndexError;
use aeternumdb_core::storage::{StorageConfig, StorageEngine, StorageError};
use std::sync::Arc;
use tempfile::NamedTempFile;

const PAGE_SIZE: usize = 8192;

async fn make_engine(pool_size: usize) -> (Arc<StorageEngine>, NamedTempFile) {
    let tmp = NamedTempFile::new().expect("temp file creation failed");
    let engine = StorageEngine::new(StorageConfig {
        data_path: tmp.path().to_path_buf(),
        buffer_pool_size: pool_size,
        page_size: PAGE_SIZE,
    })
    .await
    .expect("StorageEngine::new failed");
    (Arc::new(engine), tmp)
}

async fn make_tree(fanout: usize) -> (BTree<i64, String>, NamedTempFile) {
    let (engine, tmp) = make_engine(2048).await;
    let tree = BTree::<i64, String>::new(engine, BTreeConfig { fanout })
        .await
        .expect("BTree::new failed");
    (tree, tmp)
}

// ── Basic CRUD ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_insert_and_search() {
    let (tree, _tmp) = make_tree(10).await;
    tree.insert(1i64, "one".to_string()).await.unwrap();
    tree.insert(2i64, "two".to_string()).await.unwrap();
    tree.insert(3i64, "three".to_string()).await.unwrap();

    assert_eq!(tree.search(&1i64).await.unwrap().as_deref(), Some("one"));
    assert_eq!(tree.search(&2i64).await.unwrap().as_deref(), Some("two"));
    assert_eq!(tree.search(&3i64).await.unwrap().as_deref(), Some("three"));
    assert!(tree.search(&99i64).await.unwrap().is_none());
}

#[tokio::test]
async fn test_delete_and_search() {
    let (tree, _tmp) = make_tree(10).await;
    for i in 0..10i64 {
        tree.insert(i, i.to_string()).await.unwrap();
    }
    assert!(tree.delete(&5i64).await.unwrap());
    assert!(tree.search(&5i64).await.unwrap().is_none());
    // Other keys are unaffected.
    for i in 0..10i64 {
        if i == 5 {
            continue;
        }
        assert!(tree.search(&i).await.unwrap().is_some(), "key {i} missing");
    }
}

#[tokio::test]
async fn test_upsert_insert_and_update() {
    let (tree, _tmp) = make_tree(10).await;
    tree.upsert(42i64, "v1".to_string()).await.unwrap();
    tree.upsert(42i64, "v2".to_string()).await.unwrap();
    assert_eq!(tree.search(&42i64).await.unwrap().as_deref(), Some("v2"));
}

// ── Sequential inserts ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_sequential_inserts_1000() {
    let (tree, _tmp) = make_tree(50).await;
    for i in 0..1000i64 {
        tree.insert(i, i.to_string()).await.unwrap();
    }
    for i in 0..1000i64 {
        let v = tree.search(&i).await.unwrap();
        assert_eq!(v, Some(i.to_string()), "missing key {i}");
    }
    assert_eq!(tree.len().await, 1000);
}

// ── Random inserts ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_random_order_inserts() {
    let (tree, _tmp) = make_tree(20).await;
    let mut keys: Vec<i64> = (0..200).collect();
    // Deterministic shuffle.
    for i in 0..keys.len() {
        let j = (i * 17 + 5) % keys.len();
        keys.swap(i, j);
    }
    for &k in &keys {
        tree.insert(k, k.to_string()).await.unwrap();
    }
    for k in 0..200i64 {
        let v = tree.search(&k).await.unwrap();
        assert_eq!(v, Some(k.to_string()), "missing key {k}");
    }
}

// ── Tree height ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_tree_height_is_logarithmic() {
    let (engine, _tmp) = make_engine(2048).await;
    let fanout = 10usize;
    let tree = BTree::<i64, String>::new(engine, BTreeConfig { fanout })
        .await
        .unwrap();

    for i in 0..1000i64 {
        tree.insert(i, i.to_string()).await.unwrap();
    }

    let height = tree.height().await;
    // For fanout=10 and 1000 keys, height should be at most ~4.
    let max_expected = 5usize;
    assert!(
        height <= max_expected,
        "height {height} exceeds max expected {max_expected}"
    );
    assert!(height >= 2, "height {height} too small for 1000 keys");
}

// ── Range scans ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_range_scan_returns_correct_entries() {
    let (tree, _tmp) = make_tree(10).await;
    for i in 0..50i64 {
        tree.insert(i, i.to_string()).await.unwrap();
    }
    let iter = tree.range(10i64..=20i64).await.unwrap();
    let results: Vec<_> = iter.collect();
    assert_eq!(results.len(), 11);

    for (idx, (k_bytes, _v)) in results.iter().enumerate() {
        let k = i64::from_bytes(k_bytes).unwrap();
        assert_eq!(k, 10 + idx as i64);
    }
}

#[tokio::test]
async fn test_range_scan_exclusive() {
    let (tree, _tmp) = make_tree(10).await;
    for i in 0..20i64 {
        tree.insert(i, i.to_string()).await.unwrap();
    }
    let iter = tree.range(5i64..10i64).await.unwrap();
    let results: Vec<_> = iter.collect();
    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn test_range_scan_empty_range() {
    let (tree, _tmp) = make_tree(10).await;
    for i in 0..10i64 {
        tree.insert(i, i.to_string()).await.unwrap();
    }
    // Range with no keys.
    let iter = tree.range(100i64..200i64).await.unwrap();
    let results: Vec<_> = iter.collect();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_range_scan_full_tree() {
    let (tree, _tmp) = make_tree(10).await;
    let n = 100i64;
    for i in 0..n {
        tree.insert(i, i.to_string()).await.unwrap();
    }
    let iter = tree.range::<std::ops::RangeFull>(..).await.unwrap();
    let results: Vec<_> = iter.collect();
    assert_eq!(results.len() as i64, n);
}

// ── Bulk load ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_bulk_load() {
    let (tree, _tmp) = make_tree(20).await;
    let entries: Vec<(i64, String)> = (0..100i64).map(|i| (i, i.to_string())).collect();
    tree.bulk_load(entries).await.unwrap();
    assert_eq!(tree.len().await, 100);
    for i in 0..100i64 {
        assert!(tree.search(&i).await.unwrap().is_some());
    }
}

// ── Persistence ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_persist_and_reopen() {
    let (engine, _tmp) = make_engine(512).await;
    let meta_id = {
        let tree = BTree::<i64, String>::new(engine.clone(), BTreeConfig { fanout: 10 })
            .await
            .unwrap();
        for i in 0..20i64 {
            tree.insert(i, format!("val-{i}")).await.unwrap();
        }
        tree.meta_page_id().await
    };

    // Reopen the tree using the same engine (simulates crash-recovery).
    let tree2 = BTree::<i64, String>::open(engine, meta_id).await.unwrap();

    for i in 0..20i64 {
        let v = tree2.search(&i).await.unwrap();
        assert_eq!(v, Some(format!("val-{i}")), "key {i} missing after reopen");
    }
    assert_eq!(tree2.len().await, 20);
}

// ── Mixed operations ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_interleaved_inserts_and_deletes() {
    let (tree, _tmp) = make_tree(10).await;
    for i in 0..100i64 {
        tree.insert(i, i.to_string()).await.unwrap();
    }
    // Delete even keys.
    for i in (0..100i64).step_by(2) {
        assert!(tree.delete(&i).await.unwrap());
    }
    // Verify odd keys remain.
    for i in (1..100i64).step_by(2) {
        assert!(
            tree.search(&i).await.unwrap().is_some(),
            "odd key {i} missing"
        );
    }
    // Verify even keys are gone.
    for i in (0..100i64).step_by(2) {
        assert!(
            tree.search(&i).await.unwrap().is_none(),
            "even key {i} should be deleted"
        );
    }
}

// ── Concurrency ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_concurrent_inserts_10_threads() {
    let (engine, _tmp) = make_engine(4096).await;
    let tree = Arc::new(
        BTree::<i64, String>::new(engine, BTreeConfig { fanout: 50 })
            .await
            .unwrap(),
    );

    let n_threads = 10usize;
    let keys_per_thread = 50usize;
    let mut handles = Vec::new();

    for t in 0..n_threads {
        let tree_clone = tree.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..keys_per_thread {
                let k = (t * keys_per_thread + i) as i64;
                tree_clone.insert(k, k.to_string()).await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let total = tree.len().await;
    assert_eq!(total, (n_threads * keys_per_thread) as u64);
}

#[tokio::test]
async fn test_concurrent_reads_50_tasks() {
    let (engine, _tmp) = make_engine(2048).await;
    let tree = Arc::new(
        BTree::<i64, String>::new(engine, BTreeConfig { fanout: 20 })
            .await
            .unwrap(),
    );

    // Pre-populate.
    for i in 0..100i64 {
        tree.insert(i, i.to_string()).await.unwrap();
    }

    let mut handles = Vec::new();
    for _ in 0..50 {
        let tree_clone = tree.clone();
        handles.push(tokio::spawn(async move {
            for k in 0..100i64 {
                let v = tree_clone.search(&k).await.unwrap();
                assert_eq!(v, Some(k.to_string()));
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// ── Error paths ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_invalid_fanout_too_small() {
    let (engine, _tmp) = make_engine(64).await;
    let result = BTree::<i64, String>::new(engine, BTreeConfig { fanout: 2 }).await;
    assert!(matches!(result, Err(IndexError::InvalidFanout(2))));
}

#[tokio::test]
async fn test_invalid_fanout_too_large() {
    let (engine, _tmp) = make_engine(64).await;
    let result = BTree::<i64, String>::new(engine, BTreeConfig { fanout: 9999 }).await;
    assert!(matches!(result, Err(IndexError::InvalidFanout(9999))));
}

#[tokio::test]
async fn test_duplicate_key_returns_error() {
    let (tree, _tmp) = make_tree(10).await;
    tree.insert(1i64, "a".to_string()).await.unwrap();
    let err = tree.insert(1i64, "b".to_string()).await.unwrap_err();
    assert!(matches!(err, IndexError::DuplicateKey));
}

#[tokio::test]
async fn test_delete_nonexistent_key_returns_false() {
    let (tree, _tmp) = make_tree(10).await;
    assert!(!tree.delete(&999i64).await.unwrap());
}

#[tokio::test]
async fn test_is_empty_and_len() {
    let (tree, _tmp) = make_tree(10).await;
    assert!(tree.is_empty().await);
    assert_eq!(tree.len().await, 0);
    tree.insert(1i64, "x".to_string()).await.unwrap();
    assert!(!tree.is_empty().await);
    assert_eq!(tree.len().await, 1);
}

#[tokio::test]
async fn test_update_existing_key() {
    let (tree, _tmp) = make_tree(10).await;
    tree.insert(7i64, "original".to_string()).await.unwrap();
    tree.update(7i64, "updated".to_string()).await.unwrap();
    assert_eq!(
        tree.search(&7i64).await.unwrap().as_deref(),
        Some("updated")
    );
}

// ── IndexError Display coverage ───────────────────────────────────────────────

#[test]
fn test_index_error_display_duplicate_key() {
    let msg = IndexError::DuplicateKey.to_string();
    assert!(msg.contains("duplicate key"), "got: {msg}");
}

#[test]
fn test_index_error_display_invalid_fanout() {
    let msg = IndexError::InvalidFanout(3).to_string();
    assert!(msg.contains('3'), "got: {msg}");
    assert!(msg.contains("fanout"), "got: {msg}");
}

#[test]
fn test_index_error_display_corrupt() {
    let msg = IndexError::Corrupt("bad page".to_string()).to_string();
    assert!(msg.contains("corrupt"), "got: {msg}");
    assert!(msg.contains("bad page"), "got: {msg}");
}

#[test]
fn test_index_error_display_serialization() {
    let msg = IndexError::Serialization("decode failed".to_string()).to_string();
    assert!(msg.contains("serialization"), "got: {msg}");
    assert!(msg.contains("decode failed"), "got: {msg}");
}

#[test]
fn test_index_error_display_storage() {
    let inner = StorageError::OutOfBounds;
    let err = IndexError::Storage(inner);
    let msg = err.to_string();
    assert!(msg.contains("storage"), "got: {msg}");
}

#[test]
fn test_index_error_from_storage_error() {
    // Exercises the From<StorageError> impl for IndexError.
    let inner = StorageError::OutOfBounds;
    let converted: IndexError = IndexError::from(inner);
    assert!(matches!(converted, IndexError::Storage(_)));
}

#[test]
fn test_index_error_storage_source() {
    use std::error::Error;
    let inner = StorageError::OutOfBounds;
    let err = IndexError::Storage(inner);
    assert!(err.source().is_some());
}

#[test]
fn test_index_error_non_storage_source_is_none() {
    use std::error::Error;
    assert!(IndexError::DuplicateKey.source().is_none());
    assert!(IndexError::InvalidFanout(5).source().is_none());
}
