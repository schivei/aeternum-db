// Storage Engine — Integration Tests
// Licensed under AGPLv3.0

//! End-to-end integration tests for the storage engine stack.
//!
//! These tests exercise `StorageEngine`, `BufferPool`, and `FileManager`
//! together against real temporary files.

use aeternumdb_core::storage::{page::HEADER_SIZE, StorageConfig, StorageEngine};
use tempfile::NamedTempFile;

const PAGE_SIZE: usize = 4096;

async fn make_engine(pool_size: usize) -> (StorageEngine, NamedTempFile) {
    let tmp = NamedTempFile::new().expect("temp file creation failed");
    let engine = StorageEngine::new(StorageConfig {
        data_path: tmp.path().to_path_buf(),
        buffer_pool_size: pool_size,
        page_size: PAGE_SIZE,
    })
    .await
    .expect("StorageEngine::new failed");
    (engine, tmp)
}

// ── CRUD ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_full_crud_operations() {
    let (engine, _tmp) = make_engine(64).await;

    // Create
    let id = engine.allocate_page().await.unwrap();

    // Update
    let payload = b"integration test data";
    engine.write_page_data(id, 0, payload).await.unwrap();

    // Read
    let data = engine.read_page_data(id, 0, payload.len()).await.unwrap();
    assert_eq!(&data, payload);

    // Delete
    engine.deallocate_page(id).await.unwrap();
}

// ── Large buffer pool ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_buffer_pool_1000_pages() {
    let (engine, _tmp) = make_engine(1000).await;

    let mut ids = Vec::with_capacity(1000);
    for i in 0u64..1000 {
        let id = engine.allocate_page().await.unwrap();
        engine
            .write_page_data(id, 0, &i.to_le_bytes())
            .await
            .unwrap();
        ids.push(id);
    }

    for (i, &id) in ids.iter().enumerate() {
        let data = engine.read_page_data(id, 0, 8).await.unwrap();
        let val = u64::from_le_bytes(data.try_into().unwrap());
        assert_eq!(val, i as u64);
    }
}

// ── Eviction under memory pressure ───────────────────────────────────────────

#[tokio::test]
async fn test_page_eviction_under_memory_pressure() {
    // Pool holds only 8 pages; write 32 pages to force many evictions.
    let (engine, _tmp) = make_engine(8).await;

    let mut ids = Vec::with_capacity(32);
    for i in 0u8..32 {
        let id = engine.allocate_page().await.unwrap();
        engine.write_page_data(id, 0, &[i]).await.unwrap();
        ids.push(id);
    }

    // Re-read all pages; evicted pages must be fetched from disk.
    for (i, &id) in ids.iter().enumerate() {
        let data = engine.read_page_data(id, 0, 1).await.unwrap();
        assert_eq!(data[0], i as u8, "page {id} data mismatch");
    }
}

// ── Concurrent access ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_concurrent_page_access_10_tasks() {
    use tokio::task;

    let (engine, _tmp) = make_engine(100).await;

    // Pre-allocate 10 pages.
    let mut ids = Vec::with_capacity(10);
    for _ in 0..10 {
        ids.push(engine.allocate_page().await.unwrap());
    }

    let mut handles = Vec::new();
    for &id in &ids {
        let e = engine.clone();
        handles.push(task::spawn(async move {
            let val = [id as u8; 8];
            e.write_page_data(id, 0, &val).await.unwrap();
            let data = e.read_page_data(id, 0, 8).await.unwrap();
            assert_eq!(data, val);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// ── Crash recovery simulation ─────────────────────────────────────────────────

#[tokio::test]
async fn test_crash_recovery_dirty_pages_on_disk() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Write some pages and flush.
    {
        let engine = StorageEngine::new(StorageConfig {
            data_path: path.clone(),
            buffer_pool_size: 64,
            page_size: PAGE_SIZE,
        })
        .await
        .unwrap();

        let id0 = engine.allocate_page().await.unwrap();
        let id1 = engine.allocate_page().await.unwrap();
        engine.write_page_data(id0, 0, b"pre-crash").await.unwrap();
        engine.write_page_data(id1, 0, b"also saved").await.unwrap();
        engine.flush().await.unwrap();
        // Engine dropped here — simulates graceful shutdown / "crash" scenario
        // since write_page_data already persists to disk.
    }

    // Re-open the file ("after restart") and verify data is intact.
    {
        let engine = StorageEngine::new(StorageConfig {
            data_path: path,
            buffer_pool_size: 64,
            page_size: PAGE_SIZE,
        })
        .await
        .unwrap();

        let data0 = engine.read_page_data(0, 0, 9).await.unwrap();
        assert_eq!(&data0, b"pre-crash");

        let data1 = engine.read_page_data(1, 0, 10).await.unwrap();
        assert_eq!(&data1, b"also saved");
    }
}

// ── Checksum validation ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_write_preserves_checksum_integrity() {
    let (engine, _tmp) = make_engine(64).await;
    let id = engine.allocate_page().await.unwrap();
    engine
        .write_page_data(id, 0, b"checksum test")
        .await
        .unwrap();

    // Pin the page and verify checksum is valid.
    let page = engine.pin_page(id).await.unwrap();
    assert!(page.validate_checksum());
    engine.unpin_page(id, false).await.unwrap();
}

// ── Page type handling ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_pin_unpin_multiple_times() {
    let (engine, _tmp) = make_engine(16).await;
    let id = engine.allocate_page().await.unwrap();
    engine.write_page_data(id, 10, b"multi-pin").await.unwrap();

    for _ in 0..5 {
        let page = engine.pin_page(id).await.unwrap();
        let slice = page.read_data(10, 9).unwrap();
        assert_eq!(slice, b"multi-pin");
        engine.unpin_page(id, false).await.unwrap();
    }
}

// ── Offset writes ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_non_zero_offset_write_read() {
    let (engine, _tmp) = make_engine(16).await;
    let id = engine.allocate_page().await.unwrap();
    let offset = 100;
    engine
        .write_page_data(id, offset, b"offset data")
        .await
        .unwrap();
    let data = engine.read_page_data(id, offset, 11).await.unwrap();
    assert_eq!(&data, b"offset data");
}

// ── Deallocation and reuse ────────────────────────────────────────────────────

#[tokio::test]
async fn test_deallocated_slot_reused() {
    let (engine, _tmp) = make_engine(16).await;
    let id0 = engine.allocate_page().await.unwrap();
    engine.deallocate_page(id0).await.unwrap();
    let id_new = engine.allocate_page().await.unwrap();
    // The freed slot must be reused.
    assert_eq!(id_new, id0);
}

// ── Data capacity boundary ────────────────────────────────────────────────────

#[tokio::test]
async fn test_write_full_page_data_section() {
    let (engine, _tmp) = make_engine(16).await;
    let id = engine.allocate_page().await.unwrap();
    let full_payload = vec![0xABu8; PAGE_SIZE - HEADER_SIZE];
    engine.write_page_data(id, 0, &full_payload).await.unwrap();
    let read_back = engine
        .read_page_data(id, 0, PAGE_SIZE - HEADER_SIZE)
        .await
        .unwrap();
    assert_eq!(read_back, full_payload);
}
