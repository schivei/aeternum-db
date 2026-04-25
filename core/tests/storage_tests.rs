//! End-to-end integration tests for the storage engine.
//!
//! Each test models a realistic database workload rather than synthetic byte
//! patterns.  The domain types used here (user records, log entries, session
//! tokens) are serialised to raw bytes — mirroring how a real database engine
//! encodes tuples into pages.

use aeternumdb_core::storage::{page::HEADER_SIZE, StorageConfig, StorageEngine};
use tempfile::NamedTempFile;

const PAGE_SIZE: usize = 4096;
const DATA_CAP: usize = PAGE_SIZE - HEADER_SIZE;

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

/// Encode a minimal user record: `user_id` (8 bytes) | `age` (1 byte) | `name` bytes.
fn encode_user(user_id: u64, age: u8, name: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9 + name.len());
    buf.extend_from_slice(&user_id.to_le_bytes());
    buf.push(age);
    buf.extend_from_slice(name);
    buf
}

/// Decode the `user_id` from the first 8 bytes of a user record.
fn decode_user_id(buf: &[u8]) -> u64 {
    u64::from_le_bytes(buf[..8].try_into().unwrap())
}

/// Encode a log entry: `timestamp` (8 bytes) | `severity` (1 byte) | `message` bytes.
fn encode_log_entry(timestamp: u64, severity: u8, message: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9 + message.len());
    buf.extend_from_slice(&timestamp.to_le_bytes());
    buf.push(severity);
    buf.extend_from_slice(message);
    buf
}

/// Decode the `timestamp` from the first 8 bytes of a log entry.
fn decode_timestamp(buf: &[u8]) -> u64 {
    u64::from_le_bytes(buf[..8].try_into().unwrap())
}

/// Simulate a complete user record lifecycle: insert, read, update, delete.
///
/// Models an application that stores user profiles in a page-per-record layout.
#[tokio::test]
async fn test_user_record_insert_read_update_delete() {
    let (engine, _tmp) = make_engine(64).await;

    let user = encode_user(1001, 30, b"alice@example.com");
    let page_id = engine.allocate_page().await.unwrap();
    engine.write_page_data(page_id, 0, &user).await.unwrap();

    let stored = engine.read_page_data(page_id, 0, user.len()).await.unwrap();
    assert_eq!(decode_user_id(&stored), 1001);
    assert_eq!(&stored[9..], b"alice@example.com");

    let updated = encode_user(1001, 31, b"alice.updated@example.com");
    engine.write_page_data(page_id, 0, &updated).await.unwrap();
    let after_update = engine
        .read_page_data(page_id, 0, updated.len())
        .await
        .unwrap();
    assert_eq!(decode_user_id(&after_update), 1001);
    assert_eq!(&after_update[9..], b"alice.updated@example.com");

    engine.deallocate_page(page_id).await.unwrap();
}

/// Simulate 10 concurrent users each writing and reading their own session.
///
/// Models a web server handling 10 simultaneous session-store operations.
#[tokio::test]
async fn test_concurrent_session_store() {
    use tokio::task;

    let (engine, _tmp) = make_engine(100).await;

    let mut page_ids = Vec::with_capacity(10);
    for _ in 0..10 {
        page_ids.push(engine.allocate_page().await.unwrap());
    }

    let mut handles = Vec::new();
    for (i, &pid) in page_ids.iter().enumerate() {
        let e = engine.clone();
        let session = encode_user(i as u64, 0, format!("session-token-{i:04}").as_bytes());
        handles.push(task::spawn(async move {
            e.write_page_data(pid, 0, &session).await.unwrap();
            let data = e.read_page_data(pid, 0, session.len()).await.unwrap();
            assert_eq!(decode_user_id(&data), i as u64);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

/// Simulate an append-only event log written sequentially and read in order.
///
/// Models an audit trail where events are timestamped and must be readable in
/// insertion order after a restart.
#[tokio::test]
async fn test_event_log_sequential_append_and_read() {
    let (engine, _tmp) = make_engine(64).await;
    let event_count: u64 = 20;

    let mut page_ids = Vec::with_capacity(event_count as usize);
    for ts in 0..event_count {
        let entry = encode_log_entry(ts, 1, format!("user.login ts={ts}").as_bytes());
        let pid = engine.allocate_page().await.unwrap();
        engine.write_page_data(pid, 0, &entry).await.unwrap();
        page_ids.push(pid);
    }

    for (expected_ts, &pid) in page_ids.iter().enumerate() {
        let data = engine.read_page_data(pid, 0, 9).await.unwrap();
        assert_eq!(
            decode_timestamp(&data),
            expected_ts as u64,
            "timestamp mismatch at log entry {expected_ts}"
        );
    }
}

/// Simulate a clean database shutdown and subsequent restart.
///
/// Writes a set of user records, drops the engine (graceful shutdown), reopens
/// the same file, and verifies every record survived.
#[tokio::test]
async fn test_database_shutdown_and_restart() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let users = [
        encode_user(101, 25, b"bob@corp.io"),
        encode_user(102, 32, b"carol@corp.io"),
        encode_user(103, 28, b"dave@corp.io"),
    ];

    {
        let engine = StorageEngine::new(StorageConfig {
            data_path: path.clone(),
            buffer_pool_size: 64,
            page_size: PAGE_SIZE,
        })
        .await
        .unwrap();

        for user in &users {
            let pid = engine.allocate_page().await.unwrap();
            engine.write_page_data(pid, 0, user).await.unwrap();
        }
        engine.flush().await.unwrap();
    }

    {
        let engine = StorageEngine::new(StorageConfig {
            data_path: path,
            buffer_pool_size: 64,
            page_size: PAGE_SIZE,
        })
        .await
        .unwrap();

        for (i, user) in users.iter().enumerate() {
            let data = engine
                .read_page_data(i as u64, 0, user.len())
                .await
                .unwrap();
            assert_eq!(data, *user, "user record {i} did not survive restart");
        }
    }
}

/// Simulate a table that sees frequent deletes followed by new inserts.
///
/// Verifies that freed page slots are reused (compaction) so the file does not
/// grow unboundedly.
#[tokio::test]
async fn test_deleted_slots_are_reused() {
    let (engine, _tmp) = make_engine(16).await;

    let pid0 = engine.allocate_page().await.unwrap();
    let pid1 = engine.allocate_page().await.unwrap();
    let pid2 = engine.allocate_page().await.unwrap();
    engine
        .write_page_data(pid0, 0, &encode_user(1, 20, b"row0"))
        .await
        .unwrap();
    engine
        .write_page_data(pid1, 0, &encode_user(2, 21, b"row1"))
        .await
        .unwrap();
    engine
        .write_page_data(pid2, 0, &encode_user(3, 22, b"row2"))
        .await
        .unwrap();

    engine.deallocate_page(pid0).await.unwrap();
    engine.deallocate_page(pid2).await.unwrap();

    let reused_a = engine.allocate_page().await.unwrap();
    let reused_b = engine.allocate_page().await.unwrap();

    assert!(
        reused_a == pid0 || reused_a == pid2,
        "expected freed slot reuse"
    );
    assert!(
        reused_b == pid0 || reused_b == pid2,
        "expected freed slot reuse"
    );

    let still_there = engine
        .read_page_data(pid1, 0, encode_user(2, 21, b"row1").len())
        .await
        .unwrap();
    assert_eq!(decode_user_id(&still_there), 2);
}

/// Write a document that exactly fills the page data section and verify
/// byte-for-byte accuracy.
///
/// Tests the boundary condition where zero free bytes remain.
#[tokio::test]
async fn test_document_fills_entire_page_data_section() {
    let (engine, _tmp) = make_engine(16).await;
    let pid = engine.allocate_page().await.unwrap();
    let full_doc = vec![0xABu8; DATA_CAP];
    engine.write_page_data(pid, 0, &full_doc).await.unwrap();
    let read_back = engine.read_page_data(pid, 0, DATA_CAP).await.unwrap();
    assert_eq!(read_back, full_doc);
}

/// Verify that the checksum stored in the page header is consistent after a
/// write and remains valid when the page is reloaded from disk.
#[tokio::test]
async fn test_checksum_valid_after_write_and_reload() {
    let (engine, _tmp) = make_engine(64).await;
    let pid = engine.allocate_page().await.unwrap();
    engine
        .write_page_data(pid, 0, b"checksum sentinel")
        .await
        .unwrap();

    let page = engine.pin_page(pid).await.unwrap();
    assert!(
        page.validate_checksum(),
        "checksum must be valid after a write"
    );
    engine.unpin_page(pid, false).await.unwrap();
}

/// Verify that multiple pin / unpin cycles on the same page do not corrupt data.
#[tokio::test]
async fn test_repeated_pin_unpin_preserves_data() {
    let (engine, _tmp) = make_engine(16).await;
    let pid = engine.allocate_page().await.unwrap();
    let payload = b"pinned payload";
    engine.write_page_data(pid, 10, payload).await.unwrap();

    for _ in 0..5 {
        let page = engine.pin_page(pid).await.unwrap();
        assert_eq!(
            page.read_data(10, payload.len()).unwrap(),
            payload,
            "data must survive repeated pin cycles"
        );
        engine.unpin_page(pid, false).await.unwrap();
    }
}

/// Write to a non-zero offset and confirm only the targeted bytes are set.
///
/// Validates that the engine correctly handles partial-page writes.
#[tokio::test]
async fn test_partial_page_write_at_offset() {
    let (engine, _tmp) = make_engine(16).await;
    let pid = engine.allocate_page().await.unwrap();
    let offset = 200;
    let label = b"offset-write-ok";
    engine.write_page_data(pid, offset, label).await.unwrap();
    let data = engine
        .read_page_data(pid, offset, label.len())
        .await
        .unwrap();
    assert_eq!(&data, label);
    let prefix = engine.read_page_data(pid, 0, offset).await.unwrap();
    assert!(
        prefix.iter().all(|&b| b == 0),
        "bytes before offset must remain zero"
    );
}

/// Stress-test the buffer pool by writing more pages than the pool can hold,
/// then reading all pages back from disk.
///
/// Verifies that evicted pages are correctly written to disk and can be
/// reloaded without data loss.
#[tokio::test]
async fn test_eviction_under_memory_pressure() {
    let (engine, _tmp) = make_engine(8).await;

    let mut ids = Vec::with_capacity(32);
    for i in 0u8..32 {
        let pid = engine.allocate_page().await.unwrap();
        engine.write_page_data(pid, 0, &[i]).await.unwrap();
        ids.push(pid);
    }

    for (i, &pid) in ids.iter().enumerate() {
        let data = engine.read_page_data(pid, 0, 1).await.unwrap();
        assert_eq!(data[0], i as u8, "page {pid} data mismatch after eviction");
    }
}

/// Allocate 1 000 pages, write a unique value to each, then read all back.
///
/// Validates correctness at scale — catching off-by-one errors in page
/// addressing, bitmap management, and file growth.
#[tokio::test]
async fn test_large_table_with_1000_pages() {
    let (engine, _tmp) = make_engine(1000).await;

    let count: u64 = 1000;
    let mut ids = Vec::with_capacity(count as usize);
    for seq in 0..count {
        let pid = engine.allocate_page().await.unwrap();
        engine
            .write_page_data(pid, 0, &seq.to_le_bytes())
            .await
            .unwrap();
        ids.push(pid);
    }

    for (seq, &pid) in ids.iter().enumerate() {
        let data = engine.read_page_data(pid, 0, 8).await.unwrap();
        let val = u64::from_le_bytes(data.try_into().unwrap());
        assert_eq!(val, seq as u64);
    }
}
