# PR 1.8: Write-Ahead Log (WAL)

## 📋 Overview

**PR Number:** 1.8
**Phase:** 1 - Core Foundation
**Priority:** 🔴 Critical
**Estimated Effort:** 5 days
**Dependencies:** PR 1.1 (Storage Engine), PR 1.7 (Transaction Manager)

## 🎯 Objectives

Implement a Write-Ahead Logging (WAL) system for durability and crash recovery. This includes:

- Append-only WAL file management
- Log record structures for different operations
- REDO and UNDO logging
- Crash recovery protocol
- Fuzzy checkpointing
- Log archiving and rotation

## 📝 Detailed Prompt for Implementation

```
Implement a complete Write-Ahead Logging system for AeternumDB with the following requirements:

1. **WAL File Management**
   - Append-only log files with sequential IDs
   - Log record structure: LSN, type, transaction ID, data
   - Log file rotation at configurable size (default 64MB)
   - Efficient log writing with buffering

2. **Log Record Types**
   - BEGIN: Transaction start
   - COMMIT: Transaction commit
   - ABORT: Transaction rollback
   - UPDATE: Data modification (REDO/UNDO info)
   - CHECKPOINT: System checkpoint

3. **Logging Protocol**
   - Force-log-at-commit (durability guarantee)
   - Write log before modifying data (write-ahead rule)
   - LSN (Log Sequence Number) assignment
   - Log flushing with fsync

4. **Recovery Protocol**
   - Analysis phase: Scan log to find last checkpoint
   - REDO phase: Replay all committed transactions
   - UNDO phase: Rollback uncommitted transactions
   - Recovery validation

5. **Checkpointing**
   - Fuzzy checkpoint (don't block operations)
   - Checkpoint record with active transactions
   - Checkpoint interval (time-based or size-based)
   - Checkpoint cleanup of old log files

6. **Log Archiving**
   - Archive old log files after checkpoint
   - Configurable retention policy
   - Compressed archives

7. **Performance Requirements**
   - >50,000 log records/sec
   - Log write latency <5ms
   - Recovery time <10s for 100K operations
   - Minimal impact on transaction throughput

Use Rust best practices, comprehensive error handling, and include detailed documentation.
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/wal/mod.rs`**
   - Public API for WAL
   - WAL manager implementation
   - Configuration

2. **`core/src/wal/logger.rs`**
   - Log writing
   - Log buffering
   - Log file management

3. **`core/src/wal/record.rs`**
   - Log record structures
   - Record serialization
   - Record parsing

4. **`core/src/wal/recovery.rs`**
   - Recovery coordinator
   - Analysis, REDO, UNDO phases
   - Recovery validation

5. **`core/src/wal/checkpoint.rs`**
   - Checkpoint manager
   - Fuzzy checkpoint implementation
   - Checkpoint records

6. **`core/src/wal/archive.rs`**
   - Log archiving
   - Retention management
   - Compression

### Test Files

7. **`core/tests/wal_tests.rs`**
   - Integration tests

8. **`core/benches/wal_bench.rs`**
   - Performance benchmarks

## 🔧 Implementation Details

### Log Record Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    pub lsn: LSN,
    pub prev_lsn: Option<LSN>,
    pub txn_id: TransactionId,
    pub record_type: LogRecordType,
    pub data: Vec<u8>,
    pub checksum: u32,
}

pub type LSN = u64; // Log Sequence Number

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogRecordType {
    Begin,
    Commit,
    Abort,
    Update {
        table_id: TableId,
        row_id: RowId,
        before_image: Vec<u8>,
        after_image: Vec<u8>,
    },
    Checkpoint {
        active_txns: Vec<TransactionId>,
        dirty_pages: Vec<PageId>,
    },
}

impl LogRecord {
    pub fn new(
        lsn: LSN,
        prev_lsn: Option<LSN>,
        txn_id: TransactionId,
        record_type: LogRecordType,
    ) -> Self {
        let data = bincode::serialize(&record_type).unwrap();
        let checksum = crc32fast::hash(&data);

        Self {
            lsn,
            prev_lsn,
            txn_id,
            record_type,
            data,
            checksum,
        }
    }

    pub fn verify_checksum(&self) -> bool {
        crc32fast::hash(&self.data) == self.checksum
    }
}
```

### WAL Manager

```rust
pub struct WalManager {
    log_dir: PathBuf,
    current_log: Arc<RwLock<LogWriter>>,
    lsn_counter: AtomicU64,
    log_buffer: Arc<RwLock<LogBuffer>>,
    checkpoint_mgr: Arc<CheckpointManager>,
    config: WalConfig,
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub log_dir: PathBuf,
    pub log_file_size: u64,  // 64MB default
    pub buffer_size: usize,   // 4MB default
    pub sync_interval: Duration,
    pub checkpoint_interval: Duration,
}

impl WalManager {
    pub async fn new(config: WalConfig) -> Result<Self> {
        let log_dir = config.log_dir.clone();
        fs::create_dir_all(&log_dir).await?;

        let current_log = LogWriter::open(&log_dir, 0).await?;

        Ok(Self {
            log_dir: log_dir.clone(),
            current_log: Arc::new(RwLock::new(current_log)),
            lsn_counter: AtomicU64::new(1),
            log_buffer: Arc::new(RwLock::new(LogBuffer::new(config.buffer_size))),
            checkpoint_mgr: Arc::new(CheckpointManager::new(log_dir)),
            config,
        })
    }

    pub async fn append(
        &self,
        txn_id: TransactionId,
        record_type: LogRecordType,
    ) -> Result<LSN> {
        // Allocate LSN
        let lsn = self.allocate_lsn();

        // Get previous LSN for this transaction
        let prev_lsn = self.get_prev_lsn(txn_id)?;

        // Create log record
        let record = LogRecord::new(lsn, prev_lsn, txn_id, record_type);

        // Add to buffer
        let mut buffer = self.log_buffer.write().await;
        buffer.append(record)?;

        // Flush if buffer is full
        if buffer.should_flush() {
            drop(buffer);
            self.flush().await?;
        }

        Ok(lsn)
    }

    pub async fn flush(&self) -> Result<()> {
        // Get buffered records
        let mut buffer = self.log_buffer.write().await;
        let records = buffer.drain();

        if records.is_empty() {
            return Ok(());
        }

        // Write to log file
        let mut log = self.current_log.write().await;

        for record in records {
            log.write_record(&record).await?;
        }

        // Force fsync for durability
        log.sync().await?;

        // Check if log rotation needed
        if log.size() >= self.config.log_file_size {
            self.rotate_log().await?;
        }

        Ok(())
    }

    pub async fn commit(&self, txn_id: TransactionId) -> Result<LSN> {
        // Append commit record
        let lsn = self.append(txn_id, LogRecordType::Commit).await?;

        // Force flush (force-log-at-commit)
        self.flush().await?;

        Ok(lsn)
    }

    async fn rotate_log(&self) -> Result<()> {
        let mut log = self.current_log.write().await;
        let old_log_id = log.id();

        // Create new log file
        let new_log_id = old_log_id + 1;
        let new_log = LogWriter::open(&self.log_dir, new_log_id).await?;

        // Replace current log
        *log = new_log;

        // Archive old log
        self.checkpoint_mgr.archive_log(old_log_id).await?;

        Ok(())
    }

    fn allocate_lsn(&self) -> LSN {
        self.lsn_counter.fetch_add(1, Ordering::SeqCst)
    }
}
```

### Log Writer

```rust
pub struct LogWriter {
    id: u64,
    file: File,
    position: u64,
}

impl LogWriter {
    pub async fn open(log_dir: &Path, id: u64) -> Result<Self> {
        let path = log_dir.join(format!("wal-{:010}.log", id));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(Self {
            id,
            file,
            position: 0,
        })
    }

    pub async fn write_record(&mut self, record: &LogRecord) -> Result<()> {
        // Serialize record
        let data = bincode::serialize(record)?;
        let len = data.len() as u32;

        // Write length prefix
        self.file.write_all(&len.to_le_bytes()).await?;

        // Write record data
        self.file.write_all(&data).await?;

        self.position += 4 + len as u64;

        Ok(())
    }

    pub async fn sync(&mut self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.position
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}
```

### Recovery Manager

```rust
pub struct RecoveryManager {
    wal_dir: PathBuf,
    storage: Arc<StorageEngine>,
}

impl RecoveryManager {
    pub async fn recover(&self) -> Result<()> {
        println!("Starting crash recovery...");

        // Phase 1: Analysis
        let analysis = self.analyze().await?;
        println!("Analysis complete. Active txns: {}, Dirty pages: {}",
                 analysis.active_txns.len(), analysis.dirty_pages.len());

        // Phase 2: REDO
        self.redo(&analysis).await?;
        println!("REDO phase complete");

        // Phase 3: UNDO
        self.undo(&analysis).await?;
        println!("UNDO phase complete");

        println!("Recovery complete!");

        Ok(())
    }

    async fn analyze(&self) -> Result<AnalysisResult> {
        let mut active_txns = HashMap::new();
        let mut dirty_pages = HashSet::new();
        let mut checkpoint_lsn = None;

        // Find all log files
        let mut log_files = self.find_log_files().await?;
        log_files.sort();

        // Scan log files
        for log_file in log_files {
            let reader = LogReader::open(&log_file).await?;

            while let Some(record) = reader.read_record().await? {
                match record.record_type {
                    LogRecordType::Begin => {
                        active_txns.insert(record.txn_id, record.lsn);
                    }
                    LogRecordType::Commit | LogRecordType::Abort => {
                        active_txns.remove(&record.txn_id);
                    }
                    LogRecordType::Update { table_id, row_id, .. } => {
                        let page_id = row_id_to_page_id(row_id);
                        dirty_pages.insert(page_id);
                    }
                    LogRecordType::Checkpoint { .. } => {
                        checkpoint_lsn = Some(record.lsn);
                    }
                }
            }
        }

        Ok(AnalysisResult {
            checkpoint_lsn,
            active_txns,
            dirty_pages,
        })
    }

    async fn redo(&self, analysis: &AnalysisResult) -> Result<()> {
        // Start from checkpoint (or beginning)
        let start_lsn = analysis.checkpoint_lsn.unwrap_or(0);

        let mut log_files = self.find_log_files().await?;
        log_files.sort();

        for log_file in log_files {
            let reader = LogReader::open(&log_file).await?;

            while let Some(record) = reader.read_record().await? {
                if record.lsn < start_lsn {
                    continue;
                }

                // Replay update operations
                if let LogRecordType::Update {
                    table_id,
                    row_id,
                    after_image,
                    ..
                } = record.record_type
                {
                    // Get page LSN
                    let page = self.storage.get_page(row_id_to_page_id(row_id)).await?;
                    let page_lsn = page.lsn();

                    // Only redo if page LSN < record LSN
                    if page_lsn < record.lsn {
                        self.storage.write_row(table_id, row_id, &after_image).await?;
                        self.storage.set_page_lsn(
                            row_id_to_page_id(row_id),
                            record.lsn
                        ).await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn undo(&self, analysis: &AnalysisResult) -> Result<()> {
        // For each active transaction, undo its changes
        for (txn_id, _) in &analysis.active_txns {
            self.undo_transaction(*txn_id).await?;
        }

        Ok(())
    }

    async fn undo_transaction(&self, txn_id: TransactionId) -> Result<()> {
        // Find all update records for this transaction
        let updates = self.find_transaction_updates(txn_id).await?;

        // Undo in reverse order
        for record in updates.into_iter().rev() {
            if let LogRecordType::Update {
                table_id,
                row_id,
                before_image,
                ..
            } = record.record_type
            {
                // Restore before image
                self.storage.write_row(table_id, row_id, &before_image).await?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct AnalysisResult {
    checkpoint_lsn: Option<LSN>,
    active_txns: HashMap<TransactionId, LSN>,
    dirty_pages: HashSet<PageId>,
}
```

### Checkpoint Manager

```rust
pub struct CheckpointManager {
    log_dir: PathBuf,
    last_checkpoint: AtomicU64,
}

impl CheckpointManager {
    pub async fn create_checkpoint(
        &self,
        active_txns: Vec<TransactionId>,
        dirty_pages: Vec<PageId>,
    ) -> Result<LSN> {
        // Create checkpoint record
        let record_type = LogRecordType::Checkpoint {
            active_txns,
            dirty_pages,
        };

        // Append to log (will be handled by WAL manager)
        let lsn = self.allocate_checkpoint_lsn();

        // Update last checkpoint
        self.last_checkpoint.store(lsn, Ordering::SeqCst);

        Ok(lsn)
    }

    pub async fn archive_log(&self, log_id: u64) -> Result<()> {
        let log_path = self.log_dir.join(format!("wal-{:010}.log", log_id));
        let archive_path = self.log_dir.join(format!("wal-{:010}.log.gz", log_id));

        // Compress and archive
        let input = File::open(&log_path).await?;
        let output = File::create(&archive_path).await?;

        // Compress with gzip
        let mut encoder = GzipEncoder::new(output);
        io::copy(&mut input, &mut encoder).await?;

        // Remove original
        fs::remove_file(&log_path).await?;

        Ok(())
    }

    fn allocate_checkpoint_lsn(&self) -> LSN {
        self.last_checkpoint.fetch_add(1, Ordering::SeqCst)
    }
}
```

### API Examples

```rust
use aeternumdb::wal::{WalManager, WalConfig, LogRecordType};

// Create WAL manager
let config = WalConfig {
    log_dir: PathBuf::from("/var/aeternumdb/wal"),
    log_file_size: 64 * 1024 * 1024, // 64MB
    buffer_size: 4 * 1024 * 1024,     // 4MB
    sync_interval: Duration::from_millis(100),
    checkpoint_interval: Duration::from_secs(60),
};

let wal = WalManager::new(config).await?;

// Begin transaction
let txn_id = 1;
wal.append(txn_id, LogRecordType::Begin).await?;

// Log update
wal.append(txn_id, LogRecordType::Update {
    table_id: 1,
    row_id: 100,
    before_image: old_data,
    after_image: new_data,
}).await?;

// Commit transaction (forces flush)
wal.commit(txn_id).await?;

// Recovery after crash
let recovery = RecoveryManager::new(wal_dir, storage);
recovery.recover().await?;
```

## ✅ Tests Required

### Unit Tests

1. **Log Record Tests** (`record.rs`)
   - ✅ Serialize/deserialize records
   - ✅ Checksum validation
   - ✅ All record types

2. **Log Writer Tests** (`logger.rs`)
   - ✅ Write records
   - ✅ Log rotation
   - ✅ Sync/flush

3. **Recovery Tests** (`recovery.rs`)
   - ✅ Analysis phase
   - ✅ REDO phase
   - ✅ UNDO phase
   - ✅ Full recovery

4. **Checkpoint Tests** (`checkpoint.rs`)
   - ✅ Create checkpoint
   - ✅ Archive logs
   - ✅ Checkpoint cleanup

### Integration Tests

5. **WAL Tests** (`wal_tests.rs`)
   - ✅ Log 10K operations
   - ✅ Crash recovery simulation
   - ✅ Concurrent logging
   - ✅ Recovery correctness
   - ✅ Checkpoint effectiveness

### Performance Benchmarks

6. **Benchmarks** (`wal_bench.rs`)
   - ✅ Log write throughput
   - ✅ Flush latency
   - ✅ Recovery time

## 📊 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Log write throughput | >50K records/sec | Benchmark |
| Flush latency | <5ms | p99 |
| Recovery time (100K ops) | <10s | Test |
| Log overhead | <15% | Comparison |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Comprehensive rustdoc comments
   - Recovery protocol explanation

2. **WAL Guide** (`docs/wal-architecture.md`)
   - WAL design
   - Recovery algorithm
   - Checkpoint strategy
   - Configuration tuning

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] All operations logged
- [ ] Crash recovery works
- [ ] Checkpoints created
- [ ] Logs archived
- [ ] Durability guaranteed

### Quality Requirements
- [ ] All tests pass
- [ ] Code coverage >85%
- [ ] Documentation complete

### Performance Requirements
- [ ] Meets throughput targets
- [ ] Fast recovery
- [ ] Low overhead

## 🔗 Dependencies

This PR depends on:
- **PR 1.1**: Storage Engine
- **PR 1.7**: Transaction Manager

This PR is required by:
- **PR 1.14**: Integration Tests

## 📦 Dependencies to Add

```toml
[dependencies]
crc32fast = "1.3"
flate2 = "1.0"  # For gzip compression
bincode = "1.3"
```

## 🚀 Implementation Steps

### Day 1: Log Record & Writer
- Define log record structures
- Implement log writer
- Write unit tests

### Day 2: WAL Manager
- Implement WAL manager
- Log buffering and flushing
- Log rotation
- Write tests

### Day 3: Recovery (Analysis & REDO)
- Implement analysis phase
- Implement REDO phase
- Write tests

### Day 4: Recovery (UNDO) & Checkpoint
- Implement UNDO phase
- Implement checkpointing
- Write tests

### Day 5: Integration & Documentation
- End-to-end integration tests
- Performance benchmarks
- Write WAL guide
- Code review

## 🐛 Known Edge Cases to Handle

1. **Log corruption**: Checksum validation
2. **Incomplete records**: Handle gracefully
3. **Disk full**: Error handling
4. **Concurrent flushing**: Synchronization
5. **Recovery failure**: Validation

## 💡 Future Enhancements (Out of Scope)

- Parallel recovery → Phase 3
- Point-in-time recovery → Phase 5
- Log streaming for replication → Phase 3
- Incremental checkpoints → Phase 5

## 🏁 Definition of Done

This PR is complete when:
1. All code implemented and tested
2. All acceptance criteria met
3. CI/CD passes
4. Documentation complete
5. Performance targets met

---

**Ready to implement?** Use this document as your complete specification. Good luck! 🚀
