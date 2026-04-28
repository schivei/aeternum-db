# PR 1.7: Enhanced Transaction Manager

## 📋 Overview

**PR Number:** 1.7
**Phase:** 1 - Core Foundation
**Priority:** 🟡 High
**Estimated Effort:** 6 days
**Dependencies:** PR 1.1 (Storage Engine)

## 🎯 Objectives

Enhance the existing basic transaction module to implement Multi-Version Concurrency Control (MVCC), sophisticated locking mechanisms, and deadlock detection. This includes:

- MVCC with snapshot isolation
- Row-level locking with lock manager
- Deadlock detection and resolution
- Transaction isolation levels
- Two-phase commit preparation
- Integration with storage engine

## 📝 Detailed Prompt for Implementation

```
Enhance the existing transaction system in AeternumDB with the following requirements:

1. **MVCC (Multi-Version Concurrency Control)**
   - Snapshot isolation by default
   - Transaction timestamps (begin_ts, commit_ts)
   - Version chains for rows
   - Garbage collection of old versions
   - Read views for consistent reads

2. **Lock Manager**
   - Row-level locking (shared and exclusive)
   - Lock table with hash-based storage
   - Lock waiting and timeout
   - Lock upgrade (shared to exclusive)
   - Deadlock detection graph

3. **Isolation Levels**
   - Read Uncommitted
   - Read Committed
   - Repeatable Read
   - Serializable
   - Implement according to SQL standard

4. **Deadlock Detection**
   - Wait-for graph construction
   - Cycle detection algorithm
   - Victim selection (youngest transaction)
   - Automatic rollback of victim

5. **Transaction States**
   - Active, Preparing, Committed, Aborted
   - State transitions
   - Transaction log

6. **Two-Phase Commit**
   - Prepare phase
   - Commit phase
   - Coordinator and participant roles
   - Recovery from failures

7. **Performance Requirements**
   - >10,000 transactions/sec throughput
   - <10ms transaction latency
   - Minimal lock contention
   - Efficient garbage collection

Use Rust best practices, comprehensive error handling, and include detailed documentation.
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/transaction/mod.rs`**
   - Refactor from `core/src/acid.rs`
   - Public API for transactions
   - TransactionManager implementation

2. **`core/src/transaction/mvcc.rs`**
   - MVCC implementation
   - Version chains
   - Snapshot isolation
   - Read views

3. **`core/src/transaction/lock_manager.rs`**
   - LockManager implementation
   - Lock table
   - Lock waiting and timeout

4. **`core/src/transaction/deadlock.rs`**
   - Deadlock detector
   - Wait-for graph
   - Victim selection

5. **`core/src/transaction/isolation.rs`**
   - Isolation level implementations
   - Visibility checks

6. **`core/src/transaction/two_phase_commit.rs`**
   - 2PC coordinator
   - 2PC participant
   - Recovery protocol

### Test Files

7. **`core/tests/transaction_tests.rs`**
   - Integration tests for transactions

8. **`core/benches/transaction_bench.rs`**
   - Transaction performance benchmarks

## 🔧 Implementation Details

### Transaction Structure

```rust
pub struct Transaction {
    pub id: TransactionId,
    pub begin_ts: Timestamp,
    pub commit_ts: Option<Timestamp>,
    pub state: TransactionState,
    pub isolation_level: IsolationLevel,
    pub read_view: Option<ReadView>,
    pub write_set: HashSet<RowId>,
    pub lock_set: HashSet<LockId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

pub type TransactionId = u64;
pub type Timestamp = u64;
```

### MVCC Implementation

```rust
pub struct MvccManager {
    timestamp_counter: AtomicU64,
    active_transactions: RwLock<HashMap<TransactionId, Transaction>>,
    committed_versions: RwLock<BTreeMap<Timestamp, TransactionId>>,
}

impl MvccManager {
    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Transaction> {
        let txn_id = self.allocate_transaction_id();
        let begin_ts = self.get_timestamp();

        let read_view = match isolation_level {
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                Some(self.create_read_view(begin_ts))
            }
            _ => None,
        };

        let txn = Transaction {
            id: txn_id,
            begin_ts,
            commit_ts: None,
            state: TransactionState::Active,
            isolation_level,
            read_view,
            write_set: HashSet::new(),
            lock_set: HashSet::new(),
        };

        let mut active = self.active_transactions.write();
        active.insert(txn_id, txn.clone());

        Ok(txn)
    }

    pub fn commit_transaction(&self, txn: &mut Transaction) -> Result<()> {
        // Validate transaction can commit
        if txn.state != TransactionState::Active {
            return Err(Error::InvalidTransactionState);
        }

        // Assign commit timestamp
        let commit_ts = self.get_timestamp();
        txn.commit_ts = Some(commit_ts);
        txn.state = TransactionState::Committed;

        // Record in committed versions
        let mut committed = self.committed_versions.write();
        committed.insert(commit_ts, txn.id);

        // Remove from active
        let mut active = self.active_transactions.write();
        active.remove(&txn.id);

        Ok(())
    }

    pub fn is_visible(
        &self,
        row_version: &RowVersion,
        txn: &Transaction,
    ) -> bool {
        // Check if row version is visible to transaction
        match txn.isolation_level {
            IsolationLevel::ReadUncommitted => {
                // See all versions, even uncommitted
                true
            }
            IsolationLevel::ReadCommitted => {
                // See committed versions up to now
                row_version.is_committed() &&
                    row_version.commit_ts <= self.get_timestamp()
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // Use read view for consistency
                if let Some(ref read_view) = txn.read_view {
                    read_view.is_visible(row_version)
                } else {
                    false
                }
            }
        }
    }

    fn create_read_view(&self, snapshot_ts: Timestamp) -> ReadView {
        let active = self.active_transactions.read();
        let active_txn_ids: Vec<TransactionId> = active.keys().copied().collect();

        ReadView {
            snapshot_ts,
            active_txn_ids,
        }
    }

    fn get_timestamp(&self) -> Timestamp {
        self.timestamp_counter.fetch_add(1, Ordering::SeqCst)
    }
}

pub struct ReadView {
    snapshot_ts: Timestamp,
    active_txn_ids: Vec<TransactionId>,
}

impl ReadView {
    pub fn is_visible(&self, row_version: &RowVersion) -> bool {
        // Version created by this transaction is always visible
        if row_version.created_by == self.snapshot_ts {
            return true;
        }

        // Version created after snapshot is not visible
        if row_version.created_by > self.snapshot_ts {
            return false;
        }

        // Version created by active transaction at snapshot time is not visible
        if self.active_txn_ids.contains(&row_version.created_by) {
            return false;
        }

        // Version must be committed
        row_version.is_committed()
    }
}
```

### Row Version Structure

```rust
pub struct RowVersion {
    pub row_id: RowId,
    pub version_id: VersionId,
    pub created_by: TransactionId,
    pub deleted_by: Option<TransactionId>,
    pub commit_ts: Option<Timestamp>,
    pub data: Vec<u8>,
    pub next_version: Option<VersionId>,
}

impl RowVersion {
    pub fn is_committed(&self) -> bool {
        self.commit_ts.is_some()
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_by.is_some()
    }
}
```

### Lock Manager

```rust
pub struct LockManager {
    locks: RwLock<HashMap<RowId, LockEntry>>,
    wait_graph: RwLock<WaitForGraph>,
    timeout: Duration,
}

#[derive(Debug)]
struct LockEntry {
    holders: Vec<LockHolder>,
    waiters: VecDeque<LockWaiter>,
}

#[derive(Debug)]
struct LockHolder {
    txn_id: TransactionId,
    lock_mode: LockMode,
}

#[derive(Debug)]
struct LockWaiter {
    txn_id: TransactionId,
    lock_mode: LockMode,
    notify: Arc<Notify>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

impl LockManager {
    pub async fn acquire_lock(
        &self,
        txn_id: TransactionId,
        row_id: RowId,
        lock_mode: LockMode,
    ) -> Result<()> {
        let start = Instant::now();

        loop {
            // Try to acquire lock
            let result = self.try_acquire_lock(txn_id, row_id, lock_mode)?;

            match result {
                AcquireResult::Granted => {
                    return Ok(());
                }
                AcquireResult::Wait(notify) => {
                    // Add to wait-for graph
                    self.add_wait_edge(txn_id, row_id)?;

                    // Check for deadlock
                    if self.has_deadlock(txn_id)? {
                        return Err(Error::Deadlock);
                    }

                    // Wait with timeout
                    match timeout(self.timeout, notify.notified()).await {
                        Ok(_) => {
                            // Notified, try again
                            continue;
                        }
                        Err(_) => {
                            // Timeout
                            self.remove_waiter(txn_id, row_id)?;
                            return Err(Error::LockTimeout);
                        }
                    }
                }
            }

            // Check total elapsed time
            if start.elapsed() > self.timeout {
                return Err(Error::LockTimeout);
            }
        }
    }

    fn try_acquire_lock(
        &self,
        txn_id: TransactionId,
        row_id: RowId,
        lock_mode: LockMode,
    ) -> Result<AcquireResult> {
        let mut locks = self.locks.write();
        let entry = locks.entry(row_id).or_insert_with(LockEntry::new);

        // Check if compatible with existing locks
        if entry.is_compatible(lock_mode) {
            // Grant lock
            entry.holders.push(LockHolder { txn_id, lock_mode });
            Ok(AcquireResult::Granted)
        } else {
            // Must wait
            let notify = Arc::new(Notify::new());
            entry.waiters.push_back(LockWaiter {
                txn_id,
                lock_mode,
                notify: notify.clone(),
            });
            Ok(AcquireResult::Wait(notify))
        }
    }

    pub fn release_lock(&self, txn_id: TransactionId, row_id: RowId) -> Result<()> {
        let mut locks = self.locks.write();

        if let Some(entry) = locks.get_mut(&row_id) {
            // Remove lock holder
            entry.holders.retain(|h| h.txn_id != txn_id);

            // Wake up waiters if possible
            while let Some(waiter) = entry.waiters.pop_front() {
                if entry.is_compatible(waiter.lock_mode) {
                    entry.holders.push(LockHolder {
                        txn_id: waiter.txn_id,
                        lock_mode: waiter.lock_mode,
                    });
                    waiter.notify.notify_one();
                } else {
                    // Put back and stop
                    entry.waiters.push_front(waiter);
                    break;
                }
            }

            // Remove entry if empty
            if entry.holders.is_empty() && entry.waiters.is_empty() {
                locks.remove(&row_id);
            }
        }

        Ok(())
    }
}

enum AcquireResult {
    Granted,
    Wait(Arc<Notify>),
}

impl LockEntry {
    fn is_compatible(&self, mode: LockMode) -> bool {
        if self.holders.is_empty() {
            return true;
        }

        match mode {
            LockMode::Shared => {
                // Shared is compatible with other shared locks
                self.holders.iter().all(|h| h.lock_mode == LockMode::Shared)
            }
            LockMode::Exclusive => {
                // Exclusive is not compatible with any lock
                false
            }
        }
    }
}
```

### Deadlock Detection

```rust
pub struct WaitForGraph {
    edges: HashMap<TransactionId, HashSet<TransactionId>>,
}

impl WaitForGraph {
    pub fn add_edge(&mut self, from: TransactionId, to: TransactionId) {
        self.edges.entry(from).or_default().insert(to);
    }

    pub fn remove_node(&mut self, txn_id: TransactionId) {
        self.edges.remove(&txn_id);
        for edges in self.edges.values_mut() {
            edges.remove(&txn_id);
        }
    }

    pub fn has_cycle(&self, start: TransactionId) -> bool {
        let mut visited = HashSet::new();
        let mut stack = HashSet::new();
        self.dfs(start, &mut visited, &mut stack)
    }

    fn dfs(
        &self,
        node: TransactionId,
        visited: &mut HashSet<TransactionId>,
        stack: &mut HashSet<TransactionId>,
    ) -> bool {
        if stack.contains(&node) {
            return true; // Cycle detected
        }

        if visited.contains(&node) {
            return false;
        }

        visited.insert(node);
        stack.insert(node);

        if let Some(neighbors) = self.edges.get(&node) {
            for &neighbor in neighbors {
                if self.dfs(neighbor, visited, stack) {
                    return true;
                }
            }
        }

        stack.remove(&node);
        false
    }
}

impl LockManager {
    fn has_deadlock(&self, txn_id: TransactionId) -> Result<bool> {
        let graph = self.wait_graph.read();
        Ok(graph.has_cycle(txn_id))
    }

    fn add_wait_edge(&self, waiter: TransactionId, row_id: RowId) -> Result<()> {
        let locks = self.locks.read();
        if let Some(entry) = locks.get(&row_id) {
            let mut graph = self.wait_graph.write();
            for holder in &entry.holders {
                graph.add_edge(waiter, holder.txn_id);
            }
        }
        Ok(())
    }
}
```

### API Examples

```rust
use aeternumdb::transaction::{TransactionManager, IsolationLevel};

// Create transaction manager
let txn_mgr = TransactionManager::new(storage.clone())?;

// Begin transaction
let mut txn = txn_mgr.begin(IsolationLevel::RepeatableRead).await?;

// Read data (acquires shared lock)
let row = txn.read(table_id, row_id).await?;

// Update data (acquires exclusive lock)
txn.write(table_id, row_id, new_data).await?;

// Commit transaction
txn.commit().await?;

// Or rollback
// txn.rollback().await?;

// Example: deadlock handling
let result = txn.write(table_id, row_id, data).await;
match result {
    Err(Error::Deadlock) => {
        // Transaction was rolled back due to deadlock
        println!("Deadlock detected, retry transaction");
    }
    Err(Error::LockTimeout) => {
        // Lock acquisition timed out
        println!("Lock timeout, retry transaction");
    }
    Ok(_) => {
        // Success
    }
}
```

## ✅ Tests Required

### Unit Tests

1. **MVCC Tests** (`mvcc.rs`)
   - ✅ Begin transaction
   - ✅ Commit transaction
   - ✅ Visibility checks (all isolation levels)
   - ✅ Read views
   - ✅ Version chains
   - ✅ Garbage collection

2. **Lock Manager Tests** (`lock_manager.rs`)
   - ✅ Acquire shared lock
   - ✅ Acquire exclusive lock
   - ✅ Lock compatibility
   - ✅ Lock upgrade
   - ✅ Lock timeout
   - ✅ Concurrent lock requests

3. **Deadlock Tests** (`deadlock.rs`)
   - ✅ Detect simple cycle (A→B→A)
   - ✅ Detect complex cycle (A→B→C→A)
   - ✅ No false positives
   - ✅ Victim selection

4. **Isolation Tests** (`isolation.rs`)
   - ✅ Read uncommitted
   - ✅ Read committed
   - ✅ Repeatable read
   - ✅ Serializable
   - ✅ Phantom reads prevention

### Integration Tests

5. **Transaction Tests** (`transaction_tests.rs`)
   - ✅ Concurrent reads (no blocking)
   - ✅ Concurrent writes (proper locking)
   - ✅ Lost update prevention
   - ✅ Dirty read prevention
   - ✅ Non-repeatable read prevention
   - ✅ Phantom read prevention
   - ✅ Deadlock resolution
   - ✅ Long-running transactions

### Performance Benchmarks

6. **Benchmarks** (`transaction_bench.rs`)
   - ✅ Transaction throughput
   - ✅ Lock acquisition latency
   - ✅ MVCC overhead
   - ✅ Concurrent transaction scaling

## 📊 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Transaction throughput | >10K txns/sec | Benchmark |
| Transaction latency | <10ms | p99 |
| Lock acquisition | <1ms | p99 |
| Deadlock detection | <100ms | Benchmark |
| MVCC overhead | <10% | Comparison |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Comprehensive rustdoc comments
   - MVCC algorithm explanation
   - Lock manager design

2. **Transaction Guide** (`docs/transactions.md`)
   - Isolation levels explained
   - MVCC architecture
   - Deadlock prevention tips
   - Best practices

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] MVCC works correctly
- [ ] All isolation levels implemented
- [ ] Deadlocks detected and resolved
- [ ] Locks acquired and released properly
- [ ] Concurrent transactions safe

### Quality Requirements
- [ ] All tests pass
- [ ] Code coverage >85%
- [ ] No clippy warnings
- [ ] Documentation complete

### Performance Requirements
- [ ] Throughput meets targets
- [ ] Low latency
- [ ] Minimal contention

## 🔗 Dependencies

This PR depends on:
- **PR 1.1**: Storage Engine

This PR is required by:
- **PR 1.5**: Query Executor (uses transactions)
- **PR 1.8**: WAL (transaction recovery)

## 📦 Dependencies to Add

```toml
[dependencies]
tokio = { version = "1.35", features = ["sync", "time"] }
petgraph = "0.6"  # For deadlock detection graph
```

## 🚀 Implementation Steps

### Day 1: MVCC Foundation
- Implement timestamp management
- Implement transaction structure
- Implement read views
- Write tests

### Day 2: Version Management
- Implement row versioning
- Implement version chains
- Implement visibility checks
- Write tests

### Day 3: Lock Manager (Part 1)
- Implement lock table
- Implement lock acquisition
- Implement lock release
- Write tests

### Day 4: Lock Manager (Part 2)
- Implement lock waiting
- Implement timeout handling
- Lock upgrade support
- Write tests

### Day 5: Deadlock Detection
- Implement wait-for graph
- Implement cycle detection
- Implement victim selection
- Write tests

### Day 6: Integration & Documentation
- Integration tests
- Performance benchmarks
- Write transaction guide
- Code review

## 🐛 Known Edge Cases to Handle

1. **Lock upgrade deadlock**: Careful ordering
2. **Timestamp overflow**: Handle wraparound
3. **Long transactions**: Memory management
4. **Aborted transaction cleanup**: Release locks
5. **Version chain length**: Garbage collection
6. **Read-only transactions**: Optimize locks

## 💡 Future Enhancements (Out of Scope)

- Optimistic concurrency control → Phase 5
- Distributed transactions → Phase 3
- Savepoints → Phase 5
- Prepared transactions → Phase 5

## 🔄 Deferred AeternumDB Extensions

The following AeternumDB extensions from earlier PRs (primarily PR 1.3) interact
with the transaction manager and must be implemented here or in a later PR:

### `FOR SYSTEM_TIME AS OF` queries

Temporal / time-travel queries require the transaction manager to expose committed
version history so the executor can reconstruct row state at an arbitrary past
timestamp.  This requires extending the MVCC read-view to accept an explicit
snapshot timestamp rather than always using the current timestamp.

### Versioned table row history

`CreateTableStatement::versioned = true` indicates the table retains full row
history.  The version chain must **never** be garbage-collected for versioned
tables; old versions must remain accessible for `FOR SYSTEM_TIME AS OF` reads.

## 🏁 Definition of Done

This PR is complete when:
1. All code implemented and tested
2. All acceptance criteria met
3. CI/CD passes
4. Documentation complete
5. Performance targets met

---

**Ready to implement?** Use this document as your complete specification. Good luck! 🚀
