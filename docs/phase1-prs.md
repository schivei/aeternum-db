# Phase 1: Core Foundation - PR Plan

This document details all pull requests needed to complete Phase 1 of the AeternumDB implementation.

## Overview

**Phase Goal:** Build a solid foundational database engine with storage, transaction management, and basic SQL support.

**Current Status:** Basic transaction, decimal, JSON, and versioning modules implemented. Need storage engine, SQL parser, and integration.

**Estimated Timeline:** 4-6 weeks
**Estimated PRs:** 14

---

## PR 1.1: Storage Engine - Basic Architecture

**Priority:** 🔴 Critical
**Estimated Effort:** 5 days
**Dependencies:** None

### Objectives
- Design and implement basic storage engine architecture
- File-based page storage
- Buffer pool manager
- Page lifecycle management

### Implementation Details

**Files to Create:**
- `core/src/storage/mod.rs`
- `core/src/storage/page.rs`
- `core/src/storage/buffer_pool.rs`
- `core/src/storage/file_manager.rs`

**Key Features:**
- Fixed-size pages (4KB, 8KB, 16KB configurable)
- LRU eviction policy for buffer pool
- Page pinning/unpinning mechanism
- Dirty page tracking
- Async I/O support using tokio

**Tests Required:**
- Page allocation and deallocation
- Buffer pool with various cache sizes
- Page eviction under memory pressure
- Concurrent page access
- Crash recovery simulation

**Documentation:**
- Storage architecture diagram
- API documentation
- Configuration options

### Acceptance Criteria
- [ ] Pages can be read/written to disk
- [ ] Buffer pool manages at least 1000 pages
- [ ] All tests pass
- [ ] Documentation complete
- [ ] Performance benchmark: >10K pages/sec

---

## PR 1.2: B-Tree Index Implementation

**Priority:** 🔴 Critical
**Estimated Effort:** 6 days
**Dependencies:** PR 1.1

### Objectives
- Implement B-tree index structure
- Support insert, delete, search operations
- Range queries
- Transaction-safe operations

### Implementation Details

**Files to Create:**
- `core/src/index/mod.rs`
- `core/src/index/btree.rs`
- `core/src/index/btree_node.rs`
- `core/src/index/btree_iterator.rs`

**Key Features:**
- Generic key/value types
- Variable fanout (configurable)
- Split and merge operations
- Concurrent access with latching
- Bulk loading optimization

**Tests Required:**
- Insert 1M keys
- Sequential and random inserts
- Range queries
- Concurrent modifications
- Index recovery after crash

**Documentation:**
- B-tree design document
- Performance characteristics
- Usage examples

### Acceptance Criteria
- [ ] B-tree supports all CRUD operations
- [ ] Range queries work correctly
- [ ] Concurrent access is safe
- [ ] All tests pass
- [ ] Performance: >100K inserts/sec

---

## PR 1.3: SQL Parser Integration

**Priority:** 🔴 Critical
**Estimated Effort:** 4 days
**Dependencies:** None

### Objectives
- Integrate sqlparser-rs library
- Parse common SQL statements (SELECT, INSERT, UPDATE, DELETE)
- AST representation
- Error handling and validation

### Implementation Details

**Files to Create:**
- `core/src/sql/mod.rs`
- `core/src/sql/parser.rs`
- `core/src/sql/ast.rs`
- `core/src/sql/validator.rs`

**Key Features:**
- Support SQL-92 subset
- Parse DDL (CREATE TABLE, DROP TABLE)
- Parse DML (SELECT, INSERT, UPDATE, DELETE)
- Syntax validation
- Semantic validation

**Tests Required:**
- Parse various SQL statements
- Error handling for invalid SQL
- Complex queries with JOINs
- Subqueries
- Aggregate functions

**Documentation:**
- Supported SQL syntax
- Limitations and extensions
- Examples

### Acceptance Criteria
- [ ] Parses all basic SQL statements
- [ ] Proper error messages
- [ ] All tests pass
- [ ] Documentation complete

---

## PR 1.4: Query Planner & Optimizer

**Priority:** 🟡 High
**Estimated Effort:** 7 days
**Dependencies:** PR 1.3

### Objectives
- Implement basic query planner
- Cost-based optimization
- Physical plan generation
- Execution plan visualization

### Implementation Details

**Files to Create:**
- `core/src/query/mod.rs`
- `core/src/query/planner.rs`
- `core/src/query/optimizer.rs`
- `core/src/query/physical_plan.rs`
- `core/src/query/cost_model.rs`

**Key Features:**
- Logical plan from AST
- Physical plan generation
- Index selection
- Join order optimization
- Statistics collection

**Tests Required:**
- Simple SELECT queries
- Complex JOINs
- Aggregations
- Subqueries
- Plan comparison

**Documentation:**
- Query optimization strategy
- Cost model explanation
- Debugging query plans

### Acceptance Criteria
- [ ] Generates correct execution plans
- [ ] Chooses indexes when available
- [ ] Join order optimization works
- [ ] All tests pass
- [ ] Documentation complete

---

## PR 1.5: Query Executor

**Priority:** 🔴 Critical
**Estimated Effort:** 8 days
**Dependencies:** PR 1.2, PR 1.4

### Objectives
- Execute physical query plans
- Iterator-based execution model
- Support various operators
- Memory management

### Implementation Details

**Files to Create:**
- `core/src/executor/mod.rs`
- `core/src/executor/scan.rs`
- `core/src/executor/filter.rs`
- `core/src/executor/join.rs`
- `core/src/executor/aggregate.rs`
- `core/src/executor/sort.rs`

**Key Features:**
- Sequential scan operator
- Index scan operator
- Nested loop join
- Hash join
- Sort-merge join
- Aggregation (SUM, COUNT, AVG, etc.)
- LIMIT and OFFSET

**Tests Required:**
- All operators individually
- Complex query execution
- Large datasets (>1M rows)
- Memory limits
- Cancellation

**Documentation:**
- Executor architecture
- Operator implementation guide
- Performance characteristics

### Acceptance Criteria
- [ ] All basic operators implemented
- [ ] Queries execute correctly
- [ ] Memory usage controlled
- [ ] All tests pass
- [ ] Performance: >100K rows/sec scan

---

## PR 1.6: Table Catalog & Schema Management

**Priority:** 🟡 High
**Estimated Effort:** 4 days
**Dependencies:** PR 1.1

### Objectives
- Implement system catalog
- Table metadata management
- Schema evolution support
- Persistence of metadata

### Implementation Details

**Files to Create:**
- `core/src/catalog/mod.rs`
- `core/src/catalog/table.rs`
- `core/src/catalog/column.rs`
- `core/src/catalog/schema.rs`

**Key Features:**
- Table registry
- Column definitions
- Primary keys and constraints
- Index registry
- Schema versioning

**Tests Required:**
- Create/drop tables
- Alter table operations
- Concurrent schema changes
- Recovery of catalog

**Documentation:**
- Catalog architecture
- Schema management API
- Migration guide

### Acceptance Criteria
- [ ] Tables can be created/dropped
- [ ] Metadata persists across restarts
- [ ] Schema changes supported
- [ ] All tests pass
- [ ] Documentation complete

---

## PR 1.7: Enhanced Transaction Manager

**Priority:** 🟡 High
**Estimated Effort:** 6 days
**Dependencies:** PR 1.1

### Objectives
- Enhance existing transaction module
- Multi-version concurrency control (MVCC)
- Deadlock detection
- Two-phase commit preparation

### Implementation Details

**Files to Update:**
- `core/src/acid.rs` → `core/src/transaction/mod.rs`
- Create `core/src/transaction/mvcc.rs`
- Create `core/src/transaction/lock_manager.rs`
- Create `core/src/transaction/deadlock.rs`

**Key Features:**
- MVCC with timestamp ordering
- Row-level locking
- Lock manager with timeout
- Deadlock detection algorithm
- Transaction log (WAL)

**Tests Required:**
- Concurrent transactions
- Isolation level verification
- Deadlock scenarios
- Rollback and recovery
- Long-running transactions

**Documentation:**
- MVCC design
- Isolation levels explained
- Transaction best practices

### Acceptance Criteria
- [ ] MVCC working correctly
- [ ] Deadlocks detected and handled
- [ ] All isolation levels supported
- [ ] All tests pass
- [ ] Performance: >10K transactions/sec

---

## PR 1.8: Write-Ahead Log (WAL)

**Priority:** 🔴 Critical
**Estimated Effort:** 5 days
**Dependencies:** PR 1.1, PR 1.7

### Objectives
- Implement Write-Ahead Logging
- Crash recovery
- Checkpointing
- Log archiving

### Implementation Details

**Files to Create:**
- `core/src/wal/mod.rs`
- `core/src/wal/logger.rs`
- `core/src/wal/recovery.rs`
- `core/src/wal/checkpoint.rs`

**Key Features:**
- Append-only log files
- Log record structure
- REDO and UNDO operations
- Fuzzy checkpointing
- Log rotation and archiving

**Tests Required:**
- Log writing and reading
- Crash recovery scenarios
- Checkpoint creation
- Log replay
- Performance under load

**Documentation:**
- WAL architecture
- Recovery protocol
- Configuration options

### Acceptance Criteria
- [ ] WAL records all changes
- [ ] Recovery works after crash
- [ ] Checkpoints reduce recovery time
- [ ] All tests pass
- [ ] Performance: >50K log entries/sec

---

## PR 1.9: Data Types System

**Priority:** 🟡 High
**Estimated Effort:** 4 days
**Dependencies:** None

### Objectives
- Define comprehensive type system
- Type conversion and coercion
- NULL handling
- Binary encoding/decoding

### Implementation Details

**Files to Create:**
- `core/src/types/mod.rs`
- `core/src/types/integer.rs`
- `core/src/types/decimal.rs` (enhance existing)
- `core/src/types/string.rs`
- `core/src/types/timestamp.rs`
- `core/src/types/json.rs` (enhance existing)

**Key Features:**
- Support INT, BIGINT, DECIMAL, VARCHAR, TEXT, TIMESTAMP, JSON, BOOLEAN
- Type checking and validation
- Binary serialization
- Comparison operators
- Type conversion rules

**Tests Required:**
- All data types
- Type conversions
- NULL handling
- Binary encoding/decoding
- Edge cases

**Documentation:**
- Supported data types
- Type conversion rules
- Precision and limits

### Acceptance Criteria
- [ ] All basic types implemented
- [ ] Type system is extensible
- [ ] Conversions work correctly
- [ ] All tests pass
- [ ] Documentation complete

---

## PR 1.10: Tuple & Record Format

**Priority:** 🟡 High
**Estimated Effort:** 3 days
**Dependencies:** PR 1.9

### Objectives
- Define tuple storage format
- Row serialization/deserialization
- Variable-length field handling
- NULL bitmap

### Implementation Details

**Files to Create:**
- `core/src/tuple/mod.rs`
- `core/src/tuple/row.rs`
- `core/src/tuple/codec.rs`

**Key Features:**
- Compact binary format
- NULL bitmap
- Variable-length fields
- Header with metadata
- Forward/backward compatibility

**Tests Required:**
- Serialize/deserialize various rows
- NULL values
- Large rows (>64KB)
- Schema evolution

**Documentation:**
- Tuple format specification
- Encoding details

### Acceptance Criteria
- [ ] Tuples can be encoded/decoded
- [ ] NULL handling works
- [ ] Format is space-efficient
- [ ] All tests pass

---

## PR 1.11: Basic Network Protocol

**Priority:** 🟢 Medium
**Estimated Effort:** 5 days
**Dependencies:** PR 1.5

### Objectives
- Define client-server protocol
- Connection management
- Query submission and results
- Error handling

### Implementation Details

**Files to Create:**
- `core/src/network/mod.rs`
- `core/src/network/protocol.rs`
- `core/src/network/server.rs`
- `core/src/network/connection.rs`

**Key Features:**
- TCP-based protocol
- Message framing
- Authentication handshake
- Query request/response
- Prepared statements
- Result streaming

**Tests Required:**
- Connection establishment
- Query execution
- Error scenarios
- Concurrent connections
- Large result sets

**Documentation:**
- Protocol specification
- Message formats
- Example client code

### Acceptance Criteria
- [ ] Clients can connect
- [ ] Queries can be executed
- [ ] Results returned correctly
- [ ] All tests pass
- [ ] Protocol documented

---

## PR 1.12: Configuration System

**Priority:** 🟢 Medium
**Estimated Effort:** 3 days
**Dependencies:** None

### Objectives
- Configuration file support
- Runtime configuration
- Environment variables
- Validation

### Implementation Details

**Files to Create:**
- `core/src/config/mod.rs`
- `core/src/config/loader.rs`
- `core/src/config/validator.rs`

**Key Features:**
- TOML configuration files
- Environment variable override
- Default values
- Configuration validation
- Hot reload (where applicable)

**Tests Required:**
- Load various configs
- Validation
- Defaults
- Environment overrides

**Documentation:**
- Configuration reference
- All options documented
- Examples

### Acceptance Criteria
- [ ] Config file loads correctly
- [ ] All options validated
- [ ] Environment vars work
- [ ] Documentation complete

---

## PR 1.13: Logging & Diagnostics

**Priority:** 🟢 Medium
**Estimated Effort:** 3 days
**Dependencies:** None

### Objectives
- Structured logging
- Performance metrics
- Debug tracing
- Log levels and filtering

### Implementation Details

**Files to Create:**
- `core/src/logging/mod.rs`
- `core/src/metrics/mod.rs`

**Dependencies:**
- `tracing` crate
- `tracing-subscriber`
- `metrics` crate

**Key Features:**
- Multiple log levels
- Structured fields
- Performance counters
- Span tracing
- Output to file/console

**Tests Required:**
- Logging at various levels
- Metrics collection
- Performance overhead

**Documentation:**
- Logging configuration
- Available metrics
- Debugging guide

### Acceptance Criteria
- [ ] Logging works correctly
- [ ] Metrics collected
- [ ] Low performance overhead
- [ ] Documentation complete

---

## PR 1.14: Integration & End-to-End Tests

**Priority:** 🟡 High
**Estimated Effort:** 4 days
**Dependencies:** All previous PRs

### Objectives
- Comprehensive integration tests
- End-to-end workflows
- Performance benchmarks
- Stress testing

### Implementation Details

**Files to Create:**
- `tests/integration/mod.rs`
- `tests/integration/basic_queries.rs`
- `tests/integration/transactions.rs`
- `tests/integration/concurrency.rs`
- `tests/benchmarks/mod.rs`

**Test Scenarios:**
- Full CRUD operations
- Complex queries
- Transaction scenarios
- Concurrent clients
- Crash recovery
- Performance benchmarks

**Benchmarks:**
- Insert throughput
- Query latency
- Transaction throughput
- Index performance
- Concurrent connections

### Acceptance Criteria
- [ ] All integration tests pass
- [ ] Performance meets targets
- [ ] No memory leaks
- [ ] Stress tests pass
- [ ] Benchmark report created

---

## Phase 1 Summary

**Total PRs:** 14
**Estimated Timeline:** 4-6 weeks
**Critical Path:** PR 1.1 → 1.2 → 1.4 → 1.5 → 1.14

**Key Milestones:**
- ✅ Storage engine functional
- ✅ SQL queries working
- ✅ Transactions safe
- ✅ Tests passing
- ✅ Basic performance acceptable

**Phase Complete When:**
- All 14 PRs merged
- Integration tests passing
- Documentation complete
- Performance benchmarks meet targets
- Ready for Phase 2 (Extensibility)

---

**Next Phase:** [Phase 2 - Extensibility](./phase2-prs.md)
