# PR 1.1: Storage Engine - Basic Architecture

## 📋 Overview

**PR Number:** 1.1
**Phase:** 1 - Core Foundation
**Priority:** 🔴 Critical
**Estimated Effort:** 5 days
**Dependencies:** None

## 🎯 Objectives

Design and implement the foundational storage engine architecture that will serve as the backbone for all data persistence in AeternumDB. This includes:

- File-based page storage system
- Buffer pool manager with LRU eviction
- Page lifecycle management (allocation, deallocation, pinning)
- Async I/O support for high performance
- Foundation for future MVCC and WAL integration

## 📝 Detailed Prompt for Implementation

```
Implement a complete storage engine for AeternumDB with the following requirements:

1. **Page-Based Storage**
   - Fixed-size pages (configurable: 4KB, 8KB, 16KB)
   - Page structure: header (16 bytes) + data
   - Page header includes: page_id, page_type, free_space, checksum
   - Support for different page types: data, index, overflow

2. **File Manager**
   - Single file or multiple files per table
   - Page allocation/deallocation
   - Free space tracking using bitmap
   - File growth strategy (pre-allocate in chunks)

3. **Buffer Pool Manager**
   - LRU eviction policy
   - Page pinning/unpinning mechanism
   - Dirty page tracking
   - Configurable pool size
   - Thread-safe concurrent access using RwLock

4. **Async I/O**
   - Use tokio for async file operations
   - Batch writes for efficiency
   - Non-blocking reads when page in buffer

5. **Performance Requirements**
   - >10,000 pages/sec throughput
   - <1ms average page access latency (buffer hit)
   - <10ms average page access latency (disk read)

Use Rust best practices, comprehensive error handling, and include detailed documentation.
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/storage/mod.rs`**
   - Public API for storage engine
   - StorageEngine struct and implementation
   - Configuration structures

2. **`core/src/storage/page.rs`**
   - Page structure definition
   - Page header and data layout
   - Page serialization/deserialization
   - Checksum calculation

3. **`core/src/storage/buffer_pool.rs`**
   - BufferPool struct with LRU cache
   - Pin/unpin tracking
   - Dirty page management
   - Eviction logic

4. **`core/src/storage/file_manager.rs`**
   - FileManager for disk I/O
   - Page allocation/deallocation
   - Free space bitmap
   - File growth strategy

### Test Files

5. **`core/tests/storage_tests.rs`**
   - Integration tests for storage engine

6. **`core/benches/storage_bench.rs`**
   - Performance benchmarks

## 🔧 Implementation Details

### Page Structure

```rust
pub struct Page {
    pub id: PageId,
    pub header: PageHeader,
    pub data: Vec<u8>,
}

pub struct PageHeader {
    pub page_id: u64,
    pub page_type: PageType,
    pub free_space: u16,
    pub checksum: u32,
}

pub enum PageType {
    Data,
    Index,
    Overflow,
    Free,
}
```

### Buffer Pool Configuration

```rust
pub struct BufferPoolConfig {
    pub capacity: usize,      // Number of pages in buffer
    pub page_size: usize,     // Size in bytes (4KB, 8KB, 16KB)
    pub eviction_policy: EvictionPolicy,
}

pub enum EvictionPolicy {
    LRU,
    Clock,  // Future: implement clock algorithm
}
```

### API Examples

```rust
// Initialize storage engine
let storage = StorageEngine::new(StorageConfig {
    data_directory: "/var/aeternumdb/data",
    buffer_pool_size: 1000,
    page_size: 8192,
})?;

// Allocate a new page
let page_id = storage.allocate_page().await?;

// Write to page
let mut page = storage.pin_page(page_id).await?;
page.write_data(0, &data)?;
storage.unpin_page(page_id, true).await?; // true = mark dirty

// Read from page
let page = storage.pin_page(page_id).await?;
let data = page.read_data(0, 100)?;
storage.unpin_page(page_id, false).await?;

// Deallocate page
storage.deallocate_page(page_id).await?;
```

## ✅ Tests Required

### Unit Tests

1. **Page Tests** (`page.rs`)
   - ✅ Create page with correct header
   - ✅ Serialize and deserialize page
   - ✅ Checksum calculation and validation
   - ✅ Page type conversions

2. **Buffer Pool Tests** (`buffer_pool.rs`)
   - ✅ Add pages to buffer pool
   - ✅ LRU eviction when pool is full
   - ✅ Pin/unpin tracking
   - ✅ Dirty page handling
   - ✅ Concurrent access (multiple threads)

3. **File Manager Tests** (`file_manager.rs`)
   - ✅ Allocate and deallocate pages
   - ✅ Write and read pages from disk
   - ✅ Free space bitmap management
   - ✅ File growth when full

### Integration Tests

4. **Storage Engine Tests** (`storage_tests.rs`)
   - ✅ Full CRUD operations on pages
   - ✅ Buffer pool with 1000 pages
   - ✅ Page eviction under memory pressure
   - ✅ Concurrent page access (10+ threads)
   - ✅ Crash recovery simulation (write dirty pages, simulate crash, verify recovery)

### Performance Benchmarks

5. **Benchmarks** (`storage_bench.rs`)
   - ✅ Sequential page writes
   - ✅ Random page reads
   - ✅ Mixed workload (80% read, 20% write)
   - ✅ Concurrent access benchmark

## 📊 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Throughput (sequential write) | >10,000 pages/sec | `cargo bench` |
| Throughput (random read) | >15,000 pages/sec | `cargo bench` |
| Latency (buffer hit) | <1ms | 99th percentile |
| Latency (disk read) | <10ms | 99th percentile |
| Memory overhead | <5% of buffer pool | Measure with allocators |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Comprehensive rustdoc comments for all public APIs
   - Example usage in doc comments
   - Safety notes for unsafe code (if any)

2. **Architecture Document** (`docs/storage-architecture.md`)
   - Storage engine design overview
   - Page layout diagram
   - Buffer pool algorithm explanation
   - Configuration options reference

3. **API Documentation** (rustdoc)
   - Generate with `cargo doc --no-deps --open`
   - Ensure all public APIs documented

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] Pages can be allocated and deallocated
- [ ] Pages can be written to and read from disk
- [ ] Buffer pool manages at least 1000 pages
- [ ] LRU eviction works correctly
- [ ] Dirty pages are tracked and persisted
- [ ] Concurrent access is safe (no data corruption)
- [ ] Checksum validation prevents corruption

### Quality Requirements
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Code coverage >85%
- [ ] No clippy warnings
- [ ] Code formatted with `cargo fmt`
- [ ] Documentation complete and accurate

### Performance Requirements
- [ ] Throughput meets targets (>10K pages/sec)
- [ ] Latency meets targets (<1ms buffer, <10ms disk)
- [ ] Memory usage is reasonable
- [ ] No memory leaks (run with valgrind or similar)

### Documentation Requirements
- [ ] All public APIs documented
- [ ] Architecture document complete
- [ ] Configuration options documented
- [ ] Examples provided

## 🔗 Related Files

- `Cargo.toml` - Add dependencies: `tokio`, `bytes`, `crc32fast`
- `core/src/lib.rs` - Add storage module export
- `.github/workflows/ci.yml` - Ensure tests run in CI

## 📦 Dependencies to Add

```toml
[dependencies]
tokio = { version = "1.35", features = ["fs", "io-util", "rt-multi-thread"] }
bytes = "1.5"
crc32fast = "1.3"
parking_lot = "0.12"  # For efficient RwLock

[dev-dependencies]
criterion = "0.5"
tempfile = "3.8"
```

## 🚀 Implementation Steps

1. **Day 1: Page Structure & Serialization**
   - Define `Page` and `PageHeader` structs
   - Implement serialization/deserialization
   - Add checksum calculation
   - Write unit tests

2. **Day 2: File Manager**
   - Implement `FileManager` for disk I/O
   - Page allocation with bitmap
   - File growth strategy
   - Write unit tests

3. **Day 3: Buffer Pool**
   - Implement `BufferPool` with LRU
   - Pin/unpin mechanism
   - Dirty page tracking
   - Thread-safety with RwLock
   - Write unit tests

4. **Day 4: Storage Engine Integration**
   - Implement `StorageEngine` API
   - Connect file manager and buffer pool
   - Async I/O with tokio
   - Write integration tests

5. **Day 5: Performance & Documentation**
   - Run benchmarks and optimize
   - Write architecture documentation
   - Code review and cleanup
   - Final testing

## 🐛 Known Edge Cases to Handle

1. **Buffer pool full**: Ensure LRU eviction works and pinned pages are not evicted
2. **Disk full**: Handle file growth failures gracefully
3. **Corrupted pages**: Checksum validation should catch corruption
4. **Concurrent access**: Multiple threads accessing same page
5. **Crash during write**: Dirty pages may be lost (handled by WAL in future PR)

## 💡 Future Enhancements (Out of Scope)

- Write-Ahead Log (WAL) integration → PR 1.8
- MVCC support → PR 1.7
- Page compression → Phase 5
- Encryption at rest → Phase 6
- Advanced eviction policies (Clock, LRU-K) → Phase 5

## 🏁 Definition of Done

This PR is complete when:
1. All code is implemented and tested
2. All acceptance criteria met
3. CI/CD pipeline passes
4. Code reviewed and approved
5. Documentation published
6. Performance benchmarks meet targets
7. No known bugs or issues

---

**Ready to implement?** Use this document as your complete specification. All details needed are provided above. Good luck! 🚀
