# PR 1.2: B-Tree Index Implementation

## 📋 Overview

**PR Number:** 1.2
**Phase:** 1 - Core Foundation
**Priority:** 🔴 Critical
**Estimated Effort:** 6 days
**Dependencies:** PR 1.1 (Storage Engine)

## 🎯 Objectives

Implement a complete B-tree index structure that provides fast key-value lookups, range queries, and supports concurrent access. This is the foundation for primary keys, secondary indexes, and query optimization.

## 📝 Detailed Prompt for Implementation

```
Implement a production-ready B-tree index for AeternumDB with:

1. **B-Tree Structure**
   - Generic key/value types (K: Ord, V: Clone)
   - Configurable fanout (default 100, range 4-1000)
   - Internal nodes: keys + page pointers
   - Leaf nodes: keys + values + sibling pointers
   - Root tracking and height management

2. **Core Operations**
   - Insert: O(log n) with node splitting
   - Delete: O(log n) with node merging
   - Search: O(log n) exact match
   - Range scan: return iterator over range
   - Bulk loading optimization for initial load

3. **Concurrency**
   - Latch coupling (crabbing) for concurrent access
   - Read locks on internal nodes during descent
   - Write locks only on nodes being modified
   - Lock-free reads when possible

4. **Integration**
   - Use storage engine from PR 1.1 for persistence
   - Each B-tree node stored as a page
   - Node serialization/deserialization
   - Crash recovery support

5. **Performance Requirements**
   - >100,000 inserts/sec (single thread)
   - >500,000 reads/sec (single thread)
   - >50,000 inserts/sec (10 concurrent threads)
   - Support 1M+ keys efficiently
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/index/mod.rs`**
   - Public index API
   - Index trait definition
   - Index types enum

2. **`core/src/index/btree.rs`**
   - BTree struct and implementation
   - Core operations: insert, delete, search
   - Tree management

3. **`core/src/index/btree_node.rs`**
   - Node structure (internal vs leaf)
   - Node operations (split, merge, redistribute)
   - Serialization/deserialization

4. **`core/src/index/btree_iterator.rs`**
   - BTreeIterator for range scans
   - Forward and backward iteration

### Test Files

5. **`core/tests/btree_tests.rs`**
   - Comprehensive B-tree tests

6. **`core/benches/btree_bench.rs`**
   - Performance benchmarks

## 🔧 Implementation Details

### B-Tree Node Structure

```rust
pub struct BTree<K, V> {
    root_page_id: PageId,
    fanout: usize,
    height: usize,
    storage: Arc<StorageEngine>,
}

pub enum Node<K, V> {
    Internal(InternalNode<K>),
    Leaf(LeafNode<K, V>),
}

pub struct InternalNode<K> {
    keys: Vec<K>,
    children: Vec<PageId>,  // Page IDs of child nodes
}

pub struct LeafNode<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    next_leaf: Option<PageId>,  // For range scans
    prev_leaf: Option<PageId>,  // For backward iteration
}
```

### API Examples

```rust
// Create a new B-tree index
let btree = BTree::<i64, String>::new(
    storage.clone(),
    BTreeConfig {
        fanout: 100,
    }
).await?;

// Insert key-value pairs
btree.insert(42, "value".to_string()).await?;
btree.insert(100, "another".to_string()).await?;

// Search for exact key
if let Some(value) = btree.search(&42).await? {
    println!("Found: {}", value);
}

// Range scan
let iter = btree.range(10..=50).await?;
for (key, value) in iter {
    println!("{}: {}", key, value);
}

// Delete key
btree.delete(&42).await?;

// Bulk load (optimized)
let data = vec![(1, "a"), (2, "b"), (3, "c")];
btree.bulk_load(data).await?;
```

### Split and Merge Logic

```rust
// When node is full (fanout exceeded)
fn split_node<K, V>(node: &mut Node<K, V>) -> (K, Node<K, V>) {
    let mid = node.keys.len() / 2;
    let split_key = node.keys[mid].clone();

    // Split into two nodes
    let left_keys = node.keys[..mid].to_vec();
    let right_keys = node.keys[mid+1..].to_vec();

    // Return split key and new right node
    (split_key, right_node)
}

// When node is underfull (less than fanout/2)
fn merge_nodes<K, V>(left: Node<K, V>, right: Node<K, V>) -> Node<K, V> {
    // Combine keys and children/values
    // ...
}
```

## ✅ Tests Required

### Unit Tests

1. **Node Tests** (`btree_node.rs`)
   - ✅ Create internal and leaf nodes
   - ✅ Node split when full
   - ✅ Node merge when underfull
   - ✅ Serialize and deserialize nodes
   - ✅ Key insertion in sorted order

2. **BTree Tests** (`btree.rs`)
   - ✅ Insert single key
   - ✅ Insert multiple keys in order
   - ✅ Insert keys in random order
   - ✅ Search for existing keys
   - ✅ Search for non-existing keys
   - ✅ Delete keys
   - ✅ Tree height increases with splits
   - ✅ Tree height decreases with merges

3. **Iterator Tests** (`btree_iterator.rs`)
   - ✅ Forward iteration
   - ✅ Backward iteration
   - ✅ Range queries (inclusive, exclusive)
   - ✅ Empty range
   - ✅ Unbounded ranges

### Integration Tests

4. **Large Scale Tests** (`btree_tests.rs`)
   - ✅ Insert 1 million keys sequentially
   - ✅ Insert 1 million keys randomly
   - ✅ Mix insert and delete operations
   - ✅ Verify tree structure after operations
   - ✅ Range scan over entire tree
   - ✅ Verify tree height is optimal (log_fanout(n))

5. **Concurrency Tests**
   - ✅ Concurrent inserts (10 threads)
   - ✅ Concurrent reads (50 threads)
   - ✅ Mixed concurrent operations
   - ✅ No data races or corruption
   - ✅ All operations complete successfully

6. **Persistence Tests**
   - ✅ Insert keys, restart, verify keys exist
   - ✅ Tree structure persisted correctly
   - ✅ Corruption detection with checksums

### Performance Benchmarks

7. **Benchmarks** (`btree_bench.rs`)
   - ✅ Sequential insert throughput
   - ✅ Random insert throughput
   - ✅ Point query latency
   - ✅ Range scan throughput
   - ✅ Delete throughput
   - ✅ Bulk load vs individual inserts

## 📊 Performance Targets

| Operation | Target | Measurement |
|-----------|--------|-------------|
| Insert (sequential) | >100,000 ops/sec | Single thread |
| Insert (random) | >80,000 ops/sec | Single thread |
| Search (point) | >500,000 ops/sec | Single thread |
| Range scan | >200,000 keys/sec | Iterate 100K keys |
| Concurrent insert | >50,000 ops/sec | 10 threads |
| Bulk load | >300,000 keys/sec | 1M keys |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Rustdoc for all public APIs
   - Algorithm explanations
   - Complexity analysis

2. **Design Document** (`docs/btree-design.md`)
   - B-tree algorithm overview
   - Node split/merge strategy
   - Concurrency control explanation
   - Performance characteristics
   - Comparison with other index types

3. **Usage Guide** (`docs/btree-usage.md`)
   - How to create indexes
   - When to use B-trees
   - Configuration tuning
   - Examples

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] All CRUD operations work correctly
- [ ] Range queries return correct results
- [ ] Tree structure maintained after splits/merges
- [ ] Concurrent access is safe and correct
- [ ] Persistence and recovery work
- [ ] Tree height is optimal (logarithmic)

### Quality Requirements
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] All concurrency tests pass
- [ ] Code coverage >90%
- [ ] No clippy warnings
- [ ] Code formatted with `cargo fmt`

### Performance Requirements
- [ ] Insert throughput >100K ops/sec
- [ ] Search throughput >500K ops/sec
- [ ] Concurrent operations meet targets
- [ ] Memory usage is reasonable (<100 bytes overhead per key)

### Documentation Requirements
- [ ] All public APIs documented
- [ ] Design document complete
- [ ] Usage examples provided

## 🔗 Related Files

- `core/src/storage/` - Depends on storage engine
- `Cargo.toml` - May need additional dependencies

## 📦 Dependencies to Add

```toml
[dependencies]
# Likely no new dependencies needed
# Reuse existing: tokio, parking_lot, bytes
```

## 🚀 Implementation Steps

1. **Day 1: Node Structure**
   - Define `Node`, `InternalNode`, `LeafNode`
   - Implement basic operations (insert key, search key)
   - Serialization/deserialization
   - Unit tests

2. **Day 2: Core B-Tree**
   - Implement `BTree` struct
   - Insert with split logic
   - Search algorithm
   - Unit tests

3. **Day 3: Delete and Merge**
   - Delete operation
   - Node merge when underfull
   - Redistribute keys between siblings
   - Unit tests

4. **Day 4: Range Queries & Iterator**
   - Implement `BTreeIterator`
   - Range scan logic
   - Forward and backward iteration
   - Unit tests

5. **Day 5: Concurrency**
   - Add latch coupling
   - Concurrent insert/delete/search
   - Test with multiple threads
   - Stress tests

6. **Day 6: Performance & Documentation**
   - Bulk load optimization
   - Run benchmarks and optimize
   - Write documentation
   - Final testing

## 🐛 Known Edge Cases to Handle

1. **Empty tree**: Handle root being empty
2. **Single key**: Tree with only one key
3. **Duplicate keys**: Decide on behavior (error or update)
4. **Node splits at root**: New root creation
5. **Node merges at root**: Height reduction
6. **Concurrent split/merge**: Proper locking
7. **Range queries with no matches**: Empty iterator

## 💡 Future Enhancements (Out of Scope)

- B+ tree variant (all values in leaves) → Phase 5
- Bulk delete operations → Phase 5
- Prefix compression → Phase 5
- Write-optimized B-tree (LSM-like) → Phase 5
- Persistent iterators across transactions → Phase 5

## 🏁 Definition of Done

This PR is complete when:
1. All code implemented and tested
2. All acceptance criteria met
3. CI/CD passes
4. Code reviewed and approved
5. Documentation complete
6. Performance benchmarks meet targets
7. Integration with storage engine verified
8. No known bugs

---

**Ready to implement?** This specification provides everything needed. Start coding! 🚀
