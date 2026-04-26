# B-Tree Index Design

## Overview

AeternumDB uses a **B+ tree** variant for its primary index structure.  All
key-value pairs are stored in leaf nodes; internal nodes hold only separator
keys and child page pointers.  Leaves are doubly-linked so range scans can
traverse the leaf chain without revisiting internal nodes.

## Architecture

```
           ┌────────────────────────────────────┐
           │          StorageEngine              │
           │  (page-level I/O, buffer pool)      │
           └───────────────┬────────────────────┘
                           │ allocate / read / write pages
           ┌───────────────▼────────────────────┐
           │          BTree<K, V>               │
           │  root_page_id, height, num_keys,   │
           │  fanout  (metadata page)           │
           └───────────────┬────────────────────┘
                           │ serialize nodes ↔ pages
         ┌─────────────────┴──────────────────────┐
         │                                        │
  ┌──────▼──────┐                         ┌──────▼──────┐
  │InternalNode │  keys + child PageIds   │  LeafNode   │
  │  (height>1) │ ──────────────────────► │  (height=1) │
  └─────────────┘                         │ keys+values │
                                          │ next/prev ◄─┤ sibling links
                                          └─────────────┘
```

## Node Structure

### Internal Node

```text
┌─────────────────────────────────────────────────────────┐
│ node_type  : u8 = 0                                     │
│ num_keys   : u32 (little-endian)                        │
│ [key_len:u32 | key_bytes] × num_keys                    │
│ [child_page_id:u64] × (num_keys + 1)                    │
└─────────────────────────────────────────────────────────┘
```

An internal node with `n` keys has `n+1` children.  Child `i` holds all keys
in `[keys[i-1], keys[i])`.

### Leaf Node

```text
┌─────────────────────────────────────────────────────────┐
│ node_type  : u8 = 1                                     │
│ num_pairs  : u32                                        │
│ [(key_len:u32 | key_bytes | val_len:u32 | val_bytes)] × n │
│ next_leaf  : u8 (0=None) [+ u64 PageId if present]     │
│ prev_leaf  : u8 (0=None) [+ u64 PageId if present]     │
└─────────────────────────────────────────────────────────┘
```

### Metadata Page

The first page allocated by `BTree::new` stores tree-level state:

```text
│ root_page_id : u64 (LE)  │
│ height       : u64 (LE)  │
│ num_keys     : u64 (LE)  │
│ fanout       : u32 (LE)  │
```

## Key Encoding

The `BTreeKey` trait requires keys to produce byte sequences that preserve the
natural ordering under lexicographic comparison.  Implementations:

| Type    | Encoding                                              |
|---------|-------------------------------------------------------|
| `i64`   | Big-endian with sign-bit flip: `(k as u64) ^ (1<<63)` |
| `u64`   | Big-endian                                            |
| `String`| Raw UTF-8 bytes                                       |
| `Vec<u8>`| Identity                                             |

## Algorithms

### Insert

1. Descend from root to the target leaf, following child pointers in internal
   nodes via binary search on separator keys.
2. Insert the key-value pair into the leaf in sorted order.
3. If the leaf overflows (> fanout entries), split it at the midpoint:
   - Left half stays in the original page.
   - Right half is written to a new page.
   - The split key (first key of the right half) is pushed up to the parent.
4. Propagate splits up the tree as needed.
5. If the root splits, allocate a new root with two children and increment the
   tree height.

**Complexity:** O(log_fanout(n)) I/Os.

### Search

1. Descend from root to the leaf containing the key (binary search at each
   level).
2. Binary search within the leaf.

**Complexity:** O(log_fanout(n)) I/Os.

### Delete

1. Descend to the target leaf and remove the entry.
2. If the resulting leaf is empty, remove it from its parent and update
   sibling pointers.
3. If the parent becomes empty after removal, propagate the cleanup upward.
4. If the root becomes an empty internal node, collapse to its sole child,
   decrementing the tree height.

**Note:** This implementation uses *lazy deletion* — nodes may temporarily
underflow below `⌈fanout/2⌉`.  Full merge/redistribute is planned for a
future phase.

### Range Scan

1. Find the leaf containing the lower bound (or the leftmost leaf for
   unbounded queries) using the same descent as Search.
2. Pre-load all leaves in the range by following `next_leaf` sibling pointers.
3. Return a `BTreeIterator` over the pre-loaded leaves.

Pre-loading is done at `range()` call time so the iterator can be synchronous.

## Concurrency

The tree metadata (`root_page_id`, `height`, `num_keys`) is protected by a
`tokio::sync::RwLock`.  Readers take a shared lock; mutating operations (insert,
delete) take an exclusive lock for the duration of the operation.

**Future work:** Latch coupling (crabbing) will allow concurrent access to
different subtrees by releasing parent latches as the descent proceeds.

## Persistence and Recovery

- `BTree::new` allocates a metadata page and root leaf page; both are written
  to disk before returning.
- Every node write flushes through `StorageEngine::write_page_data`, which
  persists the page and updates the CRC-32 checksum.
- `BTree::open(engine, meta_page_id)` reconstructs tree state from the
  metadata page; the rest of the tree is loaded on demand from the buffer pool
  / disk.
- On crash, the engine's buffer pool is lost.  Any pages that were flushed
  before the crash (via `write_page_data`) are durable.  The WAL (PR 1.8)
  will provide full transactional durability.

## Performance Characteristics

| Operation        | Complexity    | Notes                           |
|------------------|---------------|---------------------------------|
| Insert           | O(log n)      | Amortized; splits are rare      |
| Delete           | O(log n)      | Lazy; no merge yet              |
| Point search     | O(log n)      |                                 |
| Range scan       | O(log n + k)  | k = number of matching keys     |
| Bulk load        | O(n log n)    | Sequential insert path          |

### Page size vs. fanout

For an 8 KiB page and 64-byte keys + 64-byte values, each leaf can hold
roughly 60 entries (with serialization overhead).  A tree of height 3 with
fanout 60 holds up to 60³ ≈ 216,000 keys without any disk reads for warm
queries.

## Comparison with Other Index Types

| Structure    | Strengths                       | Weaknesses                     |
|--------------|---------------------------------|---------------------------------|
| B-tree       | Good range scans, durable       | Write amplification on splits   |
| Hash index   | O(1) point lookups              | No range queries                |
| LSM-tree     | Write-optimised                 | Read amplification, compaction  |
| Trie/Radix   | Prefix queries                  | Memory-heavy for sparse keys    |

The B+ tree is the right choice for AeternumDB's primary and secondary indexes
because it supports both point lookups and range queries with predictable I/O.
