---
sidebar_position: 2
---

# 🌲 BTree Design

[![B+ Tree](https://img.shields.io/badge/Structure-B%2B%20Tree-green)](https://github.com/schivei/aeternum-db)
[![Disk-Backed](https://img.shields.io/badge/Storage-Disk--Backed-blue)](https://github.com/schivei/aeternum-db)
[![Sibling Links](https://img.shields.io/badge/Range%20Scan-Sibling%20Links-orange)](https://github.com/schivei/aeternum-db)

AeternumDB uses a **B+ tree** variant for its primary index structure. All key-value pairs are stored in leaf nodes; internal nodes hold only separator keys and child page pointers. Leaves are **doubly-linked** so range scans traverse the leaf chain without revisiting internal nodes.

---

## High-Level Architecture

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

---

## On-Disk Node Formats

### Internal Node

```
┌─────────────────────────────────────────────────────────┐
│ node_type  : u8 = 0                                     │
│ num_keys   : u32 (little-endian)                        │
│ [key_len:u32 | key_bytes] × num_keys                    │
│ [child_page_id:u64] × (num_keys + 1)                    │
└─────────────────────────────────────────────────────────┘
```

An internal node with `n` keys has `n+1` children. Child `i` holds all keys in `[keys[i-1], keys[i])`.

### Leaf Node

```
┌─────────────────────────────────────────────────────────┐
│ node_type  : u8 = 1                                     │
│ num_pairs  : u32 (little-endian)                        │
│ [(key_len:u32 | key_bytes | val_len:u32 | val_bytes)] × n │
│ next_leaf  : u8 (0=None) [+ u64 PageId if present]     │
│ prev_leaf  : u8 (0=None) [+ u64 PageId if present]     │
└─────────────────────────────────────────────────────────┘
```

### Metadata Page

The first page allocated by `BTree.CreateAsync` stores tree-level state:

```
│ root_page_id : u64 (LE)  │
│ height       : u64 (LE)  │
│ num_keys     : u64 (LE)  │
│ fanout       : u32 (LE)  │
```

This page is how the tree survives process restarts — save the metadata `PageId` and pass it to `BTree.OpenAsync` on the next startup.

---

## Key Encoding

The `IBTreeKey` interface requires keys to produce byte sequences that preserve the natural ordering under **lexicographic comparison**:

| Type | Encoding |
|---|---|
| `long` (`i64`) | Big-endian with sign-bit flip: `(k as ulong) ^ (1UL << 63)` |
| `ulong` (`u64`) | Big-endian |
| `string` | UTF-8 bytes |
| `byte[]` | As-is |

:::caution
Custom key types **must** produce bytes where lexicographic order matches the type's natural ordering (`IComparable`). Use big-endian encoding for all numeric types.
:::

---

## Split Algorithm

When a node overflows (more than `Fanout` keys), AeternumDB performs a **middle split**:

1. Identify the median key (index `Fanout / 2`)
2. Create a new right sibling page
3. Move keys `[median+1 .. end]` to the right sibling
4. Promote the median key up to the parent
5. If the parent overflows, recursively split upward
6. If the root splits, allocate a new root page and increase tree height

---

## Merge / Redistribution

When a node underflows after deletion (fewer than `Fanout / 2` keys):

1. **Redistribute** — borrow a key from a sibling (if sibling has spare keys)
2. **Merge** — combine with a sibling and remove the separator from the parent

If the root has only one child after a merge, the child becomes the new root (tree height decreases).

---

## Range Scan Algorithm

Because leaf nodes are doubly-linked, range scans are O(k + log n) where k is the result size:

1. Traverse internal nodes from root to the leftmost matching leaf (O(log n))
2. Scan leaf entries forward through sibling links (O(k))
3. Stop when the key exceeds the upper bound or the end of the chain is reached

This is far more efficient than a full tree traversal for range queries.

---

## Concurrency Model

`BTree` uses the `StorageEngine`'s internal locking. The `BTree` instance itself can be passed to multiple tasks — all operations are serialized through the storage engine's `Mutex`.

```csharp
// Safe to share across tasks
var tree = await BTree.CreateAsync(storage);

await Task.WhenAll(
    Task.Run(() => tree.InsertAsync(1L, 10UL)),
    Task.Run(() => tree.InsertAsync(2L, 20UL)),
    Task.Run(() => tree.SearchAsync(1L))
);
```

---

## Crash Recovery

The BTree is designed for **crash safety**:

1. The metadata page is written **last** after all node changes are flushed
2. A node split writes the new sibling page before updating the parent — so a crash leaves an orphaned page (safe) rather than a corrupt tree
3. On open, `BTree.OpenAsync` reads the metadata page and validates `root_page_id` and `height`

:::info WAL
A full Write-Ahead Log (WAL) for atomic multi-page transactions is planned for **Phase 2**.
:::

---

## Performance Characteristics

| Operation | Complexity |
|---|---|
| Insert | O(log n) |
| Search | O(log n) |
| Delete | O(log n) |
| Range scan (k results) | O(log n + k) |
| Bulk load (sorted input) | O(n) |

With the default fanout of 100 and 8 KiB pages:

| Tree Size | Height | Pages |
|---|---|---|
| 10 000 keys | 2 | ~102 |
| 1 000 000 keys | 3 | ~10 200 |
| 100 000 000 keys | 4 | ~1 020 000 |
