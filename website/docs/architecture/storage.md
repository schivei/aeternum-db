---
sidebar_position: 1
---

# 🗄️ Storage Engine Architecture

[![Async I/O](https://img.shields.io/badge/I%2FO-Async-blue)](https://github.com/schivei/aeternum-db)
[![LRU Buffer Pool](https://img.shields.io/badge/Cache-LRU%20Buffer%20Pool-green)](https://github.com/schivei/aeternum-db)
[![CRC-32](https://img.shields.io/badge/Integrity-CRC--32-orange)](https://github.com/schivei/aeternum-db)

The storage engine is the **lowest layer** of AeternumDB's persistence stack. It provides page-level read/write access to a single database file and keeps a hot subset of pages in an in-memory buffer pool to avoid unnecessary disk I/O.

---

## Component Overview

```
┌──────────────────────────────────────────────────────────┐
│                     StorageEngine                        │
│  (public API — async allocate/read/write/flush)          │
├────────────────────────────┬─────────────────────────────┤
│        BufferPool          │         FileManager         │
│  (LRU cache, pin tracking) │  (async file I/O, bitmap)   │
├────────────────────────────┴─────────────────────────────┤
│                       Page / PageHeader                  │
│           (fixed-size page, CRC-32 checksum)             │
└──────────────────────────────────────────────────────────┘
```

---

## Page Structure

Every unit of storage is a **page** — a fixed-size block (typically 4/8/16 KiB):

```
┌─────────────────────────────────────────────────────────────┐
│ PageHeader  (16 bytes, little-endian)                       │
│   page_id    : u64   bytes 0–7                              │
│   page_type  : u8    byte  8  (Data=0, Index=1, …)         │
│   reserved   : u8    byte  9                                │
│   free_space : u16   bytes 10–11                            │
│   checksum   : u32   bytes 12–15  (CRC-32 of payload)      │
├─────────────────────────────────────────────────────────────┤
│ Data payload  (page_size − 16 bytes)                        │
└─────────────────────────────────────────────────────────────┘
```

### PageType Values

| Value | Constant | Description |
|---|---|---|
| 0 | `Data` | Row / heap data |
| 1 | `Index` | B-tree or hash index node |
| 2 | `Overflow` | Large-value continuation |
| 3 | `Free` | Unallocated slot |

### CRC-32 Integrity

The **checksum** is computed over the data payload and stored in the header. A mismatch on read signals **corruption** and raises `StorageError.ChecksumMismatch`.

---

## FileManager

The `FileManager` owns a single database file on disk and provides:

| Method | Description |
|---|---|
| `AllocatePageAsync()` | Assigns a free slot and writes a zeroed page before marking it allocated |
| `DeallocatePageAsync(id)` | Writes a `Free` header before releasing the slot — crash-safe |
| `ReadPageAsync(id)` | Async seek + read at offset `id × pageSize` |
| `WritePageAsync(page)` | Async seek + write at offset `id × pageSize` |

### Free-Space Bitmap

A bitmap tracks which page slots are allocated. A LIFO free-list records freed slot IDs for quick **LIFO reuse** before growing the file.

### File Growth Strategy

When all slots are occupied, the file is extended by **64 pages at a time** — amortising the cost of system-level resize calls.

---

## Buffer Pool (LRU Cache)

The `BufferPool` is an in-memory **LRU cache** of pages.

```
page_table : Dictionary<PageId, frameIndex>
frames     : Frame[]                    // indexed by frameIndex
lru_order  : Queue<frameIndex>          // front = LRU (victim), back = MRU
free_frames: Queue<frameIndex>          // unused frame slots
```

Each `Frame` holds:
- `Page` — the actual page data
- `PinCount` — pages with `PinCount > 0` cannot be evicted
- `Dirty` — must be flushed to disk before the frame can be reused

### LRU Eviction

When a new page must be loaded and no free frame exists, the pool scans the `lru_order` queue from the front for the first unpinned, clean frame.

:::warning Dirty Pages
Dirty pages must be explicitly flushed via `StorageEngine.FlushAsync()` before they become eviction candidates. Until flushed, they are treated as pinned.
:::

---

## StorageEngine — Public API

```csharp
// Open or create a database file
await using var engine = await StorageEngine.OpenAsync("mydb.adb");

// Allocate a new page
ulong pageId = await engine.AllocatePageAsync();

// Write data to a page
await engine.WritePageDataAsync(pageId, offset: 0, data);

// Read data from a page
byte[] data = await engine.ReadPageDataAsync(pageId, offset: 0, length: 100);

// Flush all dirty pages to disk
await engine.FlushAsync();

// Free a page
await engine.DeallocatePageAsync(pageId);
```

### Key Methods

| Method | Description |
|---|---|
| `AllocatePageAsync()` | Acquire a new page slot, returns its `PageId` |
| `DeallocatePageAsync(id)` | Free a page slot |
| `PinPageAsync(id)` | Load page into pool, increment pin count |
| `UnpinPageAsync(id, dirty)` | Decrement pin count, optionally mark dirty |
| `WritePageDataAsync(id, offset, data)` | Write bytes, persist to disk |
| `ReadPageDataAsync(id, offset, length)` | Read bytes (load from disk on cache miss) |
| `FlushAsync()` | Write all dirty unpinned pages to disk |

---

## Sharding

Each `StorageEngine` instance manages **exactly one shard** (one database file). Horizontal sharding is achieved by running multiple engine instances:

```csharp
// Route logical page to shard
static (int shardId, ulong localPageId) Route(ulong logicalId, int numShards)
    => ((int)(logicalId % (ulong)numShards), logicalId / (ulong)numShards);

// One engine per shard
var shards = await Task.WhenAll(
    Enumerable.Range(0, 4).Select(i =>
        StorageEngine.OpenAsync($"/data/shard-{i:04}.adb")));
```

`StorageEngine` implements `IDisposable` and is thread-safe. Instances can be freely shared across async tasks.

---

## Read Replicas

A read replica opens the **same file path** with its own `StorageEngine` instance:

```csharp
// Primary (read-write)
var primary = await StorageEngine.OpenAsync("/data/primary.adb");

// Read replica (read-only by convention)
var replica = await StorageEngine.OpenAsync("/data/primary.adb");
```

Because `WritePageDataAsync` flushes to disk immediately, a replica that reopens the file sees up-to-date data after a brief delay.

---

## Error Handling

All public methods return `Task<Result<T>>` — no unexpected exceptions in production paths.

| Error Type | Variants |
|---|---|
| `PageError` | `WriteOutOfBounds`, `ReadOutOfBounds` |
| `FileManagerError` | `InvalidPageId`, `PageAlreadyFree`, `PageNotAllocated`, `Io`, `CorruptPage` |
| `BufferPoolError` | `PoolFull`, `PageNotFound`, `NotPinned`, `PageSizeMismatch` |
| `StorageError` | `BufferPool(…)`, `FileManager(…)`, `OutOfBounds`, `ChecksumMismatch`, `PagePinned` |

---

## Performance Targets

| Metric | Target |
|---|---|
| Sequential write throughput | > 10 000 pages/s |
| Random read throughput | > 15 000 pages/s |
| Buffer hit latency | < 1 ms (p99) |
| Disk read latency | < 10 ms (p99) |
| Memory overhead | < 5 % of pool size |

---

## Roadmap

| Feature | Phase |
|---|---|
| Write-Ahead Log (WAL) — crash durability | Phase 2 |
| MVCC — multi-version concurrency control | Phase 2 |
| Page compression (LZ4/Zstd) | Phase 5 |
| Encryption at rest (AES-256-GCM) | Phase 6 |
| Advanced eviction (LRU-K / Clock) | Phase 5 |
