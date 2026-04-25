# AeternumDB Storage Engine Architecture

## Overview

The storage engine is the lowest layer of AeternumDB's persistence stack.  It
provides page-level read/write access to a single database file and keeps a
subset of pages in an in-memory buffer pool to avoid unnecessary disk I/O.

---

## Components

```
┌──────────────────────────────────────────────────────────┐
│                     StorageEngine                        │
│  (public API — async pin/unpin/read/write/allocate)      │
├────────────────────────────┬─────────────────────────────┤
│        BufferPool          │         FileManager         │
│  (LRU cache, pin tracking) │  (async file I/O, bitmap)   │
├────────────────────────────┴─────────────────────────────┤
│                       Page / PageHeader                  │
│           (fixed-size page, CRC-32 checksum)             │
└──────────────────────────────────────────────────────────┘
```

### `page.rs` — Page & PageHeader

Every unit of storage is a **page** whose total size is fixed at
configuration time (typical values: 4 KB, 8 KB, 16 KB).

```
┌─────────────────────────────────────────────────────────────┐
│ PageHeader  (16 bytes, little-endian)                       │
│   page_id    : u64   bytes 0–7                              │
│   page_type  : u8    byte  8  (Data=0, Index=1, …)         │
│   reserved   : u8    byte  9                                │
│   free_space : u16   bytes 10–11                            │
│   checksum   : u32   bytes 12–15  (CRC-32 of data payload) │
├─────────────────────────────────────────────────────────────┤
│ Data payload  (page_size − 16 bytes)                        │
└─────────────────────────────────────────────────────────────┘
```

**Checksum** — CRC-32 (via `crc32fast`) is computed over the data payload and
stored in the header.  A mismatch signals corruption.

**PageType** values:

| Value | Constant     | Description               |
|-------|--------------|---------------------------|
| 0     | `Data`       | Row / heap data           |
| 1     | `Index`      | B-tree or hash index node |
| 2     | `Overflow`   | Large-value continuation  |
| 3     | `Free`       | Unallocated slot          |

---

### `file_manager.rs` — FileManager

The `FileManager` owns a single database file on disk and provides:

- **`allocate_page()`** — Assigns a free slot (reuses freed slots before
  growing) and writes a blank page header to disk.
- **`deallocate_page(id)`** — Marks the slot free in the in-memory bitmap and
  overwrites the on-disk header with `PageType::Free`.
- **`read_page(id)`** / **`write_page(page)`** — Async seek+read/write at
  offset `id × page_size`.

#### Free-space Bitmap

A `Vec<bool>` tracks which page slots are allocated.  A LIFO `VecDeque<PageId>`
(`free_list`) records freed slot IDs for quick reuse.

#### File Growth Strategy

When all slots are occupied the file is extended by **64 pages** at a time
(`GROWTH_CHUNK_PAGES = 64`), amortising the cost of system-level truncation
calls.

---

### `buffer_pool.rs` — BufferPool

The `BufferPool` is an in-memory LRU cache of `Page` objects.

#### Data Structures

```
page_table : HashMap<PageId, frame_index>
frames     : Vec<Option<Frame>>          // indexed by frame_index
lru_order  : VecDeque<frame_index>       // front = LRU (victim), back = MRU
free_frames: VecDeque<frame_index>       // unused frame slots
```

Each `Frame` holds:
- `page: Page`
- `pin_count: u32` — pages with `pin_count > 0` cannot be evicted
- `dirty: bool` — must be written to disk before the frame is reused

#### LRU Eviction

When a new page must be inserted and no free frame exists, the pool scans
`lru_order` from the front to find the first frame whose `pin_count == 0`.
That frame is reclaimed and its `PageId` removed from `page_table`.

#### Thread Safety

`BufferPool` itself is `!Sync`.  The `StorageEngine` wraps it in a
`tokio::sync::Mutex` so it can be safely shared across async tasks via `Arc`.

---

### `mod.rs` — StorageEngine

The `StorageEngine` is the single public entry point for callers.

```rust
pub struct StorageEngine {
    inner: Arc<Mutex<EngineInner>>, // tokio Mutex
    page_size: usize,
}

struct EngineInner {
    file_manager: FileManager,
    buffer_pool:  BufferPool,
    page_size:    usize,
}
```

It is cheaply `Clone`-able (clone shares the same `Arc`).

#### Key operations

| Method                         | Description                                  |
|-------------------------------|----------------------------------------------|
| `allocate_page()`             | Acquire a new page slot, return its `PageId` |
| `deallocate_page(id)`         | Free a page slot                             |
| `pin_page(id)`                | Load page into pool, increment pin count     |
| `unpin_page(id, dirty)`       | Decrement pin count, optionally mark dirty   |
| `write_page_data(id, off, …)` | Write bytes at offset, persist to disk       |
| `read_page_data(id, off, …)`  | Read bytes at offset (load from disk if miss)|
| `flush()`                     | Write all dirty unpinned pages to disk       |

---

## Configuration

```rust
StorageConfig {
    data_path:        PathBuf,  // database file path
    buffer_pool_size: usize,    // pages kept in RAM
    page_size:        usize,    // bytes per page (> 16)
}
```

Common page sizes and their trade-offs:

| Page size | Buffer @ 1 000 pages | Notes                     |
|-----------|---------------------|---------------------------|
| 4 096 B   |  ~4 MB              | Matches OS page, low waste |
| 8 192 B   |  ~8 MB              | Good default               |
| 16 384 B  | ~16 MB              | Fewer seeks, more waste   |

---

## Performance Targets

| Metric                        | Target         |
|------------------------------|----------------|
| Throughput — sequential write | > 10 000 p/s   |
| Throughput — random read      | > 15 000 p/s   |
| Latency — buffer hit          | < 1 ms (p99)   |
| Latency — disk read           | < 10 ms (p99)  |
| Memory overhead               | < 5 % of pool  |

Run the Criterion benchmark suite:

```sh
cargo bench --bench storage_bench
```

---

## Error Handling

All public methods return typed `Result` values.  No panics occur in
production paths; `assert!`/`panic` is used only in constructors for
programmer-error conditions (e.g. `page_size == 0`).

| Type                | Variants                                               |
|--------------------|--------------------------------------------------------|
| `PageError`         | `WriteOutOfBounds`, `ReadOutOfBounds`, `ChecksumMismatch` |
| `FileManagerError`  | `InvalidPageId`, `PageAlreadyFree`, `PageAlreadyAllocated`, `Io`, `CorruptPage` |
| `BufferPoolError`   | `PoolFull`, `PageNotFound`, `NotPinned`                |
| `StorageError`      | `BufferPool(…)`, `FileManager(…)`, `OutOfBounds`, `ChecksumMismatch` |

---

## Future Work

- **WAL (Write-Ahead Log)** — durability across crashes (PR 1.8)
- **MVCC** — multi-version concurrency control (PR 1.7)
- **Page compression** — transparent LZ4/Zstd (Phase 5)
- **Encryption at rest** — AES-256-GCM (Phase 6)
- **Advanced eviction** — LRU-K / Clock (Phase 5)
