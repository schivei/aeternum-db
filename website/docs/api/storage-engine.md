---
sidebar_position: 2
---

# 🗄️ StorageEngine API

Full API reference for `AeternumDB.Core.Storage.StorageEngine`.

---

## Opening / Closing

```csharp
// Open or create a database file
await using var storage = await StorageEngine.OpenAsync("mydb.adb");

// With explicit configuration
await using var storage = await StorageEngine.OpenAsync(new StorageConfig
{
    DataPath       = "/var/lib/mydb/data.adb",
    BufferPoolSize = 4_000,
    PageSize       = 8_192,
});
```

`StorageEngine` implements `IAsyncDisposable` — use `await using` to ensure all buffers are flushed on close.

---

## Page Lifecycle

```csharp
// Allocate a new page slot
PageId pageId = await storage.AllocatePageAsync();

// Free a page slot (marks it for reuse)
await storage.DeallocatePageAsync(pageId);
```

### AllocatePageAsync

| Aspect | Detail |
|---|---|
| Returns | `PageId` — the new page's unique identifier |
| Behaviour | Reuses freed slots before extending the file; extends by 64 pages when full |
| Crash-safety | Zeroed page is written to disk **before** the slot is marked allocated |

### DeallocatePageAsync

| Aspect | Detail |
|---|---|
| Behaviour | Writes a `Free`-type header before releasing the in-memory slot |
| Crash-safety | On restart, freed page is detected via its `PageType.Free` header |

---

## Reading and Writing

```csharp
// Write bytes to a page at a given byte offset
await storage.WritePageDataAsync(pageId, offset: 0, data: myBytes);

// Read bytes from a page
byte[] data = await storage.ReadPageDataAsync(pageId, offset: 0, length: 256);
```

| Method | Description |
|---|---|
| `WritePageDataAsync(id, offset, data)` | Writes `data` starting at `offset` bytes within the page; marks page dirty |
| `ReadPageDataAsync(id, offset, length)` | Reads `length` bytes from `offset`; loads page from disk on cache miss |

### Offset / Length Constraints

- `offset + data.Length` must be ≤ `PageSize − 16` (page header size)
- Violating this raises `StorageError.OutOfBounds`

---

## Buffer Pool Control

```csharp
// Pin a page (loads from disk if not in pool; increments pin count)
await storage.PinPageAsync(pageId);

// Unpin (decrement pin count; optionally mark dirty)
await storage.UnpinPageAsync(pageId, dirty: true);

// Flush all dirty unpinned pages to disk
await storage.FlushAsync();
```

### Pin / Unpin Semantics

- A page with `PinCount > 0` **cannot be evicted** from the buffer pool
- Every `PinPageAsync` call must be matched with a corresponding `UnpinPageAsync`
- Forgetting to unpin eventually causes `BufferPoolError.PoolFull`

---

## Properties

```csharp
int  PageSize       { get; }  // bytes per page
int  BufferPoolSize { get; }  // max pages in memory
long AllocatedPages { get; }  // total allocated page count
```

---

## Error Reference

| Error | Cause |
|---|---|
| `StorageError.ChecksumMismatch` | Page CRC-32 failed — data may be corrupt |
| `StorageError.OutOfBounds` | Offset + length exceeds page payload size |
| `StorageError.PagePinned` | Attempted to flush/deallocate a pinned page |
| `StorageError.BufferPool(PoolFull)` | All frames pinned or dirty; increase `BufferPoolSize` |
| `StorageError.FileManager(InvalidPageId)` | `PageId` was never allocated |

---

## Example — Custom Page Layout

```csharp
// Write a small record at a fixed offset within a page
static async Task WriteRecord(StorageEngine storage, PageId pageId, MyRecord record)
{
    var bytes = record.Serialize();                       // your serialization
    await storage.WritePageDataAsync(pageId, offset: 0, bytes);
}

// Read it back
static async Task<MyRecord> ReadRecord(StorageEngine storage, PageId pageId)
{
    byte[] bytes = await storage.ReadPageDataAsync(pageId, offset: 0, MyRecord.Size);
    return MyRecord.Deserialize(bytes);
}
```
