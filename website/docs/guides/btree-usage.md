---
sidebar_position: 4
---

# 🌲 BTree Index Usage

[![Disk-Backed](https://img.shields.io/badge/Index-Disk--Backed%20B%2B%20Tree-green)](https://github.com/schivei/aeternum-db)
[![Async](https://img.shields.io/badge/API-Async-blue)](https://github.com/schivei/aeternum-db)

The `BTree<K, V>` in `AeternumDB.Core.Index` provides a **persistent, concurrent B+ tree index** backed by the AeternumDB storage engine.

---

## Creating an Index

```csharp
using AeternumDB.Core.Index;
using AeternumDB.Core.Storage;

// 1. Open storage
var storage = await StorageEngine.OpenAsync(new StorageConfig
{
    DataPath       = "/var/lib/mydb/index.adb",
    BufferPoolSize = 1_000,
    PageSize       = 8_192,
});

// 2. Create a new BTree (save the metaPageId for later reopens!)
var btree = await BTree.CreateAsync(storage, new BTreeConfig { Fanout = 100 });
long metaPageId = btree.MetaPageId;
Console.WriteLine($"📄 Metadata page: {metaPageId}");
```

---

## Reopening an Existing Index

```csharp
// On next startup — reopen using the saved metadata page ID
var btree = await BTree.OpenAsync(storage, metaPageId);
Console.WriteLine($"✅ Index reopened. Count: {await btree.CountAsync()}");
```

---

## Configuration

```csharp
new BTreeConfig
{
    Fanout = 100,  // Valid range: 4–1000. Default: 100.
}
```

### Choosing a Fanout

| Fanout | Tree Levels | Best For |
|---|---|---|
| Low (10–30) | More levels | Large values per entry |
| Default (100) | ~3 levels | Most workloads ✅ |
| High (500+) | ~2 levels | Small keys, maximum lookup speed |

:::tip
Keep `Fanout × max_entry_bytes` well below `PageSize − 16` to avoid serialization errors.
:::

---

## Supported Key & Value Types

| Type | As Key | As Value |
|---|---|---|
| `long` / `int` | ✅ | ✅ |
| `ulong` / `uint` | ✅ | ✅ |
| `string` | ✅ | ✅ |
| `byte[]` | ✅ | ✅ |
| Custom (implement `IBTreeKey`) | ✅ | N/A |

### Custom Key Type

```csharp
using AeternumDB.Core.Index;

public readonly struct UserId : IBTreeKey<UserId>
{
    public readonly ulong Value;
    public UserId(ulong v) => Value = v;

    // Must produce bytes that preserve Ord (use big-endian for numerics)
    public byte[] ToBytes() => BitConverter.GetBytes(
        BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(Value) : Value);

    public static UserId FromBytes(ReadOnlySpan<byte> bytes) =>
        new(BinaryPrimitives.ReadUInt64BigEndian(bytes));
}
```

---

## CRUD Operations

### ✍️ Insert

```csharp
await btree.InsertAsync(42L, "hello");
// Throws IndexError.DuplicateKey if key already exists
```

### ↕️ Upsert (Insert or Update)

```csharp
await btree.UpsertAsync(42L, "world");
// Inserts if new; replaces value if key exists
```

### 🔍 Search

```csharp
var result = await btree.SearchAsync(42L);

if (result is { } value)
    Console.WriteLine($"Found: {value}");
else
    Console.WriteLine("Not found");
```

### 🗑️ Delete

```csharp
bool removed = await btree.DeleteAsync(42L);
Console.WriteLine(removed ? "✅ Key removed" : "⚠️ Key not found");
```

---

## Range Queries

Range queries accept any bounds, including open/half-open ranges:

```csharp
// Inclusive range: keys 10 to 20
await foreach (var (key, value) in btree.RangeScanAsync(10L, 20L))
    Console.WriteLine($"  {key} → {value}");

// From a key to the end (no upper bound)
await foreach (var (key, value) in btree.RangeScanAsync(10L, null))
    Console.WriteLine($"  {key}");

// Full scan
await foreach (var (key, value) in btree.RangeScanAsync(null, null))
    Console.WriteLine($"  {key}");
```

---

## Bulk Loading

When you have a large pre-sorted dataset, use `BulkLoadAsync` instead of individual inserts — **orders of magnitude faster**:

```csharp
var entries = Enumerable
    .Range(0, 1_000_000)
    .Select(i => ((long)i, (ulong)(i * 10)));

await btree.BulkLoadAsync(entries);
Console.WriteLine($"✅ Bulk loaded. Count: {await btree.CountAsync()}");
```

:::info
Input does not need to be sorted, but ascending key order gives the best performance. Duplicate keys raise `IndexError.DuplicateKey`.
:::

---

## Concurrent Access

`BTree` is `Clone`-able and thread-safe. Clones share the same underlying storage:

```csharp
var sharedTree = btree;

var tasks = Enumerable.Range(0, 10).Select(t => Task.Run(async () =>
{
    for (long i = 0; i < 100; i++)
    {
        long key = t * 100 + i;
        await sharedTree.InsertAsync(key, (ulong)key);
    }
}));

await Task.WhenAll(tasks);
Console.WriteLine($"✅ Final count: {await btree.CountAsync()}");
```

---

## Utility Methods

```csharp
long  count     = await btree.CountAsync();    // Total key-value pairs
bool  isEmpty   = await btree.IsEmptyAsync();  // True when count == 0
int   height    = await btree.HeightAsync();   // Tree depth (1 = root is a leaf)
long  metaPage  = btree.MetaPageId;           // Page ID needed for reopen
```

---

## Error Handling

All operations return a `Result` type. Never throws unless there is a programmer error.

| Error | Meaning |
|---|---|
| `IndexError.Storage` | Underlying storage engine error |
| `IndexError.Serialization` | Node serialization / deserialization failure |
| `IndexError.Corrupt` | Tree structure is inconsistent |
| `IndexError.InvalidFanout` | Fanout outside [4, 1000] |
| `IndexError.DuplicateKey` | `InsertAsync` called with an existing key |

---

## Performance Tips

1. **Buffer pool size** — aim for the working set to fit in the pool; dramatically reduces disk I/O
2. **Page size** — 8 KiB is a good default; 16 KiB helps with sequential-read-heavy workloads
3. **Fanout** — keep `Fanout × max_value_size` well below `PageSize − 16`
4. **Bulk load** — use `BulkLoadAsync` for initial population instead of inserting one at a time
5. **Async context** — all operations are `async`; run inside a `Task`-based async context

---

:::tip
See [BTree Design](../architecture/btree-design.md) for a deep dive into the on-disk node format, metadata page structure, and B+ tree split algorithms.
:::
