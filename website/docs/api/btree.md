---
sidebar_position: 3
---

# 🌲 BTree API

Full API reference for `AeternumDB.Core.Index.BTree`.

---

## Creating and Opening

```csharp
// Create a new BTree (saves metadata to a new page)
var btree = await BTree.CreateAsync(storage);
var btree = await BTree.CreateAsync(storage, new BTreeConfig { Fanout = 50 });

// Reopen an existing BTree using the saved metadata page ID
var btree = await BTree.OpenAsync(storage, metaPageId: 1L);
```

---

## Properties

```csharp
long  MetaPageId { get; }           // Metadata page ID — save this for reopens!
```

---

## Insert

```csharp
// Insert a key-value pair. Throws IndexError.DuplicateKey if key exists.
await btree.InsertAsync(key: 42L, value: 420UL);
```

| Parameter | Type | Description |
|---|---|---|
| `key` | `TKey` | Must implement `IBTreeKey` |
| `value` | `TValue` | Must implement `IBTreeValue` |

---

## Upsert (Insert or Update)

```csharp
// Insert if new; replace value if key exists.
await btree.UpsertAsync(key: 42L, value: 999UL);
```

---

## Search

```csharp
TValue? result = await btree.SearchAsync(key: 42L);

if (result is not null)
    Console.WriteLine($"Found: {result}");
```

---

## Delete

```csharp
bool removed = await btree.DeleteAsync(key: 42L);
// true  → key was present and removed
// false → key was not found (no-op)
```

---

## Range Scan

```csharp
// Inclusive range
await foreach (var (key, value) in btree.RangeScanAsync(from: 10L, to: 20L))
    Console.WriteLine($"{key} → {value}");

// Open upper bound (from key to end)
await foreach (var (key, value) in btree.RangeScanAsync(10L, null))
    Console.WriteLine(key);

// Full scan
await foreach (var (key, value) in btree.RangeScanAsync(null, null))
    Console.WriteLine(key);
```

---

## Bulk Load

```csharp
// Populate a large dataset efficiently
var entries = Enumerable.Range(0, 1_000_000)
    .Select(i => ((long)i, (ulong)(i * 10)));

await btree.BulkLoadAsync(entries);
```

:::tip
`BulkLoadAsync` is orders of magnitude faster than sequential `InsertAsync` for initial population. Input need not be sorted but ascending order gives best performance.
:::

---

## Utility Methods

```csharp
long count   = await btree.CountAsync();    // Total number of key-value pairs
bool isEmpty = await btree.IsEmptyAsync();  // Equivalent to count == 0
int  height  = await btree.HeightAsync();   // Tree height (1 = root is a leaf)
```

---

## Error Reference

| Error | Condition |
|---|---|
| `IndexError.DuplicateKey` | `InsertAsync` called with an existing key |
| `IndexError.Storage` | Underlying `StorageEngine` error |
| `IndexError.Serialization` | Node could not be serialized/deserialized |
| `IndexError.Corrupt` | Unexpected node structure detected |
| `IndexError.InvalidFanout` | `BTreeConfig.Fanout` outside `[4, 1000]` |

---

## Custom Key/Value Types

Implement `IBTreeKey` to use your own key type:

```csharp
public interface IBTreeKey<TSelf> : IComparable<TSelf>
{
    byte[]      ToBytes();
    static abstract TSelf FromBytes(ReadOnlySpan<byte> bytes);
}
```

:::caution
`ToBytes()` must produce bytes where **lexicographic order = natural order**. Use big-endian encoding for all numeric types.
:::

Implement `IBTreeValue` for custom value types:

```csharp
public interface IBTreeValue<TSelf>
{
    byte[]      ToBytes();
    static abstract TSelf FromBytes(ReadOnlySpan<byte> bytes);
}
```
