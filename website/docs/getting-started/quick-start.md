---
sidebar_position: 2
---

# ⚡ Quick Start

Get a fully working storage + index pipeline running in minutes.

---

## Goal

By the end of this guide you will have:

- ✅ Opened a database file with the `StorageEngine`
- ✅ Created a disk-backed **BTree index**
- ✅ Inserted, searched, and range-scanned records
- ✅ Planned and explained a SQL query

---

## Step 1 — Open the Storage Engine

```csharp
using AeternumDB.Core.Storage;

// Opens an existing file or creates a new one.
await using var storage = await StorageEngine.OpenAsync("quickstart.adb");

Console.WriteLine("✅ Storage engine ready");
```

The `StorageEngine` manages a **page-based file** with an LRU buffer pool.
Every subsequent operation reads/writes through this engine.

---

## Step 2 — Create a BTree Index

```csharp
using AeternumDB.Core.Index;

// Create a new BTree (returns the metadata page ID — save it!)
var btree = await BTree.CreateAsync(storage);
long metaPageId = btree.MetaPageId;

Console.WriteLine($"📄 BTree metadata page: {metaPageId}");
```

:::tip Persisting the index
Save `metaPageId` to reopen the index on the next run:
```csharp
var btree = await BTree.OpenAsync(storage, metaPageId);
```
:::

---

## Step 3 — Insert Data

```csharp
// Insert key → value pairs (key: long, value: ulong page reference)
await btree.InsertAsync(1L,  10UL);
await btree.InsertAsync(2L,  20UL);
await btree.InsertAsync(5L,  50UL);
await btree.InsertAsync(10L, 100UL);
await btree.InsertAsync(42L, 420UL);

Console.WriteLine($"📝 Inserted 5 records. Total: {await btree.CountAsync()}");
```

---

## Step 4 — Search for a Key

```csharp
var result = await btree.SearchAsync(42L);

if (result.HasValue)
    Console.WriteLine($"🔍 Key 42 → Page {result.Value}");  // Key 42 → Page 420
else
    Console.WriteLine("❌ Key not found");
```

---

## Step 5 — Range Scan

```csharp
Console.WriteLine("📊 Keys between 2 and 10:");

await foreach (var (key, value) in btree.RangeScanAsync(2L, 10L))
    Console.WriteLine($"  Key={key,-5} → Page={value}");

// Output:
//   Key=2     → Page=20
//   Key=5     → Page=50
//   Key=10    → Page=100
```

---

## Step 6 — Plan a SQL Query

```csharp
using AeternumDB.Core.Query;
using AeternumDB.Core.Sql;

var parser  = new SqlParser();
var planner = new QueryPlanner();

var stmt     = parser.ParseOne("SELECT id FROM users WHERE age > 18");
var context  = new PlannerContext(catalog);     // inject your catalog
var physical = planner.Plan(stmt, context);

// Print the query plan with cost annotations
Console.WriteLine(planner.Explain(physical));
```

Example output:

```
Physical Plan:
└─ Filter [predicate: age]
   Est. rows: 100 | Cost: 1000.20 (I/O: 0.00, CPU: 0.20)
   └─ SeqScan [table: users]
      Est. rows: 1000 | Cost: 10.00 (I/O: 10.00, CPU: 0.00)

Total Cost: 1010.20
Estimated Rows: 100
```

---

## Full Example

```csharp
using AeternumDB.Core.Storage;
using AeternumDB.Core.Index;

await using var storage = await StorageEngine.OpenAsync("demo.adb");
var btree = await BTree.CreateAsync(storage);

// Populate
for (long i = 0; i < 100; i++)
    await btree.InsertAsync(i, (ulong)(i * 10));

// Point lookup
var v = await btree.SearchAsync(77L);
Console.WriteLine($"Search(77) = {v}");      // 770

// Range scan
var count = 0L;
await foreach (var _ in btree.RangeScanAsync(10L, 20L))
    count++;
Console.WriteLine($"Range [10,20] = {count} records");  // 11

// Stats
Console.WriteLine($"Height: {await btree.HeightAsync()}");
Console.WriteLine($"Count:  {await btree.CountAsync()}");
```

---

## Next Steps

| Guide | Description |
|---|---|
| [Configuration](./configuration.md) | Tune page size, buffer pool, fanout |
| [SQL Reference](../guides/sql-reference.md) | Full SQL dialect |
| [Query Optimization](../guides/query-optimization.md) | How the optimizer works |
| [BTree Usage](../guides/btree-usage.md) | Advanced index operations |
| [Storage Architecture](../architecture/storage.md) | Deep dive into the storage layer |
