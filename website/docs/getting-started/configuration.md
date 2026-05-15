---
sidebar_position: 3
---

# ⚙️ Configuration

Tune AeternumDB for your workload — from laptops to production servers.

---

## StorageEngine Configuration

```csharp
var storage = await StorageEngine.OpenAsync(new StorageConfig
{
    DataPath       = "/var/lib/mydb/data.adb",
    BufferPoolSize = 1_000,   // pages kept in memory
    PageSize       = 8_192,   // bytes per page
});
```

### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `DataPath` | `string` | *(required)* | Path to the database file |
| `BufferPoolSize` | `int` | `1000` | Number of pages in the in-memory LRU buffer |
| `PageSize` | `int` | `8192` | Bytes per page — must be a power of 2 and > 16 |

### Choosing a Page Size

| Page Size | Buffer @ 1 000 pages | Best For |
|---|---|---|
| `4096` (4 KiB) | ~4 MB | Matches OS page size; low write-amplification |
| `8192` (8 KiB) | ~8 MB | **Good default** for most workloads |
| `16384` (16 KiB) | ~16 MB | Large sequential reads, fewer seeks |

:::caution
All pages in a database file must use the **same** `PageSize`. Opening an existing file with a different value raises `StorageError.PageSizeMismatch`.
:::

### Choosing a Buffer Pool Size

| Scenario | Recommended Size |
|---|---|
| Unit tests / development | 256 pages |
| Small production database | 4 000 pages (~32 MB at 8 KiB) |
| Production — working set fits in RAM | As large as available memory allows |

A larger pool means more cache hits and fewer disk reads. Monitor `StorageEngine.BufferHitRate` to tune this value.

---

## BTree Configuration

```csharp
var btree = await BTree.CreateAsync(storage, new BTreeConfig
{
    Fanout = 100,  // keys per internal/leaf node
});
```

### Options

| Option | Type | Default | Range | Description |
|---|---|---|---|---|
| `Fanout` | `int` | `100` | `4`–`1000` | Maximum number of keys per node |

### Choosing a Fanout

- **Higher fanout** → fewer tree levels → fewer I/O operations per lookup
- **Lower fanout** → smaller nodes → better fit for large values

:::tip Rule of thumb
Keep `Fanout × max_value_size_bytes` well below `PageSize − 16` (page header). The default of `100` with 8 KiB pages fits most string and numeric workloads.
:::

---

## Query Planner Configuration

```csharp
var context = new PlannerContext(catalog)
{
    Statistics = new StatisticsRegistry()
        .Register("users", new TableStats { RowCount = 1_000_000, PageCount = 12_500 })
        .Register("orders", new TableStats { RowCount = 5_000_000, PageCount = 62_500 }),

    CostModel = new CostModel
    {
        IoCostFactor      = 1.0,
        CpuCostFactor     = 0.01,
        NetworkCostFactor = 10.0,
    },
};
```

### Statistics

Providing accurate statistics dramatically improves plan quality. Without statistics, the planner uses conservative defaults:

| Statistic | Default Value |
|---|---|
| Row count | 1 000 |
| Page count | 10 |
| Column selectivity | 10 % |

### Cost Model Factors

| Factor | Default | Effect |
|---|---|---|
| `IoCostFactor` | `1.0` | Weight of page reads vs CPU work |
| `CpuCostFactor` | `0.01` | Weight of per-row processing |
| `NetworkCostFactor` | `10.0` | Reserved for future distributed plans |

---

## Next Steps

- [SQL Reference](../guides/sql-reference.md) — Learn the query language
- [Storage Architecture](../architecture/storage.md) — Understand the storage internals
- [BTree Design](../architecture/btree-design.md) — How the index is structured on disk
