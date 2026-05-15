---
sidebar_position: 1
---

# 🔷 Core Types

Key types, enums, and interfaces shared across all AeternumDB modules.

---

## Value

`Value` represents any scalar value that can be stored or returned by the executor.

```csharp
public abstract record Value
{
    public sealed record Null      : Value;
    public sealed record Boolean   (bool Data)    : Value;
    public sealed record Integer   (long Data)    : Value;
    public sealed record Float     (double Data)  : Value;
    public sealed record Text      (string Data)  : Value;
    public sealed record Bytes     (byte[] Data)  : Value;
    public sealed record Array     (IReadOnlyList<Value> Items) : Value;
}
```

### Usage

```csharp
Value v = new Value.Integer(42);

string display = v switch
{
    Value.Null        => "NULL",
    Value.Boolean  b  => b.Data.ToString(),
    Value.Integer  i  => i.Data.ToString(),
    Value.Float    f  => f.Data.ToString("G"),
    Value.Text     t  => $"'{t.Data}'",
    Value.Bytes    b  => $"0x{Convert.ToHexString(b.Data)}",
    Value.Array    a  => $"[{string.Join(", ", a.Items)}]",
    _                 => throw new UnreachableException(),
};
```

---

## PageId

`PageId` is a `ulong` alias that uniquely identifies a page within a database file.

```csharp
using PageId = ulong;

PageId first  = 1UL;
PageId second = 2UL;
```

---

## StorageConfig

```csharp
public sealed record StorageConfig
{
    /// <summary>Path to the database file.</summary>
    public required string DataPath       { get; init; }

    /// <summary>Number of pages kept in the in-memory LRU buffer pool.</summary>
    public int BufferPoolSize { get; init; } = 1_000;

    /// <summary>Bytes per page. Must be a power of 2 and greater than 16.</summary>
    public int PageSize       { get; init; } = 8_192;
}
```

---

## BTreeConfig

```csharp
public sealed record BTreeConfig
{
    /// <summary>Maximum number of keys per BTree node. Range: 4–1000.</summary>
    public int Fanout { get; init; } = 100;
}
```

---

## PlannerContext

```csharp
public sealed class PlannerContext
{
    public Catalog            Catalog    { get; }
    public StatisticsRegistry Statistics { get; init; } = new();
    public CostModel          CostModel  { get; init; } = CostModel.Default;

    public PlannerContext(Catalog catalog) => Catalog = catalog;
}
```

---

## CostModel

```csharp
public sealed record CostModel
{
    public static readonly CostModel Default = new();

    public double IoCostFactor      { get; init; } = 1.0;
    public double CpuCostFactor     { get; init; } = 0.01;
    public double NetworkCostFactor { get; init; } = 10.0;
}
```

---

## Catalog and Schema Types

```csharp
public sealed class Catalog
{
    public void AddTable(TableSchema schema) { … }
    public TableSchema? GetTable(string name) { … }
    public IReadOnlyList<TableSchema> Tables { get; }
}

public sealed record TableSchema
{
    public required string              Name    { get; init; }
    public required IList<ColumnSchema> Columns { get; init; }
    public bool                         IsFlat  { get; init; }
}

public sealed record ColumnSchema
{
    public required string   Name     { get; init; }
    public required DataType DataType { get; init; }
    public bool              Nullable { get; init; } = true;
}
```

---

## Error Types

AeternumDB uses a discriminated-union error model — no unexpected exceptions in production paths.

| Error Type | Namespace | Key Variants |
|---|---|---|
| `StorageError` | `AeternumDB.Core.Storage` | `BufferPool`, `FileManager`, `OutOfBounds`, `ChecksumMismatch`, `PagePinned` |
| `IndexError` | `AeternumDB.Core.Index` | `Storage`, `Serialization`, `Corrupt`, `InvalidFanout`, `DuplicateKey` |
| `PlannerError` | `AeternumDB.Core.Query` | `UnknownTable`, `UnknownColumn`, `CrossDatabaseJoin`, `FlatTableJoin`, `InvalidExpression` |
| `SqlError` | `AeternumDB.Core.Sql` | `LexerError`, `ParseError`, `ValidationError` |
| `ExecutorError` | `AeternumDB.Core.Executor` | `TypeMismatch`, `DivisionByZero`, `ColumnNotFound`, `TableNotFound` |

---

## Result Pattern

```csharp
// All public APIs return Result<T> — never throw
var result = await storage.ReadPageDataAsync(pageId, 0, 100);

if (result.IsSuccess)
    ProcessData(result.Value);
else
    logger.LogError("Storage error: {Error}", result.Error);
```
