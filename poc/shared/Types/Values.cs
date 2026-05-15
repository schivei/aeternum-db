using System.Runtime.InteropServices;
using System.Text.Json.Serialization;

namespace AeternumDB.PoC.Shared.Types;

/// <summary>
/// A SQL column value.
/// Mirrors Rust: enum Value in record_batch.rs
/// </summary>
[JsonDerivedType(typeof(DbValue.Null), "null")]
[JsonDerivedType(typeof(DbValue.Boolean), "boolean")]
[JsonDerivedType(typeof(DbValue.Integer), "integer")]
[JsonDerivedType(typeof(DbValue.Float), "float")]
[JsonDerivedType(typeof(DbValue.Text), "text")]
[JsonDerivedType(typeof(DbValue.Bytes), "bytes")]
[JsonDerivedType(typeof(DbValue.Array), "array")]
public abstract class DbValue : IEquatable<DbValue>
{
    private DbValue() { }

    public sealed class Null : DbValue
    {
        public static readonly Null Instance = new();
        private Null() { }
        public override string ToString() => "NULL";
        public override bool Equals(DbValue? other) => other is Null;
        public override int GetHashCode() => 0;
    }

    public sealed class Boolean(bool value) : DbValue
    {
        public bool Value { get; } = value;
        public override string ToString() => Value ? "TRUE" : "FALSE";
        public override bool Equals(DbValue? other) => other is Boolean b && b.Value == Value;
        public override int GetHashCode() => Value.GetHashCode();
    }

    public sealed class Integer(long value) : DbValue
    {
        public long Value { get; } = value;
        public override string ToString() => Value.ToString();
        public override bool Equals(DbValue? other) => other is Integer i && i.Value == Value;
        public override int GetHashCode() => Value.GetHashCode();
    }

    public sealed class Float(double value) : DbValue
    {
        public double Value { get; } = value;
        public override string ToString() => Value.ToString("G17");
        public override bool Equals(DbValue? other) => other is Float f && f.Value == Value;
        public override int GetHashCode() => Value.GetHashCode();
    }

    public sealed class Text(string value) : DbValue
    {
        public string Value { get; } = value;
        public override string ToString() => Value;
        public override bool Equals(DbValue? other) => other is Text t && t.Value == Value;
        public override int GetHashCode() => Value.GetHashCode(StringComparison.Ordinal);
    }

    public sealed class Bytes(byte[] value) : DbValue
    {
        public byte[] Value { get; } = value;
        public override string ToString() => $"<{Value.Length} bytes>";
        public override bool Equals(DbValue? other) =>
            other is Bytes b && b.Value.AsSpan().SequenceEqual(Value);
        public override int GetHashCode() => Value.Length;
    }

    public sealed class Array(DbValue[] items) : DbValue
    {
        public DbValue[] Items { get; } = items;
        public override string ToString() => $"[{string.Join(", ", (IEnumerable<DbValue>)Items)}]";
        public override bool Equals(DbValue? other) =>
            other is Array a && a.Items.SequenceEqual(Items);
        public override int GetHashCode() => Items.Length;
    }

    public bool IsNull => this is Null;
    public abstract bool Equals(DbValue? other);
    public override bool Equals(object? obj) => obj is DbValue v && Equals(v);
    public abstract override int GetHashCode();
}

/// <summary>
/// A database row: named column → value mapping.
/// Mirrors Rust: struct Row
/// </summary>
public sealed class DbRow
{
    private readonly Dictionary<string, DbValue> _columns;

    public DbRow(Dictionary<string, DbValue> columns)
    {
        _columns = columns;
    }

    public DbRow() : this(new Dictionary<string, DbValue>(StringComparer.OrdinalIgnoreCase)) { }

    public DbValue Get(string column) =>
        _columns.TryGetValue(column, out var v) ? v : DbValue.Null.Instance;

    public void Set(string column, DbValue value) => _columns[column] = value;
    public IEnumerable<KeyValuePair<string, DbValue>> Columns => _columns;
    public int ColumnCount => _columns.Count;

    public static DbRow FromPairs(IEnumerable<(string Name, DbValue Value)> pairs)
    {
        var row = new DbRow();
        foreach (var (name, value) in pairs)
            row.Set(name, value);
        return row;
    }
}

/// <summary>
/// A batch of rows returned by an execution plan operator.
/// Mirrors Rust: struct RecordBatch
/// </summary>
public sealed class RecordBatch
{
    public IReadOnlyList<DbRow> Rows { get; }
    public IReadOnlyList<ColumnMeta> Schema { get; }

    public RecordBatch(IReadOnlyList<DbRow> rows, IReadOnlyList<ColumnMeta> schema)
    {
        Rows = rows;
        Schema = schema;
    }

    public int RowCount => Rows.Count;

    public static RecordBatch Empty(IReadOnlyList<ColumnMeta> schema) =>
        new([], schema);
}

/// <summary>
/// Column metadata.
/// Mirrors Rust: struct ColumnMeta
/// </summary>
public sealed class ColumnMeta
{
    public string Name { get; }
    public string TypeName { get; }
    public int? InnerCount { get; set; }

    public ColumnMeta(string name, string typeName, int? innerCount = null)
    {
        Name = name;
        TypeName = typeName;
        InnerCount = innerCount;
    }

    public bool IsArray =>
        TypeName.Contains("array", StringComparison.OrdinalIgnoreCase)
        || TypeName.Contains("vector", StringComparison.OrdinalIgnoreCase);
}
