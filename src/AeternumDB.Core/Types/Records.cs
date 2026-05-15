namespace AeternumDB.Core.Types;

/// <summary>
/// Column metadata: name, type and optional inner element count for array columns.
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

/// <summary>
/// A database row: ordered named column → value mapping.
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
/// A batch of rows produced by an execution plan operator.
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

    public static RecordBatch Empty(IReadOnlyList<ColumnMeta> schema) => new([], schema);
}
