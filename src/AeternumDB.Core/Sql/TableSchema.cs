namespace AeternumDB.Core.Sql;

/// <summary>Metadata for a single table in the catalog.</summary>
public sealed class TableSchema
{
    public string Name { get; }
    public IReadOnlyList<ColumnSchema> Columns { get; }
    public int SchemaVersion { get; }
    public DateTimeOffset CreatedAt { get; }
    public DateTimeOffset ModifiedAt { get; }
    public long RowCount { get; }

    public TableSchema(
        string name,
        IReadOnlyList<ColumnSchema> columns,
        int schemaVersion = 1,
        DateTimeOffset? createdAt = null,
        DateTimeOffset? modifiedAt = null,
        long rowCount = 0)
    {
        var now = DateTimeOffset.UtcNow;
        Name = name;
        Columns = columns;
        SchemaVersion = schemaVersion;
        CreatedAt = createdAt ?? now;
        ModifiedAt = modifiedAt ?? CreatedAt;
        RowCount = rowCount;
    }

    /// <summary>Look up a column by name (case-insensitive).</summary>
    public ColumnSchema? GetColumn(string name) =>
        Columns.FirstOrDefault(col => string.Equals(col.Name, name, StringComparison.OrdinalIgnoreCase));
}
