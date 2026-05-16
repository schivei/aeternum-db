namespace AeternumDB.Core.Sql;

/// <summary>Metadata for a registered index.</summary>
public sealed class IndexSchema
{
    public string Name { get; }
    public string Table { get; }
    public IReadOnlyList<string> Columns { get; }
    public bool Unique { get; }
    public IndexType IndexType { get; }
    public DateTimeOffset CreatedAt { get; }

    public IndexSchema(
        string name,
        string table,
        IReadOnlyList<string> columns,
        bool unique,
        IndexType indexType,
        DateTimeOffset? createdAt = null)
    {
        Name = name;
        Table = table;
        Columns = columns;
        Unique = unique;
        IndexType = indexType;
        CreatedAt = createdAt ?? DateTimeOffset.UtcNow;
    }
}
