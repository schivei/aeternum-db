namespace AeternumDB.Core.Sql.CatalogSnapshots;

internal sealed class TableSnapshot
{
    public string Name { get; set; } = "";
    public int SchemaVersion { get; set; } = 1;
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset ModifiedAt { get; set; } = DateTimeOffset.UtcNow;
    public long RowCount { get; set; }
    public List<ColumnSnapshot> Columns { get; set; } = [];
}
