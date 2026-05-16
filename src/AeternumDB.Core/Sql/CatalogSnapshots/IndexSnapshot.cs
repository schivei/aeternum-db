namespace AeternumDB.Core.Sql.CatalogSnapshots;

internal sealed class IndexSnapshot
{
    public string Name { get; set; } = "";
    public string Table { get; set; } = "";
    public List<string> Columns { get; set; } = [];
    public bool Unique { get; set; }
    public string IndexType { get; set; } = CatalogSnapshotConstants.DefaultIndexTypeName;
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
}
