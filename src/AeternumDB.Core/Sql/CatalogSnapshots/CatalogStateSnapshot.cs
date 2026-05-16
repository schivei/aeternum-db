namespace AeternumDB.Core.Sql.CatalogSnapshots;

internal sealed class CatalogStateSnapshot
{
    public List<TableSnapshot> Tables { get; set; } = [];
    public List<TypeSnapshot> Types { get; set; } = [];
    public List<IndexSnapshot> Indexes { get; set; } = [];
    public long NextObjectId { get; set; } = 1;
}
