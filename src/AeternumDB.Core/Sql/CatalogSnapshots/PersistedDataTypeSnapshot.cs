namespace AeternumDB.Core.Sql.CatalogSnapshots;

internal sealed class PersistedDataTypeSnapshot
{
    public string? Kind { get; set; }
    public string? Name { get; set; }
    public string? Table { get; set; }
    public string? Column { get; set; }
    public string? DataId { get; set; }
    public string? ElementTypeText { get; set; }
}
