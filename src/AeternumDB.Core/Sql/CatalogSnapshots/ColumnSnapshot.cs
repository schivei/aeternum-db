namespace AeternumDB.Core.Sql.CatalogSnapshots;

internal sealed class ColumnSnapshot
{
    public string Name { get; set; } = "";
    public string DataTypeText { get; set; } = "UNKNOWN";
    public bool Nullable { get; set; } = true;
    public string? UserDefinedTypeName { get; set; }
}
