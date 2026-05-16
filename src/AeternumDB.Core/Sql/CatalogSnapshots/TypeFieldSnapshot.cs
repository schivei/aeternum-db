namespace AeternumDB.Core.Sql.CatalogSnapshots;

internal sealed class TypeFieldSnapshot
{
    public string Name { get; set; } = "";
    public string DataTypeText { get; set; } = "UNKNOWN";
    public string? UserDefinedTypeName { get; set; }
}
