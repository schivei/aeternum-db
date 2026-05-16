namespace AeternumDB.Core.Sql.CatalogSnapshots;

internal sealed class TypeSnapshot
{
    public string Name { get; set; } = "";
    public string Kind { get; set; } = "composite";
    public bool Flag { get; set; }
    public List<EnumVariantSnapshot> Variants { get; set; } = [];
    public List<ulong> ResolvedValues { get; set; } = [];
    public List<TypeFieldSnapshot> Fields { get; set; } = [];
}
