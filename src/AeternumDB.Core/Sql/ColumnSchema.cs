using AeternumDB.Core.Sql.Ast;

namespace AeternumDB.Core.Sql;

/// <summary>Metadata for a single column in the catalog.</summary>
public sealed class ColumnSchema
{
    public string Name { get; }
    public DataType DataType { get; }
    public bool Nullable { get; }
    public string? UserDefinedTypeName { get; }

    public ColumnSchema(string name, DataType dataType, bool nullable = true)
    {
        Name = name;
        DataType = dataType;
        Nullable = nullable;
        UserDefinedTypeName = dataType is DataType.EnumRef enumRef ? enumRef.Name : null;
    }
}
