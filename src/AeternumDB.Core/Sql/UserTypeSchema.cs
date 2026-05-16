namespace AeternumDB.Core.Sql;

/// <summary>A named user-defined type stored in the catalog.</summary>
public sealed class UserTypeSchema
{
    public string Name { get; }
    public UserTypeKind Kind { get; }

    public UserTypeSchema(string name, UserTypeKind kind)
    {
        Name = name;
        Kind = kind;
    }
}
