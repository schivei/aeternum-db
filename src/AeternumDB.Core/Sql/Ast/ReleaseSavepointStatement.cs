namespace AeternumDB.Core.Sql;

public sealed class ReleaseSavepointStatement(string name)
{
    public string Name { get; } = name;
}
