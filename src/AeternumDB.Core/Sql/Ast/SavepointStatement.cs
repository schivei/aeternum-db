namespace AeternumDB.Core.Sql;

public sealed class SavepointStatement(string name)
{
    public string Name { get; } = name;
}
