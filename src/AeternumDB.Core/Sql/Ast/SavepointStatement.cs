namespace AeternumDB.Core.Sql.Ast;

public sealed class SavepointStatement(string name)
{
    public string Name { get; } = name;
}
