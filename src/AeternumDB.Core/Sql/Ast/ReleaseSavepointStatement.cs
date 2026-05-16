namespace AeternumDB.Core.Sql.Ast;

public sealed class ReleaseSavepointStatement(string name)
{
    public string Name { get; } = name;
}
