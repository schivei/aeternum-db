namespace AeternumDB.Core.Sql.Ast;

public sealed class RollbackStatement(RollbackScope scope, bool chain)
{
    public RollbackScope Scope { get; } = scope;
    public bool Chain { get; } = chain;
}
