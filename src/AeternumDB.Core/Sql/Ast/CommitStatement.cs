namespace AeternumDB.Core.Sql.Ast;

public sealed class CommitStatement(CommitScope scope, bool chain)
{
    public CommitScope Scope { get; } = scope;
    public bool Chain { get; } = chain;
}
