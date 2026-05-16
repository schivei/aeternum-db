namespace AeternumDB.Core.Sql.Ast;

public sealed class BeginTransactionStatement
{
    public string? Name { get; init; }
    public IsolationLevel? IsolationLevel { get; init; }
    public bool ReadOnly { get; init; }
}
