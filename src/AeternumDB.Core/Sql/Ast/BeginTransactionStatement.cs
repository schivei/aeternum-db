using AeternumDB.Core.Sql.Ast;

namespace AeternumDB.Core.Sql;

public sealed class BeginTransactionStatement
{
    public string? Name { get; init; }
    public IsolationLevel? IsolationLevel { get; init; }
    public bool ReadOnly { get; init; }
}
