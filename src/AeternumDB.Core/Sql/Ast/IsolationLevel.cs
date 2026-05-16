namespace AeternumDB.Core.Sql.Ast;

/// <summary>Transaction isolation levels.</summary>
public enum IsolationLevel
{
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}
