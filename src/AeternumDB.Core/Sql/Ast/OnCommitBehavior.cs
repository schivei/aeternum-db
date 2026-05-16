namespace AeternumDB.Core.Sql.Ast;

/// <summary>Behavior of a temporary table when a transaction is committed.</summary>
public enum OnCommitBehavior
{
    PreserveRows,
    DeleteRows,
    Drop,
}
