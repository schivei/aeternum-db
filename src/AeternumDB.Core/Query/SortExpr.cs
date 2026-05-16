namespace AeternumDB.Core.Query;

using AeternumDB.Core.Sql;

/// <summary>One term in an ORDER BY clause.</summary>
public sealed class SortExpr(Expr expr, bool ascending)
{
    /// <summary>The expression to sort by.</summary>
    public Expr Expr { get; } = expr;

    /// <summary>Whether to sort in ascending order.</summary>
    public bool Ascending { get; } = ascending;
}
