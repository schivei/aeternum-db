namespace AeternumDB.Core.Query;

using AeternumDB.Core.Sql;

/// <summary>A single aggregate function applied to a group.</summary>
public sealed class AggregateExpr(Expr func, string? alias)
{
    /// <summary>The aggregate function expression.</summary>
    public Expr Func { get; } = func;

    /// <summary>The optional output alias for this aggregate.</summary>
    public string? Alias { get; } = alias;
}
