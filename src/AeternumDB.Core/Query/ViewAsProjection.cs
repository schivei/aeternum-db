namespace AeternumDB.Core.Query;

using AeternumDB.Core.Sql;

/// <summary>A single item in a ViewAs projection.</summary>
public sealed class ViewAsProjection(Expr expr, string alias)
{
    /// <summary>The expression for this view-as item.</summary>
    public Expr Expr { get; } = expr;

    /// <summary>The required output alias for this view-as item.</summary>
    public string Alias { get; } = alias;
}
