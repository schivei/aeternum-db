namespace AeternumDB.Core.Query;

using AeternumDB.Core.Sql;

/// <summary>An expression paired with an output alias, used in projections.</summary>
public sealed class ProjectionItem(Expr expr, string? alias)
{
    /// <summary>The projected expression.</summary>
    public Expr Expr { get; } = expr;

    /// <summary>The optional output alias for this projection.</summary>
    public string? Alias { get; } = alias;
}
