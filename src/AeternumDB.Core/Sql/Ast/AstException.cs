namespace AeternumDB.Core.Sql.Ast;

/// <summary>Errors that can occur while parsing or lowering an AST node.</summary>
public sealed class AstException : Exception
{
    /// <summary>The specific error kind.</summary>
    public AstErrorKind Kind { get; }

    /// <summary>Initializes a new instance of <see cref="AstException"/>.</summary>
    public AstException(AstErrorKind kind, string message)
        : base(message)
    {
        Kind = kind;
    }
}
