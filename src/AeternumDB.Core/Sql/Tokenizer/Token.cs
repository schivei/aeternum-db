namespace AeternumDB.Core.Sql.Tokenizer;

/// <summary>A single SQL token produced by <see cref="Tokenizer"/>.</summary>
internal sealed class Token
{
    #region Properties

    /// <summary>Gets the kind of this token.</summary>
    public TokenKind Kind { get; }

    /// <summary>Gets the raw text matched from the SQL source.</summary>
    public string Text { get; }

    /// <summary>Gets the one-based source line on which this token appears.</summary>
    public int Line { get; }

    #endregion

    #region Constructor

    /// <summary>Initializes a new <see cref="Token"/>.</summary>
    public Token(TokenKind kind, string text, int line)
    {
        Kind = kind;
        Text = text;
        Line = line;
    }

    #endregion

    /// <inheritdoc/>
    public override string ToString() => $"[{Kind} '{Text}' L{Line}]";
}
