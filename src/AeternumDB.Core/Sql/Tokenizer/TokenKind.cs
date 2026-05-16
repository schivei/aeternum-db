namespace AeternumDB.Core.Sql.Tokenizer;

/// <summary>Classifies every token produced by the SQL tokenizer.</summary>
internal enum TokenKind
{
    #region Literals

    IntLiteral,
    FloatLiteral,
    StringLiteral,

    #endregion

    #region Identifiers and keywords

    Ident,
    Keyword,

    #endregion

    #region Punctuation

    Comma,
    Semicolon,
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Dot,
    Star,
    Eq,

    #endregion

    #region Comparison operators

    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,

    #endregion

    #region Arithmetic

    Plus,
    Minus,
    Slash,
    Percent,
    Caret,

    #endregion

    #region Bitwise

    Ampersand,
    Pipe,
    Tilde,
    ShiftLeft,
    ShiftRight,

    #endregion

    #region String concatenation

    PipePipe,

    #endregion

    #region End of input

    Eof,

    #endregion
}
