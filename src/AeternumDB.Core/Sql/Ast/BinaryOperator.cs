namespace AeternumDB.Core.Sql.Ast;

/// <summary>Binary operators supported in SQL expressions.</summary>
public enum BinaryOperator
{
    #region Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    #endregion

    #region Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    #endregion

    #region Logical
    And,
    Or,
    #endregion

    #region Pattern matching
    Like,
    NotLike,
    ILike,
    NotILike,
    SimilarTo,
    NotSimilarTo,
    #endregion

    #region Regular expression
    Regexp,
    NotRegexp,
    RegexpMatch,
    RegexpIMatch,
    NotRegexpMatch,
    NotRegexpIMatch,
    #endregion

    #region Bitwise
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    ShiftLeft,
    ShiftRight,
    #endregion

    #region String concatenation
    StringConcat,
    #endregion

    #region Reverse pattern matching (AeternumDB extension)
    RevLike,
    NotRevLike,
    RevILike,
    NotRevILike,
    RevRegexp,
    NotRevRegexp,
    RevRegexpIMatch,
    NotRevRegexpIMatch,
    #endregion
}
