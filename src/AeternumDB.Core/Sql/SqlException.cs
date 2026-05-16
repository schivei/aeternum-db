namespace AeternumDB.Core.Sql;

/// <summary>Represents an exception thrown by the SQL parser.</summary>
public sealed class SqlException : Exception
{
    /// <summary>Gets the category of SQL parser error.</summary>
    public SqlErrorKind Kind { get; }

    /// <summary>Gets the line number where the error occurred, when available.</summary>
    public int? Line { get; }

    /// <summary>Gets the column number where the error occurred, when available.</summary>
    public int? Col { get; }

    /// <summary>Initializes a new instance of the <see cref="SqlException"/> class.</summary>
    /// <param name="kind">The category of SQL parser error.</param>
    /// <param name="message">The exception message.</param>
    /// <param name="line">The line number where the error occurred, when available.</param>
    /// <param name="col">The column number where the error occurred, when available.</param>
    public SqlException(SqlErrorKind kind, string message, int? line = null, int? col = null)
        : base(message)
    {
        Kind = kind;
        Line = line;
        Col = col;
    }
}
