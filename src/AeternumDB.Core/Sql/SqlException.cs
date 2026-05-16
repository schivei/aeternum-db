namespace AeternumDB.Core.Sql;

/// <summary>Errors returned by SqlParser.</summary>
public sealed class SqlException : Exception
{
    public SqlErrorKind Kind { get; }
    public int? Line { get; }
    public int? Col { get; }

    public SqlException(SqlErrorKind kind, string message, int? line = null, int? col = null)
        : base(message)
    {
        Kind = kind;
        Line = line;
        Col = col;
    }
}
