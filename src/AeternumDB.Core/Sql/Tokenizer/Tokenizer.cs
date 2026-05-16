using System.Text;
using AeternumDB.Core.Sql;

namespace AeternumDB.Core.Sql.Tokenizer;

/// <summary>
/// Converts a raw SQL string into a flat list of <see cref="Token"/> values.
/// AOT-compatible — no reflection.
/// </summary>
internal sealed class Tokenizer
{
    #region Fields

    private readonly string _src;
    private int _pos;
    private int _line = 1;

    private static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT",
        "FROM",
        "WHERE",
        "GROUP",
        "BY",
        "HAVING",
        "ORDER",
        "LIMIT",
        "OFFSET",
        "DISTINCT",
        "AS",
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "CROSS",
        "ON",
        "FILTER",
        "AND",
        "OR",
        "NOT",
        "IN",
        "BETWEEN",
        "LIKE",
        "ILIKE",
        "SIMILAR",
        "TO",
        "IS",
        "NULL",
        "TRUE",
        "FALSE",
        "INSERT",
        "INTO",
        "VALUES",
        "UPDATE",
        "SET",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "TABLE",
        "INDEX",
        "VIEW",
        "DATABASE",
        "SCHEMA",
        "TYPE",
        "ENUM",
        "USER",
        "IF",
        "EXISTS",
        "TEMPORARY",
        "TEMP",
        "INHERITS",
        "SYSTEM",
        "VERSIONING",
        "FLAT",
        "WITH",
        "ADD",
        "COLUMN",
        "RENAME",
        "MATERIALIZED",
        "GRANT",
        "REVOKE",
        "PRIVILEGES",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "TRANSACTION",
        "START",
        "PRIMARY",
        "KEY",
        "UNIQUE",
        "CHECK",
        "DEFAULT",
        "AUTO_INCREMENT",
        "UNSIGNED",
        "BIGINT",
        "INT",
        "INTEGER",
        "SMALLINT",
        "TINYINT",
        "MEDIUMINT",
        "FLOAT",
        "DOUBLE",
        "PRECISION",
        "DECIMAL",
        "NUMERIC",
        "CHAR",
        "VARCHAR",
        "TEXT",
        "TINYTEXT",
        "MEDIUMTEXT",
        "LONGTEXT",
        "BLOB",
        "TINYBLOB",
        "MEDIUMBLOB",
        "LONGBLOB",
        "BINARY",
        "VARBINARY",
        "BOOLEAN",
        "BOOL",
        "DATE",
        "TIME",
        "DATETIME",
        "TIMESTAMP",
        "UUID",
        "CAST",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "REPLACE",
        "SUBSTRING",
        "SUBSTR",
        "POSITION",
        "TRIM",
        "LEADING",
        "TRAILING",
        "BOTH",
        "OVERLAY",
        "PLACING",
        "MATCH",
        "AGAINST",
        "REGEXP",
        "RLIKE",
        "REVLIKE",
        "REVILIKE",
        "REVREGEXP",
        "ALL",
        "ANY",
        "SOME",
        "EXCEPT",
        "UNION",
        "INTERSECT",
        "EXPLAIN",
        "ANALYZE",
        "PRESERVE",
        "ROWS",
        "ISOLATION",
        "LEVEL",
        "READ",
        "WRITE",
        "UNCOMMITTED",
        "COMMITTED",
        "REPEATABLE",
        "SERIALIZABLE",
        "CHAIN",
        "USE",
        "VERSIONED",
        "FLAG",
        "NONE",
        "EXPAND",
        "ARRAY",
        "VECTOR",
        "OBJECT",
        "JSON",
    };

    #endregion

    #region Constructor

    /// <summary>Initializes a new tokenizer over the given SQL source string.</summary>
    public Tokenizer(string src)
    {
        _src = src;
    }

    #endregion

    #region Public API

    /// <summary>
    /// Tokenizes the entire source and returns the token list,
    /// always terminated with a <see cref="TokenKind.Eof"/> sentinel.
    /// </summary>
    public List<Token> Tokenize()
    {
        var tokens = new List<Token>();
        while (_pos < _src.Length)
        {
            SkipWhitespaceAndComments();
            if (_pos >= _src.Length)
                break;
            var token = ReadNextToken(_line);
            if (token != null)
                tokens.Add(token);
        }
        tokens.Add(new Token(TokenKind.Eof, "", _line));
        return tokens;
    }

    #endregion

    #region Token dispatch

    private Token? ReadNextToken(int line)
    {
        var ch = _src[_pos];
        if (ch == '\'' || ch == '"')
            return ReadStringLiteral(line);
        if (ch == '`')
            return ReadBacktickIdent(line);
        if (IsNumberStart(ch))
            return ReadNumber(line);
        if (char.IsLetter(ch) || ch == '_')
            return ReadIdentOrKeyword(line);
        return ReadOperator(line);
    }

    private bool IsNumberStart(char ch) =>
        char.IsAsciiDigit(ch)
        || (ch == '.' && _pos + 1 < _src.Length && char.IsAsciiDigit(_src[_pos + 1]));

    #endregion

    #region Whitespace and comment skipping

    private void SkipWhitespaceAndComments()
    {
        while (_pos < _src.Length)
        {
            var ch = _src[_pos];
            if (TrySkipWhitespace(ch) || TrySkipLineComment(ch) || TrySkipBlockComment(ch))
                continue;
            break;
        }
    }

    private bool TrySkipWhitespace(char ch)
    {
        if (ch == '\n')
        {
            _line++;
            _pos++;
            return true;
        }

        if (!char.IsWhiteSpace(ch))
            return false;
        _pos++;
        return true;
    }

    private bool TrySkipLineComment(char ch)
    {
        if (ch != '-' || _pos + 1 >= _src.Length || _src[_pos + 1] != '-')
            return false;
        while (_pos < _src.Length && _src[_pos] != '\n')
            _pos++;
        return true;
    }

    private bool TrySkipBlockComment(char ch)
    {
        if (ch != '/' || _pos + 1 >= _src.Length || _src[_pos + 1] != '*')
            return false;
        _pos += 2;
        while (_pos + 1 < _src.Length && !(_src[_pos] == '*' && _src[_pos + 1] == '/'))
        {
            if (_src[_pos] == '\n')
                _line++;
            _pos++;
        }

        if (_pos + 1 >= _src.Length)
        {
            _pos = _src.Length;
            return true;
        }

        _pos += 2;
        return true;
    }

    #endregion

    #region String literals

    private Token ReadStringLiteral(int line)
    {
        var quote = _src[_pos++];
        var sb = new StringBuilder();
        while (_pos < _src.Length)
        {
            var ch = _src[_pos];
            if (TryReadEscapedQuote(sb, quote))
                continue;
            if (TryReadEscapedCharacter(sb))
                continue;
            AppendStringCharacter(sb, ch);
        }
        return new Token(TokenKind.StringLiteral, sb.ToString(), line);
    }

    private bool TryReadEscapedQuote(StringBuilder sb, char quote)
    {
        if (_pos >= _src.Length)
            return false;
        if (_src[_pos] != quote)
            return false;
        _pos++;
        if (_pos < _src.Length && _src[_pos] == quote)
        {
            sb.Append(quote);
            _pos++;
            return true;
        }

        return false;
    }

    private bool TryReadEscapedCharacter(StringBuilder sb)
    {
        if (_pos >= _src.Length)
            return false;
        if (_src[_pos] != '\\' || _pos + 1 >= _src.Length)
            return false;
        _pos++;
        sb.Append(
            _src[_pos] switch
            {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                var x => x,
            }
        );
        _pos++;
        return true;
    }

    private void AppendStringCharacter(StringBuilder sb, char ch)
    {
        if (ch == '\n')
            _line++;
        sb.Append(ch);
        _pos++;
    }

    private Token ReadBacktickIdent(int line)
    {
        _pos++;
        var start = _pos;
        while (_pos < _src.Length && _src[_pos] != '`')
            _pos++;
        var name = _src[start.._pos];
        if (_pos < _src.Length)
            _pos++;
        return new Token(TokenKind.Ident, name, line);
    }

    #endregion

    #region Number literals

    private Token ReadNumber(int line)
    {
        var start = _pos;
        bool isFloat = false;
        while (_pos < _src.Length && char.IsAsciiDigit(_src[_pos]))
            _pos++;
        ReadFraction(ref isFloat);
        ReadExponent(ref isFloat);
        return new Token(
            isFloat ? TokenKind.FloatLiteral : TokenKind.IntLiteral,
            _src[start.._pos],
            line
        );
    }

    private void ReadFraction(ref bool isFloat)
    {
        if (_pos >= _src.Length || _src[_pos] != '.')
            return;
        isFloat = true;
        _pos++;
        while (_pos < _src.Length && char.IsAsciiDigit(_src[_pos]))
            _pos++;
    }

    private void ReadExponent(ref bool isFloat)
    {
        if (_pos >= _src.Length || (_src[_pos] != 'e' && _src[_pos] != 'E'))
            return;
        isFloat = true;
        _pos++;
        if (_pos < _src.Length && (_src[_pos] == '+' || _src[_pos] == '-'))
            _pos++;
        while (_pos < _src.Length && char.IsAsciiDigit(_src[_pos]))
            _pos++;
    }

    #endregion

    #region Identifiers and keywords

    private Token ReadIdentOrKeyword(int line)
    {
        var start = _pos;
        while (_pos < _src.Length && (char.IsLetterOrDigit(_src[_pos]) || _src[_pos] == '_'))
            _pos++;
        var text = _src[start.._pos];
        var kind = Keywords.Contains(text) ? TokenKind.Keyword : TokenKind.Ident;
        return new Token(kind, text, line);
    }

    #endregion

    #region Operators

    private Token? ReadOperator(int line)
    {
        var ch = _src[_pos++];
        return ch switch
        {
            ',' => new Token(TokenKind.Comma, ",", line),
            ';' => new Token(TokenKind.Semicolon, ";", line),
            '(' => new Token(TokenKind.LeftParen, "(", line),
            ')' => new Token(TokenKind.RightParen, ")", line),
            '[' => new Token(TokenKind.LeftBracket, "[", line),
            ']' => new Token(TokenKind.RightBracket, "]", line),
            '.' => new Token(TokenKind.Dot, ".", line),
            '*' => new Token(TokenKind.Star, "*", line),
            '+' => new Token(TokenKind.Plus, "+", line),
            '-' => new Token(TokenKind.Minus, "-", line),
            '/' => new Token(TokenKind.Slash, "/", line),
            '%' => new Token(TokenKind.Percent, "%", line),
            '~' => new Token(TokenKind.Tilde, "~", line),
            '^' => new Token(TokenKind.Caret, "^", line),
            '=' => new Token(TokenKind.Eq, "=", line),
            '!'
                => _pos < _src.Length && _src[_pos] == '='
                    ? AdvanceAndReturn(new Token(TokenKind.NotEq, "!=", line))
                    : throw new SqlException(
                        SqlErrorKind.ParseError,
                        $"Unexpected character '!' at line {line}; did you mean '!='?"
                    ),
            '<' => ReadLtToken(line),
            '>' => ReadGtToken(line),
            '&' => new Token(TokenKind.Ampersand, "&", line),
            '|'
                => _pos < _src.Length && _src[_pos] == '|'
                    ? AdvanceAndReturn(new Token(TokenKind.PipePipe, "||", line))
                    : new Token(TokenKind.Pipe, "|", line),
            _ => null,
        };
    }

    private Token AdvanceAndReturn(Token t)
    {
        _pos++;
        return t;
    }

    private Token ReadLtToken(int line)
    {
        if (_pos < _src.Length && _src[_pos] == '=')
            return AdvanceAndReturn(new Token(TokenKind.LtEq, "<=", line));
        if (_pos < _src.Length && _src[_pos] == '>')
            return AdvanceAndReturn(new Token(TokenKind.NotEq, "<>", line));
        if (_pos < _src.Length && _src[_pos] == '<')
            return AdvanceAndReturn(new Token(TokenKind.ShiftLeft, "<<", line));
        return new Token(TokenKind.Lt, "<", line);
    }

    private Token ReadGtToken(int line)
    {
        if (_pos < _src.Length && _src[_pos] == '=')
            return AdvanceAndReturn(new Token(TokenKind.GtEq, ">=", line));
        if (_pos < _src.Length && _src[_pos] == '>')
            return AdvanceAndReturn(new Token(TokenKind.ShiftRight, ">>", line));
        return new Token(TokenKind.Gt, ">", line);
    }

    #endregion
}
