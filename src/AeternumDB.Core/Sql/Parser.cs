// SQL parser for AeternumDB.
//
// Transpiled from poc/rust/src/sql/parser.rs.
// Implements a manual tokenizer + recursive-descent parser for the SQL subset
// needed by AeternumDB. AOT-compatible — no reflection.

using System.Globalization;
using System.Text;

namespace AeternumDB.Core.Sql;

// ── Token types ───────────────────────────────────────────────────────────────

internal enum TokenKind
{
    // Literals
    IntLiteral, FloatLiteral, StringLiteral,
    // Identifiers & keywords
    Ident, Keyword,
    // Punctuation
    Comma, Semicolon, LeftParen, RightParen, LeftBracket, RightBracket, Dot, Star, Eq,
    // Comparison operators
    NotEq, Lt, LtEq, Gt, GtEq,
    // Arithmetic
    Plus, Minus, Slash, Percent, Caret,
    // Bitwise
    Ampersand, Pipe, Tilde, ShiftLeft, ShiftRight,
    // String concat
    PipePipe,
    // Other
    Eof,
}

internal sealed class Token
{
    public TokenKind Kind { get; }
    public string Text { get; }
    public int Line { get; }

    public Token(TokenKind kind, string text, int line) { Kind = kind; Text = text; Line = line; }
    public override string ToString() => $"[{Kind} '{Text}' L{Line}]";
}

// ── SqlError ──────────────────────────────────────────────────────────────────

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

public enum SqlErrorKind
{
    ParseError,
    AstError,
    EmptyInput,
}

// ── Tokenizer ─────────────────────────────────────────────────────────────────

internal sealed class Tokenizer
{
    private readonly string _src;
    private int _pos;
    private int _line = 1;

    private static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT","FROM","WHERE","GROUP","BY","HAVING","ORDER","LIMIT","OFFSET","DISTINCT",
        "AS","JOIN","INNER","LEFT","RIGHT","FULL","CROSS","ON","FILTER","AND","OR","NOT",
        "IN","BETWEEN","LIKE","ILIKE","SIMILAR","TO","IS","NULL","TRUE","FALSE",
        "INSERT","INTO","VALUES","UPDATE","SET","DELETE",
        "CREATE","DROP","ALTER","TABLE","INDEX","VIEW","DATABASE","SCHEMA","TYPE","ENUM","USER",
        "IF","EXISTS","TEMPORARY","TEMP","INHERITS","SYSTEM","VERSIONING","FLAT","WITH",
        "ADD","COLUMN","RENAME","MATERIALIZED",
        "GRANT","REVOKE","PRIVILEGES","ON","TO","FROM",
        "BEGIN","COMMIT","ROLLBACK","SAVEPOINT","RELEASE","TRANSACTION","START",
        "PRIMARY","KEY","UNIQUE","CHECK","DEFAULT","NOT","NULL","AUTO_INCREMENT",
        "UNSIGNED","BIGINT","INT","INTEGER","SMALLINT","TINYINT","MEDIUMINT",
        "FLOAT","DOUBLE","PRECISION","DECIMAL","NUMERIC","CHAR","VARCHAR","TEXT",
        "TINYTEXT","MEDIUMTEXT","LONGTEXT","BLOB","TINYBLOB","MEDIUMBLOB","LONGBLOB",
        "BINARY","VARBINARY","BOOLEAN","BOOL","DATE","TIME","DATETIME","TIMESTAMP","UUID",
        "CAST","CASE","WHEN","THEN","ELSE","END","REPLACE","INTO",
        "SUBSTRING","SUBSTR","POSITION","TRIM","LEADING","TRAILING","BOTH","OVERLAY","PLACING",
        "MATCH","AGAINST","REGEXP","RLIKE","REVLIKE","REVILIKE","REVREGEXP",
        "ALL","ANY","SOME","EXCEPT","UNION","INTERSECT","EXPLAIN","ANALYZE",
        "PRESERVE","ROWS","ON","COMMIT","ISOLATION","LEVEL","READ","WRITE",
        "UNCOMMITTED","COMMITTED","REPEATABLE","SERIALIZABLE","CHAIN",
        "USE","FLAT","VERSIONED","INHERITS","FLAG","NONE",
        "EXPAND","ARRAY","VECTOR","OBJECT","JSON","UUID","REVOKE",
        "DROP","IF","NOT","EXISTS",
    };

    public Tokenizer(string src) { _src = src; }

    public List<Token> Tokenize()
    {
        var tokens = new List<Token>();
        while (_pos < _src.Length)
        {
            SkipWhitespaceAndComments();
            if (_pos >= _src.Length) break;
            var token = ReadNextToken(_line);
            if (token != null) tokens.Add(token);
        }
        tokens.Add(new Token(TokenKind.Eof, "", _line));
        return tokens;
    }

    private Token? ReadNextToken(int line)
    {
        var ch = _src[_pos];
        if (ch == '\'' || ch == '"') return ReadStringLiteral(line);
        if (ch == '`') return ReadBacktickIdent(line);
        if (IsNumberStart(ch)) return ReadNumber(line);
        if (char.IsLetter(ch) || ch == '_') return ReadIdentOrKeyword(line);
        return ReadOperator(line);
    }

    private bool IsNumberStart(char ch) =>
        char.IsAsciiDigit(ch) || (ch == '.' && _pos + 1 < _src.Length && char.IsAsciiDigit(_src[_pos + 1]));

    private void SkipWhitespaceAndComments()
    {
        while (_pos < _src.Length)
        {
            var ch = _src[_pos];
            if (TrySkipWhitespace(ch) || TrySkipLineComment(ch) || TrySkipBlockComment(ch)) continue;
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

        if (!char.IsWhiteSpace(ch)) return false;
        _pos++;
        return true;
    }

    private bool TrySkipLineComment(char ch)
    {
        if (ch != '-' || _pos + 1 >= _src.Length || _src[_pos + 1] != '-') return false;
        while (_pos < _src.Length && _src[_pos] != '\n') _pos++;
        return true;
    }

    private bool TrySkipBlockComment(char ch)
    {
        if (ch != '/' || _pos + 1 >= _src.Length || _src[_pos + 1] != '*') return false;
        _pos += 2;
        while (_pos + 1 < _src.Length && !(_src[_pos] == '*' && _src[_pos + 1] == '/'))
        {
            if (_src[_pos] == '\n') _line++;
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

    private Token ReadStringLiteral(int line)
    {
        var quote = _src[_pos++];
        var sb = new StringBuilder();
        while (_pos < _src.Length)
        {
            var ch = _src[_pos];
            if (TryReadEscapedQuote(sb, quote)) continue;
            if (TryReadEscapedCharacter(sb)) continue;
            AppendStringCharacter(sb, ch);
        }
        return new Token(TokenKind.StringLiteral, sb.ToString(), line);
    }

    private bool TryReadEscapedQuote(StringBuilder sb, char quote)
    {
        if (_pos >= _src.Length) return false;
        if (_src[_pos] != quote) return false;
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
        if (_pos >= _src.Length) return false;
        if (_src[_pos] != '\\' || _pos + 1 >= _src.Length) return false;
        _pos++;
        sb.Append(_src[_pos] switch { 'n' => '\n', 't' => '\t', 'r' => '\r', var x => x });
        _pos++;
        return true;
    }

    private void AppendStringCharacter(StringBuilder sb, char ch)
    {
        if (ch == '\n') _line++;
        sb.Append(ch);
        _pos++;
    }

    private Token ReadBacktickIdent(int line)
    {
        _pos++; // skip `
        var start = _pos;
        while (_pos < _src.Length && _src[_pos] != '`') _pos++;
        var name = _src[start.._pos];
        if (_pos < _src.Length) _pos++; // skip closing `
        return new Token(TokenKind.Ident, name, line);
    }

    private Token ReadNumber(int line)
    {
        var start = _pos;
        bool isFloat = false;
        while (_pos < _src.Length && char.IsAsciiDigit(_src[_pos])) _pos++;
        ReadFraction(ref isFloat);
        ReadExponent(ref isFloat);
        return new Token(isFloat ? TokenKind.FloatLiteral : TokenKind.IntLiteral, _src[start.._pos], line);
    }

    private void ReadFraction(ref bool isFloat)
    {
        if (_pos >= _src.Length || _src[_pos] != '.') return;
        isFloat = true;
        _pos++;
        while (_pos < _src.Length && char.IsAsciiDigit(_src[_pos])) _pos++;
    }

    private void ReadExponent(ref bool isFloat)
    {
        if (_pos >= _src.Length || (_src[_pos] != 'e' && _src[_pos] != 'E')) return;
        isFloat = true;
        _pos++;
        if (_pos < _src.Length && (_src[_pos] == '+' || _src[_pos] == '-')) _pos++;
        while (_pos < _src.Length && char.IsAsciiDigit(_src[_pos])) _pos++;
    }

    private Token ReadIdentOrKeyword(int line)
    {
        var start = _pos;
        while (_pos < _src.Length && (char.IsLetterOrDigit(_src[_pos]) || _src[_pos] == '_')) _pos++;
        var text = _src[start.._pos];
        var kind = Keywords.Contains(text) ? TokenKind.Keyword : TokenKind.Ident;
        return new Token(kind, text, line);
    }

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
            '!' => _pos < _src.Length && _src[_pos] == '='
                  ? AdvanceAndReturn(new Token(TokenKind.NotEq, "!=", line))
                  : throw new SqlException(SqlErrorKind.ParseError, $"Unexpected character '!' at line {line}; did you mean '!='?"),
            '<' => ReadLtToken(line),
            '>' => ReadGtToken(line),
            '&' => new Token(TokenKind.Ampersand, "&", line),
            '|' => _pos < _src.Length && _src[_pos] == '|' ? AdvanceAndReturn(new Token(TokenKind.PipePipe, "||", line))
                  : new Token(TokenKind.Pipe, "|", line),
            _ => null, // skip unknown characters
        };
    }

    private Token AdvanceAndReturn(Token t) { _pos++; return t; }

    private Token ReadLtToken(int line)
    {
        if (_pos < _src.Length && _src[_pos] == '=') return AdvanceAndReturn(new Token(TokenKind.LtEq, "<=", line));
        if (_pos < _src.Length && _src[_pos] == '>') return AdvanceAndReturn(new Token(TokenKind.NotEq, "<>", line));
        if (_pos < _src.Length && _src[_pos] == '<') return AdvanceAndReturn(new Token(TokenKind.ShiftLeft, "<<", line));
        return new Token(TokenKind.Lt, "<", line);
    }

    private Token ReadGtToken(int line)
    {
        if (_pos < _src.Length && _src[_pos] == '=') return AdvanceAndReturn(new Token(TokenKind.GtEq, ">=", line));
        if (_pos < _src.Length && _src[_pos] == '>') return AdvanceAndReturn(new Token(TokenKind.ShiftRight, ">>", line));
        return new Token(TokenKind.Gt, ">", line);
    }
}

// ── Recursive-descent parser ──────────────────────────────────────────────────

internal sealed class Parser
{
    private readonly List<Token> _tokens;
    private int _pos;

    private const string KwSelect = "SELECT";
    private const string KwDelete = "DELETE";
    private const string KwSavepoint = "SAVEPOINT";
    private const string KwWhere = "WHERE";
    private const string KwTable = "TABLE";
    private const string KwUnique = "UNIQUE";
    private const string KwExists = "EXISTS";
    private const string KwPrimary = "PRIMARY";
    private const string KwCheck = "CHECK";
    private const string KwTransaction = "TRANSACTION";

    private sealed class ColumnOptions
    {
        public bool Nullable { get; set; } = true;
        public bool PrimaryKey { get; set; }
        public bool Unique { get; set; }
        public bool AutoIncrement { get; set; }
        public Expr? Default { get; set; }
        public Expr? Check { get; set; }
        public ReferentialAction? OnUpdate { get; set; }
        public ReferentialAction? OnDelete { get; set; }
        public ulong? MinLength { get; set; }
        public ulong? MaxLength { get; set; }
        public bool RequiresDistinctReferences { get; set; }
    }

    public Parser(List<Token> tokens) { _tokens = tokens; }

    private Token Current => _pos < _tokens.Count ? _tokens[_pos] : _tokens[^1];
    private Token Peek(int offset = 1) => _pos + offset < _tokens.Count ? _tokens[_pos + offset] : _tokens[^1];

    private Token Consume()
    {
        var t = Current;
        _pos++;
        return t;
    }

    private void Expect(TokenKind kind, string? expected = null)
    {
        if (Current.Kind != kind)
            throw ParseError($"expected {expected ?? kind.ToString()}, got '{Current.Text}'");
        Consume();
    }

    private Token ExpectKeyword(string kw)
    {
        if (!IsKeyword(kw))
            throw ParseError($"expected keyword '{kw}', got '{Current.Text}'");
        return Consume();
    }

    private bool IsKeyword(string kw) =>
        Current.Kind == TokenKind.Keyword && Current.Text.Equals(kw, StringComparison.OrdinalIgnoreCase);

    private bool IsKeywordAny(params string[] kws) =>
        Current.Kind == TokenKind.Keyword && kws.Any(k => Current.Text.Equals(k, StringComparison.OrdinalIgnoreCase));

    private SqlException ParseError(string msg) =>
        new(SqlErrorKind.ParseError, msg, Current.Line);

    // Consume if keyword matches, return whether consumed
    private bool TryConsumeKeyword(string kw)
    {
        if (IsKeyword(kw)) { Consume(); return true; }
        return false;
    }

    // ── Top-level entry ──────────────────────────────────────────────────────

    public List<Statement> ParseStatements()
    {
        var stmts = new List<Statement>();
        while (Current.Kind != TokenKind.Eof)
        {
            TryConsumeKeyword(";");
            if (Current.Kind == TokenKind.Semicolon) { Consume(); continue; }
            if (Current.Kind == TokenKind.Eof) break;
            stmts.Add(ParseStatement());
            if (Current.Kind == TokenKind.Semicolon) Consume();
        }
        return stmts;
    }

    private Statement ParseStatement()
    {
        if (Current.Kind == TokenKind.Keyword)
        {
            var kw = Current.Text.ToUpperInvariant();
            return kw switch
            {
                KwSelect or "WITH" => new Statement.Select(ParseSelect()),
                "INSERT" => ParseInsert(),
                "UPDATE" => ParseUpdate(),
                KwDelete => ParseDelete(),
                "CREATE" => ParseCreate(),
                "DROP" => ParseDrop(),
                "ALTER" => ParseAlterTable(),
                "GRANT" => ParseGrant(),
                "REVOKE" => ParseRevoke(),
                "BEGIN" or "START" => ParseBeginTransaction(),
                "COMMIT" => ParseCommit(),
                "ROLLBACK" => ParseRollback(),
                KwSavepoint => ParseSavepoint(),
                "RELEASE" => ParseReleaseSavepoint(),
                "USE" => ParseUseDatabase(),
                "EXPLAIN" => ParseExplain(),
                _ => throw ParseError($"unexpected keyword '{Current.Text}'"),
            };
        }
        throw ParseError($"unexpected token '{Current.Text}'");
    }

    // ── SELECT ───────────────────────────────────────────────────────────────

    private SelectStatement ParseSelect()
    {
        // WITH clause
        List<CommonTableExpr> withClause = [];
        if (TryConsumeKeyword("WITH"))
            withClause = ParseWithClause();

        ExpectKeyword(KwSelect);
        bool distinct = TryConsumeKeyword("DISTINCT");

        var columns = ParseSelectList();

        TableReference? from = null;
        if (TryConsumeKeyword("FROM"))
            from = ParseTableReference();

        Expr? where = null;
        if (TryConsumeKeyword(KwWhere))
            where = ParseExpr();

        List<Expr> groupBy = [];
        if (TryConsumeKeyword("GROUP"))
        {
            ExpectKeyword("BY");
            groupBy = ParseExprList();
        }

        Expr? having = null;
        if (TryConsumeKeyword("HAVING"))
            having = ParseExpr();

        List<OrderByExpr> orderBy = [];
        if (TryConsumeKeyword("ORDER"))
        {
            ExpectKeyword("BY");
            orderBy = ParseOrderByList();
        }

        ulong? limit = null;
        if (TryConsumeKeyword("LIMIT"))
            limit = ParseUInt64();

        ulong? offset = null;
        if (TryConsumeKeyword("OFFSET"))
            offset = ParseUInt64();

        return new SelectStatement
        {
            With = withClause,
            Distinct = distinct,
            Columns = columns,
            From = from,
            WhereClause = where,
            GroupBy = groupBy,
            Having = having,
            OrderBy = orderBy,
            Limit = limit,
            Offset = offset,
        };
    }

    private List<CommonTableExpr> ParseWithClause()
    {
        var ctes = new List<CommonTableExpr>();
        do
        {
            var name = ParseIdent();
            List<string> cols = [];
            if (Current.Kind == TokenKind.LeftParen)
            {
                Consume();
                cols = ParseIdentList();
                Expect(TokenKind.RightParen, ")");
            }
            ExpectKeyword("AS");
            Expect(TokenKind.LeftParen, "(");
            var query = ParseSelect();
            Expect(TokenKind.RightParen, ")");
            ctes.Add(new CommonTableExpr(name, cols, query));
        } while (Current.Kind == TokenKind.Comma && (Consume() != null));
        return ctes;
    }

    private List<SelectItem> ParseSelectList()
    {
        var items = new List<SelectItem>();
        do
        {
            items.Add(ParseSelectItem());
        } while (Current.Kind == TokenKind.Comma && (Consume() != null));
        return items;
    }

    private SelectItem ParseSelectItem()
    {
        if (Current.Kind == TokenKind.Star)
        {
            Consume();
            return SelectItem.Wildcard.Instance;
        }

        // table.* or table.*
        if ((Current.Kind == TokenKind.Ident || Current.Kind == TokenKind.Keyword)
            && Peek().Kind == TokenKind.Dot
            && Peek(2).Kind == TokenKind.Star)
        {
            var tbl = Consume().Text;
            Consume(); // .
            Consume(); // *
            return new SelectItem.QualifiedWildcard(tbl);
        }

        // EXPAND(col) [AS alias]
        if (IsKeyword("EXPAND"))
        {
            Consume();
            Expect(TokenKind.LeftParen, "(");
            var expandExpr = ParseExpr();
            Expect(TokenKind.RightParen, ")");
            string? alias = null;
            if (TryConsumeKeyword("AS")) alias = ParseIdent();
            return new SelectItem.Expand(expandExpr, alias);
        }

        var expr = ParseExpr();
        string? al = null;
        if (TryConsumeKeyword("AS"))
            al = ParseIdent();
        else if (Current.Kind == TokenKind.Ident || (Current.Kind == TokenKind.Keyword && !IsKeywordAny("FROM", KwWhere, "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "EXCEPT", "INTERSECT", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "ON", "AND", "OR", "NOT", "WHEN", "THEN", "ELSE", "END", "IN", "BETWEEN", "LIKE", "IS", "AS", "INTO", "SET", "VALUES", "COMMA")))
            al = Consume().Text; // implicit alias

        return new SelectItem.ExprItem(expr, al);
    }

    private List<OrderByExpr> ParseOrderByList()
    {
        var items = new List<OrderByExpr>();
        do
        {
            var expr = ParseExpr();
            bool asc = true;
            if (TryConsumeKeyword("DESC")) asc = false;
            else TryConsumeKeyword("ASC");
            items.Add(new OrderByExpr(expr, asc));
        } while (Current.Kind == TokenKind.Comma && (Consume() != null));
        return items;
    }

    // ── INSERT ───────────────────────────────────────────────────────────────

    private Statement.Insert ParseInsert()
    {
        ExpectKeyword("INSERT");
        ExpectKeyword("INTO");
        var table = ParseIdent();

        List<string> cols = [];
        if (Current.Kind == TokenKind.LeftParen && !IsFollowedBySelect())
        {
            Consume();
            cols = ParseIdentList();
            Expect(TokenKind.RightParen, ")");
        }

        ExpectKeyword("VALUES");
        var allRows = new List<IReadOnlyList<Expr>>();
        do
        {
            Expect(TokenKind.LeftParen, "(");
            var row = ParseExprList();
            Expect(TokenKind.RightParen, ")");
            allRows.Add(row);
        } while (Current.Kind == TokenKind.Comma && (Consume() != null));

        return new Statement.Insert(new InsertStatement(table, cols, allRows));
    }

    private bool IsFollowedBySelect()
    {
        // Peek ahead to see if this is INSERT INTO table (SELECT ...)
        var saved = _pos;
        _pos++;
        SkipParens();
        bool result = IsKeyword(KwSelect) || IsKeyword("WITH");
        _pos = saved;
        return result;
    }

    private void SkipParens()
    {
        int depth = 0;
        while (_pos < _tokens.Count)
        {
            if (_tokens[_pos].Kind == TokenKind.LeftParen) depth++;
            else if (_tokens[_pos].Kind == TokenKind.RightParen) { depth--; if (depth <= 0) break; }
            _pos++;
        }
    }

    // ── UPDATE ───────────────────────────────────────────────────────────────

    private Statement.Update ParseUpdate()
    {
        ExpectKeyword("UPDATE");
        var table = ParseIdent();
        ExpectKeyword("SET");
        var assignments = new List<(string, Expr)>();
        do
        {
            var col = ParseIdent();
            Expect(TokenKind.Eq, "=");
            var val = ParseExpr();
            assignments.Add((col, val));
        } while (Current.Kind == TokenKind.Comma && (Consume() != null));

        Expr? where = null;
        if (TryConsumeKeyword(KwWhere)) where = ParseExpr();

        return new Statement.Update(new UpdateStatement(table, assignments, where));
    }

    // ── DELETE ───────────────────────────────────────────────────────────────

    private Statement.Delete ParseDelete()
    {
        ExpectKeyword(KwDelete);
        ExpectKeyword("FROM");
        var table = ParseIdent();
        Expr? where = null;
        if (TryConsumeKeyword(KwWhere)) where = ParseExpr();
        return new Statement.Delete(new DeleteStatement(table, where));
    }

    // ── CREATE ───────────────────────────────────────────────────────────────

    private Statement ParseCreate()
    {
        ExpectKeyword("CREATE");
        if (IsKeyword(KwTable) || IsKeyword("TEMPORARY") || IsKeyword("TEMP"))
            return ParseCreateTable();
        if (IsKeyword("INDEX") || IsKeyword(KwUnique))
            return ParseCreateIndex();
        if (IsKeyword("MATERIALIZED"))
        {
            Consume();
            return ParseCreateMaterializedView();
        }
        if (IsKeyword("VIEW"))
            return ParseCreateMaterializedView();
        if (IsKeyword("TYPE"))
            return ParseCreateType();
        if (IsKeyword("ENUM") || IsKeyword("FLAG"))
            return ParseCreateEnum();
        if (IsKeyword("DATABASE"))
        {
            Consume();
            bool ifne = TryConsumeKeyword("IF") && ExpectKeyword("NOT") != null && ExpectKeyword(KwExists) != null;
            var dbname = ParseIdent();
            return new Statement.CreateDatabase(new CreateDatabaseStatement(dbname, ifne));
        }
        if (IsKeyword("SCHEMA"))
        {
            Consume();
            bool ifne = TryConsumeKeyword("IF") && ExpectKeyword("NOT") != null && ExpectKeyword(KwExists) != null;
            var schname = ParseIdent();
            return new Statement.CreateSchema(new CreateSchemaStatement(null, schname, ifne));
        }
        if (IsKeyword("USER"))
        {
            Consume();
            var uname = ParseIdent();
            return new Statement.CreateUser(new CreateUserStatement(uname, null, []));
        }
        throw ParseError($"unexpected CREATE subtype '{Current.Text}'");
    }

    private Statement.CreateTable ParseCreateTable()
    {
        bool temp = false;
        if (TryConsumeKeyword("TEMPORARY") || TryConsumeKeyword("TEMP")) temp = true;
        bool flat = TryConsumeKeyword("FLAT");
        ExpectKeyword(KwTable);
        bool ifne = false;
        if (TryConsumeKeyword("IF")) { ExpectKeyword("NOT"); ExpectKeyword(KwExists); ifne = true; }

        var (db, schema, table) = ParseQualifiedName();

        Expect(TokenKind.LeftParen, "(");
        var cols = new List<ColumnDef>();
        var constraints = new List<TableConstraint>();
        ParseColumnDefsAndConstraints(cols, constraints);
        Expect(TokenKind.RightParen, ")");

        bool versioned = false;
        List<string> inherits = [];
        OnCommitBehavior? onCommit = null;

        while (IsKeywordAny("INHERITS", "WITH", "ON"))
        {
            if (TryConsumeKeyword("INHERITS"))
            {
                Expect(TokenKind.LeftParen, "(");
                inherits = ParseIdentList();
                Expect(TokenKind.RightParen, ")");
            }
            else if (TryConsumeKeyword("WITH"))
            {
                ExpectKeyword("SYSTEM"); ExpectKeyword("VERSIONING");
                versioned = true;
            }
            else if (TryConsumeKeyword("ON"))
            {
                ExpectKeyword("COMMIT");
                if (TryConsumeKeyword("PRESERVE")) { ExpectKeyword("ROWS"); onCommit = OnCommitBehavior.PreserveRows; }
                else if (TryConsumeKeyword(KwDelete)) { ExpectKeyword("ROWS"); onCommit = OnCommitBehavior.DeleteRows; }
                else if (TryConsumeKeyword("DROP")) onCommit = OnCommitBehavior.Drop;
            }
        }

        return new Statement.CreateTable(new CreateTableStatement
        {
            Database = db,
            Schema = schema,
            Table = table,
            Columns = cols,
            IfNotExists = ifne,
            Temporary = temp,
            Flat = flat,
            Inherits = inherits,
            Versioned = versioned,
            OnCommit = onCommit,
            Constraints = constraints,
        });
    }

    private void ParseColumnDefsAndConstraints(List<ColumnDef> cols, List<TableConstraint> constraints)
    {
        while (Current.Kind != TokenKind.RightParen && Current.Kind != TokenKind.Eof)
        {
            if (!TryParseTableConstraint(constraints))
            {
                cols.Add(ParseColumnDef());
            }

            if (Current.Kind == TokenKind.Comma) Consume();
            else break;
        }
    }

    private bool TryParseTableConstraint(List<TableConstraint> constraints)
    {
        if (TryParseUnnamedTableConstraint(constraints)) return true;
        return TryParseNamedTableConstraint(constraints);
    }

    private bool TryParseUnnamedTableConstraint(List<TableConstraint> constraints)
    {
        if (IsKeyword(KwPrimary))
        {
            constraints.Add(ParsePrimaryKeyConstraint(null));
            return true;
        }

        if (IsKeyword(KwUnique))
        {
            constraints.Add(ParseUniqueConstraint(null));
            return true;
        }

        if (!IsKeyword(KwCheck)) return false;
        constraints.Add(ParseCheckConstraint(null));
        return true;
    }

    private bool TryParseNamedTableConstraint(List<TableConstraint> constraints)
    {
        if (!TryConsumeKeyword("CONSTRAINT")) return false;
        var name = ParseIdent();
        if (IsKeyword(KwPrimary))
        {
            constraints.Add(ParsePrimaryKeyConstraint(name));
            return true;
        }

        if (IsKeyword(KwUnique))
        {
            constraints.Add(ParseUniqueConstraint(name));
            return true;
        }

        if (IsKeyword(KwCheck))
        {
            constraints.Add(ParseCheckConstraint(name));
            return true;
        }

        throw ParseError($"unexpected CONSTRAINT subtype '{Current.Text}'");
    }

    private TableConstraint.PrimaryKey ParsePrimaryKeyConstraint(string? name)
    {
        Consume();
        ExpectKeyword("KEY");
        Expect(TokenKind.LeftParen, "(");
        var columns = ParseIdentList();
        Expect(TokenKind.RightParen, ")");
        return new TableConstraint.PrimaryKey(name, columns);
    }

    private TableConstraint.Unique ParseUniqueConstraint(string? name)
    {
        Consume();
        Expect(TokenKind.LeftParen, "(");
        var columns = ParseIdentList();
        Expect(TokenKind.RightParen, ")");
        return new TableConstraint.Unique(name, columns);
    }

    private TableConstraint.Check ParseCheckConstraint(string? name)
    {
        Consume();
        Expect(TokenKind.LeftParen, "(");
        var expr = ParseExpr();
        Expect(TokenKind.RightParen, ")");
        return new TableConstraint.Check(name, expr);
    }

    private ColumnDef ParseColumnDef()
    {
        var name = ParseIdent();
        var dt = ParseDataType();
        var options = ParseColumnOptions();

        return new ColumnDef
        {
            Name = name,
            DataType = dt,
            Nullable = options.Nullable,
            PrimaryKey = options.PrimaryKey,
            Unique = options.Unique,
            AutoIncrement = options.AutoIncrement,
            Default = options.Default,
            Check = options.Check,
            OnUpdate = options.OnUpdate,
            OnDelete = options.OnDelete,
            MinLength = options.MinLength,
            MaxLength = options.MaxLength,
            RequiresDistinctReferences = options.RequiresDistinctReferences,
        };
    }

    private ColumnOptions ParseColumnOptions()
    {
        var options = new ColumnOptions();
        while (!IsColumnDefEnd())
        {
            if (!TryParseColumnOption(options)) break;
        }

        return options;
    }

    private bool TryParseColumnOption(ColumnOptions options)
    {
        if (TryParseColumnNullability(options)) return true;
        if (TryParseColumnKeyOptions(options)) return true;
        if (TryParseColumnDefaultOrCheck(options)) return true;
        if (TryParseColumnReferentialAction(options)) return true;
        if (TryParseColumnLengthOption(options)) return true;
        if (!IsKeyword("UNIQUES")) return false;
        Consume();
        options.RequiresDistinctReferences = true;
        return true;
    }

    private bool TryParseColumnNullability(ColumnOptions options)
    {
        if (IsKeyword("NOT"))
        {
            Consume();
            ExpectKeyword("NULL");
            options.Nullable = false;
            return true;
        }

        if (!IsKeyword("NULL")) return false;
        Consume();
        options.Nullable = true;
        return true;
    }

    private bool TryParseColumnKeyOptions(ColumnOptions options)
    {
        if (IsKeyword(KwPrimary))
        {
            Consume();
            ExpectKeyword("KEY");
            options.PrimaryKey = true;
            return true;
        }

        if (IsKeyword(KwUnique))
        {
            Consume();
            options.Unique = true;
            return true;
        }

        if (!IsKeyword("AUTO_INCREMENT") && !IsKeyword("AUTOINCREMENT")) return false;
        Consume();
        options.AutoIncrement = true;
        return true;
    }

    private bool TryParseColumnDefaultOrCheck(ColumnOptions options)
    {
        if (IsKeyword("DEFAULT"))
        {
            Consume();
            options.Default = ParsePrimaryExpr();
            return true;
        }

        if (!IsKeyword(KwCheck)) return false;
        Consume();
        Expect(TokenKind.LeftParen, "(");
        options.Check = ParseExpr();
        Expect(TokenKind.RightParen, ")");
        return true;
    }

    private bool TryParseColumnReferentialAction(ColumnOptions options)
    {
        if (!IsKeyword("ON")) return false;
        Consume();
        if (IsKeyword("UPDATE"))
        {
            Consume();
            options.OnUpdate = ParseReferentialAction();
            return true;
        }

        if (!IsKeyword(KwDelete)) return false;
        Consume();
        options.OnDelete = ParseReferentialAction();
        return true;
    }

    private bool TryParseColumnLengthOption(ColumnOptions options)
    {
        if (IsKeyword("MIN_LENGTH"))
        {
            Consume();
            Expect(TokenKind.Eq, "=");
            options.MinLength = ParseUInt64();
            return true;
        }

        if (!IsKeyword("MAX_LENGTH")) return false;
        Consume();
        Expect(TokenKind.Eq, "=");
        options.MaxLength = ParseUInt64();
        return true;
    }

    private bool IsColumnDefEnd() =>
        Current.Kind is TokenKind.Comma or TokenKind.RightParen or TokenKind.Eof
        || IsKeywordAny("CONSTRAINT", KwPrimary, KwUnique, KwCheck);

    private ReferentialAction ParseReferentialAction()
    {
        if (TryConsumeKeyword("CASCADE")) return ReferentialAction.Cascade;
        if (TryConsumeKeyword("SET"))
        {
            if (TryConsumeKeyword("NULL")) return ReferentialAction.SetNull;
            if (TryConsumeKeyword("DEFAULT")) return ReferentialAction.SetDefault;
        }
        if (TryConsumeKeyword("RESTRICT")) return ReferentialAction.Restrict;
        if (TryConsumeKeyword("NO")) { ExpectKeyword("ACTION"); return ReferentialAction.NoAction; }
        return ReferentialAction.NoAction;
    }

    private DataType ParseDataType()
    {
        if (Current.Kind == TokenKind.LeftBracket)
        {
            Consume();
            var elem = ParseDataType();
            Expect(TokenKind.RightBracket, "]");
            return new DataType.Vector(elem);
        }

        var name = Current.Text.ToUpperInvariant();
        Consume();

        return name switch
        {
            "TINYINT" => ParseUnsignedVariant(DataType.TinyInt.Instance, DataType.UnsignedTinyInt.Instance),
            "SMALLINT" => ParseUnsignedVariant(DataType.SmallInt.Instance, DataType.UnsignedSmallInt.Instance),
            "MEDIUMINT" => ParseUnsignedVariant(DataType.MediumInt.Instance, DataType.UnsignedMediumInt.Instance),
            "INT" or "INTEGER" => ParseUnsignedVariant(DataType.Integer.Instance, DataType.UnsignedInt.Instance),
            "BIGINT" => ParseUnsignedVariant(DataType.BigInt.Instance, DataType.UnsignedBigInt.Instance),
            "FLOAT" or "REAL" => DataType.Float.Instance,
            "DOUBLE" => ParseDoubleType(),
            "DECIMAL" or "NUMERIC" => ParseDecimalType(),
            "VARCHAR" or "CHARACTER VARYING" => new DataType.Varchar(TryParseParenUInt64()),
            "TEXT" => new DataType.Varchar(null),
            "TINYTEXT" => DataType.TinyText.Instance,
            "MEDIUMTEXT" => DataType.MediumText.Instance,
            "LONGTEXT" => DataType.LongText.Instance,
            "CHAR" or "CHARACTER" => new DataType.Char(TryParseParenUInt64()),
            "BOOLEAN" or "BOOL" => DataType.Boolean.Instance,
            "DATE" => DataType.Date.Instance,
            "TIME" => ParseTimeType(DataType.TimeTz.Instance, DataType.Time.Instance),
            "DATETIME" => DataType.DateTime.Instance,
            "TIMESTAMP" => ParseTimeType(DataType.TimestampTz.Instance, DataType.Timestamp.Instance),
            "UUID" or "GUID" => DataType.Uuid.Instance,
            "BINARY" => new DataType.Binary(TryParseParenUInt64()),
            "VARBINARY" => new DataType.Varbinary(TryParseParenUInt64()),
            "BLOB" => new DataType.Blob(TryParseParenUInt64()),
            "TINYBLOB" => DataType.TinyBlob.Instance,
            "MEDIUMBLOB" => DataType.MediumBlob.Instance,
            "LONGBLOB" => DataType.LongBlob.Instance,
            "JSON" => new DataType.Other("JSON"),
            _ => new DataType.Other(name),
        };
    }

    private DataType ParseUnsignedVariant(DataType signed, DataType unsigned) =>
        TryConsumeKeyword("UNSIGNED") ? unsigned : signed;

    private DataType ParseTimeType(DataType withTz, DataType withoutTz)
    {
        if (!TryConsumeKeyword("WITH")) return withoutTz;
        ExpectKeyword("TIME");
        ExpectKeyword("ZONE");
        return withTz;
    }

    private DataType.Double ParseDoubleType()
    {
        TryConsumeKeyword("PRECISION");
        return DataType.Double.Instance;
    }

    private DataType.Decimal ParseDecimalType()
    {
        if (Current.Kind != TokenKind.LeftParen) return new DataType.Decimal(null, null);
        Consume();
        var p = ParseUInt64();
        ulong? s = null;
        if (Current.Kind == TokenKind.Comma) { Consume(); s = ParseUInt64(); }
        Expect(TokenKind.RightParen, ")");
        return new DataType.Decimal(p, s);
    }

    private ulong? TryParseParenUInt64()
    {
        if (Current.Kind != TokenKind.LeftParen) return null;
        Consume();
        var n = ParseUInt64();
        Expect(TokenKind.RightParen, ")");
        return n;
    }

    // ── CREATE INDEX ─────────────────────────────────────────────────────────

    private Statement.CreateIndex ParseCreateIndex()
    {
        bool unique = TryConsumeKeyword(KwUnique);
        ExpectKeyword("INDEX");
        bool ifne = false;
        if (TryConsumeKeyword("IF")) { ExpectKeyword("NOT"); ExpectKeyword(KwExists); ifne = true; }
        string? idxName = null;
        if (Current.Kind == TokenKind.Ident || (Current.Kind == TokenKind.Keyword && !IsKeyword("ON")))
            idxName = Consume().Text;
        ExpectKeyword("ON");
        var tbl = ParseIdent();
        Expect(TokenKind.LeftParen, "(");
        var idxCols = ParseIndexColumns();
        Expect(TokenKind.RightParen, ")");
        IndexType idxType = IndexType.BTree.Instance;
        if (TryConsumeKeyword("USING"))
        {
            var itname = ParseIdent().ToUpperInvariant();
            idxType = itname switch
            {
                "HASH" => IndexType.Hash.Instance,
                "GIN" => IndexType.Gin.Instance,
                "GIST" => IndexType.Gist.Instance,
                "SPGIST" => IndexType.SpGist.Instance,
                "BRIN" => IndexType.Brin.Instance,
                "BLOOM" => IndexType.Bloom.Instance,
                "FULLTEXT" or "FULL_TEXT" => IndexType.FullText.Instance,
                "TRIGRAM" => IndexType.Trigram.Instance,
                _ => new IndexType.Other(itname),
            };
        }
        return new Statement.CreateIndex(new CreateIndexStatement
        {
            Name = idxName,
            Table = tbl,
            Columns = idxCols,
            Unique = unique,
            IfNotExists = ifne,
            IndexType = idxType,
        });
    }

    private List<IndexColumn> ParseIndexColumns()
    {
        var cols = new List<IndexColumn>();
        do
        {
            var col = ParseIdent();
            bool? asc = null;
            if (TryConsumeKeyword("ASC")) asc = true;
            else if (TryConsumeKeyword("DESC")) asc = false;
            cols.Add(new IndexColumn(col, asc));
        } while (Current.Kind == TokenKind.Comma && (Consume() != null));
        return cols;
    }

    // ── CREATE MATERIALIZED VIEW ──────────────────────────────────────────────

    private Statement.CreateMaterializedView ParseCreateMaterializedView()
    {
        if (IsKeyword("VIEW")) Consume();
        bool ifne = false;
        if (TryConsumeKeyword("IF")) { ExpectKeyword("NOT"); ExpectKeyword(KwExists); ifne = true; }
        bool orReplace = false;
        if (TryConsumeKeyword("OR")) { ExpectKeyword("REPLACE"); orReplace = true; }
        var name = ParseIdent();
        ExpectKeyword("AS");
        Expect(TokenKind.LeftParen, "(");
        var query = ParseSelect();
        Expect(TokenKind.RightParen, ")");
        return new Statement.CreateMaterializedView(new CreateMaterializedViewStatement(name, query, ifne, orReplace));
    }

    // ── CREATE TYPE ───────────────────────────────────────────────────────────

    private Statement ParseCreateType()
    {
        ExpectKeyword("TYPE");
        bool ifne = false;
        if (TryConsumeKeyword("IF")) { ExpectKeyword("NOT"); ExpectKeyword(KwExists); ifne = true; }
        var typeName = ParseIdent();
        ExpectKeyword("AS");
        if (IsKeyword("ENUM") || IsKeyword("FLAG"))
        {
            bool flag = TryConsumeKeyword("FLAG");
            if (IsKeyword("ENUM")) Consume();
            Expect(TokenKind.LeftParen, "(");
            var variants = ParseEnumVariants();
            Expect(TokenKind.RightParen, ")");
            return new Statement.CreateEnum(new CreateEnumStatement(typeName, flag, variants, ifne));
        }
        else
        {
            Expect(TokenKind.LeftParen, "(");
            var fields = ParseCompositeFields();
            Expect(TokenKind.RightParen, ")");
            return new Statement.CreateType(new CreateTypeStatement(typeName, new TypeDefinition.Composite(fields)));
        }
    }

    private Statement.CreateEnum ParseCreateEnum()
    {
        bool flag = TryConsumeKeyword("FLAG");
        if (IsKeyword("ENUM")) Consume();
        bool ifne = false;
        if (TryConsumeKeyword("IF")) { ExpectKeyword("NOT"); ExpectKeyword(KwExists); ifne = true; }
        var name = ParseIdent();
        Expect(TokenKind.LeftParen, "(");
        var variants = ParseEnumVariants();
        Expect(TokenKind.RightParen, ")");
        return new Statement.CreateEnum(new CreateEnumStatement(name, flag, variants, ifne));
    }

    private List<EnumVariant> ParseEnumVariants()
    {
        var variants = new List<EnumVariant>();
        while (Current.Kind != TokenKind.RightParen && Current.Kind != TokenKind.Eof)
        {
            string vname;
            if (Current.Kind == TokenKind.StringLiteral) vname = Consume().Text;
            else vname = ParseIdent();
            bool isNone = vname.Equals("NONE", StringComparison.OrdinalIgnoreCase);
            variants.Add(new EnumVariant(vname.ToLowerInvariant(), isNone));
            if (Current.Kind == TokenKind.Comma) Consume();
            else break;
        }
        return variants;
    }

    private List<CompositeField> ParseCompositeFields()
    {
        var fields = new List<CompositeField>();
        while (Current.Kind != TokenKind.RightParen && Current.Kind != TokenKind.Eof)
        {
            var fname = ParseIdent();
            var ftype = ParseDataType();
            bool notNull = false;
            if (TryConsumeKeyword("NOT")) { ExpectKeyword("NULL"); notNull = true; }
            fields.Add(new CompositeField(fname, ftype, notNull));
            if (Current.Kind == TokenKind.Comma) Consume();
            else break;
        }
        return fields;
    }

    // ── DROP ─────────────────────────────────────────────────────────────────

    private Statement ParseDrop()
    {
        ExpectKeyword("DROP");

        var dropTable = TryParseDropTable();
        if (dropTable is not null) return dropTable;

        var dropIndex = TryParseDropIndex();
        if (dropIndex is not null) return dropIndex;

        var dropType = TryParseDropType();
        if (dropType is not null) return dropType;

        var dropEnum = TryParseDropEnum();
        if (dropEnum is not null) return dropEnum;

        var dropDatabase = TryParseDropDatabase();
        if (dropDatabase is not null) return dropDatabase;

        var dropSchema = TryParseDropSchema();
        if (dropSchema is not null) return dropSchema;

        var dropUser = TryParseDropUser();
        if (dropUser is not null) return dropUser;

        throw ParseError($"unexpected DROP subtype '{Current.Text}'");
    }

    private Statement.DropTable? TryParseDropTable()
    {
        if (!TryConsumeKeyword(KwTable)) return null;

        var ifExists = TryParseIfExists();
        return new Statement.DropTable(new DropTableStatement(ParseIdentList(), ifExists));
    }

    private Statement.DropIndex? TryParseDropIndex()
    {
        if (!TryConsumeKeyword("INDEX")) return null;

        var ifExists = TryParseIfExists();
        return new Statement.DropIndex(new DropIndexStatement(ParseIdentList(), ifExists));
    }

    private Statement.DropType? TryParseDropType()
    {
        if (!TryConsumeKeyword("TYPE")) return null;

        var ifExists = TryParseIfExists();
        return new Statement.DropType(new DropTypeStatement(ParseIdent(), ifExists));
    }

    private Statement.DropEnum? TryParseDropEnum()
    {
        if (!TryConsumeKeyword("ENUM")) return null;

        var ifExists = TryParseIfExists();
        return new Statement.DropEnum(new DropEnumStatement(ParseIdent(), ifExists));
    }

    private Statement.DropDatabase? TryParseDropDatabase()
    {
        if (!TryConsumeKeyword("DATABASE")) return null;

        var ifExists = TryParseIfExists();
        return new Statement.DropDatabase(new DropDatabaseStatement(ParseIdent(), ifExists));
    }

    private Statement.DropSchema? TryParseDropSchema()
    {
        if (!TryConsumeKeyword("SCHEMA")) return null;

        var ifExists = TryParseIfExists();
        return new Statement.DropSchema(new DropSchemaStatement(null, ParseIdent(), ifExists));
    }

    private Statement.DropUser? TryParseDropUser()
    {
        if (!TryConsumeKeyword("USER")) return null;

        var ifExists = TryParseIfExists();
        return new Statement.DropUser(new DropUserStatement(ParseIdentList(), ifExists));
    }

    private bool TryParseIfExists()
    {
        if (!TryConsumeKeyword("IF")) return false;
        ExpectKeyword(KwExists);
        return true;
    }

    // ── ALTER TABLE ────────────────────────────────────────────────────────────

    private Statement.AlterTable ParseAlterTable()
    {
        ExpectKeyword("ALTER");
        ExpectKeyword(KwTable);
        var table = ParseIdent();
        var ops = new List<AlterTableOperation>();
        do
        {
            if (TryConsumeKeyword("ADD"))
            {
                TryConsumeKeyword("COLUMN");
                ops.Add(new AlterTableOperation.AddColumn(ParseColumnDef()));
            }
            else if (TryConsumeKeyword("DROP"))
            {
                TryConsumeKeyword("COLUMN");
                bool ifex = false;
                if (TryConsumeKeyword("IF")) { ExpectKeyword(KwExists); ifex = true; }
                ops.Add(new AlterTableOperation.DropColumn(ParseIdent(), ifex));
            }
            else if (TryConsumeKeyword("RENAME"))
            {
                if (TryConsumeKeyword("TO"))
                {
                    ops.Add(new AlterTableOperation.RenameTable(ParseIdent()));
                }
                else
                {
                    TryConsumeKeyword("COLUMN");
                    var oldName = ParseIdent();
                    ExpectKeyword("TO");
                    ops.Add(new AlterTableOperation.RenameColumn(oldName, ParseIdent()));
                }
            }
            else break;
        } while (Current.Kind == TokenKind.Comma && (Consume() != null));
        return new Statement.AlterTable(new AlterTableStatement(table, ops));
    }

    // ── GRANT / REVOKE ────────────────────────────────────────────────────────

    private Statement.Grant ParseGrant()
    {
        ExpectKeyword("GRANT");
        var privs = ParseIdentList();
        ExpectKeyword("ON");
        var on = ParseIdent();
        ExpectKeyword("TO");
        var to = ParseIdentList();
        return new Statement.Grant(new GrantStatement(privs, [], on, to));
    }

    private Statement.Revoke ParseRevoke()
    {
        ExpectKeyword("REVOKE");
        var privs = ParseIdentList();
        ExpectKeyword("ON");
        var on = ParseIdent();
        ExpectKeyword("FROM");
        var from = ParseIdentList();
        return new Statement.Revoke(new RevokeStatement(privs, [], on, from));
    }

    // ── Transaction control ───────────────────────────────────────────────────

    private Statement.BeginTransaction ParseBeginTransaction()
    {
        if (IsKeyword("START")) { Consume(); ExpectKeyword(KwTransaction); }
        else { ExpectKeyword("BEGIN"); TryConsumeKeyword(KwTransaction); }
        string? txName = null;
        if (Current.Kind == TokenKind.Ident) txName = Consume().Text;
        IsolationLevel? iso = null;
        bool readOnly = false;
        // Basic mode parsing
        while (IsKeywordAny("ISOLATION", "READ", "WRITE"))
        {
            if (TryConsumeKeyword("ISOLATION")) { ExpectKeyword("LEVEL"); iso = ParseIsolationLevel(); }
            else if (TryConsumeKeyword("READ")) { readOnly = !TryConsumeKeyword("WRITE"); if (!readOnly) TryConsumeKeyword("ONLY"); }
        }
        return new Statement.BeginTransaction(new BeginTransactionStatement { Name = txName, IsolationLevel = iso, ReadOnly = readOnly });
    }

    private IsolationLevel ParseIsolationLevel()
    {
        if (TryConsumeKeyword("READ"))
        {
            if (TryConsumeKeyword("UNCOMMITTED")) return IsolationLevel.ReadUncommitted;
            if (TryConsumeKeyword("COMMITTED")) return IsolationLevel.ReadCommitted;
        }
        if (TryConsumeKeyword("REPEATABLE")) { TryConsumeKeyword("READ"); return IsolationLevel.RepeatableRead; }
        if (TryConsumeKeyword("SERIALIZABLE")) return IsolationLevel.Serializable;
        return IsolationLevel.ReadCommitted;
    }

    private Statement.Commit ParseCommit()
    {
        ExpectKeyword("COMMIT");
        TryConsumeKeyword(KwTransaction);
        bool chain = TryConsumeKeyword("AND") && TryConsumeKeyword("CHAIN");
        CommitScope scope = CommitScope.Current.Instance;
        if (TryConsumeKeyword("ALL")) scope = CommitScope.All.Instance;
        else if (Current.Kind == TokenKind.Ident) scope = new CommitScope.Named(Consume().Text);
        return new Statement.Commit(new CommitStatement(scope, chain));
    }

    private Statement.Rollback ParseRollback()
    {
        ExpectKeyword("ROLLBACK");
        TryConsumeKeyword(KwTransaction);
        bool chain = TryConsumeKeyword("AND") && TryConsumeKeyword("CHAIN");
        RollbackScope scope = RollbackScope.Current.Instance;
        if (TryConsumeKeyword("ALL")) scope = RollbackScope.All.Instance;
        else if (TryConsumeKeyword("TO"))
        {
            TryConsumeKeyword(KwSavepoint);
            scope = new RollbackScope.ToSavepoint(ParseIdent());
        }
        else if (Current.Kind == TokenKind.Ident) scope = new RollbackScope.Named(Consume().Text);
        return new Statement.Rollback(new RollbackStatement(scope, chain));
    }

    private Statement.Savepoint ParseSavepoint()
    {
        ExpectKeyword(KwSavepoint);
        return new Statement.Savepoint(new SavepointStatement(ParseIdent()));
    }

    private Statement.ReleaseSavepoint ParseReleaseSavepoint()
    {
        ExpectKeyword("RELEASE");
        TryConsumeKeyword(KwSavepoint);
        return new Statement.ReleaseSavepoint(new ReleaseSavepointStatement(ParseIdent()));
    }

    private Statement.UseDatabase ParseUseDatabase()
    {
        ExpectKeyword("USE");
        TryConsumeKeyword("DATABASE");
        return new Statement.UseDatabase(new UseDatabaseStatement(ParseIdent()));
    }

    private Statement ParseExplain()
    {
        ExpectKeyword("EXPLAIN");
        TryConsumeKeyword("ANALYZE");
        // Parse the inner statement and wrap as a select for now
        var inner = ParseStatement();
        // Return as-is; EXPLAIN is treated as a pass-through in this POC
        return inner;
    }

    // ── FROM / JOIN ───────────────────────────────────────────────────────────

    private TableReference ParseTableReference()
    {
        var left = ParseSingleTableRef();
        while (IsJoinKeyword())
        {
            var jt = ParseJoinType();
            var right = ParseSingleTableRef();
            Expr? on = null;
            if (TryConsumeKeyword("ON") || TryConsumeKeyword("FILTER")) { TryConsumeKeyword("BY"); on = ParseExpr(); }
            left = new TableReference.Join(left, right, jt, on);
        }
        return left;
    }

    private bool IsJoinKeyword() =>
        IsKeywordAny("JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS");

    private JoinType ParseJoinType()
    {
        if (TryConsumeKeyword("INNER")) { ExpectKeyword("JOIN"); return JoinType.Inner; }
        if (TryConsumeKeyword("LEFT")) { TryConsumeKeyword("OUTER"); ExpectKeyword("JOIN"); return JoinType.Left; }
        if (TryConsumeKeyword("RIGHT")) { TryConsumeKeyword("OUTER"); ExpectKeyword("JOIN"); return JoinType.Right; }
        if (TryConsumeKeyword("FULL")) { TryConsumeKeyword("OUTER"); ExpectKeyword("JOIN"); return JoinType.Full; }
        if (TryConsumeKeyword("CROSS")) { ExpectKeyword("JOIN"); return JoinType.Cross; }
        ExpectKeyword("JOIN"); return JoinType.Inner;
    }

    private TableReference ParseSingleTableRef()
    {
        if (Current.Kind == TokenKind.LeftParen)
        {
            Consume();
            var subq = ParseSelect();
            Expect(TokenKind.RightParen, ")");
            ExpectKeyword("AS");
            var al = ParseIdent();
            return new TableReference.Subquery(subq, al);
        }

        var (db, schema, name) = ParseQualifiedName();
        string? alias = null;
        if (TryConsumeKeyword("AS")) alias = ParseIdent();
        else if (Current.Kind == TokenKind.Ident && !IsJoinKeyword() && !IsKeywordAny(KwWhere, "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "ON", "FILTER"))
            alias = Consume().Text;
        return new TableReference.Named(db, schema, name, alias);
    }

    // ── Expression parsing ────────────────────────────────────────────────────
    // Pratt-style precedence: lowest first

    private Expr ParseExpr() => ParseOr();

    private Expr ParseOr()
    {
        var left = ParseAnd();
        while (IsKeyword("OR"))
        {
            Consume();
            left = new Expr.BinaryOp(left, BinaryOperator.Or, ParseAnd());
        }
        return left;
    }

    private Expr ParseAnd()
    {
        var left = ParseNot();
        while (IsKeyword("AND"))
        {
            Consume();
            left = new Expr.BinaryOp(left, BinaryOperator.And, ParseNot());
        }
        return left;
    }

    private Expr ParseNot()
    {
        if (IsKeyword("NOT")) { Consume(); return new Expr.UnaryOp(UnaryOperator.Not, ParseNot()); }
        return ParseComparison();
    }

    private Expr ParseComparison()
    {
        var left = ParseBitOr();
        var hasMoreComparisons = TryParseComparisonContinuation(ref left);
        while (hasMoreComparisons)
        {
            hasMoreComparisons = TryParseComparisonContinuation(ref left);
        }
        return left;
    }

    private bool TryParseComparisonContinuation(ref Expr left)
    {
        if (TryParseSimpleComparison(ref left)) return true;
        if (TryParseNullComparison(ref left)) return true;
        if (TryParseBetweenComparison(ref left)) return true;
        if (TryParseInComparison(ref left)) return true;
        return TryParsePatternComparison(ref left);
    }

    private bool TryParseSimpleComparison(ref Expr left)
    {
        BinaryOperator? op = Current.Kind switch
        {
            TokenKind.Eq => BinaryOperator.Eq,
            TokenKind.NotEq => BinaryOperator.NotEq,
            TokenKind.Lt => BinaryOperator.Lt,
            TokenKind.LtEq => BinaryOperator.LtEq,
            TokenKind.Gt => BinaryOperator.Gt,
            TokenKind.GtEq => BinaryOperator.GtEq,
            _ => null,
        };

        if (op is null) return false;
        Consume();
        left = new Expr.BinaryOp(left, op.Value, ParseBitOr());
        return true;
    }

    private bool TryParseNullComparison(ref Expr left)
    {
        if (!TryConsumeKeyword("IS")) return false;
        bool negated = TryConsumeKeyword("NOT");
        ExpectKeyword("NULL");
        left = new Expr.IsNull(left, negated);
        return true;
    }

    private bool TryParseBetweenComparison(ref Expr left)
    {
        if (TryConsumeKeyword("BETWEEN"))
        {
            left = BuildBetweenExpr(left, false);
            return true;
        }

        if (!IsKeyword("NOT") || !PeekKeyword("BETWEEN", 1)) return false;
        Consume();
        Consume();
        left = BuildBetweenExpr(left, true);
        return true;
    }

    private Expr.Between BuildBetweenExpr(Expr left, bool negated)
    {
        var lower = ParseBitOr();
        ExpectKeyword("AND");
        var upper = ParseBitOr();
        return new Expr.Between(left, lower, upper, negated);
    }

    private bool TryParseInComparison(ref Expr left)
    {
        if (TryConsumeKeyword("IN"))
        {
            left = ParseInExpr(left, false);
            return true;
        }

        if (!IsKeyword("NOT") || !PeekKeyword("IN", 1)) return false;
        Consume();
        Consume();
        left = ParseInExpr(left, true);
        return true;
    }

    private bool TryParsePatternComparison(ref Expr left)
    {
        if (TryParseDirectPatternComparison(ref left)) return true;
        return TryParseNegatedPatternComparison(ref left);
    }

    private bool TryParseDirectPatternComparison(ref Expr left)
    {
        if (TryParseBinaryKeywordOperator(ref left, "LIKE", BinaryOperator.Like)) return true;
        if (TryParseBinaryKeywordOperator(ref left, "ILIKE", BinaryOperator.ILike)) return true;
        if (IsKeyword("REGEXP") || IsKeyword("RLIKE"))
        {
            Consume();
            left = new Expr.BinaryOp(left, BinaryOperator.Regexp, ParseBitOr());
            return true;
        }

        if (!IsKeyword("SIMILAR") || !PeekKeyword("TO", 1)) return false;
        Consume();
        Consume();
        left = new Expr.BinaryOp(left, BinaryOperator.SimilarTo, ParseBitOr());
        return true;
    }

    private bool TryParseNegatedPatternComparison(ref Expr left)
    {
        if (TryParseCompoundKeywordOperator(ref left, "LIKE", BinaryOperator.NotLike)) return true;
        return TryParseCompoundKeywordOperator(ref left, "ILIKE", BinaryOperator.NotILike);
    }

    private bool TryParseBinaryKeywordOperator(ref Expr left, string keyword, BinaryOperator op)
    {
        if (!TryConsumeKeyword(keyword)) return false;
        left = new Expr.BinaryOp(left, op, ParseBitOr());
        return true;
    }

    private bool TryParseCompoundKeywordOperator(ref Expr left, string keyword, BinaryOperator op)
    {
        if (!IsKeyword("NOT") || !PeekKeyword(keyword, 1)) return false;
        Consume();
        Consume();
        left = new Expr.BinaryOp(left, op, ParseBitOr());
        return true;
    }

    private bool PeekKeyword(string kw, int offset)
    {
        var t = Peek(offset);
        return (t.Kind == TokenKind.Keyword || t.Kind == TokenKind.Ident) && t.Text.Equals(kw, StringComparison.OrdinalIgnoreCase);
    }

    private Expr ParseInExpr(Expr left, bool negated)
    {
        if (Current.Kind == TokenKind.LeftParen)
        {
            Consume();
            // Check if it's a subquery
            if (IsKeyword(KwSelect) || IsKeyword("WITH"))
            {
                var sub = ParseSelect();
                Expect(TokenKind.RightParen, ")");
                return new Expr.InSubquery(left, sub, negated);
            }
            var list = ParseExprList();
            Expect(TokenKind.RightParen, ")");
            return new Expr.InList(left, list, negated);
        }
        // IN [expr, expr, ...]
        Expect(TokenKind.LeftBracket, "[");
        var arr = ParseExprList();
        Expect(TokenKind.RightBracket, "]");
        return new Expr.InList(left, arr, negated);
    }

    private Expr ParseBitOr()
    {
        var left = ParseBitAnd();
        while (Current.Kind == TokenKind.Pipe) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.BitwiseOr, ParseBitAnd()); }
        while (Current.Kind == TokenKind.PipePipe) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.StringConcat, ParseBitAnd()); }
        return left;
    }

    private Expr ParseBitAnd()
    {
        var left = ParseShift();
        while (Current.Kind == TokenKind.Ampersand) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.BitwiseAnd, ParseShift()); }
        return left;
    }

    private Expr ParseShift()
    {
        var left = ParseAdd();
        while (true)
        {
            if (Current.Kind == TokenKind.ShiftLeft) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.ShiftLeft, ParseAdd()); }
            else if (Current.Kind == TokenKind.ShiftRight) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.ShiftRight, ParseAdd()); }
            else break;
        }
        return left;
    }

    private Expr ParseAdd()
    {
        var left = ParseMul();
        while (true)
        {
            if (Current.Kind == TokenKind.Plus) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.Plus, ParseMul()); }
            else if (Current.Kind == TokenKind.Minus) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.Minus, ParseMul()); }
            else break;
        }
        return left;
    }

    private Expr ParseMul()
    {
        var left = ParseUnary();
        while (true)
        {
            if (Current.Kind == TokenKind.Star) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.Multiply, ParseUnary()); }
            else if (Current.Kind == TokenKind.Slash) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.Divide, ParseUnary()); }
            else if (Current.Kind == TokenKind.Percent) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.Modulo, ParseUnary()); }
            else if (Current.Kind == TokenKind.Caret) { Consume(); left = new Expr.BinaryOp(left, BinaryOperator.BitwiseXor, ParseUnary()); }
            else break;
        }
        return left;
    }

    private Expr ParseUnary()
    {
        if (Current.Kind == TokenKind.Minus) { Consume(); return new Expr.UnaryOp(UnaryOperator.Minus, ParsePrimaryExpr()); }
        if (Current.Kind == TokenKind.Tilde) { Consume(); return new Expr.UnaryOp(UnaryOperator.BitwiseNot, ParsePrimaryExpr()); }
        if (IsKeyword("NOT")) { Consume(); return new Expr.UnaryOp(UnaryOperator.Not, ParsePrimaryExpr()); }
        return ParsePrimaryExpr();
    }

    private Expr ParsePrimaryExpr()
    {
        if (TryParseKeywordPrimaryExpr(out var expr)) return expr;
        if (TryParseLiteralPrimaryExpr(out expr)) return expr;
        if (TryParseGroupedPrimaryExpr(out expr)) return expr;
        if (TryParseIdentifierPrimaryExpr(out expr)) return expr;

        throw ParseError($"unexpected token in expression: '{Current.Text}'");
    }

    private bool TryParseKeywordPrimaryExpr(out Expr expr)
    {
        if (IsKeyword("CAST")) { expr = ParseCastExpr(); return true; }
        if (IsKeyword("CASE")) { expr = ParseCaseExpr(); return true; }
        if (TryParseExistsExpr(out expr)) return true;
        if (IsKeyword("SUBSTRING") || IsKeyword("SUBSTR")) { expr = ParseSubstringExpr(); return true; }
        if (IsKeyword("TRIM")) { expr = ParseTrimExpr(); return true; }
        if (TryParsePositionExpr(out expr)) return true;
        expr = null!;
        return false;
    }

    private bool TryParseExistsExpr(out Expr expr)
    {
        if (!TryConsumeKeyword(KwExists))
        {
            expr = null!;
            return false;
        }

        Expect(TokenKind.LeftParen, "(");
        var subquery = ParseSelect();
        Expect(TokenKind.RightParen, ")");
        expr = new Expr.Function("EXISTS", [new Expr.Subquery(subquery)], false);
        return true;
    }

    private bool TryParsePositionExpr(out Expr expr)
    {
        if (!TryConsumeKeyword("POSITION"))
        {
            expr = null!;
            return false;
        }

        Expect(TokenKind.LeftParen, "(");
        var substring = ParseExpr();
        ExpectKeyword("IN");
        var input = ParseExpr();
        Expect(TokenKind.RightParen, ")");
        expr = new Expr.Position(substring, input);
        return true;
    }

    private bool TryParseLiteralPrimaryExpr(out Expr expr)
    {
        if (TryParseKeywordLiteral(out expr)) return true;
        if (TryParseNumericLiteral(out expr)) return true;
        if (Current.Kind == TokenKind.StringLiteral)
        {
            expr = new Expr.Literal(new SqlValue.SqlString(Consume().Text));
            return true;
        }

        expr = null!;
        return false;
    }

    private bool TryParseKeywordLiteral(out Expr expr)
    {
        if (TryConsumeKeyword("NULL"))
        {
            expr = new Expr.Literal(SqlValue.Null.Instance);
            return true;
        }

        if (TryConsumeKeyword("TRUE"))
        {
            expr = new Expr.Literal(new SqlValue.Boolean(true));
            return true;
        }

        if (!TryConsumeKeyword("FALSE"))
        {
            expr = null!;
            return false;
        }

        expr = new Expr.Literal(new SqlValue.Boolean(false));
        return true;
    }

    private bool TryParseNumericLiteral(out Expr expr)
    {
        if (Current.Kind == TokenKind.IntLiteral)
        {
            expr = new Expr.Literal(new SqlValue.Integer(long.Parse(Current.Text, CultureInfo.InvariantCulture)));
            Consume();
            return true;
        }

        if (Current.Kind != TokenKind.FloatLiteral)
        {
            expr = null!;
            return false;
        }

        expr = new Expr.Literal(new SqlValue.Float(double.Parse(Current.Text, CultureInfo.InvariantCulture)));
        Consume();
        return true;
    }

    private bool TryParseGroupedPrimaryExpr(out Expr expr)
    {
        if (Current.Kind != TokenKind.LeftParen)
        {
            expr = null!;
            return false;
        }

        Consume();
        if (IsKeyword(KwSelect) || IsKeyword("WITH"))
        {
            var subquery = ParseSelect();
            Expect(TokenKind.RightParen, ")");
            expr = new Expr.Subquery(subquery);
            return true;
        }

        expr = ParseExpr();
        Expect(TokenKind.RightParen, ")");
        return true;
    }

    private bool TryParseIdentifierPrimaryExpr(out Expr expr)
    {
        if (Current.Kind == TokenKind.Star)
        {
            Consume();
            expr = Expr.Wildcard.Instance;
            return true;
        }

        if (Current.Kind != TokenKind.Ident && Current.Kind != TokenKind.Keyword)
        {
            expr = null!;
            return false;
        }

        expr = ParseFunctionOrIdent();
        return true;
    }

    private Expr.Cast ParseCastExpr()
    {
        Consume();
        Expect(TokenKind.LeftParen, "(");
        var castExpr = ParseExpr();
        ExpectKeyword("AS");
        var castType = ParseDataType();
        Expect(TokenKind.RightParen, ")");
        return new Expr.Cast(castExpr, castType);
    }

    private Expr.Case ParseCaseExpr()
    {
        Consume();
        Expr? operand = null;
        if (!IsKeyword("WHEN")) operand = ParseExpr();
        var conditions = new List<(Expr, Expr)>();
        while (IsKeyword("WHEN"))
        {
            Consume();
            var cond = ParseExpr();
            ExpectKeyword("THEN");
            var res = ParseExpr();
            conditions.Add((cond, res));
        }
        Expr? elseRes = null;
        if (TryConsumeKeyword("ELSE")) elseRes = ParseExpr();
        ExpectKeyword("END");
        return new Expr.Case(operand, conditions, elseRes);
    }

    private Expr.Substring ParseSubstringExpr()
    {
        Consume();
        Expect(TokenKind.LeftParen, "(");
        var sub = ParseExpr();
        Expr? from = null, len = null;
        if (TryConsumeKeyword("FROM") || Current.Kind == TokenKind.Comma)
        {
            if (Current.Kind == TokenKind.Comma) Consume();
            from = ParseExpr();
        }
        if (TryConsumeKeyword("FOR") || Current.Kind == TokenKind.Comma)
        {
            if (Current.Kind == TokenKind.Comma) Consume();
            len = ParseExpr();
        }
        Expect(TokenKind.RightParen, ")");
        return new Expr.Substring(sub, from, len);
    }

    private Expr.Trim ParseTrimExpr()
    {
        Consume();
        Expect(TokenKind.LeftParen, "(");
        TrimWhereField? tw = null;
        if (TryConsumeKeyword("LEADING")) tw = TrimWhereField.Leading;
        else if (TryConsumeKeyword("TRAILING")) tw = TrimWhereField.Trailing;
        else if (TryConsumeKeyword("BOTH")) tw = TrimWhereField.Both;
        Expr? what = null;
        if (!IsKeyword("FROM") && Current.Kind != TokenKind.RightParen)
            what = ParseExpr();
        if (IsKeyword("FROM")) Consume();
        var trimExpr = ParseExpr();
        Expect(TokenKind.RightParen, ")");
        return new Expr.Trim(trimExpr, tw, what);
    }

    private Expr ParseFunctionOrIdent()
    {
        var name = Consume().Text;
        // Function call
        if (Current.Kind == TokenKind.LeftParen)
        {
            Consume();
            bool distinct = TryConsumeKeyword("DISTINCT");
            List<Expr> args = [];
            if (Current.Kind != TokenKind.RightParen)
            {
                if (Current.Kind == TokenKind.Star) { Consume(); args.Add(Expr.Wildcard.Instance); }
                else args = ParseExprList();
            }
            Expect(TokenKind.RightParen, ")");
            return new Expr.Function(name.ToUpperInvariant(), args, distinct);
        }
        // Qualified column: table.col
        if (Current.Kind == TokenKind.Dot)
        {
            Consume();
            if (Current.Kind == TokenKind.Star) { Consume(); return Expr.Wildcard.Instance; }
            var col = ParseIdent();
            return new Expr.Column(name, col);
        }
        // Unqualified column
        return new Expr.Column(null, name);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private List<Expr> ParseExprList()
    {
        var list = new List<Expr> { ParseExpr() };
        while (Current.Kind == TokenKind.Comma) { Consume(); list.Add(ParseExpr()); }
        return list;
    }

    private List<string> ParseIdentList()
    {
        var list = new List<string> { ParseIdent() };
        while (Current.Kind == TokenKind.Comma) { Consume(); list.Add(ParseIdent()); }
        return list;
    }

    private string ParseIdent()
    {
        if (Current.Kind != TokenKind.Ident && Current.Kind != TokenKind.Keyword)
            throw ParseError($"expected identifier, got '{Current.Text}'");
        return Consume().Text;
    }

    private (string? db, string? schema, string name) ParseQualifiedName()
    {
        var first = ParseIdent();
        if (Current.Kind != TokenKind.Dot) return (null, null, first);
        Consume();
        var second = ParseIdent();
        if (Current.Kind != TokenKind.Dot) return (null, first, second);
        Consume();
        var third = ParseIdent();
        return (first, second, third);
    }

    private ulong ParseUInt64()
    {
        if (Current.Kind != TokenKind.IntLiteral)
            throw ParseError($"expected integer, got '{Current.Text}'");
        var val = ulong.Parse(Current.Text, CultureInfo.InvariantCulture);
        Consume();
        return val;
    }
}

// ── Public SqlParser ──────────────────────────────────────────────────────────

/// <summary>
/// The AeternumDB SQL parser. Manual recursive-descent, AOT-compatible.
/// Transpiled from poc/rust/src/sql/parser.rs.
/// </summary>
public sealed class SqlParser
{
    private SqlParser() { }

    /// <summary>Parse one or more semicolon-separated SQL statements.</summary>
    public static List<Statement> Parse(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new SqlException(SqlErrorKind.EmptyInput, "empty SQL input");

        try
        {
            var tokens = new Tokenizer(sql).Tokenize();
            var parser = new Parser(tokens);
            var stmts = parser.ParseStatements();
            if (stmts.Count == 0)
                throw new SqlException(SqlErrorKind.EmptyInput, "empty SQL input");
            return stmts;
        }
        catch (SqlException) { throw; }
        catch (Exception ex)
        {
            throw new SqlException(SqlErrorKind.ParseError, ex.Message);
        }
    }

    /// <summary>Parse exactly one SQL statement.</summary>
    public static Statement ParseOne(string sql)
    {
        var stmts = Parse(sql);
        if (stmts.Count != 1)
            throw new SqlException(SqlErrorKind.ParseError, $"expected exactly one statement, got {stmts.Count}");
        return stmts[0];
    }

    /// <summary>Parse a SQL expression (not a full statement).</summary>
    public static Expr ParseExpr(string exprSql)
    {
        if (string.IsNullOrWhiteSpace(exprSql))
            throw new SqlException(SqlErrorKind.EmptyInput, "empty SQL expression");

        var wrapped = $"SELECT {exprSql}";
        var stmt = ParseOne(wrapped);
        if (stmt is Statement.Select sel && sel.Query.Columns.Count == 1)
        {
            return sel.Query.Columns[0] switch
            {
                SelectItem.ExprItem ei => ei.Expr,
                SelectItem.Wildcard => Expr.Wildcard.Instance,
                _ => throw new SqlException(SqlErrorKind.ParseError, "expected a single scalar expression"),
            };
        }
        throw new SqlException(SqlErrorKind.ParseError, "failed to parse expression");
    }
}
