namespace AeternumDB.PoC.Shared.Sql;

// ── DataType ──────────────────────────────────────────────────────────────────

/// <summary>SQL column data type. Mirrors Rust: enum DataType</summary>
public abstract class DataType
{
    private DataType() { }
    public sealed class Integer : DataType { public static readonly Integer Instance = new(); }
    public sealed class BigInt : DataType { public static readonly BigInt Instance = new(); }
    public sealed class Float : DataType { public static readonly Float Instance = new(); }
    public sealed class Decimal(int? precision, int? scale) : DataType
    {
        public int? Precision { get; } = precision;
        public int? Scale { get; } = scale;
    }
    public sealed class Varchar(int? length) : DataType { public int? Length { get; } = length; }
    public sealed class Text : DataType { public static readonly Text Instance = new(); }
    public sealed class Boolean : DataType { public static readonly Boolean Instance = new(); }
    public sealed class Blob : DataType { public static readonly Blob Instance = new(); }
    public sealed class Date : DataType { public static readonly Date Instance = new(); }
    public sealed class Timestamp : DataType { public static readonly Timestamp Instance = new(); }
    public sealed class Array(DataType elementType) : DataType { public DataType ElementType { get; } = elementType; }
    public sealed class Json : DataType { public static readonly Json Instance = new(); }
    public sealed class Custom(string name) : DataType { public string Name { get; } = name; }
}

// ── SQL AST expressions ───────────────────────────────────────────────────────

/// <summary>SQL expression. Mirrors Rust: enum Expr</summary>
public abstract class Expr
{
    private Expr() { }

    public sealed class Literal(object? value) : Expr { public object? Value { get; } = value; }
    public sealed class Column(string? table, string name) : Expr
    {
        public string? Table { get; } = table;
        public string Name { get; } = name;
    }
    public sealed class Wildcard : Expr { public static readonly Wildcard Instance = new(); }
    public sealed class Alias(Expr inner, string alias) : Expr
    {
        public Expr Inner { get; } = inner;
        public string AliasName { get; } = alias;
    }
    public sealed class BinaryOp(Expr left, string op, Expr right) : Expr
    {
        public Expr Left { get; } = left;
        public string Op { get; } = op;
        public Expr Right { get; } = right;
    }
    public sealed class UnaryOp(string op, Expr operand) : Expr
    {
        public string Op { get; } = op;
        public Expr Operand { get; } = operand;
    }
    public sealed class IsNull(Expr inner, bool negated) : Expr
    {
        public Expr Inner { get; } = inner;
        public bool Negated { get; } = negated;
    }
    public sealed class In(Expr inner, IReadOnlyList<Expr> list, bool negated) : Expr
    {
        public Expr Inner { get; } = inner;
        public IReadOnlyList<Expr> List { get; } = list;
        public bool Negated { get; } = negated;
    }
    public sealed class Between(Expr inner, Expr low, Expr high, bool negated) : Expr
    {
        public Expr Inner { get; } = inner;
        public Expr Low { get; } = low;
        public Expr High { get; } = high;
        public bool Negated { get; } = negated;
    }
    public sealed class Cast(Expr inner, DataType type) : Expr
    {
        public Expr Inner { get; } = inner;
        public DataType Type { get; } = type;
    }
    public sealed class Function(string name, IReadOnlyList<Expr> args) : Expr
    {
        public string Name { get; } = name;
        public IReadOnlyList<Expr> Args { get; } = args;
    }
    public sealed class Subquery(object plan) : Expr { public object Plan { get; } = plan; }
}

// ── SQL statement ─────────────────────────────────────────────────────────────

public enum JoinType { Inner, LeftOuter, RightOuter, FullOuter, Cross }
public enum OrderDirection { Asc, Desc }

public sealed class SelectItem
{
    public Expr Expression { get; init; } = Expr.Wildcard.Instance;
    public string? Alias { get; init; }
}

public sealed class TableReference
{
    public string? Schema { get; init; }
    public string Name { get; init; } = "";
    public string? Alias { get; init; }
}

public sealed class JoinClause
{
    public TableReference Table { get; init; } = new();
    public JoinType Type { get; init; }
    public Expr? Condition { get; init; }
}

public sealed class OrderByItem
{
    public Expr Expression { get; init; } = Expr.Wildcard.Instance;
    public OrderDirection Direction { get; init; } = OrderDirection.Asc;
}

/// <summary>SQL statement. Mirrors Rust: enum Statement</summary>
public abstract class SqlStatement
{
    private SqlStatement() { }

    public sealed class Select : SqlStatement
    {
        public IReadOnlyList<SelectItem> Columns { get; init; } = [];
        public TableReference? From { get; init; }
        public IReadOnlyList<JoinClause> Joins { get; init; } = [];
        public Expr? Where { get; init; }
        public IReadOnlyList<Expr> GroupBy { get; init; } = [];
        public Expr? Having { get; init; }
        public IReadOnlyList<OrderByItem> OrderBy { get; init; } = [];
        public int? Limit { get; init; }
        public int? Offset { get; init; }
    }

    public sealed class Insert : SqlStatement
    {
        public string Table { get; init; } = "";
        public IReadOnlyList<string> Columns { get; init; } = [];
        public IReadOnlyList<IReadOnlyList<Expr>> Values { get; init; } = [];
    }

    public sealed class Update : SqlStatement
    {
        public string Table { get; init; } = "";
        public IReadOnlyList<(string Column, Expr Value)> Assignments { get; init; } = [];
        public Expr? Where { get; init; }
    }

    public sealed class Delete : SqlStatement
    {
        public string Table { get; init; } = "";
        public Expr? Where { get; init; }
    }
}
