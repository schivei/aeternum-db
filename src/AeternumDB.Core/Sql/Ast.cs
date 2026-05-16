namespace AeternumDB.Core.Sql;

using AeternumDB.Core.Sql.Ast;

// ── SQL literal Value ─────────────────────────────────────────────────────────

/// <summary>A literal SQL value used in AST expressions.</summary>
public abstract class SqlValue
{
    private SqlValue() { }

    public sealed class Integer(long value) : SqlValue
    {
        public long Value { get; } = value;

        public override string ToString() => Value.ToString();
    }

    public sealed class Float(double value) : SqlValue
    {
        public double Value { get; } = value;

        public override string ToString() => Value.ToString("G17");
    }

    public sealed class SqlString(string value) : SqlValue
    {
        public string Value { get; } = value;

        public override string ToString() => $"'{Value}'";
    }

    public sealed class Boolean(bool value) : SqlValue
    {
        public bool Value { get; } = value;

        public override string ToString() => Value ? "TRUE" : "FALSE";
    }

    public sealed class Null : SqlValue
    {
        public static readonly Null Instance = new();

        private Null() { }

        public override string ToString() => "NULL";
    }
}

// ── Enum variant + TypeDefinition ─────────────────────────────────────────────

public sealed class EnumVariant
{
    public string Name { get; }
    public bool IsNone { get; }

    public EnumVariant(string name, bool isNone = false)
    {
        Name = name;
        IsNone = isNone;
    }
}

public sealed class TermsDirective
{
    public string Name { get; }
    public TermsDirectiveKind Kind { get; }
    public IReadOnlyList<string> EnumVariants { get; }

    public TermsDirective(
        string name,
        TermsDirectiveKind kind,
        IReadOnlyList<string>? enumVariants = null
    )
    {
        Name = name;
        Kind = kind;
        EnumVariants = enumVariants ?? [];
    }
}

public sealed class TextDirective(string defaultLocale)
{
    public string DefaultLocale { get; } = defaultLocale;
}

public abstract class TypeDefinition
{
    private TypeDefinition() { }

    public sealed class Enum(bool flag, IReadOnlyList<EnumVariant> variants) : TypeDefinition
    {
        public bool Flag { get; } = flag;
        public IReadOnlyList<EnumVariant> Variants { get; } = variants;
    }

    public sealed class Composite(IReadOnlyList<CompositeField> fields) : TypeDefinition
    {
        public IReadOnlyList<CompositeField> Fields { get; } = fields;
    }
}

public sealed class CompositeField(string name, DataType dataType, bool notNull)
{
    public string Name { get; } = name;
    public DataType DataType { get; } = dataType;
    public bool NotNull { get; } = notNull;
}

// ── Expressions ───────────────────────────────────────────────────────────────

public abstract class Expr
{
    private Expr() { }

    public sealed class Literal(SqlValue value) : Expr
    {
        public SqlValue Value { get; } = value;
    }

    public sealed class Column(string? table, string name) : Expr
    {
        public string? Table { get; } = table;
        public string Name { get; } = name;
    }

    public sealed class Wildcard : Expr
    {
        public static readonly Wildcard Instance = new();

        private Wildcard() { }
    }

    public sealed class BinaryOp(Expr left, BinaryOperator op, Expr right) : Expr
    {
        public Expr Left { get; } = left;
        public BinaryOperator Op { get; } = op;
        public Expr Right { get; } = right;
    }

    public sealed class UnaryOp(UnaryOperator op, Expr expr) : Expr
    {
        public UnaryOperator Op { get; } = op;
        public Expr Inner { get; } = expr;
    }

    public sealed class Function(string name, IReadOnlyList<Expr> args, bool distinct) : Expr
    {
        public string Name { get; } = name;
        public IReadOnlyList<Expr> Args { get; } = args;
        public bool Distinct { get; } = distinct;
    }

    public sealed class IsNull(Expr expr, bool negated) : Expr
    {
        public Expr Inner { get; } = expr;
        public bool Negated { get; } = negated;
    }

    public sealed class Between(Expr expr, Expr low, Expr high, bool negated) : Expr
    {
        public Expr Inner { get; } = expr;
        public Expr Low { get; } = low;
        public Expr High { get; } = high;
        public bool Negated { get; } = negated;
    }

    public sealed class InList(Expr expr, IReadOnlyList<Expr> list, bool negated) : Expr
    {
        public Expr Inner { get; } = expr;
        public IReadOnlyList<Expr> List { get; } = list;
        public bool Negated { get; } = negated;
    }

    public sealed class InSubquery(Expr expr, SelectStatement subquery, bool negated) : Expr
    {
        public Expr Inner { get; } = expr;
        public new SelectStatement Subquery { get; } = subquery;
        public bool Negated { get; } = negated;
    }

    public sealed class ArrayOp(
        Expr expr,
        BinaryOperator op,
        ArrayQuantifier quantifier,
        Expr right
    ) : Expr
    {
        public Expr Inner { get; } = expr;
        public BinaryOperator Op { get; } = op;
        public ArrayQuantifier Quantifier { get; } = quantifier;
        public Expr Right { get; } = right;
    }

    public sealed class Subquery(SelectStatement query) : Expr
    {
        public SelectStatement Query { get; } = query;
    }

    public sealed class Cast(Expr expr, DataType dataType) : Expr
    {
        public Expr Inner { get; } = expr;
        public DataType DataType { get; } = dataType;
    }

    public sealed class Case(
        Expr? operand,
        IReadOnlyList<(Expr Condition, Expr Result)> conditions,
        Expr? elseResult
    ) : Expr
    {
        public Expr? Operand { get; } = operand;
        public IReadOnlyList<(Expr Condition, Expr Result)> Conditions { get; } = conditions;
        public Expr? ElseResult { get; } = elseResult;
    }

    public sealed class Substring(Expr expr, Expr? fromPos, Expr? len) : Expr
    {
        public Expr Inner { get; } = expr;
        public Expr? FromPos { get; } = fromPos;
        public Expr? Len { get; } = len;
    }

    public sealed class Position(Expr substr, Expr inExpr) : Expr
    {
        public Expr Substr { get; } = substr;
        public Expr InExpr { get; } = inExpr;
    }

    public sealed class Trim(Expr expr, TrimWhereField? trimWhere, Expr? trimWhat) : Expr
    {
        public Expr Inner { get; } = expr;
        public TrimWhereField? TrimWhere { get; } = trimWhere;
        public Expr? TrimWhat { get; } = trimWhat;
    }

    public sealed class Overlay(Expr expr, Expr overlayWhat, Expr fromPos, Expr? forLen) : Expr
    {
        public Expr Inner { get; } = expr;
        public Expr OverlayWhat { get; } = overlayWhat;
        public Expr FromPos { get; } = fromPos;
        public Expr? ForLen { get; } = forLen;
    }

    public sealed class MatchAgainst(
        IReadOnlyList<string> columns,
        Expr matchValue,
        TextSearchModifier? modifier
    ) : Expr
    {
        public IReadOnlyList<string> Columns { get; } = columns;
        public Expr MatchValue { get; } = matchValue;
        public TextSearchModifier? Modifier { get; } = modifier;
    }
}

// ── SelectItem discriminated union ────────────────────────────────────────────

public abstract class SelectItem
{
    private SelectItem() { }

    public sealed class Wildcard : SelectItem
    {
        public static readonly Wildcard Instance = new();

        private Wildcard() { }
    }

    public sealed class QualifiedWildcard(string table) : SelectItem
    {
        public string Table { get; } = table;
    }

    public sealed class ExprItem(Expr expr, string? alias) : SelectItem
    {
        public Expr Expr { get; } = expr;
        public string? Alias { get; } = alias;
    }

    public sealed class Expand(Expr expr, string? alias) : SelectItem
    {
        public Expr Expr { get; } = expr;
        public string? Alias { get; } = alias;
    }
}

// ── VIEW AS item ──────────────────────────────────────────────────────────────

public sealed class ViewAsItem(Expr expr, string alias)
{
    public Expr Expr { get; } = expr;
    public string Alias { get; } = alias;
}

// ── TableReference discriminated union ────────────────────────────────────────

public abstract class TableReference
{
    private TableReference() { }

    public sealed class Named(string? database, string? schema, string name, string? alias)
        : TableReference
    {
        public string? Database { get; } = database;
        public string? Schema { get; } = schema;
        public string Name { get; } = name;
        public string? Alias { get; } = alias;
    }

    public sealed class Subquery(SelectStatement query, string alias) : TableReference
    {
        public SelectStatement Query { get; } = query;
        public string Alias { get; } = alias;
    }

    public sealed class Join(
        TableReference left,
        TableReference right,
        JoinType joinType,
        Expr? filterBy
    ) : TableReference
    {
        public TableReference Left { get; } = left;
        public TableReference Right { get; } = right;
        public JoinType JoinType { get; } = joinType;
        public Expr? FilterBy { get; } = filterBy;
    }
}

// ── ORDER BY ──────────────────────────────────────────────────────────────────

public sealed class OrderByExpr(Expr expr, bool ascending)
{
    public Expr Expr { get; } = expr;
    public bool Ascending { get; } = ascending;
}

// ── Common Table Expression ───────────────────────────────────────────────────

public sealed class CommonTableExpr(
    string name,
    IReadOnlyList<string> columns,
    SelectStatement query
)
{
    public string Name { get; } = name;
    public IReadOnlyList<string> Columns { get; } = columns;
    public SelectStatement Query { get; } = query;
}

// ── DML statement bodies ──────────────────────────────────────────────────────

public sealed class SelectStatement
{
    public IReadOnlyList<CommonTableExpr> With { get; init; } = [];
    public IReadOnlyList<SelectItem> Columns { get; init; } = [];
    public TableReference? From { get; init; }
    public Expr? WhereClause { get; init; }
    public IReadOnlyList<Expr> GroupBy { get; init; } = [];
    public Expr? Having { get; init; }
    public IReadOnlyList<OrderByExpr> OrderBy { get; init; } = [];
    public ulong? Limit { get; init; }
    public ulong? Offset { get; init; }
    public bool Distinct { get; init; }
    public IReadOnlyList<ViewAsItem>? ViewAs { get; init; }
}

public sealed class InsertStatement(
    string table,
    IReadOnlyList<string> columns,
    IReadOnlyList<IReadOnlyList<Expr>> values
)
{
    public string Table { get; } = table;
    public IReadOnlyList<string> Columns { get; } = columns;
    public IReadOnlyList<IReadOnlyList<Expr>> Values { get; } = values;
}

public sealed class UpdateStatement(
    string table,
    IReadOnlyList<(string Column, Expr Value)> assignments,
    Expr? whereClause
)
{
    public string Table { get; } = table;
    public IReadOnlyList<(string Column, Expr Value)> Assignments { get; } = assignments;
    public Expr? WhereClause { get; } = whereClause;
}

public sealed class DeleteStatement(string table, Expr? whereClause)
{
    public string Table { get; } = table;
    public Expr? WhereClause { get; } = whereClause;
}

// ── DDL statement bodies ──────────────────────────────────────────────────────

public sealed class ColumnDef
{
    public string Name { get; init; } = "";
    public DataType DataType { get; init; } = new DataType.Other("UNKNOWN");
    public bool Nullable { get; init; } = true;
    public bool PrimaryKey { get; init; }
    public bool Unique { get; init; }
    public Expr? Default { get; init; }
    public bool AutoIncrement { get; init; }
    public ulong? MinLength { get; init; }
    public ulong? MaxLength { get; init; }
    public bool RequiresDistinctReferences { get; init; }
    public Expr? Check { get; init; }
    public TextDirective? TextDirective { get; init; }
    public IReadOnlyList<TermsDirective> TermsDirectives { get; init; } = [];
    public ReferentialAction? OnUpdate { get; init; }
    public ReferentialAction? OnDelete { get; init; }
}

public abstract class TableConstraint
{
    private TableConstraint() { }

    public sealed class PrimaryKey(string? name, IReadOnlyList<string> columns) : TableConstraint
    {
        public string? Name { get; } = name;
        public IReadOnlyList<string> Columns { get; } = columns;
    }

    public sealed class Unique(string? name, IReadOnlyList<string> columns) : TableConstraint
    {
        public string? Name { get; } = name;
        public IReadOnlyList<string> Columns { get; } = columns;
    }

    public sealed class Check(string? name, Expr expr) : TableConstraint
    {
        public string? Name { get; } = name;
        public Expr Expr { get; } = expr;
    }
}

public sealed class IndexColumn(string name, bool? ascending)
{
    public string Name { get; } = name;
    public bool? Ascending { get; } = ascending;
}

public sealed class CreateTableStatement
{
    public string? Database { get; init; }
    public string? Schema { get; init; }
    public string Table { get; init; } = "";
    public IReadOnlyList<ColumnDef> Columns { get; init; } = [];
    public bool IfNotExists { get; init; }
    public bool Temporary { get; init; }
    public IReadOnlyList<string> Inherits { get; init; } = [];
    public OnCommitBehavior? OnCommit { get; init; }
    public IReadOnlyList<TableConstraint> Constraints { get; init; } = [];
    public bool Versioned { get; init; }
    public bool Flat { get; init; }
}

public sealed class DropTableStatement(IReadOnlyList<string> tables, bool ifExists)
{
    public IReadOnlyList<string> Tables { get; } = tables;
    public bool IfExists { get; } = ifExists;
}

public abstract class AlterTableOperation
{
    private AlterTableOperation() { }

    public sealed class AddColumn(ColumnDef column) : AlterTableOperation
    {
        public ColumnDef Column { get; } = column;
    }

    public sealed class DropColumn(string name, bool ifExists) : AlterTableOperation
    {
        public string Name { get; } = name;
        public bool IfExists { get; } = ifExists;
    }

    public sealed class RenameColumn(string oldName, string newName) : AlterTableOperation
    {
        public string OldName { get; } = oldName;
        public string NewName { get; } = newName;
    }

    public sealed class RenameTable(string newName) : AlterTableOperation
    {
        public string NewName { get; } = newName;
    }
}

public sealed class AlterTableStatement(string table, IReadOnlyList<AlterTableOperation> operations)
{
    public string Table { get; } = table;
    public IReadOnlyList<AlterTableOperation> Operations { get; } = operations;
}

public sealed class CreateIndexStatement
{
    public string? Name { get; init; }
    public string Table { get; init; } = "";
    public IReadOnlyList<IndexColumn> Columns { get; init; } = [];
    public bool Unique { get; init; }
    public bool IfNotExists { get; init; }
    public IndexType IndexType { get; init; } = IndexType.BTree.Instance;
}

public sealed class DropIndexStatement(IReadOnlyList<string> names, bool ifExists)
{
    public IReadOnlyList<string> Names { get; } = names;
    public bool IfExists { get; } = ifExists;
}

// ── DCL scaffolding ────────────────────────────────────────────────────────────

public sealed class GrantStatement(
    IReadOnlyList<string> privileges,
    IReadOnlyList<string> columns,
    string on,
    IReadOnlyList<string> to
)
{
    public IReadOnlyList<string> Privileges { get; } = privileges;
    public IReadOnlyList<string> Columns { get; } = columns;
    public string On { get; } = on;
    public IReadOnlyList<string> To { get; } = to;
}

public sealed class RevokeStatement(
    IReadOnlyList<string> privileges,
    IReadOnlyList<string> columns,
    string on,
    IReadOnlyList<string> from
)
{
    public IReadOnlyList<string> Privileges { get; } = privileges;
    public IReadOnlyList<string> Columns { get; } = columns;
    public string On { get; } = on;
    public IReadOnlyList<string> From { get; } = from;
}

// ── Materialized View ─────────────────────────────────────────────────────────

public sealed class CreateMaterializedViewStatement(
    string name,
    SelectStatement query,
    bool ifNotExists,
    bool orReplace
)
{
    public string Name { get; } = name;
    public SelectStatement Query { get; } = query;
    public bool IfNotExists { get; } = ifNotExists;
    public bool OrReplace { get; } = orReplace;
}

// ── Transaction control ────────────────────────────────────────────────────────

public abstract class CommitScope
{
    private CommitScope() { }

    public sealed class Current : CommitScope
    {
        public static readonly Current Instance = new();

        private Current() { }
    }

    public sealed class Named(string name) : CommitScope
    {
        public string Name { get; } = name;
    }

    public sealed class All : CommitScope
    {
        public static readonly All Instance = new();

        private All() { }
    }
}

public abstract class RollbackScope
{
    private RollbackScope() { }

    public sealed class Current : RollbackScope
    {
        public static readonly Current Instance = new();

        private Current() { }
    }

    public sealed class ToSavepoint(string name) : RollbackScope
    {
        public string Name { get; } = name;
    }

    public sealed class Named(string name) : RollbackScope
    {
        public string Name { get; } = name;
    }

    public sealed class All : RollbackScope
    {
        public static readonly All Instance = new();

        private All() { }
    }
}

public sealed class BeginTransactionStatement
{
    public string? Name { get; init; }
    public IsolationLevel? IsolationLevel { get; init; }
    public bool ReadOnly { get; init; }
}

public sealed class CommitStatement(CommitScope scope, bool chain)
{
    public CommitScope Scope { get; } = scope;
    public bool Chain { get; } = chain;
}

public sealed class RollbackStatement(RollbackScope scope, bool chain)
{
    public RollbackScope Scope { get; } = scope;
    public bool Chain { get; } = chain;
}

public sealed class SavepointStatement(string name)
{
    public string Name { get; } = name;
}

public sealed class ReleaseSavepointStatement(string name)
{
    public string Name { get; } = name;
}

// ── User management scaffolding ────────────────────────────────────────────────

public sealed class CreateUserStatement(string name, string? password, IReadOnlyList<string> roles)
{
    public string Name { get; } = name;
    public string? Password { get; } = password;
    public IReadOnlyList<string> Roles { get; } = roles;
}

public sealed class DropUserStatement(IReadOnlyList<string> names, bool ifExists)
{
    public IReadOnlyList<string> Names { get; } = names;
    public bool IfExists { get; } = ifExists;
}

// ── Enum DDL ──────────────────────────────────────────────────────────────────

public sealed class CreateEnumStatement(
    string name,
    bool flag,
    IReadOnlyList<EnumVariant> variants,
    bool ifNotExists
)
{
    public string Name { get; } = name;
    public bool Flag { get; } = flag;
    public IReadOnlyList<EnumVariant> Variants { get; } = variants;
    public bool IfNotExists { get; } = ifNotExists;
}

public sealed class DropEnumStatement(string name, bool ifExists)
{
    public string Name { get; } = name;
    public bool IfExists { get; } = ifExists;
}

// ── Type DDL ──────────────────────────────────────────────────────────────────

public sealed class CreateTypeStatement(string name, TypeDefinition definition)
{
    public string Name { get; } = name;
    public TypeDefinition Definition { get; } = definition;
}

public sealed class DropTypeStatement(string name, bool ifExists)
{
    public string Name { get; } = name;
    public bool IfExists { get; } = ifExists;
}

// ── Database / Schema DDL ─────────────────────────────────────────────────────

public sealed class CreateDatabaseStatement(string name, bool ifNotExists)
{
    public string Name { get; } = name;
    public bool IfNotExists { get; } = ifNotExists;
}

public sealed class DropDatabaseStatement(string name, bool ifExists)
{
    public string Name { get; } = name;
    public bool IfExists { get; } = ifExists;
}

public sealed class UseDatabaseStatement(string name)
{
    public string Name { get; } = name;
}

public sealed class CreateSchemaStatement(string? database, string name, bool ifNotExists)
{
    public string? Database { get; } = database;
    public string Name { get; } = name;
    public bool IfNotExists { get; } = ifNotExists;
}

public sealed class DropSchemaStatement(string? database, string name, bool ifExists)
{
    public string? Database { get; } = database;
    public string Name { get; } = name;
    public bool IfExists { get; } = ifExists;
}

// ── Top-level Statement discriminated union ───────────────────────────────────

public abstract class Statement
{
    private Statement() { }

    public sealed class Select(SelectStatement query) : Statement
    {
        public SelectStatement Query { get; } = query;
    }

    public sealed class Insert(InsertStatement stmt) : Statement
    {
        public InsertStatement Stmt { get; } = stmt;
    }

    public sealed class Update(UpdateStatement stmt) : Statement
    {
        public UpdateStatement Stmt { get; } = stmt;
    }

    public sealed class Delete(DeleteStatement stmt) : Statement
    {
        public DeleteStatement Stmt { get; } = stmt;
    }

    public sealed class CreateTable(CreateTableStatement stmt) : Statement
    {
        public CreateTableStatement Stmt { get; } = stmt;
    }

    public sealed class DropTable(DropTableStatement stmt) : Statement
    {
        public DropTableStatement Stmt { get; } = stmt;
    }

    public sealed class AlterTable(AlterTableStatement stmt) : Statement
    {
        public AlterTableStatement Stmt { get; } = stmt;
    }

    public sealed class Grant(GrantStatement stmt) : Statement
    {
        public GrantStatement Stmt { get; } = stmt;
    }

    public sealed class Revoke(RevokeStatement stmt) : Statement
    {
        public RevokeStatement Stmt { get; } = stmt;
    }

    public sealed class CreateMaterializedView(CreateMaterializedViewStatement stmt) : Statement
    {
        public CreateMaterializedViewStatement Stmt { get; } = stmt;
    }

    public sealed class BeginTransaction(BeginTransactionStatement stmt) : Statement
    {
        public BeginTransactionStatement Stmt { get; } = stmt;
    }

    public sealed class Commit(CommitStatement stmt) : Statement
    {
        public CommitStatement Stmt { get; } = stmt;
    }

    public sealed class Rollback(RollbackStatement stmt) : Statement
    {
        public RollbackStatement Stmt { get; } = stmt;
    }

    public sealed class Savepoint(SavepointStatement stmt) : Statement
    {
        public SavepointStatement Stmt { get; } = stmt;
    }

    public sealed class ReleaseSavepoint(ReleaseSavepointStatement stmt) : Statement
    {
        public ReleaseSavepointStatement Stmt { get; } = stmt;
    }

    public sealed class CreateIndex(CreateIndexStatement stmt) : Statement
    {
        public CreateIndexStatement Stmt { get; } = stmt;
    }

    public sealed class DropIndex(DropIndexStatement stmt) : Statement
    {
        public DropIndexStatement Stmt { get; } = stmt;
    }

    public sealed class CreateUser(CreateUserStatement stmt) : Statement
    {
        public CreateUserStatement Stmt { get; } = stmt;
    }

    public sealed class DropUser(DropUserStatement stmt) : Statement
    {
        public DropUserStatement Stmt { get; } = stmt;
    }

    public sealed class CreateEnum(CreateEnumStatement stmt) : Statement
    {
        public CreateEnumStatement Stmt { get; } = stmt;
    }

    public sealed class DropEnum(DropEnumStatement stmt) : Statement
    {
        public DropEnumStatement Stmt { get; } = stmt;
    }

    public sealed class CreateType(CreateTypeStatement stmt) : Statement
    {
        public CreateTypeStatement Stmt { get; } = stmt;
    }

    public sealed class DropType(DropTypeStatement stmt) : Statement
    {
        public DropTypeStatement Stmt { get; } = stmt;
    }

    public sealed class CreateDatabase(CreateDatabaseStatement stmt) : Statement
    {
        public CreateDatabaseStatement Stmt { get; } = stmt;
    }

    public sealed class DropDatabase(DropDatabaseStatement stmt) : Statement
    {
        public DropDatabaseStatement Stmt { get; } = stmt;
    }

    public sealed class UseDatabase(UseDatabaseStatement stmt) : Statement
    {
        public UseDatabaseStatement Stmt { get; } = stmt;
    }

    public sealed class CreateSchema(CreateSchemaStatement stmt) : Statement
    {
        public CreateSchemaStatement Stmt { get; } = stmt;
    }

    public sealed class DropSchema(DropSchemaStatement stmt) : Statement
    {
        public DropSchemaStatement Stmt { get; } = stmt;
    }
}
