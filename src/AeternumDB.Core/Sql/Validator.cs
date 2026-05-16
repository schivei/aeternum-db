// Semantic validator for AeternumDB SQL.
//
// Transpiled from poc/rust/src/sql/validator.rs.
// AOT-compatible — no reflection.

using System.Collections.Generic;
using AeternumDB.Core.Errors;

namespace AeternumDB.Core.Sql;

// ── Catalog schemas ────────────────────────────────────────────────────────────

/// <summary>Metadata for a single column in the catalog.</summary>
public sealed class ColumnSchema
{
    public string Name { get; }
    public DataType DataType { get; }
    public bool Nullable { get; }
    public string? UserDefinedTypeName { get; }

    public ColumnSchema(string name, DataType dataType, bool nullable = true)
    {
        Name = name;
        DataType = dataType;
        Nullable = nullable;
        UserDefinedTypeName = dataType is DataType.EnumRef enumRef ? enumRef.Name : null;
    }
}

/// <summary>Metadata for a single table in the catalog.</summary>
public sealed class TableSchema
{
    public string Name { get; }
    public IReadOnlyList<ColumnSchema> Columns { get; }
    public int SchemaVersion { get; }
    public DateTimeOffset CreatedAt { get; }
    public DateTimeOffset ModifiedAt { get; }
    public long RowCount { get; }

    public TableSchema(
        string name,
        IReadOnlyList<ColumnSchema> columns,
        int schemaVersion = 1,
        DateTimeOffset? createdAt = null,
        DateTimeOffset? modifiedAt = null,
        long rowCount = 0)
    {
        var now = DateTimeOffset.UtcNow;
        Name = name;
        Columns = columns;
        SchemaVersion = schemaVersion;
        CreatedAt = createdAt ?? now;
        ModifiedAt = modifiedAt ?? CreatedAt;
        RowCount = rowCount;
    }

    /// <summary>Look up a column by name (case-insensitive).</summary>
    public ColumnSchema? GetColumn(string name) =>
        Columns.FirstOrDefault(col => string.Equals(col.Name, name, StringComparison.OrdinalIgnoreCase));
}

// ── User-defined type catalog ─────────────────────────────────────────────────

/// <summary>The kind of a user-defined type.</summary>
public abstract class UserTypeKind
{
    private UserTypeKind() { }

    public sealed class Enum(bool flag, IReadOnlyList<EnumVariant> variants, IReadOnlyList<ulong> resolvedValues) : UserTypeKind
    {
        public bool Flag { get; } = flag;
        public IReadOnlyList<EnumVariant> Variants { get; } = variants;
        public IReadOnlyList<ulong> ResolvedValues { get; } = resolvedValues;
    }

    public sealed class Composite(IReadOnlyList<(string Name, DataType Type)> fields) : UserTypeKind
    {
        public IReadOnlyList<(string Name, DataType Type)> Fields { get; } = fields;
    }
}

/// <summary>A named user-defined type stored in the catalog.</summary>
public sealed class UserTypeSchema
{
    public string Name { get; }
    public UserTypeKind Kind { get; }

    public UserTypeSchema(string name, UserTypeKind kind)
    {
        Name = name;
        Kind = kind;
    }
}

// ── Catalog ───────────────────────────────────────────────────────────────────

/// <summary>Catalog with schema metadata, DDL/index mutations, and optional persistence for semantic validation.</summary>
public sealed partial class Catalog
{
    private readonly Dictionary<string, TableSchema> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, UserTypeSchema> _types = new(StringComparer.OrdinalIgnoreCase);

    // ── Table management ──────────────────────────────────────────────────────

    /// <summary>Register a table in the catalog.</summary>
    public void AddTable(TableSchema schema)
    {
        lock (SyncRoot)
        {
            _tables[schema.Name.ToLowerInvariant()] = schema;
            PersistIfConfiguredUnsafe();
        }
    }

    /// <summary>Remove a table from the catalog.</summary>
    public void RemoveTable(string name)
    {
        lock (SyncRoot)
        {
            var removed = _tables.Remove(name.ToLowerInvariant());
            if (!removed)
                return;

            RemoveIndexesForTableUnsafe(name);
            PersistIfConfiguredUnsafe();
        }
    }

    /// <summary>Check whether a table exists.</summary>
    public bool TableExists(string name)
    {
        lock (SyncRoot)
            return _tables.ContainsKey(name.ToLowerInvariant());
    }

    /// <summary>Retrieve a table schema.</summary>
    public TableSchema? GetTable(string name)
    {
        lock (SyncRoot)
        {
            _tables.TryGetValue(name.ToLowerInvariant(), out var schema);
            return schema;
        }
    }

    // ── User-defined type management ──────────────────────────────────────────

    /// <summary>Register a user-defined type.</summary>
    public void AddType(UserTypeSchema schema)
    {
        lock (SyncRoot)
        {
            _types[schema.Name.ToLowerInvariant()] = schema;
            PersistIfConfiguredUnsafe();
        }
    }

    /// <summary>Retrieve a user-defined type by name (case-insensitive).</summary>
    public UserTypeSchema? GetType(string name)
    {
        lock (SyncRoot)
        {
            _types.TryGetValue(name.ToLowerInvariant(), out var schema);
            return schema;
        }
    }

    /// <summary>Check whether a user-defined type exists.</summary>
    public bool TypeExists(string name)
    {
        lock (SyncRoot)
            return _types.ContainsKey(name.ToLowerInvariant());
    }

    /// <summary>
    /// Remove a user-defined type. Throws <see cref="PlannerException"/> if the type is still in use.
    /// </summary>
    public void RemoveType(string name)
    {
        lock (SyncRoot)
        {
            if (IsTypeInUseUnsafe(name))
                throw new PlannerException(PlannerErrorKind.CatalogError,
                    $"cannot drop type '{name}': it is still referenced by one or more columns");
            _types.Remove(name.ToLowerInvariant());
            PersistIfConfiguredUnsafe();
        }
    }

    /// <summary>Returns true if any column in any table references this type via EnumRef.</summary>
    public bool IsTypeInUse(string name)
    {
        lock (SyncRoot)
            return IsTypeInUseUnsafe(name);
    }

    private bool IsTypeInUseUnsafe(string name)
    {
        return _tables.Values
            .SelectMany(table => table.Columns)
            .Where(col => col.UserDefinedTypeName is not null)
            .Any(col => string.Equals(col.UserDefinedTypeName, name, StringComparison.OrdinalIgnoreCase));
    }
}

// ── Validation errors ─────────────────────────────────────────────────────────

/// <summary>Errors produced by semantic validation.</summary>
public abstract class ValidationException : Exception
{
    protected ValidationException(string message) : base(message) { }

    public sealed class TableNotFoundException(string table)
        : ValidationException($"table '{table}' does not exist")
    {
        public string Table { get; } = table;
    }

    public sealed class ColumnNotFoundException(string table, string column)
        : ValidationException($"column '{column}' does not exist in table '{table}'")
    {
        public string Table { get; } = table;
        public string Column { get; } = column;
    }

    public sealed class TypeMismatchException(DataType expected, DataType found, string context)
        : ValidationException($"type mismatch in {context}: expected {expected}, found {found}")
    {
        public DataType Expected { get; } = expected;
        public DataType Found { get; } = found;
        public string Context { get; } = context;
    }

    public sealed class InvalidAggregateUsageException(string message)
        : ValidationException($"invalid aggregate usage: {message}") { }

    public sealed class NullConstraintViolationException(string table, string column)
        : ValidationException($"null constraint violation: column '{column}' in table '{table}' is NOT NULL")
    {
        public string Table { get; } = table;
        public string Column { get; } = column;
    }

    public sealed class ConstraintViolationException(string message)
        : ValidationException($"constraint violation: {message}") { }

    public sealed class TypeNotFoundException(string name)
        : ValidationException($"user-defined type '{name}' does not exist") { }

    public sealed class TypeInUseException(string name)
        : ValidationException($"cannot drop type '{name}': it is still referenced by one or more columns") { }

    public sealed class InvalidEnumValueException(string column, string value)
        : ValidationException($"invalid enum value '{value}' for column '{column}'")
    {
        public string Column { get; } = column;
        public string Value { get; } = value;
    }

    public sealed class NoActiveTransactionException()
        : ValidationException("no active transaction") { }

    public sealed class TransactionNameConflictException(string name)
        : ValidationException($"transaction name '{name}' is already in use in this session") { }

    public sealed class TransactionNotFoundException(string name)
        : ValidationException($"transaction '{name}' is not active in the current session") { }

    public sealed class TransactionNestingViolationException(string target, string blocking)
        : ValidationException($"cannot commit or rollback transaction '{target}': nested transaction '{blocking}' is still open")
    {
        public string Target { get; } = target;
        public string Blocking { get; } = blocking;
    }

    public sealed class ViewAsAggregateNotAllowedException(string func)
        : ValidationException($"aggregate function '{func}' is not allowed in a VIEW AS clause") { }

    public sealed class ViewAsSubqueryNotAllowedException()
        : ValidationException("sub-selects are not allowed in a VIEW AS clause") { }
}

// ── SqlValidator ──────────────────────────────────────────────────────────────

/// <summary>
/// Semantic validator for internal SQL AST nodes.
/// Transpiled from poc/rust/src/sql/validator.rs.
/// </summary>
public sealed class SqlValidator
{
    private readonly Catalog _catalog;

    public SqlValidator(Catalog catalog) { _catalog = catalog; }

    /// <summary>Validate a top-level Statement.</summary>
    public void Validate(Statement stmt)
    {
        switch (stmt)
        {
            case Statement.Select s: ValidateSelect(s.Query); break;
            case Statement.Insert s: ValidateInsert(s.Stmt); break;
            case Statement.Update s: ValidateUpdate(s.Stmt); break;
            case Statement.Delete s: ValidateDelete(s.Stmt); break;
            case Statement.CreateTable s: ValidateCreateTable(s.Stmt); break;
            case Statement.AlterTable s: ValidateAlterTable(s.Stmt); break;
            case Statement.CreateEnum s: ValidateCreateEnum(s.Stmt); break;
            case Statement.DropEnum s: ValidateDropEnum(s.Stmt); break;
            // Everything else passes structural validation
            default: break;
        }
    }

    /// <summary>Validate a sequence of statements, tracking the transaction stack.</summary>
    public void ValidateSequence(IReadOnlyList<Statement> stmts)
    {
        var stack = new List<string?>();
        foreach (var stmt in stmts)
        {
            Validate(stmt);
            switch (stmt)
            {
                case Statement.BeginTransaction bt:
                    SeqBegin(stack, bt.Stmt.Name);
                    break;
                case Statement.Commit c:
                    SeqCommit(stack, c.Stmt.Scope);
                    break;
                case Statement.Rollback r:
                    SeqRollback(stack, r.Stmt.Scope);
                    break;
                case Statement.Savepoint or Statement.ReleaseSavepoint when stack.Count == 0:
                    throw new ValidationException.NoActiveTransactionException();
                default:
                    break;
            }
        }
    }

    // ── Private validation helpers ─────────────────────────────────────────────

    private void ValidateSelect(SelectStatement sel)
    {
        var aliases = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableNames = new List<string>();

        if (sel.From != null)
            ValidateTableReference(sel.From, aliases, tableNames);

        string? defaultTable = tableNames.Count == 1 ? tableNames[0] : null;

        foreach (var col in sel.Columns)
            ValidateSelectItem(col, defaultTable, aliases);

        if (sel.WhereClause != null)
        {
            ValidateSelectExpr(sel.WhereClause, defaultTable, aliases);
            CheckNoAggregateInWhere(sel.WhereClause);
        }
        foreach (var g in sel.GroupBy)
            ValidateSelectExpr(g, defaultTable, aliases);
        if (sel.Having != null)
            ValidateSelectExpr(sel.Having, defaultTable, aliases);
        foreach (var o in sel.OrderBy)
            ValidateSelectExpr(o.Expr, defaultTable, aliases);
        if (sel.ViewAs != null)
            foreach (var va in sel.ViewAs)
                ValidateViewAsItem(va);
    }

    private void ValidateSelectItem(SelectItem item, string? defaultTable, Dictionary<string, string> aliases)
    {
        switch (item)
        {
            case SelectItem.Wildcard: break;
            case SelectItem.QualifiedWildcard qw:
                RequireTable(aliases.TryGetValue(qw.Table, out var rt) ? rt : qw.Table);
                break;
            case SelectItem.ExprItem ei:
                ValidateSelectExpr(ei.Expr, defaultTable, aliases);
                break;
            case SelectItem.Expand ex:
                ValidateSelectExpr(ex.Expr, defaultTable, aliases);
                break;
        }
    }

    private void ValidateTableReference(TableReference tref, Dictionary<string, string> aliases, List<string> tableNames)
    {
        switch (tref)
        {
            case TableReference.Named n:
                RequireTable(n.Name);
                tableNames.Add(n.Name);
                if (n.Alias != null) aliases[n.Alias] = n.Name;
                break;
            case TableReference.Join j:
                ValidateTableReference(j.Left, aliases, tableNames);
                ValidateTableReference(j.Right, aliases, tableNames);
                break;
            case TableReference.Subquery s:
                ValidateSelect(s.Query);
                break;
        }
    }

    private void ValidateSelectExpr(Expr expr, string? defaultTable, Dictionary<string, string> aliases)
    {
        if (expr is Expr.Column { Table: { } tbl } col)
        {
            var resolved = aliases.TryGetValue(tbl, out var rt) ? rt : tbl;
            RequireTable(resolved);
            var schema = _catalog.GetTable(resolved)
                ?? throw new ValidationException.TableNotFoundException(resolved);
            RequireColumn(schema, col.Name);
            return;
        }
        ValidateExpr(expr, defaultTable);
    }

    private void ValidateExpr(Expr expr, string? defaultTable)
    {
        switch (expr)
        {
            case Expr.Column c:
                ValidateColumnExpr(c, defaultTable);
                break;
            case Expr.BinaryOp b:
                ValidateExpr(b.Left, defaultTable);
                ValidateExpr(b.Right, defaultTable);
                break;
            case Expr.UnaryOp u:
                ValidateExpr(u.Inner, defaultTable);
                break;
            case Expr.Function f:
                foreach (var arg in f.Args) ValidateExpr(arg, defaultTable);
                break;
            case Expr.IsNull n:
                ValidateExpr(n.Inner, defaultTable);
                break;
            case Expr.Between b:
                ValidateExpr(b.Inner, defaultTable);
                ValidateExpr(b.Low, defaultTable);
                ValidateExpr(b.High, defaultTable);
                break;
            case Expr.InList il:
                ValidateExpr(il.Inner, defaultTable);
                foreach (var e in il.List) ValidateExpr(e, defaultTable);
                break;
            case Expr.InSubquery iq:
                ValidateExpr(iq.Inner, defaultTable);
                ValidateSelect(iq.Subquery);
                break;
            case Expr.ArrayOp ao:
                ValidateExpr(ao.Inner, defaultTable);
                ValidateExpr(ao.Right, defaultTable);
                break;
            case Expr.Subquery sq:
                ValidateSelect(sq.Query);
                break;
            case Expr.Cast c:
                ValidateExpr(c.Inner, defaultTable);
                break;
            case Expr.Case c:
                ValidateCaseExpr(c, defaultTable);
                break;
            case Expr.Substring s:
                ValidateSubstringExpr(s, defaultTable);
                break;
            case Expr.Trim t:
                ValidateExpr(t.Inner, defaultTable);
                if (t.TrimWhat != null) ValidateExpr(t.TrimWhat, defaultTable);
                break;
            case Expr.Literal or Expr.Wildcard:
                break;
        }
    }

    private void ValidateColumnExpr(Expr.Column c, string? defaultTable)
    {
        if (defaultTable == null) return;
        var schema = _catalog.GetTable(defaultTable);
        if (schema != null) RequireColumn(schema, c.Name);
    }

    private void ValidateCaseExpr(Expr.Case c, string? defaultTable)
    {
        if (c.Operand != null) ValidateExpr(c.Operand, defaultTable);
        foreach (var (cond, res) in c.Conditions)
        {
            ValidateExpr(cond, defaultTable);
            ValidateExpr(res, defaultTable);
        }
        if (c.ElseResult != null) ValidateExpr(c.ElseResult, defaultTable);
    }

    private void ValidateSubstringExpr(Expr.Substring s, string? defaultTable)
    {
        ValidateExpr(s.Inner, defaultTable);
        if (s.FromPos != null) ValidateExpr(s.FromPos, defaultTable);
        if (s.Len != null) ValidateExpr(s.Len, defaultTable);
    }

    private static void ValidateInsertRow(TableSchema schema, IReadOnlyList<string> columns, IReadOnlyList<Expr> row)
    {
        if (row.Count != columns.Count)
            throw new ValidationException.ConstraintViolationException(
                $"INSERT column count ({columns.Count}) does not match value count ({row.Count})");

        for (int i = 0; i < columns.Count; i++)
        {
            var col = schema.GetColumn(columns[i]);
            if (col != null && !col.Nullable && row[i] is Expr.Literal { Value: SqlValue.Null })
                throw new ValidationException.NullConstraintViolationException(schema.Name, columns[i]);
        }
    }

    private void ValidateInsert(InsertStatement ins)
    {
        RequireTable(ins.Table);
        var schema = _catalog.GetTable(ins.Table);
        if (schema == null) return;

        if (ins.Columns.Count == 0) return;

        foreach (var col in ins.Columns)
            RequireColumn(schema, col);

        foreach (var row in ins.Values)
            ValidateInsertRow(schema, ins.Columns, row);

        // Check NOT NULL columns without defaults
        foreach (var col in schema.Columns)
        {
            if (!col.Nullable && !ins.Columns.Any(c => c.Equals(col.Name, StringComparison.OrdinalIgnoreCase)))
                throw new ValidationException.NullConstraintViolationException(ins.Table, col.Name);
        }
    }

    private void ValidateUpdate(UpdateStatement upd)
    {
        RequireTable(upd.Table);
        var schema = _catalog.GetTable(upd.Table);
        if (schema == null) return;

        foreach (var (col, val) in upd.Assignments)
        {
            RequireColumn(schema, col);
            var colMeta = schema.GetColumn(col);
            if (colMeta != null && !colMeta.Nullable && val is Expr.Literal { Value: SqlValue.Null })
                throw new ValidationException.NullConstraintViolationException(upd.Table, col);
        }
        if (upd.WhereClause != null) ValidateExpr(upd.WhereClause, upd.Table);
    }

    private void ValidateDelete(DeleteStatement del)
    {
        RequireTable(del.Table);
        if (del.WhereClause != null) ValidateExpr(del.WhereClause, del.Table);
    }

    private void ValidateCreateTable(CreateTableStatement ct)
    {
        // Validate that EnumRef types exist in the catalog
        foreach (var col in ct.Columns)
        {
            if (col.DataType is DataType.EnumRef er && !_catalog.TypeExists(er.Name))
                throw new ValidationException.TypeNotFoundException(er.Name);
        }
    }

    private void ValidateAlterTable(AlterTableStatement alt)
    {
        RequireTable(alt.Table);
        var schema = _catalog.GetTable(alt.Table);
        if (schema == null) return;

        foreach (var op in alt.Operations)
        {
            switch (op)
            {
                case AlterTableOperation.DropColumn dc:
                    if (!dc.IfExists) RequireColumn(schema, dc.Name);
                    break;
                case AlterTableOperation.RenameColumn rc:
                    RequireColumn(schema, rc.OldName);
                    break;
            }
        }
    }

    private static void ValidateCreateEnum(CreateEnumStatement ce)
    {
        if (ce.Variants.Count == 0)
            throw new ValidationException.ConstraintViolationException($"enum '{ce.Name}' must have at least one variant");
    }

    private void ValidateDropEnum(DropEnumStatement de)
    {
        if (!de.IfExists && !_catalog.TypeExists(de.Name))
            throw new ValidationException.TypeNotFoundException(de.Name);
        if (_catalog.IsTypeInUse(de.Name))
            throw new ValidationException.TypeInUseException(de.Name);
    }

    private void ValidateViewAsItem(ViewAsItem item)
    {
        CheckNoAggregateInViewAs(item.Expr);
        CheckNoSubqueryInViewAs(item.Expr);
    }

    private void CheckNoAggregateInViewAs(Expr expr)
    {
        if (expr is Expr.Function f && IsAggregate(f.Name))
            throw new ValidationException.ViewAsAggregateNotAllowedException(f.Name);
        WalkExpr(expr, CheckNoAggregateInViewAs);
    }

    private void CheckNoSubqueryInViewAs(Expr expr)
    {
        if (expr is Expr.Subquery)
            throw new ValidationException.ViewAsSubqueryNotAllowedException();
        WalkExpr(expr, CheckNoSubqueryInViewAs);
    }

    private void CheckNoAggregateInWhere(Expr expr)
    {
        if (expr is Expr.Function f && IsAggregate(f.Name))
            throw new ValidationException.InvalidAggregateUsageException($"aggregate '{f.Name}' not allowed in WHERE clause");
        WalkExpr(expr, CheckNoAggregateInWhere);
    }

    private static void WalkExpr(Expr expr, Action<Expr> visit)
    {
        switch (expr)
        {
            case Expr.BinaryOp b: visit(b.Left); visit(b.Right); break;
            case Expr.UnaryOp u: visit(u.Inner); break;
            case Expr.Function fn:
                foreach (var a in fn.Args) { visit(a); }
                break;
            case Expr.IsNull n: visit(n.Inner); break;
            case Expr.Between b: visit(b.Inner); visit(b.Low); visit(b.High); break;
            case Expr.InList il:
                visit(il.Inner);
                foreach (var e in il.List) { visit(e); }
                break;
            case Expr.Cast c: visit(c.Inner); break;
            case Expr.Case c:
                if (c.Operand != null) visit(c.Operand);
                foreach (var (cond, res) in c.Conditions) { visit(cond); visit(res); }
                if (c.ElseResult != null) visit(c.ElseResult);
                break;
            case Expr.Substring s:
                visit(s.Inner);
                if (s.FromPos != null) visit(s.FromPos);
                if (s.Len != null) visit(s.Len);
                break;
            case Expr.Trim t:
                visit(t.Inner);
                if (t.TrimWhat != null) visit(t.TrimWhat);
                break;
        }
    }

    private static readonly HashSet<string> AggregateFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "ARRAY_AGG",
        "STRING_AGG", "BOOL_AND", "BOOL_OR", "STDDEV", "VARIANCE",
    };

    private static bool IsAggregate(string name) => AggregateFunctions.Contains(name);

    private void RequireTable(string name)
    {
        if (!_catalog.TableExists(name))
            throw new ValidationException.TableNotFoundException(name);
    }

    private static void RequireColumn(TableSchema schema, string column)
    {
        if (schema.GetColumn(column) == null)
            throw new ValidationException.ColumnNotFoundException(schema.Name, column);
    }

    // ── Transaction sequence helpers ───────────────────────────────────────────

    private static void SeqBegin(List<string?> stack, string? name)
    {
        if (name != null && stack.Any(open => open != null && open.Equals(name, StringComparison.OrdinalIgnoreCase)))
            throw new ValidationException.TransactionNameConflictException(name);
        stack.Add(name);
    }

    private static void SeqCommit(List<string?> stack, CommitScope scope)
    {
        if (stack.Count == 0)
            throw new ValidationException.NoActiveTransactionException();

        switch (scope)
        {
            case CommitScope.Current:
                stack.RemoveAt(stack.Count - 1);
                break;
            case CommitScope.All:
                stack.Clear();
                break;
            case CommitScope.Named n:
                var idx = FindTransaction(stack, n.Name);
                if (idx < 0) throw new ValidationException.TransactionNotFoundException(n.Name);
                if (idx < stack.Count - 1)
                {
                    var blocking = stack[^1] ?? "(anonymous)";
                    throw new ValidationException.TransactionNestingViolationException(n.Name, blocking);
                }
                stack.RemoveAt(idx);
                break;
        }
    }

    private static void SeqRollback(List<string?> stack, RollbackScope scope)
    {
        if (stack.Count == 0)
            throw new ValidationException.NoActiveTransactionException();

        switch (scope)
        {
            case RollbackScope.Current:
                stack.RemoveAt(stack.Count - 1);
                break;
            case RollbackScope.All:
                stack.Clear();
                break;
            case RollbackScope.ToSavepoint:
                // Savepoints are within a transaction; just verify one is open
                break;
            case RollbackScope.Named n:
                var idx = FindTransaction(stack, n.Name);
                if (idx < 0) throw new ValidationException.TransactionNotFoundException(n.Name);
                if (idx < stack.Count - 1)
                {
                    var blocking = stack[^1] ?? "(anonymous)";
                    throw new ValidationException.TransactionNestingViolationException(n.Name, blocking);
                }
                stack.RemoveAt(idx);
                break;
        }
    }

    private static int FindTransaction(List<string?> stack, string name)
    {
        for (int i = stack.Count - 1; i >= 0; i--)
            if (stack[i] != null && stack[i]!.Equals(name, StringComparison.OrdinalIgnoreCase))
                return i;
        return -1;
    }
}
