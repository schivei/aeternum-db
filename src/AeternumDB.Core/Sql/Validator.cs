// Semantic validator for AeternumDB SQL.
//
// Transpiled from poc/rust/src/sql/validator.rs.
// AOT-compatible — no reflection.

using System.Collections.Generic;
using AeternumDB.Core.Sql.Ast;

namespace AeternumDB.Core.Sql;

/// <summary>
/// Semantic validator for internal SQL AST nodes.
/// Transpiled from poc/rust/src/sql/validator.rs.
/// </summary>
public sealed class SqlValidator
{
    private readonly Catalog _catalog;

    public SqlValidator(Catalog catalog)
    {
        _catalog = catalog;
    }

    /// <summary>Validate a top-level Statement.</summary>
    public void Validate(Statement stmt)
    {
        switch (stmt)
        {
            case Statement.Select s:
                ValidateSelect(s.Query);
                break;
            case Statement.Insert s:
                ValidateInsert(s.Stmt);
                break;
            case Statement.Update s:
                ValidateUpdate(s.Stmt);
                break;
            case Statement.Delete s:
                ValidateDelete(s.Stmt);
                break;
            case Statement.CreateTable s:
                ValidateCreateTable(s.Stmt);
                break;
            case Statement.AlterTable s:
                ValidateAlterTable(s.Stmt);
                break;
            case Statement.CreateEnum s:
                ValidateCreateEnum(s.Stmt);
                break;
            case Statement.DropEnum s:
                ValidateDropEnum(s.Stmt);
                break;
            default:
                break;
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

    private void ValidateSelectItem(
        SelectItem item,
        string? defaultTable,
        Dictionary<string, string> aliases
    )
    {
        switch (item)
        {
            case SelectItem.Wildcard:
                break;
            case SelectItem.QualifiedWildcard qw:
                RequireTable(
                    aliases.TryGetValue(qw.Table, out var resolvedTable) ? resolvedTable : qw.Table
                );
                break;
            case SelectItem.ExprItem ei:
                ValidateSelectExpr(ei.Expr, defaultTable, aliases);
                break;
            case SelectItem.Expand ex:
                ValidateSelectExpr(ex.Expr, defaultTable, aliases);
                break;
        }
    }

    private void ValidateTableReference(
        TableReference tref,
        Dictionary<string, string> aliases,
        List<string> tableNames
    )
    {
        switch (tref)
        {
            case TableReference.Named n:
                RequireTable(n.Name);
                tableNames.Add(n.Name);
                if (n.Alias != null)
                    aliases[n.Alias] = n.Name;
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

    private void ValidateSelectExpr(
        Expr expr,
        string? defaultTable,
        Dictionary<string, string> aliases
    )
    {
        if (expr is Expr.Column { Table: { } tbl } col)
        {
            var resolved = aliases.TryGetValue(tbl, out var resolvedTable) ? resolvedTable : tbl;
            RequireTable(resolved);
            var schema =
                _catalog.GetTable(resolved)
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
                foreach (var arg in f.Args)
                    ValidateExpr(arg, defaultTable);
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
                foreach (var item in il.List)
                    ValidateExpr(item, defaultTable);
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
                if (t.TrimWhat != null)
                    ValidateExpr(t.TrimWhat, defaultTable);
                break;
            case Expr.Literal:
            case Expr.Wildcard:
                break;
        }
    }

    private void ValidateColumnExpr(Expr.Column c, string? defaultTable)
    {
        if (defaultTable == null)
            return;

        var schema = _catalog.GetTable(defaultTable);
        if (schema != null)
            RequireColumn(schema, c.Name);
    }

    private void ValidateCaseExpr(Expr.Case c, string? defaultTable)
    {
        if (c.Operand != null)
            ValidateExpr(c.Operand, defaultTable);

        foreach (var (cond, res) in c.Conditions)
        {
            ValidateExpr(cond, defaultTable);
            ValidateExpr(res, defaultTable);
        }

        if (c.ElseResult != null)
            ValidateExpr(c.ElseResult, defaultTable);
    }

    private void ValidateSubstringExpr(Expr.Substring s, string? defaultTable)
    {
        ValidateExpr(s.Inner, defaultTable);
        if (s.FromPos != null)
            ValidateExpr(s.FromPos, defaultTable);
        if (s.Len != null)
            ValidateExpr(s.Len, defaultTable);
    }

    private static void ValidateInsertRow(
        TableSchema schema,
        IReadOnlyList<string> columns,
        IReadOnlyList<Expr> row
    )
    {
        if (row.Count != columns.Count)
            throw new ValidationException.ConstraintViolationException(
                $"INSERT column count ({columns.Count}) does not match value count ({row.Count})"
            );

        for (int i = 0; i < columns.Count; i++)
        {
            var col = schema.GetColumn(columns[i]);
            if (col != null && !col.Nullable && row[i] is Expr.Literal { Value: SqlValue.Null })
                throw new ValidationException.NullConstraintViolationException(
                    schema.Name,
                    columns[i]
                );
        }
    }

    private void ValidateInsert(InsertStatement ins)
    {
        RequireTable(ins.Table);
        var schema = _catalog.GetTable(ins.Table);
        if (schema == null || ins.Columns.Count == 0)
            return;

        foreach (var col in ins.Columns)
            RequireColumn(schema, col);

        foreach (var row in ins.Values)
            ValidateInsertRow(schema, ins.Columns, row);

        foreach (var col in schema.Columns)
        {
            if (
                !col.Nullable
                && !ins.Columns.Any(c => c.Equals(col.Name, StringComparison.OrdinalIgnoreCase))
            )
                throw new ValidationException.NullConstraintViolationException(ins.Table, col.Name);
        }
    }

    private void ValidateUpdate(UpdateStatement upd)
    {
        RequireTable(upd.Table);
        var schema = _catalog.GetTable(upd.Table);
        if (schema == null)
            return;

        foreach (var (col, val) in upd.Assignments)
        {
            RequireColumn(schema, col);
            var colMeta = schema.GetColumn(col);
            if (
                colMeta != null
                && !colMeta.Nullable
                && val is Expr.Literal { Value: SqlValue.Null }
            )
                throw new ValidationException.NullConstraintViolationException(upd.Table, col);
        }

        if (upd.WhereClause != null)
            ValidateExpr(upd.WhereClause, upd.Table);
    }

    private void ValidateDelete(DeleteStatement del)
    {
        RequireTable(del.Table);
        if (del.WhereClause != null)
            ValidateExpr(del.WhereClause, del.Table);
    }

    private void ValidateCreateTable(CreateTableStatement ct)
    {
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
        if (schema == null)
            return;

        foreach (var op in alt.Operations)
        {
            switch (op)
            {
                case AlterTableOperation.DropColumn dc:
                    if (!dc.IfExists)
                        RequireColumn(schema, dc.Name);
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
            throw new ValidationException.ConstraintViolationException(
                $"enum '{ce.Name}' must have at least one variant"
            );
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
            throw new ValidationException.InvalidAggregateUsageException(
                $"aggregate '{f.Name}' not allowed in WHERE clause"
            );
        WalkExpr(expr, CheckNoAggregateInWhere);
    }

    private static void WalkExpr(Expr expr, Action<Expr> visit)
    {
        switch (expr)
        {
            case Expr.BinaryOp b:
                visit(b.Left);
                visit(b.Right);
                break;
            case Expr.UnaryOp u:
                visit(u.Inner);
                break;
            case Expr.Function fn:
                foreach (var arg in fn.Args)
                    visit(arg);
                break;
            case Expr.IsNull n:
                visit(n.Inner);
                break;
            case Expr.Between b:
                visit(b.Inner);
                visit(b.Low);
                visit(b.High);
                break;
            case Expr.InList il:
                visit(il.Inner);
                foreach (var item in il.List)
                    visit(item);
                break;
            case Expr.Cast c:
                visit(c.Inner);
                break;
            case Expr.Case c:
                if (c.Operand != null)
                    visit(c.Operand);
                foreach (var (cond, res) in c.Conditions)
                {
                    visit(cond);
                    visit(res);
                }

                if (c.ElseResult != null)
                    visit(c.ElseResult);
                break;
            case Expr.Substring s:
                visit(s.Inner);
                if (s.FromPos != null)
                    visit(s.FromPos);
                if (s.Len != null)
                    visit(s.Len);
                break;
            case Expr.Trim t:
                visit(t.Inner);
                if (t.TrimWhat != null)
                    visit(t.TrimWhat);
                break;
        }
    }

    private static readonly HashSet<string> AggregateFunctions = new(
        StringComparer.OrdinalIgnoreCase
    )
    {
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "GROUP_CONCAT",
        "ARRAY_AGG",
        "STRING_AGG",
        "BOOL_AND",
        "BOOL_OR",
        "STDDEV",
        "VARIANCE",
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

    private static void SeqBegin(List<string?> stack, string? name)
    {
        if (
            name != null
            && stack.Any(open =>
                open != null && open.Equals(name, StringComparison.OrdinalIgnoreCase)
            )
        )
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
                if (idx < 0)
                    throw new ValidationException.TransactionNotFoundException(n.Name);
                if (idx < stack.Count - 1)
                {
                    var blocking = stack[^1] ?? "(anonymous)";
                    throw new ValidationException.TransactionNestingViolationException(
                        n.Name,
                        blocking
                    );
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
                break;
            case RollbackScope.Named n:
                var idx = FindTransaction(stack, n.Name);
                if (idx < 0)
                    throw new ValidationException.TransactionNotFoundException(n.Name);
                if (idx < stack.Count - 1)
                {
                    var blocking = stack[^1] ?? "(anonymous)";
                    throw new ValidationException.TransactionNestingViolationException(
                        n.Name,
                        blocking
                    );
                }

                stack.RemoveAt(idx);
                break;
        }
    }

    private static int FindTransaction(List<string?> stack, string name)
    {
        for (int i = stack.Count - 1; i >= 0; i--)
        {
            if (stack[i] != null && stack[i]!.Equals(name, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }
}
