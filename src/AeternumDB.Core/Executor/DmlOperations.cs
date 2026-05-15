// DML execution helpers: INSERT, UPDATE, DELETE, GRANT, REVOKE, referential integrity.
// Transpiled from poc/rust/src/executor/dml.rs.

namespace AeternumDB.Core.Executor;

using System.Collections.Generic;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Types;

/// <summary>
/// Static helpers that execute data-manipulation language (DML) operations.
/// All methods require the concrete <see cref="ExecutionContext"/> to access ACL
/// and mutable table operations.
/// </summary>
public static class DmlOperations
{
    /// <summary>Execute an INSERT into <paramref name="table"/>.</summary>
    public static async ValueTask<int> ExecuteInsertAsync(
        ExecutionContext ctx,
        string table,
        IReadOnlyList<string> columns,
        IReadOnlyList<IReadOnlyList<DbValue>> values)
    {
        ctx.CheckPrivilege(table, "INSERT");

        var schema = ctx.TableProvider.Schema(table);
        var rows = new List<DbRow>(values.Count);

        foreach (var valueRow in values)
        {
            var row = new DbRow();
            for (int i = 0; i < columns.Count; i++)
            {
                if (i < valueRow.Count)
                    row.Set(columns[i], valueRow[i]);
            }

            // Fill missing columns with NULL
            foreach (var meta in schema)
            {
                if (!HasColumn(row, meta.Name))
                    row.Set(meta.Name, DbValue.Null.Instance);
            }

            rows.Add(row);
        }

        return await ctx.TableProvider.InsertAsync(table, rows).ConfigureAwait(false);
    }

    /// <summary>Execute an UPDATE on <paramref name="table"/>, replacing column values for all rows.</summary>
    public static async ValueTask<int> ExecuteUpdateAsync(
        ExecutionContext ctx,
        string table,
        IReadOnlyDictionary<string, DbValue> updates)
    {
        ctx.CheckPrivilege(table, "UPDATE");

        if (ctx.TableProvider is not ITableMutations mutations)
            throw new ExecutorException(ExecutorErrorKind.Other,
                $"Table provider does not support UPDATE on '{table}'");

        return await mutations.UpdateAsync(table, updates).ConfigureAwait(false);
    }

    /// <summary>Execute a DELETE on <paramref name="table"/>, removing all rows.</summary>
    public static async ValueTask<int> ExecuteDeleteAsync(ExecutionContext ctx, string table)
    {
        ctx.CheckPrivilege(table, "DELETE");
        return await ctx.TableProvider.DeleteAsync(table).ConfigureAwait(false);
    }

    /// <summary>Grant <paramref name="privilege"/> on <paramref name="obj"/> to <paramref name="user"/>.</summary>
    public static void ExecuteGrant(ExecutionContext ctx, string user, string obj, string privilege) =>
        ctx.GrantPrivilege(user, obj, privilege);

    /// <summary>Revoke <paramref name="privilege"/> on <paramref name="obj"/> from <paramref name="user"/>.</summary>
    public static void ExecuteRevoke(ExecutionContext ctx, string user, string obj, string privilege) =>
        ctx.RevokePrivilege(user, obj, privilege);

    /// <summary>
    /// Returns <see langword="true"/> if any row in <paramref name="parentTable"/> has
    /// <paramref name="parentColumn"/> equal to <paramref name="childValue"/>.
    /// </summary>
    public static async ValueTask<bool> CheckReferentialIntegrityAsync(
        ExecutionContext ctx,
        string parentTable,
        string parentColumn,
        DbValue childValue)
    {
        await foreach (var row in ctx.TableProvider.ScanAsync(parentTable).ConfigureAwait(false))
        {
            var val = row.Get(parentColumn);
            if (!val.IsNull && !childValue.IsNull &&
                ExpressionEvaluator.CompareDbValues(val, childValue) == 0)
                return true;
        }
        return false;
    }

    /// <summary>
    /// Apply a referential action (CASCADE / SET NULL / SET DEFAULT / RESTRICT) when
    /// a parent row is deleted or updated.
    /// </summary>
    public static async ValueTask ApplyReferentialActionAsync(
        ExecutionContext ctx,
        string action,
        string table,
        string column)
    {
        switch (action.ToUpperInvariant())
        {
            case "CASCADE":
                await ExecuteDeleteAsync(ctx, table).ConfigureAwait(false);
                break;

            case "SET NULL":
            case "SET DEFAULT":
            {
                var updates = new Dictionary<string, DbValue>(StringComparer.OrdinalIgnoreCase)
                {
                    [column] = DbValue.Null.Instance
                };
                await ExecuteUpdateAsync(ctx, table, updates).ConfigureAwait(false);
                break;
            }

            case "RESTRICT":
            case "NO ACTION":
                throw new ExecutorException(
                    ExecutorErrorKind.ReferentialIntegrityViolation,
                    $"Cannot delete/update due to foreign key constraint on {table}");

            default:
                // Unknown action — silently ignore (permissive default)
                break;
        }
    }

    // ── private helpers ───────────────────────────────────────────────────────

    private static bool HasColumn(DbRow row, string column)
    {
        foreach (var kv in row.Columns)
        {
            if (string.Equals(kv.Key, column, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }
}
