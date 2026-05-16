// Physical operator implementations — async streaming pull model.
// Transpiled from poc/rust/src/executor/ (filter, project, scan, join, aggregate, sort, limit, distinct, unnest, values).

namespace AeternumDB.Core.Executor;

using System.Collections.Generic;
using System.Text;
using AeternumDB.Core.Abstractions.Executor;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Query;
using AeternumDB.Core.Sql;
using AeternumDB.Core.Types;

// ── Internal helpers ──────────────────────────────────────────────────────────

internal static class OperatorHelpers
{
    /// <summary>Materialise all rows and return the schema from the first batch.</summary>
    internal static async Task<(List<DbRow> Rows, IReadOnlyList<ColumnMeta> Schema)> CollectAsync(
        IAsyncEnumerable<RecordBatch> batches)
    {
        var rows = new List<DbRow>();
        IReadOnlyList<ColumnMeta> schema = Array.Empty<ColumnMeta>();
        await foreach (var batch in batches.ConfigureAwait(false))
        {
            if (schema.Count == 0) schema = batch.Schema;
            rows.AddRange(batch.Rows);
        }
        return (rows, schema);
    }

    /// <summary>Merge right-row columns into a copy of left-row (right columns win on conflict).</summary>
    internal static DbRow MergeRows(DbRow left, DbRow right)
    {
        var merged = new DbRow();
        foreach (var kv in left.Columns) merged.Set(kv.Key, kv.Value);
        foreach (var kv in right.Columns) merged.Set(kv.Key, kv.Value);
        return merged;
    }

    /// <summary>Build a deterministic string key for a row for deduplication.</summary>
    internal static string RowKey(DbRow row)
    {
        var pairs = row.Columns.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToList();
        var sb = new StringBuilder();
        foreach (var kv in pairs)
        {
            sb.Append(kv.Key);
            sb.Append('=');
            sb.Append(kv.Value.ToString());
            sb.Append('|');
        }
        return sb.ToString();
    }

    /// <summary>Build a hash join key string from a list of expressions evaluated against a row.</summary>
    internal static string KeyString(IReadOnlyList<Expr> keyExprs, DbRow row)
    {
        var sb = new StringBuilder();
        foreach (var expr in keyExprs)
        {
            var val = ExpressionEvaluator.Eval(expr, row);
            sb.Append(val.ToString());
            sb.Append('|');
        }
        return sb.ToString();
    }

    internal static IReadOnlyList<ColumnMeta> SchemaFromNames(IEnumerable<string> names) =>
        names.Select(n => new ColumnMeta(n, "unknown")).ToList();
}

// ── Accumulator interface and implementations ─────────────────────────────────

internal interface IAccumulator
{
    void Accumulate(DbValue value);
    DbValue Finalize();
}

internal sealed class CountAccumulator : IAccumulator
{
    private long _count;
    public void Accumulate(DbValue value) { if (!value.IsNull) _count++; }
    public DbValue Finalize() => new DbValue.Integer(_count);
}

internal sealed class CountDistinctAccumulator : IAccumulator
{
    private readonly HashSet<string> _seen = [];
    public void Accumulate(DbValue value)
    {
        if (!value.IsNull) _seen.Add(value.ToString()!);
    }
    public DbValue Finalize() => new DbValue.Integer(_seen.Count);
}

internal sealed class SumAccumulator : IAccumulator
{
    private double? _sum;
    public void Accumulate(DbValue value)
    {
        if (value.IsNull) return;
        double? d = value.AsFloat() ?? (value.AsInteger() is long l ? (double)l : null);
        if (d.HasValue) _sum = (_sum ?? 0.0) + d.Value;
    }
    public DbValue Finalize() => _sum.HasValue ? new DbValue.Float(_sum.Value) : DbValue.Null.Instance;
}

internal sealed class AvgAccumulator : IAccumulator
{
    private double _sum;
    private long _count;
    public void Accumulate(DbValue value)
    {
        if (value.IsNull) return;
        double? d = value.AsFloat() ?? (value.AsInteger() is long l ? (double)l : null);
        if (d.HasValue) { _sum += d.Value; _count++; }
    }
    public DbValue Finalize() => _count > 0 ? new DbValue.Float(_sum / _count) : DbValue.Null.Instance;
}

internal sealed class MinAccumulator : IAccumulator
{
    private DbValue? _min;
    public void Accumulate(DbValue value)
    {
        if (value.IsNull) return;
        if (_min is null || ExpressionEvaluator.CompareDbValues(value, _min) < 0)
            _min = value;
    }
    public DbValue Finalize() => _min ?? DbValue.Null.Instance;
}

internal sealed class MaxAccumulator : IAccumulator
{
    private DbValue? _max;
    public void Accumulate(DbValue value)
    {
        if (value.IsNull) return;
        if (_max is null || ExpressionEvaluator.CompareDbValues(value, _max) > 0)
            _max = value;
    }
    public DbValue Finalize() => _max ?? DbValue.Null.Instance;
}

internal static class AccumulatorFactory
{
    internal static IAccumulator Create(string functionName) =>
        functionName.ToUpperInvariant() switch
        {
            "COUNT" => new CountAccumulator(),
            "COUNT_DISTINCT" => new CountDistinctAccumulator(),
            "SUM" => new SumAccumulator(),
            "AVG" => new AvgAccumulator(),
            "MIN" => new MinAccumulator(),
            "MAX" => new MaxAccumulator(),
            _ => new CountAccumulator()
        };
}

// ── SeqScanExec ───────────────────────────────────────────────────────────────

/// <summary>Sequential table scan with optional column projection and row filter.</summary>
public sealed class SeqScanExec : IExecutionPlan
{
    public string Table { get; }
    public string? Alias { get; }
    public IReadOnlyList<string>? Columns { get; }
    public Expr? Filter { get; }

    public SeqScanExec(string table, string? alias, IReadOnlyList<string>? columns, Expr? filter)
    {
        Table = table;
        Alias = alias;
        Columns = columns;
        Filter = filter;
    }

    public IReadOnlyList<ColumnMeta> Schema =>
        Columns is not null
            ? OperatorHelpers.SchemaFromNames(Columns)
            : Array.Empty<ColumnMeta>();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var tableSchema = ctx.TableProvider.Schema(Table);
        var colNames = Columns?.ToList() ?? tableSchema.Select(m => m.Name).ToList();
        var outSchema = OperatorHelpers.SchemaFromNames(colNames);
        var rows = new List<DbRow>();

        await foreach (var row in ctx.TableProvider.ScanAsync(Table).ConfigureAwait(false))
        {
            if (Filter is not null)
            {
                var result = ExpressionEvaluator.Eval(Filter, row);
                if (result.AsBool() != true) continue;
            }
            var projected = new DbRow();
            foreach (var col in colNames)
                projected.Set(col, row.Get(col));
            rows.Add(projected);
        }

        yield return new RecordBatch(rows, outSchema);
    }
}

// ── IndexScanExec ─────────────────────────────────────────────────────────────

/// <summary>Index-assisted scan with key predicate and optional residual filter.</summary>
public sealed class IndexScanExec : IExecutionPlan
{
    public string Table { get; }
    public string? Alias { get; }
    public string Index { get; }
    public IReadOnlyList<string>? Columns { get; }
    public Expr KeyPredicate { get; }
    public Expr? Filter { get; }

    public IndexScanExec(string table, string? alias, string index, IReadOnlyList<string>? columns, Expr keyPredicate, Expr? filter)
    {
        Table = table;
        Alias = alias;
        Index = index;
        Columns = columns;
        KeyPredicate = keyPredicate;
        Filter = filter;
    }

    public IReadOnlyList<ColumnMeta> Schema =>
        Columns is not null
            ? OperatorHelpers.SchemaFromNames(Columns)
            : Array.Empty<ColumnMeta>();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var tableSchema = ctx.TableProvider.Schema(Table);
        var colNames = Columns?.ToList() ?? tableSchema.Select(m => m.Name).ToList();
        var outSchema = OperatorHelpers.SchemaFromNames(colNames);
        var rows = new List<DbRow>();

        await foreach (var row in ctx.TableProvider.ScanAsync(Table).ConfigureAwait(false))
        {
            var keyMatch = ExpressionEvaluator.Eval(KeyPredicate, row);
            if (keyMatch.AsBool() != true) continue;
            if (Filter is not null)
            {
                var result = ExpressionEvaluator.Eval(Filter, row);
                if (result.AsBool() != true) continue;
            }
            var projected = new DbRow();
            foreach (var col in colNames)
                projected.Set(col, row.Get(col));
            rows.Add(projected);
        }

        yield return new RecordBatch(rows, outSchema);
    }
}

// ── FilterExec ────────────────────────────────────────────────────────────────

/// <summary>Row-level predicate filter.</summary>
public sealed class FilterExec(IExecutionPlan input, Expr predicate) : IExecutionPlan
{
    public IExecutionPlan Input { get; } = input;
    public Expr Predicate { get; } = predicate;

    public IReadOnlyList<ColumnMeta> Schema => Input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        await foreach (var batch in Input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var filtered = new List<DbRow>();
            foreach (var row in batch.Rows)
            {
                var val = ExpressionEvaluator.Eval(Predicate, row);
                if (val.AsBool() == true)
                    filtered.Add(row);
            }
            if (filtered.Count > 0)
                yield return new RecordBatch(filtered, batch.Schema);
        }
    }
}

// ── ProjectExec ───────────────────────────────────────────────────────────────

/// <summary>Column projection and computed-expression evaluation.</summary>
public sealed class ProjectExec(IExecutionPlan input, IReadOnlyList<ProjectionItem> items) : IExecutionPlan
{
    public IExecutionPlan Input { get; } = input;
    public IReadOnlyList<ProjectionItem> Items { get; } = items;

    private List<ColumnMeta>? _schema;
    public IReadOnlyList<ColumnMeta> Schema =>
        _schema ??= Items.Select((item, i) =>
            new ColumnMeta(item.Alias ?? $"col_{i}", "unknown")).ToList();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var outSchema = Schema;
        await foreach (var batch in Input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var projected = new List<DbRow>(batch.Rows.Count);
            foreach (var row in batch.Rows)
            {
                var newRow = new DbRow();
                for (int i = 0; i < Items.Count; i++)
                {
                    var item = Items[i];
                    var colName = item.Alias ?? $"col_{i}";
                    var val = ExpressionEvaluator.Eval(item.Expr, row);
                    newRow.Set(colName, val);
                }
                projected.Add(newRow);
            }
            yield return new RecordBatch(projected, outSchema);
        }
    }
}

// ── LimitExec ─────────────────────────────────────────────────────────────────

/// <summary>Offset + limit executor.</summary>
public sealed class LimitExec(IExecutionPlan input, int limitCount, int offset) : IExecutionPlan
{
    public IExecutionPlan Input { get; } = input;
    public int LimitCount { get; } = limitCount;
    public int Offset { get; } = offset;

    public IReadOnlyList<ColumnMeta> Schema => Input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        int skipped = 0;
        int emitted = 0;

        await foreach (var batch in Input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var limited = new List<DbRow>();
            foreach (var row in batch.Rows)
            {
                if (skipped < Offset) { skipped++; continue; }
                if (emitted >= LimitCount) break;
                limited.Add(row);
                emitted++;
            }
            if (limited.Count > 0)
                yield return new RecordBatch(limited, batch.Schema);
            if (emitted >= LimitCount)
                yield break;
        }
    }
}

// ── SortExec ──────────────────────────────────────────────────────────────────

/// <summary>In-memory sort over the full input stream.</summary>
public sealed class SortExec(IExecutionPlan input, IReadOnlyList<SortExpr> orderBy) : IExecutionPlan
{
    public IExecutionPlan Input { get; } = input;
    public IReadOnlyList<SortExpr> OrderBy { get; } = orderBy;

    public IReadOnlyList<ColumnMeta> Schema => Input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var (rows, schema) = await OperatorHelpers.CollectAsync(Input.ExecuteAsync(ctx))
            .ConfigureAwait(false);

        rows.Sort((a, b) =>
        {
            foreach (var sortExpr in OrderBy)
            {
                var av = ExpressionEvaluator.Eval(sortExpr.Expr, a);
                var bv = ExpressionEvaluator.Eval(sortExpr.Expr, b);
                int cmp = ExpressionEvaluator.CompareDbValues(av, bv);
                if (!sortExpr.Ascending) cmp = -cmp;
                if (cmp != 0) return cmp;
            }
            return 0;
        });

        yield return new RecordBatch(rows, schema);
    }
}

// ── DistinctExec ──────────────────────────────────────────────────────────────

/// <summary>Hash-based duplicate elimination.</summary>
public sealed class DistinctExec(IExecutionPlan input) : IExecutionPlan
{
    public IExecutionPlan Input { get; } = input;
    public IReadOnlyList<ColumnMeta> Schema => Input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);

        await foreach (var batch in Input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var distinct = new List<DbRow>();
            foreach (var row in batch.Rows)
            {
                var key = OperatorHelpers.RowKey(row);
                if (seen.Add(key))
                    distinct.Add(row);
            }
            if (distinct.Count > 0)
                yield return new RecordBatch(distinct, batch.Schema);
        }
    }
}

// ── UnnestExec ────────────────────────────────────────────────────────────────

/// <summary>Explodes an array column into one row per element.</summary>
public sealed class UnnestExec(IExecutionPlan input, Expr column, string? alias) : IExecutionPlan
{
    public IExecutionPlan Input { get; } = input;
    public Expr Column { get; } = column;
    public string Alias { get; } = alias ?? "unnest";

    public IReadOnlyList<ColumnMeta> Schema
    {
        get
        {
            var s = Input.Schema.ToList();
            s.Add(new ColumnMeta(Alias, "unknown"));
            return s;
        }
    }

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        await foreach (var batch in Input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var outSchema = batch.Schema.Append(new ColumnMeta(Alias, "unknown")).ToList();
            var rows = new List<DbRow>();

            foreach (var row in batch.Rows)
            {
                var arrayVal = ExpressionEvaluator.Eval(Column, row);
                if (arrayVal.AsArray() is DbValue[] items)
                {
                    foreach (var item in items)
                    {
                        var newRow = OperatorHelpers.MergeRows(row, new DbRow());
                        newRow.Set(Alias, item);
                        rows.Add(newRow);
                    }
                }
                else if (!arrayVal.IsNull)
                {
                    var newRow = OperatorHelpers.MergeRows(row, new DbRow());
                    newRow.Set(Alias, arrayVal);
                    rows.Add(newRow);
                }
            }

            yield return new RecordBatch(rows, outSchema);
        }
    }
}

// ── ValuesExec ────────────────────────────────────────────────────────────────

/// <summary>Inline constant-row source (VALUES clause).</summary>
public sealed class ValuesExec(IReadOnlyList<IReadOnlyList<Expr>> rows, IReadOnlyList<string> schema) : IExecutionPlan
{
    public IReadOnlyList<IReadOnlyList<Expr>> Rows { get; } = rows;
    private readonly IReadOnlyList<string> _schema = schema;

    public IReadOnlyList<ColumnMeta> Schema => OperatorHelpers.SchemaFromNames(_schema);

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        await Task.CompletedTask.ConfigureAwait(false); // satisfy async requirement
        var outSchema = Schema;
        var emptyRow = new DbRow();
        var resultRows = new List<DbRow>(Rows.Count);

        foreach (var rowExprs in Rows)
        {
            var newRow = new DbRow();
            for (int i = 0; i < rowExprs.Count; i++)
            {
                var colName = i < _schema.Count ? _schema[i] : $"col_{i}";
                var val = ExpressionEvaluator.Eval(rowExprs[i], emptyRow);
                newRow.Set(colName, val);
            }
            resultRows.Add(newRow);
        }

        yield return new RecordBatch(resultRows, outSchema);
    }
}

// ── NestedLoopJoinExec ────────────────────────────────────────────────────────

/// <summary>Nested-loop join with optional ON condition.</summary>
public sealed class NestedLoopJoinExec(
    IExecutionPlan left,
    IExecutionPlan right,
    JoinType joinType,
    Expr? condition) : IExecutionPlan
{
    public IExecutionPlan Left { get; } = left;
    public IExecutionPlan Right { get; } = right;
    public JoinType JoinType { get; } = joinType;
    public Expr? Condition { get; } = condition;

    private List<ColumnMeta>? _schema;
    public IReadOnlyList<ColumnMeta> Schema => _schema ??= Left.Schema.Concat(Right.Schema).ToList();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var (leftRows, leftSchema) = await OperatorHelpers.CollectAsync(Left.ExecuteAsync(ctx)).ConfigureAwait(false);
        var (rightRows, rightSchema) = await OperatorHelpers.CollectAsync(Right.ExecuteAsync(ctx)).ConfigureAwait(false);

        var outSchema = leftSchema.Concat(rightSchema).ToList();
        var output = new List<DbRow>();

        ProcessLeftRows(leftRows, rightRows, rightSchema, output);

        if (JoinType is JoinType.Right || JoinType is JoinType.Full)
            ProcessUnmatchedRightRows(leftRows, leftSchema, rightRows, output);

        yield return new RecordBatch(output, outSchema);
    }

    private void ProcessLeftRows(List<DbRow> leftRows, List<DbRow> rightRows, IReadOnlyList<ColumnMeta> rightSchema, List<DbRow> output)
    {
        foreach (var leftRow in leftRows)
        {
            bool matched = false;
            foreach (var rightRow in rightRows)
            {
                var joined = OperatorHelpers.MergeRows(leftRow, rightRow);
                bool passes = Condition is null
                    || ExpressionEvaluator.Eval(Condition, joined).AsBool() == true;

                if (passes)
                {
                    matched = true;
                    output.Add(joined);
                }
            }

            if (!matched && (JoinType is JoinType.Left || JoinType is JoinType.Full))
                output.Add(BuildNullPaddedRow(leftRow, rightSchema));
        }
    }

    private static DbRow BuildNullPaddedRow(DbRow leftRow, IReadOnlyList<ColumnMeta> rightSchema)
    {
        var outRow = new DbRow();
        foreach (var kv in leftRow.Columns) outRow.Set(kv.Key, kv.Value);
        foreach (var meta in rightSchema) outRow.Set(meta.Name, DbValue.Null.Instance);
        return outRow;
    }

    private void ProcessUnmatchedRightRows(List<DbRow> leftRows, IReadOnlyList<ColumnMeta> leftSchema, List<DbRow> rightRows, List<DbRow> output)
    {
        foreach (var rightRow in rightRows)
        {
            bool matched = leftRows.Any(leftRow =>
            {
                var joined = OperatorHelpers.MergeRows(leftRow, rightRow);
                return Condition is null || ExpressionEvaluator.Eval(Condition, joined).AsBool() == true;
            });
            if (!matched)
            {
                var outRow = new DbRow();
                foreach (var meta in leftSchema) outRow.Set(meta.Name, DbValue.Null.Instance);
                foreach (var kv in rightRow.Columns) outRow.Set(kv.Key, kv.Value);
                output.Add(outRow);
            }
        }
    }
}

// ── HashJoinExec ──────────────────────────────────────────────────────────────

/// <summary>Hash-based equi-join.</summary>
public sealed class HashJoinExec(
    IExecutionPlan left,
    IExecutionPlan right,
    JoinType joinType,
    IReadOnlyList<Expr> leftKeys,
    IReadOnlyList<Expr> rightKeys,
    Expr? residual) : IExecutionPlan
{
    public IExecutionPlan Left { get; } = left;
    public IExecutionPlan Right { get; } = right;
    public JoinType JoinType { get; } = joinType;
    public IReadOnlyList<Expr> LeftKeys { get; } = leftKeys;
    public IReadOnlyList<Expr> RightKeys { get; } = rightKeys;
    public Expr? Residual { get; } = residual;

    private List<ColumnMeta>? _schema;
    public IReadOnlyList<ColumnMeta> Schema => _schema ??= Left.Schema.Concat(Right.Schema).ToList();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var (leftRows, leftSchema) = await OperatorHelpers.CollectAsync(Left.ExecuteAsync(ctx)).ConfigureAwait(false);
        var (rightRows, rightSchema) = await OperatorHelpers.CollectAsync(Right.ExecuteAsync(ctx)).ConfigureAwait(false);

        var hashTable = new Dictionary<string, List<DbRow>>(StringComparer.Ordinal);
        foreach (var row in leftRows)
        {
            var key = OperatorHelpers.KeyString(LeftKeys, row);
            if (!hashTable.TryGetValue(key, out var bucket))
                hashTable[key] = bucket = [];
            bucket.Add(row);
        }

        var outSchema = leftSchema.Concat(rightSchema).ToList();
        var output = new List<DbRow>();

        foreach (var rightRow in rightRows)
        {
            var key = OperatorHelpers.KeyString(RightKeys, rightRow);
            if (!hashTable.TryGetValue(key, out var matchedLeft)) continue;
            foreach (var leftRow in matchedLeft)
            {
                var joined = OperatorHelpers.MergeRows(leftRow, rightRow);
                bool passes = Residual is null
                    || ExpressionEvaluator.Eval(Residual, joined).AsBool() == true;
                if (passes) output.Add(joined);
            }
        }

        yield return new RecordBatch(output, outSchema);
    }
}

// ── SortMergeJoinExec ─────────────────────────────────────────────────────────

/// <summary>Sort-merge equi-join (inputs assumed sorted).</summary>
public sealed class SortMergeJoinExec(
    IExecutionPlan left,
    IExecutionPlan right,
    JoinType joinType,
    IReadOnlyList<Expr> leftKeys,
    IReadOnlyList<Expr> rightKeys) : IExecutionPlan
{
    public IExecutionPlan Left { get; } = left;
    public IExecutionPlan Right { get; } = right;
    public JoinType JoinType { get; } = joinType;
    public IReadOnlyList<Expr> LeftKeys { get; } = leftKeys;
    public IReadOnlyList<Expr> RightKeys { get; } = rightKeys;

    private List<ColumnMeta>? _schema;
    public IReadOnlyList<ColumnMeta> Schema => _schema ??= Left.Schema.Concat(Right.Schema).ToList();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var (leftRows, leftSchema) = await OperatorHelpers.CollectAsync(Left.ExecuteAsync(ctx)).ConfigureAwait(false);
        var (rightRows, rightSchema) = await OperatorHelpers.CollectAsync(Right.ExecuteAsync(ctx)).ConfigureAwait(false);

        var outSchema = leftSchema.Concat(rightSchema).ToList();
        var output = new List<DbRow>();

        foreach (var leftRow in leftRows)
        {
            foreach (var rightRow in rightRows)
            {
                bool allMatch = true;
                for (int k = 0; k < LeftKeys.Count && k < RightKeys.Count; k++)
                {
                    var lv = ExpressionEvaluator.Eval(LeftKeys[k], leftRow);
                    var rv = ExpressionEvaluator.Eval(RightKeys[k], rightRow);
                    if (ExpressionEvaluator.CompareDbValues(lv, rv) != 0) { allMatch = false; break; }
                }
                if (allMatch)
                    output.Add(OperatorHelpers.MergeRows(leftRow, rightRow));
            }
        }

        yield return new RecordBatch(output, outSchema);
    }
}

// ── HashAggregateExec ─────────────────────────────────────────────────────────

/// <summary>Hash-based GROUP BY with COUNT/SUM/AVG/MIN/MAX accumulators.</summary>
public sealed class HashAggregateExec(
    IExecutionPlan input,
    IReadOnlyList<Expr> groupBy,
    IReadOnlyList<AggregateExpr> aggregates,
    Expr? having) : IExecutionPlan
{
    public IExecutionPlan Input { get; } = input;
    public IReadOnlyList<Expr> GroupBy { get; } = groupBy;
    public IReadOnlyList<AggregateExpr> Aggregates { get; } = aggregates;
    public Expr? Having { get; } = having;

    public IReadOnlyList<ColumnMeta> Schema
    {
        get
        {
            var s = GroupBy.Select((_, i) => new ColumnMeta($"group_{i}", "unknown")).ToList();
            s.AddRange(Aggregates.Select(a => new ColumnMeta(a.Alias ?? "agg", "unknown")));
            return s;
        }
    }

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var (allRows, _) = await OperatorHelpers.CollectAsync(Input.ExecuteAsync(ctx)).ConfigureAwait(false);

        // groups: key → (group-values row, accumulators)
        var groups = new Dictionary<string, (DbRow GroupVals, IAccumulator[] Accs)>(StringComparer.Ordinal);

        foreach (var row in allRows)
        {
            var groupKey = new StringBuilder();
            var groupVals = new DbRow();

            for (int i = 0; i < GroupBy.Count; i++)
            {
                var val = ExpressionEvaluator.Eval(GroupBy[i], row);
                groupKey.Append(val.ToString()).Append('|');
                groupVals.Set($"group_{i}", val);
            }

            var key = groupKey.ToString();
            if (!groups.TryGetValue(key, out var entry))
            {
                var accs = Aggregates.Select(a => AccumulatorFactory.Create(FuncName(a.Func))).ToArray();
                entry = (groupVals, accs);
                groups[key] = entry;
            }

            for (int j = 0; j < Aggregates.Count; j++)
            {
                var agg = Aggregates[j];
                string fn = FuncName(agg.Func);
                DbValue argVal = fn == "COUNT" && agg.Func is Expr.Wildcard
                    ? new DbValue.Integer(1)
                    : EvalAggArg(agg.Func, row);
                entry.Accs[j].Accumulate(argVal);
            }
        }

        var outSchema = Schema;
        var output = new List<DbRow>(groups.Count);

        foreach (var (_, (groupVals, accs)) in groups)
        {
            var outRow = new DbRow();
            foreach (var kv in groupVals.Columns) outRow.Set(kv.Key, kv.Value);

            for (int j = 0; j < Aggregates.Count; j++)
            {
                var alias = Aggregates[j].Alias ?? "agg";
                outRow.Set(alias, accs[j].Finalize());
            }

            if (Having is not null && ExpressionEvaluator.Eval(Having, outRow).AsBool() != true)
                continue;
            output.Add(outRow);
        }

        yield return new RecordBatch(output, outSchema);
    }

    private static string FuncName(Expr expr) =>
        expr is Expr.Function fn ? fn.Name.ToUpperInvariant() : "COUNT";

    private static DbValue EvalAggArg(Expr func, DbRow row)
    {
        if (func is Expr.Function fn && fn.Args.Count > 0)
            return ExpressionEvaluator.Eval(fn.Args[0], row);
        return ExpressionEvaluator.Eval(func, row);
    }
}
