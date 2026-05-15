using System.Buffers;
using System.Runtime.InteropServices;
using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;
using JoinType = AeternumDB.PoC.Shared.Sql.JoinType;

namespace AeternumDB.PoC.Safe.Executor;

// ── Execution Context ─────────────────────────────────────────────────────────

[Injectable<IExecutionContext>(ServiceLifetime.Scoped)]
public sealed class SafeExecutionContext : IExecutionContext
{
    private long _idCounter;
    public ITableProvider TableProvider { get; }
    public string CurrentUser { get; }

    public SafeExecutionContext(ITableProvider tableProvider, string user = "system")
    {
        TableProvider = tableProvider;
        CurrentUser = user;
    }

    public long NextObjectId() => Interlocked.Increment(ref _idCounter);
}

// ── In-Memory Table Provider ──────────────────────────────────────────────────

[Injectable<ITableProvider>(ServiceLifetime.Singleton)]
public sealed class SafeInMemoryTableProvider : ITableProvider
{
    private readonly Dictionary<string, (List<ColumnMeta> Schema, List<DbRow> Rows)>
        _tables = new(StringComparer.OrdinalIgnoreCase);

    public void CreateTable(string table, IReadOnlyList<ColumnMeta> schema) =>
        _tables[table] = (schema.ToList(), []);

    public bool TableExists(string t) => _tables.ContainsKey(t);
    public IReadOnlyList<ColumnMeta> Schema(string t) =>
        _tables.TryGetValue(t, out var v) ? v.Schema : [];

    public async IAsyncEnumerable<DbRow> ScanAsync(string table)
    {
        if (!_tables.TryGetValue(table, out var t)) yield break;
        // Use index-based loop — CollectionsMarshal.AsSpan cannot cross yield boundary
        var rows = t.Rows;
        for (int i = 0; i < rows.Count; i++)
            yield return rows[i];
        await ValueTask.CompletedTask;
    }

    public ValueTask<int> InsertAsync(string table, IReadOnlyList<DbRow> rows)
    {
        if (!_tables.TryGetValue(table, out var t)) return ValueTask.FromResult(0);
        t.Rows.AddRange(rows);
        return ValueTask.FromResult(rows.Count);
    }

    public ValueTask<int> DeleteAsync(string table)
    {
        if (!_tables.TryGetValue(table, out var t)) return ValueTask.FromResult(0);
        int n = t.Rows.Count;
        t.Rows.Clear();
        return ValueTask.FromResult(n);
    }
}

// ── Operators (re-use same logic as unsafe; safe C# has zero unsafe keywords) ─

public sealed class SafeSeqScanExec : IExecutionPlan
{
    private readonly string _table;
    private readonly Func<DbRow, bool>? _filter;
    private readonly IReadOnlyList<ColumnMeta> _schema;

    public SafeSeqScanExec(string table, IReadOnlyList<ColumnMeta> schema, Func<DbRow, bool>? filter = null)
    { _table = table; _schema = schema; _filter = filter; }

    public IReadOnlyList<ColumnMeta> Schema => _schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var rows = new List<DbRow>();
        await foreach (var row in ctx.TableProvider.ScanAsync(_table).ConfigureAwait(false))
            if (_filter is null || _filter(row)) rows.Add(row);
        yield return new RecordBatch(rows, _schema);
    }
}

public sealed class SafeValuesExec(
    IReadOnlyList<IReadOnlyList<DbValue>> rows,
    IReadOnlyList<ColumnMeta> schema) : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema => schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var dbRows = new List<DbRow>(rows.Count);
        foreach (var r in rows)
        {
            var row = new DbRow();
            for (int i = 0; i < Math.Min(r.Count, schema.Count); i++)
                row.Set(schema[i].Name, r[i]);
            dbRows.Add(row);
        }
        yield return new RecordBatch(dbRows, schema);
        await ValueTask.CompletedTask;
    }
}

public sealed class SafeHashAggregateExec(
    IExecutionPlan input,
    IReadOnlyList<Func<DbRow, DbValue>> groupKeys,
    IReadOnlyList<(string Name, Func<IReadOnlyList<DbValue>, DbValue> Accum, Func<DbRow, DbValue> Extract)> aggregates)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema =>
        aggregates.Select(a => new ColumnMeta(a.Name, "any")).ToList();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var groups = new Dictionary<string, List<DbRow>>();
        await foreach (var batch in input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            foreach (var row in batch.Rows)
            {
                var key = string.Join("|", groupKeys.Select(k => k(row).ToString() ?? ""));
                if (!groups.TryGetValue(key, out var grp)) groups[key] = grp = [];
                grp.Add(row);
            }
        }
        var schema = Schema;
        var result = new List<DbRow>(groups.Count);
        foreach (var (_, g) in groups)
        {
            var row = new DbRow();
            foreach (var (name, accum, extract) in aggregates)
                row.Set(name, accum(g.Select(extract).ToList()));
            result.Add(row);
        }
        yield return new RecordBatch(result, schema);
    }
}

public sealed class SafeSortExec(
    IExecutionPlan input,
    IReadOnlyList<(Func<DbRow, DbValue> Key, bool Ascending)> order)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema => input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var all = new List<DbRow>();
        IReadOnlyList<ColumnMeta>? schema = null;
        await foreach (var b in input.ExecuteAsync(ctx).ConfigureAwait(false))
        { all.AddRange(b.Rows); schema ??= b.Schema; }
        schema ??= input.Schema;
        all.Sort((a, b) =>
        {
            foreach (var (key, asc) in order)
            {
                int c = Compare(key(a), key(b));
                if (c != 0) return asc ? c : -c;
            }
            return 0;
        });
        yield return new RecordBatch(all, schema);
    }

    private static int Compare(DbValue a, DbValue b) => (a, b) switch
    {
        (DbValue.Integer ia, DbValue.Integer ib) => ia.Value.CompareTo(ib.Value),
        (DbValue.Float fa, DbValue.Float fb) => fa.Value.CompareTo(fb.Value),
        (DbValue.Text ta, DbValue.Text tb) => string.Compare(ta.Value, tb.Value, StringComparison.Ordinal),
        (DbValue.Null, DbValue.Null) => 0,
        (DbValue.Null, _) => -1,
        (_, DbValue.Null) => 1,
        _ => 0,
    };
}

// ── Filter ────────────────────────────────────────────────────────────────────

/// <summary>Row-level predicate filter. Mirrors Rust: FilterExec</summary>
public sealed class SafeFilterExec(IExecutionPlan input, Func<DbRow, bool> predicate)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema => input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        await foreach (var batch in input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var filtered = new List<DbRow>(batch.RowCount);
            foreach (var row in batch.Rows)
                if (predicate(row)) filtered.Add(row);
            yield return new RecordBatch(filtered, batch.Schema);
        }
    }
}

// ── Project ───────────────────────────────────────────────────────────────────

/// <summary>Column projection. Mirrors Rust: ProjectExec</summary>
public sealed class SafeProjectExec(
    IExecutionPlan input,
    IReadOnlyList<(string OutputName, Func<DbRow, DbValue> Expr)> projections)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema =>
        projections.Select(p => new ColumnMeta(p.OutputName, "any")).ToList();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var schema = Schema;
        await foreach (var batch in input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var rows = new List<DbRow>(batch.RowCount);
            foreach (var row in batch.Rows)
            {
                var newRow = new DbRow();
                foreach (var (name, expr) in projections)
                    newRow.Set(name, expr(row));
                rows.Add(newRow);
            }
            yield return new RecordBatch(rows, schema);
        }
    }
}

// ── Limit ─────────────────────────────────────────────────────────────────────

/// <summary>LIMIT / OFFSET operator. Mirrors Rust: LimitExec</summary>
public sealed class SafeLimitExec(IExecutionPlan input, long limit, long offset) : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema => input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        long skipped = 0, taken = 0;
        await foreach (var batch in input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var rows = new List<DbRow>();
            foreach (var row in batch.Rows)
            {
                if (skipped < offset) { skipped++; continue; }
                if (taken >= limit) break;
                rows.Add(row);
                taken++;
            }
            if (rows.Count > 0) yield return new RecordBatch(rows, batch.Schema);
            if (taken >= limit) break;
        }
    }
}

// ── Distinct ──────────────────────────────────────────────────────────────────

/// <summary>Hash-based duplicate elimination. Mirrors Rust: DistinctExec</summary>
public sealed class SafeDistinctExec(IExecutionPlan input) : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema => input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var seen = new HashSet<string>();
        await foreach (var batch in input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var distinct = new List<DbRow>();
            foreach (var row in batch.Rows)
            {
                // Use a composite hash key rather than string concatenation to avoid
                // false collisions from delimiter characters in column values.
                var key = ComputeRowKey(row);
                if (seen.Add(key)) distinct.Add(row);
            }
            if (distinct.Count > 0) yield return new RecordBatch(distinct, batch.Schema);
        }
    }

    private static string ComputeRowKey(DbRow row)
    {
        var h = new HashCode();
        foreach (var (k, v) in row.Columns)
        {
            h.Add(k, StringComparer.Ordinal);
            h.Add(v);
        }
        return h.ToHashCode().ToString("x8");
    }
}

// ── NestedLoopJoin ────────────────────────────────────────────────────────────

/// <summary>Nested-loop join. Mirrors Rust: NestedLoopJoinExec</summary>
public sealed class SafeNestedLoopJoinExec(
    IExecutionPlan left,
    IExecutionPlan right,
    Func<DbRow, DbRow, bool>? condition,
    JoinType joinType = JoinType.Inner)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema =>
        left.Schema.Concat(right.Schema).ToList();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var leftRows = new List<DbRow>();
        await foreach (var b in left.ExecuteAsync(ctx).ConfigureAwait(false))
            leftRows.AddRange(b.Rows);
        var rightRows = new List<DbRow>();
        await foreach (var b in right.ExecuteAsync(ctx).ConfigureAwait(false))
            rightRows.AddRange(b.Rows);

        var schema = Schema;
        var results = new List<DbRow>();
        foreach (var l in leftRows)
        {
            bool matched = false;
            foreach (var r in rightRows)
            {
                if (condition is null || condition(l, r))
                {
                    results.Add(MergeRows(l, r));
                    matched = true;
                }
            }
            if (!matched && (joinType is JoinType.LeftOuter or JoinType.FullOuter))
                results.Add(MergeRows(l, NullRow(right.Schema)));
        }
        if (joinType is JoinType.RightOuter or JoinType.FullOuter)
        {
            foreach (var r in rightRows)
            {
                bool matched = leftRows.Any(l => condition is null || condition(l, r));
                if (!matched) results.Add(MergeRows(NullRow(left.Schema), r));
            }
        }
        yield return new RecordBatch(results, schema);
    }

    private static DbRow MergeRows(DbRow l, DbRow r)
    {
        var row = new DbRow();
        foreach (var (k, v) in l.Columns) row.Set(k, v);
        foreach (var (k, v) in r.Columns) row.Set(k, v);
        return row;
    }

    private static DbRow NullRow(IReadOnlyList<ColumnMeta> schema)
    {
        var row = new DbRow();
        foreach (var col in schema) row.Set(col.Name, DbValue.Null.Instance);
        return row;
    }
}

// ── HashJoin ──────────────────────────────────────────────────────────────────

/// <summary>Hash equi-join. Mirrors Rust: HashJoinExec</summary>
public sealed class SafeHashJoinExec(
    IExecutionPlan left,
    IExecutionPlan right,
    Func<DbRow, DbValue> leftKey,
    Func<DbRow, DbValue> rightKey)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema =>
        left.Schema.Concat(right.Schema).ToList();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var hashTable = new Dictionary<string, List<DbRow>>();
        await foreach (var b in left.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            foreach (var row in b.Rows)
            {
                var k = leftKey(row).ToString() ?? "";
                if (!hashTable.TryGetValue(k, out var lst)) hashTable[k] = lst = [];
                lst.Add(row);
            }
        }
        var schema = Schema;
        var results = new List<DbRow>();
        await foreach (var b in right.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            foreach (var r in b.Rows)
            {
                var k = rightKey(r).ToString() ?? "";
                if (!hashTable.TryGetValue(k, out var matches)) continue;
                foreach (var l in matches)
                {
                    var row = new DbRow();
                    foreach (var (col, val) in l.Columns) row.Set(col, val);
                    foreach (var (col, val) in r.Columns) row.Set(col, val);
                    results.Add(row);
                }
            }
        }
        yield return new RecordBatch(results, schema);
    }
}

// ── Unnest ────────────────────────────────────────────────────────────────────

/// <summary>Array column explosion. Mirrors Rust: UnnestExec</summary>
public sealed class SafeUnnestExec(IExecutionPlan input, string arrayColumn, string outputColumn)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema
    {
        get
        {
            var cols = input.Schema
                .Where(c => !c.Name.Equals(arrayColumn, StringComparison.OrdinalIgnoreCase))
                .ToList();
            cols.Add(new ColumnMeta(outputColumn, "any"));
            return cols;
        }
    }

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var schema = Schema;
        await foreach (var batch in input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var results = new List<DbRow>();
            foreach (var row in batch.Rows)
            {
                if (row.Get(arrayColumn) is DbValue.Array arr)
                {
                    foreach (var item in arr.Items)
                    {
                        var newRow = new DbRow();
                        foreach (var (k, v) in row.Columns)
                            if (!k.Equals(arrayColumn, StringComparison.OrdinalIgnoreCase))
                                newRow.Set(k, v);
                        newRow.Set(outputColumn, item);
                        results.Add(newRow);
                    }
                }
            }
            yield return new RecordBatch(results, schema);
        }
    }
}
