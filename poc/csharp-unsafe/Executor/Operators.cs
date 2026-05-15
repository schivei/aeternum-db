using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

namespace AeternumDB.PoC.Unsafe.Executor;

// ── Execution Context ─────────────────────────────────────────────────────────

[Injectable<IExecutionContext>(ServiceLifetime.Scoped)]
public sealed class UnsafeExecutionContext : IExecutionContext
{
    private long _idCounter;

    public ITableProvider TableProvider { get; }
    public string CurrentUser { get; }

    public UnsafeExecutionContext(ITableProvider tableProvider, string user = "system")
    {
        TableProvider = tableProvider;
        CurrentUser = user;
    }

    public long NextObjectId() => Interlocked.Increment(ref _idCounter);
}

// ── Table Provider (in-memory, unsafe row iteration) ─────────────────────────

/// <summary>
/// In-memory table provider with bounds-check-free iteration via
/// <see cref="MemoryMarshal.GetArrayDataReference{T}"/>.
/// Mirrors Rust: InMemoryTableProvider
/// </summary>
[Injectable<ITableProvider>(ServiceLifetime.Singleton)]
public sealed class UnsafeInMemoryTableProvider : ITableProvider
{
    private readonly Dictionary<string, (List<ColumnMeta> Schema, List<DbRow> Rows)>
        _tables = new(StringComparer.OrdinalIgnoreCase);

    public bool TableExists(string table) => _tables.ContainsKey(table);

    public void CreateTable(string table, IReadOnlyList<ColumnMeta> schema)
    {
        var cols = schema.ToList();
        _tables[table] = (cols, []);
    }

    public IReadOnlyList<ColumnMeta> Schema(string table) =>
        _tables.TryGetValue(table, out var t) ? t.Schema : [];

    public async IAsyncEnumerable<DbRow> ScanAsync(string table)
    {
        if (!_tables.TryGetValue(table, out var t))
            yield break;
        var rows = t.Rows;
        for (int i = 0; i < rows.Count; i++)
            yield return rows[i];
        await ValueTask.CompletedTask; // suppress CS1998
    }

    public ValueTask<int> InsertAsync(string table, IReadOnlyList<DbRow> rows)
    {
        if (!_tables.TryGetValue(table, out var t))
            return ValueTask.FromResult(0);
        t.Rows.AddRange(rows);
        return ValueTask.FromResult(rows.Count);
    }

    public ValueTask<int> DeleteAsync(string table)
    {
        if (!_tables.TryGetValue(table, out var t))
            return ValueTask.FromResult(0);
        int n = t.Rows.Count;
        t.Rows.Clear();
        return ValueTask.FromResult(n);
    }
}

// ── SeqScan ───────────────────────────────────────────────────────────────────

/// <summary>Sequential table scan. Mirrors Rust: SeqScanExec</summary>
public sealed class SeqScanExec : IExecutionPlan
{
    private readonly string _table;
    private readonly Func<DbRow, bool>? _filter;
    private readonly IReadOnlyList<ColumnMeta> _schema;

    public SeqScanExec(string table, IReadOnlyList<ColumnMeta> schema,
        Func<DbRow, bool>? filter = null)
    {
        _table = table;
        _schema = schema;
        _filter = filter;
    }

    public IReadOnlyList<ColumnMeta> Schema => _schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var rows = new List<DbRow>();
        await foreach (var row in ctx.TableProvider.ScanAsync(_table).ConfigureAwait(false))
        {
            if (_filter is null || _filter(row))
                rows.Add(row);
        }
        yield return new RecordBatch(rows, _schema);
    }
}

// ── Filter ────────────────────────────────────────────────────────────────────

/// <summary>Row-level predicate filter. Mirrors Rust: FilterExec</summary>
public sealed class FilterExec(IExecutionPlan input, Func<DbRow, bool> predicate)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema => input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        await foreach (var batch in input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            var filtered = new List<DbRow>(batch.RowCount);
            var rowSpan = batch.Rows as List<DbRow>; // avoid extra allocation
            foreach (var row in batch.Rows)
                if (predicate(row)) filtered.Add(row);
            yield return new RecordBatch(filtered, batch.Schema);
        }
    }
}

// ── Project ───────────────────────────────────────────────────────────────────

/// <summary>Column projection. Mirrors Rust: ProjectExec</summary>
public sealed class ProjectExec(
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

// ── Values ────────────────────────────────────────────────────────────────────

/// <summary>Inline constant rows. Mirrors Rust: ValuesExec</summary>
public sealed class ValuesExec(
    IReadOnlyList<IReadOnlyList<DbValue>> rows,
    IReadOnlyList<ColumnMeta> schema) : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema => schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var dbRows = new List<DbRow>(rows.Count);
        foreach (var rowValues in rows)
        {
            var row = new DbRow();
            for (int i = 0; i < Math.Min(rowValues.Count, schema.Count); i++)
                row.Set(schema[i].Name, rowValues[i]);
            dbRows.Add(row);
        }
        yield return new RecordBatch(dbRows, schema);
        await ValueTask.CompletedTask;
    }
}

// ── Limit ─────────────────────────────────────────────────────────────────────

/// <summary>LIMIT / OFFSET operator. Mirrors Rust: LimitExec</summary>
public sealed class LimitExec(IExecutionPlan input, long limit, long offset) : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema => input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        long skipped = 0;
        long taken = 0;
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
            if (rows.Count > 0)
                yield return new RecordBatch(rows, batch.Schema);
            if (taken >= limit) break;
        }
    }
}

// ── Sort ──────────────────────────────────────────────────────────────────────

/// <summary>In-memory sort. Mirrors Rust: SortExec</summary>
public sealed class SortExec(
    IExecutionPlan input,
    IReadOnlyList<(Func<DbRow, DbValue> Key, bool Ascending)> orderBy)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema => input.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var all = new List<DbRow>();
        IReadOnlyList<ColumnMeta>? schema = null;
        await foreach (var batch in input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            all.AddRange(batch.Rows);
            schema ??= batch.Schema;
        }
        schema ??= input.Schema;

        all.Sort((a, b) =>
        {
            foreach (var (key, asc) in orderBy)
            {
                int c = CompareValues(key(a), key(b));
                if (c != 0) return asc ? c : -c;
            }
            return 0;
        });
        yield return new RecordBatch(all, schema);
    }

    private static int CompareValues(DbValue a, DbValue b) => (a, b) switch
    {
        (DbValue.Integer ia, DbValue.Integer ib) => ia.Value.CompareTo(ib.Value),
        (DbValue.Float fa, DbValue.Float fb) => fa.Value.CompareTo(fb.Value),
        (DbValue.Text ta, DbValue.Text tb) => string.Compare(ta.Value, tb.Value, StringComparison.Ordinal),
        (DbValue.Null, DbValue.Null) => 0,
        (DbValue.Null, _) => -1,
        (_, DbValue.Null) => 1,
        _ => string.Compare(a.ToString(), b.ToString(), StringComparison.Ordinal),
    };
}

// ── Distinct ──────────────────────────────────────────────────────────────────

/// <summary>Hash-based duplicate elimination. Mirrors Rust: DistinctExec</summary>
public sealed class DistinctExec(IExecutionPlan input) : IExecutionPlan
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
                // Build a canonical key string for the row
                var key = string.Join("|", row.Columns.Select(c => $"{c.Key}={c.Value}"));
                if (seen.Add(key)) distinct.Add(row);
            }
            if (distinct.Count > 0)
                yield return new RecordBatch(distinct, batch.Schema);
        }
    }
}

// ── HashAggregate ─────────────────────────────────────────────────────────────

/// <summary>Hash GROUP BY aggregate. Mirrors Rust: HashAggregateExec</summary>
public sealed class HashAggregateExec(
    IExecutionPlan input,
    IReadOnlyList<Func<DbRow, DbValue>> groupKeys,
    IReadOnlyList<(string Name, Func<IReadOnlyList<DbValue>, DbValue> Accumulate, Func<DbRow, DbValue> Extract)> aggregates)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema =>
        aggregates.Select(a => new ColumnMeta(a.Name, "any")).ToList();

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx)
    {
        var groups = new Dictionary<string, (List<DbRow> Rows, List<string> KeyValues)>();
        IReadOnlyList<ColumnMeta>? schema = null;

        await foreach (var batch in input.ExecuteAsync(ctx).ConfigureAwait(false))
        {
            schema ??= batch.Schema;
            foreach (var row in batch.Rows)
            {
                var keyParts = groupKeys.Select(k => k(row).ToString() ?? "").ToList();
                var key = string.Join("|", keyParts);
                if (!groups.TryGetValue(key, out var grp))
                {
                    grp = (new List<DbRow>(), keyParts);
                    groups[key] = grp;
                }
                grp.Rows.Add(row);
            }
        }

        var outSchema = Schema;
        var resultRows = new List<DbRow>(groups.Count);
        foreach (var (_, (rows, _)) in groups)
        {
            var outRow = new DbRow();
            foreach (var (name, accum, extract) in aggregates)
            {
                var vals = rows.Select(extract).ToList();
                outRow.Set(name, accum(vals));
            }
            resultRows.Add(outRow);
        }
        yield return new RecordBatch(resultRows, outSchema);
    }
}

// ── NestedLoopJoin ────────────────────────────────────────────────────────────

/// <summary>Nested-loop join. Mirrors Rust: NestedLoopJoinExec</summary>
public sealed class NestedLoopJoinExec(
    IExecutionPlan left,
    IExecutionPlan right,
    Func<DbRow, DbRow, bool>? condition,
    Shared.Sql.JoinType joinType = Shared.Sql.JoinType.Inner)
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
            if (!matched && (joinType is Shared.Sql.JoinType.LeftOuter or Shared.Sql.JoinType.FullOuter))
                results.Add(MergeRows(l, NullRow(right.Schema)));
        }
        if (joinType is Shared.Sql.JoinType.RightOuter or Shared.Sql.JoinType.FullOuter)
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
public sealed class HashJoinExec(
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
public sealed class UnnestExec(IExecutionPlan input, string arrayColumn, string outputColumn)
    : IExecutionPlan
{
    public IReadOnlyList<ColumnMeta> Schema
    {
        get
        {
            var cols = input.Schema.Where(c => !c.Name.Equals(arrayColumn, StringComparison.OrdinalIgnoreCase)).ToList();
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
                var arrVal = row.Get(arrayColumn);
                if (arrVal is DbValue.Array arr)
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
