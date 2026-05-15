using System.Buffers;
using System.Runtime.InteropServices;
using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

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
