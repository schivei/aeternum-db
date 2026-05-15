using AeternumDB.PoC.Safe.Executor;
using AeternumDB.PoC.Shared.Types;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace AeternumDB.PoC.Safe.Benchmarks;

[SimpleJob(RuntimeMoniker.Net80)]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class RowScanBenchmarks
{
    private SafeInMemoryTableProvider _provider = null!;
    private SafeExecutionContext _ctx = null!;
    private SafeSeqScanExec _scanAll = null!;
    private SafeSeqScanExec _scanFiltered = null!;

    [Params(1_000, 10_000, 100_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _provider = new SafeInMemoryTableProvider();
        var schema = new List<ColumnMeta>
        {
            new("id", "integer"),
            new("value", "float"),
            new("label", "varchar"),
        };
        _provider.CreateTable("bench", schema);
        var rows = Enumerable.Range(0, RowCount).Select(i =>
        {
            var row = new DbRow();
            row.Set("id", new DbValue.Integer(i));
            row.Set("value", new DbValue.Float(i * 1.1));
            row.Set("label", new DbValue.Text($"row-{i}"));
            return row;
        }).ToList();
        _provider.InsertAsync("bench", rows).AsTask().Wait();
        _ctx = new SafeExecutionContext(_provider);
        _scanAll = new SafeSeqScanExec("bench", schema);
        _scanFiltered = new SafeSeqScanExec("bench", schema,
            row => row.Get("id") is DbValue.Integer i && i.Value % 2 == 0);
    }

    [Benchmark(Description = "SeqScan all rows")]
    public async Task SeqScanAll()
    {
        await foreach (var _ in _scanAll.ExecuteAsync(_ctx)) { }
    }

    [Benchmark(Description = "SeqScan with filter")]
    public async Task SeqScanFiltered()
    {
        await foreach (var _ in _scanFiltered.ExecuteAsync(_ctx)) { }
    }

    [Benchmark(Description = "Values inline")]
    public async Task ValuesInline()
    {
        var schema = new List<ColumnMeta> { new("x", "integer"), new("y", "float") };
        var rows = Enumerable.Range(0, RowCount)
            .Select(i => (IReadOnlyList<DbValue>)new List<DbValue>
                { new DbValue.Integer(i), new DbValue.Float(i * 1.5) })
            .ToList();
        var exec = new SafeValuesExec(rows, schema);
        await foreach (var _ in exec.ExecuteAsync(_ctx)) { }
    }

    [Benchmark(Description = "HashAggregate COUNT")]
    public async Task HashAggregateCount()
    {
        var agg = new SafeHashAggregateExec(
            _scanAll,
            groupKeys: [],
            aggregates: [("count", vals => new DbValue.Integer(vals.Count(v => !v.IsNull)), row => row.Get("id"))]);
        await foreach (var _ in agg.ExecuteAsync(_ctx)) { }
    }

    [Benchmark(Description = "Sort by id")]
    public async Task SortById()
    {
        var sort = new SafeSortExec(_scanAll, [(row => row.Get("id"), true)]);
        await foreach (var _ in sort.ExecuteAsync(_ctx)) { }
    }
}
