using AeternumDB.PoC.Unsafe.Index;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace AeternumDB.PoC.Unsafe.Benchmarks;

/// <summary>B-tree index benchmarks — mirrors core/benches/ index scenarios.</summary>
[SimpleJob(RuntimeMoniker.Net80)]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class BTreeBenchmarks
{
    private UnsafeBTree<long, string> _tree = null!;

    [Params(1_000, 10_000, 100_000)]
    public int EntryCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _tree = new UnsafeBTree<long, string>(100);
        for (long i = 0; i < EntryCount; i++)
            await _tree.InsertAsync(i, $"value-{i}");
    }

    [Benchmark(Description = "BTree Insert")]
    public async Task Insert()
    {
        var t = new UnsafeBTree<long, string>(100);
        for (long i = 0; i < EntryCount; i++)
            await t.InsertAsync(i, $"v{i}");
    }

    [Benchmark(Description = "BTree Search")]
    public async Task Search()
    {
        for (long i = 0; i < EntryCount; i++)
            await _tree.SearchAsync(i);
    }

    [Benchmark(Description = "BTree Range")]
    public async Task Range()
    {
        long mid = EntryCount / 2;
        await _tree.RangeAsync(0, mid);
    }

    [Benchmark(Description = "BTree Delete")]
    public async Task Delete()
    {
        var t = new UnsafeBTree<long, string>(100);
        for (long i = 0; i < EntryCount; i++)
            await t.InsertAsync(i, $"v{i}");
        for (long i = 0; i < EntryCount; i++)
            await t.DeleteAsync(i);
    }

    [Benchmark(Description = "BTree BulkLoad")]
    public async Task BulkLoad()
    {
        var entries = Enumerable.Range(0, EntryCount)
            .Select(i => ((long)i, $"v{i}"))
            .ToList();
        var t = new UnsafeBTree<long, string>(100);
        await t.BulkLoadAsync(entries);
    }
}
