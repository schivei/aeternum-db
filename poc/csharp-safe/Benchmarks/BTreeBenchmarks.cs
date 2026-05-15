using AeternumDB.PoC.Safe.Index;
using BenchmarkDotNet.Attributes;

namespace AeternumDB.PoC.Safe.Benchmarks;

[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class BTreeBenchmarks
{
    private SafeBTree<long, string> _tree = null!;

    [Params(1_000, 10_000, 100_000)]
    public int EntryCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _tree = new SafeBTree<long, string>(100);
        for (long i = 0; i < EntryCount; i++)
            await _tree.InsertAsync(i, $"value-{i}");
    }

    [Benchmark(Description = "BTree Insert")]
    public async Task Insert()
    {
        var t = new SafeBTree<long, string>(100);
        for (long i = 0; i < EntryCount; i++) await t.InsertAsync(i, $"v{i}");
    }

    [Benchmark(Description = "BTree Search")]
    public async Task Search()
    {
        for (long i = 0; i < EntryCount; i++) await _tree.SearchAsync(i);
    }

    [Benchmark(Description = "BTree Range")]
    public async Task Range() => await _tree.RangeAsync(0, EntryCount / 2);

    [Benchmark(Description = "BTree Delete")]
    public async Task Delete()
    {
        var t = new SafeBTree<long, string>(100);
        for (long i = 0; i < EntryCount; i++) await t.InsertAsync(i, $"v{i}");
        for (long i = 0; i < EntryCount; i++) await t.DeleteAsync(i);
    }

    [Benchmark(Description = "BTree BulkLoad")]
    public async Task BulkLoad()
    {
        var entries = Enumerable.Range(0, EntryCount).Select(i => ((long)i, $"v{i}")).ToList();
        var t = new SafeBTree<long, string>(100);
        await t.BulkLoadAsync(entries);
    }
}
