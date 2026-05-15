using AeternumDB.PoC.Unsafe.Core;
using BenchmarkDotNet.Attributes;

namespace AeternumDB.PoC.Unsafe.Benchmarks;

/// <summary>
/// B-tree benchmarks mirroring core/benches/btree_bench.rs.
/// Uses the unsafe B-tree variant with bounds-check-free binary search.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public class BTreeBenchmarks
{
    [Params(100, 500, 1000)]
    public int N { get; set; }

    // ── Sequential insert ─────────────────────────────────────────────────────

    [Benchmark(Description = "sequential_insert")]
    public void SequentialInsert()
    {
        var tree = new BTree<long, string>(order: 100);
        for (long i = 0; i < N; i++)
            tree.Insert(i, i.ToString());
    }

    // ── Random insert ─────────────────────────────────────────────────────────

    [Benchmark(Description = "random_insert")]
    public void RandomInsert()
    {
        var keys = BuildShuffledKeys(N);
        var tree = new BTree<long, string>(order: 100);
        foreach (long k in keys)
            tree.Insert(k, k.ToString());
    }

    // ── Point query ───────────────────────────────────────────────────────────

    private BTree<long, string>? _pointTree;

    [GlobalSetup(Targets = new[] { nameof(PointQuery) })]
    public void SetupPointQuery()
    {
        _pointTree = new BTree<long, string>(order: 100);
        for (long i = 0; i < N; i++)
            _pointTree.Insert(i, i.ToString());
    }

    [Benchmark(Description = "point_query")]
    public string? PointQuery()
    {
        string? last = null;
        for (long i = 0; i < N; i++)
            last = _pointTree!.Search(i);
        return last;
    }

    // ── Range scan ────────────────────────────────────────────────────────────

    private BTree<long, string>? _rangeTree;

    [GlobalSetup(Targets = new[] { nameof(RangeScan) })]
    public void SetupRangeScan()
    {
        _rangeTree = new BTree<long, string>(order: 100);
        for (long i = 0; i < N; i++)
            _rangeTree.Insert(i, i.ToString());
    }

    [Benchmark(Description = "range_scan")]
    public int RangeScan()
    {
        var results = _rangeTree!.Range(0L, (long)(N - 1));
        return results.Count;
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    [Benchmark(Description = "delete")]
    public void Delete()
    {
        var tree = new BTree<long, string>(order: 100);
        for (long i = 0; i < N; i++)
            tree.Insert(i, i.ToString());
        for (long i = 0; i < N; i++)
            tree.Delete(i);
    }

    // ── Bulk load ─────────────────────────────────────────────────────────────

    [Benchmark(Description = "bulk_load")]
    public void BulkLoad()
    {
        var entries = new List<(long, string)>(N);
        for (long i = 0; i < N; i++)
            entries.Add((i, i.ToString()));

        var tree = new BTree<long, string>(order: 100);
        tree.BulkLoad(entries);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static long[] BuildShuffledKeys(int n)
    {
        var keys = new long[n];
        for (int i = 0; i < n; i++) keys[i] = i;
        for (int i = 0; i < n; i++)
        {
            int j = (i * 17 + 5) % n;
            (keys[i], keys[j]) = (keys[j], keys[i]);
        }
        return keys;
    }
}
