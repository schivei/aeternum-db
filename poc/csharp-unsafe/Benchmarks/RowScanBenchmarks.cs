using AeternumDB.PoC.Unsafe.Core;
using BenchmarkDotNet.Attributes;

namespace AeternumDB.PoC.Unsafe.Benchmarks;

/// <summary>
/// Row-scan benchmarks mirroring core/benches/executor_bench.rs.
/// Uses the unsafe <see cref="RowStore"/> with <c>delegate*</c> function pointers
/// and bounds-check-free inner loop.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public unsafe class RowScanBenchmarks
{
    private RowStore _store = null!;

    [GlobalSetup]
    public void Setup()
    {
        _store = new RowStore(initialCapacity: 1024);
        for (int i = 0; i < 1000; i++)
            _store.Add(new Row(i, i % 80, $"user_{i}"));
    }

    // ── Seq scan without filter ───────────────────────────────────────────────

    [Benchmark(Description = "seq_scan_no_filter")]
    public int SeqScanNoFilter() => _store.Scan();

    // ── Seq scan with filter (age > 18) ──────────────────────────────────────

    // Static method so we can take its address as delegate*.
    private static bool AgeFilter(in Row r) => r.Age > 18;

    [Benchmark(Description = "seq_scan_with_filter")]
    public int SeqScanWithFilter() => _store.Scan(&AgeFilter);

    // ── VALUES: materialise 100 inline rows ───────────────────────────────────

    [Benchmark(Description = "values_executor_100_rows")]
    public int ValuesExecutor()
    {
        var store = new RowStore(initialCapacity: 128);
        for (int i = 0; i < 100; i++)
            store.Add(new Row(i, i, $"row_{i}"));
        return store.Count;
    }
}
