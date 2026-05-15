using AeternumDB.PoC.Safe.Core;
using BenchmarkDotNet.Attributes;

namespace AeternumDB.PoC.Safe.Benchmarks;

/// <summary>
/// Row-scan benchmarks mirroring core/benches/executor_bench.rs.
///
/// Operations:
///   - Seq scan 1000 rows (no filter)
///   - Seq scan 1000 rows (age > 18 filter)
///   - VALUES: materialise 100 inline rows
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public class RowScanBenchmarks
{
    private RowStore _store = null!;

    [GlobalSetup]
    public void Setup()
    {
        _store = new RowStore();
        for (int i = 0; i < 1000; i++)
            _store.Add(new Row(i, i % 80, $"user_{i}"));
    }

    // ── Seq scan without filter ───────────────────────────────────────────────

    [Benchmark(Description = "seq_scan_no_filter")]
    public int SeqScanNoFilter() => _store.Scan();

    // ── Seq scan with filter (age > 18) ──────────────────────────────────────

    [Benchmark(Description = "seq_scan_with_filter")]
    public int SeqScanWithFilter() => _store.Scan(static r => r.Age > 18);

    // ── VALUES: materialise 100 inline rows ───────────────────────────────────

    [Benchmark(Description = "values_executor_100_rows")]
    public int ValuesExecutor()
    {
        var store = new RowStore();
        for (int i = 0; i < 100; i++)
            store.Add(new Row(i, i, $"row_{i}"));
        return store.Count;
    }
}
