using AeternumDB.PoC.Unsafe.Core;
using BenchmarkDotNet.Attributes;

namespace AeternumDB.PoC.Unsafe.Benchmarks;

/// <summary>
/// Storage benchmarks mirroring core/benches/storage_bench.rs.
/// Hot paths use <see cref="NativeMemory"/> (via <see cref="PageBuffer"/>)
/// instead of <see cref="System.Buffers.ArrayPool{T}"/>.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public class StorageBenchmarks
{
    private const int PageSize = 8192;
    private static readonly byte[] Payload = new byte[64];

    [Params(100, 500, 1000)]
    public int N { get; set; }

    // ── Sequential write ──────────────────────────────────────────────────────

    [Benchmark(Description = "sequential_write")]
    public void SequentialWrite()
    {
        using var buf = new PageBuffer(PageSize);
        for (int i = 0; i < N; i++)
        {
            ulong id = buf.AllocatePage();
            buf.WritePage(id, 0, Payload);
        }
    }

    // ── Random read ───────────────────────────────────────────────────────────

    [Benchmark(Description = "random_read")]
    public void RandomRead()
    {
        using var buf = new PageBuffer(PageSize);
        var ids = new ulong[N];
        for (int i = 0; i < N; i++)
        {
            ids[i] = buf.AllocatePage();
            buf.WritePage(ids[i], 0, Payload);
        }

        Span<byte> dest = stackalloc byte[64];
        int idx = 0;
        for (int i = 0; i < N; i++)
        {
            idx = XorShuffleIndex(idx, N);
            buf.ReadPage(ids[idx], 0, 64, dest);
        }
    }

    // ── Mixed 80 % read / 20 % write ─────────────────────────────────────────

    [Benchmark(Description = "mixed_80r_20w")]
    public void MixedWorkload()
    {
        using var buf = new PageBuffer(PageSize);
        var ids = new ulong[N];
        for (int i = 0; i < N; i++)
        {
            ids[i] = buf.AllocatePage();
            buf.WritePage(ids[i], 0, Payload);
        }

        Span<byte> dest = stackalloc byte[64];
        for (int i = 0; i < N; i++)
        {
            if (i % 5 == 0)
                buf.WritePage(ids[i % ids.Length], 0, Payload);
            else
                buf.ReadPage(ids[i % ids.Length], 0, 64, dest);
        }
    }

    // ── Buffer-hit read ───────────────────────────────────────────────────────

    [Benchmark(Description = "buffer_hit_read")]
    public void BufferHitRead()
    {
        using var buf = new PageBuffer(PageSize);
        var ids = new ulong[N];
        for (int i = 0; i < N; i++)
        {
            ids[i] = buf.AllocatePage();
            buf.WritePage(ids[i], 0, Payload);
        }

        Span<byte> dest = stackalloc byte[64];
        foreach (var id in ids)
            buf.ReadPage(id, 0, 64, dest);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static int XorShuffleIndex(int current, int n) =>
        (current ^ (current >> 3) ^ 0xABCD) % n;
}
