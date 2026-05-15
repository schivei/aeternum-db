using AeternumDB.PoC.Unsafe.Storage;
using AeternumDB.PoC.Shared.Config;
using AeternumDB.PoC.Shared.Types;
using BenchmarkDotNet.Attributes;

namespace AeternumDB.PoC.Unsafe.Benchmarks;

/// <summary>
/// Storage layer benchmarks for the unsafe PoC.
/// Mirrors core/benches/ storage scenarios.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class StorageBenchmarks : IDisposable
{
    private UnsafeStorageEngine _engine = null!;
    private UnsafeBufferPool _pool = null!;
    private UnsafeFileManager _fm = null!;
    private string _tmpFile = null!;

    [Params(100, 1_000, 10_000)]
    public int PageCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _tmpFile = Path.GetTempFileName();
        var cfg = new StorageConfig { DataPath = _tmpFile, BufferPoolSize = 512, PageSize = 8192 };
        _fm = new UnsafeFileManager(_tmpFile, cfg.PageSize);
        _pool = new UnsafeBufferPool(cfg.BufferPoolSize, cfg.PageSize);
        _engine = new UnsafeStorageEngine(_fm, _pool, cfg);
        await Task.CompletedTask;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _engine.DisposeAsync();
        if (File.Exists(_tmpFile)) File.Delete(_tmpFile);
    }

    [Benchmark(Description = "StorageWrite")]
    public async Task WritePages()
    {
        var data = new byte[8192 - 16];
        for (int i = 0; i < PageCount; i++)
        {
            var id = await _engine.AllocatePageAsync();
            await _engine.WritePageDataAsync(id, 0, data);
        }
    }

    [Benchmark(Description = "StorageRead")]
    public async Task ReadPages()
    {
        var ids = new List<PageId>();
        var data = new byte[8192 - 16];
        for (int i = 0; i < PageCount; i++)
        {
            var id = await _engine.AllocatePageAsync();
            await _engine.WritePageDataAsync(id, 0, data);
            ids.Add(id);
        }
        foreach (var id in ids)
            await _engine.ReadPageDataAsync(id, 0, 64);
    }

    [Benchmark(Description = "BufferHit")]
    public async Task BufferHit()
    {
        var id = await _engine.AllocatePageAsync();
        var data = new byte[64];
        await _engine.WritePageDataAsync(id, 0, data);
        for (int i = 0; i < PageCount; i++)
            await _engine.ReadPageDataAsync(id, 0, 64);
    }

    public void Dispose() => _engine.DisposeAsync().AsTask().Wait();
}
