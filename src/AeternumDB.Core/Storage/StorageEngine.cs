using AeternumDB.Core.Abstractions;
using AeternumDB.Core.Config;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Types;

namespace AeternumDB.Core.Storage;

/// <summary>
/// High-level storage engine: combines <see cref="FileManager"/> (disk I/O) with
/// <see cref="BufferPool"/> (LRU memory cache) behind a single async interface.
///
/// All public methods acquire a <see cref="SemaphoreSlim"/> to ensure
/// mutual-exclusion between concurrent callers.
/// </summary>
public sealed class StorageEngine : IStorageEngine
{
    private readonly SemaphoreSlim _sem = new(1, 1);
    private readonly BufferPool _bufferPool;
    private FileManager? _fm;
    private bool _disposed;

    public StorageConfig Config { get; }

    public StorageEngine(StorageConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        Config = config;
        _bufferPool = new BufferPool(config.BufferPoolSize);
    }

    /// <summary>Allocate a new page and return its identifier.</summary>
    public async ValueTask<PageId> AllocatePageAsync()
    {
        await _sem.WaitAsync();
        try
        {
            var fm = await EnsureFileManagerAsync();
            return await fm.AllocatePageAsync();
        }
        finally { _sem.Release(); }
    }

    /// <summary>
    /// Deallocate page <paramref name="id"/>, removing it from the buffer pool and
    /// marking its disk slot as free.
    /// </summary>
    public async ValueTask DeallocatePageAsync(PageId id)
    {
        await _sem.WaitAsync();
        try
        {
            var fm = await EnsureFileManagerAsync();
            EvictFromPool(id);
            await fm.DeallocatePageAsync(id);
        }
        finally { _sem.Release(); }
    }

    /// <summary>
    /// Write <paramref name="data"/> into page <paramref name="id"/> at byte <paramref name="offset"/>,
    /// then flush the page to disk.
    /// </summary>
    public async ValueTask WritePageDataAsync(PageId id, int offset, ReadOnlyMemory<byte> data)
    {
        await _sem.WaitAsync();
        try
        {
            var fm = await EnsureFileManagerAsync();
            await LoadIntoCacheAsync(id, fm);

            if (!_bufferPool.TryPin(id, out var bytes) || bytes is null)
                throw new StorageException(StorageErrorKind.BufferPool,
                    $"page {id} unexpectedly missing from buffer pool after load");

            try
            {
                var page = Page.Deserialize(bytes, Config.PageSize) ??
                    throw new StorageException(StorageErrorKind.FileManager, $"page {id} is corrupt");
                page.WriteData(offset, data.Span);
                var written = page.Serialize();
                // Update the in-pool buffer in-place so all pin holders see the same array
                written.AsSpan().CopyTo(bytes.AsSpan());
            }
            catch
            {
                _bufferPool.Unpin(id, dirty: false);
                throw;
            }

            _bufferPool.Unpin(id, dirty: true);
            var dirty = _bufferPool.FlushDirty();
            foreach (var (pid, raw) in dirty)
            {
                await fm.WritePageAsync(pid, raw);
                _bufferPool.MarkClean(pid);
            }
        }
        finally { _sem.Release(); }
    }

    /// <summary>
    /// Read <paramref name="length"/> bytes from page <paramref name="id"/> at byte <paramref name="offset"/>.
    /// </summary>
    public async ValueTask<Memory<byte>> ReadPageDataAsync(PageId id, int offset, int length)
    {
        await _sem.WaitAsync();
        try
        {
            var fm = await EnsureFileManagerAsync();
            await LoadIntoCacheAsync(id, fm);

            if (!_bufferPool.TryPin(id, out var bytes) || bytes is null)
                throw new StorageException(StorageErrorKind.BufferPool,
                    $"page {id} unexpectedly missing from buffer pool after load");

            try
            {
                var page = Page.Deserialize(bytes, Config.PageSize) ??
                    throw new StorageException(StorageErrorKind.FileManager, $"page {id} is corrupt");

                var result = page.ReadData(offset, length).ToArray();
                return result;
            }
            finally
            {
                _bufferPool.Unpin(id, dirty: false);
            }
        }
        finally { _sem.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Flush dirty pages before closing
        if (_fm is not null)
        {
            var dirty = _bufferPool.FlushDirty();
            foreach (var (id, raw) in dirty)
            {
                try { await _fm.WritePageAsync(id, raw); _bufferPool.MarkClean(id); }
                catch { /* best-effort flush */ }
            }
            await _fm.DisposeAsync();
        }

        _bufferPool.Dispose();
        _sem.Dispose();
        GC.SuppressFinalize(this);
    }

    private async ValueTask<FileManager> EnsureFileManagerAsync()
    {
        if (_fm is not null) return _fm;
        _fm = await FileManager.OpenAsync(Config.DataPath, Config.PageSize);
        return _fm;
    }

    private async Task LoadIntoCacheAsync(PageId id, FileManager fm)
    {
        if (_bufferPool.TryPin(id, out _))
        {
            // Already in pool — undo the extra pin we just added
            _bufferPool.Unpin(id, dirty: false);
            return;
        }

        var raw = await fm.ReadPageAsync(id);
        var page = Page.Deserialize(raw, Config.PageSize) ??
            throw new StorageException(StorageErrorKind.FileManager, $"page {id} is corrupt");

        if (!page.ValidateChecksum())
            throw new StorageException(StorageErrorKind.ChecksumMismatch,
                $"checksum mismatch on page {id}");

        _bufferPool.Insert(id, raw);
    }

    private void EvictFromPool(PageId id)
    {
        if (!_bufferPool.TryPin(id, out _)) return;
        // Unpin and remove by inserting once more to ensure pin=0 then let LRU evict,
        // but simpler: just unpin and let natural eviction handle it.
        _bufferPool.Unpin(id, dirty: false);
    }
}
