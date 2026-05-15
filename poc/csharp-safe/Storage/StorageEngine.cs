using System.Buffers;
using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Config;
using AeternumDB.PoC.Shared.Errors;
using AeternumDB.PoC.Shared.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

namespace AeternumDB.PoC.Safe.Storage;

/// <summary>Safe file manager using async File I/O. Mirrors Rust: FileManager.</summary>
[Injectable<IFileManager>(ServiceLifetime.Singleton)]
public sealed class SafeFileManager : IFileManager
{
    private const int FreeListHeaderPages = 1;
    private readonly string _path;
    private readonly int _pageSize;
    private readonly FileStream _file;
    private readonly List<ulong> _freeList = [];
    private ulong _nextPageId = FreeListHeaderPages;
    private bool _disposed;

    public SafeFileManager(string path, int pageSize)
    {
        _path = path;
        _pageSize = pageSize;
        _file = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite,
            FileShare.None, bufferSize: pageSize, useAsync: true);
    }

    public async ValueTask<PageId> AllocatePageAsync()
    {
        ulong id = _freeList.Count > 0
            ? _freeList[^1]
            : _nextPageId++;
        if (_freeList.Count > 0) _freeList.RemoveAt(_freeList.Count - 1);

        var buf = ArrayPool<byte>.Shared.Rent(_pageSize);
        try
        {
            buf.AsSpan(0, _pageSize).Clear();
            await WriteRawAsync(id, buf.AsMemory(0, _pageSize)).ConfigureAwait(false);
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
        return id;
    }

    public ValueTask DeallocatePageAsync(PageId id)
    {
        _freeList.Add(id);
        return ValueTask.CompletedTask;
    }

    public async ValueTask WritePageAsync(PageId id, ReadOnlyMemory<byte> data)
    {
        if (data.Length != _pageSize)
            throw new StorageException(StorageErrorKind.FileManager,
                $"Expected {_pageSize} bytes, got {data.Length}");
        await WriteRawAsync(id, data).ConfigureAwait(false);
    }

    public async ValueTask<byte[]> ReadPageAsync(PageId id)
    {
        _file.Seek((long)(ulong)id * _pageSize, SeekOrigin.Begin);
        var buf = new byte[_pageSize];
        int read = 0;
        while (read < _pageSize)
        {
            int n = await _file.ReadAsync(buf.AsMemory(read, _pageSize - read)).ConfigureAwait(false);
            if (n == 0)
                throw new StorageException(StorageErrorKind.FileManager,
                    $"Unexpected EOF reading page {id}");
            read += n;
        }
        return buf;
    }

    private async ValueTask WriteRawAsync(ulong id, ReadOnlyMemory<byte> data)
    {
        _file.Seek((long)id * _pageSize, SeekOrigin.Begin);
        await _file.WriteAsync(data).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _file.FlushAsync().ConfigureAwait(false);
        await _file.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>Safe storage engine combining SafeFileManager + SafeBufferPool.</summary>
[Injectable<IStorageEngine>(ServiceLifetime.Singleton)]
public sealed class SafeStorageEngine : IStorageEngine
{
    private const int HeaderSize = 16;
    private readonly IFileManager _file;
    private readonly IBufferPool _pool;
    private bool _disposed;

    public StorageConfig Config { get; }

    public SafeStorageEngine(IFileManager file, IBufferPool pool, StorageConfig config)
    {
        _file = file;
        _pool = pool;
        Config = config;
    }

    public async ValueTask<PageId> AllocatePageAsync()
    {
        var id = await _file.AllocatePageAsync().ConfigureAwait(false);
        var page = new byte[Config.PageSize];
        _pool.Insert(id, page);
        return id;
    }

    public async ValueTask DeallocatePageAsync(PageId id)
    {
        _pool.Unpin(id, dirty: false);
        await _file.DeallocatePageAsync(id).ConfigureAwait(false);
    }

    public async ValueTask WritePageDataAsync(PageId id, int offset, ReadOnlyMemory<byte> data)
    {
        int cap = Config.PageSize - HeaderSize;
        if (offset < 0 || offset + data.Length > cap)
            throw new StorageException(StorageErrorKind.OutOfBounds,
                $"Write out of bounds offset={offset} length={data.Length}");

        var page = await FetchAsync(id).ConfigureAwait(false);
        data.Span.CopyTo(page.AsSpan(HeaderSize + offset));
        WriteChecksum(page);
        _pool.Insert(id, page);
        _pool.Unpin(id, dirty: true);

        foreach (var (pid, buf) in _pool.FlushDirty())
            await _file.WritePageAsync(pid, buf).ConfigureAwait(false);
    }

    public async ValueTask<Memory<byte>> ReadPageDataAsync(PageId id, int offset, int length)
    {
        int cap = Config.PageSize - HeaderSize;
        if (offset < 0 || offset + length > cap)
            throw new StorageException(StorageErrorKind.OutOfBounds,
                $"Read out of bounds offset={offset} length={length}");

        var page = await FetchAsync(id).ConfigureAwait(false);
        VerifyChecksum(id, page);
        return page.AsMemory(HeaderSize + offset, length);
    }

    private async ValueTask<byte[]> FetchAsync(PageId id)
    {
        if (_pool.TryPin(id, out var cached) && cached is not null)
            return cached;
        var buf = await _file.ReadPageAsync(id).ConfigureAwait(false);
        _pool.Insert(id, buf);
        _pool.TryPin(id, out _);
        return buf;
    }

    private static void WriteChecksum(byte[] page)
    {
        // CRC-32/C over page (checksum field zeroed before compute)
        page[12] = page[13] = page[14] = page[15] = 0;
        var crc = Crc32C(page.AsSpan());
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
            page.AsSpan(12, 4), crc);
    }

    private static void VerifyChecksum(PageId id, byte[] page)
    {
        uint stored = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(12, 4));
        page[12] = page[13] = page[14] = page[15] = 0;
        uint computed = Crc32C(page.AsSpan());
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(12, 4), stored);
        if (stored != computed)
            throw new StorageException(StorageErrorKind.ChecksumMismatch,
                $"Checksum mismatch page {id}");
    }

    private static uint Crc32C(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        foreach (var b in data)
            crc = (crc >> 8) ^ Crc32CTable[(crc ^ b) & 0xFF];
        return crc ^ 0xFFFFFFFF;
    }

    private static readonly uint[] Crc32CTable = BuildTable();
    private static uint[] BuildTable()
    {
        const uint poly = 0x82F63B78;
        var t = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) != 0 ? (c >> 1) ^ poly : c >> 1;
            t[i] = c;
        }
        return t;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        _pool.Dispose();
        await _file.DisposeAsync().ConfigureAwait(false);
    }
}
