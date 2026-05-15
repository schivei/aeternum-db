using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Config;
using AeternumDB.PoC.Shared.Errors;
using AeternumDB.PoC.Shared.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;
using CRTUnsafe = System.Runtime.CompilerServices.Unsafe;

namespace AeternumDB.PoC.Unsafe.Storage;

/// <summary>
/// Full storage engine: combines <see cref="UnsafeFileManager"/> and
/// <see cref="UnsafeBufferPool"/> into the single API used by higher layers.
///
/// Page data is held in native memory by the buffer pool; file I/O is async.
/// CRC-32/C checksum is verified on every read.
///
/// Mirrors Rust: struct StorageEngine in storage/mod.rs
/// </summary>
[Injectable<IStorageEngine>(ServiceLifetime.Singleton)]
public sealed class UnsafeStorageEngine : IStorageEngine
{
    // Header layout constants (mirrors Rust PageHeader)
    private const int HeaderSize = 16;
    private const int OffsetChecksum = 12;

    private readonly IFileManager _file;
    private readonly IBufferPool _pool;
    private bool _disposed;

    public StorageConfig Config { get; }

    public UnsafeStorageEngine(IFileManager file, IBufferPool pool, StorageConfig config)
    {
        _file = file;
        _pool = pool;
        Config = config;
    }

    public async ValueTask<PageId> AllocatePageAsync()
    {
        var id = await _file.AllocatePageAsync().ConfigureAwait(false);
        var page = BuildEmptyPage(id, PageType.Free);
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
        int dataCapacity = Config.PageSize - HeaderSize;
        if (offset < 0 || offset + data.Length > dataCapacity)
            throw new StorageException(StorageErrorKind.OutOfBounds,
                $"Write out of bounds: offset={offset} length={data.Length} capacity={dataCapacity}");

        var page = await FetchPageAsync(id).ConfigureAwait(false);

        // Unsafe copy: no managed bounds check on the hot-path memcpy
        CopyIntoPage(page, HeaderSize + offset, data.Span);
        WriteChecksum(page);
        _pool.Insert(id, page);
        _pool.Unpin(id, dirty: true);

        foreach (var (pid, buf) in _pool.FlushDirty())
            await _file.WritePageAsync(pid, buf).ConfigureAwait(false);
    }

    public async ValueTask<Memory<byte>> ReadPageDataAsync(PageId id, int offset, int length)
    {
        int dataCapacity = Config.PageSize - HeaderSize;
        if (offset < 0 || offset + length > dataCapacity)
            throw new StorageException(StorageErrorKind.OutOfBounds,
                $"Read out of bounds: offset={offset} length={length} capacity={dataCapacity}");

        var page = await FetchPageAsync(id).ConfigureAwait(false);
        VerifyChecksum(id, page);

        var result = new byte[length];
        CopyFromPage(page, HeaderSize + offset, result);
        return result;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async ValueTask<byte[]> FetchPageAsync(PageId id)
    {
        if (_pool.TryPin(id, out var cached) && cached is not null)
            return cached;
        var buf = await _file.ReadPageAsync(id).ConfigureAwait(false);
        _pool.Insert(id, buf);
        _pool.TryPin(id, out _);
        return buf;
    }

    private byte[] BuildEmptyPage(PageId id, PageType type)
    {
        var page = new byte[Config.PageSize];
        WritePageIdAndType(page, id, type);
        WriteChecksum(page);
        return page;
    }

    // ── Unsafe copy helpers (no await, no GC issue) ───────────────────────────

    private static unsafe void CopyIntoPage(byte[] page, int dstOffset, ReadOnlySpan<byte> src)
    {
        fixed (byte* dst = page)
        fixed (byte* s = src)
            CRTUnsafe.CopyBlockUnaligned(dst + dstOffset, s, (uint)src.Length);
    }

    private static unsafe void CopyFromPage(byte[] page, int srcOffset, byte[] dst)
    {
        fixed (byte* src = page)
        fixed (byte* d = dst)
            CRTUnsafe.CopyBlockUnaligned(d, src + srcOffset, (uint)dst.Length);
    }

    private static unsafe void WritePageIdAndType(byte[] page, PageId id, PageType type)
    {
        fixed (byte* p = page)
        {
            *(ulong*)p = (ulong)id;
            *(ushort*)(p + 8) = (ushort)type;
            *(ushort*)(p + 10) = (ushort)(page.Length - HeaderSize);
        }
    }

    private static unsafe void WriteChecksum(byte[] page)
    {
        fixed (byte* p = page)
        {
            *(uint*)(p + OffsetChecksum) = 0;
            uint crc = Crc32C(p, (uint)page.Length);
            *(uint*)(p + OffsetChecksum) = crc;
        }
    }

    private static unsafe void VerifyChecksum(PageId id, byte[] page)
    {
        fixed (byte* p = page)
        {
            uint stored = *(uint*)(p + OffsetChecksum);
            *(uint*)(p + OffsetChecksum) = 0;
            uint computed = Crc32C(p, (uint)page.Length);
            *(uint*)(p + OffsetChecksum) = stored;
            if (stored != computed)
                throw new StorageException(StorageErrorKind.ChecksumMismatch,
                    $"Checksum mismatch on page {id}: stored={stored} computed={computed}");
        }
    }

    private static unsafe uint Crc32C(byte* data, uint length)
    {
        uint crc = 0xFFFFFFFF;
        for (uint i = 0; i < length; i++)
            crc = (crc >> 8) ^ Crc32CTable[(crc ^ data[i]) & 0xFF];
        return crc ^ 0xFFFFFFFF;
    }

    private static readonly uint[] Crc32CTable = BuildCrc32CTable();

    private static uint[] BuildCrc32CTable()
    {
        const uint poly = 0x82F63B78;
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) != 0 ? (c >> 1) ^ poly : c >> 1;
            table[i] = c;
        }
        return table;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        _pool.Dispose();
        await _file.DisposeAsync().ConfigureAwait(false);
    }
}

