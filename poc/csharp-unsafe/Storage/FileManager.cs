using System.Runtime.InteropServices;
using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Config;
using AeternumDB.PoC.Shared.Errors;
using AeternumDB.PoC.Shared.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

namespace AeternumDB.PoC.Unsafe.Storage;

/// <summary>
/// File manager: page-level I/O to a single database file.
/// The hot-path allocation uses <c>NativeMemory.AllocZeroed</c> inside a
/// non-async unsafe block, then bridges to async I/O outside it.
///
/// Mirrors Rust: struct FileManager in storage/file_manager.rs
/// </summary>
[Injectable<IFileManager>(ServiceLifetime.Singleton)]
public sealed class UnsafeFileManager : IFileManager
{
    private const int FreeListHeaderPages = 1;

    private readonly int _pageSize;
    private readonly FileStream _file;
    private readonly List<ulong> _freeList = [];
    private ulong _nextPageId = FreeListHeaderPages;
    private bool _disposed;

    public UnsafeFileManager(string path, int pageSize)
    {
        _pageSize = pageSize;
        _file = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite,
            FileShare.None, bufferSize: pageSize, useAsync: true);
    }

    public async ValueTask<PageId> AllocatePageAsync()
    {
        ulong id;
        if (_freeList.Count > 0)
        {
            id = _freeList[^1];
            _freeList.RemoveAt(_freeList.Count - 1);
        }
        else
        {
            id = _nextPageId++;
        }

        // Allocate with NativeMemory (unsafe block — no await inside)
        byte[] managed;
        unsafe
        {
            byte* buf = (byte*)NativeMemory.AllocZeroed((nuint)_pageSize);
            try
            {
                managed = new byte[_pageSize];
                new ReadOnlySpan<byte>(buf, _pageSize).CopyTo(managed);
            }
            finally { NativeMemory.Free(buf); }
        }

        // Async write is outside the unsafe block — compiler is happy
        await WriteRawAsync(id, managed).ConfigureAwait(false);
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
                $"Write: expected {_pageSize} bytes, got {data.Length}");
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
