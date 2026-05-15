using System.Buffers;
using System.Runtime.CompilerServices;

namespace AeternumDB.PoC.Safe.Core;

/// <summary>
/// In-memory page buffer backed by <see cref="ArrayPool{T}"/>.
/// Models the storage engine's buffer pool without any unsafe code.
/// Hot paths use <see cref="Span{T}"/> to avoid redundant copies.
/// </summary>
public sealed class PageBuffer : IDisposable
{
    private const int DefaultPageSize = 8192;

    private readonly ArrayPool<byte> _pool = ArrayPool<byte>.Shared;
    private readonly Dictionary<ulong, byte[]> _pages = new();
    private readonly int _pageSize;
    private ulong _nextId;
    private bool _disposed;

    public PageBuffer(int pageSize = DefaultPageSize)
    {
        _pageSize = pageSize;
    }

    public ulong AllocatePage()
    {
        var id = ++_nextId;
        _pages[id] = _pool.Rent(_pageSize);
        return id;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WritePage(ulong id, int offset, ReadOnlySpan<byte> data)
    {
        data.CopyTo(_pages[id].AsSpan(offset));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReadPage(ulong id, int offset, int length, Span<byte> destination)
    {
        _pages[id].AsSpan(offset, length).CopyTo(destination);
    }

    public int PageCount => _pages.Count;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var page in _pages.Values)
            _pool.Return(page);
        _pages.Clear();
    }
}
