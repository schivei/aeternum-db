using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace AeternumDB.PoC.Unsafe.Core;

/// <summary>
/// In-memory page buffer backed by <see cref="NativeMemory"/>.
/// Pages are allocated outside the GC heap — no GC pressure in hot paths.
/// <c>unsafe</c> is confined to this class; callers use the safe <see cref="Span{T}"/> API.
/// </summary>
public sealed unsafe class PageBuffer : IDisposable
{
    private const int DefaultPageSize = 8192;

    // Stores raw native pointers as nint so the dictionary itself stays managed.
    private readonly Dictionary<ulong, nint> _pages = new();
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
        _pages[id] = (nint)NativeMemory.AllocZeroed((nuint)_pageSize);
        return id;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WritePage(ulong id, int offset, ReadOnlySpan<byte> data)
    {
        byte* page = (byte*)_pages[id] + offset;
        fixed (byte* src = data)
            Buffer.MemoryCopy(src, page, _pageSize - offset, data.Length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReadPage(ulong id, int offset, int length, Span<byte> destination)
    {
        byte* page = (byte*)_pages[id] + offset;
        fixed (byte* dst = destination)
            Buffer.MemoryCopy(page, dst, length, length);
    }

    public int PageCount => _pages.Count;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var ptr in _pages.Values)
            NativeMemory.Free((void*)ptr);
        _pages.Clear();
    }
}
