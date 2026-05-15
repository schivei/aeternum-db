using System.Buffers;
using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Config;
using AeternumDB.PoC.Shared.Errors;
using AeternumDB.PoC.Shared.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

namespace AeternumDB.PoC.Safe.Storage;

/// <summary>
/// Buffer pool backed by <see cref="ArrayPool{T}"/>.
/// Pages live in managed arrays rented from the shared pool.
/// LRU eviction uses a <see cref="LinkedList{T}"/> for O(1) moves.
///
/// Mirrors Rust: struct BufferPool — safe/GC variant.
/// </summary>
[Injectable<IBufferPool>(ServiceLifetime.Singleton)]
public sealed class SafeBufferPool : IBufferPool
{
    private readonly int _capacity;
    private readonly int _pageSize;
    private readonly Dictionary<ulong, (byte[] Data, LinkedListNode<ulong> Node, int PinCount, bool Dirty)>
        _pages = new();
    private readonly LinkedList<ulong> _lru = new();
    private bool _disposed;

    public int Capacity => _capacity;
    public int Count => _pages.Count;

    public SafeBufferPool(int capacity, int pageSize)
    {
        _capacity = capacity;
        _pageSize = pageSize;
    }

    public void Insert(PageId id, byte[] page)
    {
        if (_pages.TryGetValue(id, out var existing))
        {
            page.AsSpan().CopyTo(existing.Data.AsSpan());
            _lru.Remove(existing.Node);
            _lru.AddFirst(existing.Node);
            _pages[id] = existing with { Data = existing.Data };
            return;
        }

        if (_pages.Count >= _capacity)
            Evict(id);

        var buf = ArrayPool<byte>.Shared.Rent(_pageSize);
        page.AsSpan(0, Math.Min(page.Length, _pageSize)).CopyTo(buf);
        var node = _lru.AddFirst((ulong)id);
        _pages[(ulong)id] = (buf, node, 0, false);
    }

    public bool TryPin(PageId id, out byte[]? page)
    {
        if (!_pages.TryGetValue(id, out var entry))
        { page = null; return false; }
        var copy = new byte[_pageSize];
        entry.Data.AsSpan(0, _pageSize).CopyTo(copy);
        _lru.Remove(entry.Node);
        _lru.AddFirst(entry.Node);
        _pages[id] = entry with { PinCount = entry.PinCount + 1 };
        page = copy;
        return true;
    }

    public void Unpin(PageId id, bool dirty)
    {
        if (!_pages.TryGetValue(id, out var entry))
            throw new StorageException(StorageErrorKind.BufferPool, $"Page {id} not in pool");
        if (entry.PinCount == 0)
            throw new StorageException(StorageErrorKind.BufferPool, $"Page {id} is not pinned");
        _pages[id] = entry with { PinCount = entry.PinCount - 1, Dirty = entry.Dirty || dirty };
    }

    public IReadOnlyList<(PageId Id, byte[] Data)> FlushDirty()
    {
        var result = new List<(PageId, byte[])>();
        foreach (var (id, entry) in _pages)
        {
            if (!entry.Dirty) continue;
            var copy = new byte[_pageSize];
            entry.Data.AsSpan(0, _pageSize).CopyTo(copy);
            result.Add((id, copy));
            _pages[id] = entry with { Dirty = false };
        }
        return result;
    }

    private void Evict(PageId incoming)
    {
        var node = _lru.Last;
        while (node is not null)
        {
            var pid = node.Value;
            if (_pages.TryGetValue(pid, out var e) && e.PinCount == 0)
            {
                _lru.Remove(node);
                ArrayPool<byte>.Shared.Return(e.Data);
                _pages.Remove(pid);
                return;
            }
            node = node.Previous;
        }
        throw new StorageException(StorageErrorKind.BufferPool, "Buffer pool full — all pages pinned");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var (_, entry) in _pages)
            ArrayPool<byte>.Shared.Return(entry.Data);
        _pages.Clear();
        _lru.Clear();
    }
}
