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
/// Buffer pool backed by <c>NativeMemory</c>.
/// Each page lives outside the GC heap — no GC involvement on allocation or
/// eviction.  LRU ordering kept via a doubly-linked list of <see cref="PageSlot"/>
/// structs stored in a <c>NativeMemory</c>-allocated array.
///
/// Mirrors Rust: struct BufferPool in storage/buffer_pool.rs
/// </summary>
[Injectable<IBufferPool>(ServiceLifetime.Singleton)]
public sealed class UnsafeBufferPool : IBufferPool
{
    private readonly int _capacity;
    private readonly int _pageSize;

    // Slot array allocated from native heap: capacity * sizeof(PageSlot)
    private unsafe PageSlot* _slots;
    private unsafe int* _lruHead;
    private unsafe int* _lruTail;

    // page_id → slot index map (managed, but only metadata)
    private readonly Dictionary<ulong, int> _index = new();
    private int _count;
    private bool _disposed;

    public int Capacity => _capacity;
    public int Count => _count;

    public unsafe UnsafeBufferPool(int capacity, int pageSize)
    {
        _capacity = capacity;
        _pageSize = pageSize;

        nuint slotBytes = (nuint)(capacity * sizeof(PageSlot));
        _slots = (PageSlot*)NativeMemory.AllocZeroed(slotBytes);

        _lruHead = (int*)NativeMemory.AllocZeroed((nuint)sizeof(int));
        _lruTail = (int*)NativeMemory.AllocZeroed((nuint)sizeof(int));
        *_lruHead = -1;
        *_lruTail = -1;

        for (int i = 0; i < capacity; i++)
        {
            _slots[i].Data = (byte*)NativeMemory.AllocZeroed((nuint)pageSize);
            _slots[i].PageId = 0;
            _slots[i].PinCount = 0;
            _slots[i].Dirty = 0;
            _slots[i].Prev = -1;
            _slots[i].Next = -1;
        }
    }

    public unsafe void Insert(PageId id, byte[] page)
    {
        if (page.Length > _pageSize)
            throw new StorageException(StorageErrorKind.BufferPool,
                $"Page {id}: {page.Length} > pageSize {_pageSize}");

        if (_index.TryGetValue(id, out int slot))
        {
            fixed (byte* src = page)
                CopyBlock(_slots[slot].Data, src, (uint)page.Length);
            TouchLru(slot);
            return;
        }

        int freeSlot = FindFreeOrEvict(id);
        _slots[freeSlot].PageId = id;
        _slots[freeSlot].PinCount = 0;
        _slots[freeSlot].Dirty = 0;
        fixed (byte* src = page)
            CopyBlock(_slots[freeSlot].Data, src, (uint)page.Length);

        _index[id] = freeSlot;
        _count++;
        TouchLru(freeSlot);
    }

    public unsafe bool TryPin(PageId id, out byte[]? page)
    {
        if (!_index.TryGetValue(id, out int slot))
        {
            page = null;
            return false;
        }
        _slots[slot].PinCount++;
        var buf = new byte[_pageSize];
        fixed (byte* dst = buf)
            CopyBlock(dst, _slots[slot].Data, (uint)_pageSize);
        page = buf;
        TouchLru(slot);
        return true;
    }

    public unsafe void Unpin(PageId id, bool dirty)
    {
        if (!_index.TryGetValue(id, out int slot))
            throw new StorageException(StorageErrorKind.BufferPool,
                $"Page {id} not in buffer pool");
        if (_slots[slot].PinCount == 0)
            throw new StorageException(StorageErrorKind.BufferPool,
                $"Page {id} is not pinned");
        _slots[slot].PinCount--;
        if (dirty) _slots[slot].Dirty = 1;
    }

    public unsafe IReadOnlyList<(PageId Id, byte[] Data)> FlushDirty()
    {
        var result = new List<(PageId, byte[])>();
        foreach (var (id, slot) in _index)
        {
            if (_slots[slot].Dirty != 0)
            {
                var buf = new byte[_pageSize];
                fixed (byte* dst = buf)
                    CopyBlock(dst, _slots[slot].Data, (uint)_pageSize);
                result.Add((id, buf));
                _slots[slot].Dirty = 0;
            }
        }
        return result;
    }

    private unsafe int FindFreeOrEvict(PageId newId)
    {
        for (int i = 0; i < _capacity; i++)
            if (_slots[i].PageId == 0) return i;

        int cur = *_lruTail;
        while (cur != -1)
        {
            if (_slots[cur].PinCount == 0)
            {
                _index.Remove(_slots[cur].PageId);
                _slots[cur].PageId = 0;
                _count--;
                return cur;
            }
            cur = _slots[cur].Prev;
        }

        throw new StorageException(StorageErrorKind.BufferPool,
            "Buffer pool full — all pages are pinned");
    }

    private unsafe void TouchLru(int slot)
    {
        int p = _slots[slot].Prev;
        int n = _slots[slot].Next;
        if (p != -1) _slots[p].Next = n;
        else *_lruHead = n;
        if (n != -1) _slots[n].Prev = p;
        else *_lruTail = p;

        _slots[slot].Prev = -1;
        _slots[slot].Next = *_lruHead;
        if (*_lruHead != -1) _slots[*_lruHead].Prev = slot;
        *_lruHead = slot;
        if (*_lruTail == -1) *_lruTail = slot;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe void CopyBlock(byte* dst, byte* src, uint bytes) =>
        CRTUnsafe.CopyBlockUnaligned(dst, src, bytes);

    public unsafe void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        for (int i = 0; i < _capacity; i++)
            NativeMemory.Free(_slots[i].Data);
        NativeMemory.Free(_slots);
        NativeMemory.Free(_lruHead);
        NativeMemory.Free(_lruTail);
        _index.Clear();
    }

    [System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
    private unsafe struct PageSlot
    {
        public byte* Data;
        public ulong PageId;
        public int PinCount;
        public int Dirty;
        public int Prev;
        public int Next;
    }
}
