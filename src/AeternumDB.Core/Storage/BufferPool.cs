using AeternumDB.Core.Abstractions.Storage;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Types;

namespace AeternumDB.Core.Storage;

/// <summary>
/// LRU buffer pool: caches raw page bytes in memory to reduce disk I/O.
///
/// Eviction evicts the least-recently-used page whose pin count is zero
/// and whose dirty flag is clear.  Dirty pages must be flushed before
/// their slot can be reused.
///
/// Thread-safe via <see langword="lock"/>.
/// </summary>
public sealed class BufferPool : IBufferPool
{
    private sealed class Frame(LinkedListNode<PageId> node, byte[] data)
    {
        public LinkedListNode<PageId> Node { get; } = node;
        public byte[] Data { get; } = data;
        public bool Dirty { get; set; }
        public int PinCount { get; set; }
    }

    private readonly int _capacity;
    private readonly Dictionary<PageId, Frame> _frames;
    private readonly LinkedList<PageId> _lru; // front = LRU, back = MRU
    private readonly object _gate = new();

    public int Capacity => _capacity;
    public int Count { get { lock (_gate) return _frames.Count; } }

    public BufferPool(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(capacity, 0);
        _capacity = capacity;
        _frames = new Dictionary<PageId, Frame>(capacity);
        _lru = new LinkedList<PageId>();
    }

    /// <summary>
    /// Insert <paramref name="page"/> bytes for <paramref name="id"/> into the pool (pin count = 0).
    /// If the page is already present, this is a no-op.
    /// Evicts the LRU clean, unpinned page when the pool is full.
    /// </summary>
    public void Insert(PageId id, byte[] page)
    {
        lock (_gate)
        {
            if (_frames.ContainsKey(id))
                return;

            if (_frames.Count >= _capacity)
                EvictLruVictim();

            var node = _lru.AddLast(id);
            _frames[id] = new Frame(node, page);
        }
    }

    /// <summary>
    /// Pin page <paramref name="id"/>: increment its pin count, move to MRU, and return its bytes.
    /// Returns <see langword="false"/> when the page is not in the pool.
    /// </summary>
    public bool TryPin(PageId id, out byte[]? page)
    {
        lock (_gate)
        {
            if (!_frames.TryGetValue(id, out var frame))
            {
                page = null;
                return false;
            }
            frame.PinCount++;
            TouchLru(frame.Node);
            page = frame.Data;
            return true;
        }
    }

    /// <summary>
    /// Decrement pin count for <paramref name="id"/>.
    /// Pass <paramref name="dirty"/> = <see langword="true"/> when the page was modified.
    /// </summary>
    public void Unpin(PageId id, bool dirty)
    {
        lock (_gate)
        {
            if (!_frames.TryGetValue(id, out var frame))
                throw new StorageException(StorageErrorKind.BufferPool,
                    $"page {id} not in buffer pool");
            if (frame.PinCount == 0)
                throw new StorageException(StorageErrorKind.PagePinned,
                    $"page {id} is not pinned");
            frame.PinCount--;
            if (dirty) frame.Dirty = true;
        }
    }

    /// <summary>
    /// Collect all dirty, unpinned pages without clearing the dirty flag.
    /// The caller writes them to disk and then calls <see cref="MarkClean"/> per page.
    /// </summary>
    public IReadOnlyList<(PageId Id, byte[] Data)> FlushDirty()
    {
        lock (_gate)
        {
            var result = new List<(PageId, byte[])>();
            foreach (var (id, frame) in _frames)
                if (frame.Dirty && frame.PinCount == 0)
                    result.Add((id, frame.Data));
            return result;
        }
    }

    /// <summary>Clear the dirty flag for <paramref name="id"/> after a successful disk write.</summary>
    public void MarkClean(PageId id)
    {
        lock (_gate)
        {
            if (_frames.TryGetValue(id, out var frame))
                frame.Dirty = false;
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    private void EvictLruVictim()
    {
        var node = _lru.First;
        while (node is not null)
        {
            var candidate = node.Value;
            if (_frames.TryGetValue(candidate, out var frame) &&
                frame.PinCount == 0 && !frame.Dirty)
            {
                _lru.Remove(node);
                _frames.Remove(candidate);
                return;
            }
            node = node.Next;
        }
        throw new StorageException(StorageErrorKind.BufferPool,
            "buffer pool is full; all pages are pinned or dirty — flush dirty pages and retry");
    }

    private void TouchLru(LinkedListNode<PageId> node)
    {
        _lru.Remove(node);
        _lru.AddLast(node);
    }
}
