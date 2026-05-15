using System.Buffers.Binary;
using AeternumDB.Core.Abstractions;
using AeternumDB.Core.Config;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Types;

namespace AeternumDB.Core.Index;

public sealed class BTree<TKey, TValue> : IBTree<TKey, TValue>
    where TKey : IComparable<TKey>
{
    private const int MetaSize = 8 + 4 + 4 + 4; // root, height, count, fanout
    private const string InternalNodeOutOfRange = "internal node child index out of range";

    private readonly record struct BTreeState(PageId MetaPageId, PageId RootPageId, int Height, int Count, int Fanout);

    private readonly IStorageEngine _storage;
    private readonly IIndexCodec<TKey> _keyCodec;
    private readonly IIndexCodec<TValue> _valueCodec;
    private readonly SemaphoreSlim _sem = new(1, 1);
    private readonly int _pagePayloadSize;

    private PageId _metaPageId;
    private PageId _rootPageId;
    private int _height;
    private int _count;
    private readonly int _fanout;

    private BTree(
        IStorageEngine storage,
        IIndexCodec<TKey> keyCodec,
        IIndexCodec<TValue> valueCodec,
        BTreeState state)
    {
        _storage = storage;
        _keyCodec = keyCodec;
        _valueCodec = valueCodec;
        _metaPageId = state.MetaPageId;
        _rootPageId = state.RootPageId;
        _height = state.Height;
        _count = state.Count;
        _fanout = state.Fanout;
        _pagePayloadSize = storage.Config.PageSize - PageHeader.Size;
    }

    public int Count => _count;

    public static async ValueTask<BTree<TKey, TValue>> CreateAsync(
        IStorageEngine storage,
        BTreeConfig? config = null,
        IIndexCodec<TKey>? keyCodec = null,
        IIndexCodec<TValue>? valueCodec = null)
    {
        config ??= new BTreeConfig();
        if (config.Fanout is < 4 or > 1000)
            throw new IndexException(IndexErrorKind.InvalidFanout, $"invalid fanout {config.Fanout}: must be in range [4, 1000]");

        var metaPageId = await storage.AllocatePageAsync();
        var rootPageId = await storage.AllocatePageAsync();

        var tree = new BTree<TKey, TValue>(
            storage,
            keyCodec ?? IndexCodec.Default<TKey>(),
            valueCodec ?? IndexCodec.Default<TValue>(),
            new BTreeState(metaPageId, rootPageId, Height: 1, Count: 0, Fanout: config.Fanout));

        await tree.WriteLeafAsync(rootPageId, new LeafNode());
        await tree.WriteMetaAsync();
        return tree;
    }

    public static async ValueTask<BTree<TKey, TValue>> OpenAsync(
        IStorageEngine storage,
        PageId metaPageId,
        IIndexCodec<TKey>? keyCodec = null,
        IIndexCodec<TValue>? valueCodec = null)
    {
        var payload = await ReadBlobAsync(storage, metaPageId, storage.Config.PageSize - PageHeader.Size);
        if (payload.Length < MetaSize)
            throw new IndexException(IndexErrorKind.TreeCorrupted, "btree metadata page is too small");

        const int rootOffset = 0;
        const int heightOffset = 8;
        const int countOffset = 12;
        const int fanoutOffset = 16;

        var root = (PageId)BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(rootOffset, 8));
        var height = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(heightOffset, 4));
        var count = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(countOffset, 4));
        var fanout = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(fanoutOffset, 4));

        if (height < 1)
            throw new IndexException(IndexErrorKind.TreeCorrupted, "btree metadata invalid: height must be >= 1");
        if (fanout is < 4 or > 1000)
            throw new IndexException(IndexErrorKind.InvalidFanout, $"invalid fanout {fanout}: must be in range [4, 1000]");

        return new BTree<TKey, TValue>(
            storage,
            keyCodec ?? IndexCodec.Default<TKey>(),
            valueCodec ?? IndexCodec.Default<TValue>(),
            new BTreeState(metaPageId, root, height, Math.Max(0, count), fanout));
    }

    public async ValueTask<PageId> MetaPageIdAsync()
    {
        await _sem.WaitAsync();
        try { return _metaPageId; }
        finally { _sem.Release(); }
    }

    public async ValueTask InsertAsync(TKey key, TValue value)
    {
        await _sem.WaitAsync();
        try
        {
            var keyBytes = _keyCodec.Serialize(key);
            var valueBytes = _valueCodec.Serialize(value);

            var result = await InsertRecursiveAsync(_rootPageId, _height, keyBytes, valueBytes);
            if (result.SplitKey is not null && result.RightPage.HasValue)
            {
                var newRoot = new InternalNode();
                newRoot.Keys.Add(result.SplitKey);
                newRoot.Children.Add(_rootPageId);
                newRoot.Children.Add(result.RightPage.Value);

                var newRootPage = await _storage.AllocatePageAsync();
                await WriteInternalAsync(newRootPage, newRoot);
                _rootPageId = newRootPage;
                _height += 1;
            }

            if (result.InsertedNew)
                _count += 1;

            await WriteMetaAsync();
        }
        finally { _sem.Release(); }
    }

    public async ValueTask<TValue?> SearchAsync(TKey key)
    {
        await _sem.WaitAsync();
        try
        {
            var keyBytes = _keyCodec.Serialize(key);
            var found = await SearchRecursiveAsync(_rootPageId, _height, keyBytes);
            return found is null ? default : _valueCodec.Deserialize(found);
        }
        finally { _sem.Release(); }
    }

    public async ValueTask<IReadOnlyList<(TKey Key, TValue Value)>> RangeAsync(TKey from, TKey to)
    {
        await _sem.WaitAsync();
        try
        {
            if (from.CompareTo(to) > 0) return [];

            var startBytes = _keyCodec.Serialize(from);
            var endKey = to;

            var (_, leaf) = await FindLeafAsync(_rootPageId, _height, startBytes);
            var startPos = leaf.FindKeyIndex(startBytes, CompareKeyBytes, out _);
            var list = new List<(TKey Key, TValue Value)>();

            var current = leaf;
            var pos = startPos;

            while (true)
            {
                while (pos < current.Keys.Count)
                {
                    var key = _keyCodec.Deserialize(current.Keys[pos]);
                    if (key.CompareTo(endKey) > 0)
                        return list;
                    var value = _valueCodec.Deserialize(current.Values[pos]);
                    list.Add((key, value));
                    pos++;
                }

                if (!current.NextLeaf.HasValue) break;
                current = await ReadLeafAsync(current.NextLeaf.Value);
                pos = 0;
            }

            return list;
        }
        finally { _sem.Release(); }
    }

    public async ValueTask<bool> DeleteAsync(TKey key)
    {
        await _sem.WaitAsync();
        try
        {
            var keyBytes = _keyCodec.Serialize(key);
            var deleted = await DeleteRecursiveAsync(_rootPageId, _height, keyBytes);

            if (!deleted)
                return false;

            _count = Math.Max(0, _count - 1);

            if (_height > 1)
            {
                var rootNode = await ReadNodeTypeAsync(_rootPageId);
                if (rootNode == BTreeNodeConstants.InternalType)
                {
                    var root = await ReadInternalAsync(_rootPageId);
                    if (root.Keys.Count == 0 && root.Children.Count > 0)
                    {
                        var oldRoot = _rootPageId;
                        _rootPageId = root.Children[0];
                        _height -= 1;
                        await _storage.DeallocatePageAsync(oldRoot);
                    }
                }
            }

            await WriteMetaAsync();
            return true;
        }
        finally { _sem.Release(); }
    }

    public async ValueTask BulkLoadAsync(IReadOnlyList<(TKey Key, TValue Value)> entries)
    {
        // Current implementation favors correctness and parity with existing
        // insert semantics. A bottom-up bulk loader can be added later.
        foreach (var (key, value) in entries)
            await InsertAsync(key, value);
    }

    private async ValueTask WriteMetaAsync()
    {
        var bytes = new byte[MetaSize];
        var pos = 0;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(pos, 8), _rootPageId);
        pos += 8;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(pos, 4), _height);
        pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(pos, 4), _count);
        pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(pos, 4), _fanout);
        await WriteBlobAsync(_storage, _metaPageId, bytes, _pagePayloadSize);
    }

    private async ValueTask<(byte[]? SplitKey, PageId? RightPage, bool InsertedNew)> InsertRecursiveAsync(
        PageId pageId,
        int height,
        byte[] keyBytes,
        byte[] valueBytes)
    {
        if (height == 1)
            return await InsertIntoLeafAsync(pageId, keyBytes, valueBytes);

        var internalNode = await ReadInternalAsync(pageId);
        var childIndex = internalNode.FindChildIndex(keyBytes, CompareKeyBytes);
        if (childIndex < 0 || childIndex >= internalNode.Children.Count)
            throw new IndexException(IndexErrorKind.TreeCorrupted, InternalNodeOutOfRange);

        var childPage = internalNode.Children[childIndex];
        var childResult = await InsertRecursiveAsync(childPage, height - 1, keyBytes, valueBytes);
        if (childResult.SplitKey is null || !childResult.RightPage.HasValue)
            return (null, null, childResult.InsertedNew);

        internalNode.Keys.Insert(childIndex, childResult.SplitKey);
        internalNode.Children.Insert(childIndex + 1, childResult.RightPage.Value);

        if (internalNode.Keys.Count <= _fanout)
        {
            await WriteInternalAsync(pageId, internalNode);
            return (null, null, childResult.InsertedNew);
        }

        var split = SplitInternal(internalNode);
        var rightPage = await _storage.AllocatePageAsync();
        await WriteInternalAsync(pageId, split.Left);
        await WriteInternalAsync(rightPage, split.Right);
        return (split.PushUpKey, rightPage, childResult.InsertedNew);
    }

    private async ValueTask<(byte[]? SplitKey, PageId? RightPage, bool InsertedNew)> InsertIntoLeafAsync(
        PageId pageId,
        byte[] keyBytes,
        byte[] valueBytes)
    {
        var leaf = await ReadLeafAsync(pageId);
        var idx = leaf.FindKeyIndex(keyBytes, CompareKeyBytes, out var found);
        if (found)
        {
            leaf.Values[idx] = valueBytes;
            await WriteLeafAsync(pageId, leaf);
            return (null, null, false);
        }

        leaf.Keys.Insert(idx, keyBytes);
        leaf.Values.Insert(idx, valueBytes);
        if (leaf.Keys.Count <= _fanout)
        {
            await WriteLeafAsync(pageId, leaf);
            return (null, null, true);
        }

        var split = SplitLeaf(leaf);
        var rightPage = await _storage.AllocatePageAsync();
        split.Right.NextLeaf = split.Left.NextLeaf;
        split.Right.PrevLeaf = pageId;
        split.Left.NextLeaf = rightPage;

        if (split.Right.NextLeaf.HasValue)
        {
            var oldNextPage = split.Right.NextLeaf.Value;
            var oldNextLeaf = await ReadLeafAsync(oldNextPage);
            oldNextLeaf.PrevLeaf = rightPage;
            await WriteLeafAsync(oldNextPage, oldNextLeaf);
        }

        await WriteLeafAsync(pageId, split.Left);
        await WriteLeafAsync(rightPage, split.Right);
        return (split.SplitKey, rightPage, true);
    }

    private async ValueTask<byte[]?> SearchRecursiveAsync(PageId pageId, int height, byte[] keyBytes)
    {
        if (height == 1)
        {
            var leaf = await ReadLeafAsync(pageId);
            var idx = leaf.FindKeyIndex(keyBytes, CompareKeyBytes, out var found);
            return found ? leaf.Values[idx] : null;
        }

        var internalNode = await ReadInternalAsync(pageId);
        var childIndex = internalNode.FindChildIndex(keyBytes, CompareKeyBytes);
        if (childIndex < 0 || childIndex >= internalNode.Children.Count)
            throw new IndexException(IndexErrorKind.TreeCorrupted, InternalNodeOutOfRange);
        return await SearchRecursiveAsync(internalNode.Children[childIndex], height - 1, keyBytes);
    }

    private async ValueTask<bool> DeleteRecursiveAsync(PageId pageId, int height, byte[] keyBytes)
    {
        if (height == 1)
        {
            var leaf = await ReadLeafAsync(pageId);
            var idx = leaf.FindKeyIndex(keyBytes, CompareKeyBytes, out var found);
            if (!found) return false;
            leaf.Keys.RemoveAt(idx);
            leaf.Values.RemoveAt(idx);
            await WriteLeafAsync(pageId, leaf);
            return true;
        }

        var internalNode = await ReadInternalAsync(pageId);
        var childIndex = internalNode.FindChildIndex(keyBytes, CompareKeyBytes);
        if (childIndex < 0 || childIndex >= internalNode.Children.Count)
            throw new IndexException(IndexErrorKind.TreeCorrupted, InternalNodeOutOfRange);
        return await DeleteRecursiveAsync(internalNode.Children[childIndex], height - 1, keyBytes);
    }

    private async ValueTask<(PageId pageId, LeafNode leaf)> FindLeafAsync(PageId startPage, int height, byte[] keyBytes)
    {
        var currentPage = startPage;
        var currentHeight = height;
        while (currentHeight > 1)
        {
            var internalNode = await ReadInternalAsync(currentPage);
            var childIndex = internalNode.FindChildIndex(keyBytes, CompareKeyBytes);
            if (childIndex < 0 || childIndex >= internalNode.Children.Count)
                throw new IndexException(IndexErrorKind.TreeCorrupted, InternalNodeOutOfRange);
            currentPage = internalNode.Children[childIndex];
            currentHeight--;
        }

        return (currentPage, await ReadLeafAsync(currentPage));
    }

    private int CompareKeyBytes(byte[] left, byte[] right)
    {
        var l = _keyCodec.Deserialize(left);
        var r = _keyCodec.Deserialize(right);
        return l.CompareTo(r);
    }

    private async ValueTask<byte> ReadNodeTypeAsync(PageId pageId)
    {
        var bytes = await ReadBlobAsync(_storage, pageId, _pagePayloadSize);
        if (bytes.Length == 0)
            throw new IndexException(IndexErrorKind.TreeCorrupted, $"empty node payload at page {pageId}");
        return bytes[0];
    }

    private async ValueTask<InternalNode> ReadInternalAsync(PageId pageId)
    {
        var bytes = await ReadBlobAsync(_storage, pageId, _pagePayloadSize);
        return InternalNode.Deserialize(bytes);
    }

    private async ValueTask<LeafNode> ReadLeafAsync(PageId pageId)
    {
        var bytes = await ReadBlobAsync(_storage, pageId, _pagePayloadSize);
        return LeafNode.Deserialize(bytes);
    }

    private async ValueTask WriteInternalAsync(PageId pageId, InternalNode node) =>
        await WriteBlobAsync(_storage, pageId, node.Serialize(), _pagePayloadSize);

    private async ValueTask WriteLeafAsync(PageId pageId, LeafNode node) =>
        await WriteBlobAsync(_storage, pageId, node.Serialize(), _pagePayloadSize);

    private static (LeafNode Left, LeafNode Right, byte[] SplitKey) SplitLeaf(LeafNode leaf)
    {
        // Midpoint split keeps both sides balanced and minimizes tree height growth.
        var mid = leaf.Keys.Count / 2;
        var left = new LeafNode
        {
            PrevLeaf = leaf.PrevLeaf
        };
        left.Keys.AddRange(leaf.Keys.Take(mid));
        left.Values.AddRange(leaf.Values.Take(mid));

        var right = new LeafNode
        {
            NextLeaf = leaf.NextLeaf,
            PrevLeaf = leaf.PrevLeaf
        };
        right.Keys.AddRange(leaf.Keys.Skip(mid));
        right.Values.AddRange(leaf.Values.Skip(mid));

        return (left, right, right.Keys[0]);
    }

    private static (InternalNode Left, InternalNode Right, byte[] PushUpKey) SplitInternal(InternalNode node)
    {
        // Midpoint split keeps both sides balanced and minimizes tree height growth.
        var mid = node.Keys.Count / 2;
        var pushUp = node.Keys[mid];

        var left = new InternalNode();
        left.Keys.AddRange(node.Keys.Take(mid));
        left.Children.AddRange(node.Children.Take(mid + 1));

        var right = new InternalNode();
        right.Keys.AddRange(node.Keys.Skip(mid + 1));
        right.Children.AddRange(node.Children.Skip(mid + 1));

        return (left, right, pushUp);
    }

    private static async ValueTask WriteBlobAsync(IStorageEngine storage, PageId pageId, byte[] payload, int pagePayloadSize)
    {
        if (payload.Length + 4 > pagePayloadSize)
            throw new IndexException(IndexErrorKind.Serialization, $"node payload too large for page: {payload.Length} bytes");

        var sizeBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(sizeBytes, payload.Length);
        await storage.WritePageDataAsync(pageId, 0, sizeBytes);
        await storage.WritePageDataAsync(pageId, 4, payload);
    }

    private static async ValueTask<byte[]> ReadBlobAsync(IStorageEngine storage, PageId pageId, int pagePayloadSize)
    {
        var sizeMem = await storage.ReadPageDataAsync(pageId, 0, 4);
        var size = BinaryPrimitives.ReadInt32LittleEndian(sizeMem.Span);
        if (size < 0 || size > pagePayloadSize - 4)
            throw new IndexException(IndexErrorKind.TreeCorrupted, $"invalid node payload size {size} on page {pageId}");
        var payload = await storage.ReadPageDataAsync(pageId, 4, size);
        return payload.ToArray();
    }
}
