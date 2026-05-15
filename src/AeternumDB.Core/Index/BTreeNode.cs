using System.Buffers.Binary;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Types;

namespace AeternumDB.Core.Index;

internal static class BTreeNodeConstants
{
    public const byte InternalType = 0;
    public const byte LeafType = 1;
}

internal sealed class InternalNode
{
    public List<byte[]> Keys { get; } = [];
    public List<PageId> Children { get; } = [];

    public int FindChildIndex(byte[] searchKey, Func<byte[], byte[], int> compare)
    {
        var lo = 0;
        var hi = Keys.Count;
        while (lo < hi)
        {
            var mid = lo + ((hi - lo) / 2);
            var cmp = compare(Keys[mid], searchKey);
            if (cmp <= 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    public byte[] Serialize()
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        bw.Write(BTreeNodeConstants.InternalType);
        bw.Write(Keys.Count);
        foreach (var key in Keys)
        {
            bw.Write(key.Length);
            bw.Write(key);
        }

        var expected = Keys.Count + 1;
        if (Children.Count != expected)
            throw new IndexException(IndexErrorKind.TreeCorrupted, $"invalid internal node children count: keys={Keys.Count}, children={Children.Count}");

        foreach (var child in Children)
            bw.Write((ulong)child);

        return ms.ToArray();
    }

    public static InternalNode Deserialize(ReadOnlySpan<byte> data)
    {
        var pos = 0;
        if (data.Length < 1 + 4)
            throw new IndexException(IndexErrorKind.Serialization, "internal node buffer too small");

        var type = data[pos++];
        if (type != BTreeNodeConstants.InternalType)
            throw new IndexException(IndexErrorKind.Serialization, $"expected internal node type {BTreeNodeConstants.InternalType}, got {type}");

        var keyCount = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos, 4));
        pos += 4;
        if (keyCount < 0)
            throw new IndexException(IndexErrorKind.Serialization, "invalid key count");

        var node = new InternalNode();
        for (var i = 0; i < keyCount; i++)
        {
            if (pos + 4 > data.Length)
                throw new IndexException(IndexErrorKind.Serialization, "unexpected end of internal node key length");
            var len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos, 4));
            pos += 4;
            if (len < 0 || pos + len > data.Length)
                throw new IndexException(IndexErrorKind.Serialization, "unexpected end of internal node key payload");
            node.Keys.Add(data.Slice(pos, len).ToArray());
            pos += len;
        }

        for (var i = 0; i < keyCount + 1; i++)
        {
            if (pos + 8 > data.Length)
                throw new IndexException(IndexErrorKind.Serialization, "unexpected end of internal node children");
            var pageId = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos, 8));
            pos += 8;
            node.Children.Add((PageId)pageId);
        }

        return node;
    }
}

internal sealed class LeafNode
{
    public List<byte[]> Keys { get; } = [];
    public List<byte[]> Values { get; } = [];
    public PageId? NextLeaf { get; set; }
    public PageId? PrevLeaf { get; set; }

    public int FindKeyIndex(byte[] searchKey, Func<byte[], byte[], int> compare, out bool found)
    {
        var lo = 0;
        var hi = Keys.Count;
        while (lo < hi)
        {
            var mid = lo + ((hi - lo) / 2);
            var cmp = compare(Keys[mid], searchKey);
            if (cmp < 0) lo = mid + 1;
            else hi = mid;
        }

        found = lo < Keys.Count && compare(Keys[lo], searchKey) == 0;
        return lo;
    }

    public byte[] Serialize()
    {
        if (Keys.Count != Values.Count)
            throw new IndexException(IndexErrorKind.TreeCorrupted, $"invalid leaf node arrays: keys={Keys.Count}, values={Values.Count}");

        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        bw.Write(BTreeNodeConstants.LeafType);
        bw.Write(Keys.Count);
        for (var i = 0; i < Keys.Count; i++)
        {
            bw.Write(Keys[i].Length);
            bw.Write(Keys[i]);
            bw.Write(Values[i].Length);
            bw.Write(Values[i]);
        }

        bw.Write(NextLeaf.HasValue);
        if (NextLeaf.HasValue) bw.Write((ulong)NextLeaf.Value);
        bw.Write(PrevLeaf.HasValue);
        if (PrevLeaf.HasValue) bw.Write((ulong)PrevLeaf.Value);

        return ms.ToArray();
    }

    public static LeafNode Deserialize(ReadOnlySpan<byte> data)
    {
        var pos = 0;
        if (data.Length < 1 + 4)
            throw new IndexException(IndexErrorKind.Serialization, "leaf node buffer too small");

        var type = data[pos++];
        if (type != BTreeNodeConstants.LeafType)
            throw new IndexException(IndexErrorKind.Serialization, $"expected leaf node type {BTreeNodeConstants.LeafType}, got {type}");

        var count = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos, 4));
        pos += 4;
        if (count < 0)
            throw new IndexException(IndexErrorKind.Serialization, "invalid key count");

        var leaf = new LeafNode();
        for (var i = 0; i < count; i++)
        {
            var (key, value) = ReadLeafEntry(data, ref pos);
            leaf.Keys.Add(key);
            leaf.Values.Add(value);
        }

        leaf.NextLeaf = ReadOptionalPageId(data, ref pos,
            "unexpected end of leaf next flag", "unexpected end of leaf next pointer");
        leaf.PrevLeaf = ReadOptionalPageId(data, ref pos,
            "unexpected end of leaf prev flag", "unexpected end of leaf prev pointer");

        return leaf;
    }

    private static (byte[] key, byte[] value) ReadLeafEntry(ReadOnlySpan<byte> data, ref int pos)
    {
        if (pos + 4 > data.Length)
            throw new IndexException(IndexErrorKind.Serialization, "unexpected end of leaf key length");
        var keyLen = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos, 4));
        pos += 4;
        if (keyLen < 0 || pos + keyLen > data.Length)
            throw new IndexException(IndexErrorKind.Serialization, "unexpected end of leaf key payload");
        var key = data.Slice(pos, keyLen).ToArray();
        pos += keyLen;

        if (pos + 4 > data.Length)
            throw new IndexException(IndexErrorKind.Serialization, "unexpected end of leaf value length");
        var valueLen = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos, 4));
        pos += 4;
        if (valueLen < 0 || pos + valueLen > data.Length)
            throw new IndexException(IndexErrorKind.Serialization, "unexpected end of leaf value payload");
        var value = data.Slice(pos, valueLen).ToArray();
        pos += valueLen;

        return (key, value);
    }

    private static PageId? ReadOptionalPageId(ReadOnlySpan<byte> data, ref int pos, string flagError, string pointerError)
    {
        if (pos + 1 > data.Length)
            throw new IndexException(IndexErrorKind.Serialization, flagError);
        var hasValue = data[pos++] != 0;
        if (!hasValue) return null;

        if (pos + 8 > data.Length)
            throw new IndexException(IndexErrorKind.Serialization, pointerError);
        var pageId = (PageId)BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos, 8));
        pos += 8;
        return pageId;
    }
}

