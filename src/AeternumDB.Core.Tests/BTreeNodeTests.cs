using AeternumDB.Core.Errors;
using AeternumDB.Core.Index;
using AeternumDB.Core.Types;

namespace AeternumDB.Core.Tests;

public sealed class BTreeNodeTests
{
    [Fact]
    public void InternalNode_Roundtrip_Works()
    {
        var node = new InternalNode();
        node.Keys.Add([1]);
        node.Keys.Add([5]);
        node.Children.Add((PageId)10UL);
        node.Children.Add((PageId)20UL);
        node.Children.Add((PageId)30UL);

        var bytes = node.Serialize();
        var restored = InternalNode.Deserialize(bytes);

        Assert.Equal(2, restored.Keys.Count);
        Assert.Equal((PageId)10UL, restored.Children[0]);
        Assert.Equal((PageId)30UL, restored.Children[2]);
    }

    [Fact]
    public void InternalNode_InvalidChildren_Throws()
    {
        var node = new InternalNode();
        node.Keys.Add([1]);
        node.Children.Add((PageId)10UL); // should be 2 children

        var ex = Assert.Throws<IndexException>(() => node.Serialize());
        Assert.Equal(IndexErrorKind.TreeCorrupted, ex.Kind);
    }

    [Fact]
    public void InternalNode_Deserialize_WrongType_Throws()
    {
        var bytes = new byte[] { BTreeNodeConstants.LeafType, 0, 0, 0, 0 };
        var ex = Assert.Throws<IndexException>(() => InternalNode.Deserialize(bytes));
        Assert.Equal(IndexErrorKind.Serialization, ex.Kind);
    }

    [Fact]
    public void InternalNode_Deserialize_Truncated_Throws()
    {
        var bytes = new byte[] { BTreeNodeConstants.InternalType, 1, 0, 0, 0, 1, 0, 0, 0 };
        var ex = Assert.Throws<IndexException>(() => InternalNode.Deserialize(bytes));
        Assert.Equal(IndexErrorKind.Serialization, ex.Kind);
    }

    [Fact]
    public void InternalNode_FindChildIndex_Works()
    {
        var node = new InternalNode();
        node.Keys.Add([10]);
        node.Keys.Add([20]);
        node.Children.Add((PageId)1UL);
        node.Children.Add((PageId)2UL);
        node.Children.Add((PageId)3UL);

        static int Cmp(byte[] l, byte[] r) => l[0].CompareTo(r[0]);

        Assert.Equal(0, node.FindChildIndex([5], Cmp));
        Assert.Equal(1, node.FindChildIndex([10], Cmp));
        Assert.Equal(1, node.FindChildIndex([15], Cmp));
        Assert.Equal(2, node.FindChildIndex([21], Cmp));
    }

    [Fact]
    public void LeafNode_Roundtrip_Works()
    {
        var leaf = new LeafNode
        {
            NextLeaf = (PageId)99UL,
            PrevLeaf = (PageId)77UL
        };
        leaf.Keys.Add([1]);
        leaf.Values.Add([11]);
        leaf.Keys.Add([2]);
        leaf.Values.Add([22]);

        var bytes = leaf.Serialize();
        var restored = LeafNode.Deserialize(bytes);

        Assert.Equal(2, restored.Keys.Count);
        Assert.Equal((PageId)99UL, restored.NextLeaf);
        Assert.Equal((PageId)77UL, restored.PrevLeaf);
    }

    [Fact]
    public void LeafNode_InvalidParallelArrays_Throws()
    {
        var leaf = new LeafNode();
        leaf.Keys.Add([1]);

        var ex = Assert.Throws<IndexException>(() => leaf.Serialize());
        Assert.Equal(IndexErrorKind.TreeCorrupted, ex.Kind);
    }

    [Fact]
    public void LeafNode_Deserialize_WrongType_Throws()
    {
        var bytes = new byte[] { BTreeNodeConstants.InternalType, 0, 0, 0, 0 };
        var ex = Assert.Throws<IndexException>(() => LeafNode.Deserialize(bytes));
        Assert.Equal(IndexErrorKind.Serialization, ex.Kind);
    }

    [Fact]
    public void LeafNode_Deserialize_Truncated_Throws()
    {
        var bytes = new byte[] { BTreeNodeConstants.LeafType, 1, 0, 0, 0, 1, 0, 0, 0 };
        var ex = Assert.Throws<IndexException>(() => LeafNode.Deserialize(bytes));
        Assert.Equal(IndexErrorKind.Serialization, ex.Kind);
    }

    [Fact]
    public void LeafNode_FindKeyIndex_Works()
    {
        var leaf = new LeafNode();
        leaf.Keys.Add([10]);
        leaf.Values.Add([1]);
        leaf.Keys.Add([20]);
        leaf.Values.Add([2]);

        static int Cmp(byte[] l, byte[] r) => l[0].CompareTo(r[0]);

        var idx1 = leaf.FindKeyIndex([10], Cmp, out var found1);
        var idx2 = leaf.FindKeyIndex([15], Cmp, out var found2);
        var idx3 = leaf.FindKeyIndex([1], Cmp, out var found3);

        Assert.True(found1);
        Assert.Equal(0, idx1);
        Assert.False(found2);
        Assert.Equal(1, idx2);
        Assert.False(found3);
        Assert.Equal(0, idx3);
    }

    [Fact]
    public void InternalNode_Deserialize_NegativeKeyCount_Throws()
    {
        var bytes = new byte[] { BTreeNodeConstants.InternalType, 255, 255, 255, 255 };
        var ex = Assert.Throws<IndexException>(() => InternalNode.Deserialize(bytes));
        Assert.Equal(IndexErrorKind.Serialization, ex.Kind);
    }

    [Fact]
    public void InternalNode_Deserialize_KeyPayloadAndChildrenTruncated_Throws()
    {
        var keyPayloadTruncated = new byte[]
        {
            BTreeNodeConstants.InternalType,
            1, 0, 0, 0,
            3, 0, 0, 0,
            1, 2
        };
        var ex1 = Assert.Throws<IndexException>(() => InternalNode.Deserialize(keyPayloadTruncated));
        Assert.Equal(IndexErrorKind.Serialization, ex1.Kind);

        var childrenTruncated = new byte[]
        {
            BTreeNodeConstants.InternalType,
            1, 0, 0, 0,
            1, 0, 0, 0,
            9,
            1, 2, 3, 4
        };
        var ex2 = Assert.Throws<IndexException>(() => InternalNode.Deserialize(childrenTruncated));
        Assert.Equal(IndexErrorKind.Serialization, ex2.Kind);
    }

    [Fact]
    public void LeafNode_Deserialize_NegativeCount_Throws()
    {
        var bytes = new byte[] { BTreeNodeConstants.LeafType, 255, 255, 255, 255 };
        var ex = Assert.Throws<IndexException>(() => LeafNode.Deserialize(bytes));
        Assert.Equal(IndexErrorKind.Serialization, ex.Kind);
    }

    [Fact]
    public void LeafNode_Deserialize_EntryAndPointerTruncation_Throws()
    {
        var keyPayloadTruncated = new byte[]
        {
            BTreeNodeConstants.LeafType,
            1, 0, 0, 0,
            3, 0, 0, 0,
            1, 2
        };
        var ex1 = Assert.Throws<IndexException>(() => LeafNode.Deserialize(keyPayloadTruncated));
        Assert.Equal(IndexErrorKind.Serialization, ex1.Kind);

        var valuePayloadTruncated = new byte[]
        {
            BTreeNodeConstants.LeafType,
            1, 0, 0, 0,
            1, 0, 0, 0,
            7,
            3, 0, 0, 0,
            1, 2
        };
        var ex2 = Assert.Throws<IndexException>(() => LeafNode.Deserialize(valuePayloadTruncated));
        Assert.Equal(IndexErrorKind.Serialization, ex2.Kind);

        var nextPointerTruncated = new byte[]
        {
            BTreeNodeConstants.LeafType,
            0, 0, 0, 0,
            1,
            1, 2, 3
        };
        var ex3 = Assert.Throws<IndexException>(() => LeafNode.Deserialize(nextPointerTruncated));
        Assert.Equal(IndexErrorKind.Serialization, ex3.Kind);

        var prevPointerTruncated = new byte[]
        {
            BTreeNodeConstants.LeafType,
            0, 0, 0, 0,
            0,
            1,
            1, 2, 3
        };
        var ex4 = Assert.Throws<IndexException>(() => LeafNode.Deserialize(prevPointerTruncated));
        Assert.Equal(IndexErrorKind.Serialization, ex4.Kind);
    }
}
