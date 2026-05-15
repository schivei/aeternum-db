using System.Runtime.CompilerServices;

namespace AeternumDB.PoC.Safe.Core;

/// <summary>
/// In-memory B-tree with array-backed nodes.
/// Uses binary search and pre-allocated node arrays to minimise heap pressure.
/// No unsafe code. NativeAOT compatible — no reflection.
/// </summary>
public sealed class BTree<TKey, TValue>
    where TKey : IComparable<TKey>
{
    private readonly int _order;
    private BTreeNode _root;
    private int _count;

    public BTree(int order = 100)
    {
        _order = order;
        _root = new BTreeNode(order, isLeaf: true);
    }

    public int Count => _count;

    // ── Insert ────────────────────────────────────────────────────────────────

    public void Insert(TKey key, TValue value)
    {
        if (_root.IsFull(_order))
        {
            var newRoot = new BTreeNode(_order, isLeaf: false);
            newRoot.Children[0] = _root;
            SplitChild(newRoot, 0);
            _root = newRoot;
        }
        InsertNonFull(_root, key, value);
        _count++;
    }

    private void InsertNonFull(BTreeNode node, TKey key, TValue value)
    {
        int i = node.Count - 1;
        if (node.IsLeaf)
        {
            while (i >= 0 && key.CompareTo(node.Keys[i]) < 0)
            {
                node.Keys[i + 1] = node.Keys[i];
                node.Values[i + 1] = node.Values[i];
                i--;
            }
            node.Keys[i + 1] = key;
            node.Values[i + 1] = value;
            node.Count++;
        }
        else
        {
            while (i >= 0 && key.CompareTo(node.Keys[i]) < 0) i--;
            i++;
            if (node.Children[i]!.IsFull(_order))
            {
                SplitChild(node, i);
                if (key.CompareTo(node.Keys[i]) > 0) i++;
            }
            InsertNonFull(node.Children[i]!, key, value);
        }
    }

    private void SplitChild(BTreeNode parent, int idx)
    {
        var child = parent.Children[idx]!;
        var sibling = new BTreeNode(_order, child.IsLeaf);
        int mid = (_order - 1) / 2;

        sibling.Count = child.Count - mid - 1;
        Array.Copy(child.Keys, mid + 1, sibling.Keys, 0, sibling.Count);
        Array.Copy(child.Values, mid + 1, sibling.Values, 0, sibling.Count);
        if (!child.IsLeaf)
            Array.Copy(child.Children, mid + 1, sibling.Children, 0, sibling.Count + 1);

        for (int j = parent.Count; j > idx; j--)
        {
            parent.Keys[j] = parent.Keys[j - 1];
            parent.Values[j] = parent.Values[j - 1];
            parent.Children[j + 1] = parent.Children[j];
        }
        parent.Keys[idx] = child.Keys[mid];
        parent.Values[idx] = child.Values[mid];
        parent.Children[idx + 1] = sibling;
        parent.Count++;
        child.Count = mid;
    }

    // ── Search ────────────────────────────────────────────────────────────────

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TValue? Search(TKey key)
    {
        var node = _root;
        while (true)
        {
            int pos = BinarySearch(node, key);
            if (pos < node.Count && node.Keys[pos].CompareTo(key) == 0)
                return node.Values[pos];
            if (node.IsLeaf) return default;
            node = node.Children[pos]!;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int BinarySearch(BTreeNode node, TKey key)
    {
        int lo = 0, hi = node.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (node.Keys[mid].CompareTo(key) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    // ── Range ─────────────────────────────────────────────────────────────────

    public IReadOnlyList<(TKey Key, TValue Value)> Range(TKey from, TKey to)
    {
        var results = new List<(TKey, TValue)>();
        RangeSearch(_root, from, to, results);
        return results;
    }

    private static void RangeSearch(
        BTreeNode node, TKey from, TKey to, List<(TKey, TValue)> results)
    {
        int i = BinarySearch(node, from);
        while (i < node.Count && node.Keys[i].CompareTo(to) <= 0)
        {
            if (!node.IsLeaf) RangeSearch(node.Children[i]!, from, to, results);
            if (node.Keys[i].CompareTo(from) >= 0)
                results.Add((node.Keys[i], node.Values[i]!));
            i++;
        }
        if (!node.IsLeaf && i <= node.Count && node.Children[i] is not null)
            RangeSearch(node.Children[i]!, from, to, results);
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    public bool Delete(TKey key)
    {
        bool deleted = DeleteFromNode(_root, key);
        if (deleted) _count--;
        return deleted;
    }

    private static bool DeleteFromNode(BTreeNode node, TKey key)
    {
        int pos = BinarySearch(node, key);
        if (node.IsLeaf)
        {
            if (pos >= node.Count || node.Keys[pos].CompareTo(key) != 0) return false;
            for (int j = pos; j < node.Count - 1; j++)
            {
                node.Keys[j] = node.Keys[j + 1];
                node.Values[j] = node.Values[j + 1];
            }
            node.Count--;
            return true;
        }
        if (pos < node.Count && node.Keys[pos].CompareTo(key) == 0)
        {
            var pred = GetMax(node.Children[pos]!);
            node.Keys[pos] = pred.Key;
            node.Values[pos] = pred.Value;
            return DeleteFromNode(node.Children[pos]!, pred.Key);
        }
        return node.Children[pos] is not null
            && DeleteFromNode(node.Children[pos]!, key);
    }

    private static (TKey Key, TValue Value) GetMax(BTreeNode node)
    {
        while (!node.IsLeaf)
            node = node.Children[node.Count]!;
        return (node.Keys[node.Count - 1], node.Values[node.Count - 1]!);
    }

    // ── Bulk load ─────────────────────────────────────────────────────────────

    public void BulkLoad(IReadOnlyList<(TKey Key, TValue Value)> entries)
    {
        foreach (var (k, v) in entries)
            Insert(k, v);
    }

    // ── Node ──────────────────────────────────────────────────────────────────

    private sealed class BTreeNode
    {
        public readonly TKey[] Keys;
        public readonly TValue?[] Values;
        public readonly BTreeNode?[] Children;
        public int Count;
        public readonly bool IsLeaf;

        public BTreeNode(int order, bool isLeaf)
        {
            Keys = new TKey[order];
            Values = new TValue?[order];
            Children = new BTreeNode?[order + 1];
            IsLeaf = isLeaf;
            Count = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsFull(int order) => Count >= order - 1;
    }
}
