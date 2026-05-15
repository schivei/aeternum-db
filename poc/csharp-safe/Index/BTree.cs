using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Errors;
using AeternumDB.PoC.Shared.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

namespace AeternumDB.PoC.Safe.Index;

/// <summary>
/// B+ tree using fully managed arrays.
/// Binary search uses <see cref="CollectionsMarshal.AsSpan{T}"/> internally.
/// Hot paths use <see cref="Span{T}"/> to avoid bounds checks from JIT analysis.
///
/// Mirrors Rust: struct BTree — safe/GC variant.
/// </summary>
public sealed class SafeBTree<TKey, TValue> : IBTree<TKey, TValue>
    where TKey : IComparable<TKey>
{
    private readonly int _order;
    private BTreeNode _root;
    private int _count;

    public int Count => _count;

    public SafeBTree(int order = 100)
    {
        if (order is < 4 or > 1000)
            throw new IndexException(IndexErrorKind.InvalidFanout,
                $"Fanout must be in [4,1000], got {order}");
        _order = order;
        _root = new BTreeNode(order, isLeaf: true);
    }

    public ValueTask InsertAsync(TKey key, TValue value)
    {
        if (_root.IsFull(_order))
        {
            var newRoot = new BTreeNode(_order, false);
            newRoot.Children[0] = _root;
            SplitChild(newRoot, 0);
            _root = newRoot;
        }
        InsertNonFull(_root, key, value);
        _count++;
        return ValueTask.CompletedTask;
    }

    private void InsertNonFull(BTreeNode node, TKey key, TValue value)
    {
        var keys = node.Keys.AsSpan(0, node.Count);
        int i = keys.Length - 1;
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

    public ValueTask<TValue?> SearchAsync(TKey key)
    {
        var node = _root;
        while (true)
        {
            int pos = BinarySearch(node.Keys.AsSpan(0, node.Count), key);
            if (pos < node.Count && node.Keys[pos].CompareTo(key) == 0)
                return ValueTask.FromResult(node.Values[pos]);
            if (node.IsLeaf) return ValueTask.FromResult(default(TValue?));
            node = node.Children[pos]!;
        }
    }

    private static int BinarySearch(Span<TKey> keys, TKey key)
    {
        int lo = 0, hi = keys.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (keys[mid].CompareTo(key) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    public ValueTask<IReadOnlyList<(TKey Key, TValue Value)>> RangeAsync(TKey from, TKey to)
    {
        var results = new List<(TKey, TValue)>();
        RangeSearch(_root, from, to, results);
        return ValueTask.FromResult<IReadOnlyList<(TKey, TValue)>>(results);
    }

    private static void RangeSearch(BTreeNode node, TKey from, TKey to,
        List<(TKey, TValue)> results)
    {
        int i = BinarySearch(node.Keys.AsSpan(0, node.Count), from);
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

    public ValueTask<bool> DeleteAsync(TKey key)
    {
        bool ok = DeleteFromNode(_root, key);
        if (ok) _count--;
        return ValueTask.FromResult(ok);
    }

    private static bool DeleteFromNode(BTreeNode node, TKey key)
    {
        int pos = BinarySearch(node.Keys.AsSpan(0, node.Count), key);
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
            var (pk, pv) = GetMax(node.Children[pos]!);
            node.Keys[pos] = pk;
            node.Values[pos] = pv;
            return DeleteFromNode(node.Children[pos]!, pk);
        }
        return node.Children[pos] is not null && DeleteFromNode(node.Children[pos]!, key);
    }

    private static (TKey Key, TValue Value) GetMax(BTreeNode node)
    {
        while (!node.IsLeaf) node = node.Children[node.Count]!;
        return (node.Keys[node.Count - 1], node.Values[node.Count - 1]!);
    }

    public async ValueTask BulkLoadAsync(IReadOnlyList<(TKey Key, TValue Value)> entries)
    {
        foreach (var (k, v) in entries)
            await InsertAsync(k, v).ConfigureAwait(false);
    }

    private sealed class BTreeNode(int order, bool isLeaf)
    {
        public readonly TKey[] Keys = new TKey[order];
        public readonly TValue?[] Values = new TValue?[order];
        public readonly BTreeNode?[] Children = new BTreeNode?[order + 1];
        public int Count;
        public readonly bool IsLeaf = isLeaf;
        public bool IsFull(int ord) => Count >= ord - 1;
    }
}
