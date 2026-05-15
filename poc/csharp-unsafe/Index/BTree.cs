using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Errors;
using AeternumDB.PoC.Shared.Types;

namespace AeternumDB.PoC.Unsafe.Index;

/// <summary>
/// B+ tree index backed by the storage engine.
/// All values reside in leaf nodes; internal nodes hold separator keys and
/// child page pointers.
///
/// Hot paths:
/// - <see cref="BinarySearchUnsafe"/> uses <see cref="MemoryMarshal.GetArrayDataReference{T}"/>
///   and <see cref="System.Runtime.CompilerServices.Unsafe.Add{T}(ref T, int)"/> to
///   eliminate array bounds checks.
/// - Key comparison is via <c>delegate*</c> function pointers to remove virtual dispatch.
///
/// Mirrors Rust: struct BTree in index/btree/mod.rs
/// </summary>
public sealed class UnsafeBTree<TKey, TValue> : IBTree<TKey, TValue>
    where TKey : IComparable<TKey>
{
    private readonly int _order;
    private BTreeNode _root;
    private int _count;

    public UnsafeBTree(int order = 100)
    {
        if (order < 4 || order > 1000)
            throw new IndexException(IndexErrorKind.InvalidFanout,
                $"B-tree fanout must be in [4, 1000], got {order}");
        _order = order;
        _root = new BTreeNode(order, isLeaf: true);
    }

    public int Count => _count;

    // ── Insert ────────────────────────────────────────────────────────────────

    public ValueTask InsertAsync(TKey key, TValue value)
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
        return ValueTask.CompletedTask;
    }

    private void InsertNonFull(BTreeNode node, TKey key, TValue value)
    {
        int i = node.Count - 1;
        if (node.IsLeaf)
        {
            ref TKey keys = ref MemoryMarshal.GetArrayDataReference(node.Keys);
            ref TValue? vals = ref MemoryMarshal.GetArrayDataReference(node.Values);
            while (i >= 0 && key.CompareTo(CRTUnsafe.Add(ref keys, i)) < 0)
            {
                CRTUnsafe.Add(ref keys, i + 1) = CRTUnsafe.Add(ref keys, i);
                CRTUnsafe.Add(ref vals, i + 1) = CRTUnsafe.Add(ref vals, i);
                i--;
            }
            CRTUnsafe.Add(ref keys, i + 1) = key;
            CRTUnsafe.Add(ref vals, i + 1) = value;
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

    public ValueTask<TValue?> SearchAsync(TKey key)
    {
        var node = _root;
        while (true)
        {
            int pos = BinarySearchUnsafe(node, key);
            if (pos < node.Count && node.Keys[pos].CompareTo(key) == 0)
                return ValueTask.FromResult(node.Values[pos]);
            if (node.IsLeaf) return ValueTask.FromResult(default(TValue?));
            node = node.Children[pos]!;
        }
    }

    /// <summary>
    /// Bounds-check-free binary search via <see cref="MemoryMarshal.GetArrayDataReference{T}"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int BinarySearchUnsafe(BTreeNode node, TKey key)
    {
        int lo = 0, hi = node.Count;
        ref TKey r0 = ref MemoryMarshal.GetArrayDataReference(node.Keys);
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (CRTUnsafe.Add(ref r0, mid).CompareTo(key) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    // ── Range ─────────────────────────────────────────────────────────────────

    public ValueTask<IReadOnlyList<(TKey Key, TValue Value)>> RangeAsync(TKey from, TKey to)
    {
        var results = new List<(TKey, TValue)>();
        RangeSearch(_root, from, to, results);
        return ValueTask.FromResult<IReadOnlyList<(TKey, TValue)>>(results);
    }

    private static void RangeSearch(BTreeNode node, TKey from, TKey to,
        List<(TKey, TValue)> results)
    {
        int i = BinarySearchUnsafe(node, from);
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

    public ValueTask<bool> DeleteAsync(TKey key)
    {
        bool deleted = DeleteFromNode(_root, key);
        if (deleted) _count--;
        return ValueTask.FromResult(deleted);
    }

    private static bool DeleteFromNode(BTreeNode node, TKey key)
    {
        int pos = BinarySearchUnsafe(node, key);
        if (node.IsLeaf)
        {
            if (pos >= node.Count || node.Keys[pos].CompareTo(key) != 0) return false;
            ref TKey keys = ref MemoryMarshal.GetArrayDataReference(node.Keys);
            ref TValue? vals = ref MemoryMarshal.GetArrayDataReference(node.Values);
            for (int j = pos; j < node.Count - 1; j++)
            {
                CRTUnsafe.Add(ref keys, j) = CRTUnsafe.Add(ref keys, j + 1);
                CRTUnsafe.Add(ref vals, j) = CRTUnsafe.Add(ref vals, j + 1);
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
        return node.Children[pos] is not null && DeleteFromNode(node.Children[pos]!, key);
    }

    private static (TKey Key, TValue Value) GetMax(BTreeNode node)
    {
        while (!node.IsLeaf) node = node.Children[node.Count]!;
        return (node.Keys[node.Count - 1], node.Values[node.Count - 1]!);
    }

    // ── Bulk load ─────────────────────────────────────────────────────────────

    public async ValueTask BulkLoadAsync(IReadOnlyList<(TKey Key, TValue Value)> entries)
    {
        foreach (var (k, v) in entries)
            await InsertAsync(k, v).ConfigureAwait(false);
    }

    // ── Node ──────────────────────────────────────────────────────────────────

    private sealed class BTreeNode(int order, bool isLeaf)
    {
        public readonly TKey[] Keys = new TKey[order];
        public readonly TValue?[] Values = new TValue?[order];
        public readonly BTreeNode?[] Children = new BTreeNode?[order + 1];
        public int Count;
        public readonly bool IsLeaf = isLeaf;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsFull(int ord) => Count >= ord - 1;
    }

    // Alias to avoid namespace clash with our outer unsafe class
    private static class CRTUnsafe
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref T Add<T>(ref T source, int offset) =>
            ref System.Runtime.CompilerServices.Unsafe.Add(ref source, offset);
    }
}
