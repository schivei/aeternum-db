namespace AeternumDB.Core.Abstractions.Index;

using GenDI;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Disk-backed B-tree index.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IBTree<TKey, TValue>
    where TKey : IComparable<TKey>
{
    ValueTask InsertAsync(TKey key, TValue value);
    ValueTask<TValue?> SearchAsync(TKey key);
    ValueTask<IReadOnlyList<(TKey Key, TValue Value)>> RangeAsync(TKey from, TKey to);
    ValueTask<bool> DeleteAsync(TKey key);
    ValueTask BulkLoadAsync(IReadOnlyList<(TKey Key, TValue Value)> entries);
    int Count { get; }
}
