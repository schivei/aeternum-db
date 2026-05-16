namespace AeternumDB.Core.Abstractions.Storage;

using AeternumDB.Core.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Buffer pool: LRU in-memory cache of database pages.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IBufferPool : IDisposable
{
    void Insert(PageId id, byte[] page);
    bool TryPin(PageId id, out byte[]? page);
    void Unpin(PageId id, bool dirty);
    IReadOnlyList<(PageId Id, byte[] Data)> FlushDirty();
    int Capacity { get; }
    int Count { get; }
}
