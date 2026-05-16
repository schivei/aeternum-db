namespace AeternumDB.Core.Abstractions.Storage;

using AeternumDB.Core.Config;
using AeternumDB.Core.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Page-level storage engine.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IStorageEngine : IAsyncDisposable
{
    ValueTask<PageId> AllocatePageAsync();
    ValueTask DeallocatePageAsync(PageId id);
    ValueTask WritePageDataAsync(PageId id, int offset, ReadOnlyMemory<byte> data);
    ValueTask<Memory<byte>> ReadPageDataAsync(PageId id, int offset, int length);
    StorageConfig Config { get; }
}
