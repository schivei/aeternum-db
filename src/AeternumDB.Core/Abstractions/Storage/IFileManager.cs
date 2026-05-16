namespace AeternumDB.Core.Abstractions.Storage;

using AeternumDB.Core.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Low-level file I/O for database pages.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IFileManager : IAsyncDisposable
{
    ValueTask WritePageAsync(PageId id, ReadOnlyMemory<byte> data);
    ValueTask<byte[]> ReadPageAsync(PageId id);
    ValueTask<PageId> AllocatePageAsync();
    ValueTask DeallocatePageAsync(PageId id);
}
