using AeternumDB.Core.Abstractions;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Types;
using Microsoft.Win32.SafeHandles;

namespace AeternumDB.Core.Storage;

/// <summary>
/// Async file-based page storage backed by <see cref="RandomAccess"/>.
///
/// Page <c>id</c> occupies bytes <c>id × pageSize .. (id+1) × pageSize</c>.
/// An in-memory bitmap tracks which slots are allocated; a LIFO free list
/// enables O(1) reuse of deallocated slots.
///
/// When all slots are occupied the file grows by <see cref="GrowthChunkPages"/>
/// slots at a time.
/// </summary>
public sealed class FileManager : IFileManager
{
    private const int GrowthChunkPages = 64;

    private readonly SafeFileHandle _handle;
    private readonly int _pageSize;
    private readonly List<bool> _bitmap;
    private readonly Stack<PageId> _freeList;

    private FileManager(SafeFileHandle handle, int pageSize, List<bool> bitmap, Stack<PageId> freeList)
    {
        _handle = handle;
        _pageSize = pageSize;
        _bitmap = bitmap;
        _freeList = freeList;
    }

    /// <summary>Open (or create) the database file at <paramref name="path"/>.</summary>
    public static async Task<FileManager> OpenAsync(string path, int pageSize)
    {
        if (pageSize <= PageHeader.Size)
            throw new ArgumentOutOfRangeException(nameof(pageSize),
                $"pageSize must be > {PageHeader.Size}");
        if (pageSize - PageHeader.Size > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(pageSize),
                "page data capacity exceeds ushort.MaxValue");

        var handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite,
            FileShare.None, FileOptions.Asynchronous);

        long fileLen = RandomAccess.GetLength(handle);
        long pageCount = fileLen == 0 ? 0 : (fileLen + pageSize - 1) / pageSize;

        var (bitmap, freeList) = await ScanAllocationStateAsync(handle, pageCount, pageSize);
        return new FileManager(handle, pageSize, bitmap, freeList);
    }

    /// <summary>Allocate a new page slot and return its <see cref="PageId"/>.</summary>
    public async ValueTask<PageId> AllocatePageAsync()
    {
        if (_freeList.TryPop(out var reused))
        {
            var recycled = new Page(reused, PageType.Data, _pageSize - PageHeader.Size);
            await WriteRawAsync(reused, recycled.Serialize());
            _bitmap[(int)(ulong)reused] = true;
            return reused;
        }
        return await GrowAndAllocateAsync();
    }

    /// <summary>
    /// Mark page <paramref name="id"/> as free so its slot can be reused.
    /// Writes a <see cref="PageType.Free"/> header to disk before updating memory state.
    /// </summary>
    public async ValueTask DeallocatePageAsync(PageId id)
    {
        CheckValidId(id);
        if (!_bitmap[(int)(ulong)id])
            throw new StorageException(StorageErrorKind.FileManager,
                $"page {id} is already free");

        await WriteFreeMarkerAsync(id);
        _bitmap[(int)(ulong)id] = false;
        _freeList.Push(id);
    }

    /// <summary>Write <paramref name="data"/> (full serialized page) to disk at page <paramref name="id"/>.</summary>
    public async ValueTask WritePageAsync(PageId id, ReadOnlyMemory<byte> data)
    {
        CheckValidId(id);
        if (!_bitmap[(int)(ulong)id])
            throw new StorageException(StorageErrorKind.FileManager,
                $"page {id} is not allocated");
        if (data.Length != _pageSize)
            throw new StorageException(StorageErrorKind.FileManager,
                $"page {id} size mismatch: expected {_pageSize}, got {data.Length}");

        await RandomAccess.WriteAsync(_handle, data, FileOffset(id));
    }

    /// <summary>Read and return the full serialized bytes for page <paramref name="id"/>.</summary>
    public async ValueTask<byte[]> ReadPageAsync(PageId id)
    {
        CheckValidId(id);
        if (!_bitmap[(int)(ulong)id])
            throw new StorageException(StorageErrorKind.FileManager,
                $"page {id} is not allocated");

        var buf = new byte[_pageSize];
        int read = await RandomAccess.ReadAsync(_handle, buf.AsMemory(), FileOffset(id));
        if (read < _pageSize)
            throw new StorageException(StorageErrorKind.FileManager,
                $"page {id} is corrupt (short read: {read}/{_pageSize})");
        return buf;
    }

    public async ValueTask DisposeAsync()
    {
        await ValueTask.CompletedTask;
        _handle.Dispose();
        GC.SuppressFinalize(this);
    }

    private long FileOffset(PageId id) => (long)(id.Value * (ulong)_pageSize);

    private void CheckValidId(PageId id)
    {
        if ((int)(ulong)id >= _bitmap.Count)
            throw new StorageException(StorageErrorKind.FileManager,
                $"invalid page id {id}");
    }

    private async Task<PageId> GrowAndAllocateAsync()
    {
        var oldCount = _bitmap.Count;
        var newCount = oldCount + GrowthChunkPages;

        await ExtendFileAsync(newCount);

        // Enqueue [oldCount+1, newCount) as free slots for future allocations
        for (int i = oldCount + 1; i < newCount; i++)
            _freeList.Push((ulong)i);

        // Allocate the first new slot directly
        PageId id = (ulong)oldCount;
        var page = new Page(id, PageType.Data, _pageSize - PageHeader.Size);
        await WriteRawAsync(id, page.Serialize());
        _bitmap[(int)(ulong)id] = true;
        return id;
    }

    private async Task ExtendFileAsync(int newCount)
    {
        var oldCount = _bitmap.Count;
        long newSize = (long)newCount * _pageSize;
        // Extend file to newSize by writing a zero byte at the last position
        await RandomAccess.WriteAsync(_handle, new byte[] { 0 }.AsMemory(), newSize - 1);

        // Grow bitmap for the new slots (all free = false)
        while (_bitmap.Count < newCount)
            _bitmap.Add(false);

        // Write PageType.Free headers for the new slots only
        await WriteFreeHeadersForRangeAsync(oldCount, newCount);
    }

    private async Task WriteFreeHeadersForRangeAsync(int from, int to)
    {
        int dataSize = _pageSize - PageHeader.Size;
        uint checksum = Page.ZeroDataChecksum(dataSize);

        for (int i = from; i < to; i++)
        {
            PageId id = (ulong)i;
            var header = new PageHeader(id.Value, PageType.Free, (ushort)dataSize, checksum);
            var hdr = new byte[PageHeader.Size];
            Page.SerializeHeader(header, hdr);
            await RandomAccess.WriteAsync(_handle, hdr.AsMemory(), FileOffset(id));
        }
    }

    private async Task WriteFreeMarkerAsync(PageId id)
    {
        int dataSize = _pageSize - PageHeader.Size;
        uint checksum = Page.ZeroDataChecksum(dataSize);
        var header = new PageHeader(id.Value, PageType.Free, (ushort)dataSize, checksum);
        var hdr = new byte[PageHeader.Size];
        Page.SerializeHeader(header, hdr);
        await RandomAccess.WriteAsync(_handle, hdr.AsMemory(), FileOffset(id));
    }

    private async ValueTask WriteRawAsync(PageId id, ReadOnlyMemory<byte> data) =>
        await RandomAccess.WriteAsync(_handle, data, FileOffset(id));

    private static async Task<(List<bool> Bitmap, Stack<PageId> FreeList)>
        ScanAllocationStateAsync(SafeFileHandle handle, long pageCount, int pageSize)
    {
        var bitmap = new List<bool>((int)pageCount);
        var freeIds = new List<PageId>(); // collect in order, push to Stack (LIFO) at end

        var headerBuf = new byte[PageHeader.Size];
        for (long i = 0; i < pageCount; i++)
        {
            PageId id = (ulong)i;
            int read = await RandomAccess.ReadAsync(handle, headerBuf.AsMemory(),
                i * pageSize);

            if (read < PageHeader.Size || IsHeaderFree(headerBuf))
            {
                bitmap.Add(false);
                freeIds.Add(id);
            }
            else
            {
                bitmap.Add(true);
            }
        }

        // Push in ascending order so highest ids are on top (LIFO reuse of newest slots)
        var freeList = new Stack<PageId>(freeIds.Count);
        for (int i = 0; i < freeIds.Count; i++)
            freeList.Push(freeIds[i]);

        return (bitmap, freeList);
    }

    private static bool IsHeaderFree(byte[] buf) =>
        buf[8] == (byte)PageType.Free || IsAllZero(buf);

    private static bool IsAllZero(byte[] buf)
    {
        foreach (var b in buf)
            if (b != 0) return false;
        return true;
    }
}
