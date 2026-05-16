using AeternumDB.Core.Errors;
using AeternumDB.Core.Storage;
using AeternumDB.Core.Types;

namespace AeternumDB.Core.Tests;

public sealed class StorageTests
{
    [Fact]
    public void BufferPool_Insert_Pin_Unpin_FlushAndMarkClean_Works()
    {
        var pool = new BufferPool(2);
        pool.Insert(1UL, new byte[32]);
        pool.Insert(1UL, new byte[32]);

        Assert.True(pool.TryPin(1UL, out var bytes));
        Assert.NotNull(bytes);
        pool.Unpin(1UL, dirty: true);
        Assert.Equal(1, pool.Count);

        var dirty = pool.FlushDirty();
        Assert.Single(dirty);
        Assert.Equal((PageId)1UL, dirty[0].Id);

        pool.MarkClean(1UL);
        Assert.Empty(pool.FlushDirty());
    }

    [Fact]
    public void BufferPool_EvictsLruCleanUnpinned()
    {
        var pool = new BufferPool(1);
        pool.Insert(1UL, new byte[8]);
        pool.Insert(2UL, new byte[8]);

        Assert.False(pool.TryPin(1UL, out _));
        Assert.True(pool.TryPin(2UL, out _));
        pool.Unpin(2UL, dirty: false);
    }

    [Fact]
    public void BufferPool_InsertWhenFullWithDirtyOrPinnedPages_Throws()
    {
        var pool = new BufferPool(1);
        pool.Insert(1UL, new byte[8]);
        Assert.True(pool.TryPin(1UL, out _));
        pool.Unpin(1UL, dirty: true);

        var ex = Assert.Throws<StorageException>(() => pool.Insert(2UL, new byte[8]));
        Assert.Equal(StorageErrorKind.BufferPool, ex.Kind);
    }

    [Fact]
    public void BufferPool_UnpinMissingOrNotPinned_Throws()
    {
        var pool = new BufferPool(1);
        var ex1 = Assert.Throws<StorageException>(() => pool.Unpin(1UL, dirty: false));
        Assert.Equal(StorageErrorKind.BufferPool, ex1.Kind);

        pool.Insert(1UL, new byte[8]);
        var ex2 = Assert.Throws<StorageException>(() => pool.Unpin(1UL, dirty: false));
        Assert.Equal(StorageErrorKind.PagePinned, ex2.Kind);
    }

    [Fact]
    public void Page_ZeroDataChecksum_MatchesComputedZeroBuffer()
    {
        const int size = 200;
        var expected = Page.ComputeChecksum(new byte[size]);
        var actual = Page.ZeroDataChecksum(size);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Page_WriteReadAndChecksum_Works()
    {
        var page = new Page(7UL, PageType.Data, 64);
        page.WriteData(4, [1, 2, 3, 4, 5]);
        Assert.True(page.ValidateChecksum());

        var read = page.ReadData(4, 5).ToArray();
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, read);
        Assert.Equal((ushort)(64 - 9), page.Header.FreeSpace);
    }

    [Fact]
    public void Page_OutOfBoundsReadWrite_Throws()
    {
        var page = new Page(1UL, PageType.Data, 8);
        var ex1 = Assert.Throws<StorageException>(() => page.WriteData(6, [1, 2, 3]));
        Assert.Equal(StorageErrorKind.OutOfBounds, ex1.Kind);

        var ex2 = Assert.Throws<StorageException>(() => page.ReadData(7, 2));
        Assert.Equal(StorageErrorKind.OutOfBounds, ex2.Kind);
    }

    [Fact]
    public void Page_DeserializeInvalidOrShort_ReturnsNull()
    {
        Assert.Null(Page.Deserialize([1, 2, 3], 32));

        var raw = new byte[32];
        raw[8] = 250; // invalid page type
        Assert.Null(Page.Deserialize(raw, 32));
    }

    [Fact]
    public async Task FileManager_AllocateWriteReadDeallocateAndReuse_Works()
    {
        var path = Path.Join(Path.GetTempPath(), $"aeternum-fm-{Guid.NewGuid():N}.db");
        try
        {
            await using var fm = await FileManager.OpenAsync(path, 128);
            var id = await fm.AllocatePageAsync();

            var page = new Page(id, PageType.Data, 112);
            page.WriteData(0, [9, 8, 7]);
            await fm.WritePageAsync(id, page.Serialize());

            var readRaw = await fm.ReadPageAsync(id);
            var restored = Page.Deserialize(readRaw, 128);
            Assert.NotNull(restored);
            Assert.Equal(new byte[] { 9, 8, 7 }, restored!.ReadData(0, 3).ToArray());

            await fm.DeallocatePageAsync(id);
            var reused = await fm.AllocatePageAsync();
            Assert.Equal(id, reused);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task FileManager_InvalidOperations_Throw()
    {
        var path = Path.Join(Path.GetTempPath(), $"aeternum-fm-{Guid.NewGuid():N}.db");
        try
        {
            await using var fm = await FileManager.OpenAsync(path, 128);
            var id = await fm.AllocatePageAsync();

            await Assert.ThrowsAsync<StorageException>(async () => await fm.ReadPageAsync(9999UL));
            await Assert.ThrowsAsync<StorageException>(async () => await fm.WritePageAsync(id, new byte[127]));
            await Assert.ThrowsAsync<StorageException>(async () => await fm.ReadPageAsync(id + 1));

            await fm.DeallocatePageAsync(id);
            await Assert.ThrowsAsync<StorageException>(async () => await fm.DeallocatePageAsync(id));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task FileManager_InvalidPageSize_Throws()
    {
        var path = Path.Join(Path.GetTempPath(), $"aeternum-fm-{Guid.NewGuid():N}.db");
        try
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await FileManager.OpenAsync(path, 16));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await FileManager.OpenAsync(path, 70000));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task StorageEngine_Deallocate_RemovesBothCachedAndNonCachedPages()
    {
        var path = Path.Join(Path.GetTempPath(), $"aeternum-se-{Guid.NewGuid():N}.db");
        try
        {
            await using var se = new StorageEngine(new AeternumDB.Core.Config.StorageConfig
            {
                DataPath = path,
                BufferPoolSize = 4,
                PageSize = 256
            });

            var a = await se.AllocatePageAsync();
            var b = await se.AllocatePageAsync();

            await se.WritePageDataAsync(a, 0, new byte[] { 1, 2, 3 });
            _ = await se.ReadPageDataAsync(a, 0, 3); // cache page a before deallocation

            await se.DeallocatePageAsync(a);
            await se.DeallocatePageAsync(b); // non-cached path in EvictFromPool

            var reused1 = await se.AllocatePageAsync();
            var reused2 = await se.AllocatePageAsync();
            Assert.True(reused1 == a || reused1 == b);
            Assert.True(reused2 == a || reused2 == b);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task FileManager_Reopen_ScansFreeHeaders_AndReusesSlots()
    {
        var path = Path.Join(Path.GetTempPath(), $"aeternum-fm-{Guid.NewGuid():N}.db");
        try
        {
            PageId id1;
            PageId id2;

            await using (var fm = await FileManager.OpenAsync(path, 128))
            {
                id1 = await fm.AllocatePageAsync();
                id2 = await fm.AllocatePageAsync();
                await fm.DeallocatePageAsync(id1);
            }

            await using (var reopened = await FileManager.OpenAsync(path, 128))
            {
                await Assert.ThrowsAsync<StorageException>(async () => await reopened.ReadPageAsync(id1));
                await reopened.WritePageAsync(id2, new Page(id2, PageType.Data, 112).Serialize());
                _ = await reopened.AllocatePageAsync();
            }
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task FileManager_ReadPage_ShortRead_ThrowsCorruption()
    {
        var path = Path.Join(Path.GetTempPath(), $"aeternum-fm-{Guid.NewGuid():N}.db");
        try
        {
            PageId id;
            await using (var fm = await FileManager.OpenAsync(path, 128))
            {
                id = await fm.AllocatePageAsync();
            }

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None))
                fs.SetLength(32);

            await using var reopened = await FileManager.OpenAsync(path, 128);
            var ex = await Assert.ThrowsAsync<StorageException>(async () => await reopened.ReadPageAsync(id));
            Assert.Equal(StorageErrorKind.FileManager, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task StorageEngine_ReadChecksumMismatch_Throws()
    {
        var path = Path.Join(Path.GetTempPath(), $"aeternum-se-{Guid.NewGuid():N}.db");
        try
        {
            var config = new AeternumDB.Core.Config.StorageConfig
            {
                DataPath = path,
                BufferPoolSize = 4,
                PageSize = 256
            };

            PageId id;
            await using (var se = new StorageEngine(config))
            {
                id = await se.AllocatePageAsync();
                await se.WritePageDataAsync(id, 0, new byte[] { 1, 2, 3, 4 });
            }

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                fs.Position = (long)id.Value * config.PageSize + PageHeader.Size;
                fs.WriteByte(255);
            }

            await using var reopened = new StorageEngine(config);
            var ex = await Assert.ThrowsAsync<StorageException>(async () => await reopened.ReadPageDataAsync(id, 0, 1));
            Assert.Equal(StorageErrorKind.ChecksumMismatch, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task StorageEngine_ReadInvalidHeader_ThrowsFileManagerError()
    {
        var path = Path.Join(Path.GetTempPath(), $"aeternum-se-{Guid.NewGuid():N}.db");
        try
        {
            var config = new AeternumDB.Core.Config.StorageConfig
            {
                DataPath = path,
                BufferPoolSize = 4,
                PageSize = 256
            };

            PageId id;
            await using (var se = new StorageEngine(config))
            {
                id = await se.AllocatePageAsync();
                await se.WritePageDataAsync(id, 0, new byte[] { 9 });
            }

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                fs.Position = (long)id.Value * config.PageSize + 8;
                fs.WriteByte(250);
            }

            await using var reopened = new StorageEngine(config);
            var ex = await Assert.ThrowsAsync<StorageException>(async () => await reopened.ReadPageDataAsync(id, 0, 1));
            Assert.Equal(StorageErrorKind.FileManager, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task StorageEngine_DisposeTwice_IsSafe()
    {
        var path = Path.Join(Path.GetTempPath(), $"aeternum-se-{Guid.NewGuid():N}.db");
        var config = new AeternumDB.Core.Config.StorageConfig
        {
            DataPath = path,
            BufferPoolSize = 4,
            PageSize = 256
        };

        var se = new StorageEngine(config);
        await se.AllocatePageAsync();
        await se.DisposeAsync();
        await se.DisposeAsync();
        Assert.True(File.Exists(path));
        File.Delete(path);
    }
}
