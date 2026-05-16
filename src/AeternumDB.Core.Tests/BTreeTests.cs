using AeternumDB.Core.Config;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Index;
using AeternumDB.Core.Storage;
using AeternumDB.Core.Types;

namespace AeternumDB.Core.Tests;

public sealed class BTreeTests
{
    private static StorageConfig MakeStorageConfig(string path) =>
        new()
        {
            DataPath = path,
            BufferPoolSize = 128,
            PageSize = 4096
        };

    [Fact]
    public async Task Create_InvalidFanout_Throws()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var ex = await Assert.ThrowsAsync<IndexException>(async () =>
                await BTree<long, string>.CreateAsync(storage, new BTreeConfig { Fanout = 2 }));
            Assert.Equal(IndexErrorKind.InvalidFanout, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task InsertSearch_AndReopen_Works()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using (var storage = new StorageEngine(MakeStorageConfig(path)))
            {
                var tree = await BTree<long, string>.CreateAsync(storage, new BTreeConfig { Fanout = 4 });
                var meta = await tree.MetaPageIdAsync();

                await tree.InsertAsync(10, "ten");
                await tree.InsertAsync(20, "twenty");

                Assert.Equal("ten", await tree.SearchAsync(10));
                Assert.Equal("twenty", await tree.SearchAsync(20));
                Assert.Equal(2, tree.Count);

                // Reopen and validate persistence.
                var reopened = await BTree<long, string>.OpenAsync(storage, meta);
                Assert.Equal("ten", await reopened.SearchAsync(10));
                Assert.Equal("twenty", await reopened.SearchAsync(20));
            }
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Insert_SameKey_DoesNotIncreaseCount_AndUpdatesValue()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var tree = await BTree<long, string>.CreateAsync(storage, new BTreeConfig { Fanout = 4 });

            await tree.InsertAsync(10, "v1");
            await tree.InsertAsync(10, "v2");

            Assert.Equal(1, tree.Count);
            Assert.Equal("v2", await tree.SearchAsync(10));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task SplitAndRangeScan_Works()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var tree = await BTree<int, int>.CreateAsync(storage, new BTreeConfig { Fanout = 4 });

            for (var i = 1; i <= 30; i++)
                await tree.InsertAsync(i, i * 10);

            var range = await tree.RangeAsync(5, 12);
            Assert.Equal(8, range.Count);
            Assert.Equal((5, 50), range[0]);
            Assert.Equal((12, 120), range[^1]);
            Assert.Equal(30, tree.Count);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Range_WithFromGreaterThanTo_ReturnsEmpty()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var tree = await BTree<int, int>.CreateAsync(storage, new BTreeConfig { Fanout = 4 });

            for (var i = 1; i <= 10; i++)
                await tree.InsertAsync(i, i);

            var range = await tree.RangeAsync(10, 2);
            Assert.Empty(range);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Delete_Works()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var tree = await BTree<long, string>.CreateAsync(storage, new BTreeConfig { Fanout = 4 });

            await tree.InsertAsync(1, "a");
            await tree.InsertAsync(2, "b");
            await tree.InsertAsync(3, "c");

            var deleted = await tree.DeleteAsync(2);
            var missing = await tree.DeleteAsync(99);

            Assert.True(deleted);
            Assert.False(missing);
            Assert.Null(await tree.SearchAsync(2));
            Assert.Equal("a", await tree.SearchAsync(1));
            Assert.Equal("c", await tree.SearchAsync(3));
            Assert.Equal(2, tree.Count);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task BulkLoad_Works()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var tree = await BTree<int, string>.CreateAsync(storage, new BTreeConfig { Fanout = 4 });

            var entries = Enumerable.Range(1, 25).Select(i => (i, $"v{i}")).ToArray();
            await tree.BulkLoadAsync(entries);

            Assert.Equal(25, tree.Count);
            Assert.Equal("v1", await tree.SearchAsync(1));
            Assert.Equal("v25", await tree.SearchAsync(25));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Open_InvalidMetadataHeight_Throws()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var meta = await storage.AllocatePageAsync();
            var root = await storage.AllocatePageAsync();

            var payload = new byte[20];
            BitConverter.GetBytes((ulong)root).CopyTo(payload, 0);
            BitConverter.GetBytes(0).CopyTo(payload, 8);   // invalid height
            BitConverter.GetBytes(0).CopyTo(payload, 12);  // count
            BitConverter.GetBytes(100).CopyTo(payload, 16); // fanout

            await storage.WritePageDataAsync(meta, 0, BitConverter.GetBytes(payload.Length));
            await storage.WritePageDataAsync(meta, 4, payload);

            var ex = await Assert.ThrowsAsync<IndexException>(async () =>
                await BTree<long, string>.OpenAsync(storage, meta));
            Assert.Equal(IndexErrorKind.TreeCorrupted, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Open_InvalidMetadataFanout_Throws()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var meta = await storage.AllocatePageAsync();
            var root = await storage.AllocatePageAsync();

            var payload = new byte[20];
            BitConverter.GetBytes((ulong)root).CopyTo(payload, 0);
            BitConverter.GetBytes(1).CopyTo(payload, 8);   // height
            BitConverter.GetBytes(0).CopyTo(payload, 12);  // count
            BitConverter.GetBytes(2).CopyTo(payload, 16);  // invalid fanout

            await storage.WritePageDataAsync(meta, 0, BitConverter.GetBytes(payload.Length));
            await storage.WritePageDataAsync(meta, 4, payload);

            var ex = await Assert.ThrowsAsync<IndexException>(async () =>
                await BTree<long, string>.OpenAsync(storage, meta));
            Assert.Equal(IndexErrorKind.InvalidFanout, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Open_InvalidBlobLength_Throws()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var meta = await storage.AllocatePageAsync();
            await storage.WritePageDataAsync(meta, 0, BitConverter.GetBytes(int.MaxValue));

            var ex = await Assert.ThrowsAsync<IndexException>(async () =>
                await BTree<long, string>.OpenAsync(storage, meta));
            Assert.Equal(IndexErrorKind.TreeCorrupted, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Open_MetadataTooSmall_Throws()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var meta = await storage.AllocatePageAsync();
            await storage.WritePageDataAsync(meta, 0, BitConverter.GetBytes(4));
            await storage.WritePageDataAsync(meta, 4, new byte[] { 1, 2, 3, 4 });

            var ex = await Assert.ThrowsAsync<IndexException>(async () =>
                await BTree<long, string>.OpenAsync(storage, meta));
            Assert.Equal(IndexErrorKind.TreeCorrupted, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Delete_ManyKeys_ShrinksRootAndKeepsLastValue()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(MakeStorageConfig(path));
            var tree = await BTree<int, int>.CreateAsync(storage, new BTreeConfig { Fanout = 4 });

            for (var i = 1; i <= 40; i++)
                await tree.InsertAsync(i, i * 10);

            for (var i = 1; i <= 39; i++)
                Assert.True(await tree.DeleteAsync(i));

            Assert.Equal(1, tree.Count);
            Assert.Equal(400, await tree.SearchAsync(40));
            Assert.Equal(0, await tree.SearchAsync(1));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Insert_VeryLargeValue_ThrowsSerializationException()
    {
        var path = Path.Combine(Path.GetTempPath(), $"aeternum-index-{Guid.NewGuid():N}.db");
        try
        {
            await using var storage = new StorageEngine(new StorageConfig
            {
                DataPath = path,
                BufferPoolSize = 32,
                PageSize = 128
            });
            var tree = await BTree<long, string>.CreateAsync(storage, new BTreeConfig { Fanout = 4 });
            var largeValue = new string('x', 4096);

            var ex = await Assert.ThrowsAsync<IndexException>(async () =>
                await tree.InsertAsync(1, largeValue));
            Assert.Equal(IndexErrorKind.Serialization, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }
}
