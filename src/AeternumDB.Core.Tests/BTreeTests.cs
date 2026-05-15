using AeternumDB.Core.Config;
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
}

