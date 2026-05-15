using AeternumDB.Core.Types;

namespace AeternumDB.Core.Tests;

public class DbValueTests
{
    [Fact]
    public void Null_IsNull_ReturnsTrue()
    {
        DbValue v = DbValue.Null.Instance;
        Assert.True(v.IsNull);
    }

    [Fact]
    public void Integer_Equality()
    {
        DbValue a = new DbValue.Integer(42);
        DbValue b = new DbValue.Integer(42);
        Assert.Equal(a, b);
    }

    [Fact]
    public void Text_Equality()
    {
        DbValue a = new DbValue.Text("hello");
        DbValue b = new DbValue.Text("hello");
        Assert.Equal(a, b);
    }

    [Fact]
    public void DbRow_SetAndGet()
    {
        var row = new DbRow();
        row.Set("id", new DbValue.Integer(1));
        row.Set("name", new DbValue.Text("Alice"));

        Assert.Equal(new DbValue.Integer(1), row.Get("id"));
        Assert.Equal(new DbValue.Text("Alice"), row.Get("name"));
    }

    [Fact]
    public void DbRow_MissingColumn_ReturnsNull()
    {
        var row = new DbRow();
        Assert.True(row.Get("nonexistent").IsNull);
    }

    [Fact]
    public void RecordBatch_Empty()
    {
        var schema = new[] { new ColumnMeta("id", "integer") };
        var batch = RecordBatch.Empty(schema);
        Assert.Equal(0, batch.RowCount);
    }
}
