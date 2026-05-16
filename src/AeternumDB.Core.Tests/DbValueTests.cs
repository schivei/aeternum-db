using AeternumDB.Core.Types;
using AeternumDB.Core.Errors;

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

    [Fact]
    public void PrimitiveValue_Conversions_Work()
    {
        DbValue i = new DbValue.Integer(42);
        DbValue f = new DbValue.Float(3.5);
        DbValue b = new DbValue.Boolean(true);
        DbValue t = new DbValue.Text("txt");
        DbValue n = DbValue.Null.Instance;

        Assert.Equal(42, i.AsInteger());
        Assert.Equal(42d, i.AsFloat());
        Assert.Equal(3.5, f.AsFloat());
        Assert.Equal("txt", t.AsString());
        Assert.Equal("42", i.AsString());
        Assert.True(b.AsBool());
        Assert.Null(n.AsBool());
        Assert.Null(n.AsInteger());
        Assert.Null(n.AsFloat());
        Assert.Null(n.AsString());
    }

    [Fact]
    public void ArrayAndBytes_Equality_AndToString_Work()
    {
        DbValue bytes1 = new DbValue.Bytes([1, 2, 3]);
        DbValue bytes2 = new DbValue.Bytes([1, 2, 3]);
        DbValue bytes3 = new DbValue.Bytes([1, 2]);
        Assert.Equal(bytes1, bytes2);
        Assert.NotEqual(bytes1, bytes3);
        Assert.Equal("<3 bytes>", bytes1.ToString());

        DbValue array = new DbValue.Array([new DbValue.Integer(1), new DbValue.Text("x")]);
        Assert.Equal("[1, x]", array.ToString());
        var asArray = array.AsArray();
        Assert.NotNull(asArray);
        Assert.Equal(2, asArray!.Length);
    }

    [Fact]
    public void DecimalAndJson_Equality_Work()
    {
        DbValue d1 = new DbValue.Decimal(10.5m);
        DbValue d2 = new DbValue.Decimal(10.5m);
        DbValue j1 = new DbValue.Json("{\"a\":1}");
        DbValue j2 = new DbValue.Json("{\"a\":1}");
        Assert.Equal(d1, d2);
        Assert.Equal(j1, j2);
        Assert.Equal("10.5", d1.ToString());
        Assert.Equal("{\"a\":1}", j1.ToString());
    }

    [Fact]
    public void ColumnMeta_IsArray_DetectsArrayAndVector()
    {
        Assert.True(new ColumnMeta("v", "array<int>").IsArray);
        Assert.True(new ColumnMeta("v", "VECTOR(3)").IsArray);
        Assert.False(new ColumnMeta("v", "int").IsArray);
    }

    [Fact]
    public void DbRow_FromPairs_AndCaseInsensitiveGet_Work()
    {
        var row = DbRow.FromPairs([
            ("Id", (DbValue)new DbValue.Integer(7)),
            ("Name", new DbValue.Text("Ana"))
        ]);

        Assert.Equal(new DbValue.Integer(7), row.Get("id"));
        Assert.Equal(new DbValue.Text("Ana"), row.Get("NAME"));
        Assert.Equal(2, row.ColumnCount);
        Assert.Equal(2, row.Columns.Count());
    }

    [Fact]
    public void RecordBatch_Constructor_StoresSchemaAndRows()
    {
        var schema = new[] { new ColumnMeta("id", "integer", innerCount: null) };
        var row = new DbRow();
        row.Set("id", new DbValue.Integer(1));
        var batch = new RecordBatch([row], schema);
        Assert.Equal(1, batch.RowCount);
        Assert.Single(batch.Rows);
        Assert.Single(batch.Schema);
    }

    [Fact]
    public void ErrorTypes_ExposeKind()
    {
        var se = new StorageException(StorageErrorKind.ChecksumMismatch, "x");
        var ie = new IndexException(IndexErrorKind.TreeCorrupted, "x");
        var ee = new ExecutorException(ExecutorErrorKind.EvalError, "x");
        var pe = new PlannerException(PlannerErrorKind.CatalogError, "x");

        Assert.Equal(StorageErrorKind.ChecksumMismatch, se.Kind);
        Assert.Equal(IndexErrorKind.TreeCorrupted, ie.Kind);
        Assert.Equal(ExecutorErrorKind.EvalError, ee.Kind);
        Assert.Equal(PlannerErrorKind.CatalogError, pe.Kind);
    }

    [Fact]
    public void DbValue_ObjectEquals_HashCode_AndToStringBranches_AreCovered()
    {
        DbValue n = DbValue.Null.Instance;
        Assert.Equal("NULL", n.ToString());
        Assert.True(n.Equals((object)DbValue.Null.Instance));
        _ = n.GetHashCode();

        var b = new DbValue.Boolean(true);
        Assert.Equal("TRUE", b.ToString());
        Assert.True(b.Equals((DbValue)new DbValue.Boolean(true)));
        Assert.Equal(new DbValue.Boolean(true).GetHashCode(), b.GetHashCode());

        var i = new DbValue.Integer(5);
        Assert.Equal("5", i.ToString());
        Assert.True(i.Equals((DbValue)new DbValue.Integer(5)));
        Assert.Equal(new DbValue.Integer(5).GetHashCode(), i.GetHashCode());

        var f = new DbValue.Float(1.5);
        Assert.Equal("1.5", f.ToString());
        Assert.True(f.Equals((DbValue)new DbValue.Float(1.5)));
        Assert.Equal(new DbValue.Float(1.5).GetHashCode(), f.GetHashCode());

        var t = new DbValue.Text("abc");
        Assert.Equal("abc", t.ToString());
        Assert.True(t.Equals((DbValue)new DbValue.Text("abc")));
        Assert.Equal(new DbValue.Text("abc").GetHashCode(), t.GetHashCode());

        var by = new DbValue.Bytes([1, 2, 3]);
        Assert.True(by.Equals((DbValue)new DbValue.Bytes([1, 2, 3])));
        Assert.Equal(new DbValue.Bytes([1, 2, 3]).GetHashCode(), by.GetHashCode());

        var arr = new DbValue.Array([new DbValue.Integer(1)]);
        Assert.True(arr.Equals((DbValue)new DbValue.Array([new DbValue.Integer(1)])));
        Assert.Equal(new DbValue.Array([new DbValue.Integer(1)]).GetHashCode(), arr.GetHashCode());

        var d = new DbValue.Decimal(1.25m);
        Assert.True(d.Equals((DbValue)new DbValue.Decimal(1.25m)));
        Assert.Equal(new DbValue.Decimal(1.25m).GetHashCode(), d.GetHashCode());

        var j = new DbValue.Json("{\"k\":1}");
        Assert.True(j.Equals((DbValue)new DbValue.Json("{\"k\":1}")));
        Assert.Equal(new DbValue.Json("{\"k\":1}").GetHashCode(), j.GetHashCode());
    }

    [Fact]
    public void DbValue_ConversionDefaultBranches_ReturnNull()
    {
        DbValue t = new DbValue.Text("x");
        DbValue b = new DbValue.Boolean(false);
        DbValue i = new DbValue.Integer(1);

        Assert.Null(t.AsInteger());
        Assert.Null(b.AsFloat());
        Assert.Null(i.AsBool());
        Assert.Null(t.AsArray());
        Assert.Null(DbValue.Null.Instance.AsArray());
    }

    [Fact]
    public void DbValue_EqualsAndToString_FalseBranches_AreCovered()
    {
        var boolean = new DbValue.Boolean(false);
        Assert.Equal("FALSE", boolean.ToString());
        Assert.False(boolean.Equals((DbValue)new DbValue.Boolean(true)));
        Assert.False(boolean.Equals((DbValue)new DbValue.Integer(0)));
        Assert.False(boolean.Equals((object)new DbValue.Boolean(true)));
        Assert.False(boolean.Equals((object)new DbValue.Integer(0)));

        var integer = new DbValue.Integer(10);
        Assert.False(integer.Equals((DbValue)new DbValue.Integer(11)));
        Assert.False(integer.Equals((DbValue)new DbValue.Boolean(true)));
        Assert.False(integer.Equals((object)new DbValue.Integer(11)));
        Assert.False(integer.Equals((object)new DbValue.Text("10")));

        var floating = new DbValue.Float(2.5);
        Assert.False(floating.Equals((DbValue)new DbValue.Float(2.75)));
        Assert.False(floating.Equals((DbValue)new DbValue.Text("2.5")));
        Assert.False(floating.Equals((object)new DbValue.Float(2.75)));
        Assert.False(floating.Equals((object)new DbValue.Integer(2)));

        var text = new DbValue.Text("abc");
        Assert.False(text.Equals((DbValue)new DbValue.Text("xyz")));
        Assert.False(text.Equals((DbValue)new DbValue.Bytes([97, 98, 99])));
        Assert.False(text.Equals((object)new DbValue.Text("xyz")));
        Assert.False(text.Equals((object)new DbValue.Json("\"abc\"")));

        var bytes = new DbValue.Bytes([1, 2, 3]);
        Assert.False(bytes.Equals((DbValue)new DbValue.Bytes([1, 2, 4])));
        Assert.False(bytes.Equals((DbValue)new DbValue.Integer(1)));
        Assert.False(bytes.Equals((object)new DbValue.Bytes([1, 2, 4])));
        Assert.False(bytes.Equals((object)new DbValue.Array([new DbValue.Integer(1)])));

        var array = new DbValue.Array([new DbValue.Integer(1)]);
        Assert.False(array.Equals((DbValue)new DbValue.Array([new DbValue.Integer(2)])));
        Assert.False(array.Equals((DbValue)new DbValue.Text("1")));
        Assert.False(array.Equals((object)new DbValue.Array([new DbValue.Integer(2)])));
        Assert.False(array.Equals((object)new DbValue.Bytes([1])));

        var decimalValue = new DbValue.Decimal(10.1m);
        Assert.False(decimalValue.Equals((DbValue)new DbValue.Decimal(11.1m)));
        Assert.False(decimalValue.Equals((DbValue)new DbValue.Boolean(true)));
        Assert.False(decimalValue.Equals((object)new DbValue.Decimal(11.1m)));
        Assert.False(decimalValue.Equals((object)new DbValue.Integer(10)));

        var json = new DbValue.Json("{\"a\":1}");
        Assert.False(json.Equals((DbValue)new DbValue.Json("{\"a\":2}")));
        Assert.False(json.Equals((DbValue)new DbValue.Decimal(1)));
        Assert.False(json.Equals((object)new DbValue.Json("{\"a\":2}")));
        Assert.False(json.Equals((object)new DbValue.Text("{\"a\":1}")));
    }
}
