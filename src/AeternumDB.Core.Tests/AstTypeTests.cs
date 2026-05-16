using AeternumDB.Core.Sql.Ast;

namespace AeternumDB.Core.Tests;

public sealed class AstTypeTests
{
    public static TheoryData<DataType, string> DataTypeToStringCases =>
        new()
        {
            { DataType.Integer.Instance, "INTEGER" },
            { DataType.UnsignedInt.Instance, "INTEGER UNSIGNED" },
            { DataType.TinyInt.Instance, "TINYINT" },
            { DataType.UnsignedTinyInt.Instance, "TINYINT UNSIGNED" },
            { DataType.SmallInt.Instance, "SMALLINT" },
            { DataType.UnsignedSmallInt.Instance, "SMALLINT UNSIGNED" },
            { DataType.MediumInt.Instance, "MEDIUMINT" },
            { DataType.UnsignedMediumInt.Instance, "MEDIUMINT UNSIGNED" },
            { DataType.BigInt.Instance, "BIGINT" },
            { DataType.UnsignedBigInt.Instance, "BIGINT UNSIGNED" },
            { DataType.Float.Instance, "FLOAT" },
            { DataType.Double.Instance, "DOUBLE" },
            { new DataType.Decimal(10, 2), "DECIMAL(10,2)" },
            { new DataType.Decimal(10, null), "DECIMAL(10)" },
            { new DataType.Decimal(null, null), "DECIMAL" },
            { DataType.Boolean.Instance, "BOOLEAN" },
            { DataType.Date.Instance, "DATE" },
            { DataType.Time.Instance, "TIME" },
            { DataType.TimeTz.Instance, "TIME WITH TIME ZONE" },
            { DataType.DateTime.Instance, "DATETIME" },
            { DataType.Timestamp.Instance, "TIMESTAMP" },
            { DataType.TimestampTz.Instance, "TIMESTAMP WITH TIME ZONE" },
            { new DataType.Char(16), "CHAR(16)" },
            { new DataType.Char(null), "CHAR" },
            { new DataType.Varchar(255), "VARCHAR(255)" },
            { new DataType.Varchar(null), "TEXT" },
            { DataType.TinyText.Instance, "TINYTEXT" },
            { DataType.MediumText.Instance, "MEDIUMTEXT" },
            { DataType.LongText.Instance, "LONGTEXT" },
            { new DataType.Binary(32), "BINARY(32)" },
            { new DataType.Binary(null), "BINARY" },
            { new DataType.Varbinary(32), "VARBINARY(32)" },
            { new DataType.Varbinary(null), "VARBINARY" },
            { new DataType.Blob(32), "BLOB(32)" },
            { new DataType.Blob(null), "BLOB" },
            { DataType.TinyBlob.Instance, "TINYBLOB" },
            { DataType.MediumBlob.Instance, "MEDIUMBLOB" },
            { DataType.LongBlob.Instance, "LONGBLOB" },
            { DataType.Uuid.Instance, "UUID" },
            { new DataType.Vector(DataType.Integer.Instance), "[INTEGER]" },
            { new DataType.Reference("users"), "users" },
            { new DataType.ReferenceArray("users"), "[users]" },
            { new DataType.VirtualReference("users", "id"), "~users(id)" },
            { new DataType.VirtualReferenceArray("users", "id"), "~[users](id)" },
            { new DataType.EnumRef("state"), "state" },
            { new DataType.Other("GEOGRAPHY"), "GEOGRAPHY" },
        };

    [Theory]
    [MemberData(nameof(DataTypeToStringCases))]
    public void DataType_ToString_ReturnsExpectedText(DataType value, string expected)
    {
        Assert.Equal(expected, value.ToString());
    }

    [Fact]
    public void DataType_Varchar_NullLengthRepresentsText()
    {
        var value = new DataType.Varchar(null);

        Assert.Null(value.Length);
        Assert.Equal("TEXT", value.ToString());
    }

    [Fact]
    public void IndexType_Singletons_AreStableAndAvailable()
    {
        Assert.Same(IndexType.BTree.Instance, IndexType.BTree.Instance);
        Assert.Same(IndexType.Hash.Instance, IndexType.Hash.Instance);
        Assert.Same(IndexType.Gin.Instance, IndexType.Gin.Instance);
        Assert.Same(IndexType.Gist.Instance, IndexType.Gist.Instance);
        Assert.Same(IndexType.SpGist.Instance, IndexType.SpGist.Instance);
        Assert.Same(IndexType.Brin.Instance, IndexType.Brin.Instance);
        Assert.Same(IndexType.Bloom.Instance, IndexType.Bloom.Instance);
        Assert.Same(IndexType.FullText.Instance, IndexType.FullText.Instance);
        Assert.Same(IndexType.Trigram.Instance, IndexType.Trigram.Instance);
    }

    [Fact]
    public void IndexType_Other_StoresCustomName()
    {
        var value = new IndexType.Other("CUSTOM");

        Assert.Equal("CUSTOM", value.Name);
    }
}
