using AeternumDB.Core.Sql.Ast;

namespace AeternumDB.Core.Tests;

public sealed class AstTypeTests
{
    public static TheoryData<string, string> DataTypeToStringCases =>
        new()
        {
            { "Integer", "INTEGER" },
            { "UnsignedInt", "INTEGER UNSIGNED" },
            { "TinyInt", "TINYINT" },
            { "UnsignedTinyInt", "TINYINT UNSIGNED" },
            { "SmallInt", "SMALLINT" },
            { "UnsignedSmallInt", "SMALLINT UNSIGNED" },
            { "MediumInt", "MEDIUMINT" },
            { "UnsignedMediumInt", "MEDIUMINT UNSIGNED" },
            { "BigInt", "BIGINT" },
            { "UnsignedBigInt", "BIGINT UNSIGNED" },
            { "Float", "FLOAT" },
            { "Double", "DOUBLE" },
            { "Decimal_10_2", "DECIMAL(10,2)" },
            { "Decimal_10_null", "DECIMAL(10)" },
            { "Decimal_null_null", "DECIMAL" },
            { "Boolean", "BOOLEAN" },
            { "Date", "DATE" },
            { "Time", "TIME" },
            { "TimeTz", "TIME WITH TIME ZONE" },
            { "DateTime", "DATETIME" },
            { "Timestamp", "TIMESTAMP" },
            { "TimestampTz", "TIMESTAMP WITH TIME ZONE" },
            { "Char_16", "CHAR(16)" },
            { "Char_null", "CHAR" },
            { "Varchar_255", "VARCHAR(255)" },
            { "Varchar_null", "TEXT" },
            { "TinyText", "TINYTEXT" },
            { "MediumText", "MEDIUMTEXT" },
            { "LongText", "LONGTEXT" },
            { "Binary_32", "BINARY(32)" },
            { "Binary_null", "BINARY" },
            { "Varbinary_32", "VARBINARY(32)" },
            { "Varbinary_null", "VARBINARY" },
            { "Blob_32", "BLOB(32)" },
            { "Blob_null", "BLOB" },
            { "TinyBlob", "TINYBLOB" },
            { "MediumBlob", "MEDIUMBLOB" },
            { "LongBlob", "LONGBLOB" },
            { "Uuid", "UUID" },
            { "Vector_Integer", "[INTEGER]" },
            { "Reference_users", "users" },
            { "ReferenceArray_users", "[users]" },
            { "VirtualReference_users_id", "~users(id)" },
            { "VirtualReferenceArray_users_id", "~[users](id)" },
            { "EnumRef_state", "state" },
            { "Other_GEOGRAPHY", "GEOGRAPHY" },
        };

    [Theory]
    [MemberData(nameof(DataTypeToStringCases))]
    public void DataType_ToString_ReturnsExpectedText(string caseName, string expected)
    {
        var value = CreateDataTypeCase(caseName);
        Assert.Equal(expected, value.ToString());
    }

    private static DataType CreateDataTypeCase(string caseName) =>
        caseName switch
        {
            "Integer" => DataType.Integer.Instance,
            "UnsignedInt" => DataType.UnsignedInt.Instance,
            "TinyInt" => DataType.TinyInt.Instance,
            "UnsignedTinyInt" => DataType.UnsignedTinyInt.Instance,
            "SmallInt" => DataType.SmallInt.Instance,
            "UnsignedSmallInt" => DataType.UnsignedSmallInt.Instance,
            "MediumInt" => DataType.MediumInt.Instance,
            "UnsignedMediumInt" => DataType.UnsignedMediumInt.Instance,
            "BigInt" => DataType.BigInt.Instance,
            "UnsignedBigInt" => DataType.UnsignedBigInt.Instance,
            "Float" => DataType.Float.Instance,
            "Double" => DataType.Double.Instance,
            "Decimal_10_2" => new DataType.Decimal(10, 2),
            "Decimal_10_null" => new DataType.Decimal(10, null),
            "Decimal_null_null" => new DataType.Decimal(null, null),
            "Boolean" => DataType.Boolean.Instance,
            "Date" => DataType.Date.Instance,
            "Time" => DataType.Time.Instance,
            "TimeTz" => DataType.TimeTz.Instance,
            "DateTime" => DataType.DateTime.Instance,
            "Timestamp" => DataType.Timestamp.Instance,
            "TimestampTz" => DataType.TimestampTz.Instance,
            "Char_16" => new DataType.Char(16),
            "Char_null" => new DataType.Char(null),
            "Varchar_255" => new DataType.Varchar(255),
            "Varchar_null" => new DataType.Varchar(null),
            "TinyText" => DataType.TinyText.Instance,
            "MediumText" => DataType.MediumText.Instance,
            "LongText" => DataType.LongText.Instance,
            "Binary_32" => new DataType.Binary(32),
            "Binary_null" => new DataType.Binary(null),
            "Varbinary_32" => new DataType.Varbinary(32),
            "Varbinary_null" => new DataType.Varbinary(null),
            "Blob_32" => new DataType.Blob(32),
            "Blob_null" => new DataType.Blob(null),
            "TinyBlob" => DataType.TinyBlob.Instance,
            "MediumBlob" => DataType.MediumBlob.Instance,
            "LongBlob" => DataType.LongBlob.Instance,
            "Uuid" => DataType.Uuid.Instance,
            "Vector_Integer" => new DataType.Vector(DataType.Integer.Instance),
            "Reference_users" => new DataType.Reference("users"),
            "ReferenceArray_users" => new DataType.ReferenceArray("users"),
            "VirtualReference_users_id" => new DataType.VirtualReference("users", "id"),
            "VirtualReferenceArray_users_id" => new DataType.VirtualReferenceArray("users", "id"),
            "EnumRef_state" => new DataType.EnumRef("state"),
            "Other_GEOGRAPHY" => new DataType.Other("GEOGRAPHY"),
            _ => throw new ArgumentOutOfRangeException(nameof(caseName), caseName, null),
        };

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
