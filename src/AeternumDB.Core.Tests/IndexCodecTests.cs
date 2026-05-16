using AeternumDB.Core.Errors;
using AeternumDB.Core.Index;

namespace AeternumDB.Core.Tests;

public sealed class IndexCodecTests
{
    [Fact]
    public void Default_Int64_Roundtrip()
    {
        var codec = IndexCodec.Default<long>();
        var bytes = codec.Serialize(-1234567890L);
        var value = codec.Deserialize(bytes);
        Assert.Equal(-1234567890L, value);
    }

    [Fact]
    public void Default_Int32_Roundtrip()
    {
        var codec = IndexCodec.Default<int>();
        var bytes = codec.Serialize(-12345);
        var value = codec.Deserialize(bytes);
        Assert.Equal(-12345, value);
    }

    [Fact]
    public void Default_UInt64_Roundtrip()
    {
        var codec = IndexCodec.Default<ulong>();
        var bytes = codec.Serialize(1234567890UL);
        var value = codec.Deserialize(bytes);
        Assert.Equal(1234567890UL, value);
    }

    [Fact]
    public void Default_UInt32_Roundtrip()
    {
        var codec = IndexCodec.Default<uint>();
        var bytes = codec.Serialize(12345U);
        var value = codec.Deserialize(bytes);
        Assert.Equal(12345U, value);
    }

    [Fact]
    public void Default_String_Roundtrip()
    {
        var codec = IndexCodec.Default<string>();
        var bytes = codec.Serialize("áéí");
        var value = codec.Deserialize(bytes);
        Assert.Equal("áéí", value);
    }

    [Fact]
    public void Default_Bytes_Roundtrip()
    {
        var codec = IndexCodec.Default<byte[]>();
        var bytes = codec.Serialize([1, 2, 3, 4]);
        var value = codec.Deserialize(bytes);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, value);
    }

    [Fact]
    public void StringCodec_Null_Throws()
    {
        var codec = new StringCodec();
        var ex = Assert.Throws<IndexException>(() => codec.Serialize(null!));
        Assert.Equal(IndexErrorKind.Serialization, ex.Kind);
    }

    [Fact]
    public void BytesCodec_Null_Throws()
    {
        var codec = new BytesCodec();
        var ex = Assert.Throws<IndexException>(() => codec.Serialize(null!));
        Assert.Equal(IndexErrorKind.Serialization, ex.Kind);
    }

    [Fact]
    public void UnsupportedCodec_Throws()
    {
        var codec = IndexCodec.Default<DateTime>();
        var ex1 = Assert.Throws<IndexException>(() => codec.Serialize(DateTime.UtcNow));
        var ex2 = Assert.Throws<IndexException>(() => codec.Deserialize([1, 2, 3]));
        Assert.Equal(IndexErrorKind.Serialization, ex1.Kind);
        Assert.Equal(IndexErrorKind.Serialization, ex2.Kind);
    }

    [Fact]
    public void PrimitiveCodecs_InvalidPayloadLength_Throws()
    {
        var i64 = IndexCodec.Default<long>();
        var u64 = IndexCodec.Default<ulong>();
        var i32 = IndexCodec.Default<int>();
        var u32 = IndexCodec.Default<uint>();

        Assert.Throws<IndexException>(() => i64.Deserialize([1, 2, 3]));
        Assert.Throws<IndexException>(() => u64.Deserialize([1, 2, 3, 4, 5, 6, 7]));
        Assert.Throws<IndexException>(() => i32.Deserialize([1, 2, 3]));
        Assert.Throws<IndexException>(() => u32.Deserialize([1, 2, 3, 4, 5]));
    }
}
