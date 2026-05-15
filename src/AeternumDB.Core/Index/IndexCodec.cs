using System.Buffers.Binary;
using System.Text;
using AeternumDB.Core.Errors;

namespace AeternumDB.Core.Index;

public interface IIndexCodec<T>
{
    byte[] Serialize(T value);
    T Deserialize(ReadOnlySpan<byte> bytes);
}

public static class IndexCodec
{
    public static IIndexCodec<T> Default<T>() => IndexCodecProvider<T>.Instance;
}

internal static class IndexCodecProvider<T>
{
    public static readonly IIndexCodec<T> Instance = Build();

    private static IIndexCodec<T> Build()
    {
        var t = typeof(T);
        if (t == typeof(long)) return (IIndexCodec<T>)(object)new Int64Codec();
        if (t == typeof(ulong)) return (IIndexCodec<T>)(object)new UInt64Codec();
        if (t == typeof(int)) return (IIndexCodec<T>)(object)new Int32Codec();
        if (t == typeof(uint)) return (IIndexCodec<T>)(object)new UInt32Codec();
        if (t == typeof(string)) return (IIndexCodec<T>)(object)new StringCodec();
        if (t == typeof(byte[])) return (IIndexCodec<T>)(object)new BytesCodec();
        return new UnsupportedCodec<T>();
    }
}

internal sealed class Int64Codec : IIndexCodec<long>
{
    public byte[] Serialize(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }

    public long Deserialize(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 8)
            throw new IndexException(IndexErrorKind.Serialization, "int64 must be 8 bytes");
        return BinaryPrimitives.ReadInt64LittleEndian(bytes);
    }
}

internal sealed class UInt64Codec : IIndexCodec<ulong>
{
    public byte[] Serialize(ulong value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        return bytes;
    }

    public ulong Deserialize(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 8)
            throw new IndexException(IndexErrorKind.Serialization, "uint64 must be 8 bytes");
        return BinaryPrimitives.ReadUInt64LittleEndian(bytes);
    }
}

internal sealed class Int32Codec : IIndexCodec<int>
{
    public byte[] Serialize(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        return bytes;
    }

    public int Deserialize(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 4)
            throw new IndexException(IndexErrorKind.Serialization, "int32 must be 4 bytes");
        return BinaryPrimitives.ReadInt32LittleEndian(bytes);
    }
}

internal sealed class UInt32Codec : IIndexCodec<uint>
{
    public byte[] Serialize(uint value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, value);
        return bytes;
    }

    public uint Deserialize(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 4)
            throw new IndexException(IndexErrorKind.Serialization, "uint32 must be 4 bytes");
        return BinaryPrimitives.ReadUInt32LittleEndian(bytes);
    }
}

internal sealed class StringCodec : IIndexCodec<string>
{
    public byte[] Serialize(string value)
    {
        if (value is null)
            throw new IndexException(IndexErrorKind.Serialization, "string key/value cannot be null");
        return Encoding.UTF8.GetBytes(value);
    }

    public string Deserialize(ReadOnlySpan<byte> bytes)
    {
        try
        {
            return Encoding.UTF8.GetString(bytes);
        }
        catch (Exception ex)
        {
            throw new IndexException(IndexErrorKind.Serialization, $"invalid utf-8 string: {ex.Message}");
        }
    }
}

internal sealed class BytesCodec : IIndexCodec<byte[]>
{
    public byte[] Serialize(byte[] value)
    {
        if (value is null)
            throw new IndexException(IndexErrorKind.Serialization, "byte[] key/value cannot be null");
        return [.. value];
    }

    public byte[] Deserialize(ReadOnlySpan<byte> bytes) => bytes.ToArray();
}

internal sealed class UnsupportedCodec<T> : IIndexCodec<T>
{
    public byte[] Serialize(T value) =>
        throw new IndexException(IndexErrorKind.Serialization,
            $"no default index codec for type '{typeof(T).FullName}'. Provide an explicit IIndexCodec<{typeof(T).Name}>.");

    public T Deserialize(ReadOnlySpan<byte> bytes) =>
        throw new IndexException(IndexErrorKind.Serialization,
            $"no default index codec for type '{typeof(T).FullName}'. Provide an explicit IIndexCodec<{typeof(T).Name}>.");
}
