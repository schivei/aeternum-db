using System.Buffers.Binary;
using System.IO.Hashing;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Types;

namespace AeternumDB.Core.Storage;

/// <summary>
/// A single database page: 16-byte header followed by a data payload.
///
/// Layout (little-endian):
///   bytes  0.. 8  → PageId (u64)
///   byte   8      → PageType (u8)
///   byte   9      → reserved (0)
///   bytes 10..12  → FreeSpace (u16)
///   bytes 12..16  → Checksum (u32, CRC-32 of data payload)
///   bytes 16..    → Data payload
/// </summary>
public sealed class Page
{
    public PageHeader Header { get; private set; }
    public byte[] Data { get; }

    public int TotalSize => PageHeader.Size + Data.Length;
    public PageId Id => (PageId)Header.PageIdRaw;

    public Page(PageId id, PageType type, int dataCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(dataCapacity, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(dataCapacity, (int)ushort.MaxValue);

        Data = new byte[dataCapacity];
        var checksum = ComputeChecksum(Data);
        Header = new PageHeader((ulong)id, type, (ushort)dataCapacity, checksum);
    }

    private Page(PageHeader header, byte[] data)
    {
        Header = header;
        Data = data;
    }

    /// <summary>Compute CRC-32 checksum over the provided data span.</summary>
    public static uint ComputeChecksum(ReadOnlySpan<byte> data)
    {
        var crc = new Crc32();
        crc.Append(data);
        Span<byte> dest = stackalloc byte[sizeof(uint)];
        crc.GetCurrentHash(dest);
        return BinaryPrimitives.ReadUInt32LittleEndian(dest);
    }

    /// <summary>
    /// Compute CRC-32 of an all-zero payload of <paramref name="dataSize"/> bytes
    /// without allocating a buffer.
    /// </summary>
    public static uint ZeroDataChecksum(int dataSize)
    {
        Span<byte> chunk = stackalloc byte[64];
        chunk.Clear();
        var crc = new Crc32();
        int remaining = dataSize;
        while (remaining > 0)
        {
            int n = Math.Min(remaining, 64);
            crc.Append(chunk[..n]);
            remaining -= n;
        }
        Span<byte> dest = stackalloc byte[sizeof(uint)];
        crc.GetCurrentHash(dest);
        return BinaryPrimitives.ReadUInt32LittleEndian(dest);
    }

    /// <summary>Recompute and store the header checksum from current data contents.</summary>
    public void UpdateChecksum() =>
        Header = new PageHeader(Header.PageIdRaw, Header.PageType, Header.FreeSpace,
            ComputeChecksum(Data));

    /// <summary>Returns <see langword="true"/> when the stored checksum matches the data payload.</summary>
    public bool ValidateChecksum() => ComputeChecksum(Data) == Header.Checksum;

    /// <summary>Serialize the full page (header + data) into a new byte array.</summary>
    public byte[] Serialize()
    {
        var buf = new byte[TotalSize];
        SerializeHeader(Header, buf.AsSpan(0, PageHeader.Size));
        Data.CopyTo(buf, PageHeader.Size);
        return buf;
    }

    /// <summary>
    /// Deserialize a page from raw bytes.
    /// Returns <see langword="null"/> if the buffer is too short or the header is invalid.
    /// </summary>
    public static Page? Deserialize(ReadOnlySpan<byte> buf, int pageSize)
    {
        if (buf.Length < PageHeader.Size)
            return null;
        if (!TryDeserializeHeader(buf[..PageHeader.Size], out var header))
            return null;
        var data = buf[PageHeader.Size..].ToArray();
        return new Page(header, data);
    }

    /// <summary>Write <paramref name="src"/> into the data section at <paramref name="offset"/>.</summary>
    public void WriteData(int offset, ReadOnlySpan<byte> src)
    {
        var end = (long)offset + src.Length;
        if (end > Data.Length)
            throw new StorageException(StorageErrorKind.OutOfBounds,
                $"write out of bounds: offset={offset} len={src.Length} capacity={Data.Length}");

        src.CopyTo(Data.AsSpan(offset));
        var freeSpace = (ushort)(Data.Length - (int)end);
        Header = new PageHeader(Header.PageIdRaw, Header.PageType, freeSpace, ComputeChecksum(Data));
    }

    /// <summary>Read <paramref name="length"/> bytes from the data section at <paramref name="offset"/>.</summary>
    public ReadOnlySpan<byte> ReadData(int offset, int length)
    {
        var end = (long)offset + length;
        if (end > Data.Length)
            throw new StorageException(StorageErrorKind.OutOfBounds,
                $"read out of bounds: offset={offset} len={length} capacity={Data.Length}");
        return Data.AsSpan(offset, length);
    }

    internal static void SerializeHeader(PageHeader header, Span<byte> buf)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(buf[0..8], header.PageIdRaw);
        buf[8] = (byte)header.PageType;
        buf[9] = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(buf[10..12], header.FreeSpace);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[12..16], header.Checksum);
    }

    internal static bool TryDeserializeHeader(ReadOnlySpan<byte> buf, out PageHeader header)
    {
        var pageId = BinaryPrimitives.ReadUInt64LittleEndian(buf[0..8]);
        var typeByte = buf[8];
        if (typeByte > 3) { header = default; return false; }
        var freeSpace = BinaryPrimitives.ReadUInt16LittleEndian(buf[10..12]);
        var checksum = BinaryPrimitives.ReadUInt32LittleEndian(buf[12..16]);
        header = new PageHeader(pageId, (PageType)typeByte, freeSpace, checksum);
        return true;
    }
}
