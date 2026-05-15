namespace AeternumDB.Core.Types;

/// <summary>
/// Identifies a page within a database file.
/// </summary>
public readonly record struct PageId(ulong Value)
{
    public static readonly PageId Invalid = new(0);
    public override string ToString() => Value.ToString();
    public static implicit operator ulong(PageId id) => id.Value;
    public static implicit operator PageId(ulong v) => new(v);
}

/// <summary>
/// Type tag stored in each page header.
/// </summary>
public enum PageType : byte
{
    Data = 0,
    Index = 1,
    Overflow = 2,
    Free = 3,
}

/// <summary>
/// Fixed page header layout (16 bytes).
/// </summary>
public readonly struct PageHeader
{
    public const int Size = 16;

    public readonly ulong PageIdRaw;
    public readonly PageType PageType;
#pragma warning disable CS0414
    private readonly byte _reserved;
#pragma warning restore CS0414
    public readonly ushort FreeSpace;
    public readonly uint Checksum;

    public PageHeader(ulong pageId, PageType type, ushort freeSpace, uint checksum)
    {
        PageIdRaw = pageId;
        PageType = type;
        _reserved = 0;
        FreeSpace = freeSpace;
        Checksum = checksum;
    }
}
