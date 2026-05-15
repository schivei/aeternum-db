using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using CRTUnsafe = System.Runtime.CompilerServices.Unsafe;

namespace AeternumDB.PoC.Unsafe.Core;

/// <summary>
/// In-memory row store with an unsafe filter scan hot path.
/// The backing array is accessed via <see cref="MemoryMarshal.GetArrayDataReference{T}"/>
/// and a function pointer (<c>delegate*</c>) to eliminate virtual dispatch and
/// bounds checks in the tight filter loop.
/// </summary>
public sealed class RowStore
{
    private Row[] _rows;
    private int _count;

    public RowStore(int initialCapacity = 1024)
    {
        _rows = new Row[initialCapacity];
    }

    public void Add(Row row)
    {
        if (_count == _rows.Length)
            Array.Resize(ref _rows, _rows.Length * 2);
        _rows[_count++] = row;
    }

    /// <summary>
    /// Returns the count of rows matching <paramref name="predicate"/>.
    /// Uses <see cref="MemoryMarshal.GetArrayDataReference{T}"/> to suppress
    /// bounds-check instructions in the inner loop.
    /// </summary>
    public unsafe int Scan(delegate*<in Row, bool> predicate = null)
    {
        if (predicate == null) return _count;

        int count = 0;
        ref Row r0 = ref MemoryMarshal.GetArrayDataReference(_rows);
        int n = _count;
        for (int i = 0; i < n; i++)
        {
            if (predicate(CRTUnsafe.Add(ref r0, i))) count++;
        }
        return count;
    }

    public int Count => _count;
}

/// <summary>
/// Minimal row type — unmanaged struct to allow unsafe copy and pointer arithmetic.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct Row
{
    public readonly long Id;
    public readonly int Age;
    // Name is a managed reference; kept here to match the safe PoC.
    // In a real unsafe implementation this would be an interned char* or pooled index.
    public readonly string Name;

    public Row(long id, int age, string name)
    {
        Id = id;
        Age = age;
        Name = name;
    }
}
