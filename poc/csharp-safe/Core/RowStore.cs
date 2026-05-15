using System.Runtime.InteropServices;

namespace AeternumDB.PoC.Safe.Core;

/// <summary>
/// In-memory row store that models the executor's seq-scan table.
/// Uses <see cref="CollectionsMarshal.AsSpan{T}"/> to iterate without boxing.
/// </summary>
public sealed class RowStore
{
    private readonly List<Row> _rows = new();

    public void Add(Row row) => _rows.Add(row);

    /// <summary>
    /// Returns the number of rows that satisfy <paramref name="predicate"/>.
    /// When <paramref name="predicate"/> is null, counts all rows.
    /// Uses span-based iteration to avoid enumerator allocation.
    /// </summary>
    public int Scan(Func<Row, bool>? predicate = null)
    {
        var span = CollectionsMarshal.AsSpan(_rows);
        if (predicate is null) return span.Length;

        int count = 0;
        foreach (ref readonly var row in span)
            if (predicate(row)) count++;
        return count;
    }

    public int Count => _rows.Count;
}

/// <summary>
/// Minimal row type — value type to avoid heap allocation per row.
/// </summary>
public readonly struct Row
{
    public readonly long Id;
    public readonly int Age;
    public readonly string Name;

    public Row(long id, int age, string name)
    {
        Id = id;
        Age = age;
        Name = name;
    }
}
