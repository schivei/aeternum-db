namespace AeternumDB.Core.Query;

/// <summary>Algorithm used to sort rows in a physical sort node.</summary>
public enum SortAlgorithm
{
    /// <summary>All rows fit in memory.</summary>
    InMemory,

    /// <summary>Rows are spilled to disk during sorting.</summary>
    External,
}
