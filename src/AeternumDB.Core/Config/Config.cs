namespace AeternumDB.Core.Config;

/// <summary>
/// Configuration for the storage engine.
/// </summary>
public sealed class StorageConfig
{
    public string DataPath { get; init; } = "aeternumdb.db";
    public int BufferPoolSize { get; init; } = 1024;
    public int PageSize { get; init; } = 8192;
}

/// <summary>
/// Configuration for the B-tree index.
/// </summary>
public sealed class BTreeConfig
{
    /// <summary>Maximum keys per node. Must be in [4, 1000].</summary>
    public int Fanout { get; init; } = 100;
}
