using AeternumDB.Core.Config;
using AeternumDB.Core.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

namespace AeternumDB.Core.Abstractions;

// ── Storage ───────────────────────────────────────────────────────────────────

/// <summary>
/// Page-level storage engine.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IStorageEngine : IAsyncDisposable
{
    ValueTask<PageId> AllocatePageAsync();
    ValueTask DeallocatePageAsync(PageId id);
    ValueTask WritePageDataAsync(PageId id, int offset, ReadOnlyMemory<byte> data);
    ValueTask<Memory<byte>> ReadPageDataAsync(PageId id, int offset, int length);
    StorageConfig Config { get; }
}

/// <summary>
/// Buffer pool: LRU in-memory cache of database pages.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IBufferPool : IDisposable
{
    void Insert(PageId id, byte[] page);
    bool TryPin(PageId id, out byte[]? page);
    void Unpin(PageId id, bool dirty);
    IReadOnlyList<(PageId Id, byte[] Data)> FlushDirty();
    int Capacity { get; }
    int Count { get; }
}

/// <summary>
/// Low-level file I/O for database pages.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IFileManager : IAsyncDisposable
{
    ValueTask WritePageAsync(PageId id, ReadOnlyMemory<byte> data);
    ValueTask<byte[]> ReadPageAsync(PageId id);
    ValueTask<PageId> AllocatePageAsync();
    ValueTask DeallocatePageAsync(PageId id);
}

// ── Index ─────────────────────────────────────────────────────────────────────

/// <summary>
/// Disk-backed B-tree index.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IBTree<TKey, TValue>
    where TKey : IComparable<TKey>
{
    ValueTask InsertAsync(TKey key, TValue value);
    ValueTask<TValue?> SearchAsync(TKey key);
    ValueTask<IReadOnlyList<(TKey Key, TValue Value)>> RangeAsync(TKey from, TKey to);
    ValueTask<bool> DeleteAsync(TKey key);
    ValueTask BulkLoadAsync(IReadOnlyList<(TKey Key, TValue Value)> entries);
    int Count { get; }
}

// ── Executor ──────────────────────────────────────────────────────────────────

/// <summary>
/// An execution plan operator — async stream pull model.
/// </summary>
[ServiceInjection(ServiceLifetime.Transient)]
public interface IExecutionPlan
{
    IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx);
    IReadOnlyList<ColumnMeta> Schema { get; }
}

/// <summary>
/// Execution context: table provider, current user, and object-id generator.
/// </summary>
[ServiceInjection(ServiceLifetime.Scoped)]
public interface IExecutionContext
{
    ITableProvider TableProvider { get; }
    string CurrentUser { get; }
    long NextObjectId();
}

/// <summary>
/// Table data provider used by executor operators.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface ITableProvider
{
    IAsyncEnumerable<DbRow> ScanAsync(string table);
    IReadOnlyList<ColumnMeta> Schema(string table);
    ValueTask<int> InsertAsync(string table, IReadOnlyList<DbRow> rows);
    ValueTask<int> DeleteAsync(string table);
    bool TableExists(string table);
}

// ── Query ─────────────────────────────────────────────────────────────────────

/// <summary>
/// Full SQL query planner pipeline: parse → logical → physical plan.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IQueryPlanner
{
    IExecutionPlan Plan(string sql, IExecutionContext context);
}
