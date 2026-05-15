using AeternumDB.PoC.Shared.Config;
using AeternumDB.PoC.Shared.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

namespace AeternumDB.PoC.Shared.Abstractions;

// ── Storage ───────────────────────────────────────────────────────────────────

/// <summary>
/// Page-level storage engine. Mirrors Rust: StorageEngine
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

/// <summary>Buffer pool abstraction. Mirrors Rust: BufferPool</summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IBufferPool : IDisposable
{
    /// Insert a page (replace if already present).
    void Insert(PageId id, byte[] page);
    /// Pin and return a page; returns false if not found.
    bool TryPin(PageId id, out byte[]? page);
    /// Unpin a page. Marks it dirty if dirty=true.
    void Unpin(PageId id, bool dirty);
    /// Evict all unpinned dirty pages; returns page ids evicted.
    IReadOnlyList<(PageId Id, byte[] Data)> FlushDirty();
    int Capacity { get; }
    int Count { get; }
}

/// <summary>File manager abstraction. Mirrors Rust: FileManager</summary>
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
/// B-tree index. Mirrors Rust: BTree&lt;TKey, TValue&gt;
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
/// Mirrors Rust: ExecutionPlan trait
/// </summary>
[ServiceInjection(ServiceLifetime.Transient)]
public interface IExecutionPlan
{
    /// Execute and yield batches of rows.
    IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx);
    /// Schema produced by this operator.
    IReadOnlyList<ColumnMeta> Schema { get; }
}

/// <summary>
/// Execution context: table provider + ACL + id generator.
/// Mirrors Rust: ExecutionContext
/// </summary>
[ServiceInjection(ServiceLifetime.Scoped)]
public interface IExecutionContext
{
    ITableProvider TableProvider { get; }
    string CurrentUser { get; }
    long NextObjectId();
}

/// <summary>
/// Table data provider. Mirrors Rust: TableProvider trait
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
/// Full query planner pipeline. Mirrors Rust: QueryPlanner
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IQueryPlanner
{
    object Plan(object statement, object context);
}
