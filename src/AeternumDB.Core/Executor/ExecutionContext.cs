// Execution context, ACL, ID generator, and in-memory table provider.
// Transpiled from poc/rust/src/executor/context.rs.

namespace AeternumDB.Core.Executor;

using AeternumDB.Core.Abstractions;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Types;

// ── ACL ───────────────────────────────────────────────────────────────────────

/// <summary>Access control list for privilege checking.</summary>
public sealed class Acl
{
    private readonly Dictionary<(string User, string Obj), HashSet<string>> _grants = new();

    public void Grant(string user, string obj, string privilege)
    {
        var key = (user, obj);
        if (!_grants.TryGetValue(key, out var set))
        {
            set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _grants[key] = set;
        }
        set.Add(privilege);
    }

    public void Revoke(string user, string obj, string privilege)
    {
        var key = (user, obj);
        if (_grants.TryGetValue(key, out var set))
            set.Remove(privilege);
    }

    public bool Check(string user, string obj, string privilege)
    {
        var key = (user, obj);
        return _grants.TryGetValue(key, out var set)
            && set.Contains(privilege, StringComparer.OrdinalIgnoreCase);
    }
}

// ── Object ID generator ───────────────────────────────────────────────────────

/// <summary>Generator for unique object IDs.</summary>
public interface IObjIdGenerator
{
    long NextId();
}

/// <summary>Thread-safe atomic counter-based ID generator.</summary>
public sealed class AtomicIdGenerator : IObjIdGenerator
{
    private long _counter;

    public AtomicIdGenerator(long start = 1) => _counter = start - 1;

    public long NextId() => Interlocked.Increment(ref _counter);
}

// ── Table mutation extension ──────────────────────────────────────────────────

/// <summary>
/// Extends ITableProvider with row-level update support required by DML operations.
/// </summary>
public interface ITableMutations : ITableProvider
{
    ValueTask<int> UpdateAsync(string table, IReadOnlyDictionary<string, DbValue> updates);
}

// ── InMemoryTableProvider ─────────────────────────────────────────────────────

/// <summary>Simple in-memory table provider for testing.</summary>
public sealed class InMemoryTableProvider : ITableMutations
{
    private readonly Dictionary<string, (List<ColumnMeta> Schema, List<DbRow> Rows)> _tables
        = new(StringComparer.OrdinalIgnoreCase);

    private readonly object _lock = new();

    public void AddTable(string name, IReadOnlyList<(string Name, string TypeName)> schema)
    {
        lock (_lock)
        {
            _tables[name] = (
                schema.Select(s => new ColumnMeta(s.Name, s.TypeName)).ToList(),
                new List<DbRow>());
        }
    }

    public void AddTableWithMeta(string name, IReadOnlyList<ColumnMeta> schema)
    {
        lock (_lock)
        {
            _tables[name] = (schema.ToList(), new List<DbRow>());
        }
    }

    public void SetColumnInnerCount(string table, string column, int count)
    {
        lock (_lock)
        {
            if (_tables.TryGetValue(table, out var entry))
            {
                var meta = entry.Schema.Find(m => string.Equals(m.Name, column, StringComparison.OrdinalIgnoreCase));
                if (meta is not null)
                    meta.InnerCount = count;
            }
        }
    }

    public void AddRows(string name, IReadOnlyList<DbRow> rows)
    {
        lock (_lock)
        {
            if (_tables.TryGetValue(name, out var entry))
                entry.Rows.AddRange(rows);
        }
    }

    public IAsyncEnumerable<DbRow> ScanAsync(string table)
        => ScanAsyncCore(table);

    private async IAsyncEnumerable<DbRow> ScanAsyncCore(string table)
    {
        List<DbRow> snapshot;
        lock (_lock)
        {
            if (!_tables.TryGetValue(table, out var entry))
                throw new ExecutorException(ExecutorErrorKind.TableNotFound, $"Table not found: {table}");
            snapshot = new List<DbRow>(entry.Rows);
        }
        foreach (var row in snapshot)
            yield return row;
    }

    public IReadOnlyList<ColumnMeta> Schema(string table)
    {
        lock (_lock)
        {
            if (!_tables.TryGetValue(table, out var entry))
                throw new ExecutorException(ExecutorErrorKind.TableNotFound, $"Table not found: {table}");
            return entry.Schema.AsReadOnly();
        }
    }

    public ValueTask<int> InsertAsync(string table, IReadOnlyList<DbRow> rows)
    {
        lock (_lock)
        {
            if (!_tables.TryGetValue(table, out var entry))
                throw new ExecutorException(ExecutorErrorKind.TableNotFound, $"Table not found: {table}");

            foreach (var row in rows)
            {
                foreach (var meta in entry.Schema)
                {
                    if (meta.IsArray && row.Get(meta.Name) is DbValue.Array arr)
                        meta.InnerCount = (meta.InnerCount ?? 0) + arr.Items.Length;
                }
            }
            entry.Rows.AddRange(rows);
            return ValueTask.FromResult(rows.Count);
        }
    }

    public ValueTask<int> DeleteAsync(string table)
    {
        lock (_lock)
        {
            if (!_tables.TryGetValue(table, out var entry))
                throw new ExecutorException(ExecutorErrorKind.TableNotFound, $"Table not found: {table}");
            int count = entry.Rows.Count;
            entry.Rows.Clear();
            foreach (var meta in entry.Schema)
                if (meta.IsArray) meta.InnerCount = 0;
            return ValueTask.FromResult(count);
        }
    }

    public ValueTask<int> UpdateAsync(string table, IReadOnlyDictionary<string, DbValue> updates)
    {
        lock (_lock)
        {
            if (!_tables.TryGetValue(table, out var entry))
                throw new ExecutorException(ExecutorErrorKind.TableNotFound, $"Table not found: {table}");
            foreach (var row in entry.Rows)
                foreach (var (col, val) in updates)
                    row.Set(col, val);
            return ValueTask.FromResult(entry.Rows.Count);
        }
    }

    public bool TableExists(string table)
    {
        lock (_lock) return _tables.ContainsKey(table);
    }
}

// ── ExecutionContext ───────────────────────────────────────────────────────────

/// <summary>
/// Execution context holding table provider, ACL, and object-ID generator.
/// Implements IExecutionContext for use with IExecutionPlan operators.
/// </summary>
public sealed class ExecutionContext : IExecutionContext
{
    private readonly IObjIdGenerator _objIdGen;

    public ITableProvider TableProvider { get; }
    public Acl Acl { get; }
    public string CurrentUser { get; }

    public ExecutionContext(
        ITableProvider tableProvider,
        Acl acl,
        IObjIdGenerator objIdGen,
        string currentUser)
    {
        TableProvider = tableProvider;
        Acl = acl;
        _objIdGen = objIdGen;
        CurrentUser = currentUser;
    }

    public long NextObjectId() => _objIdGen.NextId();

    /// <summary>Throws ExecutorException if the current user lacks the specified privilege.</summary>
    public void GrantPrivilege(string user, string obj, string privilege) =>
        Acl.Grant(user, obj, privilege);

    public void RevokePrivilege(string user, string obj, string privilege) =>
        Acl.Revoke(user, obj, privilege);

    public void CheckPrivilege(string obj, string privilege)
    {
        if (!Acl.Check(CurrentUser, obj, privilege))
            throw new ExecutorException(ExecutorErrorKind.PermissionDenied,
                $"User {CurrentUser} lacks {privilege} privilege on {obj}");
    }

    /// <summary>Factory for creating a default test context backed by an in-memory provider.</summary>
    public static ExecutionContext CreateForTesting() =>
        new(new InMemoryTableProvider(), new Acl(), new AtomicIdGenerator(), "test_user");
}
