namespace AeternumDB.Core.Executor;

using AeternumDB.Core.Errors;
using AeternumDB.Core.Types;

/// <summary>Simple in-memory table provider for testing.</summary>
public sealed class InMemoryTableProvider : ITableMutations
{
    private readonly Dictionary<string, (List<ColumnMeta> Schema, List<DbRow> Rows)> _tables
        = new(StringComparer.OrdinalIgnoreCase);

    private readonly object _lock = new();

    #region Public Methods

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

    public IAsyncEnumerable<DbRow> ScanAsync(string table) => ScanAsyncCore(table);

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

            foreach (var meta in entry.Schema.Where(meta => meta.IsArray))
                meta.InnerCount = 0;

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
            {
                foreach (var (col, val) in updates)
                    row.Set(col, val);
            }

            return ValueTask.FromResult(entry.Rows.Count);
        }
    }

    public bool TableExists(string table)
    {
        lock (_lock)
            return _tables.ContainsKey(table);
    }

    #endregion Public Methods

    #region Private Methods

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

    #endregion Private Methods
}
