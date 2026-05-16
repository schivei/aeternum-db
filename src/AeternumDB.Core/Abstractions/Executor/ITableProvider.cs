namespace AeternumDB.Core.Abstractions.Executor;

using AeternumDB.Core.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

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
