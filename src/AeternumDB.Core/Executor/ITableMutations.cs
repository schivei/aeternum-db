namespace AeternumDB.Core.Executor;

using AeternumDB.Core.Abstractions.Executor;
using AeternumDB.Core.Types;

/// <summary>
/// Extends <see cref="ITableProvider"/> with row-level update support required by DML operations.
/// </summary>
public interface ITableMutations : ITableProvider
{
    ValueTask<int> UpdateAsync(string table, IReadOnlyDictionary<string, DbValue> updates);
}
