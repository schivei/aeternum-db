namespace AeternumDB.Core.Abstractions.Executor;

using AeternumDB.Core.Types;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// An execution plan operator — async stream pull model.
/// </summary>
[ServiceInjection(ServiceLifetime.Transient)]
public interface IExecutionPlan
{
    IAsyncEnumerable<RecordBatch> ExecuteAsync(IExecutionContext ctx);
    IReadOnlyList<ColumnMeta> Schema { get; }
}
