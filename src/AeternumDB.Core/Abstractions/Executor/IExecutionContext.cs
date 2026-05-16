namespace AeternumDB.Core.Abstractions.Executor;

using GenDI;
using Microsoft.Extensions.DependencyInjection;

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
