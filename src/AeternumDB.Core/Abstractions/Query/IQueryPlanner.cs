namespace AeternumDB.Core.Abstractions.Query;

using AeternumDB.Core.Abstractions.Executor;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Full SQL query planner pipeline: parse → logical → physical plan.
/// </summary>
[ServiceInjection(ServiceLifetime.Singleton)]
public interface IQueryPlanner
{
    IExecutionPlan Plan(string sql, IExecutionContext context);
}
