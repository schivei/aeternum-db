namespace AeternumDB.Core.Query;

using AeternumDB.Core.Abstractions;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Executor;
using AeternumDB.Core.Sql;

public sealed class PlannerContext(Catalog catalog)
{
    public Catalog Catalog { get; } = catalog;
    public StatisticsRegistry Statistics { get; } = new();
    public CostModel CostModel { get; } = new();
    private readonly HashSet<string> _flatTables = new(StringComparer.OrdinalIgnoreCase);

    public void AddFlatTable(string name) => _flatTables.Add(name.ToLowerInvariant());
    public IReadOnlyCollection<string> FlatTables => _flatTables;
}

public sealed class QueryPlanner
{
    public static LogicalPlan CreateLogicalPlan(Statement stmt, PlannerContext ctx)
    {
        var builder = new LogicalPlanBuilder(ctx.Catalog);
        foreach (var table in ctx.FlatTables) builder.RegisterFlatTable(table);
        return builder.BuildFromStatement(stmt);
    }

    public static LogicalPlan Optimize(LogicalPlan plan, PlannerContext ctx)
    {
        var optimizer = new Optimizer(ctx.Statistics);
        return optimizer.Optimize(plan);
    }

    public static PhysicalPlan CreatePhysicalPlan(LogicalPlan plan, PlannerContext ctx)
    {
        var planner = new PhysicalPlanner(ctx.CostModel, ctx.Statistics);
        return planner.Lower(plan);
    }

    public static string Explain(PhysicalPlan plan) => global::AeternumDB.Core.Query.Explain.ExplainPhysical(plan);

    public PhysicalPlan Plan(Statement stmt, PlannerContext ctx)
    {
        var logical = CreateLogicalPlan(stmt, ctx);
        var optimized = Optimize(logical, ctx);
        return CreatePhysicalPlan(optimized, ctx);
    }
}

[GenDI.ServiceInjection(Microsoft.Extensions.DependencyInjection.ServiceLifetime.Singleton)]
public sealed class RuntimeQueryPlanner(Catalog catalog) : IQueryPlanner
{
    private readonly Catalog _catalog = catalog;
    private readonly QueryPlanner _planner = new();

    public IExecutionPlan Plan(string sql, IExecutionContext context)
    {
        Statement statement;
        try
        {
            statement = SqlParser.ParseOne(sql);
        }
        catch (SqlException ex)
        {
            throw new PlannerException(PlannerErrorKind.Other, ex.Message);
        }

        var ctx = new PlannerContext(_catalog);
        var physical = _planner.Plan(statement, ctx);
        return ExecutorBuilder.Build(physical);
    }
}
