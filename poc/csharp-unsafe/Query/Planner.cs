using AeternumDB.PoC.Shared.Abstractions;
using AeternumDB.PoC.Shared.Query;
using AeternumDB.PoC.Shared.Sql;
using AeternumDB.PoC.Shared.Types;
using AeternumDB.PoC.Unsafe.Executor;
using GenDI;
using Microsoft.Extensions.DependencyInjection;

namespace AeternumDB.PoC.Unsafe.Query;

/// <summary>
/// Translates <see cref="PhysicalPlan"/> nodes into executable <see cref="IExecutionPlan"/>
/// operator trees.
/// Mirrors Rust: executor/physical_plan.rs build_executor()
/// </summary>
[Injectable<IQueryPlanner>(ServiceLifetime.Singleton)]
public sealed class UnsafeQueryPlanner : IQueryPlanner
{
    public object Plan(object statement, object context)
    {
        // Stub: wires statement → logical → physical → operator tree
        // In a full implementation this calls LogicalPlanBuilder then PhysicalPlanner
        if (context is IExecutionContext ctx && statement is PhysicalPlan plan)
            return BuildExecutor(plan, ctx.TableProvider);
        return new ValuesExec([], []);
    }

    public static IExecutionPlan BuildExecutor(PhysicalPlan plan, ITableProvider provider) =>
        plan switch
        {
            PhysicalPlan.SeqScan scan =>
                new SeqScanExec(
                    scan.Table,
                    provider.Schema(scan.Table),
                    scan.Filter is null ? null : ExprEvaluator.BuildPredicate(scan.Filter)),

            PhysicalPlan.Filter filter =>
                new FilterExec(
                    BuildExecutor(filter.Input, provider),
                    ExprEvaluator.BuildPredicate(filter.Predicate)),

            PhysicalPlan.Project project =>
                new ProjectExec(
                    BuildExecutor(project.Input, provider),
                    project.Columns.Select(c => (
                        c.Alias ?? (c.Expression is Expr.Column col ? col.Name : "expr"),
                        (Func<DbRow, DbValue>)(row => ExprEvaluator.Eval(c.Expression, row))
                    )).ToList()),

            PhysicalPlan.Limit lim =>
                new LimitExec(BuildExecutor(lim.Input, provider), lim.LimitCount, lim.Offset),

            PhysicalPlan.Sort sort =>
                new SortExec(
                    BuildExecutor(sort.Input, provider),
                    sort.Order.Select(o => (
                        (Func<DbRow, DbValue>)(row => ExprEvaluator.Eval(o.Expression, row)),
                        o.Direction == OrderDirection.Asc
                    )).ToList()),

            PhysicalPlan.Distinct distinct =>
                new DistinctExec(BuildExecutor(distinct.Input, provider)),

            PhysicalPlan.HashAggregate agg =>
                new HashAggregateExec(
                    BuildExecutor(agg.Input, provider),
                    agg.GroupBy.Select(g => (Func<DbRow, DbValue>)(row => ExprEvaluator.Eval(g, row))).ToList(),
                    agg.Aggregates.Select(a =>
                    {
                        var (name, accum, extract) = ResolveAccumulator(a);
                        return (Name: name, Accumulate: accum, Extract: extract);
                    }).ToList()),

            PhysicalPlan.NestedLoopJoin nlj =>
                new NestedLoopJoinExec(
                    BuildExecutor(nlj.Left, provider),
                    BuildExecutor(nlj.Right, provider),
                    nlj.Condition is null ? null
                        : (l, r) =>
                        {
                            var merged = MergeRow(l, r);
                            return ExprEvaluator.BuildPredicate(nlj.Condition)(merged);
                        },
                    nlj.JoinType),

            PhysicalPlan.HashJoin hj =>
                new HashJoinExec(
                    BuildExecutor(hj.Left, provider),
                    BuildExecutor(hj.Right, provider),
                    row => hj.Condition is null ? DbValue.Null.Instance
                        : ExprEvaluator.Eval(hj.Condition, row),
                    row => hj.Condition is null ? DbValue.Null.Instance
                        : ExprEvaluator.Eval(hj.Condition, row)),

            PhysicalPlan.Values vals =>
                new ValuesExec(
                    vals.Rows.Select(r =>
                        (IReadOnlyList<DbValue>)r.Select(e => ExprEvaluator.Eval(e, new DbRow())).ToList()
                    ).ToList(),
                    []),

            PhysicalPlan.Unnest unnest =>
                new UnnestExec(BuildExecutor(unnest.Input, provider),
                    unnest.ArrayColumn, unnest.OutputColumn),

            _ => new ValuesExec([], []),
        };

    private static (string Name,
        Func<IReadOnlyList<DbValue>, DbValue> Accumulate,
        Func<DbRow, DbValue> Extract)
        ResolveAccumulator(SelectItem item)
    {
        var name = item.Alias ?? "agg";
        if (item.Expression is not Expr.Function fn)
            return (name, Accumulators.Count, row => ExprEvaluator.Eval(item.Expression, row));

        Func<IReadOnlyList<DbValue>, DbValue> accum = fn.Name.ToUpperInvariant() switch
        {
            "COUNT" => Accumulators.Count,
            "SUM" => Accumulators.Sum,
            "AVG" => Accumulators.Avg,
            "MIN" => Accumulators.Min,
            "MAX" => Accumulators.Max,
            _ => Accumulators.Count,
        };
        Func<DbRow, DbValue> extract = fn.Args.Count > 0
            ? row => ExprEvaluator.Eval(fn.Args[0], row)
            : _ => new DbValue.Integer(1);
        return (name, accum, extract);
    }

    private static DbRow MergeRow(DbRow l, DbRow r)
    {
        var row = new DbRow();
        foreach (var (k, v) in l.Columns) row.Set(k, v);
        foreach (var (k, v) in r.Columns) row.Set(k, v);
        return row;
    }
}
