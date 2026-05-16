// Maps a PhysicalPlan tree to concrete IExecutionPlan operator instances.
// Transpiled from poc/rust/src/executor/physical_plan.rs.

namespace AeternumDB.Core.Executor;

using AeternumDB.Core.Abstractions.Executor;
using AeternumDB.Core.Query;
using AeternumDB.Core.Sql;
using AeternumDB.Core.Sql.Ast;

/// <summary>
/// Builds an <see cref="IExecutionPlan"/> tree from a <see cref="PhysicalPlan"/> tree.
/// </summary>
public static class ExecutorBuilder
{
    /// <summary>Build an executor for the given physical plan node.</summary>
    public static IExecutionPlan Build(PhysicalPlan plan) =>
        plan switch
        {
            PhysicalPlan.SeqScan s =>
                new SeqScanExec(s.Table, s.Alias, s.Columns, s.Filter),

            PhysicalPlan.IndexScan s =>
                new IndexScanExec(s.Table, s.Alias, s.Index, s.Columns, s.KeyPredicate, s.Filter),

            PhysicalPlan.Filter f =>
                new FilterExec(Build(f.Input), f.Predicate),

            PhysicalPlan.Project p =>
                new ProjectExec(Build(p.Input), p.Items),

            PhysicalPlan.NestedLoopJoin j =>
                new NestedLoopJoinExec(Build(j.Left), Build(j.Right), j.JoinType, j.Condition),

            PhysicalPlan.HashJoin j =>
                new HashJoinExec(Build(j.Left), Build(j.Right), j.JoinType, j.LeftKeys, j.RightKeys, j.Residual),

            PhysicalPlan.HashAggregate a =>
                new HashAggregateExec(Build(a.Input), a.GroupBy, a.Aggregates, a.Having),

            PhysicalPlan.Sort s =>
                new SortExec(Build(s.Input), s.OrderBy),

            PhysicalPlan.Limit l =>
                new LimitExec(Build(l.Input), l.LimitCount, l.Offset),

            PhysicalPlan.Unnest u =>
                new UnnestExec(Build(u.Input), u.Column, u.Alias),

            PhysicalPlan.ViewAs v =>
                BuildViewAs(v),

            PhysicalPlan.Values v =>
                new ValuesExec(v.Rows, []),

            _ => throw new InvalidOperationException(
                $"Unsupported physical plan node: {plan.GetType().Name}")
        };

    /// <summary>Wrap a plan in a <see cref="DistinctExec"/>.</summary>
    public static IExecutionPlan BuildDistinct(IExecutionPlan inner) =>
        new DistinctExec(inner);

    /// <summary>Build a sort-merge join from pre-built left and right plans.</summary>
    public static IExecutionPlan BuildSortMergeJoin(
        IExecutionPlan left,
        IExecutionPlan right,
        JoinType joinType,
        System.Collections.Generic.IReadOnlyList<Expr> leftKeys,
        System.Collections.Generic.IReadOnlyList<Expr> rightKeys) =>
        new SortMergeJoinExec(left, right, joinType, leftKeys, rightKeys);

    // ── private helpers ───────────────────────────────────────────────────────

    private static ProjectExec BuildViewAs(PhysicalPlan.ViewAs view)
    {
        var inner = Build(view.Input);
        var projectionItems = view.Items
            .Select(item => new ProjectionItem(item.Expr, item.Alias))
            .ToList();
        return new ProjectExec(inner, projectionItems);
    }
}
