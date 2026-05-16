// Physical execution plan node hierarchy.
// Transpiled from poc/rust/src/query/physical_plan.rs and logical_plan.rs.

namespace AeternumDB.Core.Query;

using AeternumDB.Core.Sql;
using AeternumDB.Core.Sql.Ast;

// ── Supporting types ──────────────────────────────────────────────────────────

/// <summary>An expression paired with an output alias, used in projections.</summary>
public sealed class ProjectionItem(Expr expr, string? alias)
{
    public Expr Expr { get; } = expr;
    public string? Alias { get; } = alias;
}

/// <summary>A single aggregate function applied to a group.</summary>
public sealed class AggregateExpr(Expr func, string? alias)
{
    public Expr Func { get; } = func;
    public string? Alias { get; } = alias;
}

/// <summary>One term in an ORDER BY clause.</summary>
public sealed class SortExpr(Expr expr, bool ascending)
{
    public Expr Expr { get; } = expr;
    public bool Ascending { get; } = ascending;
}

/// <summary>A single item in a ViewAs projection.</summary>
public sealed class ViewAsProjection(Expr expr, string alias)
{
    public Expr Expr { get; } = expr;
    public string Alias { get; } = alias;
}

/// <summary>Cost annotation for a physical plan node.</summary>
public sealed class NodeCost(double rows, double cpu, double io)
{
    public double Rows { get; } = rows;
    public double Cpu { get; } = cpu;
    public double Io { get; } = io;
    public static readonly NodeCost Zero = new(0, 0, 0);
}

public enum SortAlgorithm { InMemory, External }

// ── PhysicalPlan discriminated union ─────────────────────────────────────────

/// <summary>A node in the physical execution plan tree.</summary>
public abstract class PhysicalPlan
{
    private PhysicalPlan() { }

    public sealed class SeqScan(
        string table,
        string? alias,
        List<string>? columns,
        Expr? filter,
        NodeCost cost) : PhysicalPlan
    {
        public string Table { get; } = table;
        public string? Alias { get; } = alias;
        public List<string>? Columns { get; } = columns;
        public new Expr? Filter { get; } = filter;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class IndexScan(
        string table,
        string? alias,
        string index,
        List<string>? columns,
        Expr keyPredicate,
        Expr? filter,
        NodeCost cost) : PhysicalPlan
    {
        public string Table { get; } = table;
        public string? Alias { get; } = alias;
        public string Index { get; } = index;
        public List<string>? Columns { get; } = columns;
        public Expr KeyPredicate { get; } = keyPredicate;
        public new Expr? Filter { get; } = filter;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class Filter(
        PhysicalPlan input,
        Expr predicate,
        NodeCost cost) : PhysicalPlan
    {
        public PhysicalPlan Input { get; } = input;
        public Expr Predicate { get; } = predicate;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class Project(
        PhysicalPlan input,
        List<ProjectionItem> items,
        NodeCost cost) : PhysicalPlan
    {
        public PhysicalPlan Input { get; } = input;
        public List<ProjectionItem> Items { get; } = items;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class NestedLoopJoin(
        PhysicalPlan left,
        PhysicalPlan right,
        JoinType joinType,
        Expr? condition,
        NodeCost cost) : PhysicalPlan
    {
        public PhysicalPlan Left { get; } = left;
        public PhysicalPlan Right { get; } = right;
        public JoinType JoinType { get; } = joinType;
        public Expr? Condition { get; } = condition;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class HashJoin(
        PhysicalPlan left,
        PhysicalPlan right,
        JoinType joinType,
        List<Expr> leftKeys,
        List<Expr> rightKeys,
        Expr? residual,
        NodeCost cost) : PhysicalPlan
    {
        public PhysicalPlan Left { get; } = left;
        public PhysicalPlan Right { get; } = right;
        public JoinType JoinType { get; } = joinType;
        public List<Expr> LeftKeys { get; } = leftKeys;
        public List<Expr> RightKeys { get; } = rightKeys;
        public Expr? Residual { get; } = residual;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class HashAggregate(
        PhysicalPlan input,
        List<Expr> groupBy,
        List<AggregateExpr> aggregates,
        Expr? having,
        NodeCost cost) : PhysicalPlan
    {
        public PhysicalPlan Input { get; } = input;
        public List<Expr> GroupBy { get; } = groupBy;
        public List<AggregateExpr> Aggregates { get; } = aggregates;
        public Expr? Having { get; } = having;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class Sort(
        PhysicalPlan input,
        List<SortExpr> orderBy,
        SortAlgorithm algorithm,
        NodeCost cost) : PhysicalPlan
    {
        public PhysicalPlan Input { get; } = input;
        public List<SortExpr> OrderBy { get; } = orderBy;
        public SortAlgorithm Algorithm { get; } = algorithm;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class Limit(
        PhysicalPlan input,
        int limitCount,
        int offset,
        NodeCost cost) : PhysicalPlan
    {
        public PhysicalPlan Input { get; } = input;
        public int LimitCount { get; } = limitCount;
        public int Offset { get; } = offset;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class Unnest(
        PhysicalPlan input,
        Expr column,
        string? alias,
        NodeCost cost) : PhysicalPlan
    {
        public PhysicalPlan Input { get; } = input;
        public Expr Column { get; } = column;
        public string? Alias { get; } = alias;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class ViewAs(
        PhysicalPlan input,
        List<ViewAsProjection> items,
        NodeCost cost) : PhysicalPlan
    {
        public PhysicalPlan Input { get; } = input;
        public List<ViewAsProjection> Items { get; } = items;
        public NodeCost Cost { get; } = cost;
    }

    public sealed class Values(
        List<List<Expr>> rows,
        NodeCost cost) : PhysicalPlan
    {
        public List<List<Expr>> Rows { get; } = rows;
        public NodeCost Cost { get; } = cost;
    }
}
