namespace AeternumDB.PoC.Shared.Query;

// ── Statistics ────────────────────────────────────────────────────────────────

/// <summary>Column-level statistics. Mirrors Rust: ColumnStats</summary>
public sealed class ColumnStats
{
    public string Name { get; init; } = "";
    public long? NumDistinct { get; set; }
    public long? NullCount { get; set; }
    public int? InnerCount { get; set; }
}

/// <summary>Table-level statistics. Mirrors Rust: TableStats</summary>
public sealed class TableStats
{
    public string TableName { get; init; } = "";
    public long NumRows { get; set; }
    public long NumPages { get; set; }
    public Dictionary<string, ColumnStats> Columns { get; } = new(StringComparer.OrdinalIgnoreCase);
}

/// <summary>Registry of all table statistics. Mirrors Rust: StatisticsRegistry</summary>
public sealed class StatisticsRegistry
{
    private readonly Dictionary<string, TableStats> _stats =
        new(StringComparer.OrdinalIgnoreCase);

    public void Add(TableStats stats) => _stats[stats.TableName] = stats;
    public TableStats? Get(string table) => _stats.GetValueOrDefault(table);
    public bool Contains(string table) => _stats.ContainsKey(table);
}

// ── CostModel ─────────────────────────────────────────────────────────────────

/// <summary>
/// Query cost estimation model.
/// Mirrors Rust: struct CostModel
/// </summary>
public sealed class CostModel
{
    public double CpuCostPerRow { get; init; } = 0.01;
    public double IoCostPerPage { get; init; } = 1.0;

    public long EstimatedRows(long baseRows, double selectivity = 1.0) =>
        (long)Math.Max(1, baseRows * selectivity);

    public double SeqScanCost(long rows, long pages) =>
        pages * IoCostPerPage + rows * CpuCostPerRow;

    public double IndexScanCost(long rows, double selectivity) =>
        rows * selectivity * (IoCostPerPage + CpuCostPerRow);

    public double NestedLoopCost(long leftRows, long rightRows) =>
        (double)leftRows * rightRows * CpuCostPerRow;
}

// ── LogicalPlan ───────────────────────────────────────────────────────────────

/// <summary>
/// Logical query plan tree.
/// Mirrors Rust: enum LogicalPlan
/// </summary>
public abstract class LogicalPlan
{
    private LogicalPlan() { }

    public sealed class Scan(string table, string? alias, Shared.Sql.Expr? filter) : LogicalPlan
    {
        public string Table { get; } = table;
        public string? Alias { get; } = alias;
        public new Shared.Sql.Expr? Filter { get; } = filter;
    }

    public sealed class Project(LogicalPlan input, IReadOnlyList<Shared.Sql.SelectItem> columns) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public IReadOnlyList<Shared.Sql.SelectItem> Columns { get; } = columns;
    }

    public sealed class Filter(LogicalPlan input, Shared.Sql.Expr predicate) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public Shared.Sql.Expr Predicate { get; } = predicate;
    }

    public sealed class Join(
        LogicalPlan left, LogicalPlan right,
        Shared.Sql.JoinType joinType,
        Shared.Sql.Expr? condition) : LogicalPlan
    {
        public LogicalPlan Left { get; } = left;
        public LogicalPlan Right { get; } = right;
        public Shared.Sql.JoinType JoinType { get; } = joinType;
        public Shared.Sql.Expr? Condition { get; } = condition;
    }

    public sealed class Aggregate(
        LogicalPlan input,
        IReadOnlyList<Shared.Sql.Expr> groupBy,
        IReadOnlyList<Shared.Sql.SelectItem> aggregates) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public IReadOnlyList<Shared.Sql.Expr> GroupBy { get; } = groupBy;
        public IReadOnlyList<Shared.Sql.SelectItem> Aggregates { get; } = aggregates;
    }

    public sealed class Sort(LogicalPlan input, IReadOnlyList<Shared.Sql.OrderByItem> order) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public IReadOnlyList<Shared.Sql.OrderByItem> Order { get; } = order;
    }

    public sealed class Limit(LogicalPlan input, long limit, long offset) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public long LimitCount { get; } = limit;
        public long Offset { get; } = offset;
    }

    public sealed class Distinct(LogicalPlan input) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
    }

    public sealed class Values(IReadOnlyList<IReadOnlyList<Shared.Sql.Expr>> rows) : LogicalPlan
    {
        public IReadOnlyList<IReadOnlyList<Shared.Sql.Expr>> Rows { get; } = rows;
    }
}

// ── PhysicalPlan ──────────────────────────────────────────────────────────────

/// <summary>Node-level cost annotation. Mirrors Rust: NodeCost</summary>
public sealed class NodeCost
{
    public double EstimatedCost { get; init; }
    public long EstimatedRows { get; init; }
    public static NodeCost Zero { get; } = new();
}

/// <summary>
/// Physical query plan tree.
/// Mirrors Rust: enum PhysicalPlan
/// </summary>
public abstract class PhysicalPlan
{
    private PhysicalPlan() { }

    public NodeCost Cost { get; init; } = NodeCost.Zero;

    public sealed class SeqScan : PhysicalPlan
    {
        public string Table { get; init; } = "";
        public string? Alias { get; init; }
        public IReadOnlyList<string>? Columns { get; init; }
        public new Shared.Sql.Expr? Filter { get; init; }
    }

    public sealed class IndexScan : PhysicalPlan
    {
        public string Table { get; init; } = "";
        public string IndexColumn { get; init; } = "";
        public Shared.Sql.Expr? Predicate { get; init; }
    }

    public sealed class Filter : PhysicalPlan
    {
        public PhysicalPlan Input { get; init; } = null!;
        public Shared.Sql.Expr Predicate { get; init; } = null!;
    }

    public sealed class Project : PhysicalPlan
    {
        public PhysicalPlan Input { get; init; } = null!;
        public IReadOnlyList<Shared.Sql.SelectItem> Columns { get; init; } = [];
    }

    public sealed class NestedLoopJoin : PhysicalPlan
    {
        public PhysicalPlan Left { get; init; } = null!;
        public PhysicalPlan Right { get; init; } = null!;
        public Shared.Sql.JoinType JoinType { get; init; }
        public Shared.Sql.Expr? Condition { get; init; }
    }

    public sealed class HashJoin : PhysicalPlan
    {
        public PhysicalPlan Left { get; init; } = null!;
        public PhysicalPlan Right { get; init; } = null!;
        public Shared.Sql.Expr? Condition { get; init; }
    }

    public sealed class HashAggregate : PhysicalPlan
    {
        public PhysicalPlan Input { get; init; } = null!;
        public IReadOnlyList<Shared.Sql.Expr> GroupBy { get; init; } = [];
        public IReadOnlyList<Shared.Sql.SelectItem> Aggregates { get; init; } = [];
    }

    public sealed class Sort : PhysicalPlan
    {
        public PhysicalPlan Input { get; init; } = null!;
        public IReadOnlyList<Shared.Sql.OrderByItem> Order { get; init; } = [];
    }

    public sealed class Limit : PhysicalPlan
    {
        public PhysicalPlan Input { get; init; } = null!;
        public long LimitCount { get; init; }
        public long Offset { get; init; }
    }

    public sealed class Distinct : PhysicalPlan
    {
        public PhysicalPlan Input { get; init; } = null!;
    }

    public sealed class Values : PhysicalPlan
    {
        public IReadOnlyList<IReadOnlyList<Shared.Sql.Expr>> Rows { get; init; } = [];
    }

    public sealed class Unnest : PhysicalPlan
    {
        public PhysicalPlan Input { get; init; } = null!;
        public string ArrayColumn { get; init; } = "";
        public string OutputColumn { get; init; } = "";
    }
}
