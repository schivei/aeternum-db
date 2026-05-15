namespace AeternumDB.Core.Query;

using AeternumDB.Core.Errors;
using AeternumDB.Core.Sql;

public abstract class LogicalPlan
{
    private LogicalPlan() { }

    public sealed class Scan(string table, string? alias, List<string>? columns, Expr? filter) : LogicalPlan
    {
        public string Table { get; } = table;
        public string? Alias { get; } = alias;
        public List<string>? Columns { get; } = columns;
        public Expr? ScanFilter { get; } = filter;
    }

    public sealed class Filter(LogicalPlan input, Expr predicate) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public Expr Predicate { get; } = predicate;
    }

    public sealed class Project(LogicalPlan input, List<ProjectionItem> items) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public List<ProjectionItem> Items { get; } = items;
    }

    public sealed class Join(LogicalPlan left, LogicalPlan right, JoinType joinType, Expr? condition) : LogicalPlan
    {
        public LogicalPlan Left { get; } = left;
        public LogicalPlan Right { get; } = right;
        public JoinType JoinType { get; } = joinType;
        public Expr? Condition { get; } = condition;
    }

    public sealed class Aggregate(
        LogicalPlan input,
        List<Expr> groupBy,
        List<AggregateExpr> aggregates,
        Expr? having) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public List<Expr> GroupBy { get; } = groupBy;
        public List<AggregateExpr> Aggregates { get; } = aggregates;
        public Expr? Having { get; } = having;
    }

    public sealed class Sort(LogicalPlan input, List<SortExpr> orderBy) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public List<SortExpr> OrderBy { get; } = orderBy;
    }

    public sealed class Limit(LogicalPlan input, int LimitCount, int Offset) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public int LimitRows { get; } = LimitCount;
        public int OffsetRows { get; } = Offset;
    }

    public sealed class Unnest(LogicalPlan input, Expr column, string? alias) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public Expr Column { get; } = column;
        public string? Alias { get; } = alias;
    }

    public sealed class ViewAs(LogicalPlan input, List<ViewAsProjection> items) : LogicalPlan
    {
        public LogicalPlan Input { get; } = input;
        public List<ViewAsProjection> Items { get; } = items;
    }

    public sealed class Values(List<List<Expr>> rows) : LogicalPlan
    {
        public List<List<Expr>> Rows { get; } = rows;
    }

    public int EstimatedRows() =>
        this switch
        {
            Scan => 1000,
            Filter f => Math.Max(1, f.Input.EstimatedRows() / 10),
            Project p => p.Input.EstimatedRows(),
            Join j => Math.Max(1, (int)Math.Min(int.MaxValue, (long)j.Left.EstimatedRows() * j.Right.EstimatedRows() / 100L)),
            Aggregate a => a.GroupBy.Count == 0 ? 1 : Math.Max(1, a.Input.EstimatedRows() / 10),
            Sort s => s.Input.EstimatedRows(),
            Limit l => l.LimitRows == int.MaxValue ? l.Input.EstimatedRows() : Math.Min(l.LimitRows, l.Input.EstimatedRows()),
            Unnest u => Math.Max(1, u.Input.EstimatedRows() * 5),
            ViewAs v => v.Input.EstimatedRows(),
            Values v => v.Rows.Count,
            _ => 1000
        };
}

public sealed class LogicalPlanBuilder(Catalog catalog)
{
    private readonly Catalog _catalog = catalog;
    private readonly HashSet<string> _flatTables = new(StringComparer.OrdinalIgnoreCase);

    public void RegisterFlatTable(string name) => _flatTables.Add(name.ToLowerInvariant());

    public LogicalPlan BuildFromStatement(Statement stmt) =>
        stmt switch
        {
            Statement.Select s => BuildSelect(s.Query),
            _ => throw new PlannerException(PlannerErrorKind.UnsupportedStatement, "only SELECT statements are supported by the query planner")
        };

    public LogicalPlan BuildSelect(SelectStatement stmt)
    {
        if (stmt.From is not null)
            CheckCrossDatabase(stmt.From);

        var basePlan = BuildFromClause(stmt.From);
        var filtered = stmt.WhereClause is null ? basePlan : new LogicalPlan.Filter(basePlan, stmt.WhereClause);
        var needsAgg = NeedsAggregate(stmt);
        var grouped = needsAgg
            ? new LogicalPlan.Aggregate(filtered, [.. stmt.GroupBy], CollectAggregates(stmt.Columns), stmt.Having)
            : filtered;
        var projected = ApplyProjection(grouped, stmt.Columns);
        var sorted = stmt.OrderBy.Count == 0
            ? projected
            : new LogicalPlan.Sort(projected, [.. stmt.OrderBy.Select(o => new SortExpr(o.Expr, o.Ascending))]);

        var limited = (stmt.Limit, stmt.Offset) switch
        {
            (null, null) => sorted,
            (ulong l, var off) => new LogicalPlan.Limit(sorted, checked((int)l), checked((int)(off ?? 0))),
            (null, ulong off) => new LogicalPlan.Limit(sorted, int.MaxValue, checked((int)off)),
        };

        return ApplyViewAs(limited, stmt.ViewAs);
    }

    private LogicalPlan BuildFromClause(TableReference? from) =>
        from is null ? new LogicalPlan.Values([[]]) : BuildTableRef(from);

    private LogicalPlan BuildTableRef(TableReference tr) =>
        tr switch
        {
            TableReference.Named n => BuildNamedTable(n.Name, n.Alias),
            TableReference.Subquery s => WrapSubquery(BuildSelect(s.Query), s.Alias),
            TableReference.Join j => BuildJoin(j.Left, j.Right, j.JoinType, j.FilterBy),
            _ => throw new PlannerException(PlannerErrorKind.Other, "unsupported table reference")
        };

    private LogicalPlan BuildNamedTable(string tableName, string? alias)
    {
        var lower = tableName.ToLowerInvariant();
        if (_catalog.GetTable(lower) is null)
            throw new PlannerException(PlannerErrorKind.CatalogError, $"table not found in catalog: {tableName}");

        return new LogicalPlan.Scan(lower, alias, null, null);
    }

    private LogicalPlan BuildJoin(TableReference left, TableReference right, JoinType joinType, Expr? filterBy)
    {
        var leftPlan = BuildTableRef(left);
        var rightPlan = BuildTableRef(right);
        RejectFlatInJoin(leftPlan);
        RejectFlatInJoin(rightPlan);
        return new LogicalPlan.Join(leftPlan, rightPlan, joinType, filterBy);
    }

    private LogicalPlan ApplyProjection(LogicalPlan input, IReadOnlyList<SelectItem> items)
    {
        if (items.Count == 0 || (items.Count == 1 && items[0] is SelectItem.Wildcard))
            return input;

        var projections = new List<ProjectionItem>();
        var unnestQueue = new List<(Expr Expr, string? Alias)>();

        foreach (var item in items)
        {
            switch (item)
            {
                case SelectItem.Wildcard:
                    projections.Add(new ProjectionItem(Expr.Wildcard.Instance, null));
                    break;
                case SelectItem.QualifiedWildcard qw:
                    projections.Add(new ProjectionItem(new Expr.Column(qw.Table, "*"), null));
                    break;
                case SelectItem.ExprItem e:
                    projections.Add(new ProjectionItem(e.Expr, e.Alias));
                    break;
                case SelectItem.Expand ex:
                    if (ex.Expr is not Expr.Column)
                        throw new PlannerException(PlannerErrorKind.InvalidExpand, "EXPAND requires a column reference");
                    unnestQueue.Add((ex.Expr, ex.Alias));
                    projections.Add(new ProjectionItem(ex.Expr, ex.Alias));
                    break;
            }
        }

        var plan = input;
        foreach (var (expr, alias) in unnestQueue)
            plan = new LogicalPlan.Unnest(plan, expr, alias);

        if (projections.Count == 0) return plan;
        return new LogicalPlan.Project(plan, projections);
    }

    private static LogicalPlan WrapSubquery(LogicalPlan inner, string alias) =>
        new LogicalPlan.Project(inner, [new ProjectionItem(new Expr.Column(alias, "*"), null)]);

    private static bool NeedsAggregate(SelectStatement stmt) =>
        stmt.GroupBy.Count > 0 || stmt.Columns.Any(c =>
            c is SelectItem.ExprItem e && ContainsAggregate(e.Expr));

    private static bool ContainsAggregate(Expr expr) =>
        expr switch
        {
            Expr.Function f => IsAggregateFunction(f.Name),
            Expr.BinaryOp b => ContainsAggregate(b.Left) || ContainsAggregate(b.Right),
            Expr.UnaryOp u => ContainsAggregate(u.Inner),
            Expr.Cast c => ContainsAggregate(c.Inner),
            Expr.IsNull i => ContainsAggregate(i.Inner),
            Expr.Between b => ContainsAggregate(b.Inner) || ContainsAggregate(b.Low) || ContainsAggregate(b.High),
            Expr.InList i => ContainsAggregate(i.Inner) || i.List.Any(ContainsAggregate),
            Expr.Case c => (c.Operand is not null && ContainsAggregate(c.Operand))
                           || c.Conditions.Any(x => ContainsAggregate(x.Condition) || ContainsAggregate(x.Result))
                           || (c.ElseResult is not null && ContainsAggregate(c.ElseResult)),
            _ => false
        };

    private static bool IsAggregateFunction(string name) =>
        name.Equals("COUNT", StringComparison.OrdinalIgnoreCase)
        || name.Equals("SUM", StringComparison.OrdinalIgnoreCase)
        || name.Equals("AVG", StringComparison.OrdinalIgnoreCase)
        || name.Equals("MIN", StringComparison.OrdinalIgnoreCase)
        || name.Equals("MAX", StringComparison.OrdinalIgnoreCase);

    private static List<AggregateExpr> CollectAggregates(IReadOnlyList<SelectItem> columns)
    {
        var result = new List<AggregateExpr>();
        foreach (var item in columns)
        {
            if (item is SelectItem.ExprItem e && ContainsAggregate(e.Expr))
                result.Add(new AggregateExpr(e.Expr, e.Alias));
        }
        return result;
    }

    private static LogicalPlan ApplyViewAs(LogicalPlan input, IReadOnlyList<ViewAsItem>? viewAs)
    {
        if (viewAs is null || viewAs.Count == 0) return input;
        var items = viewAs.Select(v => new ViewAsProjection(v.Expr, v.Alias)).ToList();
        return new LogicalPlan.ViewAs(input, items);
    }

    private static string? ScanTableName(LogicalPlan plan) =>
        plan switch
        {
            LogicalPlan.Scan s => s.Alias ?? s.Table,
            LogicalPlan.Filter f => ScanTableName(f.Input),
            LogicalPlan.Project p => ScanTableName(p.Input),
            LogicalPlan.Aggregate a => ScanTableName(a.Input),
            LogicalPlan.Sort s => ScanTableName(s.Input),
            LogicalPlan.Limit l => ScanTableName(l.Input),
            LogicalPlan.Unnest u => ScanTableName(u.Input),
            LogicalPlan.ViewAs v => ScanTableName(v.Input),
            _ => null
        };

    private void RejectFlatInJoin(LogicalPlan plan)
    {
        var table = ScanTableName(plan);
        if (!string.IsNullOrWhiteSpace(table) && _flatTables.Contains(table!))
            throw new PlannerException(PlannerErrorKind.FlatTableJoin, $"FLAT table '{table}' cannot participate in a join");
    }

    private static string? TableDb(TableReference tr) =>
        tr switch
        {
            TableReference.Named n => n.Database?.ToLowerInvariant(),
            TableReference.Subquery s => s.Query.From is null ? null : TableDb(s.Query.From),
            TableReference.Join j => TableDb(j.Left) ?? TableDb(j.Right),
            _ => null
        };

    private static void CheckCrossDatabase(TableReference from)
    {
        var dbs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        CollectDatabases(from, dbs);
        if (dbs.Count > 1)
            throw new PlannerException(PlannerErrorKind.CrossDatabaseJoin, $"cross-database joins are not permitted; databases: {string.Join(", ", dbs)}");
    }

    private static void CollectDatabases(TableReference tr, HashSet<string> dbs)
    {
        switch (tr)
        {
            case TableReference.Named n when !string.IsNullOrWhiteSpace(n.Database):
                dbs.Add(n.Database!.ToLowerInvariant());
                break;
            case TableReference.Subquery s when s.Query.From is not null:
                CollectDatabases(s.Query.From, dbs);
                break;
            case TableReference.Join j:
                CollectDatabases(j.Left, dbs);
                CollectDatabases(j.Right, dbs);
                break;
        }
    }
}
