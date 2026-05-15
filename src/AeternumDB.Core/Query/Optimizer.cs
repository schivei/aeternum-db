namespace AeternumDB.Core.Query;

using AeternumDB.Core.Query.Rules;
using AeternumDB.Core.Sql;

public sealed class ConstantFoldingRule : IOptimizationRule
{
    public string Name => "constant_folding";

    public LogicalPlan Apply(LogicalPlan plan) => RewritePlan(plan);

    private static LogicalPlan RewritePlan(LogicalPlan plan) =>
        plan switch
        {
            LogicalPlan.Filter f => new LogicalPlan.Filter(RewritePlan(f.Input), FoldExpr(f.Predicate)),
            LogicalPlan.Scan s => new LogicalPlan.Scan(s.Table, s.Alias, s.Columns, s.ScanFilter is null ? null : FoldExpr(s.ScanFilter)),
            LogicalPlan.Project p => new LogicalPlan.Project(
                RewritePlan(p.Input),
                [.. p.Items.Select(i => new ProjectionItem(FoldExpr(i.Expr), i.Alias))]),
            LogicalPlan.Join j => new LogicalPlan.Join(
                RewritePlan(j.Left),
                RewritePlan(j.Right),
                j.JoinType,
                j.Condition is null ? null : FoldExpr(j.Condition)),
            LogicalPlan.Aggregate a => new LogicalPlan.Aggregate(
                RewritePlan(a.Input),
                [.. a.GroupBy.Select(FoldExpr)],
                [.. a.Aggregates.Select(ae => new AggregateExpr(FoldExpr(ae.Func), ae.Alias))],
                a.Having is null ? null : FoldExpr(a.Having)),
            LogicalPlan.Sort s => new LogicalPlan.Sort(
                RewritePlan(s.Input),
                [.. s.OrderBy.Select(o => new SortExpr(FoldExpr(o.Expr), o.Ascending))]),
            LogicalPlan.Limit l => new LogicalPlan.Limit(RewritePlan(l.Input), l.LimitRows, l.OffsetRows),
            LogicalPlan.Unnest u => new LogicalPlan.Unnest(RewritePlan(u.Input), FoldExpr(u.Column), u.Alias),
            LogicalPlan.ViewAs v => new LogicalPlan.ViewAs(
                RewritePlan(v.Input),
                [.. v.Items.Select(i => new ViewAsProjection(FoldExpr(i.Expr), i.Alias))]),
            _ => plan
        };

    private static Expr FoldExpr(Expr expr)
    {
        if (expr is Expr.BinaryOp b)
        {
            var left = FoldExpr(b.Left);
            var right = FoldExpr(b.Right);
            return FoldBinary(left, b.Op, right);
        }

        if (expr is Expr.UnaryOp u)
        {
            var inner = FoldExpr(u.Inner);
            return FoldUnary(u.Op, inner);
        }

        return expr;
    }

    private static Expr FoldUnary(UnaryOperator op, Expr inner) =>
        (op, inner) switch
        {
            (UnaryOperator.Not, Expr.Literal { Value: SqlValue.Boolean b }) => new Expr.Literal(new SqlValue.Boolean(!b.Value)),
            (UnaryOperator.Minus, Expr.Literal { Value: SqlValue.Integer i }) => new Expr.Literal(new SqlValue.Integer(-i.Value)),
            _ => new Expr.UnaryOp(op, inner)
        };

    private static Expr FoldBinary(Expr left, BinaryOperator op, Expr right)
    {
        if (left is Expr.Literal { Value: SqlValue.Integer li } && right is Expr.Literal { Value: SqlValue.Integer ri })
        {
            return op switch
            {
                BinaryOperator.Plus => new Expr.Literal(new SqlValue.Integer(li.Value + ri.Value)),
                BinaryOperator.Minus => new Expr.Literal(new SqlValue.Integer(li.Value - ri.Value)),
                BinaryOperator.Multiply => new Expr.Literal(new SqlValue.Integer(li.Value * ri.Value)),
                _ => new Expr.BinaryOp(left, op, right)
            };
        }

        if (left is Expr.Literal { Value: SqlValue.Boolean lb })
        {
            if (op == BinaryOperator.And && lb.Value) return right;
            if (op == BinaryOperator.And && !lb.Value) return new Expr.Literal(new SqlValue.Boolean(false));
            if (op == BinaryOperator.Or && lb.Value) return new Expr.Literal(new SqlValue.Boolean(true));
            if (op == BinaryOperator.Or && !lb.Value) return right;
        }

        if (right is Expr.Literal { Value: SqlValue.Boolean rb })
        {
            if (op == BinaryOperator.And && rb.Value) return left;
            if (op == BinaryOperator.And && !rb.Value) return new Expr.Literal(new SqlValue.Boolean(false));
            if (op == BinaryOperator.Or && rb.Value) return new Expr.Literal(new SqlValue.Boolean(true));
            if (op == BinaryOperator.Or && !rb.Value) return left;
        }

        return new Expr.BinaryOp(left, op, right);
    }
}

public sealed class Optimizer(StatisticsRegistry stats)
{
    private readonly StatisticsRegistry _stats = stats;
    private readonly RuleEngine _engine = new RuleEngine()
        .AddRule(new ConstantFoldingRule())
        .AddRule(new PredicatePushdownRule())
        .AddRule(new ProjectionPushdownRule());

    public LogicalPlan Optimize(LogicalPlan plan)
    {
        var ruleOptimized = _engine.Optimize(plan);
        return ReorderJoins(ruleOptimized);
    }

    private LogicalPlan ReorderJoins(LogicalPlan plan) =>
        plan switch
        {
            LogicalPlan.Join j when j.JoinType == JoinType.Inner =>
                ReorderInnerJoin(j),
            LogicalPlan.Filter f => new LogicalPlan.Filter(ReorderJoins(f.Input), f.Predicate),
            LogicalPlan.Sort s => new LogicalPlan.Sort(ReorderJoins(s.Input), s.OrderBy),
            LogicalPlan.Project p => new LogicalPlan.Project(ReorderJoins(p.Input), p.Items),
            LogicalPlan.Aggregate a => new LogicalPlan.Aggregate(ReorderJoins(a.Input), a.GroupBy, a.Aggregates, a.Having),
            LogicalPlan.Limit l => new LogicalPlan.Limit(ReorderJoins(l.Input), l.LimitRows, l.OffsetRows),
            _ => plan
        };

    private LogicalPlan ReorderInnerJoin(LogicalPlan.Join join)
    {
        // Keep explicit join predicates in-place to avoid invalidating
        // side-qualified column references when swapping inputs.
        if (join.Condition is not null)
            return new LogicalPlan.Join(ReorderJoins(join.Left), ReorderJoins(join.Right), JoinType.Inner, join.Condition);

        var leftRows = ScanRows(join.Left);
        var rightRows = ScanRows(join.Right);
        if (rightRows < leftRows)
            return new LogicalPlan.Join(ReorderJoins(join.Right), ReorderJoins(join.Left), JoinType.Inner, join.Condition);
        return new LogicalPlan.Join(ReorderJoins(join.Left), ReorderJoins(join.Right), JoinType.Inner, join.Condition);
    }

    private int ScanRows(LogicalPlan plan) =>
        plan switch
        {
            LogicalPlan.Scan s => _stats.Get(s.Table).NumRows,
            _ => plan.EstimatedRows()
        };
}
