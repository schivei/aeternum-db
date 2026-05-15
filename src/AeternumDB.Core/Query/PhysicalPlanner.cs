namespace AeternumDB.Core.Query;

using AeternumDB.Core.Sql;

public sealed class PhysicalPlanner(CostModel costModel, StatisticsRegistry stats)
{
    private const int InMemorySortThreshold = 100_000;
    private readonly CostModel _costModel = costModel;
    private readonly StatisticsRegistry _stats = stats;

    public PhysicalPlan Lower(LogicalPlan plan) =>
        plan switch
        {
            LogicalPlan.Scan s => LowerScan(s.Table, s.Alias, s.Columns, s.ScanFilter),
            LogicalPlan.Filter f => LowerFilter(f.Input, f.Predicate),
            LogicalPlan.Project p => LowerProject(p.Input, p.Items),
            LogicalPlan.Join j => LowerJoin(j.Left, j.Right, j.JoinType, j.Condition),
            LogicalPlan.Aggregate a => LowerAggregate(a.Input, a.GroupBy, a.Aggregates, a.Having),
            LogicalPlan.Sort s => LowerSort(s.Input, s.OrderBy),
            LogicalPlan.Limit l => LowerLimit(l.Input, l.LimitRows, l.OffsetRows),
            LogicalPlan.Unnest u => LowerUnnest(u.Input, u.Column, u.Alias),
            LogicalPlan.ViewAs v => LowerViewAs(v.Input, v.Items),
            LogicalPlan.Values v => new PhysicalPlan.Values(v.Rows, new NodeCost(v.Rows.Count, 0, 0)),
            _ => new PhysicalPlan.Values([], new NodeCost(0, 0, 0))
        };

    private PhysicalPlan LowerScan(string table, string? alias, List<string>? columns, Expr? filter)
    {
        var tableStats = _stats.Get(table);
        var scanCost = _costModel.EstimateScanCost(tableStats);

        if (filter is not null && DetectIndexPredicate(filter) is string idx)
        {
            var rows = CostModel.EstimatedRows(tableStats.NumRows, 0.05);
            var io = scanCost * 0.1;
            var cpu = rows * _costModel.CpuCostFactor;
            return new PhysicalPlan.IndexScan(
                table,
                alias,
                idx,
                columns,
                filter,
                null,
                new NodeCost(rows, cpu, io));
        }

        var (seqRows, seqCpu) = filter is not null
            ? (CostModel.EstimatedRows(tableStats.NumRows, 0.1), _costModel.EstimateFilterCost(tableStats.NumRows, 0.1))
            : (tableStats.NumRows, tableStats.NumRows * _costModel.CpuCostFactor);

        return new PhysicalPlan.SeqScan(
            table,
            alias,
            columns,
            filter,
            new NodeCost(seqRows, seqCpu, tableStats.NumPages * _costModel.IoCostFactor));
    }

    private PhysicalPlan.Filter LowerFilter(LogicalPlan input, Expr predicate)
    {
        var child = Lower(input);
        var inRows = (int)ChildNodeRows(child);
        const double sel = 0.1;
        var cpu = _costModel.EstimateFilterCost(inRows, sel);
        var outRows = CostModel.EstimatedRows(inRows, sel);
        return new PhysicalPlan.Filter(child, predicate, new NodeCost(outRows, cpu, 0));
    }

    private PhysicalPlan.Project LowerProject(LogicalPlan input, List<ProjectionItem> items)
    {
        var child = Lower(input);
        var rows = (int)ChildNodeRows(child);
        var cpu = rows * _costModel.CpuCostFactor * 0.5;
        return new PhysicalPlan.Project(child, items, new NodeCost(rows, cpu, 0));
    }

    private PhysicalPlan LowerJoin(LogicalPlan left, LogicalPlan right, JoinType joinType, Expr? condition)
    {
        var leftPhys = Lower(left);
        var rightPhys = Lower(right);
        var lr = (int)ChildNodeRows(leftPhys);
        var rr = (int)ChildNodeRows(rightPhys);

        var (lk, rk, residual) = SplitJoinCondition(condition);
        var hasEquiKeys = lk.Count > 0;
        var useHash = hasEquiKeys && (lr > 100 || rr > 100);

        if (useHash)
        {
            var cpu = _costModel.EstimateHashJoinCost(lr, rr);
            var outRows = CostModel.EstimatedRows((int)Math.Min(int.MaxValue, (long)lr * rr / 100), 1.0);
            return new PhysicalPlan.HashJoin(leftPhys, rightPhys, joinType, lk, rk, residual, new NodeCost(outRows, cpu, 0));
        }

        var nlCondition = ReassembleCondition(condition, lk, rk, residual);
        var nlCpu = _costModel.EstimateNestedLoopCost(lr, rr);
        var nlRows = CostModel.EstimatedRows((int)Math.Min(int.MaxValue, (long)lr * rr / 100), 1.0);
        return new PhysicalPlan.NestedLoopJoin(leftPhys, rightPhys, joinType, nlCondition, new NodeCost(nlRows, nlCpu, 0));
    }

    private PhysicalPlan.HashAggregate LowerAggregate(LogicalPlan input, List<Expr> groupBy, List<AggregateExpr> aggs, Expr? having)
    {
        var child = Lower(input);
        var inRows = (int)ChildNodeRows(child);
        var groups = groupBy.Count == 0 ? 1 : Math.Max(1, inRows / 10);
        var cpu = _costModel.EstimateAggregateCost(inRows, groups);
        return new PhysicalPlan.HashAggregate(child, groupBy, aggs, having, new NodeCost(groups, cpu, 0));
    }

    private PhysicalPlan.Sort LowerSort(LogicalPlan input, List<SortExpr> orderBy)
    {
        var child = Lower(input);
        var rows = (int)ChildNodeRows(child);
        var cpu = _costModel.EstimateSortCost(rows);
        var algo = rows > InMemorySortThreshold ? SortAlgorithm.External : SortAlgorithm.InMemory;
        return new PhysicalPlan.Sort(child, orderBy, algo, new NodeCost(rows, cpu, 0));
    }

    private PhysicalPlan.Limit LowerLimit(LogicalPlan input, int limit, int offset)
    {
        var child = Lower(input);
        var rows = Math.Max(0, Math.Min(limit, (int)ChildNodeRows(child) - offset));
        return new PhysicalPlan.Limit(child, limit, offset, new NodeCost(rows, 0, 0));
    }

    private PhysicalPlan.Unnest LowerUnnest(LogicalPlan input, Expr column, string? alias)
    {
        var child = Lower(input);
        var rows = Math.Max(1, (int)ChildNodeRows(child) * 5);
        var cpu = rows * _costModel.CpuCostFactor;
        return new PhysicalPlan.Unnest(child, column, alias, new NodeCost(rows, cpu, 0));
    }

    private PhysicalPlan.ViewAs LowerViewAs(LogicalPlan input, List<ViewAsProjection> items)
    {
        var child = Lower(input);
        var rows = (int)ChildNodeRows(child);
        var cpu = rows * _costModel.CpuCostFactor;
        return new PhysicalPlan.ViewAs(child, items, new NodeCost(rows, cpu, 0));
    }

    private static double ChildNodeRows(PhysicalPlan plan) =>
        plan switch
        {
            PhysicalPlan.SeqScan s => s.Cost.Rows,
            PhysicalPlan.IndexScan s => s.Cost.Rows,
            PhysicalPlan.Filter f => f.Cost.Rows,
            PhysicalPlan.Project p => p.Cost.Rows,
            PhysicalPlan.NestedLoopJoin j => j.Cost.Rows,
            PhysicalPlan.HashJoin j => j.Cost.Rows,
            PhysicalPlan.HashAggregate a => a.Cost.Rows,
            PhysicalPlan.Sort s => s.Cost.Rows,
            PhysicalPlan.Limit l => l.Cost.Rows,
            PhysicalPlan.Unnest u => u.Cost.Rows,
            PhysicalPlan.ViewAs v => v.Cost.Rows,
            PhysicalPlan.Values v => v.Cost.Rows,
            _ => 0
        };

    private static string? DetectIndexPredicate(Expr pred)
    {
        if (pred is not Expr.BinaryOp b) return null;
        if (b.Op is not (BinaryOperator.Eq or BinaryOperator.Lt or BinaryOperator.LtEq or BinaryOperator.Gt or BinaryOperator.GtEq))
            return null;

        if (b.Left is Expr.Column leftColumn && b.Right is Expr.Literal)
            return $"{leftColumn.Name}_idx";
        if (b.Left is Expr.Literal && b.Right is Expr.Column rightColumn)
            return $"{rightColumn.Name}_idx";
        return null;
    }

    private static (List<Expr> leftKeys, List<Expr> rightKeys, Expr? residual) SplitJoinCondition(Expr? condition)
    {
        if (condition is Expr.BinaryOp { Op: BinaryOperator.Eq } b)
            return ([b.Left], [b.Right], null);
        return ([], [], condition);
    }

    private static Expr? BuildOriginalOrResidual(Expr? original, Expr? residual)
    {
        if (original is null) return residual;
        if (residual is null) return original;
        if (ReferenceEquals(original, residual)) return original;
        return new Expr.BinaryOp(original, BinaryOperator.And, residual);
    }

    private static Expr? ReassembleCondition(Expr? original, List<Expr> lk, List<Expr> rk, Expr? residual)
    {
        if (lk.Count == 0)
            return BuildOriginalOrResidual(original, residual);

        if (rk.Count != lk.Count)
            return BuildOriginalOrResidual(original, residual);

        var equi = new Expr.BinaryOp(lk[0], BinaryOperator.Eq, rk[0]);
        for (var i = 1; i < lk.Count; i++)
        {
            var eq = new Expr.BinaryOp(lk[i], BinaryOperator.Eq, rk[i]);
            equi = new Expr.BinaryOp(equi, BinaryOperator.And, eq);
        }

        if (residual is null) return equi;
        return new Expr.BinaryOp(equi, BinaryOperator.And, residual);
    }
}
