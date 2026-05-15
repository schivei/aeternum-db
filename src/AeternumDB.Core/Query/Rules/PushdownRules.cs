namespace AeternumDB.Core.Query.Rules;

using AeternumDB.Core.Sql;

public sealed class PredicatePushdownRule : IOptimizationRule
{
    public string Name => "predicate_pushdown";

    public LogicalPlan Apply(LogicalPlan plan) => Push(plan);

    private static LogicalPlan Push(LogicalPlan plan) =>
        plan switch
        {
            LogicalPlan.Filter f => PushFilter(f.Input, f.Predicate),
            LogicalPlan.Project p => new LogicalPlan.Project(Push(p.Input), p.Items),
            LogicalPlan.Join j => new LogicalPlan.Join(Push(j.Left), Push(j.Right), j.JoinType, j.Condition),
            LogicalPlan.Sort s => new LogicalPlan.Sort(Push(s.Input), s.OrderBy),
            LogicalPlan.Limit l => new LogicalPlan.Limit(Push(l.Input), l.LimitRows, l.OffsetRows),
            LogicalPlan.Aggregate a => new LogicalPlan.Aggregate(Push(a.Input), a.GroupBy, a.Aggregates, a.Having),
            LogicalPlan.Unnest u => new LogicalPlan.Unnest(Push(u.Input), u.Column, u.Alias),
            LogicalPlan.ViewAs v => new LogicalPlan.ViewAs(Push(v.Input), v.Items),
            _ => plan
        };

    private static LogicalPlan PushFilter(LogicalPlan input, Expr predicate)
    {
        switch (input)
        {
            case LogicalPlan.Scan s when s.ScanFilter is null:
                return new LogicalPlan.Scan(s.Table, s.Alias, s.Columns, predicate);
            case LogicalPlan.Scan s:
                return new LogicalPlan.Scan(
                    s.Table,
                    s.Alias,
                    s.Columns,
                    new Expr.BinaryOp(s.ScanFilter!, BinaryOperator.And, predicate));
            case LogicalPlan.Join j:
            {
                var leftTable = ScanTableName(j.Left);
                var rightTable = ScanTableName(j.Right);
                var refsLeft = leftTable is not null && ReferencesTable(predicate, leftTable);
                var refsRight = rightTable is not null && ReferencesTable(predicate, rightTable);

                if (refsLeft && !refsRight)
                    return new LogicalPlan.Join(
                        Push(new LogicalPlan.Filter(j.Left, predicate)),
                        Push(j.Right),
                        j.JoinType,
                        j.Condition);

                if (!refsLeft && refsRight)
                    return new LogicalPlan.Join(
                        Push(j.Left),
                        Push(new LogicalPlan.Filter(j.Right, predicate)),
                        j.JoinType,
                        j.Condition);

                if (refsLeft && refsRight && j.JoinType == JoinType.Inner)
                {
                    var merged = j.Condition is null
                        ? predicate
                        : new Expr.BinaryOp(j.Condition, BinaryOperator.And, predicate);
                    return new LogicalPlan.Join(Push(j.Left), Push(j.Right), j.JoinType, merged);
                }

                return new LogicalPlan.Filter(new LogicalPlan.Join(Push(j.Left), Push(j.Right), j.JoinType, j.Condition), predicate);
            }
            default:
                return new LogicalPlan.Filter(Push(input), predicate);
        }
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

    private static bool ReferencesTable(Expr expr, string table) =>
        expr switch
        {
            Expr.Column c => c.Table is not null && c.Table.Equals(table, StringComparison.OrdinalIgnoreCase),
            Expr.BinaryOp b => ReferencesTable(b.Left, table) || ReferencesTable(b.Right, table),
            Expr.UnaryOp u => ReferencesTable(u.Inner, table),
            Expr.Cast c => ReferencesTable(c.Inner, table),
            Expr.IsNull i => ReferencesTable(i.Inner, table),
            Expr.Function f => f.Args.Any(a => ReferencesTable(a, table)),
            Expr.Between b => ReferencesTable(b.Inner, table) || ReferencesTable(b.Low, table) || ReferencesTable(b.High, table),
            Expr.InList i => ReferencesTable(i.Inner, table) || i.List.Any(x => ReferencesTable(x, table)),
            Expr.Case c => (c.Operand is not null && ReferencesTable(c.Operand, table))
                           || c.Conditions.Any(x => ReferencesTable(x.Condition, table) || ReferencesTable(x.Result, table))
                           || (c.ElseResult is not null && ReferencesTable(c.ElseResult, table)),
            Expr.ArrayOp a => ReferencesTable(a.Inner, table) || ReferencesTable(a.Right, table),
            Expr.Substring s => ReferencesTable(s.Inner, table)
                                || (s.FromPos is not null && ReferencesTable(s.FromPos, table))
                                || (s.Len is not null && ReferencesTable(s.Len, table)),
            Expr.Position p => ReferencesTable(p.Substr, table) || ReferencesTable(p.InExpr, table),
            Expr.Trim t => ReferencesTable(t.Inner, table) || (t.TrimWhat is not null && ReferencesTable(t.TrimWhat, table)),
            Expr.Overlay o => ReferencesTable(o.Inner, table)
                              || ReferencesTable(o.OverlayWhat, table)
                              || ReferencesTable(o.FromPos, table)
                              || (o.ForLen is not null && ReferencesTable(o.ForLen, table)),
            Expr.InSubquery or Expr.Subquery or Expr.MatchAgainst => true,
            _ => false
        };
}

public sealed class ProjectionPushdownRule : IOptimizationRule
{
    public string Name => "projection_pushdown";

    public LogicalPlan Apply(LogicalPlan plan) => Push(plan, null);

    private static LogicalPlan Push(LogicalPlan plan, HashSet<string>? requiredColumns)
    {
        switch (plan)
        {
            case LogicalPlan.Project p:
            {
                var needed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var item in p.Items)
                    CollectColumnNames(item.Expr, needed);
                if (requiredColumns is not null)
                    needed.UnionWith(requiredColumns);
                return new LogicalPlan.Project(Push(p.Input, needed), p.Items);
            }
            case LogicalPlan.Scan s when requiredColumns is { Count: > 0 }:
                return new LogicalPlan.Scan(s.Table, s.Alias, [.. requiredColumns], s.ScanFilter);
            case LogicalPlan.Filter f:
            {
                var next = requiredColumns is null
                    ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                    : new HashSet<string>(requiredColumns, StringComparer.OrdinalIgnoreCase);
                CollectColumnNames(f.Predicate, next);
                return new LogicalPlan.Filter(Push(f.Input, next), f.Predicate);
            }
            case LogicalPlan.Join j:
            {
                var next = requiredColumns is null
                    ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                    : new HashSet<string>(requiredColumns, StringComparer.OrdinalIgnoreCase);
                if (j.Condition is not null)
                    CollectColumnNames(j.Condition, next);
                return new LogicalPlan.Join(Push(j.Left, next), Push(j.Right, next), j.JoinType, j.Condition);
            }
            case LogicalPlan.Sort s:
                return new LogicalPlan.Sort(Push(s.Input, requiredColumns), s.OrderBy);
            case LogicalPlan.Limit l:
                return new LogicalPlan.Limit(Push(l.Input, requiredColumns), l.LimitRows, l.OffsetRows);
            case LogicalPlan.Aggregate a:
                return new LogicalPlan.Aggregate(Push(a.Input, requiredColumns), a.GroupBy, a.Aggregates, a.Having);
            case LogicalPlan.Unnest u:
                return new LogicalPlan.Unnest(Push(u.Input, requiredColumns), u.Column, u.Alias);
            case LogicalPlan.ViewAs v:
                return new LogicalPlan.ViewAs(Push(v.Input, requiredColumns), v.Items);
            default:
                return plan;
        }
    }

    private static void CollectColumnNames(Expr expr, HashSet<string> outSet)
    {
        switch (expr)
        {
            case Expr.Column c:
                if (c.Name == "*") return;
                outSet.Add(c.Name);
                break;
            case Expr.BinaryOp b:
                CollectColumnNames(b.Left, outSet);
                CollectColumnNames(b.Right, outSet);
                break;
            case Expr.UnaryOp u:
                CollectColumnNames(u.Inner, outSet);
                break;
            case Expr.Function f:
                foreach (var arg in f.Args) CollectColumnNames(arg, outSet);
                break;
            case Expr.Cast c:
                CollectColumnNames(c.Inner, outSet);
                break;
            case Expr.Between b:
                CollectColumnNames(b.Inner, outSet);
                CollectColumnNames(b.Low, outSet);
                CollectColumnNames(b.High, outSet);
                break;
            case Expr.InList i:
                CollectColumnNames(i.Inner, outSet);
                foreach (var it in i.List) CollectColumnNames(it, outSet);
                break;
            case Expr.Case c:
                if (c.Operand is not null) CollectColumnNames(c.Operand, outSet);
                foreach (var (cond, res) in c.Conditions)
                {
                    CollectColumnNames(cond, outSet);
                    CollectColumnNames(res, outSet);
                }
                if (c.ElseResult is not null) CollectColumnNames(c.ElseResult, outSet);
                break;
        }
    }
}
