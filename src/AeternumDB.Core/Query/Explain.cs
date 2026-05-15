namespace AeternumDB.Core.Query;

using AeternumDB.Core.Sql;

public static class Explain
{
    public static string ExplainPhysical(PhysicalPlan plan)
    {
        var lines = new List<string> { "Physical Plan:" };
        FormatNode(plan, lines, "", true);
        var total = SubtreeTotal(plan);
        lines.Add("");
        lines.Add($"Total Cost: {total:F2}");
        lines.Add($"Estimated Rows: {(int)NodeCostOf(plan).Rows}");
        return string.Join(Environment.NewLine, lines);
    }

    public static string ExplainLogical(LogicalPlan plan)
    {
        var lines = new List<string> { "Logical Plan:" };
        FormatLogicalNode(plan, lines, "", true);
        return string.Join(Environment.NewLine, lines);
    }

    private static void FormatNode(PhysicalPlan plan, List<string> outLines, string prefix, bool isLast)
    {
        var connector = isLast ? "└─ " : "├─ ";
        var childPrefix = isLast ? $"{prefix}   " : $"{prefix}│  ";
        var nodeCost = NodeCostOf(plan);

        outLines.Add($"{prefix}{connector}{NodeLabel(plan)}");
        outLines.Add($"{childPrefix}Est. rows: {(int)nodeCost.Rows} | Cost: {(nodeCost.Cpu + nodeCost.Io):F2} (I/O: {nodeCost.Io:F2}, CPU: {nodeCost.Cpu:F2})");

        var children = NodeChildren(plan);
        for (var i = 0; i < children.Count; i++)
            FormatNode(children[i], outLines, childPrefix, i == children.Count - 1);
    }

    private static void FormatLogicalNode(LogicalPlan plan, List<string> outLines, string prefix, bool isLast)
    {
        var connector = isLast ? "└─ " : "├─ ";
        var childPrefix = isLast ? $"{prefix}   " : $"{prefix}│  ";

        var (label, children) = LogicalLabelAndChildren(plan);
        outLines.Add($"{prefix}{connector}{label}");
        for (var i = 0; i < children.Count; i++)
            FormatLogicalNode(children[i], outLines, childPrefix, i == children.Count - 1);
    }

    private static string NodeLabel(PhysicalPlan plan) =>
        plan switch
        {
            PhysicalPlan.SeqScan s => $"SeqScan [table: {s.Table}]",
            PhysicalPlan.IndexScan s => $"IndexScan [table: {s.Table}, index: {s.Index}]",
            PhysicalPlan.Filter f => $"Filter [predicate: {DisplayExpr(f.Predicate)}]",
            PhysicalPlan.Project p => $"Project [{string.Join(", ", p.Items.Select(i => i.Alias ?? DisplayExpr(i.Expr)))}]",
            PhysicalPlan.NestedLoopJoin j => $"NestedLoopJoin [type: {j.JoinType}]",
            PhysicalPlan.HashJoin j => $"HashJoin [type: {j.JoinType}]",
            PhysicalPlan.HashAggregate a => $"HashAggregate [group_by: [{string.Join(", ", a.GroupBy.Select(DisplayExpr))}], aggregates: [{string.Join(", ", a.Aggregates.Select(x => x.Alias ?? DisplayExpr(x.Func)))}]]",
            PhysicalPlan.Sort s => $"Sort [{s.Algorithm}]",
            PhysicalPlan.Limit l => $"Limit [limit: {l.LimitCount}, offset: {l.Offset}]",
            PhysicalPlan.Unnest u => $"Unnest [alias: {u.Alias ?? "_"}]",
            PhysicalPlan.ViewAs v => $"ViewAs [{string.Join(", ", v.Items.Select(i => i.Alias))}]",
            PhysicalPlan.Values v => $"Values [{v.Rows.Count} row(s)]",
            _ => "Node"
        };

    private static (string label, List<LogicalPlan> children) LogicalLabelAndChildren(LogicalPlan plan) =>
        plan switch
        {
            LogicalPlan.Scan s => (s.Alias is null ? $"Scan [{s.Table}]" : $"Scan [{s.Table} AS {s.Alias}]", []),
            LogicalPlan.Filter f => ($"Filter [{DisplayExpr(f.Predicate)}]", [f.Input]),
            LogicalPlan.Project p => ($"Project [{string.Join(", ", p.Items.Select(i => i.Alias ?? DisplayExpr(i.Expr)))}]", [p.Input]),
            LogicalPlan.Join j => ($"Join [{j.JoinType}]", [j.Left, j.Right]),
            LogicalPlan.Aggregate a => ($"Aggregate [group_by: [{string.Join(", ", a.GroupBy.Select(DisplayExpr))}]]", [a.Input]),
            LogicalPlan.Sort s => ("Sort", [s.Input]),
            LogicalPlan.Limit l => ($"Limit [{l.LimitRows}]", [l.Input]),
            LogicalPlan.Unnest u => ("Unnest", [u.Input]),
            LogicalPlan.ViewAs v => ($"ViewAs [{string.Join(", ", v.Items.Select(i => i.Alias))}]", [v.Input]),
            LogicalPlan.Values v => ($"Values [{v.Rows.Count} row(s)]", []),
            _ => ("Node", [])
        };

    private static List<PhysicalPlan> NodeChildren(PhysicalPlan plan) =>
        plan switch
        {
            PhysicalPlan.SeqScan or PhysicalPlan.IndexScan or PhysicalPlan.Values => [],
            PhysicalPlan.Filter f => [f.Input],
            PhysicalPlan.Project p => [p.Input],
            PhysicalPlan.HashAggregate a => [a.Input],
            PhysicalPlan.Sort s => [s.Input],
            PhysicalPlan.Limit l => [l.Input],
            PhysicalPlan.Unnest u => [u.Input],
            PhysicalPlan.ViewAs v => [v.Input],
            PhysicalPlan.NestedLoopJoin j => [j.Left, j.Right],
            PhysicalPlan.HashJoin j => [j.Left, j.Right],
            _ => []
        };

    private static NodeCost NodeCostOf(PhysicalPlan plan) =>
        plan switch
        {
            PhysicalPlan.SeqScan s => s.Cost,
            PhysicalPlan.IndexScan s => s.Cost,
            PhysicalPlan.Filter f => f.Cost,
            PhysicalPlan.Project p => p.Cost,
            PhysicalPlan.NestedLoopJoin j => j.Cost,
            PhysicalPlan.HashJoin j => j.Cost,
            PhysicalPlan.HashAggregate a => a.Cost,
            PhysicalPlan.Sort s => s.Cost,
            PhysicalPlan.Limit l => l.Cost,
            PhysicalPlan.Unnest u => u.Cost,
            PhysicalPlan.ViewAs v => v.Cost,
            PhysicalPlan.Values v => v.Cost,
            _ => new NodeCost(0, 0, 0)
        };

    private static double SubtreeTotal(PhysicalPlan node)
    {
        var c = NodeCostOf(node);
        var own = c.Cpu + c.Io;
        return own + NodeChildren(node).Sum(SubtreeTotal);
    }

    private static string DisplayExpr(Expr expr) =>
        expr switch
        {
            Expr.Column c => c.Name,
            Expr.Wildcard => "*",
            _ => "<expr>"
        };
}
