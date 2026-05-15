namespace AeternumDB.Core.Query.Rules;

public interface IOptimizationRule
{
    string Name { get; }
    LogicalPlan Apply(LogicalPlan plan);
}

public sealed class RuleEngine
{
    private readonly List<IOptimizationRule> _rules = [];
    public int MaxIterations { get; set; } = 20;

    public RuleEngine AddRule(IOptimizationRule rule)
    {
        _rules.Add(rule);
        return this;
    }

    public LogicalPlan Optimize(LogicalPlan plan)
    {
        var current = plan;
        for (var i = 0; i < MaxIterations; i++)
        {
            var changed = false;
            foreach (var rule in _rules)
            {
                var next = rule.Apply(current);
                if (!ReferenceEquals(next, current))
                {
                    changed = true;
                    current = next;
                }
            }

            if (!changed)
                break;
        }

        return current;
    }
}

