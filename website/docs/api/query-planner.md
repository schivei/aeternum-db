---
sidebar_position: 4
---

# ⚡ QueryPlanner API

Full API reference for `AeternumDB.Core.Query.QueryPlanner`.

---

## Planning a Query

```csharp
using AeternumDB.Core.Query;
using AeternumDB.Core.Sql;

var parser  = new SqlParser();
var planner = new QueryPlanner();

// 1. Parse SQL
Statement stmt = parser.ParseOne("SELECT id, name FROM users WHERE age > 18");

// 2. Build context (catalog + optional statistics)
var context = new PlannerContext(catalog)
{
    Statistics = myStatisticsRegistry,
    CostModel  = CostModel.Default,
};

// 3. Plan → PhysicalPlan
PhysicalPlan physical = planner.Plan(stmt, context);

// 4. Explain (optional)
Console.WriteLine(planner.Explain(physical));
```

---

## SqlParser

```csharp
var parser = new SqlParser();

// Parse a single statement
Statement stmt = parser.ParseOne("SELECT 1");

// Parse multiple statements
IReadOnlyList<Statement> stmts = parser.ParseMany("SELECT 1; SELECT 2;");
```

### Error Handling

```csharp
try
{
    var stmt = parser.ParseOne("SELEKT * FORM users");
}
catch (SqlParseException ex)
{
    Console.WriteLine($"Parse error at column {ex.Column}: {ex.Message}");
}
```

---

## QueryPlanner

### Plan

```csharp
PhysicalPlan Plan(Statement stmt, PlannerContext context);
```

Throws `PlannerException` (wrapping `PlannerError`) if:
- A referenced table or column is not in the catalog
- A cross-database join is detected
- A FLAT table is used in a join
- The statement contains an unsupported construct

### Explain

```csharp
// Physical plan tree with cost annotations
string tree = planner.Explain(physical);
Console.WriteLine(tree);
```

### PlanLogical (intermediate)

```csharp
// Build the logical plan only (before optimization)
LogicalPlan logical = planner.PlanLogical(stmt, context);
string logicalTree  = ExplainFormatter.ExplainLogical(logical);
```

---

## PhysicalPlan Nodes

```csharp
public abstract record PhysicalPlan
{
    public NodeCost Cost { get; init; }

    public sealed record SeqScan(string Table, Expr? Filter, NodeCost Cost)
        : PhysicalPlan;

    public sealed record IndexScan(string Table, string IndexName, Expr Predicate, NodeCost Cost)
        : PhysicalPlan;

    public sealed record Filter(PhysicalPlan Input, Expr Predicate, NodeCost Cost)
        : PhysicalPlan;

    public sealed record Project(PhysicalPlan Input, IReadOnlyList<ProjectionItem> Items, NodeCost Cost)
        : PhysicalPlan;

    public sealed record NestedLoopJoin(PhysicalPlan Left, PhysicalPlan Right, JoinType Type, Expr? Condition, NodeCost Cost)
        : PhysicalPlan;

    public sealed record HashJoin(PhysicalPlan Left, PhysicalPlan Right, JoinType Type, Expr? Condition, NodeCost Cost)
        : PhysicalPlan;

    public sealed record HashAggregate(PhysicalPlan Input, IReadOnlyList<Expr> GroupBy, IReadOnlyList<AggregateExpr> Aggregates, NodeCost Cost)
        : PhysicalPlan;

    public sealed record Sort(PhysicalPlan Input, IReadOnlyList<SortKey> Keys, SortMode Mode, NodeCost Cost)
        : PhysicalPlan;

    public sealed record Limit(PhysicalPlan Input, ulong LimitValue, ulong Offset, NodeCost Cost)
        : PhysicalPlan;

    public sealed record Values(IReadOnlyList<IReadOnlyList<Expr>> Rows, NodeCost Cost)
        : PhysicalPlan;
}
```

---

## NodeCost

```csharp
public sealed record NodeCost(
    double EstimatedRows,
    double TotalCost,
    double IoCost,
    double CpuCost
);
```

---

## StatisticsRegistry

```csharp
var registry = new StatisticsRegistry();

registry.Register("users", new TableStats
{
    RowCount  = 1_000_000,
    PageCount = 12_500,
    Columns   = new Dictionary<string, ColumnStats>
    {
        ["age"] = new ColumnStats { Selectivity = 0.05 },
    },
});

var context = new PlannerContext(catalog) { Statistics = registry };
```

---

## PlannerError Reference

| Error | Meaning |
|---|---|
| `PlannerError.UnknownTable` | Table not found in catalog |
| `PlannerError.UnknownColumn` | Column not found in table |
| `PlannerError.CrossDatabaseJoin` | Query joins tables from different databases |
| `PlannerError.FlatTableJoin` | FLAT table used in a join |
| `PlannerError.InvalidExpression` | Expression is semantically invalid |
| `PlannerError.UnsupportedStatement` | Statement type not yet supported by the planner |
