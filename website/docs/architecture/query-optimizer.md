---
sidebar_position: 3
---

# ⚡ Query Optimizer Architecture

[![Cost-Based](https://img.shields.io/badge/Type-Cost--Based-green)](https://github.com/schivei/aeternum-db)
[![Rule-Driven](https://img.shields.io/badge/Rules-Pluggable-blue)](https://github.com/schivei/aeternum-db)
[![Physical Planning](https://img.shields.io/badge/Output-PhysicalPlan-orange)](https://github.com/schivei/aeternum-db)

A deep dive into the internal architecture of AeternumDB's query optimizer — from SQL string to physical execution plan.

---

## Optimizer Layers

```
SQL string
    │
    ▼ ① SqlParser — tokenize + parse → Statement AST
    │
    ▼ ② LogicalPlanBuilder — validate + build LogicalPlan
    │
    ▼ ③ Optimizer — rule-based rewrites (convergence loop)
    │      ├─ ConstantFolding
    │      ├─ PredicatePushdown
    │      ├─ ProjectionPushdown
    │      └─ JoinReordering (post-pass)
    │
    ▼ ④ PhysicalPlanner — select physical operators
    │
    ▼ ⑤ Executor — execute physical plan → result rows
```

---

## ① SQL Parser

The `SqlParser` converts a SQL string into a strongly-typed `Statement` AST.

```csharp
var parser = new SqlParser();
Statement stmt = parser.ParseOne("SELECT id, name FROM users WHERE age > 18");
```

Supported statement types: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, all DDL, `BEGIN`/`COMMIT`/`ROLLBACK`.

---

## ② LogicalPlanBuilder

The `LogicalPlanBuilder` transforms a `Statement` into a `LogicalPlan` tree, validating against the catalog.

### Validation Checks

| Check | Error |
|---|---|
| Table exists in catalog | `PlannerError.UnknownTable` |
| Column exists in table | `PlannerError.UnknownColumn` |
| Cross-database access | `PlannerError.CrossDatabaseJoin` |
| FLAT table in join | `PlannerError.FlatTableJoin` |
| OFFSET without LIMIT | Allowed — emits `Limit { limit: ulong.MaxValue, offset }` |

### Logical Plan Encoding

| SQL Construct | LogicalPlan Node |
|---|---|
| `SELECT *` | `Expr.Wildcard` in projection |
| `SELECT t.*` | `Expr.Column { table: "t", name: "*" }` |
| `SELECT 1` (no FROM) | `LogicalPlan.Values` |
| `OFFSET 5` (no LIMIT) | `Limit { limit: ulong.MaxValue, offset: 5 }` |

---

## ③ Optimizer — Rule Engine

The `Optimizer` applies rules in a **convergence loop** (up to 20 passes) until the plan stops changing.

```csharp
public class Optimizer
{
    private readonly List<IOptimizationRule> _rules = new()
    {
        new ConstantFolding(),
        new PredicatePushdown(),
        new ProjectionPushdown(),
    };

    public LogicalPlan Optimize(LogicalPlan plan)
    {
        // convergence loop
        for (int pass = 0; pass < 20; pass++)
        {
            var next = _rules.Aggregate(plan, (p, r) => r.Apply(p));
            if (next == plan) break;
            plan = next;
        }
        // post-pass: join reordering
        return JoinReorderer.Reorder(plan);
    }
}
```

### Rule Interface

```csharp
public interface IOptimizationRule
{
    string Name { get; }
    LogicalPlan Apply(LogicalPlan plan);
}
```

---

## ④ PhysicalPlanner

The `PhysicalPlanner` converts the optimized `LogicalPlan` into a `PhysicalPlan` by selecting concrete operators based on statistics and cost estimates.

### Operator Selection Matrix

| Logical Node | Physical Choice | Condition |
|---|---|---|
| `Scan` (no index pred) | `SeqScan` | Default |
| `Scan` (equality/range pred on column) | `IndexScan` | Index available |
| `Join (Inner)` (small inputs) | `NestedLoopJoin` | est. rows ≤ 100 on either side |
| `Join (Inner)` (large inputs + equi-keys) | `HashJoin` | est. rows > 100 + extractable keys |
| `Join (Outer)` | `NestedLoopJoin` | Always (preserves NULL semantics) |
| `Sort` (small) | `Sort (InMemory)` | est. rows ≤ 100 000 |
| `Sort` (large) | `Sort (External)` | est. rows > 100 000 |
| `Aggregate` | `HashAggregate` | Always |

### NodeCost Structure

Every physical node carries a `NodeCost` annotation:

```csharp
record NodeCost(
    double EstimatedRows,
    double TotalCost,
    double IoCost,
    double CpuCost
);
```

---

## ⑤ Cost Model

```csharp
// Computed by CostModel
static NodeCost EstimateSeqScan(TableStats stats, CostModel cost)
    => new(
        EstimatedRows: stats.RowCount,
        TotalCost:     stats.PageCount * cost.IoCostFactor
                     + stats.RowCount  * cost.CpuCostFactor,
        IoCost:        stats.PageCount * cost.IoCostFactor,
        CpuCost:       stats.RowCount  * cost.CpuCostFactor
    );
```

### Filter Selectivity

When a `SeqScan` has a pushed-down filter, the optimizer applies **10% default selectivity** (configurable per column via histogram statistics):

```csharp
// 10% selectivity applied to both estimated rows and CPU cost
double filteredRows = stats.RowCount * selectivity;
```

---

## Statistics Registry

```csharp
var registry = new StatisticsRegistry();

// Register table-level statistics
registry.Register("users", new TableStats
{
    RowCount  = 1_000_000,
    PageCount = 12_500,
    Columns   = new Dictionary<string, ColumnStats>
    {
        ["age"] = new ColumnStats
        {
            Selectivity = 0.05,                         // 5% of rows match typical age filter
            Histogram   = new EqualWidthHistogram(      // range predicate selectivity
                min: 18, max: 80, buckets: 12),
        },
    },
});

var context = new PlannerContext(catalog) { Statistics = registry };
```

---

## Explain Integration

The optimizer is fully integrated with `EXPLAIN`:

```csharp
var planner  = new QueryPlanner();
var physical = planner.Plan(stmt, context);

// Human-readable plan tree with costs
Console.WriteLine(planner.Explain(physical));
```

See [EXPLAIN Reference](../guides/explain.md) for the full output format.

---

## Extension Points

### Custom Statistics Provider

```csharp
public class LiveStatisticsProvider : IStatisticsProvider
{
    public TableStats? GetStats(string tableName) =>
        _catalog.GetLiveStats(tableName);
}

var context = new PlannerContext(catalog)
{
    Statistics = new StatisticsRegistry(new LiveStatisticsProvider())
};
```

### Custom Optimization Rule

```csharp
public class MaterializedViewRewriter : IOptimizationRule
{
    public string Name => "mv_rewriter";

    public LogicalPlan Apply(LogicalPlan plan)
    {
        // detect patterns that can be served from a materialized view
        return plan;
    }
}

optimizer.AddRule(new MaterializedViewRewriter());
```

---

## Roadmap

| Feature | Phase |
|---|---|
| Adaptive statistics (live table stats) | Phase 2 |
| Partition pruning | Phase 3 |
| Materialized-view rewriting | Phase 4 |
| Sub-query flattening (correlated → join) | Phase 4 |
| `EXPLAIN ANALYZE` with runtime stats | Phase 2 |
| Parallel query execution | Phase 5 |
