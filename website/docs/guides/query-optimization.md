---
sidebar_position: 2
---

# 🧠 Query Optimization

[![Cost-Based](https://img.shields.io/badge/Optimizer-Cost--Based-green)](https://github.com/schivei/aeternum-db)
[![Rules](https://img.shields.io/badge/Rules-3%20built--in-blue)](https://github.com/schivei/aeternum-db)

AeternumDB uses a **cost-based, rule-driven query optimizer** that transforms SQL ASTs through multiple stages to produce efficient physical execution plans.

---

## 🔄 Optimization Pipeline

```
SQL string
    │
    ▼  SqlParser
Statement (AST)
    │
    ▼  LogicalPlanBuilder
LogicalPlan ─── validation errors
    │
    ▼  Optimizer
       1. ConstantFolding
       2. PredicatePushdown
       3. ProjectionPushdown
       ↺ repeat until convergence (max 20 passes)
       4. Join Reordering (post-pass, cardinality-based)
    │
    ▼  PhysicalPlanner
PhysicalPlan (annotated with NodeCost)
    │
    ▼  Executor
Result rows
```

---

## 📐 Logical Plan Nodes

| Node | SQL Construct | Notes |
|---|---|---|
| `Scan` | `FROM table` | Carries an optional pushed-down filter |
| `Filter` | `WHERE` / `HAVING` | Predicate expression tree |
| `Project` | `SELECT col, expr AS alias` | Expression evaluation + rename |
| `Join` | `JOIN … ON` | Cross-database joins rejected |
| `Aggregate` | `GROUP BY … HAVING` | Presence of aggregate functions in `SELECT` triggers this node even without `GROUP BY` |
| `Sort` | `ORDER BY` | |
| `Limit` | `LIMIT / OFFSET` | Placed above `Sort` |
| `Unnest` | `EXPAND(ref_col)` | Explodes vector/array reference columns into rows |
| `ViewAs` | `VIEW AS (…)` | Post-result rename/transform; always at the plan root |
| `Values` | `SELECT` with no `FROM` | Inline constant rows |

---

## ⚙️ Optimization Rules

### 1. Constant Folding

Evaluates sub-expressions that are entirely constant at plan time, reducing runtime CPU work.

| Pattern | Result |
|---|---|
| `n + m` | `n+m` (evaluated at plan time) |
| `TRUE AND x` | `x` |
| `FALSE AND x` | `FALSE` |
| `TRUE OR x` | `TRUE` |
| `NOT TRUE` | `FALSE` |

### 2. Predicate Pushdown ⭐

Moves `Filter` nodes as close as possible to their source `Scan` nodes. This reduces the number of rows processed by expensive operators.

**Example:**

```sql
SELECT * FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.age > 18
```

Before:
```
Filter [u.age > 18]
  Join
    Scan(users AS u)
    Scan(orders AS o)
```

After:
```
Join
  Scan(users AS u) [filter: age > 18]  ← pushed down!
  Scan(orders AS o)
```

**Rules:**
- `Filter` directly above `Scan` → merged into the scan's filter field
- `Filter` above `Join` → pushed into the appropriate join child if predicate references only one side
- INNER join predicates referencing both sides → promoted to `Join.condition` for hash join equi-key extraction
- Outer joins: predicates are left above the join to preserve NULL-extension semantics

### 3. Projection Pushdown

Annotates `Scan` nodes with the **minimal column list** required by upstream operators. When the storage layer supports column projection, this avoids reading unused columns from disk.

### 4. Join Reordering

After rule convergence, a single-pass join-reorder places the **smaller table** on the left (build) side of each inner join. This reduces hash-join build cost and improves nested-loop performance.

:::note
Only flat chains of `INNER` joins are reordered. Outer joins and complex join trees are left unchanged to preserve semantics.
:::

---

## 💰 Cost Model

The cost model assigns a dimensionless cost to each physical operator. Higher cost = slower execution.

### Cost Components

| Component | Default Factor | When Charged |
|---|---|---|
| I/O | `1.0` | Per storage page read |
| CPU | `0.01` | Per row processed |
| Network | `10.0` | Reserved for future distributed plans |

### Operator Cost Functions

| Operator | Formula |
|---|---|
| Sequential scan | `pages × io_factor + rows × cpu_factor` |
| Index scan | ~10 % of sequential scan cost |
| Filter | `rows × cpu_factor × (1 + selectivity)` |
| Nested-loop join | `left_rows × right_rows × cpu_factor` |
| Hash join | `left_rows × 1.5 × cpu_factor + right_rows × cpu_factor` |
| Sort | `n × log₂(n) × cpu_factor` |
| Hash aggregate | `rows × cpu_factor + groups × 2 × cpu_factor` |

---

## 📊 Statistics

The optimizer reads row and page estimates from a `StatisticsRegistry`. Better statistics → better plans.

| Statistic | Default (without registration) |
|---|---|
| Row count | 1 000 |
| Page count | 10 |
| Column selectivity | 10 % |

```csharp
// Register statistics for accurate planning
var stats = new StatisticsRegistry();
stats.Register("users",  new TableStats { RowCount = 1_000_000, PageCount = 12_500 });
stats.Register("orders", new TableStats { RowCount = 5_000_000, PageCount = 62_500 });

var context = new PlannerContext(catalog) { Statistics = stats };
```

Histograms are supported for range-predicate selectivity estimation (equal-width bucket format).

---

## ⚡ Physical Plan Selection

| Physical Operator | Selected When |
|---|---|
| `SeqScan` | No usable index predicate |
| `IndexScan` | Equality or range predicate on a simple column reference |
| `NestedLoopJoin` | Both inputs estimated ≤ 100 rows, OR no equi-join keys |
| `HashJoin` | At least one input > 100 rows AND equi-join keys available |
| `Sort (InMemory)` | Estimated rows ≤ 100 000 |
| `Sort (External)` | Estimated rows > 100 000 |
| `HashAggregate` | Always used for `GROUP BY` |

---

## 🔌 Custom Optimization Rules

Extend the optimizer with your own rules:

```csharp
using AeternumDB.Core.Query.Rules;

public class MyCustomRule : IOptimizationRule
{
    public string Name => "my_custom_rule";

    public LogicalPlan Apply(LogicalPlan plan) => plan; // transform here
}

// Register before optimizing
var optimizer = new Optimizer();
optimizer.AddRule(new MyCustomRule());
var optimized = optimizer.Optimize(plan);
```

---

## AeternumDB-Specific Restrictions

### FLAT Table Joins

Tables created with `CREATE FLAT TABLE` **cannot participate in joins**. Attempting to join a FLAT table returns `PlannerError.FlatTableJoin`.

### Cross-Database Joins

All table references in a single query must belong to the **same database**. Cross-database queries return `PlannerError.CrossDatabaseJoin`.

---

:::tip Use EXPLAIN
Run `EXPLAIN` on your queries to see the chosen physical plan, estimated rows, and costs. See [EXPLAIN Reference](./explain.md).
:::
