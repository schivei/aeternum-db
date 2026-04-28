# Query Optimization in AeternumDB

AeternumDB uses a **cost-based, rule-driven query optimizer** that transforms
SQL ASTs through a sequence of stages to produce efficient physical execution
plans.  This document describes the optimizer architecture, built-in rules, and
extension points.

---

## Pipeline Overview

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
       4. Join reordering (post-pass, cardinality-based)
    │
    ▼  PhysicalPlanner
PhysicalPlan (annotated with NodeCost)
    │
    ▼  Executor (PR 1.5)
Result rows
```

---

## Logical Plan Nodes

| Node | SQL construct | Notes |
|------|---------------|-------|
| `Scan` | `FROM table` | Carries an optional pushed-down filter |
| `Filter` | `WHERE` / `HAVING` | Predicate expression tree |
| `Project` | `SELECT col, expr AS alias` | Expression evaluation + rename |
| `Join` | `JOIN … ON` / reference navigation | Cross-database joins rejected |
| `Aggregate` | `GROUP BY … HAVING` | Presence of aggregate functions in SELECT triggers this node even without GROUP BY |
| `Sort` | `ORDER BY` | |
| `Limit` | `LIMIT / OFFSET` | Placed above Sort |
| `Unnest` | `EXPAND(ref_col)` | Explodes vector / array reference columns into rows |
| `ViewAs` | `VIEW AS (…)` | Post-result rename/transform; always at the plan root |
| `Values` | `SELECT` with no `FROM` | Inline constant rows |

---

## Optimization Rules

### 1. Constant Folding (`ConstantFolding`)

Evaluates sub-expressions that are entirely constant at plan time, reducing
runtime CPU work.

**Supported rewrites:**

| Pattern | Result |
|---------|--------|
| `n + m` | `n+m` (integer arithmetic) |
| `n - m` | `n-m` |
| `n * m` | `n*m` |
| `TRUE AND x` | `x` |
| `FALSE AND x` | `FALSE` |
| `TRUE OR x` | `TRUE` |
| `FALSE OR x` | `x` |
| `NOT TRUE` | `FALSE` |
| `NOT FALSE` | `TRUE` |

### 2. Predicate Pushdown (`PredicatePushdown`)

Moves `Filter` nodes as close as possible to their source `Scan` nodes.
This reduces the number of rows processed by expensive operators such as joins
and aggregations.

**Rules:**

- A `Filter` directly above a `Scan` is merged into the scan's `filter` field.
- If the scan already has a filter, the new predicate is combined with `AND`.
- A `Filter` above a `Join` is pushed into the appropriate join child when the
  predicate references only columns from that side.
- Predicates that span both sides of a join remain above the join node.

**Example:**

```sql
SELECT * FROM users u JOIN orders o ON u.id = o.user_id
WHERE u.age > 18
```

Before optimization:
```
Filter [u.age > 18]
  Join
    Scan(users AS u)
    Scan(orders AS o)
```

After optimization:
```
Join
  Scan(users AS u) [filter: age > 18]  ← predicate pushed into scan
  Scan(orders AS o)
```

### 3. Projection Pushdown (`ProjectionPushdown`)

Annotates `Scan` nodes with the minimal column list required by upstream
operators.  When the storage layer supports column projection this avoids
reading unused columns from disk.

**Current limitation:** Only a single `Project` directly above a `Scan` is
handled; more complex projection patterns across joins are left for future work.

### 4. Join Reordering

After rule convergence the optimizer applies a single-pass join-reorder pass
that places the smaller table (by estimated row count) on the left (build) side
of each inner join.  This reduces hash-join build cost and improves nested-loop
join performance.

Only flat chains of `Inner` joins are reordered; outer joins and complex join
trees are left unchanged to preserve semantics.

---

## Cost Model

The cost model assigns a dimensionless cost to each physical operator.  Higher
cost indicates slower execution.

### Cost Components

| Component | Factor | When charged |
|-----------|--------|--------------|
| I/O | `io_cost_factor` (default: 1.0) | Per storage page read |
| CPU | `cpu_cost_factor` (default: 0.01) | Per row processed |
| Network | `network_cost_factor` (default: 10.0) | Reserved (future distributed plans) |

### Operator Cost Functions

| Operator | Formula |
|----------|---------|
| Sequential scan | `pages × io_factor + rows × cpu_factor` |
| Index scan | 10 % of sequential scan cost (selectivity 5 %) |
| Filter | `rows × cpu_factor × (1 + selectivity)` |
| Nested-loop join | `left_rows × right_rows × cpu_factor` |
| Hash join | `left_rows × 1.5 × cpu_factor + right_rows × cpu_factor` |
| Sort | `n × log₂(n) × cpu_factor` |
| Hash aggregate | `rows × cpu_factor + groups × 2 × cpu_factor` |

---

## Statistics

The optimizer reads row and page estimates from a [`StatisticsRegistry`].
When no statistics are registered for a table, default values are used:

| Statistic | Default |
|-----------|---------|
| Row count | 1 000 |
| Page count | 10 |
| Column selectivity | 10 % |

Statistics are populated by the DDL executor or injected directly in tests.
Histograms are supported for range-predicate selectivity estimation
(equal-width bucket format).

---

## Physical Plan Nodes

| Physical node | Selected when |
|---------------|---------------|
| `SeqScan` | No usable index predicate |
| `IndexScan` | Equality or range predicate on a simple column reference |
| `NestedLoopJoin` | Both inputs estimated ≤ 100 rows |
| `HashJoin` | At least one input estimated > 100 rows |
| `Sort(InMemory)` | Estimated rows ≤ 100 000 |
| `Sort(External)` | Estimated rows > 100 000 |
| `HashAggregate` | Always used for `GROUP BY` |

---

## AeternumDB-Specific Semantics

### FLAT Table Restrictions

Tables created with `FLAT` cannot participate in joins.  Attempting to plan a
join involving a FLAT table returns `PlannerError::FlatTableJoin`.  FLAT tables
may still be accessed via sequential scans.

### Cross-Database Join Rejection

All table references in a single query must belong to the same database.
A query referencing tables from different databases returns
`PlannerError::CrossDatabaseJoin`.

### EXPAND(ref_col) and UNNEST

`EXPAND(col)` in the SELECT list is resolved by the planner into an `Unnest`
node followed by a projection.  When the referenced column is a vector/array
reference the unnest step emits one output row per referenced element.

Full resolution of the expanded column list against the catalog is deferred to
PR 1.5 when the reference-traversal executor is available.  The planner
currently emits the `Unnest` marker node for the executor to act on.

### VIEW AS

The `VIEW AS (expr AS alias, …)` clause is lowered into a `ViewAs` plan node
placed at the root of the plan tree.  It is evaluated last, after all
filtering, grouping, ordering, and limiting.  Only primitive expressions are
allowed (no aggregates, no subqueries) — this is enforced by the validator.

---

## Extension Points

The optimizer's rule pipeline is open for extension:

```rust
use aeternumdb_core::query::rules::OptimizationRule;
use aeternumdb_core::query::logical_plan::LogicalPlan;

struct MyRule;
impl OptimizationRule for MyRule {
    fn name(&self) -> &str { "my_rule" }
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan { plan }
}
```

Custom rules can be added to the `RuleEngine` before calling `optimize()`.

---

## Deferred / Future Work

- **Full column-list resolution for EXPAND**: Requires executor-level reference
  traversal (PR 1.5).
- **Adaptive statistics**: Live statistics from the storage engine instead of
  static DDL-time values (Phase 2).
- **Partition pruning**: When table partitioning is introduced the planner can
  eliminate partitions at plan time.
- **Materialized-view rewriting**: Detecting that a query can be served from a
  materialized view.
- **Sub-query flattening**: Converting correlated sub-queries into joins.
