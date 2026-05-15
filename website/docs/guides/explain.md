---
sidebar_position: 3
---

# 🔍 EXPLAIN

Understand exactly how AeternumDB will execute your query — before it runs.

---

## Overview

`EXPLAIN` renders the **physical query plan** chosen by the optimizer as a human-readable tree, annotated with per-operator cost and row-count estimates.

It's your primary tool for:
- 🐛 **Debugging** slow queries
- 📊 **Validating** that the optimizer picks the right plan
- ⚡ **Tuning** statistics and indexes for optimal performance

---

## Usage

```csharp
using AeternumDB.Core.Query;
using AeternumDB.Core.Sql;

var parser  = new SqlParser();
var planner = new QueryPlanner();

var stmt     = parser.ParseOne("SELECT id FROM users WHERE age > 18");
var context  = new PlannerContext(catalog);
var physical = planner.Plan(stmt, context);

// Print physical plan with cost annotations
Console.WriteLine(planner.Explain(physical));

// Or use the standalone functions
string physTree = ExplainFormatter.ExplainPhysical(physical);
string logTree  = ExplainFormatter.ExplainLogical(logical);
```

### Available Functions

| Function | Description |
|---|---|
| `ExplainFormatter.ExplainPhysical(plan)` | Physical plan tree with cost annotations |
| `ExplainFormatter.ExplainLogical(plan)` | Logical plan tree without cost annotations |
| `QueryPlanner.Explain(plan)` | Convenience wrapper for physical plan |

---

## Output Format

### Physical Plan

```
Physical Plan:
└─ Sort [InMemory]
   Est. rows: 1000 | Cost: 1157.38 (I/O: 0.00, CPU: 157.38)
   └─ Filter [predicate: age]
      Est. rows: 100 | Cost: 1000.20 (I/O: 0.00, CPU: 0.20)
      └─ SeqScan [table: users]
         Est. rows: 1000 | Cost: 10.00 (I/O: 10.00, CPU: 0.00)

Total Cost: 1157.38
Estimated Rows: 1000
```

### Logical Plan

```
Logical Plan:
└─ Sort
   └─ Filter [age]
      └─ Scan [users]
```

---

## Output Fields

### Per-Node Fields (Physical Plan)

| Field | Description |
|---|---|
| `Est. rows` | Estimated number of rows produced by this operator |
| `Cost` | Cumulative cost of this node and all its children |
| `I/O` | I/O component of the cost (page reads) |
| `CPU` | CPU component of the cost (per-row processing) |

### Footer Fields

| Field | Description |
|---|---|
| `Total Cost` | Cost of the root node |
| `Estimated Rows` | Estimated output cardinality of the root node |

---

## Node Descriptions

| Node Label | Operator | Description |
|---|---|---|
| `SeqScan [table: t]` | `SeqScan` | Reads all pages of table `t` sequentially |
| `IndexScan [table: t, index: col_idx]` | `IndexScan` | Uses index `col_idx` to find matching rows |
| `Filter [predicate: col]` | `Filter` | Applies a row predicate |
| `Project [col1, col2]` | `Project` | Evaluates and renames output columns |
| `NestedLoopJoin [type: Inner]` | `NestedLoopJoin` | Nested-loop join (small inputs) |
| `HashJoin [type: Inner]` | `HashJoin` | Hash join (large inputs) |
| `HashAggregate [group_by: …]` | `HashAggregate` | Groups rows and applies aggregate functions |
| `Sort [InMemory]` | `Sort` | In-memory sort (≤ 100 000 rows) |
| `Sort [External]` | `Sort` | External merge sort (> 100 000 rows) |
| `Limit [limit: n, offset: o]` | `Limit` | Returns at most `n` rows, skipping `o` |
| `Unnest [alias: a]` | `Unnest` | Explodes an array/vector column into rows |
| `ViewAs [col1, col2]` | `ViewAs` | Post-result rename (`VIEW AS` clause) |
| `Values [n row(s)]` | `Values` | Inline constant rows (e.g. `SELECT 1`) |

---

## Tree Notation

```
└─   last child of its parent
├─   non-last child (more siblings follow)
│    vertical connector for non-last children
```

---

## Interpreting Costs

:::tip Cost intuition
- **Higher cost = slower execution** (dimensionless units)
- Costs **accumulate** from leaves to root — root `Cost` includes all children
- `I/O` captures page-read cost; `CPU` captures per-row processing cost
- Default cost model uses heuristics — provide real statistics via `StatisticsRegistry` for accurate estimates
:::

### Example: Index vs Sequential Scan

Without an index, the planner may choose a sequential scan:

```
SeqScan [table: orders]
Est. rows: 1000000 | Cost: 10010.00 (I/O: 10000.00, CPU: 10.00)
```

After creating an index on the filter column:

```
IndexScan [table: orders, index: status_idx]
Est. rows: 50000 | Cost: 501.00 (I/O: 500.00, CPU: 1.00)
```

The `IndexScan` is ~20× cheaper. 🚀

---

## Limitations

| Limitation | Status |
|---|---|
| Index hints / forced plans | 🔄 Planned |
| `EXPLAIN ANALYZE` (with actual runtime stats) | 🔄 Planned |
| Cost estimates without statistics | ⚠️ Heuristic only |

---

:::info
Providing accurate statistics via `StatisticsRegistry.Register(...)` dramatically improves both cost estimates and plan quality. See [Configuration](../getting-started/configuration.md).
:::
