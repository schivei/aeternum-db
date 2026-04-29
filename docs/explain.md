# EXPLAIN in AeternumDB

The `EXPLAIN` facility in AeternumDB renders the physical (or logical) query
plan chosen by the planner as a human-readable tree, annotated with per-operator
cost and row-count estimates.

---

## Usage

### Via the `QueryPlanner` API

```rust
use aeternumdb_core::query::{PlannerContext, QueryPlanner};
use aeternumdb_core::sql::ast::DataType;
use aeternumdb_core::sql::parser::SqlParser;
use aeternumdb_core::sql::validator::{Catalog, ColumnSchema, TableSchema};

// Build a minimal catalog for the query.
let mut catalog = Catalog::new();
catalog.add_table(TableSchema {
    name: "users".into(),
    columns: vec![
        ColumnSchema { name: "id".into(),  data_type: DataType::Integer, nullable: false },
        ColumnSchema { name: "age".into(), data_type: DataType::Integer, nullable: true  },
    ],
});

let planner = QueryPlanner::new();
let parser  = SqlParser::new();
let stmt    = parser.parse_one("SELECT id FROM users WHERE age > 18").unwrap();
let ctx     = PlannerContext::new(&catalog);

let physical = planner.plan(&stmt, &ctx).unwrap();
println!("{}", planner.explain(&physical));
```

### Explain functions

| Function | Description |
|----------|-------------|
| `explain_physical(plan)` | Formats a `PhysicalPlan` tree with cost annotations |
| `explain_logical(plan)` | Formats a `LogicalPlan` tree without cost annotations |

Both functions are exported from `aeternumdb_core::query::explain`.

---

## Output Format

### Physical plan

```
Physical Plan:
└─ Sort [InMemory]
   Est. rows: 1000 | Cost: 1157.38 (I/O: 0.00, CPU: 157.38)
   └─ Filter [predicate: <expr>]
      Est. rows: 100 | Cost: 1000.20 (I/O: 0.00, CPU: 0.20)
      └─ SeqScan [table: users]
         Est. rows: 1000 | Cost: 10.00 (I/O: 10.00, CPU: 0.00)

Total Cost: 1157.38
Estimated Rows: 1000
```

### Logical plan

```
Logical Plan:
└─ Sort
   └─ Filter [age]
      └─ Scan [users]
```

---

## Output Fields

### Per-node fields (physical plan)

| Field | Description |
|-------|-------------|
| `Est. rows` | Estimated number of rows produced by this operator |
| `Cost` | Total cost of this node and all its children |
| `I/O` | I/O component of the cost |
| `CPU` | CPU component of the cost |

### Footer fields (physical plan)

| Field | Description |
|-------|-------------|
| `Total Cost` | Cost of the root node (same as the root's `Cost`) |
| `Estimated Rows` | Estimated output cardinality of the root node |

---

## Node Descriptions

| Node label | Physical operator | What it does |
|------------|-------------------|--------------|
| `SeqScan [table: t]` | `PhysicalPlan::SeqScan` | Reads all pages of table `t` sequentially |
| `IndexScan [table: t, index: col_idx]` | `PhysicalPlan::IndexScan` | Uses the index `col_idx` to look up matching rows |
| `Filter [predicate: col]` | `PhysicalPlan::Filter` | Applies a row predicate; `col` is the predicate column |
| `Project [col1, col2]` | `PhysicalPlan::Project` | Evaluates expressions and renames output columns |
| `NestedLoopJoin [type: Inner]` | `PhysicalPlan::NestedLoopJoin` | Nested-loop join (small inputs) |
| `HashJoin [type: Inner]` | `PhysicalPlan::HashJoin` | Hash join (large inputs) |
| `HashAggregate [group_by: [g], aggregates: [a]]` | `PhysicalPlan::HashAggregate` | Groups rows and applies aggregate functions |
| `Sort [InMemory]` | `PhysicalPlan::Sort` | In-memory sort (≤ 100 000 rows) |
| `Sort [External]` | `PhysicalPlan::Sort` | External merge sort (> 100 000 rows) |
| `Limit [limit: n, offset: o]` | `PhysicalPlan::Limit` | Returns at most `n` rows, skipping the first `o` |
| `Unnest [alias: a]` | `PhysicalPlan::Unnest` | Explodes an array/vector column into individual rows |
| `ViewAs [col1, col2]` | `PhysicalPlan::ViewAs` | Post-result rename / transform (`VIEW AS` clause) |
| `Values [n row(s)]` | `PhysicalPlan::Values` | Inline constant rows (e.g. `SELECT 1`) |

---

## Tree Notation

```
└─   last child of its parent
├─   non-last child of its parent
│    vertical connector for non-last children
```

---

## Interpreting Costs

- **Higher cost = slower execution** (costs are dimensionless).
- Costs accumulate from leaves to root: the root's `Cost` includes all children.
- `I/O` captures page-read cost; `CPU` captures per-row processing cost.
- The cost model uses default factors until the catalog provides real statistics.
  Providing statistics via `PlannerContext.statistics` improves estimate quality.

---

## Limitations

- Cost estimates are heuristic.  They improve as real table statistics are
  registered with `StatisticsRegistry`.
- Index hints and forced plan shapes are not yet supported.
- EXPLAIN ANALYZE (with actual runtime statistics) is deferred to PR 1.5.
