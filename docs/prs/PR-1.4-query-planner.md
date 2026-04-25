# PR 1.4: Query Planner & Optimizer

## 📋 Overview

**PR Number:** 1.4
**Phase:** 1 - Core Foundation
**Priority:** 🟡 High
**Estimated Effort:** 7 days
**Dependencies:** PR 1.3 (SQL Parser)

## 🎯 Objectives

Implement a cost-based query planner and optimizer that transforms SQL ASTs into efficient physical execution plans. This includes:

- Logical plan generation from SQL AST
- Cost-based optimization with statistics
- Physical plan generation with operator selection
- Index selection and join order optimization
- Plan visualization and explanation
- Foundation for query execution

## 📝 Detailed Prompt for Implementation

```
Implement a complete query planner and optimizer for AeternumDB with the following requirements:

1. **Logical Plan Generation**
   - Convert SQL AST to logical plan tree
   - Logical operators: Scan, Filter, Project, Join, Aggregate, Sort, Limit
   - Plan validation and type checking
   - Subquery handling

2. **Cost Model**
   - Statistics collection (table size, column cardinality, histograms)
   - Cost estimation for each operator
   - I/O cost vs CPU cost modeling
   - Selectivity estimation for predicates

3. **Optimizer Rules**
   - Predicate pushdown (push filters close to scans)
   - Projection pushdown (minimize columns early)
   - Join reordering (based on cardinality)
   - Index selection (choose best index for predicates)
   - Constant folding and expression simplification

4. **Physical Plan Generation**
   - Choose scan method (sequential vs index scan)
   - Choose join algorithm (nested loop, hash join, merge join)
   - Choose sort algorithm (in-memory vs external)
   - Add exchange operators for distribution (future)

5. **Plan Explanation**
   - EXPLAIN command support
   - Cost breakdown per operator
   - Statistics used
   - Human-readable format

6. **Performance Requirements**
   - Plan generation: <100ms for simple queries, <1s for complex queries
   - Optimization should improve query execution time by 2-10x
   - Support queries with up to 10 joins

Use Rust best practices, comprehensive error handling, and include detailed documentation.
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/query/mod.rs`**
   - Public API for query planning
   - QueryPlanner struct and implementation
   - Plan execution context

2. **`core/src/query/logical_plan.rs`**
   - LogicalPlan enum and node types
   - Plan builders and transformations
   - Plan validation

3. **`core/src/query/physical_plan.rs`**
   - PhysicalPlan enum and node types
   - Operator implementations
   - Plan serialization

4. **`core/src/query/optimizer.rs`**
   - Optimizer with rule-based transformations
   - Cost-based optimization
   - Join ordering algorithm

5. **`core/src/query/cost_model.rs`**
   - Cost estimation functions
   - Statistics structures
   - Selectivity estimation

6. **`core/src/query/statistics.rs`**
   - Table and column statistics
   - Histogram implementation
   - Statistics collection

7. **`core/src/query/rules/mod.rs`**
   - Optimization rule trait
   - Rule application engine

8. **`core/src/query/rules/pushdown.rs`**
   - Predicate pushdown rules
   - Projection pushdown rules

9. **`core/src/query/explain.rs`**
   - EXPLAIN command implementation
   - Plan formatting and visualization

### Test Files

10. **`core/tests/query_planner_tests.rs`**
    - Integration tests for query planning

11. **`core/benches/query_bench.rs`**
    - Planning performance benchmarks

## 🔧 Implementation Details

### Logical Plan Structure

```rust
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        columns: Vec<String>,
        filter: Option<Expr>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<Expr>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: Expr,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<SortExpr>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
        offset: usize,
    },
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}
```

### Physical Plan Structure

```rust
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan {
        table: String,
        columns: Vec<String>,
        filter: Option<Expr>,
    },
    IndexScan {
        table: String,
        index: String,
        columns: Vec<String>,
        key_range: Range<Value>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<Expr>,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        condition: Expr,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
    },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<SortExpr>,
        algorithm: SortAlgorithm,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: usize,
        offset: usize,
    },
}

#[derive(Debug, Clone)]
pub enum SortAlgorithm {
    InMemory,
    External,
}
```

### Cost Model

```rust
pub struct CostModel {
    cpu_cost_factor: f64,
    io_cost_factor: f64,
    network_cost_factor: f64,
}

impl CostModel {
    pub fn estimate_scan_cost(&self, stats: &TableStats) -> f64 {
        let io_cost = stats.num_pages as f64 * self.io_cost_factor;
        let cpu_cost = stats.num_rows as f64 * self.cpu_cost_factor;
        io_cost + cpu_cost
    }

    pub fn estimate_join_cost(
        &self,
        left_rows: usize,
        right_rows: usize,
        join_type: JoinType,
    ) -> f64 {
        match join_type {
            JoinType::NestedLoop => {
                // O(n * m)
                (left_rows * right_rows) as f64 * self.cpu_cost_factor
            }
            JoinType::Hash => {
                // O(n + m) with hash build cost
                (left_rows + right_rows) as f64 * self.cpu_cost_factor * 1.5
            }
            JoinType::MergeJoin => {
                // O(n + m) with sort cost
                let sort_cost = self.estimate_sort_cost(left_rows + right_rows);
                (left_rows + right_rows) as f64 * self.cpu_cost_factor + sort_cost
            }
        }
    }

    pub fn estimate_sort_cost(&self, num_rows: usize) -> f64 {
        // O(n log n)
        (num_rows as f64 * (num_rows as f64).log2()) * self.cpu_cost_factor
    }
}
```

### Statistics Structure

```rust
pub struct TableStats {
    pub table_name: String,
    pub num_rows: usize,
    pub num_pages: usize,
    pub avg_row_size: usize,
    pub column_stats: HashMap<String, ColumnStats>,
}

pub struct ColumnStats {
    pub column_name: String,
    pub num_distinct: usize,
    pub num_nulls: usize,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub histogram: Option<Histogram>,
}

pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
}

pub struct HistogramBucket {
    pub lower_bound: Value,
    pub upper_bound: Value,
    pub count: usize,
}
```

### API Examples

```rust
use aeternumdb::query::{QueryPlanner, PlannerContext};
use aeternumdb::sql::Parser;

// Parse SQL
let sql = "SELECT u.name, COUNT(*) FROM users u
           JOIN orders o ON u.id = o.user_id
           WHERE u.age > 18
           GROUP BY u.name";
let ast = Parser::parse(sql)?;

// Create planner with statistics
let mut planner = QueryPlanner::new();
let ctx = PlannerContext {
    catalog: &catalog,
    statistics: &statistics,
};

// Generate logical plan
let logical_plan = planner.create_logical_plan(&ast, &ctx)?;

// Optimize logical plan
let optimized_plan = planner.optimize(logical_plan, &ctx)?;

// Generate physical plan
let physical_plan = planner.create_physical_plan(optimized_plan, &ctx)?;

// Explain plan
let explanation = planner.explain(&physical_plan)?;
println!("{}", explanation);
```

### EXPLAIN Output Format

```
Physical Plan:
└─ HashAggregate [group_by: [u.name], aggregates: [COUNT(*)]]
   └─ HashJoin [type: Inner, condition: u.id = o.user_id]
      ├─ IndexScan [table: users, index: age_idx, filter: age > 18]
      │  Cost: 150.00 (I/O: 100.00, CPU: 50.00)
      │  Rows: 5000
      └─ SeqScan [table: orders]
         Cost: 500.00 (I/O: 400.00, CPU: 100.00)
         Rows: 10000

Total Cost: 1250.00
Estimated Rows: 8000
```

## ✅ Tests Required

### Unit Tests

1. **Logical Plan Tests** (`logical_plan.rs`)
   - ✅ Create logical plan from simple SELECT
   - ✅ Create logical plan with WHERE clause
   - ✅ Create logical plan with JOIN
   - ✅ Create logical plan with GROUP BY
   - ✅ Create logical plan with subquery
   - ✅ Plan validation catches errors

2. **Physical Plan Tests** (`physical_plan.rs`)
   - ✅ Convert logical scan to physical scan
   - ✅ Choose index scan over seq scan when appropriate
   - ✅ Choose hash join for large tables
   - ✅ Choose nested loop join for small tables
   - ✅ Plan serialization and deserialization

3. **Optimizer Tests** (`optimizer.rs`)
   - ✅ Predicate pushdown optimization
   - ✅ Projection pushdown optimization
   - ✅ Join reordering based on cardinality
   - ✅ Constant folding
   - ✅ Index selection

4. **Cost Model Tests** (`cost_model.rs`)
   - ✅ Scan cost estimation
   - ✅ Join cost estimation (all types)
   - ✅ Sort cost estimation
   - ✅ Aggregate cost estimation

### Integration Tests

5. **Query Planner Tests** (`query_planner_tests.rs`)
   - ✅ End-to-end planning for simple queries
   - ✅ End-to-end planning for complex queries
   - ✅ Plan comparison (verify optimizer improves cost)
   - ✅ Multi-way join optimization
   - ✅ Subquery optimization

### Performance Benchmarks

6. **Benchmarks** (`query_bench.rs`)
   - ✅ Planning time for simple queries (<10ms)
   - ✅ Planning time for complex queries (<100ms)
   - ✅ Optimization overhead
   - ✅ Plan generation throughput

## 📊 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Simple query planning | <10ms | Benchmark |
| Complex query planning | <100ms | Benchmark |
| 10-way join planning | <1s | Benchmark |
| Optimization improvement | 2-10x | Compare with/without optimization |
| Memory overhead | <10MB per plan | Memory profiler |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Comprehensive rustdoc comments for all public APIs
   - Example usage in doc comments
   - Explanation of optimization rules

2. **Query Optimization Guide** (`docs/query-optimization.md`)
   - How the optimizer works
   - Cost model explanation
   - Statistics collection strategy
   - Tuning guidelines

3. **EXPLAIN Guide** (`docs/explain.md`)
   - How to read EXPLAIN output
   - Understanding costs
   - Optimization tips

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] Logical plans generated from SQL AST
- [ ] Physical plans generated from logical plans
- [ ] Optimizer applies all rules correctly
- [ ] Index selection works
- [ ] Join order optimization works
- [ ] EXPLAIN command shows detailed plan
- [ ] Cost estimates are reasonable

### Quality Requirements
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Code coverage >85%
- [ ] No clippy warnings
- [ ] Code formatted with `cargo fmt`
- [ ] Documentation complete and accurate

### Performance Requirements
- [ ] Planning meets timing targets
- [ ] Optimizer improves query execution
- [ ] Memory usage is reasonable
- [ ] No performance regressions

### Documentation Requirements
- [ ] All public APIs documented
- [ ] Query optimization guide complete
- [ ] EXPLAIN guide complete
- [ ] Examples provided

## 🔗 Dependencies

This PR depends on:
- **PR 1.3**: SQL Parser (provides AST input)

This PR is required by:
- **PR 1.5**: Query Executor (consumes physical plans)

## 📦 Dependencies to Add

```toml
[dependencies]
petgraph = "0.6"  # For plan graph representation
priority-queue = "1.3"  # For join ordering

[dev-dependencies]
insta = "1.34"  # Snapshot testing for plans
```

## 🚀 Implementation Steps

### Day 1: Logical Plan Foundation
- Define LogicalPlan enum and all node types
- Implement plan builder from SQL AST
- Add plan validation
- Write unit tests

### Day 2: Physical Plan Generation
- Define PhysicalPlan enum and all node types
- Implement basic logical-to-physical conversion
- No optimization yet, just straightforward translation
- Write unit tests

### Day 3: Cost Model & Statistics
- Implement CostModel with estimation functions
- Define Statistics structures
- Implement basic statistics collection
- Write unit tests

### Day 4: Optimizer Rules (Part 1)
- Implement predicate pushdown rule
- Implement projection pushdown rule
- Implement constant folding
- Write unit tests

### Day 5: Optimizer Rules (Part 2)
- Implement index selection
- Implement join reordering algorithm
- Integrate rules into optimizer
- Write unit tests

### Day 6: EXPLAIN & Integration
- Implement EXPLAIN command
- Plan visualization and formatting
- End-to-end integration tests
- Performance testing

### Day 7: Optimization & Documentation
- Run benchmarks and optimize planning code
- Write query optimization guide
- Write EXPLAIN guide
- Code review and cleanup

## 🐛 Known Edge Cases to Handle

1. **Circular dependencies in subqueries**: Detect and reject
2. **Cartesian products**: Warn if no join condition detected
3. **Missing statistics**: Use default estimates gracefully
4. **Optimization time limits**: Bail out if planning takes too long
5. **Plan cache invalidation**: When statistics change significantly
6. **Type mismatches in joins**: Validate and add implicit casts
7. **Null handling in join conditions**: Proper semantics
8. **Empty tables**: Handle zero-row statistics

## 💡 Future Enhancements (Out of Scope)

- Adaptive query execution (re-optimize during execution) → Phase 3
- Materialized view support → Phase 3
- Query result caching → Phase 3
- Parallel query execution → Phase 3
- Cost model learning from actual execution → Phase 5
- Query hints and manual plan forcing → Phase 5

## 🏁 Definition of Done

This PR is complete when:
1. All code is implemented and tested
2. All acceptance criteria met
3. CI/CD pipeline passes
4. Code reviewed and approved
5. Documentation published
6. Performance benchmarks meet targets
7. No known bugs or issues
8. Integration with SQL parser verified

---

**Ready to implement?** Use this document as your complete specification. All details needed are provided above. Good luck! 🚀
