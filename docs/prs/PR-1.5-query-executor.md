# PR 1.5: Query Executor

## 📋 Overview

**PR Number:** 1.5
**Phase:** 1 - Core Foundation
**Priority:** 🔴 Critical
**Estimated Effort:** 8 days
**Dependencies:** PR 1.2 (B-Tree Index), PR 1.4 (Query Planner)

## 🎯 Objectives

Implement an iterator-based query executor that executes physical query plans efficiently. This includes:

- Iterator-based execution model (Volcano/Pipeline model)
- All basic relational operators
- Memory-efficient streaming execution
- Support for different join algorithms
- Aggregation and sorting
- Integration with storage engine and indexes

## 📝 Detailed Prompt for Implementation

```
Implement a complete query executor for AeternumDB with the following requirements:

1. **Iterator-Based Execution Model**
   - Each operator implements Iterator trait
   - Pull-based execution (demand-driven)
   - Streaming results (low memory footprint)
   - Async execution with tokio

2. **Scan Operators**
   - Sequential scan (table scan)
   - Index scan (B-tree traversal)
   - Predicate filtering during scan
   - Column projection

3. **Join Operators**
   - Nested loop join (simple, works for all cases)
   - Hash join (efficient for equality joins)
   - Sort-merge join (efficient for sorted inputs)
   - Support all join types: INNER, LEFT, RIGHT, FULL, CROSS

4. **Aggregation Operators**
   - Hash-based aggregation
   - Streaming aggregation (for sorted inputs)
   - Aggregate functions: COUNT, SUM, AVG, MIN, MAX
   - GROUP BY support

5. **Sorting Operators**
   - In-memory sorting (quicksort/mergesort)
   - External sorting (for large datasets)
   - Sort with multiple columns
   - ASC/DESC support

6. **Other Operators**
   - Filter (predicate evaluation)
   - Project (column selection)
   - Limit and Offset
   - Distinct (duplicate elimination)

7. **Memory Management**
   - Configurable memory limits per operator
   - Spill to disk when memory exceeded
   - Memory usage tracking

8. **Performance Requirements**
   - Sequential scan: >100K rows/sec
   - Index scan: >50K rows/sec
   - Hash join: >50K rows/sec
   - Aggregation: >100K rows/sec

Use Rust best practices, comprehensive error handling, and include detailed documentation.
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/executor/mod.rs`**
   - Public API for query execution
   - Executor context and configuration
   - RecordBatch structure for batched execution

2. **`core/src/executor/physical_plan.rs`**
   - ExecutionPlan trait
   - Plan execution context

3. **`core/src/executor/scan.rs`**
   - SeqScan operator
   - IndexScan operator

4. **`core/src/executor/filter.rs`**
   - Filter operator
   - Predicate evaluation

5. **`core/src/executor/project.rs`**
   - Project operator
   - Expression evaluation

6. **`core/src/executor/join.rs`**
   - NestedLoopJoin operator
   - HashJoin operator
   - SortMergeJoin operator
   - Join helper functions

7. **`core/src/executor/aggregate.rs`**
   - HashAggregate operator
   - StreamingAggregate operator
   - Aggregate function implementations

8. **`core/src/executor/sort.rs`**
   - Sort operator
   - External sort implementation
   - Sort helper functions

9. **`core/src/executor/limit.rs`**
   - Limit operator
   - Offset support

10. **`core/src/executor/distinct.rs`**
    - Distinct operator (duplicate elimination)

11. **`core/src/executor/expressions.rs`**
    - Expression evaluation
    - Scalar functions
    - Type coercion

### Test Files

12. **`core/tests/executor_tests.rs`**
    - Integration tests for executor

13. **`core/benches/executor_bench.rs`**
    - Execution performance benchmarks

## 🔧 Implementation Details

### Executor Trait

```rust
#[async_trait]
pub trait ExecutionPlan: Send + Sync {
    /// Get schema of output
    fn schema(&self) -> Arc<Schema>;

    /// Execute the plan and return record batch stream
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<SendableRecordBatchStream>;

    /// Get children plans
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;

    /// Get execution statistics
    fn statistics(&self) -> Statistics;
}

pub type SendableRecordBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;
```

### RecordBatch Structure

```rust
#[derive(Clone)]
pub struct RecordBatch {
    schema: Arc<Schema>,
    columns: Vec<ArrayRef>,
    num_rows: usize,
}

impl RecordBatch {
    pub fn new(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Result<Self> {
        // Validate all columns have same length
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);
        for col in &columns {
            if col.len() != num_rows {
                return Err(Error::InvalidRecordBatch);
            }
        }

        Ok(Self {
            schema,
            columns,
            num_rows,
        })
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn column(&self, index: usize) -> &ArrayRef {
        &self.columns[index]
    }
}
```

### Sequential Scan Operator

```rust
pub struct SeqScanExec {
    table_name: String,
    schema: Arc<Schema>,
    projection: Option<Vec<usize>>,
    filter: Option<Expr>,
    storage: Arc<StorageEngine>,
}

#[async_trait]
impl ExecutionPlan for SeqScanExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<SendableRecordBatchStream> {
        let storage = self.storage.clone();
        let table_name = self.table_name.clone();
        let filter = self.filter.clone();

        let stream = stream! {
            let mut page_id = 0;
            loop {
                let page = storage.read_page(&table_name, page_id).await?;
                if page.is_none() {
                    break;
                }

                let records = page.unwrap().parse_records()?;

                // Apply filter
                let filtered = if let Some(ref filter) = filter {
                    records.into_iter()
                        .filter(|r| eval_predicate(filter, r))
                        .collect()
                } else {
                    records
                };

                // Convert to record batch
                let batch = RecordBatch::from_rows(&filtered, &self.schema)?;
                yield Ok(batch);

                page_id += 1;
            }
        };

        Ok(Box::pin(stream))
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
}
```

### Hash Join Operator

```rust
pub struct HashJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    left_keys: Vec<Column>,
    right_keys: Vec<Column>,
    schema: Arc<Schema>,
}

#[async_trait]
impl ExecutionPlan for HashJoinExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<SendableRecordBatchStream> {
        // Phase 1: Build hash table from left side
        let left_stream = self.left.execute(ctx).await?;
        let mut hash_table = HashMap::new();

        pin_mut!(left_stream);
        while let Some(batch) = left_stream.next().await {
            let batch = batch?;
            for row_idx in 0..batch.num_rows() {
                let key = extract_join_key(&batch, row_idx, &self.left_keys)?;
                hash_table.entry(key)
                    .or_insert_with(Vec::new)
                    .push(batch.slice(row_idx, 1));
            }
        }

        // Phase 2: Probe hash table with right side
        let right_stream = self.right.execute(ctx).await?;
        let hash_table = Arc::new(hash_table);
        let join_type = self.join_type;
        let right_keys = self.right_keys.clone();
        let schema = self.schema.clone();

        let stream = stream! {
            pin_mut!(right_stream);
            while let Some(batch) = right_stream.next().await {
                let batch = batch?;
                let mut output_rows = Vec::new();

                for row_idx in 0..batch.num_rows() {
                    let key = extract_join_key(&batch, row_idx, &right_keys)?;

                    if let Some(left_rows) = hash_table.get(&key) {
                        // Match found - emit joined rows
                        for left_row in left_rows {
                            let joined = concatenate_batches(
                                left_row,
                                &batch.slice(row_idx, 1)
                            )?;
                            output_rows.push(joined);
                        }
                    } else if join_type == JoinType::Right || join_type == JoinType::Full {
                        // No match - emit right row with nulls for left
                        let joined = concatenate_with_nulls(
                            &batch.slice(row_idx, 1)
                        )?;
                        output_rows.push(joined);
                    }
                }

                if !output_rows.is_empty() {
                    let result = concatenate_batches_vertical(&output_rows)?;
                    yield Ok(result);
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }
}
```

### Hash Aggregate Operator

```rust
pub struct HashAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    group_by: Vec<Column>,
    aggregates: Vec<AggregateExpr>,
    schema: Arc<Schema>,
}

#[async_trait]
impl ExecutionPlan for HashAggregateExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(ctx).await?;
        let mut accumulators: HashMap<Vec<Value>, Vec<Box<dyn Accumulator>>> =
            HashMap::new();

        // Phase 1: Accumulate
        pin_mut!(input_stream);
        while let Some(batch) = input_stream.next().await {
            let batch = batch?;

            for row_idx in 0..batch.num_rows() {
                // Extract group key
                let group_key = self.group_by.iter()
                    .map(|col| batch.column(col.index()).value(row_idx))
                    .collect::<Vec<_>>();

                // Get or create accumulators for this group
                let accs = accumulators.entry(group_key)
                    .or_insert_with(|| {
                        self.aggregates.iter()
                            .map(|agg| create_accumulator(agg))
                            .collect()
                    });

                // Update accumulators
                for (acc, agg_expr) in accs.iter_mut().zip(&self.aggregates) {
                    let value = eval_expr(&agg_expr.expr, &batch, row_idx)?;
                    acc.accumulate(value)?;
                }
            }
        }

        // Phase 2: Emit results
        let schema = self.schema.clone();
        let stream = stream! {
            for (group_key, accs) in accumulators {
                let mut row_values = group_key;
                for acc in accs {
                    row_values.push(acc.finalize()?);
                }

                let batch = RecordBatch::from_values(
                    schema.clone(),
                    vec![row_values]
                )?;
                yield Ok(batch);
            }
        };

        Ok(Box::pin(stream))
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }
}
```

### Aggregate Functions

```rust
pub trait Accumulator: Send + Sync {
    fn accumulate(&mut self, value: Value) -> Result<()>;
    fn finalize(&self) -> Result<Value>;
}

pub struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, value: Value) -> Result<()> {
        if !value.is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::Int64(self.count))
    }
}

pub struct SumAccumulator {
    sum: Option<Decimal>,
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: Value) -> Result<()> {
        if let Value::Decimal(d) = value {
            self.sum = Some(self.sum.unwrap_or(Decimal::zero()) + d);
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.sum.map(Value::Decimal).unwrap_or(Value::Null))
    }
}

// Similarly implement: AvgAccumulator, MinAccumulator, MaxAccumulator
```

### API Examples

```rust
use aeternumdb::executor::{ExecutionContext, ExecutionPlan};
use aeternumdb::query::QueryPlanner;

// Create execution context
let ctx = ExecutionContext::new(storage.clone(), catalog.clone());

// Get physical plan from planner
let physical_plan = planner.create_physical_plan(logical_plan, &ctx)?;

// Execute plan
let mut stream = physical_plan.execute(&ctx).await?;

// Stream results
while let Some(batch) = stream.next().await {
    let batch = batch?;
    println!("Got {} rows", batch.num_rows());

    // Process batch
    for row_idx in 0..batch.num_rows() {
        for col_idx in 0..batch.num_columns() {
            let value = batch.column(col_idx).value(row_idx);
            print!("{}\t", value);
        }
        println!();
    }
}
```

## ✅ Tests Required

### Unit Tests

1. **SeqScan Tests** (`scan.rs`)
   - ✅ Scan empty table
   - ✅ Scan table with 1000 rows
   - ✅ Scan with column projection
   - ✅ Scan with filter predicate
   - ✅ Multiple concurrent scans

2. **IndexScan Tests** (`scan.rs`)
   - ✅ Index scan with exact match
   - ✅ Index scan with range query
   - ✅ Index scan with prefix match
   - ✅ Verify uses index (not seq scan)

3. **Join Tests** (`join.rs`)
   - ✅ Inner join with matches
   - ✅ Inner join with no matches
   - ✅ Left outer join
   - ✅ Right outer join
   - ✅ Full outer join
   - ✅ Cross join
   - ✅ Hash join vs nested loop join
   - ✅ Large table joins (>100K rows)

4. **Aggregate Tests** (`aggregate.rs`)
   - ✅ COUNT aggregate
   - ✅ SUM aggregate
   - ✅ AVG aggregate
   - ✅ MIN/MAX aggregate
   - ✅ GROUP BY single column
   - ✅ GROUP BY multiple columns
   - ✅ Aggregates with no groups
   - ✅ Empty input

5. **Sort Tests** (`sort.rs`)
   - ✅ Sort ascending
   - ✅ Sort descending
   - ✅ Sort by multiple columns
   - ✅ Sort with nulls (nulls first/last)
   - ✅ External sort (dataset > memory)
   - ✅ In-memory sort performance

6. **Other Operator Tests**
   - ✅ Filter operator
   - ✅ Project operator
   - ✅ Limit operator
   - ✅ Offset operator
   - ✅ Distinct operator

### Integration Tests

7. **End-to-End Tests** (`executor_tests.rs`)
   - ✅ Simple SELECT query
   - ✅ SELECT with WHERE clause
   - ✅ SELECT with JOIN
   - ✅ SELECT with GROUP BY
   - ✅ SELECT with ORDER BY
   - ✅ SELECT with LIMIT
   - ✅ Complex multi-join query
   - ✅ Subquery execution
   - ✅ Large dataset (1M rows)

### Performance Benchmarks

8. **Benchmarks** (`executor_bench.rs`)
   - ✅ Sequential scan throughput
   - ✅ Index scan throughput
   - ✅ Join throughput (all types)
   - ✅ Aggregation throughput
   - ✅ Sort throughput
   - ✅ End-to-end query latency

## 📊 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Sequential scan | >100K rows/sec | Benchmark |
| Index scan | >50K rows/sec | Benchmark |
| Hash join | >50K rows/sec | Benchmark |
| Aggregation | >100K rows/sec | Benchmark |
| In-memory sort | >50K rows/sec | Benchmark |
| Memory per operator | <100MB | Memory profiler |
| Batch size | 1K-10K rows | Tunable |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Comprehensive rustdoc comments for all public APIs
   - Example usage in doc comments
   - Explanation of each operator

2. **Executor Architecture Guide** (`docs/executor-architecture.md`)
   - Iterator model explanation
   - Operator implementations
   - Memory management strategy
   - Performance tuning

3. **API Documentation** (rustdoc)
   - Generate with `cargo doc --no-deps --open`
   - Ensure all public APIs documented

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] All scan operators work correctly
- [ ] All join operators work correctly
- [ ] Aggregation works (all functions)
- [ ] Sorting works (in-memory and external)
- [ ] All operators stream results efficiently
- [ ] Memory limits are respected
- [ ] Concurrent execution is safe

### Quality Requirements
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Code coverage >85%
- [ ] No clippy warnings
- [ ] Code formatted with `cargo fmt`
- [ ] Documentation complete and accurate

### Performance Requirements
- [ ] Throughput meets targets for all operators
- [ ] Memory usage stays within limits
- [ ] No memory leaks
- [ ] Scales to 1M+ rows

### Documentation Requirements
- [ ] All public APIs documented
- [ ] Executor architecture guide complete
- [ ] Examples provided
- [ ] Performance tuning guide

## 🔗 Dependencies

This PR depends on:
- **PR 1.2**: B-Tree Index (for index scans)
- **PR 1.4**: Query Planner (provides physical plans)

This PR is required by:
- **PR 1.11**: Network Protocol (exposes execution to clients)
- **PR 1.14**: Integration Tests

## 📦 Dependencies to Add

```toml
[dependencies]
async-stream = "0.3"
futures = "0.3"
hashbrown = "0.14"  # Fast HashMap implementation

[dev-dependencies]
proptest = "1.4"  # Property-based testing
```

## 🚀 Implementation Steps

### Day 1: Executor Foundation
- Define ExecutionPlan trait
- Implement RecordBatch structure
- Define execution context
- Write basic tests

### Day 2: Scan Operators
- Implement SeqScan operator
- Implement IndexScan operator
- Integrate with storage engine
- Write tests

### Day 3: Filter and Project
- Implement Filter operator
- Implement Project operator
- Expression evaluation framework
- Write tests

### Day 4: Join Operators (Part 1)
- Implement NestedLoopJoin
- Implement HashJoin (build and probe phases)
- Write tests

### Day 5: Join Operators (Part 2)
- Implement SortMergeJoin
- Support all join types (INNER, LEFT, RIGHT, FULL, CROSS)
- Write comprehensive join tests

### Day 6: Aggregation & Sort
- Implement HashAggregate
- Implement all aggregate functions
- Implement Sort (in-memory and external)
- Write tests

### Day 7: Other Operators & Integration
- Implement Limit/Offset
- Implement Distinct
- End-to-end integration tests
- Performance testing

### Day 8: Optimization & Documentation
- Run benchmarks and optimize hot paths
- Memory profiling and optimization
- Write executor architecture guide
- Code review and cleanup

## 🐛 Known Edge Cases to Handle

1. **Empty inputs**: All operators should handle empty record batches
2. **NULL values**: Proper NULL semantics in all operations
3. **Large values**: VARCHAR/TEXT fields that exceed batch size
4. **Memory limits**: Graceful spilling to disk
5. **Type mismatches**: Proper type coercion in expressions
6. **Division by zero**: In aggregates and expressions
7. **Overflow**: In arithmetic operations
8. **Concurrent cancellation**: Clean up resources when query cancelled

## 💡 Future Enhancements (Out of Scope)

- Vectorized execution (SIMD) → Phase 5
- Code generation (JIT compilation) → Phase 5
- Parallel execution (multi-threaded operators) → Phase 3
- GPU acceleration → Phase 5
- Adaptive execution (runtime optimization) → Phase 3
- Spilling optimizations → Phase 5

### AeternumDB-Specific Executor Requirements

The following AeternumDB extensions deferred from earlier PRs must be enforced
during query execution:

| Requirement | Description |
|-------------|-------------|
| **Reference column referential actions** | Enforce `ON DELETE` and `ON UPDATE` actions (CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION) stored in the `ColumnDef.on_delete` / `ColumnDef.on_update` AST fields when rows in referenced tables are mutated. |
| **GRANT / REVOKE enforcement** | Apply access-control rules recorded by `GRANT` / `REVOKE` statements before executing any DML; reject with a permission error when the current user lacks required privileges. |
| **Cluster-wide objid generation** | Assign a globally unique `objid` to every new row on `INSERT`.  This requires coordination with the cluster (Phase 3); for Phase 1 a node-local monotonic counter is acceptable as a placeholder. |

## 🏁 Definition of Done

This PR is complete when:
1. All code is implemented and tested
2. All acceptance criteria met
3. CI/CD pipeline passes
4. Code reviewed and approved
5. Documentation published
6. Performance benchmarks meet targets
7. No known bugs or issues
8. Integration with query planner verified

---

**Ready to implement?** Use this document as your complete specification. All details needed are provided above. Good luck! 🚀
