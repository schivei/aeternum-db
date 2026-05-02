//! Executor benchmarks.

use aeternumdb_core::executor::context::{AtomicIdGenerator, InMemoryTableProvider, ACL};
use aeternumdb_core::executor::{build_executor, ExecutionContext, Row, Value};
use aeternumdb_core::query::physical_plan::{NodeCost, PhysicalPlan};
use aeternumdb_core::sql::ast::Expr;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures::stream::StreamExt;
use std::sync::{Arc, Mutex};

fn bench_values_executor(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("values_executor_100_rows", |b| {
        b.iter(|| {
            rt.block_on(async {
                let ctx = ExecutionContext::default_test();

                let rows = (0..100)
                    .map(|i| {
                        vec![
                            Expr::Literal(crate::sql::ast::Value::Integer(i)),
                            Expr::Literal(crate::sql::ast::Value::String(format!("row_{}", i))),
                        ]
                    })
                    .collect();

                let plan = PhysicalPlan::Values {
                    rows,
                    cost: NodeCost::default(),
                };

                let executor = build_executor(&plan).unwrap();
                let mut stream = executor.execute(&ctx).await.unwrap();

                let mut total_rows = 0;
                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.unwrap();
                    total_rows += batch.row_count();
                }

                black_box(total_rows);
            });
        });
    });
}

fn bench_seq_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("seq_scan_1000_rows", |b| {
        b.iter(|| {
            rt.block_on(async {
                let provider = Arc::new(InMemoryTableProvider::new());
                provider.add_table(
                    "test_table",
                    vec![("id".to_string(), "integer".to_string())],
                );

                let rows: Vec<Row> = (0..1000)
                    .map(|i| Row::from_pairs(vec![("id".to_string(), Value::Integer(i))]))
                    .collect();
                provider.add_rows("test_table", rows);

                let ctx = ExecutionContext::new(
                    provider,
                    Arc::new(Mutex::new(ACL::new())),
                    Arc::new(AtomicIdGenerator::default()),
                    "bench_user".to_string(),
                );

                let plan = PhysicalPlan::SeqScan {
                    table: "test_table".to_string(),
                    alias: None,
                    columns: None,
                    filter: None,
                    cost: NodeCost::default(),
                };

                let executor = build_executor(&plan).unwrap();
                let mut stream = executor.execute(&ctx).await.unwrap();

                let mut total_rows = 0;
                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.unwrap();
                    total_rows += batch.row_count();
                }

                black_box(total_rows);
            });
        });
    });
}

criterion_group!(benches, bench_values_executor, bench_seq_scan);
criterion_main!(benches);
