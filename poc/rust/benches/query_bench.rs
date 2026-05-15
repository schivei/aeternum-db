//! Criterion benchmarks for the query planner pipeline.
//!
//! Run with: `cargo bench --bench query_bench`

use aeternumdb_core::query::cost_model::CostModel;
use aeternumdb_core::query::logical_plan::LogicalPlanBuilder;
use aeternumdb_core::query::optimizer::Optimizer;
use aeternumdb_core::query::physical_plan::PhysicalPlanner;
use aeternumdb_core::query::statistics::{StatisticsRegistry, TableStats};
use aeternumdb_core::query::{PlannerContext, QueryPlanner};
use aeternumdb_core::sql::ast::DataType;
use aeternumdb_core::sql::parser::SqlParser;
use aeternumdb_core::sql::validator::{Catalog, ColumnSchema, TableSchema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn make_catalog() -> Catalog {
    let mut c = Catalog::new();
    c.add_table(TableSchema {
        name: "users".into(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnSchema {
                name: "age".into(),
                data_type: DataType::Integer,
                nullable: true,
            },
            ColumnSchema {
                name: "name".into(),
                data_type: DataType::Varchar(Some(255)),
                nullable: true,
            },
        ],
    });
    c.add_table(TableSchema {
        name: "orders".into(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnSchema {
                name: "user_id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnSchema {
                name: "total".into(),
                data_type: DataType::Decimal(Some(10), Some(2)),
                nullable: false,
            },
        ],
    });
    c
}

fn make_stats() -> StatisticsRegistry {
    let mut reg = StatisticsRegistry::new();
    let mut ts = TableStats::new("users");
    ts.num_rows = 10_000;
    ts.num_pages = 100;
    reg.add(ts);
    let mut ts = TableStats::new("orders");
    ts.num_rows = 50_000;
    ts.num_pages = 500;
    reg.add(ts);
    reg
}

// ── Simple SELECT ─────────────────────────────────────────────────────────────

fn bench_simple_select(c: &mut Criterion) {
    let catalog = make_catalog();
    let mut ctx = PlannerContext::new(&catalog);
    ctx.statistics = make_stats();
    let planner = QueryPlanner::new();
    let parser = SqlParser::new();
    let stmt = parser
        .parse_one("SELECT id, name FROM users WHERE age > 18")
        .unwrap();

    c.bench_function("query_planner/simple_select", |b| {
        b.iter(|| {
            planner.plan(&stmt, &ctx).unwrap();
        });
    });
}

// ── JOIN query ────────────────────────────────────────────────────────────────

fn bench_join_query(c: &mut Criterion) {
    let catalog = make_catalog();
    let mut ctx = PlannerContext::new(&catalog);
    ctx.statistics = make_stats();
    let planner = QueryPlanner::new();
    let parser = SqlParser::new();
    let stmt = parser
        .parse_one(
            "SELECT users.id, orders.total \
             FROM users JOIN orders ON users.id = orders.user_id \
             WHERE orders.total > 100 ORDER BY orders.total LIMIT 20",
        )
        .unwrap();

    c.bench_function("query_planner/join_query", |b| {
        b.iter(|| {
            planner.plan(&stmt, &ctx).unwrap();
        });
    });
}

// ── Logical plan build ────────────────────────────────────────────────────────

fn bench_logical_plan_build(c: &mut Criterion) {
    let catalog = make_catalog();
    let parser = SqlParser::new();
    let stmts: Vec<_> = [
        "SELECT * FROM users",
        "SELECT id FROM users WHERE age > 18",
        "SELECT age, COUNT(*) FROM users GROUP BY age",
        "SELECT users.id, orders.total FROM users JOIN orders ON users.id = orders.user_id",
    ]
    .iter()
    .map(|s| parser.parse_one(s).unwrap())
    .collect();

    let mut group = c.benchmark_group("query_planner/logical_build");
    for (i, stmt) in stmts.iter().enumerate() {
        let builder = LogicalPlanBuilder::new(&catalog);
        group.bench_with_input(BenchmarkId::from_parameter(i), stmt, |b, stmt| {
            b.iter(|| {
                builder.build_from_statement(stmt).unwrap();
            });
        });
    }
    group.finish();
}

// ── Optimizer ─────────────────────────────────────────────────────────────────

fn bench_optimizer(c: &mut Criterion) {
    let catalog = make_catalog();
    let stats = make_stats();
    let optimizer = Optimizer::new(&stats);
    let builder = LogicalPlanBuilder::new(&catalog);
    let parser = SqlParser::new();
    let stmt = parser
        .parse_one(
            "SELECT users.id, orders.total \
             FROM users JOIN orders ON users.id = orders.user_id \
             WHERE users.age > 18",
        )
        .unwrap();
    let logical = builder.build_from_statement(&stmt).unwrap();

    c.bench_function("query_planner/optimizer", |b| {
        b.iter(|| {
            optimizer.optimize(logical.clone());
        });
    });
}

// ── Physical plan lowering ────────────────────────────────────────────────────

fn bench_physical_lowering(c: &mut Criterion) {
    let catalog = make_catalog();
    let stats = make_stats();
    let optimizer = Optimizer::new(&stats);
    let builder = LogicalPlanBuilder::new(&catalog);
    let parser = SqlParser::new();
    let stmt = parser
        .parse_one(
            "SELECT users.id, orders.total \
             FROM users JOIN orders ON users.id = orders.user_id \
             WHERE users.age > 18 ORDER BY orders.total LIMIT 100",
        )
        .unwrap();
    let logical = builder.build_from_statement(&stmt).unwrap();
    let optimized = optimizer.optimize(logical);
    let physical_planner = PhysicalPlanner::new(CostModel::default(), &stats);

    c.bench_function("query_planner/physical_lowering", |b| {
        b.iter(|| {
            physical_planner.lower(&optimized);
        });
    });
}

criterion_group!(
    benches,
    bench_simple_select,
    bench_join_query,
    bench_logical_plan_build,
    bench_optimizer,
    bench_physical_lowering,
);
criterion_main!(benches);
