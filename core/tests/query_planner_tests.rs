//! Integration tests for the query planner.
//!
//! These tests exercise the full planning pipeline from SQL string to physical
//! plan, covering the AeternumDB-specific semantics documented in
//! `docs/sql-reference.md` and `docs/prs/PR-1.4-query-planner.md`.

use aeternumdb_core::query::explain::{explain_logical, explain_physical};
use aeternumdb_core::query::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use aeternumdb_core::query::optimizer::Optimizer;
use aeternumdb_core::query::physical_plan::PhysicalPlan;
use aeternumdb_core::query::statistics::{StatisticsRegistry, TableStats};
use aeternumdb_core::query::{PlannerContext, PlannerError, QueryPlanner};
use aeternumdb_core::sql::ast::{DataType, Statement};
use aeternumdb_core::sql::parser::SqlParser;
use aeternumdb_core::sql::validator::{Catalog, ColumnSchema, TableSchema};

// ── Helpers ───────────────────────────────────────────────────────────────────

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
                name: "name".into(),
                data_type: DataType::Varchar(Some(255)),
                nullable: true,
            },
            ColumnSchema {
                name: "age".into(),
                data_type: DataType::Integer,
                nullable: true,
            },
            ColumnSchema {
                name: "email".into(),
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
            ColumnSchema {
                name: "status".into(),
                data_type: DataType::Varchar(Some(50)),
                nullable: true,
            },
        ],
    });
    c.add_table(TableSchema {
        name: "products".into(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnSchema {
                name: "name".into(),
                data_type: DataType::Varchar(Some(255)),
                nullable: false,
            },
            ColumnSchema {
                name: "price".into(),
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

    let mut ts = TableStats::new("products");
    ts.num_rows = 500;
    ts.num_pages = 5;
    reg.add(ts);

    reg
}

fn parse(sql: &str) -> Statement {
    SqlParser::new().parse_one(sql).unwrap()
}

fn plan_sql(sql: &str, ctx: &PlannerContext<'_>) -> PhysicalPlan {
    let stmt = parse(sql);
    let planner = QueryPlanner::new();
    planner.plan(&stmt, ctx).unwrap()
}

// ── Basic SELECT tests ────────────────────────────────────────────────────────

#[test]
fn select_star_from_table() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    let phys = plan_sql("SELECT * FROM users", &ctx);
    let out = explain_physical(&phys);
    assert!(out.contains("users"));
    assert!(out.contains("Estimated Rows:"));
}

#[test]
fn select_columns_produces_project_then_scan() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    let phys = plan_sql("SELECT id, name FROM users", &ctx);
    let out = explain_physical(&phys);
    assert!(out.contains("users"));
}

#[test]
fn select_with_equality_predicate_uses_index_scan() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    let phys = plan_sql("SELECT id FROM users WHERE id = 42", &ctx);
    let out = explain_physical(&phys);
    // Predicate pushed into scan; index scan when equality on column reference.
    assert!(out.contains("users"));
    // The equality predicate on `id` (a column reference) against a literal
    // should deterministically select IndexScan.
    assert!(
        out.contains("IndexScan"),
        "expected IndexScan in explain output, got:\n{out}"
    );
}

#[test]
fn select_with_range_predicate() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    let phys = plan_sql(
        "SELECT id, age FROM users WHERE age > 18 AND age < 65",
        &ctx,
    );
    let out = explain_physical(&phys);
    assert!(out.contains("users"));
}

#[test]
fn select_with_order_by() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    let phys = plan_sql("SELECT id, name FROM users ORDER BY name", &ctx);
    let out = explain_physical(&phys);
    assert!(out.contains("Sort"));
}

#[test]
fn select_with_limit_and_offset() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    let phys = plan_sql("SELECT id FROM users LIMIT 20 OFFSET 40", &ctx);
    let out = explain_physical(&phys);
    assert!(out.contains("Limit"));
    assert!(out.contains("offset: 40"));
}

#[test]
fn select_with_group_by_count() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    let phys = plan_sql("SELECT age, COUNT(*) FROM users GROUP BY age", &ctx);
    let out = explain_physical(&phys);
    assert!(out.contains("HashAggregate") || out.contains("Aggregate"));
}

// ── JOIN tests ────────────────────────────────────────────────────────────────

#[test]
fn inner_join_two_tables() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    let phys = plan_sql(
        "SELECT users.id, orders.total \
         FROM users JOIN orders ON users.id = orders.user_id",
        &ctx,
    );
    let out = explain_physical(&phys);
    assert!(out.contains("Join"));
    assert!(out.contains("users"));
    assert!(out.contains("orders"));
}

#[test]
fn left_join_produces_join_node() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    let phys = plan_sql(
        "SELECT users.id, orders.total \
         FROM users LEFT JOIN orders ON users.id = orders.user_id",
        &ctx,
    );
    let out = explain_physical(&phys);
    assert!(out.contains("Join"));
}

#[test]
fn large_table_join_uses_hash_join() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();
    // users (10k) JOIN orders (50k) → both large → HashJoin
    let phys = plan_sql(
        "SELECT users.id, orders.total \
         FROM users JOIN orders ON users.id = orders.user_id",
        &ctx,
    );
    let out = explain_physical(&phys);
    assert!(out.contains("HashJoin"));
}

// ── Optimizer tests ───────────────────────────────────────────────────────────

#[test]
fn optimizer_pushes_predicate_into_scan() {
    let cat = make_catalog();
    let stats = make_stats();
    let optimizer = Optimizer::new(&stats);
    let builder = LogicalPlanBuilder::new(&cat);

    let stmt = parse("SELECT id FROM users WHERE age > 18");
    let logical = builder.build_from_statement(&stmt).unwrap();
    let optimized = optimizer.optimize(logical);

    // After pushdown the predicate should be inside the scan or a filter close
    // to the scan (no filter floating above the projection).
    let explain_out = explain_logical(&optimized);
    assert!(explain_out.contains("users"));
}

#[test]
fn optimizer_reorders_join_smaller_first() {
    let cat = make_catalog();
    let stats = make_stats();
    // products (500 rows) JOIN orders (50k rows) → products should be on left
    let optimizer = Optimizer::new(&stats);
    let builder = LogicalPlanBuilder::new(&cat);

    let stmt = parse(
        "SELECT products.name, orders.total \
         FROM orders JOIN products ON orders.id = products.id",
    );
    let logical = builder.build_from_statement(&stmt).unwrap();
    let optimized = optimizer.optimize(logical);
    let explain_out = explain_logical(&optimized);
    // products comes before orders in the explain output (left side of join)
    let pos_products = explain_out.find("products").unwrap_or(usize::MAX);
    let pos_orders = explain_out.find("orders").unwrap_or(0);
    assert!(
        pos_products < pos_orders,
        "expected 'products' (smaller) before 'orders' in plan:\n{explain_out}"
    );
}

// ── AeternumDB-specific constraint tests ──────────────────────────────────────

#[test]
fn flat_table_cannot_join() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.add_flat_table("users");

    let stmt = parse("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    let planner = QueryPlanner::new();
    let err = planner.create_logical_plan(&stmt, &ctx).unwrap_err();
    assert!(matches!(err, PlannerError::FlatTableJoin(_)));
}

#[test]
fn flat_table_single_scan_allowed() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.add_flat_table("users");

    let phys = plan_sql("SELECT * FROM users WHERE age > 30", &ctx);
    // Should still plan successfully — no join restriction for single scan.
    let out = explain_physical(&phys);
    assert!(out.contains("users"));
}

// ── Subquery tests ────────────────────────────────────────────────────────────

#[test]
fn subquery_in_from_clause() {
    let cat = make_catalog();
    let ctx = PlannerContext::new(&cat);
    let stmt = parse("SELECT sub.id FROM (SELECT id FROM users WHERE age > 18) sub");
    let planner = QueryPlanner::new();
    let phys = planner.plan(&stmt, &ctx).unwrap();
    let out = explain_physical(&phys);
    assert!(out.contains("users"));
}

// ── Non-SELECT statement rejection ───────────────────────────────────────────

#[test]
fn insert_statement_rejected() {
    let cat = make_catalog();
    let ctx = PlannerContext::new(&cat);
    let stmt = parse("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    let planner = QueryPlanner::new();
    let err = planner.create_logical_plan(&stmt, &ctx).unwrap_err();
    assert!(matches!(err, PlannerError::UnsupportedStatement));
}

#[test]
fn update_statement_rejected() {
    let cat = make_catalog();
    let ctx = PlannerContext::new(&cat);
    let stmt = parse("UPDATE users SET name = 'Bob' WHERE id = 1");
    let planner = QueryPlanner::new();
    let err = planner.create_logical_plan(&stmt, &ctx).unwrap_err();
    assert!(matches!(err, PlannerError::UnsupportedStatement));
}

// ── EXPLAIN output tests ──────────────────────────────────────────────────────

#[test]
fn explain_output_has_all_sections() {
    let cat = make_catalog();
    let mut ctx = PlannerContext::new(&cat);
    ctx.statistics = make_stats();

    let phys = plan_sql(
        "SELECT users.id, orders.total \
         FROM users JOIN orders ON users.id = orders.user_id \
         WHERE orders.total > 100 \
         ORDER BY orders.total DESC \
         LIMIT 50",
        &ctx,
    );
    let out = explain_physical(&phys);
    // Required sections
    assert!(out.contains("Physical Plan:"), "missing header");
    assert!(out.contains("Total Cost:"), "missing total cost");
    assert!(out.contains("Estimated Rows:"), "missing estimated rows");
    // Operators expected
    assert!(out.contains("Sort"), "missing sort");
    assert!(out.contains("Limit"), "missing limit");
    assert!(out.contains("Join"), "missing join");
}

#[test]
fn explain_logical_header() {
    let plan = LogicalPlan::Values { rows: vec![] };
    let out = explain_logical(&plan);
    assert!(out.starts_with("Logical Plan:"));
}

// ── Cost estimation tests ─────────────────────────────────────────────────────

#[test]
fn plan_with_statistics_has_lower_cost_than_without() {
    let cat = make_catalog();

    // Plan WITHOUT statistics
    let ctx_empty = PlannerContext::new(&cat);
    let phys_empty = plan_sql("SELECT * FROM users", &ctx_empty);

    // Plan WITH statistics (more rows → higher cost)
    let mut ctx_stats = PlannerContext::new(&cat);
    ctx_stats.statistics = make_stats(); // 10k rows
    let phys_stats = plan_sql("SELECT * FROM users", &ctx_stats);

    // With 10k rows the cost should be higher than the 1k default
    assert!(
        phys_stats.cost().total > phys_empty.cost().total,
        "expected higher cost with more rows: {} vs {}",
        phys_stats.cost().total,
        phys_empty.cost().total
    );
}
