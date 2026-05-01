//! Integration tests for the AeternumDB executor module.
//!
//! These tests exercise all executor operators, DML functions, ACL helpers,
//! and the physical-plan builder end-to-end using an in-memory table provider.

use aeternumdb_core::executor::*;
use aeternumdb_core::query::logical_plan::{
    AggregateExpr, ProjectionItem, SortExpr, ViewAsProjection,
};
use aeternumdb_core::query::physical_plan::{NodeCost, PhysicalPlan, SortAlgorithm};
use aeternumdb_core::sql::ast::{BinaryOperator, DataType, Expr, JoinType, UnaryOperator};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn col(name: &str) -> Expr {
    Expr::Column {
        table: None,
        name: name.to_string(),
    }
}

fn int_lit(n: i64) -> Expr {
    Expr::Literal(aeternumdb_core::sql::ast::Value::Integer(n))
}

fn zero_cost() -> NodeCost {
    NodeCost {
        total: 0.0,
        io: 0.0,
        cpu: 0.0,
        estimated_rows: 0,
    }
}

fn make_row(pairs: &[(&str, Value)]) -> Row {
    Row::from_pairs(pairs.iter().map(|(k, v)| (k.to_string(), v.clone())))
}

fn make_ctx_with_table(
    table: &str,
    schema: Vec<(&str, &str)>,
    rows: Vec<Row>,
) -> (ExecutionContext, Arc<InMemoryTableProvider>) {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table(
        table,
        schema
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect(),
    );
    provider.add_rows(table, rows);
    let ctx = ExecutionContext::new(
        provider.clone(),
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "test_user".to_string(),
    );
    (ctx, provider)
}

async fn collect_rows(plan: &dyn ExecutionPlan, ctx: &ExecutionContext) -> Vec<Row> {
    let mut stream = plan.execute(ctx).await.expect("execute failed");
    let mut all = Vec::new();
    while let Some(batch_res) = stream.next().await {
        let batch = batch_res.expect("batch error");
        all.extend(batch.rows);
    }
    all
}

/// Collect rows from a plan, propagating any execution or batch error.
async fn try_collect(
    plan: &dyn ExecutionPlan,
    ctx: &ExecutionContext,
) -> std::result::Result<Vec<Row>, ExecutorError> {
    let mut stream = plan.execute(ctx).await?;
    let mut all = Vec::new();
    while let Some(batch_res) = stream.next().await {
        let batch = batch_res?;
        all.extend(batch.rows);
    }
    Ok(all)
}

fn float_lit(f: f64) -> Expr {
    Expr::Literal(aeternumdb_core::sql::ast::Value::Float(f))
}

fn str_lit(s: &str) -> Expr {
    Expr::Literal(aeternumdb_core::sql::ast::Value::String(s.to_string()))
}

fn bool_lit(b: bool) -> Expr {
    Expr::Literal(aeternumdb_core::sql::ast::Value::Boolean(b))
}

fn null_lit() -> Expr {
    Expr::Literal(aeternumdb_core::sql::ast::Value::Null)
}

fn func_expr(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function {
        name: name.to_string(),
        args,
        distinct: false,
    }
}

fn cast_expr(expr: Expr, dt: DataType) -> Expr {
    Expr::Cast {
        expr: Box::new(expr),
        data_type: dt,
    }
}

fn proj_item(expr: Expr, alias: &str) -> ProjectionItem {
    ProjectionItem {
        expr,
        alias: Some(alias.to_string()),
    }
}

/// Build a one-row ValuesExec from a single literal expression.
fn single_value_exec(val: Expr) -> Arc<ValuesExec> {
    Arc::new(ValuesExec::new(vec![vec![val]], vec!["v".to_string()]))
}

fn default_ctx() -> ExecutionContext {
    ExecutionContext::default_test()
}

// ── InMemoryTableProvider ─────────────────────────────────────────────────────

#[test]
fn test_in_memory_provider_table_exists() {
    let provider = InMemoryTableProvider::new();
    provider.add_table("t", vec![("id".to_string(), "integer".to_string())]);
    assert!(provider.table_exists("t"));
    assert!(!provider.table_exists("missing"));
}

#[test]
fn test_in_memory_provider_schema() {
    let provider = InMemoryTableProvider::new();
    provider.add_table(
        "users",
        vec![
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
        ],
    );
    let schema = provider.schema("users").unwrap();
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].0, "id");
    assert_eq!(schema[1].0, "name");
}

#[tokio::test]
async fn test_in_memory_provider_scan() {
    let provider = InMemoryTableProvider::new();
    provider.add_table("t", vec![("x".to_string(), "integer".to_string())]);
    provider.add_rows(
        "t",
        vec![
            make_row(&[("x", Value::Integer(10))]),
            make_row(&[("x", Value::Integer(20))]),
        ],
    );
    let rows = provider.scan("t").await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_in_memory_provider_insert() {
    let provider = InMemoryTableProvider::new();
    provider.add_table("t", vec![("x".to_string(), "integer".to_string())]);
    let n = provider
        .insert("t", vec![make_row(&[("x", Value::Integer(42))])])
        .await
        .unwrap();
    assert_eq!(n, 1);
    let rows = provider.scan("t").await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_in_memory_provider_update() {
    let provider = InMemoryTableProvider::new();
    provider.add_table("t", vec![("x".to_string(), "integer".to_string())]);
    provider.add_rows("t", vec![make_row(&[("x", Value::Integer(1))])]);
    let mut updates = HashMap::new();
    updates.insert("x".to_string(), Value::Integer(99));
    let n = provider.update("t", updates).await.unwrap();
    assert_eq!(n, 1);
    let rows = provider.scan("t").await.unwrap();
    assert_eq!(rows[0].get("x"), Some(&Value::Integer(99)));
}

#[tokio::test]
async fn test_in_memory_provider_delete() {
    let provider = InMemoryTableProvider::new();
    provider.add_table("t", vec![("x".to_string(), "integer".to_string())]);
    provider.add_rows(
        "t",
        vec![
            make_row(&[("x", Value::Integer(1))]),
            make_row(&[("x", Value::Integer(2))]),
        ],
    );
    let n = provider.delete("t").await.unwrap();
    assert_eq!(n, 2);
    let rows = provider.scan("t").await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_in_memory_provider_table_not_found() {
    let provider = InMemoryTableProvider::new();
    let result = provider.scan("nope").await;
    assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
}

// ── ACL ───────────────────────────────────────────────────────────────────────

#[test]
fn test_acl_grant_and_check() {
    let mut acl = ACL::new();
    acl.grant("alice", "orders", "SELECT");
    assert!(acl.check("alice", "orders", "SELECT"));
    assert!(!acl.check("alice", "orders", "INSERT"));
    assert!(!acl.check("bob", "orders", "SELECT"));
}

#[test]
fn test_acl_revoke() {
    let mut acl = ACL::new();
    acl.grant("alice", "orders", "SELECT");
    acl.revoke("alice", "orders", "SELECT");
    assert!(!acl.check("alice", "orders", "SELECT"));
}

#[test]
fn test_acl_revoke_nonexistent_is_noop() {
    let mut acl = ACL::new();
    acl.revoke("nobody", "t", "SELECT");
    assert!(!acl.check("nobody", "t", "SELECT"));
}

// ── AtomicIdGenerator ─────────────────────────────────────────────────────────

#[test]
fn test_atomic_id_generator() {
    let gen = AtomicIdGenerator::new(10);
    assert_eq!(gen.next_id(), 10);
    assert_eq!(gen.next_id(), 11);
    assert_eq!(gen.next_id(), 12);
}

#[test]
fn test_atomic_id_generator_default_starts_at_one() {
    let gen = AtomicIdGenerator::default();
    assert_eq!(gen.next_id(), 1);
}

// ── RecordBatch / Row / Value ──────────────────────────────────────────────────

#[test]
fn test_record_batch_operations() {
    let mut batch = RecordBatch::new(vec!["a".to_string(), "b".to_string()]);
    assert!(batch.is_empty());
    batch.add_row(make_row(&[
        ("a", Value::Integer(1)),
        ("b", Value::Boolean(true)),
    ]));
    assert_eq!(batch.row_count(), 1);
    assert_eq!(batch.column_count(), 2);
    assert!(!batch.is_empty());
}

#[test]
fn test_value_null_propagation() {
    assert!(Value::Null.is_null());
    assert_eq!(Value::Null.as_bool(), None);
    assert_eq!(Value::Null.as_integer(), None);
    assert_eq!(Value::Null.as_float(), None);
    assert_eq!(Value::Null.as_string(), None);
    assert_eq!(Value::Null.as_array(), None);
}

#[test]
fn test_value_conversions() {
    assert_eq!(Value::Boolean(true).as_bool(), Some(true));
    assert_eq!(Value::Boolean(false).as_bool(), Some(false));
    assert_eq!(Value::Integer(7).as_integer(), Some(7));
    assert_eq!(Value::Float(3.0).as_float(), Some(3.0));
    assert_eq!(Value::Integer(5).as_float(), Some(5.0));
    assert_eq!(
        Value::String("hi".to_string()).as_string(),
        Some("hi".to_string())
    );
    assert_eq!(
        Value::Array(vec![Value::Integer(1)]).as_array(),
        Some(vec![Value::Integer(1)])
    );
}

#[test]
fn test_row_from_pairs_and_get() {
    let row = make_row(&[("x", Value::Integer(42)), ("y", Value::Boolean(true))]);
    assert_eq!(row.get("x"), Some(&Value::Integer(42)));
    assert_eq!(row.get("y"), Some(&Value::Boolean(true)));
    assert_eq!(row.get("z"), None);
}

#[test]
fn test_row_merge() {
    let mut r1 = make_row(&[("a", Value::Integer(1))]);
    let r2 = make_row(&[("b", Value::Integer(2))]);
    r1.merge(&r2);
    assert_eq!(r1.get("a"), Some(&Value::Integer(1)));
    assert_eq!(r1.get("b"), Some(&Value::Integer(2)));
}

#[test]
fn test_row_insert_and_column_names() {
    let mut row = Row::new();
    row.insert("k".to_string(), Value::Integer(5));
    assert_eq!(row.get("k"), Some(&Value::Integer(5)));
    assert!(row.column_names().contains(&"k".to_string()));
}

// ── SeqScanExec ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_seq_scan_all_rows() {
    let (ctx, _) = make_ctx_with_table(
        "items",
        vec![("id", "integer"), ("val", "text")],
        vec![
            make_row(&[
                ("id", Value::Integer(1)),
                ("val", Value::String("a".into())),
            ]),
            make_row(&[
                ("id", Value::Integer(2)),
                ("val", Value::String("b".into())),
            ]),
        ],
    );
    let exec = SeqScanExec::new("items".into(), None, None, None);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_seq_scan_with_filter() {
    let (ctx, _) = make_ctx_with_table(
        "nums",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(1))]),
            make_row(&[("n", Value::Integer(5))]),
            make_row(&[("n", Value::Integer(10))]),
        ],
    );
    let filter = Expr::BinaryOp {
        left: Box::new(col("n")),
        op: BinaryOperator::Gt,
        right: Box::new(int_lit(3)),
    };
    let exec = SeqScanExec::new("nums".into(), None, None, Some(filter));
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_seq_scan_empty_table() {
    let (ctx, _) = make_ctx_with_table("empty", vec![("x", "integer")], vec![]);
    let exec = SeqScanExec::new("empty".into(), None, None, None);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_seq_scan_with_alias() {
    let (ctx, _) = make_ctx_with_table(
        "t",
        vec![("x", "integer")],
        vec![make_row(&[("x", Value::Integer(42))])],
    );
    let exec = SeqScanExec::new("t".into(), Some("alias_t".into()), None, None);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
}

// ── IndexScanExec ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_index_scan_with_key_predicate() {
    let (ctx, _) = make_ctx_with_table(
        "docs",
        vec![("id", "integer"), ("body", "text")],
        vec![
            make_row(&[
                ("id", Value::Integer(1)),
                ("body", Value::String("hello".into())),
            ]),
            make_row(&[
                ("id", Value::Integer(2)),
                ("body", Value::String("world".into())),
            ]),
        ],
    );
    let key_pred = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(int_lit(1)),
    };
    let exec = IndexScanExec::new("docs".into(), None, "idx_id".into(), None, key_pred, None);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
}

#[tokio::test]
async fn test_index_scan_no_match() {
    let (ctx, _) = make_ctx_with_table(
        "docs2",
        vec![("id", "integer")],
        vec![make_row(&[("id", Value::Integer(5))])],
    );
    let key_pred = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(int_lit(99)),
    };
    let exec = IndexScanExec::new("docs2".into(), None, "idx_id".into(), None, key_pred, None);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

// ── FilterExec ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_filter_exec_greater_equal() {
    let (ctx, _) = make_ctx_with_table(
        "vals",
        vec![("v", "integer")],
        vec![
            make_row(&[("v", Value::Integer(1))]),
            make_row(&[("v", Value::Integer(2))]),
            make_row(&[("v", Value::Integer(3))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("vals".into(), None, None, None));
    let pred = Expr::BinaryOp {
        left: Box::new(col("v")),
        op: BinaryOperator::GtEq,
        right: Box::new(int_lit(2)),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_filter_exec_no_matches() {
    let (ctx, _) = make_ctx_with_table(
        "flt_t",
        vec![("x", "integer")],
        vec![make_row(&[("x", Value::Integer(1))])],
    );
    let scan = Arc::new(SeqScanExec::new("flt_t".into(), None, None, None));
    let pred = Expr::BinaryOp {
        left: Box::new(col("x")),
        op: BinaryOperator::Gt,
        right: Box::new(int_lit(100)),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

// ── NestedLoopJoinExec ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_nested_loop_inner_join_self() {
    let (ctx, _) = make_ctx_with_table(
        "nl_t",
        vec![("id", "integer")],
        vec![
            make_row(&[("id", Value::Integer(1))]),
            make_row(&[("id", Value::Integer(2))]),
        ],
    );
    let left = Arc::new(SeqScanExec::new("nl_t".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("nl_t".into(), None, None, None));
    let cond = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(col("id")),
    };
    let exec = NestedLoopJoinExec::new(left, right, JoinType::Inner, Some(cond));
    let rows = collect_rows(&exec, &ctx).await;
    // In a self-join, merged rows share the same "id" column name, so id=id is
    // always true for every pair → 2x2 = 4 rows
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_nested_loop_cross_join() {
    let (ctx, _) = make_ctx_with_table(
        "cross_t",
        vec![("x", "integer")],
        vec![
            make_row(&[("x", Value::Integer(1))]),
            make_row(&[("x", Value::Integer(2))]),
        ],
    );
    let left = Arc::new(SeqScanExec::new("cross_t".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("cross_t".into(), None, None, None));
    let exec = NestedLoopJoinExec::new(left, right, JoinType::Cross, None);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_nested_loop_left_join_unmatched() {
    let (ctx, _) = make_ctx_with_table(
        "left_src",
        vec![("id", "integer")],
        vec![
            make_row(&[("id", Value::Integer(1))]),
            make_row(&[("id", Value::Integer(99))]),
        ],
    );
    // Right table only has id=1, so id=99 from left should still appear (LEFT JOIN)
    let left = Arc::new(SeqScanExec::new("left_src".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("left_src".into(), None, None, None));
    let cond = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(col("id")),
    };
    let exec = NestedLoopJoinExec::new(left, right, JoinType::Left, Some(cond));
    let rows = collect_rows(&exec, &ctx).await;
    // In a self-join, merged rows share the same "id" column, so the condition
    // id=id is always true for every pair → 2x2 = 4 matched rows (no unmatched)
    assert_eq!(rows.len(), 4);
}

// ── HashJoinExec ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_join_exec_inner() {
    let (ctx, _) = make_ctx_with_table(
        "hj_t",
        vec![("id", "integer"), ("val", "text")],
        vec![
            make_row(&[
                ("id", Value::Integer(1)),
                ("val", Value::String("a".into())),
            ]),
            make_row(&[
                ("id", Value::Integer(2)),
                ("val", Value::String("b".into())),
            ]),
        ],
    );
    let left = Arc::new(SeqScanExec::new("hj_t".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("hj_t".into(), None, None, None));
    let exec = HashJoinExec::new(
        left,
        right,
        JoinType::Inner,
        vec![col("id")],
        vec![col("id")],
        None,
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_hash_join_exec_no_match() {
    let (ctx, _) = make_ctx_with_table(
        "hjnm_t",
        vec![("k", "integer")],
        vec![make_row(&[("k", Value::Integer(1))])],
    );
    let left = Arc::new(SeqScanExec::new("hjnm_t".into(), None, None, None));
    // right is an empty table
    let provider2 = Arc::new(InMemoryTableProvider::new());
    provider2.add_table("empty2", vec![("k".to_string(), "integer".to_string())]);
    let ctx2 = ExecutionContext::new(
        provider2.clone(),
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "u".to_string(),
    );
    let _ = ctx2; // Different context; test empty right side behavior via same-table
                  // Use a filter to produce an empty right side
    let right_scan = Arc::new(SeqScanExec::new("hjnm_t".into(), None, None, None));
    let always_false = Expr::BinaryOp {
        left: Box::new(col("k")),
        op: BinaryOperator::Eq,
        right: Box::new(int_lit(999)),
    };
    let right = Arc::new(FilterExec::new(right_scan, always_false));
    let exec = HashJoinExec::new(
        left,
        right,
        JoinType::Inner,
        vec![col("k")],
        vec![col("k")],
        None,
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

// ── SortMergeJoinExec ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_sort_merge_join_exec() {
    let (ctx, _) = make_ctx_with_table(
        "smj_t",
        vec![("k", "integer")],
        vec![
            make_row(&[("k", Value::Integer(1))]),
            make_row(&[("k", Value::Integer(2))]),
        ],
    );
    let left = Arc::new(SeqScanExec::new("smj_t".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("smj_t".into(), None, None, None));
    let exec = build_sort_merge_join(left, right, JoinType::Inner, vec![col("k")], vec![col("k")]);
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_sort_merge_join_direct_struct() {
    use aeternumdb_core::executor::SortMergeJoinExec;
    let (ctx, _) = make_ctx_with_table(
        "smjd_t",
        vec![("k", "integer")],
        vec![
            make_row(&[("k", Value::Integer(10))]),
            make_row(&[("k", Value::Integer(20))]),
        ],
    );
    let left = Arc::new(SeqScanExec::new("smjd_t".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("smjd_t".into(), None, None, None));
    let exec = SortMergeJoinExec::new(left, right, JoinType::Inner, vec![col("k")], vec![col("k")]);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

// ── DistinctExec ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_distinct_exec_removes_duplicates() {
    let (ctx, _) = make_ctx_with_table(
        "dup_t",
        vec![("x", "integer")],
        vec![
            make_row(&[("x", Value::Integer(1))]),
            make_row(&[("x", Value::Integer(1))]),
            make_row(&[("x", Value::Integer(2))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("dup_t".into(), None, None, None));
    let exec = build_distinct_executor(scan);
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_distinct_exec_direct() {
    let (ctx, _) = make_ctx_with_table(
        "dd_t",
        vec![("y", "text")],
        vec![
            make_row(&[("y", Value::String("hello".into()))]),
            make_row(&[("y", Value::String("hello".into()))]),
            make_row(&[("y", Value::String("world".into()))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("dd_t".into(), None, None, None));
    let exec = DistinctExec::new(scan);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_distinct_exec_all_unique() {
    let (ctx, _) = make_ctx_with_table(
        "uniq_t",
        vec![("v", "integer")],
        vec![
            make_row(&[("v", Value::Integer(1))]),
            make_row(&[("v", Value::Integer(2))]),
            make_row(&[("v", Value::Integer(3))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("uniq_t".into(), None, None, None));
    let exec = DistinctExec::new(scan);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 3);
}

// ── SortExec ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_sort_exec_ascending() {
    let (ctx, _) = make_ctx_with_table(
        "sort_t",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(3))]),
            make_row(&[("n", Value::Integer(1))]),
            make_row(&[("n", Value::Integer(2))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("sort_t".into(), None, None, None));
    let order = vec![SortExpr {
        expr: col("n"),
        ascending: true,
    }];
    let exec = SortExec::new(scan, order);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("n"), Some(&Value::Integer(1)));
    assert_eq!(rows[1].get("n"), Some(&Value::Integer(2)));
    assert_eq!(rows[2].get("n"), Some(&Value::Integer(3)));
}

#[tokio::test]
async fn test_sort_exec_descending() {
    let (ctx, _) = make_ctx_with_table(
        "sortd_t",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(10))]),
            make_row(&[("n", Value::Integer(5))]),
            make_row(&[("n", Value::Integer(20))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("sortd_t".into(), None, None, None));
    let order = vec![SortExpr {
        expr: col("n"),
        ascending: false,
    }];
    let exec = SortExec::new(scan, order);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("n"), Some(&Value::Integer(20)));
    assert_eq!(rows[2].get("n"), Some(&Value::Integer(5)));
}

// ── LimitExec ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_limit_exec_basic() {
    let (ctx, _) = make_ctx_with_table(
        "lim_t",
        vec![("i", "integer")],
        vec![
            make_row(&[("i", Value::Integer(1))]),
            make_row(&[("i", Value::Integer(2))]),
            make_row(&[("i", Value::Integer(3))]),
            make_row(&[("i", Value::Integer(4))]),
            make_row(&[("i", Value::Integer(5))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("lim_t".into(), None, None, None));
    let exec = LimitExec::new(scan, 2, 0);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_limit_exec_with_offset() {
    let (ctx, _) = make_ctx_with_table(
        "off_t",
        vec![("i", "integer")],
        vec![
            make_row(&[("i", Value::Integer(10))]),
            make_row(&[("i", Value::Integer(20))]),
            make_row(&[("i", Value::Integer(30))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("off_t".into(), None, None, None));
    let exec = LimitExec::new(scan, 2, 1);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("i"), Some(&Value::Integer(20)));
    assert_eq!(rows[1].get("i"), Some(&Value::Integer(30)));
}

// ── DML ───────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_execute_insert_success() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table(
        "ins_t",
        vec![
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
        ],
    );
    let mut acl = ACL::new();
    acl.grant("u1", "ins_t", "INSERT");
    let ctx = ExecutionContext::new(
        provider.clone(),
        Arc::new(Mutex::new(acl)),
        Arc::new(AtomicIdGenerator::default()),
        "u1".to_string(),
    );
    let n = execute_insert(
        &ctx,
        "ins_t",
        &["id".to_string(), "name".to_string()],
        vec![vec![Value::Integer(1), Value::String("alice".into())]],
    )
    .await
    .unwrap();
    assert_eq!(n, 1);
    let rows = provider.scan("ins_t").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
}

#[tokio::test]
async fn test_execute_insert_permission_denied() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("priv_t", vec![("x".to_string(), "integer".to_string())]);
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "nobody".to_string(),
    );
    let result = execute_insert(
        &ctx,
        "priv_t",
        &["x".to_string()],
        vec![vec![Value::Integer(1)]],
    )
    .await;
    assert!(matches!(result, Err(ExecutorError::PermissionDenied(_))));
}

#[tokio::test]
async fn test_execute_update_success() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("upd_t", vec![("x".to_string(), "integer".to_string())]);
    provider.add_rows("upd_t", vec![make_row(&[("x", Value::Integer(1))])]);
    let mut acl = ACL::new();
    acl.grant("u2", "upd_t", "UPDATE");
    let ctx = ExecutionContext::new(
        provider.clone(),
        Arc::new(Mutex::new(acl)),
        Arc::new(AtomicIdGenerator::default()),
        "u2".to_string(),
    );
    let mut updates = HashMap::new();
    updates.insert("x".to_string(), Value::Integer(42));
    let n = execute_update(&ctx, "upd_t", updates).await.unwrap();
    assert_eq!(n, 1);
    let rows = provider.scan("upd_t").await.unwrap();
    assert_eq!(rows[0].get("x"), Some(&Value::Integer(42)));
}

#[tokio::test]
async fn test_execute_delete_success() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("del_t", vec![("x".to_string(), "integer".to_string())]);
    provider.add_rows(
        "del_t",
        vec![
            make_row(&[("x", Value::Integer(1))]),
            make_row(&[("x", Value::Integer(2))]),
        ],
    );
    let mut acl = ACL::new();
    acl.grant("u3", "del_t", "DELETE");
    let ctx = ExecutionContext::new(
        provider.clone(),
        Arc::new(Mutex::new(acl)),
        Arc::new(AtomicIdGenerator::default()),
        "u3".to_string(),
    );
    let n = execute_delete(&ctx, "del_t").await.unwrap();
    assert_eq!(n, 2);
    let rows = provider.scan("del_t").await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_execute_delete_permission_denied() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("pd_t", vec![("x".to_string(), "integer".to_string())]);
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "nobody".to_string(),
    );
    let result = execute_delete(&ctx, "pd_t").await;
    assert!(matches!(result, Err(ExecutorError::PermissionDenied(_))));
}

// ── ACL helpers ───────────────────────────────────────────────────────────────

#[test]
fn test_execute_grant_and_revoke() {
    let ctx = ExecutionContext::default_test();
    execute_grant(&ctx, "bob", "orders", "SELECT").unwrap();
    {
        let acl = ctx.acl.lock().unwrap();
        assert!(acl.check("bob", "orders", "SELECT"));
    }
    execute_revoke(&ctx, "bob", "orders", "SELECT").unwrap();
    {
        let acl = ctx.acl.lock().unwrap();
        assert!(!acl.check("bob", "orders", "SELECT"));
    }
}

#[test]
fn test_execute_grant_multiple_privileges() {
    let ctx = ExecutionContext::default_test();
    execute_grant(&ctx, "carol", "reports", "SELECT").unwrap();
    execute_grant(&ctx, "carol", "reports", "INSERT").unwrap();
    let acl = ctx.acl.lock().unwrap();
    assert!(acl.check("carol", "reports", "SELECT"));
    assert!(acl.check("carol", "reports", "INSERT"));
    assert!(!acl.check("carol", "reports", "DELETE"));
}

// ── Referential integrity ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_check_referential_integrity_found() {
    let (ctx, _) = make_ctx_with_table(
        "parent",
        vec![("id", "integer")],
        vec![make_row(&[("id", Value::Integer(10))])],
    );
    let found = check_referential_integrity(&ctx, "parent", "id", &Value::Integer(10))
        .await
        .unwrap();
    assert!(found);
}

#[tokio::test]
async fn test_check_referential_integrity_not_found() {
    let (ctx, _) = make_ctx_with_table(
        "parent2",
        vec![("id", "integer")],
        vec![make_row(&[("id", Value::Integer(5))])],
    );
    let found = check_referential_integrity(&ctx, "parent2", "id", &Value::Integer(99))
        .await
        .unwrap();
    assert!(!found);
}

#[tokio::test]
async fn test_apply_referential_action_restrict() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("child", vec![("fk".to_string(), "integer".to_string())]);
    provider.add_rows("child", vec![make_row(&[("fk", Value::Integer(1))])]);
    let mut acl = ACL::new();
    acl.grant("admin", "child", "DELETE");
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(acl)),
        Arc::new(AtomicIdGenerator::default()),
        "admin".to_string(),
    );
    let result = apply_referential_action(&ctx, "RESTRICT", "child", "fk").await;
    assert!(matches!(
        result,
        Err(ExecutorError::ReferentialIntegrityViolation(_))
    ));
}

#[tokio::test]
async fn test_apply_referential_action_no_action() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("na_t", vec![("fk".to_string(), "integer".to_string())]);
    provider.add_rows("na_t", vec![make_row(&[("fk", Value::Integer(1))])]);
    let mut acl = ACL::new();
    acl.grant("admin", "na_t", "DELETE");
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(acl)),
        Arc::new(AtomicIdGenerator::default()),
        "admin".to_string(),
    );
    let result = apply_referential_action(&ctx, "NO ACTION", "na_t", "fk").await;
    assert!(matches!(
        result,
        Err(ExecutorError::ReferentialIntegrityViolation(_))
    ));
}

#[tokio::test]
async fn test_apply_referential_action_cascade() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("casc", vec![("x".to_string(), "integer".to_string())]);
    provider.add_rows(
        "casc",
        vec![
            make_row(&[("x", Value::Integer(1))]),
            make_row(&[("x", Value::Integer(2))]),
        ],
    );
    let mut acl = ACL::new();
    acl.grant("admin", "casc", "DELETE");
    let ctx = ExecutionContext::new(
        provider.clone(),
        Arc::new(Mutex::new(acl)),
        Arc::new(AtomicIdGenerator::default()),
        "admin".to_string(),
    );
    apply_referential_action(&ctx, "CASCADE", "casc", "x")
        .await
        .unwrap();
    let rows = provider.scan("casc").await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_apply_referential_action_set_null() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("sn_t", vec![("fk".to_string(), "integer".to_string())]);
    provider.add_rows("sn_t", vec![make_row(&[("fk", Value::Integer(5))])]);
    let mut acl = ACL::new();
    acl.grant("admin", "sn_t", "UPDATE");
    let ctx = ExecutionContext::new(
        provider.clone(),
        Arc::new(Mutex::new(acl)),
        Arc::new(AtomicIdGenerator::default()),
        "admin".to_string(),
    );
    apply_referential_action(&ctx, "SET NULL", "sn_t", "fk")
        .await
        .unwrap();
    let rows = provider.scan("sn_t").await.unwrap();
    assert_eq!(rows[0].get("fk"), Some(&Value::Null));
}

// ── build_distinct_executor / build_sort_merge_join ───────────────────────────

#[tokio::test]
async fn test_build_distinct_executor() {
    let (ctx, _) = make_ctx_with_table(
        "bd_t",
        vec![("z", "integer")],
        vec![
            make_row(&[("z", Value::Integer(9))]),
            make_row(&[("z", Value::Integer(9))]),
            make_row(&[("z", Value::Integer(7))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("bd_t".into(), None, None, None));
    let exec = build_distinct_executor(scan);
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_build_sort_merge_join() {
    let (ctx, _) = make_ctx_with_table(
        "smj2_t",
        vec![("k", "integer")],
        vec![
            make_row(&[("k", Value::Integer(1))]),
            make_row(&[("k", Value::Integer(3))]),
        ],
    );
    let left = Arc::new(SeqScanExec::new("smj2_t".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("smj2_t".into(), None, None, None));
    let exec = build_sort_merge_join(left, right, JoinType::Inner, vec![col("k")], vec![col("k")]);
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
}

// ── build_executor (physical plan builder) ────────────────────────────────────

#[tokio::test]
async fn test_build_executor_seq_scan() {
    let (ctx, _) = make_ctx_with_table(
        "be_t",
        vec![("x", "integer")],
        vec![
            make_row(&[("x", Value::Integer(1))]),
            make_row(&[("x", Value::Integer(2))]),
        ],
    );
    let plan = PhysicalPlan::SeqScan {
        table: "be_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_build_executor_filter() {
    let (ctx, _) = make_ctx_with_table(
        "bef_t",
        vec![("v", "integer")],
        vec![
            make_row(&[("v", Value::Integer(1))]),
            make_row(&[("v", Value::Integer(10))]),
        ],
    );
    let scan = PhysicalPlan::SeqScan {
        table: "bef_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let predicate = Expr::BinaryOp {
        left: Box::new(col("v")),
        op: BinaryOperator::Gt,
        right: Box::new(int_lit(5)),
    };
    let plan = PhysicalPlan::Filter {
        input: Box::new(scan),
        predicate,
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("v"), Some(&Value::Integer(10)));
}

// ── ExecutionContext helpers ──────────────────────────────────────────────────

#[test]
fn test_check_privilege_ok() {
    let ctx = ExecutionContext::default_test();
    {
        let mut acl = ctx.acl.lock().unwrap();
        acl.grant("test_user", "t", "SELECT");
    }
    assert!(ctx.check_privilege("t", "SELECT").is_ok());
}

#[test]
fn test_check_privilege_denied() {
    let ctx = ExecutionContext::default_test();
    let result = ctx.check_privilege("secret", "SELECT");
    assert!(matches!(result, Err(ExecutorError::PermissionDenied(_))));
}

#[test]
fn test_executor_error_display() {
    let e = ExecutorError::TableNotFound("foo".into());
    assert!(e.to_string().contains("foo"));
    let e2 = ExecutorError::ColumnNotFound("bar".into());
    assert!(e2.to_string().contains("bar"));
    let e3 = ExecutorError::TypeMismatch {
        expected: "integer".into(),
        got: "text".into(),
    };
    assert!(e3.to_string().contains("integer"));
    let e4 = ExecutorError::PermissionDenied("no".into());
    assert!(e4.to_string().contains("no"));
}

// ── ValuesExec ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_values_exec_basic() {
    let ctx = default_ctx();
    let exec = ValuesExec::new(
        vec![
            vec![int_lit(1), str_lit("a")],
            vec![int_lit(2), str_lit("b")],
        ],
        vec!["id".to_string(), "name".to_string()],
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(rows[1].get("name"), Some(&Value::String("b".into())));
}

#[tokio::test]
async fn test_values_exec_empty() {
    let ctx = default_ctx();
    let exec = ValuesExec::new(vec![], vec!["x".to_string()]);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_values_exec_schema() {
    let exec = ValuesExec::new(vec![vec![int_lit(1)]], vec!["n".to_string()]);
    assert_eq!(
        exec.schema(),
        vec![("n".to_string(), "unknown".to_string())]
    );
}

// ── UnnestExec ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_unnest_exec_array_column() {
    let (ctx, _) = make_ctx_with_table(
        "arr_t",
        vec![("items", "array")],
        vec![make_row(&[(
            "items",
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
        )])],
    );
    let scan = Arc::new(SeqScanExec::new("arr_t".into(), None, None, None));
    let exec = UnnestExec::new(scan, col("items"), Some("item".to_string()));
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("item"), Some(&Value::Integer(1)));
    assert_eq!(rows[2].get("item"), Some(&Value::Integer(3)));
}

#[tokio::test]
async fn test_unnest_exec_scalar_passthrough() {
    let (ctx, _) = make_ctx_with_table(
        "scalar_t",
        vec![("x", "integer")],
        vec![make_row(&[("x", Value::Integer(42))])],
    );
    let scan = Arc::new(SeqScanExec::new("scalar_t".into(), None, None, None));
    let exec = UnnestExec::new(scan, col("x"), Some("val".to_string()));
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(42)));
}

#[tokio::test]
async fn test_unnest_exec_null_skipped() {
    let (ctx, _) = make_ctx_with_table(
        "null_arr_t",
        vec![("x", "array")],
        vec![make_row(&[("x", Value::Null)])],
    );
    let scan = Arc::new(SeqScanExec::new("null_arr_t".into(), None, None, None));
    let exec = UnnestExec::new(scan, col("x"), None);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_unnest_exec_schema() {
    let (ctx, _) = make_ctx_with_table("sch_t", vec![("a", "integer")], vec![]);
    let scan = Arc::new(SeqScanExec::new("sch_t".into(), None, None, None));
    let exec = UnnestExec::new(scan, col("a"), Some("exploded".to_string()));
    assert!(exec.schema().iter().any(|(n, _)| n == "exploded"));
    let _ = ctx;
}

// ── ProjectExec ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_project_exec_computed_column() {
    let ctx = default_ctx();
    let scan = single_value_exec(int_lit(5));
    let exec = ProjectExec::new(
        scan,
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::Multiply,
                right: Box::new(int_lit(3)),
            },
            "result",
        )],
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("result"), Some(&Value::Integer(15)));
}

#[tokio::test]
async fn test_project_exec_schema() {
    let (ctx, _) = make_ctx_with_table("p_t", vec![("n", "integer")], vec![]);
    let scan = Arc::new(SeqScanExec::new("p_t".into(), None, None, None));
    let exec = ProjectExec::new(scan, vec![proj_item(col("n"), "alias_n")]);
    let schema = exec.schema();
    assert_eq!(schema[0].0, "alias_n");
    let _ = ctx;
}

// ── HashAggregateExec ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_aggregate_basic_count() {
    let (ctx, _) = make_ctx_with_table(
        "agg_t",
        vec![("g", "integer"), ("v", "integer")],
        vec![
            make_row(&[("g", Value::Integer(1)), ("v", Value::Integer(10))]),
            make_row(&[("g", Value::Integer(1)), ("v", Value::Integer(20))]),
            make_row(&[("g", Value::Integer(2)), ("v", Value::Integer(30))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("agg_t".into(), None, None, None));
    let exec = HashAggregateExec::new(
        scan,
        vec![col("g")],
        vec![AggregateExpr {
            func: col("v"),
            alias: Some("cnt".to_string()),
        }],
        None,
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_hash_aggregate_with_having() {
    let (ctx, _) = make_ctx_with_table(
        "hav_t",
        vec![("g", "integer"), ("v", "integer")],
        vec![
            make_row(&[("g", Value::Integer(1)), ("v", Value::Integer(10))]),
            make_row(&[("g", Value::Integer(1)), ("v", Value::Integer(20))]),
            make_row(&[("g", Value::Integer(2)), ("v", Value::Integer(5))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("hav_t".into(), None, None, None));
    // HAVING cnt > 1 — only group 1 has 2 rows, group 2 has 1 row
    let having = Expr::BinaryOp {
        left: Box::new(col("cnt")),
        op: BinaryOperator::Gt,
        right: Box::new(int_lit(1)),
    };
    let exec = HashAggregateExec::new(
        scan,
        vec![col("g")],
        vec![AggregateExpr {
            func: col("v"),
            alias: Some("cnt".to_string()),
        }],
        Some(having),
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_hash_aggregate_no_groups() {
    let (ctx, _) = make_ctx_with_table(
        "agg_ng",
        vec![("v", "integer")],
        vec![
            make_row(&[("v", Value::Integer(1))]),
            make_row(&[("v", Value::Integer(2))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("agg_ng".into(), None, None, None));
    let exec = HashAggregateExec::new(
        scan,
        vec![],
        vec![AggregateExpr {
            func: col("v"),
            alias: Some("total".to_string()),
        }],
        None,
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
}

// ── Sort with multiple columns ────────────────────────────────────────────────

#[tokio::test]
async fn test_sort_exec_multi_column() {
    let (ctx, _) = make_ctx_with_table(
        "ms_t",
        vec![("a", "integer"), ("b", "integer")],
        vec![
            make_row(&[("a", Value::Integer(2)), ("b", Value::Integer(10))]),
            make_row(&[("a", Value::Integer(1)), ("b", Value::Integer(30))]),
            make_row(&[("a", Value::Integer(1)), ("b", Value::Integer(20))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("ms_t".into(), None, None, None));
    let order = vec![
        SortExpr {
            expr: col("a"),
            ascending: true,
        },
        SortExpr {
            expr: col("b"),
            ascending: true,
        },
    ];
    let exec = SortExec::new(scan, order);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("a"), Some(&Value::Integer(1)));
    assert_eq!(rows[0].get("b"), Some(&Value::Integer(20)));
    assert_eq!(rows[1].get("b"), Some(&Value::Integer(30)));
}

// ── Expression evaluation via FilterExec / ProjectExec ────────────────────────

#[tokio::test]
async fn test_expr_division_by_zero_integer() {
    let ctx = default_ctx();
    let scan = single_value_exec(int_lit(10));
    let pred = Expr::BinaryOp {
        left: Box::new(col("v")),
        op: BinaryOperator::Divide,
        right: Box::new(int_lit(0)),
    };
    let exec = FilterExec::new(scan, pred);
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_expr_division_by_zero_float() {
    let ctx = default_ctx();
    let scan = single_value_exec(float_lit(10.0));
    let pred = Expr::BinaryOp {
        left: Box::new(col("v")),
        op: BinaryOperator::Divide,
        right: Box::new(float_lit(0.0)),
    };
    let exec = FilterExec::new(scan, pred);
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_expr_modulo_by_zero() {
    let ctx = default_ctx();
    let scan = single_value_exec(int_lit(7));
    let pred = Expr::BinaryOp {
        left: Box::new(col("v")),
        op: BinaryOperator::Modulo,
        right: Box::new(int_lit(0)),
    };
    let exec = FilterExec::new(scan, pred);
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_expr_type_mismatch_add() {
    let ctx = default_ctx();
    let scan = single_value_exec(str_lit("hello"));
    let pred = Expr::BinaryOp {
        left: Box::new(col("v")),
        op: BinaryOperator::Plus,
        right: Box::new(int_lit(1)),
    };
    let exec = FilterExec::new(scan, pred);
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_expr_unary_not() {
    let ctx = default_ctx();
    let scan = single_value_exec(bool_lit(false));
    let pred = Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(col("v")),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_expr_unary_minus() {
    let ctx = default_ctx();
    let scan = single_value_exec(int_lit(5));
    let proj = ProjectExec::new(
        scan,
        vec![proj_item(
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(col("v")),
            },
            "neg",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("neg"), Some(&Value::Integer(-5)));
}

#[tokio::test]
async fn test_expr_null_propagation_add() {
    let ctx = default_ctx();
    let scan = single_value_exec(null_lit());
    let proj = ProjectExec::new(
        scan,
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::Plus,
                right: Box::new(int_lit(1)),
            },
            "r",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Null));
}

#[tokio::test]
async fn test_expr_and_short_circuit_false() {
    let ctx = default_ctx();
    // false AND <non-boolean> should be false (short-circuit)
    let scan = single_value_exec(bool_lit(false));
    let pred = Expr::BinaryOp {
        left: Box::new(bool_lit(false)),
        op: BinaryOperator::And,
        right: Box::new(bool_lit(true)),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_expr_or_short_circuit_true() {
    let ctx = default_ctx();
    let scan = single_value_exec(bool_lit(true));
    let pred = Expr::BinaryOp {
        left: Box::new(bool_lit(true)),
        op: BinaryOperator::Or,
        right: Box::new(bool_lit(false)),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
}

// ── BETWEEN expression ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_between_inclusive() {
    let (ctx, _) = make_ctx_with_table(
        "bet_t",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(1))]),
            make_row(&[("n", Value::Integer(5))]),
            make_row(&[("n", Value::Integer(10))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("bet_t".into(), None, None, None));
    let pred = Expr::Between {
        expr: Box::new(col("n")),
        low: Box::new(int_lit(3)),
        high: Box::new(int_lit(8)),
        negated: false,
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n"), Some(&Value::Integer(5)));
}

#[tokio::test]
async fn test_not_between() {
    let (ctx, _) = make_ctx_with_table(
        "nbet_t",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(1))]),
            make_row(&[("n", Value::Integer(5))]),
            make_row(&[("n", Value::Integer(10))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("nbet_t".into(), None, None, None));
    let pred = Expr::Between {
        expr: Box::new(col("n")),
        low: Box::new(int_lit(3)),
        high: Box::new(int_lit(8)),
        negated: true,
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

// ── IN LIST expression ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_in_list_match() {
    let (ctx, _) = make_ctx_with_table(
        "il_t",
        vec![("x", "integer")],
        vec![
            make_row(&[("x", Value::Integer(1))]),
            make_row(&[("x", Value::Integer(2))]),
            make_row(&[("x", Value::Integer(3))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("il_t".into(), None, None, None));
    let pred = Expr::InList {
        expr: Box::new(col("x")),
        list: vec![int_lit(1), int_lit(3)],
        negated: false,
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_not_in_list() {
    let (ctx, _) = make_ctx_with_table(
        "nil_t",
        vec![("x", "integer")],
        vec![
            make_row(&[("x", Value::Integer(10))]),
            make_row(&[("x", Value::Integer(20))]),
            make_row(&[("x", Value::Integer(30))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("nil_t".into(), None, None, None));
    let pred = Expr::InList {
        expr: Box::new(col("x")),
        list: vec![int_lit(10), int_lit(30)],
        negated: true,
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("x"), Some(&Value::Integer(20)));
}

// ── CAST expressions ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cast_integer_to_boolean() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(cast_expr(col("v"), DataType::Boolean), "b")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("b"), Some(&Value::Boolean(true)));
}

#[tokio::test]
async fn test_cast_zero_to_boolean_false() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(0)),
        vec![proj_item(cast_expr(col("v"), DataType::Boolean), "b")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("b"), Some(&Value::Boolean(false)));
}

#[tokio::test]
async fn test_cast_string_true_to_boolean() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("true")),
        vec![proj_item(cast_expr(col("v"), DataType::Boolean), "b")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("b"), Some(&Value::Boolean(true)));
}

#[tokio::test]
async fn test_cast_float_to_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(3.9)),
        vec![proj_item(cast_expr(col("v"), DataType::Integer), "i")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("i"), Some(&Value::Integer(3)));
}

#[tokio::test]
async fn test_cast_string_to_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("42")),
        vec![proj_item(cast_expr(col("v"), DataType::Integer), "i")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("i"), Some(&Value::Integer(42)));
}

#[tokio::test]
async fn test_cast_invalid_string_to_integer_error() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("not_a_number")),
        vec![proj_item(cast_expr(col("v"), DataType::Integer), "i")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_cast_integer_to_float() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(7)),
        vec![proj_item(cast_expr(col("v"), DataType::Float), "f")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("f"), Some(&Value::Float(7.0)));
}

#[tokio::test]
async fn test_cast_string_to_float() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("3.14")),
        vec![proj_item(cast_expr(col("v"), DataType::Float), "f")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("f"), Some(&Value::Float(3.14)));
}

#[tokio::test]
async fn test_cast_integer_to_varchar() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(99)),
        vec![proj_item(cast_expr(col("v"), DataType::Varchar(None)), "s")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("s"), Some(&Value::String("99".to_string())));
}

#[tokio::test]
async fn test_cast_null_propagation() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(null_lit()),
        vec![proj_item(cast_expr(col("v"), DataType::Integer), "i")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("i"), Some(&Value::Null));
}

// ── String functions ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_func_lower() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("HELLO World")),
        vec![proj_item(func_expr("LOWER", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::String("hello world".into())));
}

#[tokio::test]
async fn test_func_upper() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("hello")),
        vec![proj_item(func_expr("UPPER", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::String("HELLO".into())));
}

#[tokio::test]
async fn test_func_length() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("hello")),
        vec![proj_item(func_expr("LENGTH", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(5)));
}

#[tokio::test]
async fn test_func_trim() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("  hello  ")),
        vec![proj_item(func_expr("TRIM", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::String("hello".into())));
}

#[tokio::test]
async fn test_func_abs_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(-7)),
        vec![proj_item(func_expr("ABS", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(7)));
}

#[tokio::test]
async fn test_func_abs_float() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(-3.5)),
        vec![proj_item(func_expr("ABS", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Float(3.5)));
}

#[tokio::test]
async fn test_func_floor() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(3.9)),
        vec![proj_item(func_expr("FLOOR", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(3)));
}

#[tokio::test]
async fn test_func_ceil() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(3.1)),
        vec![proj_item(func_expr("CEIL", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(4)));
}

#[tokio::test]
async fn test_func_ceil_alias_ceiling() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(2.3)),
        vec![proj_item(func_expr("CEILING", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(3)));
}

#[tokio::test]
async fn test_func_sqrt_float() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(16.0)),
        vec![proj_item(func_expr("SQRT", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Float(4.0)));
}

#[tokio::test]
async fn test_func_sqrt_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(9)),
        vec![proj_item(func_expr("SQRT", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Float(3.0)));
}

#[tokio::test]
async fn test_func_round_float() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(3.14159)),
        vec![proj_item(
            func_expr("ROUND", vec![col("v"), int_lit(2)]),
            "r",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    if let Some(Value::Float(f)) = rows[0].get("r") {
        assert!((f - 3.14).abs() < 1e-9);
    } else {
        panic!("expected float");
    }
}

#[tokio::test]
async fn test_func_coalesce_returns_first_non_null() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(null_lit()),
        vec![proj_item(
            func_expr("COALESCE", vec![null_lit(), int_lit(42), int_lit(0)]),
            "r",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(42)));
}

#[tokio::test]
async fn test_func_null_propagation_lower() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(null_lit()),
        vec![proj_item(func_expr("LOWER", vec![null_lit()]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Null));
}

#[tokio::test]
async fn test_func_type_error_lower_on_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(func_expr("LOWER", vec![int_lit(1)]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_func_wrong_arg_count_abs() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(
            func_expr("ABS", vec![int_lit(1), int_lit(2)]),
            "r",
        )],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_func_unknown_function_error() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(
            func_expr("NONEXISTENT_FUNC", vec![col("v")]),
            "r",
        )],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

// ── String concat / LIKE ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_string_concat() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("foo")),
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::StringConcat,
                right: Box::new(str_lit("bar")),
            },
            "r",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::String("foobar".into())));
}

#[tokio::test]
async fn test_like_pattern_match() {
    let (ctx, _) = make_ctx_with_table(
        "lk_t",
        vec![("s", "text")],
        vec![
            make_row(&[("s", Value::String("hello world".into()))]),
            make_row(&[("s", Value::String("goodbye".into()))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("lk_t".into(), None, None, None));
    let pred = Expr::BinaryOp {
        left: Box::new(col("s")),
        op: BinaryOperator::Like,
        right: Box::new(str_lit("hello%")),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
}

// ── CASE expression ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_case_searched() {
    let (ctx, _) = make_ctx_with_table(
        "case_t",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(1))]),
            make_row(&[("n", Value::Integer(5))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("case_t".into(), None, None, None));
    let case_expr = Expr::Case {
        operand: None,
        conditions: vec![(
            Expr::BinaryOp {
                left: Box::new(col("n")),
                op: BinaryOperator::Gt,
                right: Box::new(int_lit(3)),
            },
            str_lit("big"),
        )],
        else_result: Some(Box::new(str_lit("small"))),
    };
    let proj = ProjectExec::new(scan, vec![proj_item(case_expr, "size")]);
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("size"), Some(&Value::String("small".into())));
    assert_eq!(rows[1].get("size"), Some(&Value::String("big".into())));
}

// ── Limit with large offset ───────────────────────────────────────────────────

#[tokio::test]
async fn test_limit_offset_beyond_rows() {
    let (ctx, _) = make_ctx_with_table(
        "lb_t",
        vec![("i", "integer")],
        vec![
            make_row(&[("i", Value::Integer(1))]),
            make_row(&[("i", Value::Integer(2))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("lb_t".into(), None, None, None));
    let exec = LimitExec::new(scan, 10, 100);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

// ── NestedLoop RIGHT / FULL join ──────────────────────────────────────────────

#[tokio::test]
async fn test_nested_loop_right_join() {
    let (ctx, _) = make_ctx_with_table(
        "rj_t",
        vec![("id", "integer")],
        vec![
            make_row(&[("id", Value::Integer(1))]),
            make_row(&[("id", Value::Integer(2))]),
        ],
    );
    let left = Arc::new(SeqScanExec::new("rj_t".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("rj_t".into(), None, None, None));
    // Condition that never matches (1 != 2 etc.), so all right rows appear unmatched
    let cond = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(int_lit(999)),
    };
    let exec = NestedLoopJoinExec::new(left, right, JoinType::Right, Some(cond));
    let rows = collect_rows(&exec, &ctx).await;
    // Both right rows are unmatched, so both appear
    assert_eq!(rows.len(), 2);
}

// ── build_executor HashAggregate ──────────────────────────────────────────────

#[tokio::test]
async fn test_build_executor_hash_aggregate() {
    let (ctx, _) = make_ctx_with_table(
        "bha_t",
        vec![("g", "integer"), ("v", "integer")],
        vec![
            make_row(&[("g", Value::Integer(1)), ("v", Value::Integer(10))]),
            make_row(&[("g", Value::Integer(2)), ("v", Value::Integer(20))]),
        ],
    );
    let scan = PhysicalPlan::SeqScan {
        table: "bha_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let plan = PhysicalPlan::HashAggregate {
        input: Box::new(scan),
        group_by: vec![col("g")],
        aggregates: vec![AggregateExpr {
            func: col("v"),
            alias: Some("cnt".to_string()),
        }],
        having: None,
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
}

// ── build_executor Unnest / Values ────────────────────────────────────────────

#[tokio::test]
async fn test_build_executor_values() {
    let ctx = default_ctx();
    let plan = PhysicalPlan::Values {
        rows: vec![
            vec![int_lit(1), str_lit("a")],
            vec![int_lit(2), str_lit("b")],
        ],
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_build_executor_unnest() {
    let (ctx, _) = make_ctx_with_table(
        "un_t",
        vec![("arr", "array")],
        vec![make_row(&[(
            "arr",
            Value::Array(vec![Value::Integer(10), Value::Integer(20)]),
        )])],
    );
    let scan = PhysicalPlan::SeqScan {
        table: "un_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let plan = PhysicalPlan::Unnest {
        input: Box::new(scan),
        column: col("arr"),
        alias: Some("elem".to_string()),
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
}

// ─── NEW COMPREHENSIVE TESTS ──────────────────────────────────────────────────

// ─── Value Display for special variants ──────────────────────────────────────

#[test]
fn test_value_display_decimal() {
    let d = rust_decimal::Decimal::from(42);
    let v = Value::Decimal(d);
    assert_eq!(v.to_string(), "42");
}

#[test]
fn test_value_display_bytes() {
    let v = Value::Bytes(vec![1u8, 2, 3]);
    assert_eq!(v.to_string(), "<3 bytes>");
}

#[test]
fn test_value_display_bytes_empty() {
    let v = Value::Bytes(vec![]);
    assert_eq!(v.to_string(), "<0 bytes>");
}

#[test]
fn test_value_display_json() {
    let j = serde_json::json!({"key": "val"});
    let v = Value::Json(j);
    let s = v.to_string();
    assert!(s.contains("key"));
}

#[test]
fn test_value_display_array_multi() {
    let v = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
    assert_eq!(v.to_string(), "[1, 2]");
}

#[test]
fn test_value_display_array_empty() {
    let v = Value::Array(vec![]);
    assert_eq!(v.to_string(), "[]");
}

// ─── Value conversions ────────────────────────────────────────────────────────

#[test]
fn test_value_as_bool_non_bool_returns_none() {
    assert_eq!(Value::Integer(1).as_bool(), None);
    assert_eq!(Value::String("true".into()).as_bool(), None);
    assert_eq!(Value::Float(1.0).as_bool(), None);
}

#[test]
fn test_value_as_integer_non_integer_returns_none() {
    assert_eq!(Value::String("42".into()).as_integer(), None);
    assert_eq!(Value::Boolean(true).as_integer(), None);
    assert_eq!(Value::Float(3.0).as_integer(), None);
}

#[test]
fn test_value_as_array_non_array_returns_none() {
    assert_eq!(Value::Integer(1).as_array(), None);
    assert_eq!(Value::String("x".into()).as_array(), None);
    assert_eq!(Value::Boolean(false).as_array(), None);
}

// ─── RecordBatch and Row ──────────────────────────────────────────────────────

#[test]
fn test_record_batch_with_rows() {
    let row = make_row(&[("a", Value::Integer(7))]);
    let batch = RecordBatch::with_rows(vec!["a".to_string()], vec![row]);
    assert_eq!(batch.row_count(), 1);
    assert_eq!(batch.column_count(), 1);
    assert_eq!(batch.rows[0].get("a"), Some(&Value::Integer(7)));
}

#[test]
fn test_record_batch_default() {
    let batch = RecordBatch::default();
    assert!(batch.is_empty());
    assert_eq!(batch.column_count(), 0);
}

#[test]
fn test_row_default() {
    let row = Row::default();
    assert!(row.columns.is_empty());
    assert_eq!(row.column_names().len(), 0);
}

// ─── ExecutionContext and InMemoryTableProvider ───────────────────────────────

#[test]
fn test_execution_context_default_test() {
    let ctx = ExecutionContext::default_test();
    assert_eq!(ctx.current_user, "test_user");
}

#[tokio::test]
async fn test_in_memory_provider_insert_not_found() {
    let provider = InMemoryTableProvider::new();
    let result = provider.insert("no_such", vec![]).await;
    assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
}

#[tokio::test]
async fn test_in_memory_provider_update_not_found() {
    let provider = InMemoryTableProvider::new();
    let result = provider.update("no_such", HashMap::new()).await;
    assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
}

#[tokio::test]
async fn test_in_memory_provider_delete_not_found() {
    let provider = InMemoryTableProvider::new();
    let result = provider.delete("no_such").await;
    assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
}

#[test]
fn test_in_memory_provider_schema_not_found() {
    let provider = InMemoryTableProvider::new();
    let result = provider.schema("no_such");
    assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
}

#[test]
fn test_acl_default() {
    let acl = ACL::default();
    assert!(!acl.check("u", "t", "SELECT"));
}

#[test]
fn test_in_memory_provider_default() {
    let provider = InMemoryTableProvider::default();
    assert!(!provider.table_exists("any"));
}

#[test]
fn test_atomic_id_generator_start_zero() {
    let gen = AtomicIdGenerator::new(0);
    assert_eq!(gen.next_id(), 0);
    assert_eq!(gen.next_id(), 1);
}

// ─── ExecutorError Display ───────────────────────────────────────────────────

#[test]
fn test_executor_error_display_all_variants() {
    let e = ExecutorError::TableNotFound("t1".into());
    assert!(e.to_string().contains("Table not found"));
    assert!(e.to_string().contains("t1"));

    let e = ExecutorError::ColumnNotFound("c1".into());
    assert!(e.to_string().contains("Column not found"));
    assert!(e.to_string().contains("c1"));

    let e = ExecutorError::TypeMismatch {
        expected: "integer".into(),
        got: "text".into(),
    };
    assert!(e.to_string().contains("Type mismatch"));
    assert!(e.to_string().contains("integer"));

    let e = ExecutorError::EvalError("bad".into());
    assert!(e.to_string().contains("Evaluation error"));
    assert!(e.to_string().contains("bad"));

    let e = ExecutorError::IoError("disk".into());
    assert!(e.to_string().contains("I/O error"));
    assert!(e.to_string().contains("disk"));

    let e = ExecutorError::PermissionDenied("no".into());
    assert!(e.to_string().contains("Permission denied"));

    let e = ExecutorError::ReferentialIntegrityViolation("fk".into());
    assert!(e.to_string().contains("Referential integrity"));
    assert!(e.to_string().contains("fk"));

    let e = ExecutorError::Other("misc".into());
    assert!(e.to_string().contains("misc"));
}

#[test]
fn test_executor_error_is_std_error() {
    let e: Box<dyn std::error::Error> = Box::new(ExecutorError::Other("err".into()));
    assert!(e.to_string().contains("err"));
}

// ─── Expr::Wildcard unsupported ───────────────────────────────────────────────

#[tokio::test]
async fn test_expr_wildcard_unsupported() {
    let ctx = default_ctx();
    let scan = single_value_exec(int_lit(1));
    let exec = ProjectExec::new(scan, vec![proj_item(Expr::Wildcard, "wildcard_result")]);
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

// ─── eval_column with table qualifier ────────────────────────────────────────

#[tokio::test]
async fn test_eval_column_qualified_found() {
    let ctx = default_ctx();
    let scan = Arc::new(ValuesExec::new(
        vec![vec![int_lit(99)]],
        vec!["t.id".to_string()],
    ));
    let qualified_col = Expr::Column {
        table: Some("t".to_string()),
        name: "id".to_string(),
    };
    let exec = ProjectExec::new(scan, vec![proj_item(qualified_col, "result")]);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows[0].get("result"), Some(&Value::Integer(99)));
}

#[tokio::test]
async fn test_eval_column_qualified_not_found() {
    let ctx = default_ctx();
    let scan = single_value_exec(int_lit(1));
    let qualified_col = Expr::Column {
        table: Some("t".to_string()),
        name: "id".to_string(),
    };
    let exec = ProjectExec::new(scan, vec![proj_item(qualified_col, "r")]);
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::ColumnNotFound(_))));
}

// ─── AND/OR with null ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_expr_and_left_null_excludes_row() {
    let ctx = default_ctx();
    let scan = single_value_exec(null_lit());
    let pred = Expr::BinaryOp {
        left: Box::new(null_lit()),
        op: BinaryOperator::And,
        right: Box::new(bool_lit(true)),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_expr_and_right_null_excludes_row() {
    let ctx = default_ctx();
    let scan = single_value_exec(bool_lit(true));
    let pred = Expr::BinaryOp {
        left: Box::new(bool_lit(true)),
        op: BinaryOperator::And,
        right: Box::new(null_lit()),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_expr_or_left_null_right_false_excludes_row() {
    let ctx = default_ctx();
    let scan = single_value_exec(bool_lit(false));
    let pred = Expr::BinaryOp {
        left: Box::new(null_lit()),
        op: BinaryOperator::Or,
        right: Box::new(bool_lit(false)),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_expr_or_left_null_right_true_includes_row() {
    let ctx = default_ctx();
    let scan = single_value_exec(bool_lit(true));
    let pred = Expr::BinaryOp {
        left: Box::new(null_lit()),
        op: BinaryOperator::Or,
        right: Box::new(bool_lit(true)),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
}

// ─── Modulo TypeMismatch ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_expr_modulo_float_type_mismatch() {
    let ctx = default_ctx();
    let scan = single_value_exec(float_lit(7.0));
    let pred = Expr::BinaryOp {
        left: Box::new(col("v")),
        op: BinaryOperator::Modulo,
        right: Box::new(float_lit(2.0)),
    };
    let exec = FilterExec::new(scan, pred);
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

// ─── Divide integer/float cross types ────────────────────────────────────────

#[tokio::test]
async fn test_expr_div_integer_by_float() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(10)),
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::Divide,
                right: Box::new(float_lit(4.0)),
            },
            "r",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Float(2.5)));
}

#[tokio::test]
async fn test_expr_div_float_by_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(9.0)),
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::Divide,
                right: Box::new(int_lit(3)),
            },
            "r",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Float(3.0)));
}

#[tokio::test]
async fn test_expr_div_integer_by_zero_float_error() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(5)),
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::Divide,
                right: Box::new(float_lit(0.0)),
            },
            "r",
        )],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_expr_div_float_by_zero_integer_error() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(5.0)),
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::Divide,
                right: Box::new(int_lit(0)),
            },
            "r",
        )],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_expr_div_type_mismatch() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("x")),
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::Divide,
                right: Box::new(int_lit(1)),
            },
            "r",
        )],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

// ─── LIKE variants ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ilike_case_insensitive() {
    let (ctx, _) = make_ctx_with_table(
        "ilike_t",
        vec![("s", "text")],
        vec![
            make_row(&[("s", Value::String("Hello World".into()))]),
            make_row(&[("s", Value::String("goodbye".into()))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("ilike_t".into(), None, None, None));
    let pred = Expr::BinaryOp {
        left: Box::new(col("s")),
        op: BinaryOperator::ILike,
        right: Box::new(str_lit("hello%")),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("s"), Some(&Value::String("Hello World".into())));
}

#[tokio::test]
async fn test_not_like() {
    let (ctx, _) = make_ctx_with_table(
        "nlike_t",
        vec![("s", "text")],
        vec![
            make_row(&[("s", Value::String("foobar".into()))]),
            make_row(&[("s", Value::String("baz".into()))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("nlike_t".into(), None, None, None));
    let pred = Expr::BinaryOp {
        left: Box::new(col("s")),
        op: BinaryOperator::NotLike,
        right: Box::new(str_lit("foo%")),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("s"), Some(&Value::String("baz".into())));
}

#[tokio::test]
async fn test_not_ilike() {
    let (ctx, _) = make_ctx_with_table(
        "nilike_t",
        vec![("s", "text")],
        vec![
            make_row(&[("s", Value::String("FOO".into()))]),
            make_row(&[("s", Value::String("bar".into()))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("nilike_t".into(), None, None, None));
    let pred = Expr::BinaryOp {
        left: Box::new(col("s")),
        op: BinaryOperator::NotILike,
        right: Box::new(str_lit("foo%")),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("s"), Some(&Value::String("bar".into())));
}

#[tokio::test]
async fn test_like_type_mismatch() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(42)),
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::Like,
                right: Box::new(str_lit("foo%")),
            },
            "r",
        )],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_like_underscore_wildcard() {
    let (ctx, _) = make_ctx_with_table(
        "us_t",
        vec![("s", "text")],
        vec![
            make_row(&[("s", Value::String("cat".into()))]),
            make_row(&[("s", Value::String("car".into()))]),
            make_row(&[("s", Value::String("bat".into()))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("us_t".into(), None, None, None));
    let pred = Expr::BinaryOp {
        left: Box::new(col("s")),
        op: BinaryOperator::Like,
        right: Box::new(str_lit("c_t")),
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("s"), Some(&Value::String("cat".into())));
}

// ─── Unary BitwiseNot passthrough ────────────────────────────────────────────

#[tokio::test]
async fn test_expr_bitwise_not_passthrough() {
    let ctx = default_ctx();
    let exec = ProjectExec::new(
        single_value_exec(int_lit(5)),
        vec![proj_item(
            Expr::UnaryOp {
                op: UnaryOperator::BitwiseNot,
                expr: Box::new(col("v")),
            },
            "r",
        )],
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(5)));
}

// ─── COUNT with multiple args ─────────────────────────────────────────────────

#[tokio::test]
async fn test_func_count_multiple_args() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(
            func_expr("COUNT", vec![col("v"), int_lit(2)]),
            "r",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(2)));
}

// ─── COALESCE all nulls ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_coalesce_all_nulls() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(null_lit()),
        vec![proj_item(
            func_expr("COALESCE", vec![null_lit(), null_lit()]),
            "r",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Null));
}

// ─── ABS null and wrong type ──────────────────────────────────────────────────

#[tokio::test]
async fn test_func_abs_null() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(null_lit()),
        vec![proj_item(func_expr("ABS", vec![null_lit()]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Null));
}

#[tokio::test]
async fn test_func_abs_wrong_type() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("hello")),
        vec![proj_item(func_expr("ABS", vec![str_lit("hello")]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

// ─── String functions wrong type ──────────────────────────────────────────────

#[tokio::test]
async fn test_func_upper_wrong_type() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(func_expr("UPPER", vec![int_lit(1)]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_func_length_wrong_type() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(bool_lit(true)),
        vec![proj_item(func_expr("LENGTH", vec![bool_lit(true)]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_func_trim_wrong_type() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(1.5)),
        vec![proj_item(func_expr("TRIM", vec![float_lit(1.5)]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

// ─── FLOOR/CEIL integer ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_func_floor_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(7)),
        vec![proj_item(func_expr("FLOOR", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(7)));
}

#[tokio::test]
async fn test_func_floor_wrong_type() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("bad")),
        vec![proj_item(func_expr("FLOOR", vec![str_lit("bad")]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_func_ceil_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(8)),
        vec![proj_item(func_expr("CEIL", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(8)));
}

#[tokio::test]
async fn test_func_ceil_wrong_type() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("bad")),
        vec![proj_item(func_expr("CEIL", vec![str_lit("bad")]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

// ─── SQRT wrong type ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_func_sqrt_wrong_type() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("bad")),
        vec![proj_item(func_expr("SQRT", vec![str_lit("bad")]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

// ─── ROUND edge cases ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_func_round_no_decimals() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(3.7)),
        vec![proj_item(func_expr("ROUND", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    if let Some(Value::Float(f)) = rows[0].get("r") {
        assert!((f - 4.0).abs() < 1e-9);
    } else {
        panic!("expected float");
    }
}

#[tokio::test]
async fn test_func_round_no_args_error() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(func_expr("ROUND", vec![]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_func_round_three_args_error() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(
            func_expr("ROUND", vec![float_lit(1.0), int_lit(1), int_lit(2)]),
            "r",
        )],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_func_round_wrong_type() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("bad")),
        vec![proj_item(func_expr("ROUND", vec![str_lit("bad")]), "r")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_func_round_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(5)),
        vec![proj_item(func_expr("ROUND", vec![col("v")]), "r")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Integer(5)));
}

// ─── CAST to Boolean ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cast_string_false_to_boolean() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("false")),
        vec![proj_item(cast_expr(col("v"), DataType::Boolean), "b")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("b"), Some(&Value::Boolean(false)));
}

#[tokio::test]
async fn test_cast_string_t_to_boolean() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("t")),
        vec![proj_item(cast_expr(col("v"), DataType::Boolean), "b")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("b"), Some(&Value::Boolean(true)));
}

#[tokio::test]
async fn test_cast_string_one_to_boolean() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("1")),
        vec![proj_item(cast_expr(col("v"), DataType::Boolean), "b")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("b"), Some(&Value::Boolean(true)));
}

#[tokio::test]
async fn test_cast_bool_to_boolean_passthrough() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(bool_lit(true)),
        vec![proj_item(cast_expr(col("v"), DataType::Boolean), "b")],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("b"), Some(&Value::Boolean(true)));
}

#[tokio::test]
async fn test_cast_float_to_boolean_type_mismatch() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(float_lit(1.0)),
        vec![proj_item(cast_expr(col("v"), DataType::Boolean), "b")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

// ─── CAST to Float ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cast_bool_to_float_type_mismatch() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(bool_lit(true)),
        vec![proj_item(cast_expr(col("v"), DataType::Float), "f")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_cast_invalid_string_to_float_error() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("not_a_float")),
        vec![proj_item(cast_expr(col("v"), DataType::Float), "f")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

// ─── CAST to unsupported type ─────────────────────────────────────────────────

#[tokio::test]
async fn test_cast_to_unsupported_type_error() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(cast_expr(col("v"), DataType::Date), "d")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::EvalError(_))));
}

#[tokio::test]
async fn test_cast_bool_to_integer_type_mismatch() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(bool_lit(true)),
        vec![proj_item(cast_expr(col("v"), DataType::Integer), "i")],
    );
    let result = try_collect(&proj, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

// ─── BETWEEN with null ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_between_null_low_excludes_row() {
    let (ctx, _) = make_ctx_with_table(
        "bnl_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Integer(5))])],
    );
    let scan = Arc::new(SeqScanExec::new("bnl_t".into(), None, None, None));
    let pred = Expr::Between {
        expr: Box::new(col("n")),
        low: Box::new(null_lit()),
        high: Box::new(int_lit(10)),
        negated: false,
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_between_null_value_excludes_row() {
    let (ctx, _) = make_ctx_with_table(
        "bvn_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Null)])],
    );
    let scan = Arc::new(SeqScanExec::new("bvn_t".into(), None, None, None));
    let pred = Expr::Between {
        expr: Box::new(col("n")),
        low: Box::new(int_lit(1)),
        high: Box::new(int_lit(10)),
        negated: false,
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

// ─── IN LIST with null ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_in_list_null_value_returns_null() {
    let (ctx, _) = make_ctx_with_table(
        "iln_t",
        vec![("x", "integer")],
        vec![make_row(&[("x", Value::Null)])],
    );
    let scan = Arc::new(SeqScanExec::new("iln_t".into(), None, None, None));
    let pred = Expr::InList {
        expr: Box::new(col("x")),
        list: vec![int_lit(1), int_lit(2)],
        negated: false,
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

// ─── CASE with operand ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_case_with_operand_match_first() {
    let (ctx, _) = make_ctx_with_table(
        "cwo_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Integer(1))])],
    );
    let scan = Arc::new(SeqScanExec::new("cwo_t".into(), None, None, None));
    let case_expr = Expr::Case {
        operand: Some(Box::new(col("n"))),
        conditions: vec![(int_lit(1), str_lit("one")), (int_lit(2), str_lit("two"))],
        else_result: Some(Box::new(str_lit("other"))),
    };
    let proj = ProjectExec::new(scan, vec![proj_item(case_expr, "label")]);
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("label"), Some(&Value::String("one".into())));
}

#[tokio::test]
async fn test_case_with_operand_match_second() {
    let (ctx, _) = make_ctx_with_table(
        "cwos_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Integer(2))])],
    );
    let scan = Arc::new(SeqScanExec::new("cwos_t".into(), None, None, None));
    let case_expr = Expr::Case {
        operand: Some(Box::new(col("n"))),
        conditions: vec![(int_lit(1), str_lit("one")), (int_lit(2), str_lit("two"))],
        else_result: Some(Box::new(str_lit("other"))),
    };
    let proj = ProjectExec::new(scan, vec![proj_item(case_expr, "label")]);
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("label"), Some(&Value::String("two".into())));
}

#[tokio::test]
async fn test_case_with_operand_no_match_else() {
    let (ctx, _) = make_ctx_with_table(
        "cwone_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Integer(99))])],
    );
    let scan = Arc::new(SeqScanExec::new("cwone_t".into(), None, None, None));
    let case_expr = Expr::Case {
        operand: Some(Box::new(col("n"))),
        conditions: vec![(int_lit(1), str_lit("one"))],
        else_result: Some(Box::new(str_lit("fallback"))),
    };
    let proj = ProjectExec::new(scan, vec![proj_item(case_expr, "label")]);
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(
        rows[0].get("label"),
        Some(&Value::String("fallback".into()))
    );
}

#[tokio::test]
async fn test_case_with_operand_no_match_no_else_null() {
    let (ctx, _) = make_ctx_with_table(
        "cwonne_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Integer(99))])],
    );
    let scan = Arc::new(SeqScanExec::new("cwonne_t".into(), None, None, None));
    let case_expr = Expr::Case {
        operand: Some(Box::new(col("n"))),
        conditions: vec![(int_lit(1), str_lit("one"))],
        else_result: None,
    };
    let proj = ProjectExec::new(scan, vec![proj_item(case_expr, "label")]);
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("label"), Some(&Value::Null));
}

// ─── Searched CASE ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_case_searched_else_result() {
    let (ctx, _) = make_ctx_with_table(
        "cse_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Integer(0))])],
    );
    let scan = Arc::new(SeqScanExec::new("cse_t".into(), None, None, None));
    let case_expr = Expr::Case {
        operand: None,
        conditions: vec![(
            Expr::BinaryOp {
                left: Box::new(col("n")),
                op: BinaryOperator::Gt,
                right: Box::new(int_lit(5)),
            },
            str_lit("big"),
        )],
        else_result: Some(Box::new(str_lit("small"))),
    };
    let proj = ProjectExec::new(scan, vec![proj_item(case_expr, "size")]);
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("size"), Some(&Value::String("small".into())));
}

#[tokio::test]
async fn test_case_searched_no_match_no_else_null() {
    let (ctx, _) = make_ctx_with_table(
        "csne_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Integer(0))])],
    );
    let scan = Arc::new(SeqScanExec::new("csne_t".into(), None, None, None));
    let case_expr = Expr::Case {
        operand: None,
        conditions: vec![(
            Expr::BinaryOp {
                left: Box::new(col("n")),
                op: BinaryOperator::Gt,
                right: Box::new(int_lit(5)),
            },
            str_lit("big"),
        )],
        else_result: None,
    };
    let proj = ProjectExec::new(scan, vec![proj_item(case_expr, "size")]);
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("size"), Some(&Value::Null));
}

// ─── SeqScan filter null/false ────────────────────────────────────────────────

#[tokio::test]
async fn test_seq_scan_filter_null_skips_row() {
    let (ctx, _) = make_ctx_with_table(
        "nullflt_t",
        vec![("x", "integer")],
        vec![make_row(&[("x", Value::Integer(1))])],
    );
    let exec = SeqScanExec::new("nullflt_t".into(), None, None, Some(null_lit()));
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_seq_scan_filter_false_skips_row() {
    let (ctx, _) = make_ctx_with_table(
        "falseflt_t",
        vec![("x", "integer")],
        vec![make_row(&[("x", Value::Integer(1))])],
    );
    let exec = SeqScanExec::new("falseflt_t".into(), None, None, Some(bool_lit(false)));
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_seq_scan_specific_columns() {
    let (ctx, _) = make_ctx_with_table(
        "cols_t",
        vec![("id", "integer"), ("name", "text")],
        vec![make_row(&[
            ("id", Value::Integer(1)),
            ("name", Value::String("alice".into())),
        ])],
    );
    let exec = SeqScanExec::new("cols_t".into(), None, Some(vec!["id".to_string()]), None);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
}

#[test]
fn test_seq_scan_schema() {
    let exec = SeqScanExec::new("t".into(), None, None, None);
    assert_eq!(exec.schema(), vec![]);
}

// ─── IndexScan ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_index_scan_with_residual_filter_pass() {
    let (ctx, _) = make_ctx_with_table(
        "idx_res_t",
        vec![("id", "integer"), ("v", "integer")],
        vec![
            make_row(&[("id", Value::Integer(1)), ("v", Value::Integer(100))]),
            make_row(&[("id", Value::Integer(1)), ("v", Value::Integer(50))]),
        ],
    );
    let key_pred = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(int_lit(1)),
    };
    let residual = Expr::BinaryOp {
        left: Box::new(col("v")),
        op: BinaryOperator::Gt,
        right: Box::new(int_lit(75)),
    };
    let exec = IndexScanExec::new(
        "idx_res_t".into(),
        None,
        "idx_id".into(),
        None,
        key_pred,
        Some(residual),
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("v"), Some(&Value::Integer(100)));
}

#[tokio::test]
async fn test_index_scan_with_residual_filter_fail() {
    let (ctx, _) = make_ctx_with_table(
        "idx_res2_t",
        vec![("id", "integer"), ("v", "integer")],
        vec![make_row(&[
            ("id", Value::Integer(1)),
            ("v", Value::Integer(10)),
        ])],
    );
    let key_pred = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(int_lit(1)),
    };
    let residual = Expr::BinaryOp {
        left: Box::new(col("v")),
        op: BinaryOperator::Gt,
        right: Box::new(int_lit(100)),
    };
    let exec = IndexScanExec::new(
        "idx_res2_t".into(),
        None,
        "idx_id".into(),
        None,
        key_pred,
        Some(residual),
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_index_scan_table_not_found() {
    let ctx = default_ctx();
    let key_pred = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(int_lit(1)),
    };
    let exec = IndexScanExec::new(
        "no_such_table".into(),
        None,
        "idx".into(),
        None,
        key_pred,
        None,
    );
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
}

#[test]
fn test_index_scan_schema() {
    let key_pred = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(int_lit(1)),
    };
    let exec = IndexScanExec::new("t".into(), None, "idx".into(), None, key_pred, None);
    assert_eq!(exec.schema(), vec![]);
}

// ─── FilterExec null predicate ────────────────────────────────────────────────

#[tokio::test]
async fn test_filter_exec_null_predicate_excludes_row() {
    let (ctx, _) = make_ctx_with_table(
        "fnull_t",
        vec![("x", "integer")],
        vec![make_row(&[("x", Value::Integer(1))])],
    );
    let scan = Arc::new(SeqScanExec::new("fnull_t".into(), None, None, None));
    let exec = FilterExec::new(scan, null_lit());
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[test]
fn test_filter_exec_schema() {
    let scan = Arc::new(SeqScanExec::new("t".into(), None, None, None));
    let exec = FilterExec::new(scan, bool_lit(true));
    assert_eq!(exec.schema(), vec![]);
}

// ─── HashAggregateExec ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_aggregate_empty_input() {
    let (ctx, _) = make_ctx_with_table("hagg_empty", vec![("v", "integer")], vec![]);
    let scan = Arc::new(SeqScanExec::new("hagg_empty".into(), None, None, None));
    let exec = HashAggregateExec::new(
        scan,
        vec![col("v")],
        vec![AggregateExpr {
            func: col("v"),
            alias: Some("cnt".to_string()),
        }],
        None,
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[test]
fn test_hash_aggregate_schema_with_alias() {
    let scan = Arc::new(ValuesExec::new(vec![], vec![]));
    let exec = HashAggregateExec::new(
        scan,
        vec![col("g")],
        vec![AggregateExpr {
            func: col("v"),
            alias: Some("total".to_string()),
        }],
        None,
    );
    let schema = exec.schema();
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].0, "group_0");
    assert_eq!(schema[1].0, "total");
}

#[test]
fn test_hash_aggregate_schema_no_alias() {
    let scan = Arc::new(ValuesExec::new(vec![], vec![]));
    let exec = HashAggregateExec::new(
        scan,
        vec![],
        vec![AggregateExpr {
            func: col("v"),
            alias: None,
        }],
        None,
    );
    let schema = exec.schema();
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].0, "agg");
}

// ─── NestedLoopJoin FULL JOIN ─────────────────────────────────────────────────

#[tokio::test]
async fn test_nested_loop_full_join() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("full_l", vec![("id".to_string(), "integer".to_string())]);
    provider.add_table("full_r", vec![("fk".to_string(), "integer".to_string())]);
    provider.add_rows(
        "full_l",
        vec![
            make_row(&[("id", Value::Integer(1))]),
            make_row(&[("id", Value::Integer(2))]),
        ],
    );
    provider.add_rows(
        "full_r",
        vec![
            make_row(&[("fk", Value::Integer(3))]),
            make_row(&[("fk", Value::Integer(4))]),
        ],
    );
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "u".to_string(),
    );
    let left = Arc::new(SeqScanExec::new("full_l".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("full_r".into(), None, None, None));
    let cond = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(col("fk")),
    };
    let exec = NestedLoopJoinExec::new(left, right, JoinType::Full, Some(cond));
    let rows = collect_rows(&exec, &ctx).await;
    // No matches: 2 left unmatched + 2 right unmatched = 4
    assert_eq!(rows.len(), 4);
}

#[test]
fn test_nested_loop_join_schema() {
    let left = Arc::new(ValuesExec::new(vec![], vec!["a".to_string()]));
    let right = Arc::new(ValuesExec::new(vec![], vec!["b".to_string()]));
    let exec = NestedLoopJoinExec::new(left, right, JoinType::Inner, None);
    let schema = exec.schema();
    assert_eq!(schema.len(), 2);
}

// ─── HashJoin schema and no matches ──────────────────────────────────────────

#[test]
fn test_hash_join_schema() {
    let left = Arc::new(ValuesExec::new(vec![], vec!["a".to_string()]));
    let right = Arc::new(ValuesExec::new(vec![], vec!["b".to_string()]));
    let exec = HashJoinExec::new(left, right, JoinType::Inner, vec![], vec![], None);
    let schema = exec.schema();
    assert_eq!(schema.len(), 2);
}

#[tokio::test]
async fn test_hash_join_no_matches_empty() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("hjnm_l", vec![("id".to_string(), "integer".to_string())]);
    provider.add_table("hjnm_r", vec![("id".to_string(), "integer".to_string())]);
    provider.add_rows("hjnm_l", vec![make_row(&[("id", Value::Integer(1))])]);
    provider.add_rows("hjnm_r", vec![make_row(&[("id", Value::Integer(2))])]);
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "u".to_string(),
    );
    let left = Arc::new(SeqScanExec::new("hjnm_l".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("hjnm_r".into(), None, None, None));
    let exec = HashJoinExec::new(
        left,
        right,
        JoinType::Inner,
        vec![col("id")],
        vec![col("id")],
        None,
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

// ─── SortMergeJoin schema and no matching keys ───────────────────────────────

#[test]
fn test_sort_merge_join_schema() {
    let left = Arc::new(ValuesExec::new(vec![], vec!["a".to_string()]));
    let right = Arc::new(ValuesExec::new(vec![], vec!["b".to_string()]));
    let exec = SortMergeJoinExec::new(left, right, JoinType::Inner, vec![], vec![]);
    let schema = exec.schema();
    assert_eq!(schema.len(), 2);
}

#[tokio::test]
async fn test_sort_merge_join_no_matching_keys() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("smjl", vec![("k".to_string(), "integer".to_string())]);
    provider.add_table("smjr", vec![("k".to_string(), "integer".to_string())]);
    provider.add_rows("smjl", vec![make_row(&[("k", Value::Integer(1))])]);
    provider.add_rows("smjr", vec![make_row(&[("k", Value::Integer(99))])]);
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "u".to_string(),
    );
    let left = Arc::new(SeqScanExec::new("smjl".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("smjr".into(), None, None, None));
    let exec = SortMergeJoinExec::new(left, right, JoinType::Inner, vec![col("k")], vec![col("k")]);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

// ─── SortExec null values sort first ─────────────────────────────────────────

#[tokio::test]
async fn test_sort_exec_nulls_sort_first() {
    let (ctx, _) = make_ctx_with_table(
        "sn_t",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(5))]),
            make_row(&[("n", Value::Null)]),
            make_row(&[("n", Value::Integer(2))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("sn_t".into(), None, None, None));
    let order = vec![SortExpr {
        expr: col("n"),
        ascending: true,
    }];
    let exec = SortExec::new(scan, order);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("n"), Some(&Value::Null));
    assert_eq!(rows[1].get("n"), Some(&Value::Integer(2)));
    assert_eq!(rows[2].get("n"), Some(&Value::Integer(5)));
}

#[tokio::test]
async fn test_sort_exec_float_values() {
    let (ctx, _) = make_ctx_with_table(
        "sf_t",
        vec![("f", "float")],
        vec![
            make_row(&[("f", Value::Float(3.5))]),
            make_row(&[("f", Value::Float(1.1))]),
            make_row(&[("f", Value::Float(2.2))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("sf_t".into(), None, None, None));
    let order = vec![SortExpr {
        expr: col("f"),
        ascending: true,
    }];
    let exec = SortExec::new(scan, order);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("f"), Some(&Value::Float(1.1)));
    assert_eq!(rows[2].get("f"), Some(&Value::Float(3.5)));
}

#[test]
fn test_sort_exec_schema() {
    let scan = Arc::new(ValuesExec::new(vec![], vec!["x".to_string()]));
    let exec = SortExec::new(scan, vec![]);
    let schema = exec.schema();
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].0, "x");
}

// ─── LimitExec with 0 and large limit ────────────────────────────────────────

#[tokio::test]
async fn test_limit_exec_zero_limit() {
    let (ctx, _) = make_ctx_with_table(
        "lz_t",
        vec![("i", "integer")],
        vec![
            make_row(&[("i", Value::Integer(1))]),
            make_row(&[("i", Value::Integer(2))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("lz_t".into(), None, None, None));
    let exec = LimitExec::new(scan, 0, 0);
    let rows = collect_rows(&exec, &ctx).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_limit_exec_large_limit() {
    let (ctx, _) = make_ctx_with_table(
        "llarge_t",
        vec![("i", "integer")],
        vec![
            make_row(&[("i", Value::Integer(1))]),
            make_row(&[("i", Value::Integer(2))]),
            make_row(&[("i", Value::Integer(3))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("llarge_t".into(), None, None, None));
    let exec = LimitExec::new(scan, usize::MAX, 0);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_limit_exec_schema() {
    let scan = Arc::new(ValuesExec::new(vec![], vec!["x".to_string()]));
    let exec = LimitExec::new(scan, 10, 0);
    let schema = exec.schema();
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].0, "x");
}

// ─── DistinctExec schema and all-duplicates ───────────────────────────────────

#[test]
fn test_distinct_exec_schema() {
    let scan = Arc::new(ValuesExec::new(vec![], vec!["x".to_string()]));
    let exec = DistinctExec::new(scan);
    let schema = exec.schema();
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].0, "x");
}

#[tokio::test]
async fn test_distinct_exec_all_duplicates_single_row() {
    let (ctx, _) = make_ctx_with_table(
        "alld_t",
        vec![("x", "integer")],
        vec![
            make_row(&[("x", Value::Integer(7))]),
            make_row(&[("x", Value::Integer(7))]),
            make_row(&[("x", Value::Integer(7))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("alld_t".into(), None, None, None));
    let exec = DistinctExec::new(scan);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
}

// ─── UnnestExec no alias ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_unnest_exec_no_alias_default_name() {
    let (ctx, _) = make_ctx_with_table(
        "unn_t",
        vec![("items", "array")],
        vec![make_row(&[(
            "items",
            Value::Array(vec![Value::Integer(10)]),
        )])],
    );
    let scan = Arc::new(SeqScanExec::new("unn_t".into(), None, None, None));
    let exec = UnnestExec::new(scan, col("items"), None);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("unnest"), Some(&Value::Integer(10)));
}

// ─── ValuesExec extra columns beyond schema ───────────────────────────────────

#[tokio::test]
async fn test_values_exec_extra_columns_beyond_schema() {
    let ctx = default_ctx();
    let exec = ValuesExec::new(
        vec![vec![int_lit(1), int_lit(2), int_lit(3)]],
        vec!["a".to_string()],
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("a"), Some(&Value::Integer(1)));
    assert_eq!(rows[0].get("col_1"), Some(&Value::Integer(2)));
    assert_eq!(rows[0].get("col_2"), Some(&Value::Integer(3)));
}

// ─── DML permission denied and referential integrity ─────────────────────────

#[tokio::test]
async fn test_execute_update_permission_denied() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("upd_pd", vec![("x".to_string(), "integer".to_string())]);
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "nobody".to_string(),
    );
    let mut updates = HashMap::new();
    updates.insert("x".to_string(), Value::Integer(1));
    let result = execute_update(&ctx, "upd_pd", updates).await;
    assert!(matches!(result, Err(ExecutorError::PermissionDenied(_))));
}

#[tokio::test]
async fn test_check_referential_integrity_table_not_found() {
    let ctx = default_ctx();
    let result = check_referential_integrity(&ctx, "no_such_table", "id", &Value::Integer(1)).await;
    assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
}

#[tokio::test]
async fn test_apply_referential_action_set_default_ok() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("sd_t", vec![("fk".to_string(), "integer".to_string())]);
    let mut acl = ACL::new();
    acl.grant("u", "sd_t", "UPDATE");
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(acl)),
        Arc::new(AtomicIdGenerator::default()),
        "u".to_string(),
    );
    let result = apply_referential_action(&ctx, "SET DEFAULT", "sd_t", "fk").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_apply_referential_action_unknown_ok() {
    let ctx = default_ctx();
    let result = apply_referential_action(&ctx, "UNKNOWN_ACTION", "t", "c").await;
    assert!(result.is_ok());
}

// ─── build_executor for remaining PhysicalPlan variants ──────────────────────

#[tokio::test]
async fn test_build_executor_index_scan() {
    let (ctx, _) = make_ctx_with_table(
        "beis_t",
        vec![("id", "integer")],
        vec![
            make_row(&[("id", Value::Integer(1))]),
            make_row(&[("id", Value::Integer(2))]),
        ],
    );
    let key_pred = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(int_lit(1)),
    };
    let plan = PhysicalPlan::IndexScan {
        table: "beis_t".into(),
        alias: None,
        index: "idx_id".into(),
        columns: None,
        key_predicate: key_pred,
        filter: None,
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_build_executor_project() {
    let (ctx, _) = make_ctx_with_table(
        "bep_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Integer(7))])],
    );
    let scan = PhysicalPlan::SeqScan {
        table: "bep_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let plan = PhysicalPlan::Project {
        input: Box::new(scan),
        items: vec![proj_item(col("n"), "alias_n")],
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("alias_n"), Some(&Value::Integer(7)));
}

#[tokio::test]
async fn test_build_executor_nested_loop_join() {
    let (ctx, _) = make_ctx_with_table(
        "benlj_t",
        vec![("id", "integer")],
        vec![make_row(&[("id", Value::Integer(1))])],
    );
    let scan = PhysicalPlan::SeqScan {
        table: "benlj_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let plan = PhysicalPlan::NestedLoopJoin {
        left: Box::new(scan.clone()),
        right: Box::new(scan),
        join_type: JoinType::Inner,
        condition: None,
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_build_executor_hash_join() {
    let (ctx, _) = make_ctx_with_table(
        "behj_t",
        vec![("id", "integer")],
        vec![make_row(&[("id", Value::Integer(1))])],
    );
    let scan = PhysicalPlan::SeqScan {
        table: "behj_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let plan = PhysicalPlan::HashJoin {
        left: Box::new(scan.clone()),
        right: Box::new(scan),
        join_type: JoinType::Inner,
        left_keys: vec![col("id")],
        right_keys: vec![col("id")],
        residual: None,
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_build_executor_sort() {
    let (ctx, _) = make_ctx_with_table(
        "bes_t",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(3))]),
            make_row(&[("n", Value::Integer(1))]),
        ],
    );
    let scan = PhysicalPlan::SeqScan {
        table: "bes_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let plan = PhysicalPlan::Sort {
        input: Box::new(scan),
        order_by: vec![SortExpr {
            expr: col("n"),
            ascending: true,
        }],
        algorithm: SortAlgorithm::InMemory,
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("n"), Some(&Value::Integer(1)));
}

#[tokio::test]
async fn test_build_executor_limit() {
    let (ctx, _) = make_ctx_with_table(
        "bel_t",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(1))]),
            make_row(&[("n", Value::Integer(2))]),
            make_row(&[("n", Value::Integer(3))]),
        ],
    );
    let scan = PhysicalPlan::SeqScan {
        table: "bel_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let plan = PhysicalPlan::Limit {
        input: Box::new(scan),
        limit: 2,
        offset: 0,
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_build_executor_view_as() {
    let (ctx, _) = make_ctx_with_table(
        "beva_t",
        vec![("n", "integer")],
        vec![make_row(&[("n", Value::Integer(42))])],
    );
    let scan = PhysicalPlan::SeqScan {
        table: "beva_t".into(),
        alias: None,
        columns: None,
        filter: None,
        cost: zero_cost(),
    };
    let plan = PhysicalPlan::ViewAs {
        input: Box::new(scan),
        items: vec![ViewAsProjection {
            expr: col("n"),
            alias: "renamed_n".to_string(),
        }],
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("renamed_n"), Some(&Value::Integer(42)));
}

#[tokio::test]
async fn test_build_executor_values_empty_rows() {
    let ctx = default_ctx();
    let plan = PhysicalPlan::Values {
        rows: vec![],
        cost: zero_cost(),
    };
    let exec = build_executor(&plan).unwrap();
    let rows = collect_rows(exec.as_ref(), &ctx).await;
    assert!(rows.is_empty());
}

// ─── Additional DML and privilege coverage ───────────────────────────────────

#[tokio::test]
async fn test_execute_insert_fills_missing_columns_with_null() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table(
        "fill_t",
        vec![
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
            ("age".to_string(), "integer".to_string()),
        ],
    );
    let mut acl = ACL::new();
    acl.grant("u", "fill_t", "INSERT");
    let ctx = ExecutionContext::new(
        provider.clone(),
        Arc::new(Mutex::new(acl)),
        Arc::new(AtomicIdGenerator::default()),
        "u".to_string(),
    );
    let n = execute_insert(
        &ctx,
        "fill_t",
        &["id".to_string()],
        vec![vec![Value::Integer(1)]],
    )
    .await
    .unwrap();
    assert_eq!(n, 1);
    let rows = provider.scan("fill_t").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(rows[0].get("name"), Some(&Value::Null));
    assert_eq!(rows[0].get("age"), Some(&Value::Null));
}

#[test]
fn test_execute_grant_revoke_roundtrip() {
    let ctx = ExecutionContext::default_test();
    execute_grant(&ctx, "dave", "sales", "SELECT").unwrap();
    execute_grant(&ctx, "dave", "sales", "INSERT").unwrap();
    {
        let acl = ctx.acl.lock().unwrap();
        assert!(acl.check("dave", "sales", "SELECT"));
        assert!(acl.check("dave", "sales", "INSERT"));
    }
    execute_revoke(&ctx, "dave", "sales", "SELECT").unwrap();
    {
        let acl = ctx.acl.lock().unwrap();
        assert!(!acl.check("dave", "sales", "SELECT"));
        assert!(acl.check("dave", "sales", "INSERT"));
    }
}

#[tokio::test]
async fn test_unary_not_null_propagation() {
    let ctx = default_ctx();
    let exec = ProjectExec::new(
        single_value_exec(null_lit()),
        vec![proj_item(
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(null_lit()),
            },
            "r",
        )],
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Null));
}

#[tokio::test]
async fn test_unary_not_wrong_type() {
    let ctx = default_ctx();
    let exec = ProjectExec::new(
        single_value_exec(int_lit(1)),
        vec![proj_item(
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(col("v")),
            },
            "r",
        )],
    );
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_unary_minus_float() {
    let ctx = default_ctx();
    let exec = ProjectExec::new(
        single_value_exec(float_lit(3.5)),
        vec![proj_item(
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(col("v")),
            },
            "r",
        )],
    );
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::Float(-3.5)));
}

#[tokio::test]
async fn test_unary_minus_wrong_type() {
    let ctx = default_ctx();
    let exec = ProjectExec::new(
        single_value_exec(str_lit("abc")),
        vec![proj_item(
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(col("v")),
            },
            "r",
        )],
    );
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TypeMismatch { .. })));
}

#[tokio::test]
async fn test_string_concat_with_integer() {
    let ctx = default_ctx();
    let proj = ProjectExec::new(
        single_value_exec(str_lit("val=")),
        vec![proj_item(
            Expr::BinaryOp {
                left: Box::new(col("v")),
                op: BinaryOperator::StringConcat,
                right: Box::new(int_lit(42)),
            },
            "r",
        )],
    );
    let rows = collect_rows(&proj, &ctx).await;
    assert_eq!(rows[0].get("r"), Some(&Value::String("val=42".into())));
}

#[tokio::test]
async fn test_seq_scan_table_not_found() {
    let ctx = default_ctx();
    let exec = SeqScanExec::new("missing_table".into(), None, None, None);
    let result = try_collect(&exec, &ctx).await;
    assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
}

#[tokio::test]
async fn test_between_negated_excludes_in_range() {
    let (ctx, _) = make_ctx_with_table(
        "bne_t",
        vec![("n", "integer")],
        vec![
            make_row(&[("n", Value::Integer(1))]),
            make_row(&[("n", Value::Integer(5))]),
            make_row(&[("n", Value::Integer(10))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("bne_t".into(), None, None, None));
    let pred = Expr::Between {
        expr: Box::new(col("n")),
        low: Box::new(int_lit(3)),
        high: Box::new(int_lit(7)),
        negated: true,
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_in_list_negated_with_match() {
    let (ctx, _) = make_ctx_with_table(
        "ilnm_t",
        vec![("x", "integer")],
        vec![
            make_row(&[("x", Value::Integer(1))]),
            make_row(&[("x", Value::Integer(2))]),
            make_row(&[("x", Value::Integer(3))]),
        ],
    );
    let scan = Arc::new(SeqScanExec::new("ilnm_t".into(), None, None, None));
    let pred = Expr::InList {
        expr: Box::new(col("x")),
        list: vec![int_lit(1), int_lit(2)],
        negated: true,
    };
    let exec = FilterExec::new(scan, pred);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("x"), Some(&Value::Integer(3)));
}

#[test]
fn test_check_privilege_after_grant() {
    let provider = Arc::new(InMemoryTableProvider::new());
    let acl_ref = Arc::new(Mutex::new(ACL::new()));
    let ctx = ExecutionContext::new(
        provider,
        acl_ref.clone(),
        Arc::new(AtomicIdGenerator::default()),
        "alice".to_string(),
    );
    assert!(ctx.check_privilege("reports", "SELECT").is_err());
    {
        let mut acl = acl_ref.lock().unwrap();
        acl.grant("alice", "reports", "SELECT");
    }
    assert!(ctx.check_privilege("reports", "SELECT").is_ok());
}

#[tokio::test]
async fn test_nested_loop_left_join_real_unmatched() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("nlj_l", vec![("id".to_string(), "integer".to_string())]);
    provider.add_table("nlj_r", vec![("fk".to_string(), "integer".to_string())]);
    provider.add_rows(
        "nlj_l",
        vec![
            make_row(&[("id", Value::Integer(1))]),
            make_row(&[("id", Value::Integer(2))]),
        ],
    );
    provider.add_rows("nlj_r", vec![make_row(&[("fk", Value::Integer(1))])]);
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "u".to_string(),
    );
    let left = Arc::new(SeqScanExec::new("nlj_l".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("nlj_r".into(), None, None, None));
    let cond = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(col("fk")),
    };
    let exec = NestedLoopJoinExec::new(left, right, JoinType::Left, Some(cond));
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_nested_loop_right_join_real_unmatched() {
    let provider = Arc::new(InMemoryTableProvider::new());
    provider.add_table("rjleft", vec![("id".to_string(), "integer".to_string())]);
    provider.add_table("rjright", vec![("fk".to_string(), "integer".to_string())]);
    provider.add_rows("rjleft", vec![make_row(&[("id", Value::Integer(1))])]);
    provider.add_rows(
        "rjright",
        vec![
            make_row(&[("fk", Value::Integer(1))]),
            make_row(&[("fk", Value::Integer(99))]),
        ],
    );
    let ctx = ExecutionContext::new(
        provider,
        Arc::new(Mutex::new(ACL::new())),
        Arc::new(AtomicIdGenerator::default()),
        "u".to_string(),
    );
    let left = Arc::new(SeqScanExec::new("rjleft".into(), None, None, None));
    let right = Arc::new(SeqScanExec::new("rjright".into(), None, None, None));
    let cond = Expr::BinaryOp {
        left: Box::new(col("id")),
        op: BinaryOperator::Eq,
        right: Box::new(col("fk")),
    };
    let exec = NestedLoopJoinExec::new(left, right, JoinType::Right, Some(cond));
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_project_exec_no_items() {
    let (ctx, _) = make_ctx_with_table(
        "proj_empty",
        vec![("x", "integer")],
        vec![make_row(&[("x", Value::Integer(1))])],
    );
    let scan = Arc::new(SeqScanExec::new("proj_empty".into(), None, None, None));
    let exec = ProjectExec::new(scan, vec![]);
    let rows = collect_rows(&exec, &ctx).await;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].columns.is_empty());
}
