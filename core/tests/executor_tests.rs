//! Integration tests for the AeternumDB executor module.
//!
//! These tests exercise all executor operators, DML functions, ACL helpers,
//! and the physical-plan builder end-to-end using an in-memory table provider.

use aeternumdb_core::executor::*;
use aeternumdb_core::query::logical_plan::SortExpr;
use aeternumdb_core::query::physical_plan::{NodeCost, PhysicalPlan};
use aeternumdb_core::sql::ast::{BinaryOperator, Expr, JoinType};
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
