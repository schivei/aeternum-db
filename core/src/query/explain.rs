//! EXPLAIN / plan visualization for query plans.
//!
//! [`explain_physical`] formats a [`PhysicalPlan`] tree into a human-readable
//! string similar to common EXPLAIN outputs.
//!
//! ## Example output
//!
//! ```text
//! Physical Plan:
//! └─ HashAggregate [group_by: [age], aggregates: [COUNT(*)]]
//!    Est. rows: 50 | Cost: 1250.00 (I/O: 500.00, CPU: 750.00)
//!    └─ HashJoin [type: Inner]
//!       Est. rows: 50 | Cost: 800.00 (I/O: 0.00, CPU: 800.00)
//!       ├─ IndexScan [table: users, index: age_idx]
//!       │  Est. rows: 500 | Cost: 100.00 (I/O: 50.00, CPU: 50.00)
//!       └─ SeqScan [table: orders]
//!          Est. rows: 20000 | Cost: 220.00 (I/O: 200.00, CPU: 20.00)
//!
//! Total Cost: 1250.00
//! Estimated Rows: 50
//! ```

use crate::query::logical_plan::LogicalPlan;
use crate::query::physical_plan::PhysicalPlan;

// ── Public API ────────────────────────────────────────────────────────────────

/// Format a [`PhysicalPlan`] as a human-readable EXPLAIN string.
///
/// ```rust
/// use aeternumdb_core::query::explain::explain_physical;
/// use aeternumdb_core::query::physical_plan::PhysicalPlan;
///
/// let plan = PhysicalPlan::Values {
///     rows: vec![],
///     cost: Default::default(),
/// };
/// let output = explain_physical(&plan);
/// assert!(output.contains("Total Cost"));
/// ```
pub fn explain_physical(plan: &PhysicalPlan) -> String {
    let mut out = String::from("Physical Plan:\n");
    format_node(plan, &mut out, "", true);
    let cost = plan.cost();
    out.push('\n');
    out.push_str(&format!("Total Cost: {:.2}\n", cost.total));
    out.push_str(&format!("Estimated Rows: {}\n", cost.estimated_rows));
    out
}

/// Format a [`PhysicalPlan`] as a logical tree (pre-optimization view).
///
/// Uses the same renderer as [`explain_physical`] but without cost
/// annotations.
pub fn explain_logical(plan: &LogicalPlan) -> String {
    let mut out = String::from("Logical Plan:\n");
    format_logical_node(plan, &mut out, "", true);
    out
}

// ── Recursive tree renderer ───────────────────────────────────────────────────

fn format_node(plan: &PhysicalPlan, out: &mut String, prefix: &str, is_last: bool) {
    let connector = if is_last { "└─ " } else { "├─ " };
    let child_prefix = if is_last {
        format!("{prefix}   ")
    } else {
        format!("{prefix}│  ")
    };

    let label = node_label(plan);
    let cost = plan.cost();
    out.push_str(&format!("{prefix}{connector}{label}\n"));
    out.push_str(&format!(
        "{child_prefix}Est. rows: {} | Cost: {:.2} (I/O: {:.2}, CPU: {:.2})\n",
        cost.estimated_rows, cost.total, cost.io, cost.cpu
    ));

    let children = node_children(plan);
    let last = children.len().saturating_sub(1);
    for (i, child) in children.iter().enumerate() {
        format_node(child, out, &child_prefix, i == last);
    }
}

/// Return a short label describing the physical plan node.
fn node_label(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::SeqScan { table, .. } => format!("SeqScan [table: {table}]"),
        PhysicalPlan::IndexScan { table, index, .. } => {
            format!("IndexScan [table: {table}, index: {index}]")
        }
        PhysicalPlan::Filter { predicate, .. } => {
            format!("Filter [predicate: {}]", display_expr(predicate))
        }
        PhysicalPlan::Project { items, .. } => {
            let cols: Vec<_> = items
                .iter()
                .map(|p| {
                    p.alias
                        .as_deref()
                        .unwrap_or_else(|| display_expr(&p.expr))
                        .to_string()
                })
                .collect();
            format!("Project [{}]", cols.join(", "))
        }
        PhysicalPlan::NestedLoopJoin { join_type, .. } => {
            format!("NestedLoopJoin [type: {join_type:?}]")
        }
        PhysicalPlan::HashJoin { join_type, .. } => {
            format!("HashJoin [type: {join_type:?}]")
        }
        PhysicalPlan::HashAggregate {
            group_by,
            aggregates,
            ..
        } => {
            let gb: Vec<_> = group_by.iter().map(display_expr).collect();
            let aggs: Vec<_> = aggregates
                .iter()
                .map(|a| {
                    a.alias
                        .as_deref()
                        .unwrap_or_else(|| display_expr(&a.func))
                        .to_string()
                })
                .collect();
            format!(
                "HashAggregate [group_by: [{}], aggregates: [{}]]",
                gb.join(", "),
                aggs.join(", ")
            )
        }
        PhysicalPlan::Sort { algorithm, .. } => format!("Sort [{algorithm:?}]"),
        PhysicalPlan::Limit { limit, offset, .. } => {
            format!("Limit [limit: {limit}, offset: {offset}]")
        }
        PhysicalPlan::Unnest { alias, .. } => {
            format!("Unnest [alias: {}]", alias.as_deref().unwrap_or("_"))
        }
        PhysicalPlan::ViewAs { items, .. } => {
            let names: Vec<_> = items.iter().map(|v| v.alias.as_str()).collect();
            format!("ViewAs [{}]", names.join(", "))
        }
        PhysicalPlan::Values { rows, .. } => format!("Values [{} row(s)]", rows.len()),
    }
}

/// Collect the direct child plans of a physical node.
fn node_children(plan: &PhysicalPlan) -> Vec<&PhysicalPlan> {
    match plan {
        PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::IndexScan { .. }
        | PhysicalPlan::Values { .. } => vec![],
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Unnest { input, .. }
        | PhysicalPlan::ViewAs { input, .. } => vec![input.as_ref()],
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. } => {
            vec![left.as_ref(), right.as_ref()]
        }
    }
}

// ── Logical plan renderer ─────────────────────────────────────────────────────

fn format_logical_node(plan: &LogicalPlan, out: &mut String, prefix: &str, is_last: bool) {
    let connector = if is_last { "└─ " } else { "├─ " };
    let child_prefix = if is_last {
        format!("{prefix}   ")
    } else {
        format!("{prefix}│  ")
    };

    let (label, children) = logical_label_and_children(plan);
    out.push_str(&format!("{prefix}{connector}{label}\n"));

    let last = children.len().saturating_sub(1);
    for (i, child) in children.iter().enumerate() {
        format_logical_node(child, out, &child_prefix, i == last);
    }
}

fn logical_label_and_children(plan: &LogicalPlan) -> (String, Vec<&LogicalPlan>) {
    match plan {
        LogicalPlan::Scan { table, alias, .. } => {
            let label = alias
                .as_deref()
                .map(|a| format!("Scan [{table} AS {a}]"))
                .unwrap_or_else(|| format!("Scan [{table}]"));
            (label, vec![])
        }
        LogicalPlan::Filter { input, predicate } => (
            format!("Filter [{}]", display_expr(predicate)),
            vec![input.as_ref()],
        ),
        LogicalPlan::Project { input, items } => {
            let cols: Vec<_> = items
                .iter()
                .map(|p| {
                    p.alias
                        .as_deref()
                        .unwrap_or_else(|| display_expr(&p.expr))
                        .to_string()
                })
                .collect();
            (
                format!("Project [{}]", cols.join(", ")),
                vec![input.as_ref()],
            )
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } => (
            format!("Join [{join_type:?}]"),
            vec![left.as_ref(), right.as_ref()],
        ),
        LogicalPlan::Aggregate {
            input, group_by, ..
        } => {
            let gb: Vec<_> = group_by.iter().map(display_expr).collect();
            (
                format!("Aggregate [group_by: [{}]]", gb.join(", ")),
                vec![input.as_ref()],
            )
        }
        LogicalPlan::Sort { input, .. } => ("Sort".into(), vec![input.as_ref()]),
        LogicalPlan::Limit { input, limit, .. } => {
            (format!("Limit [{limit}]"), vec![input.as_ref()])
        }
        LogicalPlan::Unnest { input, .. } => ("Unnest".into(), vec![input.as_ref()]),
        LogicalPlan::ViewAs { input, items } => {
            let names: Vec<_> = items.iter().map(|v| v.alias.as_str()).collect();
            (
                format!("ViewAs [{}]", names.join(", ")),
                vec![input.as_ref()],
            )
        }
        LogicalPlan::Values { rows } => (format!("Values [{} row(s)]", rows.len()), vec![]),
    }
}

// ── Expression display ────────────────────────────────────────────────────────

fn display_expr(expr: &crate::sql::ast::Expr) -> &str {
    match expr {
        crate::sql::ast::Expr::Column { name, .. } => name.as_str(),
        crate::sql::ast::Expr::Wildcard => "*",
        _ => "<expr>",
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::logical_plan::{AggregateExpr, LogicalPlan, ProjectionItem};
    use crate::query::physical_plan::{NodeCost, PhysicalPlan, SortAlgorithm};
    use crate::sql::ast::{Expr, JoinType, Value};

    fn default_cost(rows: usize) -> NodeCost {
        NodeCost {
            total: 100.0,
            io: 60.0,
            cpu: 40.0,
            estimated_rows: rows,
        }
    }

    #[test]
    fn explain_seq_scan() {
        let plan = PhysicalPlan::SeqScan {
            table: "users".into(),
            alias: None,
            columns: None,
            filter: None,
            cost: default_cost(1000),
        };
        let out = explain_physical(&plan);
        assert!(out.contains("SeqScan"));
        assert!(out.contains("users"));
        assert!(out.contains("Total Cost"));
        assert!(out.contains("Estimated Rows: 1000"));
    }

    #[test]
    fn explain_hash_join() {
        let left = PhysicalPlan::SeqScan {
            table: "users".into(),
            alias: None,
            columns: None,
            filter: None,
            cost: default_cost(1000),
        };
        let right = PhysicalPlan::SeqScan {
            table: "orders".into(),
            alias: None,
            columns: None,
            filter: None,
            cost: default_cost(5000),
        };
        let plan = PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            left_keys: vec![],
            right_keys: vec![],
            cost: default_cost(500),
        };
        let out = explain_physical(&plan);
        assert!(out.contains("HashJoin"));
        assert!(out.contains("SeqScan"));
    }

    #[test]
    fn explain_logical_plan() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            predicate: Expr::Literal(Value::Boolean(true)),
        };
        let out = explain_logical(&plan);
        assert!(out.contains("Logical Plan"));
        assert!(out.contains("Filter"));
        assert!(out.contains("Scan [users]"));
    }

    #[test]
    fn explain_values_node() {
        let plan = PhysicalPlan::Values {
            rows: vec![vec![Expr::Literal(Value::Integer(1))]],
            cost: default_cost(1),
        };
        let out = explain_physical(&plan);
        assert!(out.contains("Values [1 row(s)]"));
        assert!(out.contains("Estimated Rows: 1"));
    }

    #[test]
    fn explain_limit_node() {
        let inner = PhysicalPlan::SeqScan {
            table: "t".into(),
            alias: None,
            columns: None,
            filter: None,
            cost: default_cost(1000),
        };
        let plan = PhysicalPlan::Limit {
            input: Box::new(inner),
            limit: 10,
            offset: 0,
            cost: default_cost(10),
        };
        let out = explain_physical(&plan);
        assert!(out.contains("Limit [limit: 10"));
    }

    #[test]
    fn explain_sort_node() {
        let inner = PhysicalPlan::SeqScan {
            table: "t".into(),
            alias: None,
            columns: None,
            filter: None,
            cost: default_cost(1000),
        };
        let plan = PhysicalPlan::Sort {
            input: Box::new(inner),
            order_by: vec![],
            algorithm: SortAlgorithm::InMemory,
            cost: default_cost(1000),
        };
        let out = explain_physical(&plan);
        assert!(out.contains("Sort [InMemory]"));
    }

    #[test]
    fn explain_hash_aggregate() {
        let inner = PhysicalPlan::SeqScan {
            table: "t".into(),
            alias: None,
            columns: None,
            filter: None,
            cost: default_cost(100),
        };
        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(inner),
            group_by: vec![Expr::Column {
                table: None,
                name: "age".into(),
            }],
            aggregates: vec![AggregateExpr {
                func: Expr::Function {
                    name: "COUNT".into(),
                    args: vec![],
                    distinct: false,
                },
                alias: Some("cnt".into()),
            }],
            having: None,
            cost: default_cost(10),
        };
        let out = explain_physical(&plan);
        assert!(out.contains("HashAggregate"));
        assert!(out.contains("age"));
    }

    #[test]
    fn explain_project_node() {
        let inner = PhysicalPlan::SeqScan {
            table: "t".into(),
            alias: None,
            columns: None,
            filter: None,
            cost: default_cost(100),
        };
        let plan = PhysicalPlan::Project {
            input: Box::new(inner),
            items: vec![ProjectionItem {
                expr: Expr::Column {
                    table: None,
                    name: "id".into(),
                },
                alias: Some("identifier".into()),
            }],
            cost: default_cost(100),
        };
        let out = explain_physical(&plan);
        assert!(out.contains("Project"));
        assert!(out.contains("identifier"));
    }
}
