//! Predicate pushdown and projection pushdown optimization rules.
//!
//! ## Predicate pushdown
//!
//! [`PredicatePushdown`] moves `Filter` nodes as close as possible to the leaf
//! `Scan` nodes that produce the rows being filtered.  This reduces the number
//! of rows flowing through expensive operators such as joins and aggregations.
//!
//! ## Projection pushdown
//!
//! [`ProjectionPushdown`] removes columns from `Scan` nodes that are not
//! referenced by any upstream operator, reducing I/O when the storage layer
//! supports column pruning.

use crate::query::logical_plan::{LogicalPlan, ProjectionItem};
use crate::query::rules::OptimizationRule;
use crate::sql::ast::Expr;

// ── PredicatePushdown ─────────────────────────────────────────────────────────

/// Pushes `Filter` nodes towards leaf `Scan` nodes.
pub struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn name(&self) -> &str {
        "predicate_pushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        push_predicate(plan)
    }
}

fn push_predicate(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => push_predicate_filter(*input, predicate),
        LogicalPlan::Project { input, items } => LogicalPlan::Project {
            input: Box::new(push_predicate(*input)),
            items,
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
        } => LogicalPlan::Join {
            left: Box::new(push_predicate(*left)),
            right: Box::new(push_predicate(*right)),
            join_type,
            condition,
        },
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(push_predicate(*input)),
            order_by,
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(push_predicate(*input)),
            limit,
            offset,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
        } => LogicalPlan::Aggregate {
            input: Box::new(push_predicate(*input)),
            group_by,
            aggregates,
            having,
        },
        LogicalPlan::Unnest {
            input,
            column,
            alias,
        } => LogicalPlan::Unnest {
            input: Box::new(push_predicate(*input)),
            column,
            alias,
        },
        LogicalPlan::ViewAs { input, items } => LogicalPlan::ViewAs {
            input: Box::new(push_predicate(*input)),
            items,
        },
        other => other,
    }
}

fn push_predicate_filter(input: LogicalPlan, predicate: Expr) -> LogicalPlan {
    match input {
        // Filter directly above a Scan with no existing filter — merge.
        LogicalPlan::Scan {
            table,
            alias,
            columns,
            filter: None,
        } => LogicalPlan::Scan {
            table,
            alias,
            columns,
            filter: Some(predicate),
        },

        // Filter above a Scan that already has a filter — AND them.
        LogicalPlan::Scan {
            table,
            alias,
            columns,
            filter: Some(existing),
        } => LogicalPlan::Scan {
            table,
            alias,
            columns,
            filter: Some(combine_predicates(existing, predicate)),
        },

        // Filter above a Join — try to push into one of the join sides.
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
        } => push_filter_through_join(*left, *right, join_type, condition, predicate),

        // For all other input types, recurse and keep the filter.
        other => LogicalPlan::Filter {
            input: Box::new(push_predicate(other)),
            predicate,
        },
    }
}

/// Attempt to push a filter predicate through a join node.
fn push_filter_through_join(
    left: LogicalPlan,
    right: LogicalPlan,
    join_type: crate::sql::ast::JoinType,
    condition: Option<Expr>,
    predicate: Expr,
) -> LogicalPlan {
    let left_table = scan_table_name(&left);
    let right_table = scan_table_name(&right);

    let refs_left = left_table
        .as_deref()
        .map(|t| references_table(&predicate, t))
        .unwrap_or(false);
    let refs_right = right_table
        .as_deref()
        .map(|t| references_table(&predicate, t))
        .unwrap_or(false);

    match (refs_left, refs_right) {
        (true, false) => LogicalPlan::Join {
            left: Box::new(push_predicate(LogicalPlan::Filter {
                input: Box::new(left),
                predicate,
            })),
            right: Box::new(push_predicate(right)),
            join_type,
            condition,
        },
        (false, true) => LogicalPlan::Join {
            left: Box::new(push_predicate(left)),
            right: Box::new(push_predicate(LogicalPlan::Filter {
                input: Box::new(right),
                predicate,
            })),
            join_type,
            condition,
        },
        // Predicate spans both sides of the join.  For INNER joins we promote
        // it to Join.condition to enable equi-key extraction.  For outer joins
        // this rewrite changes semantics (can turn outer into inner), so we
        // keep it as a Filter above the join.
        (true, true) => {
            use crate::sql::ast::JoinType;
            if join_type == JoinType::Inner {
                let merged_condition = match condition {
                    None => Some(predicate),
                    Some(existing) => Some(combine_predicates(existing, predicate)),
                };
                LogicalPlan::Join {
                    left: Box::new(push_predicate(left)),
                    right: Box::new(push_predicate(right)),
                    join_type,
                    condition: merged_condition,
                }
            } else {
                LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Join {
                        left: Box::new(push_predicate(left)),
                        right: Box::new(push_predicate(right)),
                        join_type,
                        condition,
                    }),
                    predicate,
                }
            }
        }
        _ => LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Join {
                left: Box::new(push_predicate(left)),
                right: Box::new(push_predicate(right)),
                join_type,
                condition,
            }),
            predicate,
        },
    }
}

/// Extract the table name from a `Scan` node, if present.
fn scan_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan { table, alias, .. } => Some(alias.as_deref().unwrap_or(table).into()),
        _ => None,
    }
}

/// Returns `true` if `expr` references the given `table` qualifier.
fn references_table(expr: &Expr, table: &str) -> bool {
    match expr {
        Expr::Column { table: Some(t), .. } => t.eq_ignore_ascii_case(table),
        Expr::Column { table: None, .. } | Expr::Literal(_) | Expr::Wildcard => false,
        Expr::BinaryOp { left, right, .. } => {
            references_table(left, table) || references_table(right, table)
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            references_table(expr, table)
        }
        Expr::Function { args, .. } => args.iter().any(|a| references_table(a, table)),
        Expr::Between {
            expr, low, high, ..
        } => {
            references_table(expr, table)
                || references_table(low, table)
                || references_table(high, table)
        }
        Expr::InList { expr, list, .. } => {
            references_table(expr, table) || list.iter().any(|e| references_table(e, table))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| references_table(e, table))
                || conditions
                    .iter()
                    .any(|(w, t_)| references_table(w, table) || references_table(t_, table))
                || else_result
                    .as_ref()
                    .is_some_and(|e| references_table(e, table))
        }
        Expr::ArrayOp { expr, right, .. } => {
            references_table(expr, table) || references_table(right, table)
        }
        Expr::Substring {
            expr,
            from_pos,
            len,
        } => {
            references_table(expr, table)
                || from_pos
                    .as_ref()
                    .is_some_and(|e| references_table(e, table))
                || len.as_ref().is_some_and(|e| references_table(e, table))
        }
        Expr::Position { substr, in_expr } => {
            references_table(substr, table) || references_table(in_expr, table)
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            references_table(expr, table)
                || trim_what
                    .as_ref()
                    .is_some_and(|e| references_table(e, table))
        }
        Expr::Overlay {
            expr,
            overlay_what,
            from_pos,
            for_len,
        } => {
            references_table(expr, table)
                || references_table(overlay_what, table)
                || references_table(from_pos, table)
                || for_len.as_ref().is_some_and(|e| references_table(e, table))
        }
        // Subqueries and complex expressions with embedded column references
        // are treated conservatively: returning `true` prevents accidentally
        // pushing a predicate into a join input when the expression could
        // reference the other side.
        Expr::InSubquery { .. } | Expr::Subquery(_) | Expr::MatchAgainst { .. } => true,
    }
}

/// Combine two predicates with a logical AND.
fn combine_predicates(a: Expr, b: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(a),
        op: crate::sql::ast::BinaryOperator::And,
        right: Box::new(b),
    }
}

// ── ProjectionPushdown ────────────────────────────────────────────────────────

/// Removes unreferenced columns from `Scan` nodes.
///
/// When upstream `Project` nodes name explicit columns the rule annotates the
/// scan with the minimal column list needed, avoiding full-row reads when the
/// storage layer supports column projection.
pub struct ProjectionPushdown;

impl OptimizationRule for ProjectionPushdown {
    fn name(&self) -> &str {
        "projection_pushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        push_projection(plan)
    }
}

fn push_projection(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, items } => push_projection_project(*input, items),
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(push_projection(*input)),
            predicate,
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
        } => LogicalPlan::Join {
            left: Box::new(push_projection(*left)),
            right: Box::new(push_projection(*right)),
            join_type,
            condition,
        },
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(push_projection(*input)),
            order_by,
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(push_projection(*input)),
            limit,
            offset,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
        } => LogicalPlan::Aggregate {
            input: Box::new(push_projection(*input)),
            group_by,
            aggregates,
            having,
        },
        LogicalPlan::Unnest {
            input,
            column,
            alias,
        } => LogicalPlan::Unnest {
            input: Box::new(push_projection(*input)),
            column,
            alias,
        },
        LogicalPlan::ViewAs { input, items } => LogicalPlan::ViewAs {
            input: Box::new(push_projection(*input)),
            items,
        },
        other => other,
    }
}

fn push_projection_project(input: LogicalPlan, items: Vec<ProjectionItem>) -> LogicalPlan {
    match input {
        // Project directly above a Scan — push column list into scan.
        LogicalPlan::Scan {
            table,
            alias,
            filter,
            ..
        } => {
            // Build the column list as the union of:
            //   1. columns needed by the projection items, and
            //   2. columns referenced by the scan filter predicate.
            // If either set implies "all columns" (wildcard or complex expr),
            // fall back to None (read everything).
            let proj_cols = extract_column_names(&items);
            let columns = if proj_cols.is_empty() {
                // Wildcard or non-column exprs in projection — read all.
                None
            } else {
                let mut all_cols = proj_cols;
                if let Some(pred) = &filter {
                    collect_filter_columns(pred, &mut all_cols);
                }
                all_cols.sort_unstable();
                all_cols.dedup();
                if all_cols.is_empty() {
                    None
                } else {
                    Some(all_cols)
                }
            };

            LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    table,
                    alias,
                    columns,
                    filter,
                }),
                items,
            }
        }
        // For other inputs, recurse normally.
        other => LogicalPlan::Project {
            input: Box::new(push_projection(other)),
            items,
        },
    }
}

/// Collect bare column names referenced by a filter predicate expression.
///
/// Only `Expr::Column { name, .. }` nodes are collected; wildcards and other
/// expression forms that imply "all columns" are silently skipped.
fn collect_filter_columns(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Column { name, .. } if name != "*" => {
            out.push(name.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_filter_columns(left, out);
            collect_filter_columns(right, out);
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            collect_filter_columns(expr, out);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_filter_columns(expr, out);
            collect_filter_columns(low, out);
            collect_filter_columns(high, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_filter_columns(expr, out);
            for e in list {
                collect_filter_columns(e, out);
            }
        }
        Expr::Function { args, .. } => {
            for a in args {
                collect_filter_columns(a, out);
            }
        }
        _ => {} // Literals, subqueries, wildcards — nothing to collect.
    }
}

/// Extract simple column names referenced by a projection item list.
///
/// Returns an empty vec (meaning "read all columns") when any item is a
/// wildcard (`Expr::Wildcard` or `Expr::Column { name: "*", .. }`), because
/// the scan must read all columns to satisfy the wildcard.
fn extract_column_names(items: &[ProjectionItem]) -> Vec<String> {
    let mut names = Vec::new();
    for item in items {
        match &item.expr {
            Expr::Wildcard => return vec![],
            Expr::Column { name, .. } => {
                // "*" represents a qualified wildcard (t.*) — treat as "all columns".
                if name == "*" {
                    return vec![];
                }
                names.push(name.clone());
            }
            _ => {}
        }
    }
    names
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::logical_plan::{LogicalPlan, ProjectionItem};
    use crate::sql::ast::{BinaryOperator, Expr, JoinType, Value};

    fn col(table: &str, name: &str) -> Expr {
        Expr::Column {
            table: Some(table.into()),
            name: name.into(),
        }
    }

    fn lit(v: i64) -> Expr {
        Expr::Literal(Value::Integer(v))
    }

    fn scan(table: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.into(),
            alias: Some(table.into()),
            columns: None,
            filter: None,
        }
    }

    fn filter_plan(input: LogicalPlan, pred: Expr) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate: pred,
        }
    }

    #[test]
    fn predicate_merges_into_scan() {
        let pred = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: "age".into(),
            }),
            op: BinaryOperator::Gt,
            right: Box::new(lit(18)),
        };
        let plan = filter_plan(scan("users"), pred.clone());
        let rule = PredicatePushdown;
        let optimized = rule.apply(plan);
        assert!(
            matches!(
                &optimized,
                LogicalPlan::Scan {
                    filter: Some(_),
                    ..
                }
            ),
            "expected scan with filter, got {optimized:?}"
        );
    }

    #[test]
    fn predicate_promoted_to_join_condition_when_cross_table() {
        let pred = Expr::BinaryOp {
            left: Box::new(col("u", "id")),
            op: BinaryOperator::Eq,
            right: Box::new(col("o", "user_id")),
        };
        let join = LogicalPlan::Join {
            left: Box::new(scan("u")),
            right: Box::new(scan("o")),
            join_type: JoinType::Inner,
            condition: None,
        };
        let plan = filter_plan(join, pred);
        let rule = PredicatePushdown;
        let optimized = rule.apply(plan);
        // A cross-table predicate should be promoted to the join condition
        // so the physical planner can use it for equi-join key extraction.
        assert!(
            matches!(
                &optimized,
                LogicalPlan::Join {
                    condition: Some(_),
                    ..
                }
            ),
            "expected cross-table predicate promoted to join condition, got {optimized:?}"
        );
    }

    #[test]
    fn predicate_pushed_into_join_left_side() {
        let pred = Expr::BinaryOp {
            left: Box::new(col("users", "age")),
            op: BinaryOperator::Gt,
            right: Box::new(lit(18)),
        };
        let join = LogicalPlan::Join {
            left: Box::new(scan("users")),
            right: Box::new(scan("orders")),
            join_type: JoinType::Inner,
            condition: None,
        };
        let plan = filter_plan(join, pred);
        let rule = PredicatePushdown;
        let optimized = rule.apply(plan);
        if let LogicalPlan::Join { left, .. } = &optimized {
            assert!(
                matches!(
                    left.as_ref(),
                    LogicalPlan::Scan {
                        filter: Some(_),
                        ..
                    }
                ),
                "expected left scan with filter"
            );
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn projection_pushes_columns_into_scan() {
        let items = vec![ProjectionItem {
            expr: Expr::Column {
                table: None,
                name: "id".into(),
            },
            alias: None,
        }];
        let plan = LogicalPlan::Project {
            input: Box::new(scan("users")),
            items,
        };
        let rule = ProjectionPushdown;
        let optimized = rule.apply(plan);
        if let LogicalPlan::Project { input, .. } = &optimized {
            assert!(
                matches!(
                    input.as_ref(),
                    LogicalPlan::Scan {
                        columns: Some(_),
                        ..
                    }
                ),
                "expected scan with column list"
            );
        } else {
            panic!("expected Project");
        }
    }

    #[test]
    fn combine_predicates_produces_and() {
        let a = Expr::Literal(Value::Boolean(true));
        let b = Expr::Literal(Value::Boolean(false));
        let combined = combine_predicates(a.clone(), b.clone());
        assert!(
            matches!(
                &combined,
                Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ),
            "expected AND"
        );
    }

    #[test]
    fn extract_column_names_returns_empty_for_expr_wildcard() {
        // Expr::Wildcard (plain `*`) must signal "all columns" — i.e. return [].
        let items = vec![
            ProjectionItem {
                expr: Expr::Wildcard,
                alias: None,
            },
            ProjectionItem {
                expr: Expr::Column {
                    table: None,
                    name: "id".into(),
                },
                alias: None,
            },
        ];
        let names = extract_column_names(&items);
        assert!(
            names.is_empty(),
            "expected empty vec (all-columns) when Expr::Wildcard present, got {names:?}"
        );
    }

    #[test]
    fn projection_with_wildcard_leaves_scan_columns_none() {
        // SELECT *, id FROM users — wildcard means all columns; Scan.columns must be None.
        let items = vec![
            ProjectionItem {
                expr: Expr::Wildcard,
                alias: None,
            },
            ProjectionItem {
                expr: Expr::Column {
                    table: None,
                    name: "id".into(),
                },
                alias: None,
            },
        ];
        let plan = LogicalPlan::Project {
            input: Box::new(scan("users")),
            items,
        };
        let rule = ProjectionPushdown;
        let optimized = rule.apply(plan);
        if let LogicalPlan::Project { input, .. } = &optimized {
            // columns must remain None (all columns) because of the wildcard
            assert!(
                matches!(input.as_ref(), LogicalPlan::Scan { columns: None, .. }),
                "expected columns: None on scan due to wildcard, got {input:?}"
            );
        } else {
            panic!("expected Project, got {optimized:?}");
        }
    }
}
