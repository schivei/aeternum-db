//! Query plan optimizer.
//!
//! The [`Optimizer`] combines a [`RuleEngine`] with a set of built-in rules and
//! exposes a single [`Optimizer::optimize`] method that transforms a
//! [`LogicalPlan`] into an equivalent, lower-cost plan.
//!
//! ## Built-in rules (applied in order)
//!
//! 1. [`ConstantFolding`] — evaluate constant sub-expressions at plan time.
//! 2. [`PredicatePushdown`] — move filters towards leaf scans.
//! 3. [`ProjectionPushdown`] — prune unreferenced columns from scans.
//!
//! Join reordering based on cardinality estimates is performed separately in
//! [`reorder_joins`] and called as a post-pass after rule convergence.

use crate::query::logical_plan::{LogicalPlan, SortExpr};
use crate::query::rules::pushdown::{PredicatePushdown, ProjectionPushdown};
use crate::query::rules::{OptimizationRule, RuleEngine};
use crate::query::statistics::StatisticsRegistry;
use crate::sql::ast::{BinaryOperator, Expr, Value};

// ── ConstantFolding ───────────────────────────────────────────────────────────

/// Evaluates constant sub-expressions at plan time.
///
/// ### Supported rewrites
///
/// - `n + m` → `n+m` (integer arithmetic)
/// - `n * m` → `n*m` (integer arithmetic)
/// - `TRUE AND x` → `x`
/// - `FALSE AND x` → `FALSE`
/// - `TRUE OR x` → `TRUE`
/// - `FALSE OR x` → `x`
/// - `NOT TRUE` → `FALSE`
/// - `NOT FALSE` → `TRUE`
pub struct ConstantFolding;

impl OptimizationRule for ConstantFolding {
    fn name(&self) -> &str {
        "constant_folding"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        rewrite_plan_exprs(plan, fold_expr)
    }
}

/// Fold a single expression if it is a constant sub-expression.
fn fold_expr(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let l = fold_expr(*left);
            let r = fold_expr(*right);
            fold_binary(l, op, r)
        }
        Expr::UnaryOp { op, expr } => {
            let inner = fold_expr(*expr);
            fold_unary(op, inner)
        }
        other => other,
    }
}

fn fold_binary(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    match (&left, &op, &right) {
        // Integer arithmetic
        (
            Expr::Literal(Value::Integer(a)),
            BinaryOperator::Plus,
            Expr::Literal(Value::Integer(b)),
        ) => Expr::Literal(Value::Integer(a + b)),
        (
            Expr::Literal(Value::Integer(a)),
            BinaryOperator::Minus,
            Expr::Literal(Value::Integer(b)),
        ) => Expr::Literal(Value::Integer(a - b)),
        (
            Expr::Literal(Value::Integer(a)),
            BinaryOperator::Multiply,
            Expr::Literal(Value::Integer(b)),
        ) => Expr::Literal(Value::Integer(a * b)),
        // Boolean short-circuit
        (Expr::Literal(Value::Boolean(true)), BinaryOperator::And, _) => right,
        (_, BinaryOperator::And, Expr::Literal(Value::Boolean(true))) => left,
        (Expr::Literal(Value::Boolean(false)), BinaryOperator::And, _) => {
            Expr::Literal(Value::Boolean(false))
        }
        (_, BinaryOperator::And, Expr::Literal(Value::Boolean(false))) => {
            Expr::Literal(Value::Boolean(false))
        }
        (Expr::Literal(Value::Boolean(true)), BinaryOperator::Or, _) => {
            Expr::Literal(Value::Boolean(true))
        }
        (_, BinaryOperator::Or, Expr::Literal(Value::Boolean(true))) => {
            Expr::Literal(Value::Boolean(true))
        }
        (Expr::Literal(Value::Boolean(false)), BinaryOperator::Or, _) => right,
        (_, BinaryOperator::Or, Expr::Literal(Value::Boolean(false))) => left,
        // No simplification possible
        _ => Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
    }
}

fn fold_unary(op: crate::sql::ast::UnaryOperator, expr: Expr) -> Expr {
    use crate::sql::ast::UnaryOperator;
    match (&op, &expr) {
        (UnaryOperator::Not, Expr::Literal(Value::Boolean(b))) => Expr::Literal(Value::Boolean(!b)),
        (UnaryOperator::Minus, Expr::Literal(Value::Integer(n))) => {
            Expr::Literal(Value::Integer(-n))
        }
        _ => Expr::UnaryOp {
            op,
            expr: Box::new(expr),
        },
    }
}

// ── Plan-level expression rewrite ─────────────────────────────────────────────

/// Walk the plan tree and apply `f` to every predicate / expression node.
fn rewrite_plan_exprs(plan: LogicalPlan, f: fn(Expr) -> Expr) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(rewrite_plan_exprs(*input, f)),
            predicate: f(predicate),
        },
        LogicalPlan::Scan {
            table,
            alias,
            columns,
            filter,
        } => LogicalPlan::Scan {
            table,
            alias,
            columns,
            filter: filter.map(f),
        },
        LogicalPlan::Project { input, items } => LogicalPlan::Project {
            input: Box::new(rewrite_plan_exprs(*input, f)),
            items,
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
        } => LogicalPlan::Join {
            left: Box::new(rewrite_plan_exprs(*left, f)),
            right: Box::new(rewrite_plan_exprs(*right, f)),
            join_type,
            condition: condition.map(f),
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
        } => LogicalPlan::Aggregate {
            input: Box::new(rewrite_plan_exprs(*input, f)),
            group_by,
            aggregates,
            having: having.map(f),
        },
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(rewrite_plan_exprs(*input, f)),
            order_by: order_by
                .into_iter()
                .map(|s| SortExpr {
                    expr: f(s.expr),
                    ascending: s.ascending,
                })
                .collect(),
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(rewrite_plan_exprs(*input, f)),
            limit,
            offset,
        },
        LogicalPlan::Unnest {
            input,
            column,
            alias,
        } => LogicalPlan::Unnest {
            input: Box::new(rewrite_plan_exprs(*input, f)),
            column: f(column),
            alias,
        },
        LogicalPlan::ViewAs { input, items } => LogicalPlan::ViewAs {
            input: Box::new(rewrite_plan_exprs(*input, f)),
            items,
        },
        other => other,
    }
}

// ── Join reordering ───────────────────────────────────────────────────────────

/// Reorder a chain of inner joins by estimated cardinality (smallest first).
///
/// Only flat chains of `Inner` joins are reordered; other join types and
/// trees with non-trivial shapes are left unchanged to preserve semantics.
pub fn reorder_joins(plan: LogicalPlan, stats: &StatisticsRegistry) -> LogicalPlan {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type: crate::sql::ast::JoinType::Inner,
            condition,
        } => {
            let left_rows = scan_rows(&left, stats);
            let right_rows = scan_rows(&right, stats);
            if right_rows < left_rows {
                LogicalPlan::Join {
                    left: Box::new(reorder_joins(*right, stats)),
                    right: Box::new(reorder_joins(*left, stats)),
                    join_type: crate::sql::ast::JoinType::Inner,
                    condition,
                }
            } else {
                LogicalPlan::Join {
                    left: Box::new(reorder_joins(*left, stats)),
                    right: Box::new(reorder_joins(*right, stats)),
                    join_type: crate::sql::ast::JoinType::Inner,
                    condition,
                }
            }
        }
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(reorder_joins(*input, stats)),
            predicate,
        },
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(reorder_joins(*input, stats)),
            order_by,
        },
        LogicalPlan::Project { input, items } => LogicalPlan::Project {
            input: Box::new(reorder_joins(*input, stats)),
            items,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
        } => LogicalPlan::Aggregate {
            input: Box::new(reorder_joins(*input, stats)),
            group_by,
            aggregates,
            having,
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(reorder_joins(*input, stats)),
            limit,
            offset,
        },
        other => other,
    }
}

fn scan_rows(plan: &LogicalPlan, stats: &StatisticsRegistry) -> usize {
    match plan {
        LogicalPlan::Scan { table, .. } => stats.get(table).num_rows,
        _ => plan.estimated_rows(),
    }
}

// ── Optimizer ─────────────────────────────────────────────────────────────────

/// Entry point for logical plan optimization.
///
/// ```rust
/// use aeternumdb_core::query::optimizer::Optimizer;
/// use aeternumdb_core::query::statistics::StatisticsRegistry;
/// use aeternumdb_core::query::logical_plan::LogicalPlan;
///
/// let stats = StatisticsRegistry::new();
/// let optimizer = Optimizer::new(&stats);
/// let plan = LogicalPlan::Values { rows: vec![] };
/// let optimized = optimizer.optimize(plan);
/// ```
pub struct Optimizer<'a> {
    engine: RuleEngine,
    stats: &'a StatisticsRegistry,
}

impl<'a> Optimizer<'a> {
    /// Create a new optimizer with the default rule set.
    pub fn new(stats: &'a StatisticsRegistry) -> Self {
        let mut engine = RuleEngine::new();
        engine.add_rule(Box::new(ConstantFolding));
        engine.add_rule(Box::new(PredicatePushdown));
        engine.add_rule(Box::new(ProjectionPushdown));
        Self { engine, stats }
    }

    /// Optimize a logical plan.
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let rule_optimized = self.engine.optimize(plan);
        reorder_joins(rule_optimized, self.stats)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::logical_plan::{LogicalPlan, ProjectionItem};
    use crate::sql::ast::{BinaryOperator, Expr, JoinType, UnaryOperator, Value};

    fn scan(t: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: t.into(),
            alias: None,
            columns: None,
            filter: None,
        }
    }

    #[test]
    fn constant_fold_integer_add() {
        let rule = ConstantFolding;
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Value::Integer(2))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Literal(Value::Integer(3))),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan("t")),
            predicate: expr,
        };
        let opt = rule.apply(plan);
        if let LogicalPlan::Filter { predicate, .. } = opt {
            assert_eq!(predicate, Expr::Literal(Value::Integer(5)));
        } else {
            panic!("expected Filter");
        }
    }

    #[test]
    fn constant_fold_true_and_x() {
        let rule = ConstantFolding;
        let x = Expr::Column {
            table: None,
            name: "active".into(),
        };
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Value::Boolean(true))),
            op: BinaryOperator::And,
            right: Box::new(x.clone()),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan("t")),
            predicate: expr,
        };
        let opt = rule.apply(plan);
        if let LogicalPlan::Filter { predicate, .. } = opt {
            assert_eq!(predicate, x);
        } else {
            panic!("expected Filter");
        }
    }

    #[test]
    fn constant_fold_not_true() {
        let rule = ConstantFolding;
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Literal(Value::Boolean(true))),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan("t")),
            predicate: expr,
        };
        let opt = rule.apply(plan);
        if let LogicalPlan::Filter { predicate, .. } = opt {
            assert_eq!(predicate, Expr::Literal(Value::Boolean(false)));
        } else {
            panic!("expected Filter");
        }
    }

    #[test]
    fn predicate_pushdown_applied_by_optimizer() {
        let mut reg = StatisticsRegistry::new();
        let mut ts = crate::query::statistics::TableStats::new("users");
        ts.num_rows = 100;
        reg.add(ts);

        let opt = Optimizer::new(&reg);
        let pred = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: "age".into(),
            }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(Value::Integer(18))),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan("users")),
            predicate: pred,
        };
        let optimized = opt.optimize(plan);
        assert!(
            matches!(
                optimized,
                LogicalPlan::Scan {
                    filter: Some(_),
                    ..
                }
            ),
            "expected scan with pushed predicate"
        );
    }

    #[test]
    fn join_reorder_puts_smaller_table_first() {
        let mut reg = StatisticsRegistry::new();
        let mut big = crate::query::statistics::TableStats::new("orders");
        big.num_rows = 100_000;
        reg.add(big);
        let mut small = crate::query::statistics::TableStats::new("users");
        small.num_rows = 500;
        reg.add(small);

        let plan = LogicalPlan::Join {
            left: Box::new(scan("orders")),
            right: Box::new(scan("users")),
            join_type: JoinType::Inner,
            condition: None,
        };
        let result = reorder_joins(plan, &reg);
        if let LogicalPlan::Join { left, .. } = &result {
            assert!(
                matches!(left.as_ref(), LogicalPlan::Scan { table, .. } if table == "users"),
                "expected 'users' (smaller) on the left after reorder"
            );
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn projection_pushdown_applied_by_optimizer() {
        let reg = StatisticsRegistry::new();
        let opt = Optimizer::new(&reg);
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
        let optimized = opt.optimize(plan);
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
}
