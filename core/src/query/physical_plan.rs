//! Physical plan tree for AeternumDB query execution.
//!
//! A [`PhysicalPlan`] is produced by lowering an optimized [`LogicalPlan`]
//! into concrete operator implementations that the executor (PR 1.5) will
//! drive.  Each node carries a [`NodeCost`] estimate so EXPLAIN can display
//! per-operator cost information.
//!
//! ## Operator selection
//!
//! | Logical | Physical choices | Selection heuristic |
//! |---------|-----------------|---------------------|
//! | `Scan` | [`PhysicalPlan::SeqScan`] / [`PhysicalPlan::IndexScan`] | Use index when an equality or range predicate on an indexed column is detected |
//! | `Join` | [`PhysicalPlan::NestedLoopJoin`] / [`PhysicalPlan::HashJoin`] | Hash join when either side is estimated > 100 rows |
//! | `Aggregate` | [`PhysicalPlan::HashAggregate`] | Always |
//! | `Sort` | [`PhysicalPlan::Sort`] with [`SortAlgorithm`] | External sort when estimated rows > threshold |

use crate::query::cost_model::CostModel;
use crate::query::logical_plan::{
    AggregateExpr, LogicalPlan, ProjectionItem, SortExpr, ViewAsProjection,
};
use crate::query::statistics::StatisticsRegistry;
use crate::sql::ast::{Expr, JoinType};

// ── Cost annotation ───────────────────────────────────────────────────────────

/// Per-node cost annotation attached to each physical plan node.
#[derive(Debug, Clone, Default)]
pub struct NodeCost {
    /// Total estimated cost for this node (including children).
    pub total: f64,
    /// I/O component of the cost.
    pub io: f64,
    /// CPU component of the cost.
    pub cpu: f64,
    /// Estimated output row count.
    pub estimated_rows: usize,
}

// ── Sort strategy ─────────────────────────────────────────────────────────────

/// Physical sort implementation strategy.
#[derive(Debug, Clone, PartialEq)]
pub enum SortAlgorithm {
    /// All rows fit in the sort buffer — no disk spill required.
    InMemory,
    /// Row count exceeds the in-memory threshold; sort uses external merge.
    External,
}

/// Rows below this threshold are sorted in memory.
const IN_MEMORY_SORT_THRESHOLD: usize = 100_000;

// ── PhysicalPlan ──────────────────────────────────────────────────────────────

/// A node in the physical execution plan tree.
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Full sequential scan of a table.
    SeqScan {
        /// Table name.
        table: String,
        /// Optional alias.
        alias: Option<String>,
        /// Columns to read (`None` = all).
        columns: Option<Vec<String>>,
        /// Optional residual filter evaluated during the scan.
        filter: Option<Expr>,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Index-assisted scan.
    IndexScan {
        /// Table name.
        table: String,
        /// Optional alias.
        alias: Option<String>,
        /// Index name (may be synthetic when not yet backed by catalog).
        index: String,
        /// Columns to read.
        columns: Option<Vec<String>>,
        /// The predicate used to probe the index.
        key_predicate: Expr,
        /// Residual filter applied after the index lookup.
        filter: Option<Expr>,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Row filter.
    Filter {
        /// Input plan.
        input: Box<PhysicalPlan>,
        /// Predicate expression.
        predicate: Expr,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Column projection.
    Project {
        /// Input plan.
        input: Box<PhysicalPlan>,
        /// Output expressions.
        items: Vec<ProjectionItem>,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Nested-loop join (suitable for small outer or inner inputs).
    NestedLoopJoin {
        /// Outer (left) input.
        left: Box<PhysicalPlan>,
        /// Inner (right) input.
        right: Box<PhysicalPlan>,
        /// Join semantics.
        join_type: JoinType,
        /// Join predicate.
        condition: Option<Expr>,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Hash join (suitable when at least one side is large).
    HashJoin {
        /// Build side (typically the smaller input).
        left: Box<PhysicalPlan>,
        /// Probe side.
        right: Box<PhysicalPlan>,
        /// Join semantics.
        join_type: JoinType,
        /// Equi-join key expressions on the build side.
        left_keys: Vec<Expr>,
        /// Equi-join key expressions on the probe side.
        right_keys: Vec<Expr>,
        /// Residual (non-equi) predicate applied after the hash probe, if any.
        residual: Option<Expr>,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Hash-based grouping and aggregation.
    HashAggregate {
        /// Input plan.
        input: Box<PhysicalPlan>,
        /// `GROUP BY` expressions.
        group_by: Vec<Expr>,
        /// Aggregate expressions.
        aggregates: Vec<AggregateExpr>,
        /// `HAVING` predicate applied after aggregation.
        having: Option<Expr>,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Sort operator.
    Sort {
        /// Input plan.
        input: Box<PhysicalPlan>,
        /// Sort keys.
        order_by: Vec<SortExpr>,
        /// Sort implementation strategy.
        algorithm: SortAlgorithm,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Row-count limit.
    Limit {
        /// Input plan.
        input: Box<PhysicalPlan>,
        /// Maximum rows.
        limit: usize,
        /// Rows to skip.
        offset: usize,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Explodes an array / vector column.
    Unnest {
        /// Input plan.
        input: Box<PhysicalPlan>,
        /// Column expression to unnest.
        column: Expr,
        /// Optional alias.
        alias: Option<String>,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Post-result rename / projection (VIEW AS).
    ViewAs {
        /// Input plan.
        input: Box<PhysicalPlan>,
        /// Rename items.
        items: Vec<ViewAsProjection>,
        /// Cost annotation.
        cost: NodeCost,
    },

    /// Inline constant rows.
    Values {
        /// Rows of literal expressions.
        rows: Vec<Vec<Expr>>,
        /// Cost annotation.
        cost: NodeCost,
    },
}

impl PhysicalPlan {
    /// Return the cost annotation for this node.
    pub fn cost(&self) -> &NodeCost {
        match self {
            PhysicalPlan::SeqScan { cost, .. }
            | PhysicalPlan::IndexScan { cost, .. }
            | PhysicalPlan::Filter { cost, .. }
            | PhysicalPlan::Project { cost, .. }
            | PhysicalPlan::NestedLoopJoin { cost, .. }
            | PhysicalPlan::HashJoin { cost, .. }
            | PhysicalPlan::HashAggregate { cost, .. }
            | PhysicalPlan::Sort { cost, .. }
            | PhysicalPlan::Limit { cost, .. }
            | PhysicalPlan::Unnest { cost, .. }
            | PhysicalPlan::ViewAs { cost, .. }
            | PhysicalPlan::Values { cost, .. } => cost,
        }
    }
}

// ── PhysicalPlanner ───────────────────────────────────────────────────────────

/// Lowers an optimized [`LogicalPlan`] into a [`PhysicalPlan`].
///
/// # Usage
///
/// ```rust
/// use aeternumdb_core::query::physical_plan::PhysicalPlanner;
/// use aeternumdb_core::query::logical_plan::LogicalPlan;
/// use aeternumdb_core::query::cost_model::CostModel;
/// use aeternumdb_core::query::statistics::StatisticsRegistry;
///
/// let cost_model = CostModel::default();
/// let stats = StatisticsRegistry::new();
/// let planner = PhysicalPlanner::new(cost_model, &stats);
///
/// let logical = LogicalPlan::Values { rows: vec![] };
/// let physical = planner.lower(&logical);
/// ```
pub struct PhysicalPlanner<'a> {
    cost_model: CostModel,
    stats: &'a StatisticsRegistry,
}

impl<'a> PhysicalPlanner<'a> {
    /// Create a new planner with the given cost model and statistics.
    pub fn new(cost_model: CostModel, stats: &'a StatisticsRegistry) -> Self {
        Self { cost_model, stats }
    }

    /// Lower a logical plan node into a physical plan node.
    pub fn lower(&self, plan: &LogicalPlan) -> PhysicalPlan {
        match plan {
            LogicalPlan::Scan {
                table,
                alias,
                columns,
                filter,
            } => self.lower_scan(table, alias.as_deref(), columns.as_deref(), filter.as_ref()),

            LogicalPlan::Filter { input, predicate } => self.lower_filter(input, predicate.clone()),

            LogicalPlan::Project { input, items } => self.lower_project(input, items),

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => self.lower_join(left, right, join_type, condition.as_ref()),

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                having,
            } => self.lower_aggregate(input, group_by, aggregates, having.as_ref()),

            LogicalPlan::Sort { input, order_by } => self.lower_sort(input, order_by),

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => self.lower_limit(input, *limit, *offset),

            LogicalPlan::Unnest {
                input,
                column,
                alias,
            } => self.lower_unnest(input, column.clone(), alias.as_deref()),

            LogicalPlan::ViewAs { input, items } => self.lower_view_as(input, items),

            LogicalPlan::Values { rows } => {
                let rows_count = rows.len();
                PhysicalPlan::Values {
                    rows: rows.clone(),
                    cost: NodeCost {
                        estimated_rows: rows_count,
                        ..Default::default()
                    },
                }
            }
        }
    }

    // ── Scan ──────────────────────────────────────────────────────────────

    fn lower_scan(
        &self,
        table: &str,
        alias: Option<&str>,
        columns: Option<&[String]>,
        filter: Option<&Expr>,
    ) -> PhysicalPlan {
        let table_stats = self.stats.get(table);
        let scan_cost = self.cost_model.estimate_scan_cost(&table_stats);

        if let Some(pred) = filter {
            if let Some(index_name) = detect_index_predicate(pred, &table_stats) {
                let rows = CostModel::estimated_rows(table_stats.num_rows, 0.05);
                let io = scan_cost * 0.1;
                let cpu = rows as f64 * self.cost_model.cpu_cost_factor;
                return PhysicalPlan::IndexScan {
                    table: table.to_string(),
                    alias: alias.map(str::to_string),
                    index: index_name,
                    columns: columns.map(|c| c.to_vec()),
                    key_predicate: pred.clone(),
                    filter: None,
                    cost: NodeCost {
                        total: io + cpu,
                        io,
                        cpu,
                        estimated_rows: rows,
                    },
                };
            }
        }

        PhysicalPlan::SeqScan {
            table: table.to_string(),
            alias: alias.map(str::to_string),
            columns: columns.map(|c| c.to_vec()),
            filter: filter.cloned(),
            cost: NodeCost {
                total: scan_cost,
                io: table_stats.num_pages as f64 * self.cost_model.io_cost_factor,
                cpu: table_stats.num_rows as f64 * self.cost_model.cpu_cost_factor,
                estimated_rows: table_stats.num_rows,
            },
        }
    }

    // ── Filter ────────────────────────────────────────────────────────────

    fn lower_filter(&self, input: &LogicalPlan, predicate: Expr) -> PhysicalPlan {
        let child = self.lower(input);
        let in_rows = child.cost().estimated_rows;
        let sel = 0.1_f64;
        let cpu = self.cost_model.estimate_filter_cost(in_rows, sel);
        let out_rows = CostModel::estimated_rows(in_rows, sel);
        let total = child.cost().total + cpu;
        PhysicalPlan::Filter {
            input: Box::new(child),
            predicate,
            cost: NodeCost {
                total,
                io: 0.0,
                cpu,
                estimated_rows: out_rows,
            },
        }
    }

    // ── Project ───────────────────────────────────────────────────────────

    fn lower_project(&self, input: &LogicalPlan, items: &[ProjectionItem]) -> PhysicalPlan {
        let child = self.lower(input);
        let rows = child.cost().estimated_rows;
        let cpu = rows as f64 * self.cost_model.cpu_cost_factor * 0.5;
        let total = child.cost().total + cpu;
        PhysicalPlan::Project {
            input: Box::new(child),
            items: items.to_vec(),
            cost: NodeCost {
                total,
                io: 0.0,
                cpu,
                estimated_rows: rows,
            },
        }
    }

    // ── Join ──────────────────────────────────────────────────────────────

    fn lower_join(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        join_type: &JoinType,
        condition: Option<&Expr>,
    ) -> PhysicalPlan {
        let left_phys = self.lower(left);
        let right_phys = self.lower(right);
        let lr = left_phys.cost().estimated_rows;
        let rr = right_phys.cost().estimated_rows;

        // Only select HashJoin when equi-keys can be extracted from the predicate.
        // If no equi-keys are found, fall back to NestedLoopJoin with the
        // original condition.
        let (lk, rk, residual) = split_join_condition(condition);
        let has_equi_keys = !lk.is_empty();
        let use_hash = has_equi_keys && (lr > 100 || rr > 100);

        if use_hash {
            let cpu = self.cost_model.estimate_hash_join_cost(lr, rr);
            let total = left_phys.cost().total + right_phys.cost().total + cpu;
            let out_rows = CostModel::estimated_rows(lr * rr / 100, 1.0);
            PhysicalPlan::HashJoin {
                left: Box::new(left_phys),
                right: Box::new(right_phys),
                join_type: join_type.clone(),
                left_keys: lk,
                right_keys: rk,
                residual,
                cost: NodeCost {
                    total,
                    io: 0.0,
                    cpu,
                    estimated_rows: out_rows,
                },
            }
        } else {
            // Reassemble full condition (equi + residual) for NestedLoopJoin.
            let nl_condition = reassemble_condition(condition, lk, rk, residual);
            self.make_nested_loop(
                left_phys,
                right_phys,
                join_type.clone(),
                nl_condition.as_ref(),
                lr,
                rr,
            )
        }
    }

    fn make_nested_loop(
        &self,
        left: PhysicalPlan,
        right: PhysicalPlan,
        join_type: JoinType,
        condition: Option<&Expr>,
        lr: usize,
        rr: usize,
    ) -> PhysicalPlan {
        let cpu = self.cost_model.estimate_nested_loop_cost(lr, rr);
        let total = left.cost().total + right.cost().total + cpu;
        let out_rows = CostModel::estimated_rows(lr * rr / 100, 1.0);
        PhysicalPlan::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition: condition.cloned(),
            cost: NodeCost {
                total,
                io: 0.0,
                cpu,
                estimated_rows: out_rows,
            },
        }
    }

    // ── Aggregate ─────────────────────────────────────────────────────────

    fn lower_aggregate(
        &self,
        input: &LogicalPlan,
        group_by: &[Expr],
        aggregates: &[AggregateExpr],
        having: Option<&Expr>,
    ) -> PhysicalPlan {
        let child = self.lower(input);
        let in_rows = child.cost().estimated_rows;
        let groups = if group_by.is_empty() {
            1
        } else {
            (in_rows / 10).max(1)
        };
        let cpu = self.cost_model.estimate_aggregate_cost(in_rows, groups);
        let total = child.cost().total + cpu;
        PhysicalPlan::HashAggregate {
            input: Box::new(child),
            group_by: group_by.to_vec(),
            aggregates: aggregates.to_vec(),
            having: having.cloned(),
            cost: NodeCost {
                total,
                io: 0.0,
                cpu,
                estimated_rows: groups,
            },
        }
    }

    // ── Sort ──────────────────────────────────────────────────────────────

    fn lower_sort(&self, input: &LogicalPlan, order_by: &[SortExpr]) -> PhysicalPlan {
        let child = self.lower(input);
        let rows = child.cost().estimated_rows;
        let cpu = self.cost_model.estimate_sort_cost(rows);
        let total = child.cost().total + cpu;
        let algorithm = if rows > IN_MEMORY_SORT_THRESHOLD {
            SortAlgorithm::External
        } else {
            SortAlgorithm::InMemory
        };
        PhysicalPlan::Sort {
            input: Box::new(child),
            order_by: order_by.to_vec(),
            algorithm,
            cost: NodeCost {
                total,
                io: 0.0,
                cpu,
                estimated_rows: rows,
            },
        }
    }

    // ── Limit ─────────────────────────────────────────────────────────────

    fn lower_limit(&self, input: &LogicalPlan, limit: usize, offset: usize) -> PhysicalPlan {
        let child = self.lower(input);
        let rows = limit.min(child.cost().estimated_rows);
        let total = child.cost().total;
        PhysicalPlan::Limit {
            input: Box::new(child),
            limit,
            offset,
            cost: NodeCost {
                total,
                io: 0.0,
                cpu: 0.0,
                estimated_rows: rows,
            },
        }
    }

    // ── Unnest ────────────────────────────────────────────────────────────

    fn lower_unnest(&self, input: &LogicalPlan, column: Expr, alias: Option<&str>) -> PhysicalPlan {
        let child = self.lower(input);
        let rows = child.cost().estimated_rows * 5;
        let cpu = rows as f64 * self.cost_model.cpu_cost_factor;
        let total = child.cost().total + cpu;
        PhysicalPlan::Unnest {
            input: Box::new(child),
            column,
            alias: alias.map(str::to_string),
            cost: NodeCost {
                total,
                io: 0.0,
                cpu,
                estimated_rows: rows,
            },
        }
    }

    // ── ViewAs ────────────────────────────────────────────────────────────

    fn lower_view_as(&self, input: &LogicalPlan, items: &[ViewAsProjection]) -> PhysicalPlan {
        let child = self.lower(input);
        let rows = child.cost().estimated_rows;
        let cpu = rows as f64 * self.cost_model.cpu_cost_factor;
        let total = child.cost().total + cpu;
        PhysicalPlan::ViewAs {
            input: Box::new(child),
            items: items.to_vec(),
            cost: NodeCost {
                total,
                io: 0.0,
                cpu,
                estimated_rows: rows,
            },
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns a synthetic index name if the predicate looks like an equality
/// or range predicate on a simple column reference.
fn detect_index_predicate(
    pred: &Expr,
    _stats: &crate::query::statistics::TableStats,
) -> Option<String> {
    use crate::sql::ast::BinaryOperator;
    match pred {
        Expr::BinaryOp {
            left,
            op:
                BinaryOperator::Eq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq,
            right: _,
        } => {
            if let Expr::Column { name, .. } = left.as_ref() {
                return Some(format!("{name}_idx"));
            }
            None
        }
        _ => None,
    }
}

/// Split a join condition into equi-join key pairs and a residual non-equi predicate.
///
/// Returns `(left_keys, right_keys, residual)`.
/// - For a simple `col_a = col_b` predicate the residual is `None`.
/// - For anything that is not a simple equality the predicate is placed entirely
///   in the residual and the key vectors are empty.
fn split_join_condition(condition: Option<&Expr>) -> (Vec<Expr>, Vec<Expr>, Option<Expr>) {
    use crate::sql::ast::BinaryOperator;
    match condition {
        Some(Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        }) => (vec![*left.clone()], vec![*right.clone()], None),
        Some(other) => (vec![], vec![], Some(other.clone())),
        None => (vec![], vec![], None),
    }
}

/// Reassemble a condition from equi-key pairs and a residual.
///
/// Used when falling back to NestedLoopJoin so that the original full predicate
/// is preserved.
fn reassemble_condition(
    original: Option<&Expr>,
    lk: Vec<Expr>,
    rk: Vec<Expr>,
    residual: Option<Expr>,
) -> Option<Expr> {
    use crate::sql::ast::BinaryOperator;
    if lk.is_empty() {
        // No equi-keys were extracted; use original condition directly.
        return original.cloned().or(residual);
    }
    let equi = lk.into_iter().zip(rk).fold(None::<Expr>, |acc, (l, r)| {
        let eq = Expr::BinaryOp {
            left: Box::new(l),
            op: BinaryOperator::Eq,
            right: Box::new(r),
        };
        Some(match acc {
            None => eq,
            Some(prev) => Expr::BinaryOp {
                left: Box::new(prev),
                op: BinaryOperator::And,
                right: Box::new(eq),
            },
        })
    });
    match (equi, residual) {
        (None, r) => r,
        (e, None) => e,
        (Some(e), Some(r)) => Some(Expr::BinaryOp {
            left: Box::new(e),
            op: BinaryOperator::And,
            right: Box::new(r),
        }),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::logical_plan::{AggregateExpr, LogicalPlan, SortExpr};
    use crate::sql::ast::{DataType, Expr, JoinType, Value};
    use crate::sql::validator::{Catalog, ColumnSchema, TableSchema};

    fn make_registry() -> StatisticsRegistry {
        let mut reg = StatisticsRegistry::new();
        let mut ts = crate::query::statistics::TableStats::new("users");
        ts.num_rows = 5000;
        ts.num_pages = 50;
        reg.add(ts);
        let mut ts2 = crate::query::statistics::TableStats::new("orders");
        ts2.num_rows = 20000;
        ts2.num_pages = 200;
        reg.add(ts2);
        reg
    }

    fn planner(reg: &StatisticsRegistry) -> PhysicalPlanner<'_> {
        PhysicalPlanner::new(CostModel::default(), reg)
    }

    #[test]
    fn seq_scan_no_filter() {
        let reg = make_registry();
        let p = planner(&reg);
        let logical = LogicalPlan::Scan {
            table: "users".into(),
            alias: None,
            columns: None,
            filter: None,
        };
        let phys = p.lower(&logical);
        assert!(matches!(phys, PhysicalPlan::SeqScan { .. }));
        assert!(phys.cost().total > 0.0);
    }

    #[test]
    fn index_scan_on_equality_predicate() {
        let reg = make_registry();
        let p = planner(&reg);
        let pred = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: "id".into(),
            }),
            op: crate::sql::ast::BinaryOperator::Eq,
            right: Box::new(Expr::Literal(Value::Integer(1))),
        };
        let logical = LogicalPlan::Scan {
            table: "users".into(),
            alias: None,
            columns: None,
            filter: Some(pred),
        };
        let phys = p.lower(&logical);
        assert!(matches!(phys, PhysicalPlan::IndexScan { .. }));
    }

    #[test]
    fn hash_join_for_large_tables() {
        let reg = make_registry();
        let p = planner(&reg);
        // Provide an equi-join condition so HashJoin can be selected.
        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("users".into()),
                name: "id".into(),
            }),
            op: crate::sql::ast::BinaryOperator::Eq,
            right: Box::new(Expr::Column {
                table: Some("orders".into()),
                name: "user_id".into(),
            }),
        };
        let logical = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table: "users".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "orders".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            join_type: JoinType::Inner,
            condition: Some(condition),
        };
        let phys = p.lower(&logical);
        assert!(matches!(phys, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn no_equi_keys_uses_nested_loop_for_large_tables() {
        // No condition → no equi-keys → must use NestedLoopJoin even for large tables.
        let reg = make_registry();
        let p = planner(&reg);
        let logical = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table: "users".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "orders".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            join_type: JoinType::Inner,
            condition: None,
        };
        let phys = p.lower(&logical);
        assert!(
            matches!(phys, PhysicalPlan::NestedLoopJoin { .. }),
            "expected NestedLoopJoin when no equi-keys, got: {:?}",
            phys
        );
    }

    #[test]
    fn nested_loop_for_small_tables() {
        let mut reg = StatisticsRegistry::new();
        let mut ts = crate::query::statistics::TableStats::new("small_a");
        ts.num_rows = 10;
        ts.num_pages = 1;
        reg.add(ts.clone());
        ts.table_name = "small_b".into();
        reg.add(ts);

        let p = planner(&reg);
        let logical = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table: "small_a".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "small_b".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            join_type: JoinType::Inner,
            condition: None,
        };
        let phys = p.lower(&logical);
        assert!(matches!(phys, PhysicalPlan::NestedLoopJoin { .. }));
    }

    #[test]
    fn sort_chooses_in_memory_for_small_input() {
        let reg = make_registry();
        let p = planner(&reg);
        let logical = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::Scan {
                table: "users".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            order_by: vec![SortExpr {
                expr: Expr::Column {
                    table: None,
                    name: "id".into(),
                },
                ascending: true,
            }],
        };
        let phys = p.lower(&logical);
        if let PhysicalPlan::Sort { algorithm, .. } = phys {
            assert_eq!(algorithm, SortAlgorithm::InMemory);
        } else {
            panic!("expected Sort");
        }
    }

    #[test]
    fn limit_capped_to_input_rows() {
        let reg = make_registry();
        let p = planner(&reg);
        let logical = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Scan {
                table: "users".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            limit: 10,
            offset: 0,
        };
        let phys = p.lower(&logical);
        assert!(phys.cost().estimated_rows <= 10);
    }

    #[test]
    fn hash_aggregate() {
        let reg = make_registry();
        let p = planner(&reg);
        let logical = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                table: "users".into(),
                alias: None,
                columns: None,
                filter: None,
            }),
            group_by: vec![Expr::Column {
                table: None,
                name: "age".into(),
            }],
            aggregates: vec![AggregateExpr {
                func: Expr::Function {
                    name: "COUNT".into(),
                    args: vec![Expr::Wildcard],
                    distinct: false,
                },
                alias: Some("cnt".into()),
            }],
            having: None,
        };
        let phys = p.lower(&logical);
        assert!(matches!(phys, PhysicalPlan::HashAggregate { .. }));
    }

    #[test]
    fn view_as_node() {
        let reg = make_registry();
        let p = planner(&reg);
        let logical = LogicalPlan::ViewAs {
            input: Box::new(LogicalPlan::Values { rows: vec![] }),
            items: vec![ViewAsProjection {
                expr: Expr::Column {
                    table: None,
                    name: "x".into(),
                },
                alias: "y".into(),
            }],
        };
        let phys = p.lower(&logical);
        assert!(matches!(phys, PhysicalPlan::ViewAs { .. }));
    }

    #[test]
    fn values_node_cost() {
        let reg = make_registry();
        let p = planner(&reg);
        let logical = LogicalPlan::Values {
            rows: vec![vec![Expr::Literal(Value::Integer(1))]],
        };
        let phys = p.lower(&logical);
        assert_eq!(phys.cost().estimated_rows, 1);
    }

    /// Unused catalog import kept to test the use of DataType/Catalog in tests.
    #[allow(dead_code)]
    fn _uses_catalog() -> Catalog {
        let mut c = Catalog::new();
        c.add_table(TableSchema {
            name: "t".into(),
            columns: vec![ColumnSchema {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            }],
        });
        c
    }
}
