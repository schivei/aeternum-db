//! Build executor from physical plan.

use super::aggregate::HashAggregateExec;
use super::distinct::DistinctExec;
use super::filter::FilterExec;
use super::join::{HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec};
use super::limit::LimitExec;
use super::project::ProjectExec;
use super::scan::{IndexScanExec, SeqScanExec};
use super::sort::SortExec;
use super::unnest::UnnestExec;
use super::values::ValuesExec;
use super::{ExecutionPlan, Result};
use crate::query::physical_plan::PhysicalPlan;
use std::sync::Arc;

/// Build an execution plan from a physical plan.
pub fn build_executor(plan: &PhysicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
    match plan {
        PhysicalPlan::SeqScan {
            table,
            alias,
            columns,
            filter,
            ..
        } => Ok(Arc::new(SeqScanExec::new(
            table.clone(),
            alias.clone(),
            columns.clone(),
            filter.clone(),
        ))),

        PhysicalPlan::IndexScan {
            table,
            alias,
            index,
            columns,
            key_predicate,
            filter,
            ..
        } => Ok(Arc::new(IndexScanExec::new(
            table.clone(),
            alias.clone(),
            index.clone(),
            columns.clone(),
            key_predicate.clone(),
            filter.clone(),
        ))),

        PhysicalPlan::Filter {
            input, predicate, ..
        } => {
            let input_exec = build_executor(input)?;
            Ok(Arc::new(FilterExec::new(input_exec, predicate.clone())))
        }

        PhysicalPlan::Project { input, items, .. } => {
            let input_exec = build_executor(input)?;
            Ok(Arc::new(ProjectExec::new(input_exec, items.clone())))
        }

        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let left_exec = build_executor(left)?;
            let right_exec = build_executor(right)?;
            Ok(Arc::new(NestedLoopJoinExec::new(
                left_exec,
                right_exec,
                join_type.clone(),
                condition.clone(),
            )))
        }

        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            residual,
            ..
        } => {
            let left_exec = build_executor(left)?;
            let right_exec = build_executor(right)?;
            Ok(Arc::new(HashJoinExec::new(
                left_exec,
                right_exec,
                join_type.clone(),
                left_keys.clone(),
                right_keys.clone(),
                residual.clone(),
            )))
        }

        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            having,
            ..
        } => {
            let input_exec = build_executor(input)?;
            Ok(Arc::new(HashAggregateExec::new(
                input_exec,
                group_by.clone(),
                aggregates.clone(),
                having.clone(),
            )))
        }

        PhysicalPlan::Sort {
            input, order_by, ..
        } => {
            let input_exec = build_executor(input)?;
            Ok(Arc::new(SortExec::new(input_exec, order_by.clone())))
        }

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
            ..
        } => {
            let input_exec = build_executor(input)?;
            Ok(Arc::new(LimitExec::new(input_exec, *limit, *offset)))
        }

        PhysicalPlan::Unnest {
            input,
            column,
            alias,
            ..
        } => {
            let input_exec = build_executor(input)?;
            Ok(Arc::new(UnnestExec::new(
                input_exec,
                column.clone(),
                alias.clone(),
            )))
        }

        PhysicalPlan::ViewAs { input, items, .. } => {
            let input_exec = build_executor(input)?;
            let projection_items: Vec<crate::query::logical_plan::ProjectionItem> = items
                .iter()
                .map(|item| crate::query::logical_plan::ProjectionItem {
                    expr: item.expr.clone(),
                    alias: Some(item.alias.clone()),
                })
                .collect();
            Ok(Arc::new(ProjectExec::new(input_exec, projection_items)))
        }

        PhysicalPlan::Values { rows, .. } => {
            let schema: Vec<String> = (0..rows.get(0).map(|r| r.len()).unwrap_or(0))
                .map(|i| format!("col_{}", i))
                .collect();
            Ok(Arc::new(ValuesExec::new(rows.clone(), schema)))
        }
    }
}

/// Build a distinct executor (helper for DISTINCT queries).
pub fn build_distinct_executor(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(DistinctExec::new(input))
}

/// Build a sort-merge join executor (helper).
pub fn build_sort_merge_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: crate::sql::ast::JoinType,
    left_keys: Vec<crate::sql::ast::Expr>,
    right_keys: Vec<crate::sql::ast::Expr>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortMergeJoinExec::new(
        left, right, join_type, left_keys, right_keys,
    ))
}
