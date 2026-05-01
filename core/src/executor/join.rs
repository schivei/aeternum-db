//! Join executors (nested-loop, hash, sort-merge).

use super::record_batch::{RecordBatch, Row};
use super::{ExecutionContext, ExecutionPlan, Result};
use crate::sql::ast::{Expr, JoinType};
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use hashbrown::HashMap;
use std::sync::Arc;

/// Drain a batch stream into a flat row list, capturing the schema from the first batch.
async fn collect_stream_rows(
    stream: &mut BoxStream<'_, Result<RecordBatch>>,
) -> Result<(Vec<Row>, Vec<String>)> {
    let mut rows = Vec::new();
    let mut schema = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        if schema.is_empty() {
            schema = batch.schema.clone();
        }
        rows.extend(batch.rows);
    }
    Ok((rows, schema))
}

/// Nested-loop join executor.
pub struct NestedLoopJoinExec {
    /// Left (outer) input.
    pub left: Arc<dyn ExecutionPlan>,
    /// Right (inner) input.
    pub right: Arc<dyn ExecutionPlan>,
    /// Join type.
    pub join_type: JoinType,
    /// Join condition.
    pub condition: Option<Expr>,
}

impl NestedLoopJoinExec {
    /// Create a new nested-loop join executor.
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            condition,
        }
    }
}

#[async_trait]
impl ExecutionPlan for NestedLoopJoinExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut left_stream = self.left.execute(ctx).await?;
        let mut right_stream = self.right.execute(ctx).await?;
        let join_type = self.join_type.clone();
        let condition = self.condition.clone();

        let (left_rows, left_schema) = collect_stream_rows(&mut left_stream).await?;
        let (right_rows, right_schema) = collect_stream_rows(&mut right_stream).await?;

        let stream = stream! {
            let mut output_schema = left_schema.clone();
            output_schema.extend(right_schema);
            let mut output_batch = RecordBatch::new(output_schema);

            for left_row in &left_rows {
                let mut matched = false;
                for right_row in &right_rows {
                    let mut joined_row = left_row.clone();
                    joined_row.merge(right_row);

                    let passes = if let Some(ref cond) = condition {
                        match super::expressions::eval_expr(cond, &joined_row) {
                            Ok(val) => val.as_bool().unwrap_or(false),
                            Err(_) => false,
                        }
                    } else {
                        true
                    };

                    if passes {
                        matched = true;
                        output_batch.add_row(joined_row);
                    }
                }

                if !matched && matches!(join_type, JoinType::Left | JoinType::Full) {
                    output_batch.add_row(left_row.clone());
                }
            }

            if matches!(join_type, JoinType::Right | JoinType::Full) {
                for right_row in &right_rows {
                    let mut matched = false;
                    for left_row in &left_rows {
                        let mut joined_row = left_row.clone();
                        joined_row.merge(right_row);

                        let passes = if let Some(ref cond) = condition {
                            match super::expressions::eval_expr(cond, &joined_row) {
                                Ok(val) => val.as_bool().unwrap_or(false),
                                Err(_) => false,
                            }
                        } else {
                            true
                        };

                        if passes {
                            matched = true;
                            break;
                        }
                    }

                    if !matched {
                        output_batch.add_row(right_row.clone());
                    }
                }
            }

            yield Ok(output_batch);
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        let mut schema = self.left.schema();
        schema.extend(self.right.schema());
        schema
    }
}

/// Hash join executor.
pub struct HashJoinExec {
    /// Build side (left).
    pub left: Arc<dyn ExecutionPlan>,
    /// Probe side (right).
    pub right: Arc<dyn ExecutionPlan>,
    /// Join type.
    pub join_type: JoinType,
    /// Left join keys.
    pub left_keys: Vec<Expr>,
    /// Right join keys.
    pub right_keys: Vec<Expr>,
    /// Residual predicate.
    pub residual: Option<Expr>,
}

impl HashJoinExec {
    /// Create a new hash join executor.
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        residual: Option<Expr>,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            residual,
        }
    }
}

#[async_trait]
impl ExecutionPlan for HashJoinExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut left_stream = self.left.execute(ctx).await?;
        let mut right_stream = self.right.execute(ctx).await?;
        let left_keys = self.left_keys.clone();
        let right_keys = self.right_keys.clone();
        let residual = self.residual.clone();

        let (left_rows, left_schema) = collect_stream_rows(&mut left_stream).await?;
        let (right_rows, right_schema) = collect_stream_rows(&mut right_stream).await?;

        let stream = stream! {
            let mut hash_table: HashMap<String, Vec<Row>> = HashMap::new();

            for row in left_rows {
                let mut key_str = String::new();
                for key_expr in &left_keys {
                    let val = super::expressions::eval_expr(key_expr, &row)?;
                    key_str.push_str(&format!("{:?}", val));
                    key_str.push('|');
                }
                hash_table.entry(key_str).or_default().push(row);
            }

            let mut output_schema = left_schema.clone();
            output_schema.extend(right_schema);
            let mut output_batch = RecordBatch::new(output_schema);

            for right_row in &right_rows {
                let mut key_str = String::new();
                for key_expr in &right_keys {
                    let val = super::expressions::eval_expr(key_expr, right_row)?;
                    key_str.push_str(&format!("{:?}", val));
                    key_str.push('|');
                }

                if let Some(left_rows) = hash_table.get(&key_str) {
                    for left_row in left_rows {
                        let mut joined_row = left_row.clone();
                        joined_row.merge(right_row);

                        let passes = if let Some(ref res) = residual {
                            match super::expressions::eval_expr(res, &joined_row) {
                                Ok(val) => val.as_bool().unwrap_or(false),
                                Err(_) => false,
                            }
                        } else {
                            true
                        };

                        if passes {
                            output_batch.add_row(joined_row);
                        }
                    }
                }
            }

            yield Ok(output_batch);
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        let mut schema = self.left.schema();
        schema.extend(self.right.schema());
        schema
    }
}

/// Sort-merge join executor.
pub struct SortMergeJoinExec {
    /// Left (sorted) input.
    pub left: Arc<dyn ExecutionPlan>,
    /// Right (sorted) input.
    pub right: Arc<dyn ExecutionPlan>,
    /// Join type.
    pub join_type: JoinType,
    /// Left join keys.
    pub left_keys: Vec<Expr>,
    /// Right join keys.
    pub right_keys: Vec<Expr>,
}

impl SortMergeJoinExec {
    /// Create a new sort-merge join executor.
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
        }
    }
}

#[async_trait]
impl ExecutionPlan for SortMergeJoinExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut left_stream = self.left.execute(ctx).await?;
        let mut right_stream = self.right.execute(ctx).await?;
        let left_keys = self.left_keys.clone();
        let right_keys = self.right_keys.clone();

        let (left_rows, left_schema) = collect_stream_rows(&mut left_stream).await?;
        let (right_rows, right_schema) = collect_stream_rows(&mut right_stream).await?;

        let stream = stream! {
            let mut output_schema = left_schema.clone();
            output_schema.extend(right_schema);
            let mut output_batch = RecordBatch::new(output_schema);

            for left_row in &left_rows {
                for right_row in &right_rows {
                    let mut match_all = true;
                    for (left_key, right_key) in left_keys.iter().zip(right_keys.iter()) {
                        let left_val = super::expressions::eval_expr(left_key, left_row)?;
                        let right_val = super::expressions::eval_expr(right_key, right_row)?;
                        if format!("{:?}", left_val) != format!("{:?}", right_val) {
                            match_all = false;
                            break;
                        }
                    }

                    if match_all {
                        let mut joined_row = left_row.clone();
                        joined_row.merge(right_row);
                        output_batch.add_row(joined_row);
                    }
                }
            }

            yield Ok(output_batch);
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        let mut schema = self.left.schema();
        schema.extend(self.right.schema());
        schema
    }
}
