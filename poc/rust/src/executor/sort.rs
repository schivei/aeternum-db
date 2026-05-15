//! Sort executor.

use super::record_batch::{RecordBatch, Value};
use super::{ExecutionContext, ExecutionPlan, Result};
use crate::query::logical_plan::SortExpr;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::cmp::Ordering;
use std::sync::Arc;

/// Sort executor.
pub struct SortExec {
    /// Input execution plan.
    pub input: Arc<dyn ExecutionPlan>,
    /// Sort expressions.
    pub order_by: Vec<SortExpr>,
}

impl SortExec {
    /// Create a new sort executor.
    pub fn new(input: Arc<dyn ExecutionPlan>, order_by: Vec<SortExpr>) -> Self {
        Self { input, order_by }
    }
}

#[async_trait]
impl ExecutionPlan for SortExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut input_stream = self.input.execute(ctx).await?;
        let order_by = self.order_by.clone();

        let stream = stream! {
            let mut all_rows = Vec::new();
            let mut schema = Vec::new();

            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;
                if schema.is_empty() {
                    schema = batch.schema.clone();
                }
                all_rows.extend(batch.rows);
            }

            all_rows.sort_by(|a, b| {
                for sort_expr in &order_by {
                    let a_val = super::expressions::eval_expr(&sort_expr.expr, a).unwrap_or(Value::Null);
                    let b_val = super::expressions::eval_expr(&sort_expr.expr, b).unwrap_or(Value::Null);

                    let ord = compare_sort_values(&a_val, &b_val);
                    let ord = if !sort_expr.ascending {
                        ord.reverse()
                    } else {
                        ord
                    };

                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });

            let batch = RecordBatch::with_rows(schema, all_rows);
            yield Ok(batch);
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        self.input.schema()
    }
}

fn compare_sort_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Integer(a), Value::Float(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (Value::Float(a), Value::Integer(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}
