//! Filter executor for row-level predicates.

use super::record_batch::RecordBatch;
use super::{ExecutionContext, ExecutionPlan, Result};
use crate::sql::ast::Expr;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::sync::Arc;

/// Filter executor that evaluates a predicate on each row.
pub struct FilterExec {
    /// Input execution plan.
    pub input: Arc<dyn ExecutionPlan>,
    /// Filter predicate.
    pub predicate: Expr,
}

impl FilterExec {
    /// Create a new filter executor.
    pub fn new(input: Arc<dyn ExecutionPlan>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

#[async_trait]
impl ExecutionPlan for FilterExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut input_stream = self.input.execute(ctx).await?;
        let predicate = self.predicate.clone();

        let stream = stream! {
            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;
                let mut filtered_batch = RecordBatch::new(batch.schema.clone());

                for row in batch.rows {
                    let result = super::expressions::eval_expr(&predicate, &row)?;
                    if let Some(true) = result.as_bool() {
                        filtered_batch.add_row(row);
                    }
                }

                if !filtered_batch.is_empty() {
                    yield Ok(filtered_batch);
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        self.input.schema()
    }
}
