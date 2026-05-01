//! Project executor for column projection and computed expressions.

use super::record_batch::{RecordBatch, Row};
use super::{ExecutionContext, ExecutionPlan, Result};
use crate::query::logical_plan::ProjectionItem;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::sync::Arc;

/// Projection executor.
pub struct ProjectExec {
    /// Input execution plan.
    pub input: Arc<dyn ExecutionPlan>,
    /// Projection items.
    pub items: Vec<ProjectionItem>,
}

impl ProjectExec {
    /// Create a new projection executor.
    pub fn new(input: Arc<dyn ExecutionPlan>, items: Vec<ProjectionItem>) -> Self {
        Self { input, items }
    }
}

#[async_trait]
impl ExecutionPlan for ProjectExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut input_stream = self.input.execute(ctx).await?;
        let items = self.items.clone();

        let stream = stream! {
            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;
                let output_schema: Vec<String> = items.iter().enumerate()
                    .map(|(i, item)| item.alias.clone().unwrap_or_else(|| format!("col_{}", i)))
                    .collect();
                let mut projected_batch = RecordBatch::new(output_schema.clone());

                for row in batch.rows {
                    let mut projected_row = Row::new();
                    for (i, item) in items.iter().enumerate() {
                        let col_name = item.alias.clone().unwrap_or_else(|| format!("col_{}", i));
                        let val = super::expressions::eval_expr(&item.expr, &row)?;
                        projected_row.insert(col_name, val);
                    }
                    projected_batch.add_row(projected_row);
                }

                yield Ok(projected_batch);
            }
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        self.items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                let name = item.alias.clone().unwrap_or_else(|| format!("col_{}", i));
                (name, "unknown".to_string())
            })
            .collect()
    }
}
