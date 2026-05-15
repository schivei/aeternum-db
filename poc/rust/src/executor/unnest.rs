//! Unnest executor for array/vector explosion.

use super::record_batch::RecordBatch;
use super::{ExecutionContext, ExecutionPlan, Result};
use crate::sql::ast::Expr;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::sync::Arc;

/// Unnest executor that explodes array columns.
pub struct UnnestExec {
    /// Input execution plan.
    pub input: Arc<dyn ExecutionPlan>,
    /// Column expression to unnest.
    pub column: Expr,
    /// Optional alias for unnested column.
    pub alias: Option<String>,
}

impl UnnestExec {
    /// Create a new unnest executor.
    pub fn new(input: Arc<dyn ExecutionPlan>, column: Expr, alias: Option<String>) -> Self {
        Self {
            input,
            column,
            alias,
        }
    }
}

#[async_trait]
impl ExecutionPlan for UnnestExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut input_stream = self.input.execute(ctx).await?;
        let column = self.column.clone();
        let alias = self.alias.clone().unwrap_or_else(|| "unnest".to_string());

        let stream = stream! {
            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;
                let mut schema = batch.schema.clone();
                schema.push(alias.clone());
                let mut unnested_batch = RecordBatch::new(schema);

                for row in batch.rows {
                    let array_val = super::expressions::eval_expr(&column, &row)?;
                    if let Some(array) = array_val.as_array() {
                        for item in array {
                            let mut new_row = row.clone();
                            new_row.insert(alias.clone(), item);
                            unnested_batch.add_row(new_row);
                        }
                    } else if !array_val.is_null() {
                        let mut new_row = row.clone();
                        new_row.insert(alias.clone(), array_val);
                        unnested_batch.add_row(new_row);
                    }
                }

                yield Ok(unnested_batch);
            }
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        let mut schema = self.input.schema();
        let alias = self.alias.clone().unwrap_or_else(|| "unnest".to_string());
        schema.push((alias, "unknown".to_string()));
        schema
    }
}
