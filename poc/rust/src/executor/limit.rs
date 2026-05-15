//! Limit and offset executor.

use super::record_batch::RecordBatch;
use super::{ExecutionContext, ExecutionPlan, Result};
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::sync::Arc;

/// Limit executor that restricts the number of output rows.
pub struct LimitExec {
    /// Input execution plan.
    pub input: Arc<dyn ExecutionPlan>,
    /// Maximum number of rows to return.
    pub limit: usize,
    /// Number of rows to skip.
    pub offset: usize,
}

impl LimitExec {
    /// Create a new limit executor.
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: usize, offset: usize) -> Self {
        Self {
            input,
            limit,
            offset,
        }
    }
}

#[async_trait]
impl ExecutionPlan for LimitExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut input_stream = self.input.execute(ctx).await?;
        let limit = self.limit;
        let offset = self.offset;

        let stream = stream! {
            let mut skipped = 0usize;
            let mut emitted = 0usize;

            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;
                let mut limited_batch = RecordBatch::new(batch.schema.clone());

                for row in batch.rows {
                    if skipped < offset {
                        skipped += 1;
                        continue;
                    }
                    if emitted >= limit {
                        break;
                    }
                    limited_batch.add_row(row);
                    emitted += 1;
                }

                if !limited_batch.is_empty() {
                    yield Ok(limited_batch);
                }

                if emitted >= limit {
                    break;
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        self.input.schema()
    }
}
