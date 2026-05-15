//! Distinct executor for duplicate elimination.

use super::record_batch::RecordBatch;
use super::{ExecutionContext, ExecutionPlan, Result};
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use hashbrown::HashSet;
use std::sync::Arc;

/// Distinct executor that eliminates duplicate rows.
pub struct DistinctExec {
    /// Input execution plan.
    pub input: Arc<dyn ExecutionPlan>,
}

impl DistinctExec {
    /// Create a new distinct executor.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

#[async_trait]
impl ExecutionPlan for DistinctExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut input_stream = self.input.execute(ctx).await?;

        let stream = stream! {
            let mut seen = HashSet::new();

            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;
                let mut distinct_batch = RecordBatch::new(batch.schema.clone());

                for row in batch.rows {
                    let mut pairs: Vec<(&String, &super::record_batch::Value)> =
                        row.columns.iter().collect();
                    pairs.sort_by_key(|(k, _)| k.as_str());
                    let key = format!("{:?}", pairs);
                    if seen.insert(key) {
                        distinct_batch.add_row(row);
                    }
                }

                if !distinct_batch.is_empty() {
                    yield Ok(distinct_batch);
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        self.input.schema()
    }
}
