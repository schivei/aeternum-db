//! VALUES executor for inline constant rows.

use super::record_batch::{RecordBatch, Row};
use super::{ExecutionContext, ExecutionPlan, Result};
use crate::sql::ast::Expr;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::BoxStream;

/// Executor for inline constant rows (VALUES clause).
pub struct ValuesExec {
    /// Rows of literal expressions.
    pub rows: Vec<Vec<Expr>>,
    /// Output schema (column names).
    pub schema: Vec<String>,
}

impl ValuesExec {
    /// Create a new VALUES executor.
    pub fn new(rows: Vec<Vec<Expr>>, schema: Vec<String>) -> Self {
        Self { rows, schema }
    }
}

#[async_trait]
impl ExecutionPlan for ValuesExec {
    async fn execute(
        &self,
        _ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let rows = self.rows.clone();
        let schema = self.schema.clone();

        let stream = stream! {
            let mut batch = RecordBatch::new(schema.clone());
            let empty_row = Row::new();

            for row_exprs in &rows {
                let mut row = Row::new();
                for (i, expr) in row_exprs.iter().enumerate() {
                    let col_name = if i < schema.len() {
                        schema[i].clone()
                    } else {
                        format!("col_{}", i)
                    };
                    let val = super::expressions::eval_expr(expr, &empty_row)?;
                    row.insert(col_name, val);
                }
                batch.add_row(row);
            }

            yield Ok(batch);
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        self.schema
            .iter()
            .map(|name| (name.clone(), "unknown".to_string()))
            .collect()
    }
}
