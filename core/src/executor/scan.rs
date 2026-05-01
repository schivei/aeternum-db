//! Scan executors (sequential and index-based).

use super::record_batch::RecordBatch;
use super::{ExecutionContext, ExecutionPlan, Result};
use crate::sql::ast::Expr;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::BoxStream;

/// Sequential table scan executor.
pub struct SeqScanExec {
    /// Table name.
    pub table: String,
    /// Optional table alias.
    pub alias: Option<String>,
    /// Columns to read (None = all columns).
    pub columns: Option<Vec<String>>,
    /// Optional filter predicate.
    pub filter: Option<Expr>,
}

impl SeqScanExec {
    /// Create a new sequential scan executor.
    pub fn new(
        table: String,
        alias: Option<String>,
        columns: Option<Vec<String>>,
        filter: Option<Expr>,
    ) -> Self {
        Self {
            table,
            alias,
            columns,
            filter,
        }
    }
}

#[async_trait]
impl ExecutionPlan for SeqScanExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let rows = ctx.table_provider.scan(&self.table).await?;
        let schema = ctx.table_provider.schema(&self.table)?;
        let filter = self.filter.clone();
        let columns = self.columns.clone();

        let stream = stream! {
            let col_names: Vec<String> = if let Some(cols) = columns {
                cols
            } else {
                schema.iter().map(|meta| meta.name.clone()).collect()
            };

            let mut batch = RecordBatch::new(col_names.clone());

            for row in rows {
                if let Some(ref f) = filter {
                    let result = super::expressions::eval_expr(f, &row)?;
                    if let Some(true) = result.as_bool() {
                        let mut projected_row = super::record_batch::Row::new();
                        for col in &col_names {
                            let val = row.get(col).cloned().unwrap_or(super::record_batch::Value::Null);
                            projected_row.insert(col.clone(), val);
                        }
                        batch.add_row(projected_row);
                    } else if result.is_null() {
                        continue;
                    }
                } else {
                    let mut projected_row = super::record_batch::Row::new();
                    for col in &col_names {
                        let val = row.get(col).cloned().unwrap_or(super::record_batch::Value::Null);
                        projected_row.insert(col.clone(), val);
                    }
                    batch.add_row(projected_row);
                }
            }

            yield Ok(batch);
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        self.columns
            .as_ref()
            .map(|cols| {
                cols.iter()
                    .map(|c| (c.clone(), "unknown".to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Index scan executor.
pub struct IndexScanExec {
    /// Table name.
    pub table: String,
    /// Optional table alias.
    pub alias: Option<String>,
    /// Index name.
    pub index: String,
    /// Columns to read.
    pub columns: Option<Vec<String>>,
    /// Index lookup predicate.
    pub key_predicate: Expr,
    /// Residual filter.
    pub filter: Option<Expr>,
}

impl IndexScanExec {
    /// Create a new index scan executor.
    pub fn new(
        table: String,
        alias: Option<String>,
        index: String,
        columns: Option<Vec<String>>,
        key_predicate: Expr,
        filter: Option<Expr>,
    ) -> Self {
        Self {
            table,
            alias,
            index,
            columns,
            key_predicate,
            filter,
        }
    }
}

#[async_trait]
impl ExecutionPlan for IndexScanExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let rows = ctx.table_provider.scan(&self.table).await?;
        let schema = ctx.table_provider.schema(&self.table)?;
        let key_predicate = self.key_predicate.clone();
        let filter = self.filter.clone();
        let columns = self.columns.clone();

        let stream = stream! {
            let col_names: Vec<String> = if let Some(cols) = columns {
                cols
            } else {
                schema.iter().map(|meta| meta.name.clone()).collect()
            };

            let mut batch = RecordBatch::new(col_names.clone());

            for row in rows {
                let key_match = super::expressions::eval_expr(&key_predicate, &row)?;
                if let Some(true) = key_match.as_bool() {
                    if let Some(ref f) = filter {
                        let result = super::expressions::eval_expr(f, &row)?;
                        if let Some(true) = result.as_bool() {
                            let mut projected_row = super::record_batch::Row::new();
                            for col in &col_names {
                                let val = row.get(col).cloned().unwrap_or(super::record_batch::Value::Null);
                                projected_row.insert(col.clone(), val);
                            }
                            batch.add_row(projected_row);
                        }
                    } else {
                        let mut projected_row = super::record_batch::Row::new();
                        for col in &col_names {
                            let val = row.get(col).cloned().unwrap_or(super::record_batch::Value::Null);
                            projected_row.insert(col.clone(), val);
                        }
                        batch.add_row(projected_row);
                    }
                }
            }

            yield Ok(batch);
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        self.columns
            .as_ref()
            .map(|cols| {
                cols.iter()
                    .map(|c| (c.clone(), "unknown".to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }
}
