//! Query executor for AeternumDB.
//!
//! The executor takes a [`PhysicalPlan`] tree and produces
//! [`RecordBatch`] results by driving operator implementations.
//!
//! ## Architecture
//!
//! - **ExecutionPlan trait**: Async stream-based pull model
//! - **RecordBatch**: Row-oriented result batches
//! - **ExecutionContext**: Table access, ACL, and objid generation
//! - **Expression evaluator**: 30+ SQL functions with NULL propagation
//!
//! ## Operators
//!
//! | Operator | Implementation |
//! |----------|---------------|
//! | SeqScan | Table scan via TableProvider |
//! | IndexScan | Index-assisted scan |
//! | Filter | Row-level predicate evaluation |
//! | Project | Column projection and computed expressions |
//! | NestedLoopJoin | Cartesian product with filter |
//! | HashJoin | Hash-based equi-join |
//! | SortMergeJoin | Sorted input merge |
//! | HashAggregate | Hash-based GROUP BY with accumulators |
//! | Sort | In-memory or external merge sort |
//! | Limit | Row count limit and offset |
//! | Unnest | Array/vector explosion |
//! | Distinct | Hash-based duplicate elimination |
//! | Values | Inline constant rows |
//!
//! See [`build_executor`] to map a [`PhysicalPlan`] to an [`ExecutionPlan`].

mod aggregate;
mod context;
mod distinct;
mod dml;
mod expressions;
mod filter;
mod join;
mod limit;
mod physical_plan;
mod project;
mod record_batch;
mod scan;
mod sort;
mod unnest;
mod values;

pub use aggregate::HashAggregateExec;
pub use context::{
    AtomicIdGenerator, ExecutionContext, InMemoryTableProvider, ObjIdGenerator, TableProvider, ACL,
};
pub use distinct::DistinctExec;
pub use dml::{
    apply_referential_action, check_referential_integrity, execute_delete, execute_grant,
    execute_insert, execute_revoke, execute_update,
};
pub use filter::FilterExec;
pub use join::{HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec};
pub use limit::LimitExec;
pub use physical_plan::{build_distinct_executor, build_executor, build_sort_merge_join};
pub use project::ProjectExec;
pub use record_batch::{RecordBatch, Row, Value};
pub use scan::{IndexScanExec, SeqScanExec};
pub use sort::SortExec;
pub use unnest::UnnestExec;
pub use values::ValuesExec;

use async_trait::async_trait;
use futures::stream::BoxStream;
use std::fmt;

/// Error type for query execution.
#[derive(Debug, Clone)]
pub enum ExecutorError {
    /// Table not found in context.
    TableNotFound(String),
    /// Column not found in record batch or schema.
    ColumnNotFound(String),
    /// Type mismatch during expression evaluation.
    TypeMismatch { expected: String, got: String },
    /// Expression evaluation error.
    EvalError(String),
    /// I/O error during execution.
    IoError(String),
    /// Permission denied for operation.
    PermissionDenied(String),
    /// Referential integrity violation.
    ReferentialIntegrityViolation(String),
    /// Other execution error.
    Other(String),
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorError::TableNotFound(t) => write!(f, "Table not found: {}", t),
            ExecutorError::ColumnNotFound(c) => write!(f, "Column not found: {}", c),
            ExecutorError::TypeMismatch { expected, got } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, got)
            }
            ExecutorError::EvalError(e) => write!(f, "Evaluation error: {}", e),
            ExecutorError::IoError(e) => write!(f, "I/O error: {}", e),
            ExecutorError::PermissionDenied(msg) => write!(f, "Permission denied: {}", msg),
            ExecutorError::ReferentialIntegrityViolation(msg) => {
                write!(f, "Referential integrity violation: {}", msg)
            }
            ExecutorError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {}

/// Result type for executor operations.
pub type Result<T> = std::result::Result<T, ExecutorError>;

/// Trait for execution plan operators.
///
/// Each operator produces a stream of [`RecordBatch`] results.
#[async_trait]
pub trait ExecutionPlan: Send + Sync {
    /// Execute the operator and return a stream of record batches.
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>;

    /// Return the schema (column names and types) produced by this operator.
    fn schema(&self) -> Vec<(String, String)>;
}
