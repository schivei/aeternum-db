//! Query planning subsystem for AeternumDB.
//!
//! This module converts SQL ASTs into validated logical plans, optimizes them,
//! lowers them into physical plans, and supports plan explanation.
//!
//! # Pipeline overview
//!
//! ```text
//! SQL string
//!     │
//!     ▼  SqlParser
//! Statement (AST)
//!     │
//!     ▼  LogicalPlanBuilder
//! LogicalPlan  ←──── validate ──── PlannerError
//!     │
//!     ▼  Optimizer (rules + join reorder)
//! LogicalPlan (optimized)
//!     │
//!     ▼  PhysicalPlanner
//! PhysicalPlan  ←─── cost annotations
//!     │
//!     ▼  explain_physical
//! human-readable EXPLAIN string
//! ```
//!
//! # Quick start
//!
//! ```rust
//! use aeternumdb_core::query::{QueryPlanner, PlannerContext};
//! use aeternumdb_core::sql::validator::{Catalog, ColumnSchema, TableSchema};
//! use aeternumdb_core::sql::ast::DataType;
//! use aeternumdb_core::sql::parser::SqlParser;
//!
//! // Build a catalog
//! let mut catalog = Catalog::new();
//! catalog.add_table(TableSchema {
//!     name: "users".to_string(),
//!     columns: vec![
//!         ColumnSchema { name: "id".to_string(), data_type: DataType::Integer, nullable: false },
//!         ColumnSchema { name: "age".to_string(), data_type: DataType::Integer, nullable: true },
//!     ],
//! });
//!
//! // Parse a query
//! let parser = SqlParser::new();
//! let stmt = parser.parse_one("SELECT id FROM users WHERE age > 18").unwrap();
//!
//! // Plan the query
//! let ctx = PlannerContext::new(&catalog);
//! let planner = QueryPlanner::new();
//! let logical = planner.create_logical_plan(&stmt, &ctx).unwrap();
//! let optimized = planner.optimize(logical, &ctx);
//! let physical = planner.create_physical_plan(optimized, &ctx);
//! let explanation = planner.explain(&physical);
//! println!("{}", explanation);
//! ```

pub mod cost_model;
pub mod explain;
pub mod logical_plan;
pub mod optimizer;
pub mod physical_plan;
pub mod rules;
pub mod statistics;

use crate::query::cost_model::CostModel;
use crate::query::explain::explain_physical;
use crate::query::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use crate::query::optimizer::Optimizer;
use crate::query::physical_plan::{PhysicalPlan, PhysicalPlanner};
use crate::query::statistics::StatisticsRegistry;
use crate::sql::ast::Statement;
use crate::sql::validator::Catalog;

// ── PlannerError ──────────────────────────────────────────────────────────────

/// Errors that can occur during query planning.
#[derive(Debug, Clone, PartialEq)]
pub enum PlannerError {
    /// The statement type is not supported by the planner (only `SELECT` is
    /// supported at this stage; DML planning is deferred to PR 1.5).
    UnsupportedStatement,

    /// A table or column referenced in the query was not found in the catalog.
    CatalogError(String),

    /// The query references tables from more than one database.
    ///
    /// All tables in a single query must belong to the same database.
    CrossDatabaseJoin {
        /// The distinct database names that were detected.
        databases: Vec<String>,
    },

    /// A `FLAT` table was used in a join, which is not permitted.
    ///
    /// FLAT tables cannot participate in joins; use a single-table query to
    /// access a FLAT table.
    FlatTableJoin(String),

    /// An `EXPAND` expression could not be resolved.
    InvalidExpand(String),

    /// Any other planning error.
    Other(String),
}

impl std::fmt::Display for PlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlannerError::UnsupportedStatement => {
                write!(
                    f,
                    "only SELECT statements are supported by the query planner"
                )
            }
            PlannerError::CatalogError(msg) => write!(f, "catalog error: {msg}"),
            PlannerError::CrossDatabaseJoin { databases } => write!(
                f,
                "cross-database joins are not permitted; \
                 tables from different databases in the same query: {}",
                databases.join(", ")
            ),
            PlannerError::FlatTableJoin(table) => {
                write!(f, "FLAT table '{table}' cannot participate in a join")
            }
            PlannerError::InvalidExpand(msg) => write!(f, "invalid EXPAND expression: {msg}"),
            PlannerError::Other(msg) => write!(f, "planner error: {msg}"),
        }
    }
}

impl std::error::Error for PlannerError {}

// ── PlannerContext ────────────────────────────────────────────────────────────

/// Context provided to the planner during plan construction.
///
/// Holds the schema catalog (required) and optional table statistics.
pub struct PlannerContext<'a> {
    /// Schema catalog used for column/table resolution.
    pub catalog: &'a Catalog,
    /// Table and column statistics used by the cost model.
    pub statistics: StatisticsRegistry,
    /// Cost model parameters.
    pub cost_model: CostModel,
    /// Names of tables that were created as FLAT tables.
    flat_tables: Vec<String>,
}

impl<'a> PlannerContext<'a> {
    /// Create a context with default cost model and empty statistics.
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            statistics: StatisticsRegistry::new(),
            cost_model: CostModel::default(),
            flat_tables: Vec::new(),
        }
    }

    /// Register a FLAT table name so the planner can enforce join restrictions.
    pub fn add_flat_table(&mut self, name: impl Into<String>) {
        self.flat_tables.push(name.into().to_lowercase());
    }
}

// ── QueryPlanner ─────────────────────────────────────────────────────────────

/// Entry point for the AeternumDB query planning pipeline.
///
/// The planner is stateless — all mutable state lives in [`PlannerContext`].
/// This makes it safe to share a single `QueryPlanner` instance across threads.
///
/// # Example
///
/// See the [module-level documentation](self) for a full pipeline example.
#[derive(Debug, Default)]
pub struct QueryPlanner;

impl QueryPlanner {
    /// Create a new `QueryPlanner`.
    pub fn new() -> Self {
        Self
    }

    /// Build a logical plan from an internal SQL [`Statement`].
    ///
    /// Only `SELECT` statements produce a plan.  All other statement types
    /// return [`PlannerError::UnsupportedStatement`].
    pub fn create_logical_plan(
        &self,
        stmt: &Statement,
        ctx: &PlannerContext<'_>,
    ) -> Result<LogicalPlan, PlannerError> {
        let mut builder = LogicalPlanBuilder::new(ctx.catalog);
        for t in &ctx.flat_tables {
            builder.register_flat_table(t);
        }
        builder.build_from_statement(stmt)
    }

    /// Optimize a logical plan using the default rule set and join reordering.
    pub fn optimize(&self, plan: LogicalPlan, ctx: &PlannerContext<'_>) -> LogicalPlan {
        let optimizer = Optimizer::new(&ctx.statistics);
        optimizer.optimize(plan)
    }

    /// Lower an optimized logical plan into a physical execution plan.
    pub fn create_physical_plan(
        &self,
        plan: LogicalPlan,
        ctx: &PlannerContext<'_>,
    ) -> PhysicalPlan {
        let physical_planner = PhysicalPlanner::new(ctx.cost_model.clone(), &ctx.statistics);
        physical_planner.lower(&plan)
    }

    /// Format a physical plan as a human-readable EXPLAIN string.
    pub fn explain(&self, plan: &PhysicalPlan) -> String {
        explain_physical(plan)
    }

    /// Run the full planning pipeline: parse → logical → optimize → physical.
    ///
    /// Convenience method that combines [`create_logical_plan`],
    /// [`optimize`], and [`create_physical_plan`].
    pub fn plan(
        &self,
        stmt: &Statement,
        ctx: &PlannerContext<'_>,
    ) -> Result<PhysicalPlan, PlannerError> {
        let logical = self.create_logical_plan(stmt, ctx)?;
        let optimized = self.optimize(logical, ctx);
        Ok(self.create_physical_plan(optimized, ctx))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::DataType;
    use crate::sql::parser::SqlParser;
    use crate::sql::validator::{ColumnSchema, TableSchema};

    fn make_catalog() -> Catalog {
        let mut c = Catalog::new();
        c.add_table(TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnSchema {
                    name: "age".into(),
                    data_type: DataType::Integer,
                    nullable: true,
                },
                ColumnSchema {
                    name: "name".into(),
                    data_type: DataType::Varchar(Some(255)),
                    nullable: true,
                },
            ],
        });
        c.add_table(TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnSchema {
                    name: "user_id".into(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnSchema {
                    name: "total".into(),
                    data_type: DataType::Decimal(Some(10), Some(2)),
                    nullable: true,
                },
            ],
        });
        c
    }

    fn parse_select(sql: &str) -> Statement {
        let parser = SqlParser::new();
        parser.parse_one(sql).unwrap()
    }

    #[test]
    fn plan_simple_select() {
        let catalog = make_catalog();
        let ctx = PlannerContext::new(&catalog);
        let planner = QueryPlanner::new();

        let stmt = parse_select("SELECT id FROM users");
        let plan = planner.plan(&stmt, &ctx).unwrap();
        assert!(plan.cost().estimated_rows > 0);
    }

    #[test]
    fn plan_select_with_where() {
        let catalog = make_catalog();
        let ctx = PlannerContext::new(&catalog);
        let planner = QueryPlanner::new();

        let stmt = parse_select("SELECT id FROM users WHERE age > 18");
        let plan = planner.plan(&stmt, &ctx).unwrap();
        // The optimizer should have pushed the predicate into the scan,
        // resulting in an IndexScan or SeqScan with filter.
        let explain_out = planner.explain(&plan);
        assert!(explain_out.contains("users"));
    }

    #[test]
    fn plan_select_with_join() {
        let catalog = make_catalog();
        let ctx = PlannerContext::new(&catalog);
        let planner = QueryPlanner::new();

        let stmt = parse_select(
            "SELECT users.id, orders.total \
             FROM users \
             JOIN orders ON users.id = orders.user_id",
        );
        let plan = planner.plan(&stmt, &ctx).unwrap();
        let explain_out = planner.explain(&plan);
        assert!(explain_out.contains("Join"));
    }

    #[test]
    fn plan_select_with_group_by() {
        let catalog = make_catalog();
        let ctx = PlannerContext::new(&catalog);
        let planner = QueryPlanner::new();

        let stmt = parse_select("SELECT age, COUNT(*) FROM users GROUP BY age");
        let plan = planner.plan(&stmt, &ctx).unwrap();
        let explain_out = planner.explain(&plan);
        assert!(explain_out.contains("HashAggregate") || explain_out.contains("Aggregate"));
    }

    #[test]
    fn plan_non_select_returns_error() {
        let catalog = make_catalog();
        let ctx = PlannerContext::new(&catalog);
        let planner = QueryPlanner::new();

        let parser = SqlParser::new();
        let stmt = parser
            .parse_one("INSERT INTO users (id) VALUES (1)")
            .unwrap();
        let err = planner.create_logical_plan(&stmt, &ctx).unwrap_err();
        assert!(matches!(err, PlannerError::UnsupportedStatement));
    }

    #[test]
    fn flat_table_join_rejected_via_context() {
        let catalog = make_catalog();
        let mut ctx = PlannerContext::new(&catalog);
        ctx.add_flat_table("users");
        let planner = QueryPlanner::new();

        let stmt = parse_select("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
        let err = planner.create_logical_plan(&stmt, &ctx).unwrap_err();
        assert!(matches!(err, PlannerError::FlatTableJoin(_)));
    }

    #[test]
    fn explain_output_contains_total_cost() {
        let catalog = make_catalog();
        let ctx = PlannerContext::new(&catalog);
        let planner = QueryPlanner::new();

        let stmt = parse_select("SELECT id FROM users");
        let physical = planner.plan(&stmt, &ctx).unwrap();
        let out = planner.explain(&physical);
        assert!(out.contains("Total Cost:"));
        assert!(out.contains("Estimated Rows:"));
    }

    #[test]
    fn planner_error_display() {
        let err = PlannerError::CrossDatabaseJoin {
            databases: vec!["db1".into(), "db2".into()],
        };
        let msg = err.to_string();
        assert!(msg.contains("db1"));
        assert!(msg.contains("db2"));
    }

    #[test]
    fn planner_error_flat_table_display() {
        let err = PlannerError::FlatTableJoin("events".into());
        assert!(err.to_string().contains("events"));
    }

    #[test]
    fn plan_order_by() {
        let catalog = make_catalog();
        let ctx = PlannerContext::new(&catalog);
        let planner = QueryPlanner::new();

        let stmt = parse_select("SELECT id, name FROM users ORDER BY name");
        let physical = planner.plan(&stmt, &ctx).unwrap();
        let out = planner.explain(&physical);
        assert!(out.contains("Sort"));
    }

    #[test]
    fn plan_limit_offset() {
        let catalog = make_catalog();
        let ctx = PlannerContext::new(&catalog);
        let planner = QueryPlanner::new();

        let stmt = parse_select("SELECT id FROM users LIMIT 5 OFFSET 10");
        let physical = planner.plan(&stmt, &ctx).unwrap();
        let out = planner.explain(&physical);
        assert!(out.contains("Limit"));
    }
}
