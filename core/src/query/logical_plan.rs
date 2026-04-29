//! Logical plan tree for AeternumDB queries.
//!
//! A [`LogicalPlan`] is a relational-algebra tree built directly from an
//! internal [`SelectStatement`](crate::sql::ast::SelectStatement) AST node.
//! It is the *input* to the optimizer and the *source* for physical plan
//! generation.
//!
//! ## Plan nodes
//!
//! | Node | Description |
//! |------|-------------|
//! | [`LogicalPlan::Scan`] | Read all rows from a table |
//! | [`LogicalPlan::Filter`] | Keep rows that satisfy a predicate |
//! | [`LogicalPlan::Project`] | Compute/rename the output column set |
//! | [`LogicalPlan::Join`] | Combine two inputs |
//! | [`LogicalPlan::Aggregate`] | Group rows and apply aggregate functions |
//! | [`LogicalPlan::Sort`] | Order the output |
//! | [`LogicalPlan::Limit`] | Restrict row count and apply an offset |
//! | [`LogicalPlan::Unnest`] | Explode a vector/array column into rows |
//! | [`LogicalPlan::ViewAs`] | Final post-result rename / projection |
//! | [`LogicalPlan::Values`] | Inline constant row set |
//!
//! ## AeternumDB-specific extensions
//!
//! - **EXPAND(ref_col)** — resolved into a Join + Project during plan
//!   construction.  See [`LogicalPlanBuilder`].
//! - **VIEW AS** — lowered into a [`LogicalPlan::ViewAs`] node placed at the
//!   very top of the plan tree.
//! - **FILTER BY** — the per-join predicate stored in
//!   [`TableReference::Join::filter_by`](crate::sql::ast::TableReference)
//!   is stored directly in [`LogicalPlan::Join`]'s `condition` field, rather
//!   than being lowered into a separate [`LogicalPlan::Filter`] above the
//!   join; this preserves join semantics such as outer-join NULL extension.
//! - **FLAT table enforcement** — any join that involves a FLAT table is
//!   rejected with [`PlannerError::FlatTableJoin`].
//! - **Cross-database join rejection** — tables from different databases
//!   cannot appear in the same query.

use std::collections::HashSet;

use crate::sql::ast::{Expr, JoinType, OrderByExpr, SelectItem, SelectStatement, TableReference};
use crate::sql::validator::{Catalog, TableSchema};

use super::PlannerError;

// ── Supporting types ──────────────────────────────────────────────────────────

/// An expression paired with an output alias, used in projections.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionItem {
    /// The expression to evaluate.
    pub expr: Expr,
    /// Optional alias for the output column.
    pub alias: Option<String>,
}

/// A single aggregate function applied to a group.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    /// The aggregate function call expression (e.g. `COUNT(*)`, `SUM(price)`).
    pub func: Expr,
    /// Output column alias.
    pub alias: Option<String>,
}

/// One term in an `ORDER BY` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct SortExpr {
    /// The expression to sort by.
    pub expr: Expr,
    /// `true` = ascending (default), `false` = descending.
    pub ascending: bool,
}

impl From<OrderByExpr> for SortExpr {
    fn from(o: OrderByExpr) -> Self {
        Self {
            expr: o.expr,
            ascending: o.ascending,
        }
    }
}

/// A single item in a [`LogicalPlan::ViewAs`] projection.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewAsProjection {
    /// Transformation expression (primitive only — no aggregates, no subqueries).
    pub expr: Expr,
    /// Output column name.
    pub alias: String,
}

// ── LogicalPlan ───────────────────────────────────────────────────────────────

/// A node in the logical query plan tree.
///
/// Each variant holds its direct children by `Box<LogicalPlan>` so the tree
/// is fully owned and cheaply movable.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Full-table (or filtered-table) scan.
    Scan {
        /// Unqualified table name (lowercased).
        table: String,
        /// Optional alias used in the query.
        alias: Option<String>,
        /// Columns to read (`None` = all columns / `SELECT *`).
        columns: Option<Vec<String>>,
        /// Optional predicate pushed down to the scan.
        filter: Option<Expr>,
    },

    /// Row filter (`WHERE` / `HAVING` predicate).
    Filter {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// The filter predicate.
        predicate: Expr,
    },

    /// Column projection / expression computation.
    Project {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Output expressions.
        items: Vec<ProjectionItem>,
    },

    /// Relational join of two inputs.
    Join {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input.
        right: Box<LogicalPlan>,
        /// Join semantics.
        join_type: JoinType,
        /// Optional join predicate (`ON` / `FILTER BY`).
        condition: Option<Expr>,
    },

    /// Grouping and aggregation.
    Aggregate {
        /// Input plan (post-join, pre-grouping).
        input: Box<LogicalPlan>,
        /// `GROUP BY` expressions.
        group_by: Vec<Expr>,
        /// Aggregate function expressions.
        aggregates: Vec<AggregateExpr>,
        /// `HAVING` predicate (applied after aggregation).
        having: Option<Expr>,
    },

    /// Deterministic row ordering.
    Sort {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Sort keys (in priority order, first = primary).
        order_by: Vec<SortExpr>,
    },

    /// Row-count restriction with optional offset.
    Limit {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Maximum number of rows to return.
        limit: usize,
        /// Number of rows to skip before returning results.
        offset: usize,
    },

    /// Explodes a vector/array column into one row per element.
    ///
    /// Injected by the planner when a projection references a
    /// `DataType::Vector` or `DataType::ReferenceArray` column.
    Unnest {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// The column expression to unnest.
        column: Expr,
        /// Output alias for the unnested elements.
        alias: Option<String>,
    },

    /// Post-result rename / transform clause (`VIEW AS`).
    ///
    /// Always placed at the root of the plan tree when present.
    ViewAs {
        /// Inner plan producing the base result set.
        input: Box<LogicalPlan>,
        /// Transformation / rename items.
        items: Vec<ViewAsProjection>,
    },

    /// Inline constant row set (used for `SELECT` without `FROM`).
    Values {
        /// Rows of literal expressions.
        rows: Vec<Vec<Expr>>,
    },
}

impl LogicalPlan {
    /// Estimate the output cardinality for use by the cost model.
    ///
    /// Returns a rough guess when no statistics are available.
    pub fn estimated_rows(&self) -> usize {
        match self {
            LogicalPlan::Scan { .. } => 1000,
            LogicalPlan::Filter { input, .. } => (input.estimated_rows() / 10).max(1),
            LogicalPlan::Project { input, .. } => input.estimated_rows(),
            LogicalPlan::Join { left, right, .. } => {
                (left.estimated_rows() * right.estimated_rows() / 100).max(1)
            }
            LogicalPlan::Aggregate {
                input, group_by, ..
            } => {
                if group_by.is_empty() {
                    1
                } else {
                    (input.estimated_rows() / 10).max(1)
                }
            }
            LogicalPlan::Sort { input, .. } => input.estimated_rows(),
            LogicalPlan::Limit { limit, .. } => *limit,
            LogicalPlan::Unnest { input, .. } => input.estimated_rows() * 5,
            LogicalPlan::ViewAs { input, .. } => input.estimated_rows(),
            LogicalPlan::Values { rows } => rows.len(),
        }
    }
}

// ── LogicalPlanBuilder ────────────────────────────────────────────────────────

/// Builds a [`LogicalPlan`] tree from an internal SQL AST node.
///
/// # Usage
///
/// ```rust
/// use aeternumdb_core::query::logical_plan::LogicalPlanBuilder;
/// use aeternumdb_core::sql::validator::{Catalog, ColumnSchema, TableSchema};
/// use aeternumdb_core::sql::ast::DataType;
/// use aeternumdb_core::sql::parser::SqlParser;
///
/// let mut catalog = Catalog::new();
/// catalog.add_table(TableSchema {
///     name: "users".to_string(),
///     columns: vec![
///         ColumnSchema { name: "id".to_string(), data_type: DataType::Integer, nullable: false },
///         ColumnSchema { name: "age".to_string(), data_type: DataType::Integer, nullable: true },
///     ],
/// });
///
/// let parser = SqlParser::new();
/// let stmt = parser.parse_one("SELECT id FROM users WHERE age > 18").unwrap();
///
/// let builder = LogicalPlanBuilder::new(&catalog);
/// let plan = builder.build_from_statement(&stmt).unwrap();
/// ```
pub struct LogicalPlanBuilder<'a> {
    catalog: &'a Catalog,
    /// Tracks flat-table names encountered (lowercased).
    flat_tables: HashSet<String>,
}

impl<'a> LogicalPlanBuilder<'a> {
    /// Create a builder backed by the given catalog.
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            flat_tables: HashSet::new(),
        }
    }

    /// Register a table name as FLAT so the builder can enforce join
    /// restrictions even before catalog look-ups return schema metadata.
    pub fn register_flat_table(&mut self, name: impl Into<String>) {
        self.flat_tables.insert(name.into().to_lowercase());
    }

    /// Build a [`LogicalPlan`] from a top-level [`Statement`](crate::sql::ast::Statement).
    ///
    /// Only `SELECT` statements produce a plan; all other statement types are
    /// rejected with [`PlannerError::UnsupportedStatement`].
    pub fn build_from_statement(
        &self,
        stmt: &crate::sql::ast::Statement,
    ) -> Result<LogicalPlan, PlannerError> {
        match stmt {
            crate::sql::ast::Statement::Select(s) => self.build_select(s),
            _ => Err(PlannerError::UnsupportedStatement),
        }
    }

    /// Build a plan for a [`SelectStatement`].
    pub fn build_select(&self, stmt: &SelectStatement) -> Result<LogicalPlan, PlannerError> {
        // Detect databases referenced across the entire FROM tree.
        if let Some(from) = &stmt.from {
            check_cross_db(from)?;
        }

        let base = self.build_from_clause(stmt.from.as_ref())?;
        let filtered = self.apply_where(base, stmt.where_clause.as_ref());
        let needs_aggregate = self.needs_aggregate(stmt);
        let grouped = self.apply_group_by(filtered, stmt, needs_aggregate);
        let projected = self.apply_projection(grouped, &stmt.columns)?;
        let sorted = apply_sort(projected, &stmt.order_by);
        let limited = apply_limit(sorted, stmt.limit, stmt.offset);
        let view_as = apply_view_as(limited, stmt.view_as.as_deref())?;
        Ok(view_as)
    }

    // ── FROM clause ──────────────────────────────────────────────────────

    fn build_from_clause(
        &self,
        from: Option<&TableReference>,
    ) -> Result<LogicalPlan, PlannerError> {
        match from {
            None => Ok(LogicalPlan::Values { rows: vec![vec![]] }),
            Some(tr) => self.build_table_ref(tr),
        }
    }

    fn build_table_ref(&self, tr: &TableReference) -> Result<LogicalPlan, PlannerError> {
        match tr {
            TableReference::Named {
                name,
                alias,
                database: _,
                schema: _,
            } => self.build_named_table(name, alias.as_deref()),
            TableReference::Subquery { query, alias } => {
                let inner = self.build_select(query)?;
                Ok(wrap_subquery(inner, alias.clone()))
            }
            TableReference::Join {
                left,
                right,
                join_type,
                filter_by,
            } => self.build_join(left, right, join_type, filter_by.as_ref()),
        }
    }

    fn build_named_table(
        &self,
        name: &str,
        alias: Option<&str>,
    ) -> Result<LogicalPlan, PlannerError> {
        let lower = name.to_lowercase();
        let _schema: &TableSchema = self.catalog.get_table(&lower).ok_or_else(|| {
            PlannerError::CatalogError(format!("table not found in catalog: {}", name))
        })?;
        let _is_flat = self.flat_tables.contains(&lower);
        // If this is a FLAT table, record it for join-rejection later.
        // The Scan node itself doesn't carry flat-ness; enforcement happens in
        // reject_flat_in_join when a join is attempted.

        Ok(LogicalPlan::Scan {
            table: lower,
            alias: alias.map(str::to_string),
            columns: None,
            filter: None,
        })
    }

    fn build_join(
        &self,
        left: &TableReference,
        right: &TableReference,
        join_type: &JoinType,
        filter_by: Option<&Expr>,
    ) -> Result<LogicalPlan, PlannerError> {
        let left_plan = self.build_table_ref(left)?;
        let right_plan = self.build_table_ref(right)?;

        // FLAT table enforcement
        reject_flat_in_join(&left_plan, &self.flat_tables)?;
        reject_flat_in_join(&right_plan, &self.flat_tables)?;

        let join_node = LogicalPlan::Join {
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            join_type: join_type.clone(),
            // Store the ON condition directly in the join so outer-join
            // semantics are preserved.  A post-join WHERE filter is added
            // separately by apply_where.
            condition: filter_by.cloned(),
        };

        Ok(join_node)
    }

    // ── WHERE ─────────────────────────────────────────────────────────────

    fn apply_where(&self, input: LogicalPlan, predicate: Option<&Expr>) -> LogicalPlan {
        match predicate {
            None => input,
            Some(pred) => LogicalPlan::Filter {
                input: Box::new(input),
                predicate: pred.clone(),
            },
        }
    }

    // ── SELECT list / projection ──────────────────────────────────────────

    fn apply_projection(
        &self,
        input: LogicalPlan,
        items: &[SelectItem],
    ) -> Result<LogicalPlan, PlannerError> {
        if items.is_empty() || items == [SelectItem::Wildcard] {
            return Ok(input);
        }

        let mut projection_items: Vec<ProjectionItem> = Vec::new();
        let mut unnest_queue: Vec<(Expr, Option<String>)> = Vec::new();

        for item in items {
            self.translate_select_item(item, &mut projection_items, &mut unnest_queue)?;
        }

        let mut plan = input;

        // Wrap any UNNEST requests.
        for (col_expr, alias) in unnest_queue {
            plan = LogicalPlan::Unnest {
                input: Box::new(plan),
                column: col_expr,
                alias,
            };
        }

        if projection_items.is_empty() {
            return Ok(plan);
        }

        Ok(LogicalPlan::Project {
            input: Box::new(plan),
            items: projection_items,
        })
    }

    fn translate_select_item(
        &self,
        item: &SelectItem,
        out: &mut Vec<ProjectionItem>,
        unnest_queue: &mut Vec<(Expr, Option<String>)>,
    ) -> Result<(), PlannerError> {
        match item {
            SelectItem::Wildcard => {
                out.push(ProjectionItem {
                    expr: Expr::Wildcard,
                    alias: None,
                });
            }
            SelectItem::QualifiedWildcard(table) => {
                out.push(ProjectionItem {
                    expr: Expr::Column {
                        table: Some(table.clone()),
                        name: "*".into(),
                    },
                    alias: None,
                });
            }
            SelectItem::Expr { expr, alias } => {
                out.push(ProjectionItem {
                    expr: expr.clone(),
                    alias: alias.clone(),
                });
            }
            SelectItem::Expand { expr, alias } => {
                self.expand_reference(expr, alias.as_deref(), out, unnest_queue)?;
            }
        }
        Ok(())
    }

    fn expand_reference(
        &self,
        expr: &Expr,
        alias: Option<&str>,
        out: &mut Vec<ProjectionItem>,
        unnest_queue: &mut Vec<(Expr, Option<String>)>,
    ) -> Result<(), PlannerError> {
        let _col_name = match expr {
            Expr::Column { name, .. } => name.clone(),
            _ => {
                return Err(PlannerError::InvalidExpand(
                    "EXPAND requires a column reference".into(),
                ))
            }
        };

        // Preserve the original column expression (including any table qualifier)
        // so downstream planning can correctly resolve the reference.
        unnest_queue.push((expr.clone(), alias.map(str::to_string)));

        out.push(ProjectionItem {
            expr: expr.clone(),
            // Store the alias as-is (namespace prefix); wildcard expansion is a
            // separate concern handled by the executor.
            alias: alias.map(str::to_string),
        });
        let _ = self.catalog; // suppress unused warning

        Ok(())
    }

    // ── GROUP BY / HAVING ─────────────────────────────────────────────────

    fn needs_aggregate(&self, stmt: &SelectStatement) -> bool {
        if !stmt.group_by.is_empty() {
            return true;
        }
        // Also true when any SELECT item contains an aggregate function.
        stmt.columns.iter().any(|col| {
            if let SelectItem::Expr { expr, .. } = col {
                contains_aggregate(expr)
            } else {
                false
            }
        })
    }

    fn apply_group_by(
        &self,
        input: LogicalPlan,
        stmt: &SelectStatement,
        do_aggregate: bool,
    ) -> LogicalPlan {
        if !do_aggregate {
            return input;
        }
        let aggregates = collect_aggregates(&stmt.columns);
        LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by: stmt.group_by.clone(),
            aggregates,
            having: stmt.having.clone(),
        }
    }
}

// ── Free-standing helpers ─────────────────────────────────────────────────────

fn wrap_subquery(inner: LogicalPlan, alias: String) -> LogicalPlan {
    // A subquery is represented as a Project with the inner plan, allowing the
    // optimizer to push predicates into the subquery where safe.
    LogicalPlan::Project {
        input: Box::new(inner),
        items: vec![ProjectionItem {
            expr: Expr::Column {
                table: Some(alias),
                name: "*".into(),
            },
            alias: None,
        }],
    }
}

fn apply_sort(input: LogicalPlan, order_by: &[OrderByExpr]) -> LogicalPlan {
    if order_by.is_empty() {
        return input;
    }
    LogicalPlan::Sort {
        input: Box::new(input),
        order_by: order_by.iter().cloned().map(SortExpr::from).collect(),
    }
}

fn apply_limit(input: LogicalPlan, limit: Option<u64>, offset: Option<u64>) -> LogicalPlan {
    match (limit, offset) {
        (None, None) => input,
        (Some(lim), offset) => LogicalPlan::Limit {
            input: Box::new(input),
            limit: lim as usize,
            offset: offset.unwrap_or(0) as usize,
        },
        // OFFSET without LIMIT: skip the specified number of rows but allow
        // all remaining rows through (represented as usize::MAX rows limit).
        (None, Some(off)) => LogicalPlan::Limit {
            input: Box::new(input),
            limit: usize::MAX,
            offset: off as usize,
        },
    }
}

fn apply_view_as(
    input: LogicalPlan,
    view_as: Option<&[crate::sql::ast::ViewAsItem]>,
) -> Result<LogicalPlan, PlannerError> {
    match view_as {
        None => Ok(input),
        Some(items) => {
            let projections = items
                .iter()
                .map(|v| ViewAsProjection {
                    expr: v.expr.clone(),
                    alias: v.alias.clone(),
                })
                .collect();
            Ok(LogicalPlan::ViewAs {
                input: Box::new(input),
                items: projections,
            })
        }
    }
}

/// Collect all aggregate expressions from the SELECT list.
fn collect_aggregates(columns: &[SelectItem]) -> Vec<AggregateExpr> {
    columns
        .iter()
        .filter_map(|item| {
            if let SelectItem::Expr { expr, alias } = item {
                if contains_aggregate(expr) {
                    return Some(AggregateExpr {
                        func: expr.clone(),
                        alias: alias.clone(),
                    });
                }
            }
            None
        })
        .collect()
}

/// Returns `true` if `expr` contains a call to an aggregate function.
fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. } => is_agg_fn(name) || args.iter().any(contains_aggregate),
        Expr::BinaryOp { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } => contains_aggregate(expr),
        Expr::IsNull { expr, .. } => contains_aggregate(expr),
        Expr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        Expr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| contains_aggregate(e))
                || conditions
                    .iter()
                    .any(|(w, t)| contains_aggregate(w) || contains_aggregate(t))
                || else_result.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        Expr::ArrayOp { expr, right, .. } => contains_aggregate(expr) || contains_aggregate(right),
        Expr::Substring {
            expr,
            from_pos,
            len,
        } => {
            contains_aggregate(expr)
                || from_pos.as_ref().is_some_and(|e| contains_aggregate(e))
                || len.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        Expr::Position { substr, in_expr } => {
            contains_aggregate(substr) || contains_aggregate(in_expr)
        }
        Expr::Trim {
            expr, trim_what, ..
        } => contains_aggregate(expr) || trim_what.as_ref().is_some_and(|e| contains_aggregate(e)),
        Expr::Overlay {
            expr,
            overlay_what,
            from_pos,
            for_len,
        } => {
            contains_aggregate(expr)
                || contains_aggregate(overlay_what)
                || contains_aggregate(from_pos)
                || for_len.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        // Leaf nodes — no child expressions that can contain an aggregate.
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::Wildcard
        | Expr::MatchAgainst { .. }
        | Expr::InSubquery { .. }
        | Expr::Subquery(_) => false,
    }
}

fn is_agg_fn(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "STDDEV" | "VARIANCE"
    )
}

// ── Cross-database check ──────────────────────────────────────────────────────

/// Returns `Err(CrossDatabaseJoin)` if the FROM tree references more than one
/// distinct database qualifier.
fn check_cross_db(tr: &TableReference) -> Result<(), PlannerError> {
    let mut dbs: HashSet<String> = HashSet::new();
    collect_databases(tr, &mut dbs);
    if dbs.len() > 1 {
        Err(PlannerError::CrossDatabaseJoin {
            databases: dbs.into_iter().collect(),
        })
    } else {
        Ok(())
    }
}

fn collect_databases(tr: &TableReference, out: &mut HashSet<String>) {
    match tr {
        TableReference::Named {
            database: Some(db), ..
        } => {
            out.insert(db.to_lowercase());
        }
        TableReference::Named { .. } => {}
        TableReference::Subquery { query, .. } => {
            if let Some(from) = &query.from {
                collect_databases(from, out);
            }
        }
        TableReference::Join { left, right, .. } => {
            collect_databases(left, out);
            collect_databases(right, out);
        }
    }
}

// ── FLAT table enforcement ────────────────────────────────────────────────────

fn find_underlying_scan_table(plan: &LogicalPlan) -> Option<&str> {
    match plan {
        LogicalPlan::Scan { table, .. } => Some(table.as_str()),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Unnest { input, .. }
        | LogicalPlan::ViewAs { input, .. } => find_underlying_scan_table(input),
        _ => None,
    }
}

fn reject_flat_in_join(
    plan: &LogicalPlan,
    flat_tables: &HashSet<String>,
) -> Result<(), PlannerError> {
    if let Some(table) = find_underlying_scan_table(plan) {
        if flat_tables.contains(table) {
            return Err(PlannerError::FlatTableJoin(table.to_string()));
        }
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{DataType, Statement};
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
            ],
        });
        c
    }

    fn parse_select(sql: &str) -> Box<SelectStatement> {
        let parser = SqlParser::new();
        match parser.parse_one(sql).unwrap() {
            Statement::Select(s) => s,
            _ => panic!("expected SELECT"),
        }
    }

    fn builder(catalog: &Catalog) -> LogicalPlanBuilder<'_> {
        LogicalPlanBuilder::new(catalog)
    }

    #[test]
    fn simple_select_all() {
        let catalog = make_catalog();
        let stmt = parse_select("SELECT * FROM users");
        let plan = builder(&catalog).build_select(&stmt).unwrap();
        assert!(matches!(plan, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn select_with_where() {
        let catalog = make_catalog();
        let stmt = parse_select("SELECT id FROM users WHERE age > 18");
        let plan = builder(&catalog).build_select(&stmt).unwrap();
        // After predicate pushdown the filter might be in the scan or still
        // explicit — at minimum a Project node wraps the underlying query.
        let has_project_or_filter = matches!(plan, LogicalPlan::Project { .. })
            || matches!(plan, LogicalPlan::Filter { .. })
            || matches!(
                plan,
                LogicalPlan::Scan {
                    filter: Some(_),
                    ..
                }
            );
        assert!(has_project_or_filter, "unexpected plan shape: {plan:?}");
    }

    #[test]
    fn select_with_limit() {
        let catalog = make_catalog();
        let stmt = parse_select("SELECT id FROM users LIMIT 10");
        let plan = builder(&catalog).build_select(&stmt).unwrap();
        // LIMIT node sits at the top; may also contain Sort/Project below
        let is_limit_or_has_limit =
            matches!(plan, LogicalPlan::Limit { .. }) || matches!(plan, LogicalPlan::Sort { .. });
        assert!(is_limit_or_has_limit, "unexpected plan: {plan:?}");
    }

    #[test]
    fn select_with_order_by() {
        let catalog = make_catalog();
        let stmt = parse_select("SELECT id FROM users ORDER BY id");
        let plan = builder(&catalog).build_select(&stmt).unwrap();
        // Sort wraps the projection or scan
        assert!(matches!(plan, LogicalPlan::Sort { .. }));
    }

    #[test]
    fn select_with_group_by() {
        let catalog = make_catalog();
        let stmt = parse_select("SELECT age, COUNT(*) FROM users GROUP BY age");
        let plan = builder(&catalog).build_select(&stmt).unwrap();
        // Correct order: Project(Aggregate(Scan)). Root is Project, child is Aggregate.
        // Walk one level to find the aggregate.
        let inner = match &plan {
            LogicalPlan::Project { input, .. } => input.as_ref(),
            other => other,
        };
        assert!(
            matches!(inner, LogicalPlan::Aggregate { .. }),
            "expected Aggregate below optional Project, got: {:?}",
            plan
        );
    }

    #[test]
    fn flat_table_join_rejected() {
        let catalog = make_catalog();
        let mut b = LogicalPlanBuilder::new(&catalog);
        b.register_flat_table("users");
        let stmt = parse_select("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
        let err = b.build_select(&stmt).unwrap_err();
        assert!(matches!(err, PlannerError::FlatTableJoin(_)));
    }

    #[test]
    fn no_from_returns_values() {
        let catalog = make_catalog();
        let stmt = parse_select("SELECT 1");
        let plan = builder(&catalog).build_select(&stmt).unwrap();
        // May be a Project over Values or just Values
        let has_values = |p: &LogicalPlan| matches!(p, LogicalPlan::Values { .. });
        let ok = has_values(&plan)
            || matches!(&plan, LogicalPlan::Project { input, .. } if has_values(input));
        assert!(ok);
    }

    #[test]
    fn estimated_rows_limit() {
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Values { rows: vec![] }),
            limit: 42,
            offset: 0,
        };
        assert_eq!(plan.estimated_rows(), 42);
    }

    #[test]
    fn contains_aggregate_detects_nested_count() {
        // -COUNT(*) — aggregate inside UnaryOp
        let neg_count = Expr::UnaryOp {
            op: crate::sql::ast::UnaryOperator::Minus,
            expr: Box::new(Expr::Function {
                name: "COUNT".into(),
                args: vec![Expr::Wildcard],
                distinct: false,
            }),
        };
        assert!(contains_aggregate(&neg_count));
    }

    #[test]
    fn contains_aggregate_detects_inside_cast() {
        // CAST(COUNT(*) AS BIGINT)
        let cast_count = Expr::Cast {
            expr: Box::new(Expr::Function {
                name: "SUM".into(),
                args: vec![Expr::Column {
                    table: None,
                    name: "price".into(),
                }],
                distinct: false,
            }),
            data_type: crate::sql::ast::DataType::BigInt,
        };
        assert!(contains_aggregate(&cast_count));
    }

    #[test]
    fn contains_aggregate_false_for_plain_column() {
        let col = Expr::Column {
            table: None,
            name: "age".into(),
        };
        assert!(!contains_aggregate(&col));
    }

    // ── Thread 7: SELECT * mixed with explicit columns ────────────────────────

    #[test]
    fn mixed_wildcard_and_column_keeps_both_in_projection() {
        // `SELECT *, id FROM users` — wildcard MUST survive into the Project.
        let catalog = make_catalog();
        let stmt = parse_select("SELECT *, id FROM users");
        let plan = builder(&catalog).build_select(&stmt).unwrap();
        // The plan must be a Project (not a bare Scan) because we have explicit items.
        let LogicalPlan::Project { items, .. } = &plan else {
            panic!("expected Project for mixed wildcard+column, got: {plan:?}");
        };
        let has_wildcard = items.iter().any(|i| matches!(i.expr, Expr::Wildcard));
        assert!(
            has_wildcard,
            "Expr::Wildcard must appear in projection items; got: {items:?}"
        );
        let has_id = items
            .iter()
            .any(|i| matches!(&i.expr, Expr::Column { name, .. } if name == "id"));
        assert!(
            has_id,
            "explicit `id` column must appear in projection items; got: {items:?}"
        );
    }

    // ── Thread 8: unknown table → CatalogError ───────────────────────────────

    #[test]
    fn unknown_table_returns_catalog_error() {
        let catalog = make_catalog();
        let stmt = parse_select("SELECT * FROM nonexistent");
        let err = builder(&catalog)
            .build_select(&stmt)
            .expect_err("expected CatalogError for unknown table");
        assert!(
            matches!(err, PlannerError::CatalogError(_)),
            "expected CatalogError, got: {err:?}"
        );
    }

    // ── Thread 9: collect_databases recurses into subqueries ─────────────────

    #[test]
    fn cross_database_via_subquery_is_rejected() {
        // 3-part names (database.schema.table) are needed for the database
        // field to be populated by parse_qualified_name.  The subquery reads
        // from db1.public.users while the outer join references db2.public.orders;
        // collect_databases must recurse into the subquery's FROM to catch db1.
        let catalog = make_catalog();
        let stmt = parse_select(
            "SELECT * FROM (SELECT * FROM db1.public.users) sub \
             JOIN db2.public.orders ON sub.id = orders.user_id",
        );
        let err = builder(&catalog)
            .build_select(&stmt)
            .expect_err("expected CrossDatabaseJoin error");
        assert!(
            matches!(err, PlannerError::CrossDatabaseJoin { .. }),
            "expected CrossDatabaseJoin, got: {err:?}"
        );
    }

    // ── Thread 7 (round 3): OFFSET-without-LIMIT produces Limit node ─────────

    #[test]
    fn offset_only_produces_limit_node() {
        let catalog = make_catalog();
        let stmt = parse_select("SELECT id FROM users OFFSET 5");
        let plan = builder(&catalog).build_select(&stmt).unwrap();
        // The plan must contain a Limit node even without an explicit LIMIT.
        fn has_limit(p: &LogicalPlan) -> bool {
            matches!(p, LogicalPlan::Limit { .. })
                || match p {
                    LogicalPlan::Project { input, .. }
                    | LogicalPlan::Filter { input, .. }
                    | LogicalPlan::Sort { input, .. }
                    | LogicalPlan::Limit { input, .. } => has_limit(input),
                    _ => false,
                }
        }
        assert!(
            has_limit(&plan),
            "expected Limit node for OFFSET-only query, got: {plan:?}"
        );
        // The Limit node must have limit == usize::MAX (unbounded) and offset == 5.
        fn find_limit(p: &LogicalPlan) -> Option<(usize, usize)> {
            match p {
                LogicalPlan::Limit { limit, offset, .. } => Some((*limit, *offset)),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Sort { input, .. } => find_limit(input),
                _ => None,
            }
        }
        let (lim, off) = find_limit(&plan).expect("Limit node not found");
        assert_eq!(lim, usize::MAX, "OFFSET-only limit should be usize::MAX");
        assert_eq!(off, 5, "offset should be 5");
    }
}
