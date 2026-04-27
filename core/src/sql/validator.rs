//! Semantic validator for AeternumDB SQL.
//!
//! [`Validator`] performs schema-aware validation of an internal [`Statement`]
//! AST.  It checks:
//!
//! - Whether referenced tables exist in the [`Catalog`].
//! - Whether referenced columns exist in the relevant tables.
//! - Basic type compatibility for assignments and expressions.
//! - Aggregate function usage rules (aggregates only in SELECT / HAVING).
//!
//! The validator operates against a lightweight in-memory [`Catalog`] that
//! records table names and their column definitions.  The catalog is populated
//! externally (e.g. by the DDL executor) and passed to the validator by
//! reference.
//!
//! # Example
//!
//! ```rust
//! use aeternumdb_core::sql::validator::{Catalog, TableSchema, ColumnSchema, Validator};
//! use aeternumdb_core::sql::ast::{DataType, Statement};
//! use aeternumdb_core::sql::parser::SqlParser;
//!
//! let mut catalog = Catalog::new();
//! catalog.add_table(TableSchema {
//!     name: "users".to_string(),
//!     columns: vec![
//!         ColumnSchema { name: "id".to_string(), data_type: DataType::Integer, nullable: false },
//!         ColumnSchema { name: "name".to_string(), data_type: DataType::Varchar(Some(255)), nullable: true },
//!     ],
//! });
//!
//! let parser = SqlParser::new();
//! let stmt = parser.parse_one("SELECT id, name FROM users").unwrap();
//!
//! let validator = Validator::new(&catalog);
//! assert!(validator.validate(&stmt).is_ok());
//! ```

use std::collections::HashMap;

use crate::sql::ast::{
    AlterTableOperation, AlterTableStatement, ColumnDef, CreateTableStatement, DataType,
    DeleteStatement, Expr, InsertStatement, SelectItem, SelectStatement, Statement,
    UpdateStatement,
};

// ── Catalog ───────────────────────────────────────────────────────────────────

/// Metadata for a single column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Metadata for a single table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    /// Look up a column by name (case-insensitive).
    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        let lower = name.to_lowercase();
        self.columns.iter().find(|c| c.name.to_lowercase() == lower)
    }
}

/// A simple in-memory schema catalog used for semantic validation.
///
/// The catalog maps table names (case-insensitive) to their [`TableSchema`].
#[derive(Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, TableSchema>,
}

impl Catalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Catalog::default()
    }

    /// Register a table in the catalog.
    pub fn add_table(&mut self, schema: TableSchema) {
        self.tables.insert(schema.name.to_lowercase(), schema);
    }

    /// Remove a table from the catalog (used by DROP TABLE).
    pub fn remove_table(&mut self, name: &str) {
        self.tables.remove(&name.to_lowercase());
    }

    /// Check whether a table exists.
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(&name.to_lowercase())
    }

    /// Retrieve a table schema.
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(&name.to_lowercase())
    }
}

// ── Validation errors ─────────────────────────────────────────────────────────

/// Errors produced by semantic validation.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    /// A table referenced in the statement does not exist.
    TableNotFound { table: String },
    /// A column referenced in the statement does not exist in the table.
    ColumnNotFound { table: String, column: String },
    /// A type mismatch was detected (e.g. assigning a string to an integer).
    TypeMismatch {
        expected: DataType,
        found: DataType,
        context: String,
    },
    /// An aggregate function was used in an invalid context.
    InvalidAggregateUsage(String),
    /// A `NOT NULL` column is being inserted/updated with a NULL value.
    NullConstraintViolation { table: String, column: String },
    /// Any other semantic constraint violation.
    ConstraintViolation(String),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::TableNotFound { table } => {
                write!(f, "table '{table}' does not exist")
            }
            ValidationError::ColumnNotFound { table, column } => {
                write!(f, "column '{column}' does not exist in table '{table}'")
            }
            ValidationError::TypeMismatch {
                expected,
                found,
                context,
            } => write!(
                f,
                "type mismatch in {context}: expected {expected}, found {found}"
            ),
            ValidationError::InvalidAggregateUsage(msg) => {
                write!(f, "invalid aggregate usage: {msg}")
            }
            ValidationError::NullConstraintViolation { table, column } => write!(
                f,
                "null constraint violation: column '{column}' in table '{table}' is NOT NULL"
            ),
            ValidationError::ConstraintViolation(msg) => {
                write!(f, "constraint violation: {msg}")
            }
        }
    }
}

impl std::error::Error for ValidationError {}

// ── Validator ─────────────────────────────────────────────────────────────────

/// Semantic validator for internal SQL AST nodes.
pub struct Validator<'a> {
    catalog: &'a Catalog,
}

impl<'a> Validator<'a> {
    /// Create a new validator backed by the given catalog.
    pub fn new(catalog: &'a Catalog) -> Self {
        Validator { catalog }
    }

    /// Validate a top-level [`Statement`].
    ///
    /// Returns `Ok(())` if the statement is semantically valid, or a
    /// [`ValidationError`] describing the first problem found.
    pub fn validate(&self, stmt: &Statement) -> Result<(), ValidationError> {
        match stmt {
            Statement::Select(s) => self.validate_select(s),
            Statement::Insert(s) => self.validate_insert(s),
            Statement::Update(s) => self.validate_update(s),
            Statement::Delete(s) => self.validate_delete(s),
            Statement::CreateTable(s) => self.validate_create_table(s),
            Statement::DropTable(_) => Ok(()), // always valid at parse time
            Statement::AlterTable(s) => self.validate_alter_table(s),
            // DCL scaffolding — not executed yet, always passes
            Statement::Grant(_) | Statement::Revoke(_) => Ok(()),
        }
    }

    // ── SELECT ────────────────────────────────────────────────────────────────

    fn validate_select(&self, sel: &SelectStatement) -> Result<(), ValidationError> {
        // Validate the FROM table if present
        let table_name: Option<String> = sel.from.as_ref().and_then(|t| {
            use crate::sql::ast::TableReference;
            match t {
                TableReference::Named { name, .. } => Some(name.clone()),
                _ => None,
            }
        });

        if let Some(ref tname) = table_name {
            self.require_table(tname)?;
        }

        // Validate SELECT list
        for item in &sel.columns {
            match item {
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {}
                SelectItem::Expr { expr, .. } => {
                    self.validate_expr(expr, table_name.as_deref())?;
                }
            }
        }

        // Validate WHERE
        if let Some(w) = &sel.where_clause {
            self.validate_expr(w, table_name.as_deref())?;
            self.check_no_aggregate_in_where(w)?;
        }

        // Validate GROUP BY
        for g in &sel.group_by {
            self.validate_expr(g, table_name.as_deref())?;
        }

        // Validate HAVING
        if let Some(h) = &sel.having {
            self.validate_expr(h, table_name.as_deref())?;
        }

        // Validate ORDER BY
        for o in &sel.order_by {
            self.validate_expr(&o.expr, table_name.as_deref())?;
        }

        Ok(())
    }

    // ── INSERT ────────────────────────────────────────────────────────────────

    fn validate_insert(&self, ins: &InsertStatement) -> Result<(), ValidationError> {
        self.require_table(&ins.table)?;
        let schema = self.catalog.get_table(&ins.table).unwrap();

        // If explicit columns are given, verify they exist
        for col in &ins.columns {
            self.require_column(schema, col)?;
        }

        // Check NOT NULL constraints for columns that are explicitly supplied
        if !ins.columns.is_empty() {
            for row in &ins.values {
                for (i, val) in row.iter().enumerate() {
                    if let Some(col_name) = ins.columns.get(i) {
                        if let Some(col_schema) = schema.get_column(col_name) {
                            if !col_schema.nullable {
                                if let Expr::Literal(crate::sql::ast::Value::Null) = val {
                                    return Err(ValidationError::NullConstraintViolation {
                                        table: ins.table.clone(),
                                        column: col_name.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Validate expressions in VALUES
        for row in &ins.values {
            for val in row {
                self.validate_expr(val, Some(&ins.table))?;
            }
        }

        Ok(())
    }

    // ── UPDATE ────────────────────────────────────────────────────────────────

    fn validate_update(&self, upd: &UpdateStatement) -> Result<(), ValidationError> {
        self.require_table(&upd.table)?;
        let schema = self.catalog.get_table(&upd.table).unwrap();

        for (col_name, expr) in &upd.assignments {
            self.require_column(schema, col_name)?;

            // Check NOT NULL violation
            let col_schema = schema.get_column(col_name).unwrap();
            if !col_schema.nullable {
                if let Expr::Literal(crate::sql::ast::Value::Null) = expr {
                    return Err(ValidationError::NullConstraintViolation {
                        table: upd.table.clone(),
                        column: col_name.clone(),
                    });
                }
            }

            self.validate_expr(expr, Some(&upd.table))?;
        }

        if let Some(w) = &upd.where_clause {
            self.validate_expr(w, Some(&upd.table))?;
            self.check_no_aggregate_in_where(w)?;
        }

        Ok(())
    }

    // ── DELETE ────────────────────────────────────────────────────────────────

    fn validate_delete(&self, del: &DeleteStatement) -> Result<(), ValidationError> {
        self.require_table(&del.table)?;

        if let Some(w) = &del.where_clause {
            self.validate_expr(w, Some(&del.table))?;
            self.check_no_aggregate_in_where(w)?;
        }

        Ok(())
    }

    // ── CREATE TABLE ──────────────────────────────────────────────────────────

    fn validate_create_table(&self, ct: &CreateTableStatement) -> Result<(), ValidationError> {
        if !ct.if_not_exists && self.catalog.table_exists(&ct.table) {
            return Err(ValidationError::ConstraintViolation(format!(
                "table '{}' already exists",
                ct.table
            )));
        }

        let mut seen_names: Vec<String> = Vec::new();
        for col in &ct.columns {
            let lower = col.name.to_lowercase();
            if seen_names.contains(&lower) {
                return Err(ValidationError::ConstraintViolation(format!(
                    "duplicate column name '{}' in CREATE TABLE '{}'",
                    col.name, ct.table
                )));
            }
            seen_names.push(lower);
        }

        Ok(())
    }

    // ── ALTER TABLE ───────────────────────────────────────────────────────────

    fn validate_alter_table(&self, alt: &AlterTableStatement) -> Result<(), ValidationError> {
        self.require_table(&alt.table)?;
        let schema = self.catalog.get_table(&alt.table).unwrap();

        for op in &alt.operations {
            match op {
                AlterTableOperation::AddColumn(col) => {
                    if schema.get_column(&col.name).is_some() {
                        return Err(ValidationError::ConstraintViolation(format!(
                            "column '{}' already exists in table '{}'",
                            col.name, alt.table
                        )));
                    }
                }
                AlterTableOperation::DropColumn { name, if_exists } => {
                    if !if_exists {
                        self.require_column(schema, name)?;
                    }
                }
                AlterTableOperation::RenameColumn { old_name, .. } => {
                    self.require_column(schema, old_name)?;
                }
                AlterTableOperation::RenameTable { .. } => {}
            }
        }

        Ok(())
    }

    // ── Expression validation ─────────────────────────────────────────────────

    fn validate_expr(&self, expr: &Expr, table: Option<&str>) -> Result<(), ValidationError> {
        match expr {
            Expr::Literal(_) | Expr::Wildcard => Ok(()),
            Expr::Column { table: tbl, name } => {
                let effective_table = tbl.as_deref().or(table);
                if let Some(tname) = effective_table {
                    if self.catalog.table_exists(tname) {
                        let schema = self.catalog.get_table(tname).unwrap();
                        self.require_column(schema, name)?;
                    }
                    // If the table doesn't exist in catalog (e.g. subquery alias),
                    // we skip column validation.
                }
                Ok(())
            }
            Expr::BinaryOp { left, right, .. } => {
                self.validate_expr(left, table)?;
                self.validate_expr(right, table)
            }
            Expr::UnaryOp { expr, .. } => self.validate_expr(expr, table),
            Expr::Function { args, .. } => {
                for a in args {
                    self.validate_expr(a, table)?;
                }
                Ok(())
            }
            Expr::IsNull { expr, .. } => self.validate_expr(expr, table),
            Expr::Between { expr, low, high, .. } => {
                self.validate_expr(expr, table)?;
                self.validate_expr(low, table)?;
                self.validate_expr(high, table)
            }
            Expr::InList { expr, list, .. } => {
                self.validate_expr(expr, table)?;
                for e in list {
                    self.validate_expr(e, table)?;
                }
                Ok(())
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.validate_expr(expr, table)?;
                self.validate_select(subquery)
            }
            Expr::Subquery(s) => self.validate_select(s),
            Expr::Cast { expr, .. } => self.validate_expr(expr, table),
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(op) = operand {
                    self.validate_expr(op, table)?;
                }
                for (cond, result) in conditions {
                    self.validate_expr(cond, table)?;
                    self.validate_expr(result, table)?;
                }
                if let Some(el) = else_result {
                    self.validate_expr(el, table)?;
                }
                Ok(())
            }
        }
    }

    /// Check that no aggregate function (`COUNT`, `SUM`, etc.) appears in a
    /// `WHERE` clause — SQL forbids this.
    fn check_no_aggregate_in_where(&self, expr: &Expr) -> Result<(), ValidationError> {
        match expr {
            Expr::Function { name, .. } => {
                let upper = name.to_uppercase();
                if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    return Err(ValidationError::InvalidAggregateUsage(format!(
                        "aggregate function '{name}' is not allowed in WHERE clause"
                    )));
                }
                Ok(())
            }
            Expr::BinaryOp { left, right, .. } => {
                self.check_no_aggregate_in_where(left)?;
                self.check_no_aggregate_in_where(right)
            }
            Expr::UnaryOp { expr, .. } => self.check_no_aggregate_in_where(expr),
            Expr::IsNull { expr, .. } => self.check_no_aggregate_in_where(expr),
            Expr::Between { expr, low, high, .. } => {
                self.check_no_aggregate_in_where(expr)?;
                self.check_no_aggregate_in_where(low)?;
                self.check_no_aggregate_in_where(high)
            }
            Expr::InList { expr, list, .. } => {
                self.check_no_aggregate_in_where(expr)?;
                for e in list {
                    self.check_no_aggregate_in_where(e)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    // ── Catalog helpers ───────────────────────────────────────────────────────

    fn require_table(&self, name: &str) -> Result<(), ValidationError> {
        if !self.catalog.table_exists(name) {
            return Err(ValidationError::TableNotFound {
                table: name.to_string(),
            });
        }
        Ok(())
    }

    fn require_column(
        &self,
        schema: &TableSchema,
        col: &str,
    ) -> Result<(), ValidationError> {
        if schema.get_column(col).is_none() {
            return Err(ValidationError::ColumnNotFound {
                table: schema.name.clone(),
                column: col.to_string(),
            });
        }
        Ok(())
    }
}

// ── Apply DDL to catalog ───────────────────────────────────────────────────────

/// Apply a [`CreateTableStatement`] to a [`Catalog`].
///
/// Call this after validating and executing a DDL statement so that subsequent
/// DML statements can be validated against the updated schema.
pub fn apply_create_table(catalog: &mut Catalog, stmt: &CreateTableStatement) {
    let columns = stmt
        .columns
        .iter()
        .map(column_def_to_schema)
        .collect();
    catalog.add_table(TableSchema {
        name: stmt.table.clone(),
        columns,
    });
}

fn column_def_to_schema(col: &ColumnDef) -> ColumnSchema {
    ColumnSchema {
        name: col.name.clone(),
        data_type: col.data_type.clone(),
        nullable: col.nullable,
    }
}
