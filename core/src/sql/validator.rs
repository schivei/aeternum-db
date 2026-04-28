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
    AlterTableOperation, AlterTableStatement, BeginTransactionStatement, ColumnDef, CommitScope,
    CommitStatement, CreateTableStatement, DataType, DeleteStatement, EnumVariant, Expr,
    InsertStatement, RollbackScope, RollbackStatement, SelectItem, SelectStatement, Statement,
    UpdateStatement, ViewAsItem,
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

// ── User-defined type catalog ─────────────────────────────────────────────────

/// A named user-defined type stored in the catalog.
///
/// Once created, the resolved numeric values are immutable.  The type cannot
/// be dropped while any table column references it.
#[derive(Debug, Clone)]
pub struct UserTypeSchema {
    /// Type name (lowercased).
    pub name: String,
    /// Kind and body of the type.
    pub kind: UserTypeKind,
}

/// The kind of a [`UserTypeSchema`].
#[derive(Debug, Clone)]
pub enum UserTypeKind {
    /// An enumeration type (regular or FLAG).
    ///
    /// `resolved_values[i]` is the immutable numeric value for `variants[i]`.
    Enum {
        flag: bool,
        variants: Vec<EnumVariant>,
        /// System-assigned numeric values, parallel to `variants`.
        /// These are computed once at creation time and never change.
        resolved_values: Vec<u64>,
    },
    /// A composite (row/struct) type.
    Composite { fields: Vec<(String, DataType)> },
}

/// A simple in-memory schema catalog used for semantic validation.
#[derive(Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, TableSchema>,
    /// Named user-defined types (enum and composite).
    types: HashMap<String, UserTypeSchema>,
}

impl Catalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Catalog::default()
    }

    // ── Table management ──────────────────────────────────────────────────

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

    // ── User-defined type management ──────────────────────────────────────

    /// Register a user-defined type (enum or composite).
    ///
    /// For enum types the catalog computes and permanently stores the
    /// resolved numeric values — they cannot be changed after this call.
    pub fn add_type(&mut self, schema: UserTypeSchema) {
        self.types.insert(schema.name.to_lowercase(), schema);
    }

    /// Retrieve a user-defined type by name (case-insensitive).
    pub fn get_type(&self, name: &str) -> Option<&UserTypeSchema> {
        self.types.get(&name.to_lowercase())
    }

    /// Check whether a user-defined type exists.
    pub fn type_exists(&self, name: &str) -> bool {
        self.types.contains_key(&name.to_lowercase())
    }

    /// Remove a user-defined type from the catalog.
    ///
    /// Returns `Err(TypeInUse)` if any column in any registered table
    /// still references this type, preventing accidental data loss.
    pub fn remove_type(&mut self, name: &str) -> Result<(), ValidationError> {
        let lower = name.to_lowercase();
        if self.is_type_in_use(&lower) {
            return Err(ValidationError::TypeInUse(name.to_string()));
        }
        self.types.remove(&lower);
        Ok(())
    }

    /// Returns `true` if any column in any registered table has
    /// `DataType::EnumRef(name)` pointing at this type name.
    pub fn is_type_in_use(&self, name: &str) -> bool {
        let lower = name.to_lowercase();
        self.tables.values().any(|t| {
            t.columns
                .iter()
                .any(|c| matches!(&c.data_type, DataType::EnumRef(n) if n.to_lowercase() == lower))
        })
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
        expected: Box<DataType>,
        found: Box<DataType>,
        context: String,
    },
    /// An aggregate function was used in an invalid context.
    InvalidAggregateUsage(String),
    /// A `NOT NULL` column is being inserted/updated with a NULL value.
    NullConstraintViolation { table: String, column: String },
    /// Any other semantic constraint violation.
    ConstraintViolation(String),

    // ── User-defined type errors ───────────────────────────────────────────
    /// A column references a user-defined type name that has not been created.
    ///
    /// Use `CREATE ENUM name (...)` or `CREATE TYPE name AS (...)` first.
    TypeNotFound(String),
    /// A `DROP ENUM` or `DROP TYPE` was attempted but the type is still
    /// referenced by at least one column in a registered table.
    ///
    /// Drop or alter the referencing table(s) first.
    TypeInUse(String),
    /// An invalid value was supplied for an enum column.
    ///
    /// The value is neither a valid variant name nor a valid numeric value
    /// for the enum type.
    InvalidEnumValue { column: String, value: String },

    // ── Transaction-nesting errors ─────────────────────────────────────────
    /// A `COMMIT` or `ROLLBACK` was issued when no transaction is open.
    NoActiveTransaction,
    /// `BEGIN TRANSACTION <name>` was issued but `<name>` is already in use
    /// in this session's transaction stack.
    ///
    /// Transaction names are **session-scoped**: the same name can be used in
    /// different sessions simultaneously without conflict, but within one
    /// session each open transaction must have a unique name.
    TransactionNameConflict(String),
    /// `COMMIT TRANSACTION <name>` or `ROLLBACK TRANSACTION <name>` was
    /// issued but no transaction with that name is open in this session.
    TransactionNotFound(String),
    /// A named transaction cannot be committed or rolled back because a
    /// more-deeply nested transaction (`blocking`) is still open inside it.
    ///
    /// Transactions must be closed in LIFO (stack) order: the innermost open
    /// transaction must be resolved first.
    TransactionNestingViolation {
        /// The transaction the caller tried to commit or roll back.
        target: String,
        /// The innermost open transaction that is nested inside `target`
        /// and must be resolved first.
        blocking: String,
    },

    // ── VIEW AS restriction errors ─────────────────────────────────────────
    /// An aggregate function was used inside a `VIEW AS` clause.
    ///
    /// `VIEW AS` only allows primitive expressions — no `COUNT`, `SUM`, etc.
    ViewAsAggregateNotAllowed(String),
    /// A sub-select was used inside a `VIEW AS` clause.
    ///
    /// `VIEW AS` only allows primitive expressions — scalar sub-selects are
    /// not permitted.
    ViewAsSubqueryNotAllowed,
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
            ValidationError::TypeNotFound(name) => write!(
                f,
                "user-defined type '{name}' does not exist; \
                 create it with CREATE ENUM or CREATE TYPE first"
            ),
            ValidationError::TypeInUse(name) => write!(
                f,
                "cannot drop type '{name}': it is still referenced by one or more columns; \
                 drop or alter the referencing tables first"
            ),
            ValidationError::InvalidEnumValue { column, value } => write!(
                f,
                "invalid enum value '{value}' for column '{column}': \
                 not a recognised variant name or numeric value"
            ),
            ValidationError::NoActiveTransaction => {
                write!(f, "no active transaction")
            }
            ValidationError::TransactionNameConflict(name) => write!(
                f,
                "transaction name '{name}' is already in use in this session"
            ),
            ValidationError::TransactionNotFound(name) => write!(
                f,
                "transaction '{name}' is not active in the current session"
            ),
            ValidationError::TransactionNestingViolation { target, blocking } => write!(
                f,
                "cannot commit or rollback transaction '{target}': \
                 nested transaction '{blocking}' is still open and must be resolved first"
            ),
            ValidationError::ViewAsAggregateNotAllowed(func) => write!(
                f,
                "aggregate function '{func}' is not allowed in a VIEW AS clause; \
                 VIEW AS only supports primitive expressions"
            ),
            ValidationError::ViewAsSubqueryNotAllowed => write!(
                f,
                "sub-selects are not allowed in a VIEW AS clause; \
                 VIEW AS only supports primitive expressions"
            ),
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
            // Materialized views — structural validation is future work
            Statement::CreateMaterializedView(_) => Ok(()),
            // Transaction control — not yet validated, always passes
            Statement::BeginTransaction(_)
            | Statement::Commit(_)
            | Statement::Rollback(_)
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_) => Ok(()),
            // New DDL/DCL scaffolding — always passes
            Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::CreateUser(_)
            | Statement::DropUser(_)
            | Statement::CreateType(_)
            | Statement::DropType(_)
            | Statement::CreateDatabase(_)
            | Statement::DropDatabase(_)
            | Statement::UseDatabase(_)
            | Statement::CreateSchema(_)
            | Statement::DropSchema(_) => Ok(()),
            Statement::CreateEnum(s) => self.validate_create_enum(s),
            Statement::DropEnum(s) => self.validate_drop_enum(s),
        }
    }

    /// Validate a **sequence** of statements in execution order, tracking the
    /// nested transaction stack for this session.
    ///
    /// In addition to per-statement semantic checks (via [`validate`]), this
    /// method enforces the following transaction-nesting rules:
    ///
    /// - **Session-scoped names**: each open transaction name must be unique
    ///   within the stack.  The same name may be reused in a *different*
    ///   session simultaneously without conflict.
    /// - **LIFO ordering**: a named transaction can only be committed or
    ///   rolled back when it is the innermost open transaction.  If a more
    ///   deeply nested transaction is still open, the call returns
    ///   [`ValidationError::TransactionNestingViolation`].
    /// - **`COMMIT ALL` / `ROLLBACK ALL`**: these bypass LIFO ordering and
    ///   close the entire nesting stack at once.
    ///
    /// # Example
    ///
    /// ```rust
    /// use aeternumdb_core::sql::ast::{
    ///     BeginTransactionStatement, CommitScope, CommitStatement,
    ///     RollbackScope, RollbackStatement, Statement,
    /// };
    /// use aeternumdb_core::sql::validator::{Catalog, ValidationError, Validator};
    ///
    /// let catalog = Catalog::new();
    /// let validator = Validator::new(&catalog);
    ///
    /// // outer → inner open: committing outer fails
    /// let stmts = vec![
    ///     Statement::BeginTransaction(BeginTransactionStatement {
    ///         name: Some("outer".to_string()), isolation_level: None, read_only: false,
    ///     }),
    ///     Statement::BeginTransaction(BeginTransactionStatement {
    ///         name: Some("inner".to_string()), isolation_level: None, read_only: false,
    ///     }),
    ///     Statement::Commit(CommitStatement {
    ///         scope: CommitScope::Named("outer".to_string()), chain: false,
    ///     }),
    /// ];
    /// assert!(matches!(
    ///     validator.validate_sequence(&stmts),
    ///     Err(ValidationError::TransactionNestingViolation { .. })
    /// ));
    /// ```
    pub fn validate_sequence(&self, stmts: &[Statement]) -> Result<(), ValidationError> {
        // Each element is the optional name of one open transaction level,
        // ordered from outermost (index 0) to innermost (last index).
        let mut stack: Vec<Option<String>> = Vec::new();

        for stmt in stmts {
            // Run per-statement semantic checks first.
            self.validate(stmt)?;

            match stmt {
                Statement::BeginTransaction(BeginTransactionStatement { name, .. }) => {
                    if let Some(n) = name {
                        // Session-scoped uniqueness: name must not already be in the stack.
                        if stack.iter().any(|s| s.as_deref() == Some(n.as_str())) {
                            return Err(ValidationError::TransactionNameConflict(n.clone()));
                        }
                    }
                    stack.push(name.clone());
                }

                Statement::Commit(CommitStatement { scope, .. }) => match scope {
                    CommitScope::Current => {
                        stack.pop().ok_or(ValidationError::NoActiveTransaction)?;
                    }
                    CommitScope::Named(name) => {
                        let pos = stack
                            .iter()
                            .rposition(|s| s.as_deref() == Some(name.as_str()))
                            .ok_or_else(|| ValidationError::TransactionNotFound(name.clone()))?;
                        // Must be the innermost — no open children allowed.
                        if pos != stack.len() - 1 {
                            let blocking = stack
                                .last()
                                .and_then(|s| s.as_deref())
                                .unwrap_or("<anonymous>")
                                .to_string();
                            return Err(ValidationError::TransactionNestingViolation {
                                target: name.clone(),
                                blocking,
                            });
                        }
                        stack.truncate(pos);
                    }
                    CommitScope::All => {
                        stack.clear();
                    }
                },

                Statement::Rollback(RollbackStatement { scope, .. }) => match scope {
                    RollbackScope::Current => {
                        stack.pop().ok_or(ValidationError::NoActiveTransaction)?;
                    }
                    RollbackScope::ToSavepoint(_) => {
                        // Savepoints are within the current transaction level;
                        // they do not affect the nesting stack.
                        if stack.is_empty() {
                            return Err(ValidationError::NoActiveTransaction);
                        }
                    }
                    RollbackScope::Named(name) => {
                        let pos = stack
                            .iter()
                            .rposition(|s| s.as_deref() == Some(name.as_str()))
                            .ok_or_else(|| ValidationError::TransactionNotFound(name.clone()))?;
                        // Must be the innermost — no open children allowed.
                        if pos != stack.len() - 1 {
                            let blocking = stack
                                .last()
                                .and_then(|s| s.as_deref())
                                .unwrap_or("<anonymous>")
                                .to_string();
                            return Err(ValidationError::TransactionNestingViolation {
                                target: name.clone(),
                                blocking,
                            });
                        }
                        stack.truncate(pos);
                    }
                    RollbackScope::All => {
                        stack.clear();
                    }
                },

                Statement::Savepoint(_) | Statement::ReleaseSavepoint(_) => {
                    // Savepoints require an active transaction.
                    if stack.is_empty() {
                        return Err(ValidationError::NoActiveTransaction);
                    }
                }

                _ => {}
            }
        }

        Ok(())
    }

    fn validate_table_reference(
        &self,
        table_ref: &crate::sql::ast::TableReference,
        aliases: &mut HashMap<String, String>,
        table_names: &mut Vec<String>,
    ) -> Result<(), ValidationError> {
        use crate::sql::ast::TableReference;

        match table_ref {
            TableReference::Named { name, alias, .. } => {
                self.require_table(name)?;
                table_names.push(name.clone());
                if let Some(alias) = alias {
                    aliases.insert(alias.clone(), name.clone());
                }
                Ok(())
            }
            TableReference::Join { left, right, .. } => {
                self.validate_table_reference(left, aliases, table_names)?;
                self.validate_table_reference(right, aliases, table_names)?;
                Ok(())
            }
            TableReference::Subquery { query, .. } => self.validate_select(query),
        }
    }

    fn validate_select_expr(
        &self,
        expr: &Expr,
        default_table_name: Option<&str>,
        aliases: &HashMap<String, String>,
    ) -> Result<(), ValidationError> {
        match expr {
            Expr::Column {
                table: Some(table),
                name,
            } => {
                let resolved_table = aliases
                    .get(table)
                    .map(|t| t.as_str())
                    .unwrap_or(table.as_str());
                self.require_table(resolved_table)?;
                // After require_table succeeds, get_table must return Some since both use
                // the same underlying HashMap. Return TableNotFound if somehow they diverge.
                let schema = self.catalog.get_table(resolved_table).ok_or_else(|| {
                    ValidationError::TableNotFound {
                        table: resolved_table.to_string(),
                    }
                })?;
                self.require_column(schema, name)?;
                Ok(())
            }
            _ => self.validate_expr(expr, default_table_name),
        }
    }

    fn validate_select(&self, sel: &SelectStatement) -> Result<(), ValidationError> {
        let mut aliases = HashMap::new();
        let mut table_names = Vec::new();

        if let Some(from) = &sel.from {
            self.validate_table_reference(from, &mut aliases, &mut table_names)?;
        }

        let table_name = if table_names.len() == 1 {
            Some(table_names[0].as_str())
        } else {
            None
        };

        // Validate SELECT list
        for item in &sel.columns {
            match item {
                SelectItem::Wildcard => {}
                SelectItem::QualifiedWildcard(name) => {
                    let resolved_table = aliases
                        .get(name)
                        .map(|t| t.as_str())
                        .unwrap_or(name.as_str());
                    self.require_table(resolved_table)?;
                }
                SelectItem::Expr { expr, .. } => {
                    self.validate_select_expr(expr, table_name, &aliases)?;
                }
                SelectItem::Expand { expr, .. } => {
                    // EXPAND must reference a column; validate the inner expr
                    self.validate_select_expr(expr, table_name, &aliases)?;
                }
            }
        }

        // Validate WHERE
        if let Some(w) = &sel.where_clause {
            self.validate_select_expr(w, table_name, &aliases)?;
            self.check_no_aggregate_in_where(w)?;
        }

        // Validate GROUP BY
        for g in &sel.group_by {
            self.validate_select_expr(g, table_name, &aliases)?;
        }

        // Validate HAVING
        if let Some(h) = &sel.having {
            self.validate_select_expr(h, table_name, &aliases)?;
        }

        // Validate ORDER BY
        for o in &sel.order_by {
            self.validate_select_expr(&o.expr, table_name, &aliases)?;
        }

        // Validate VIEW AS — only primitive expressions allowed
        if let Some(view_as) = &sel.view_as {
            for item in view_as {
                self.validate_view_as_item(item)?;
            }
        }

        Ok(())
    }

    /// Validate a single [`ViewAsItem`]: reject aggregates and sub-selects.
    fn validate_view_as_item(&self, item: &ViewAsItem) -> Result<(), ValidationError> {
        self.check_no_aggregate_in_view_as(&item.expr)?;
        self.check_no_subquery_in_view_as(&item.expr)?;
        Ok(())
    }

    /// Recursively check that `expr` contains no aggregate function calls.
    fn check_no_aggregate_in_view_as(&self, expr: &Expr) -> Result<(), ValidationError> {
        if let Expr::Function { name, .. } = expr {
            if is_aggregate_function(name) {
                return Err(ValidationError::ViewAsAggregateNotAllowed(name.clone()));
            }
        }
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                self.check_no_aggregate_in_view_as(left)?;
                self.check_no_aggregate_in_view_as(right)?;
            }
            Expr::UnaryOp { expr, .. } => self.check_no_aggregate_in_view_as(expr)?,
            Expr::Function { args, .. } => {
                for a in args {
                    self.check_no_aggregate_in_view_as(a)?;
                }
            }
            Expr::Cast { expr, .. } => self.check_no_aggregate_in_view_as(expr)?,
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(op) = operand {
                    self.check_no_aggregate_in_view_as(op)?;
                }
                for (cond, then) in conditions {
                    self.check_no_aggregate_in_view_as(cond)?;
                    self.check_no_aggregate_in_view_as(then)?;
                }
                if let Some(e) = else_result {
                    self.check_no_aggregate_in_view_as(e)?;
                }
            }
            Expr::IsNull { expr, .. } => self.check_no_aggregate_in_view_as(expr)?,
            Expr::Between {
                expr, low, high, ..
            } => {
                self.check_no_aggregate_in_view_as(expr)?;
                self.check_no_aggregate_in_view_as(low)?;
                self.check_no_aggregate_in_view_as(high)?;
            }
            Expr::InList { expr, list, .. } => {
                self.check_no_aggregate_in_view_as(expr)?;
                for e in list {
                    self.check_no_aggregate_in_view_as(e)?;
                }
            }
            Expr::Substring {
                expr,
                from_pos,
                len,
            } => {
                self.check_no_aggregate_in_view_as(expr)?;
                if let Some(fp) = from_pos {
                    self.check_no_aggregate_in_view_as(fp)?;
                }
                if let Some(l) = len {
                    self.check_no_aggregate_in_view_as(l)?;
                }
            }
            Expr::Position { substr, in_expr } => {
                self.check_no_aggregate_in_view_as(substr)?;
                self.check_no_aggregate_in_view_as(in_expr)?;
            }
            Expr::Trim {
                expr, trim_what, ..
            } => {
                self.check_no_aggregate_in_view_as(expr)?;
                if let Some(tw) = trim_what {
                    self.check_no_aggregate_in_view_as(tw)?;
                }
            }
            Expr::MatchAgainst { match_value, .. } => {
                self.check_no_aggregate_in_view_as(match_value)?;
            }
            Expr::ArrayOp { expr, right, .. } => {
                self.check_no_aggregate_in_view_as(expr)?;
                self.check_no_aggregate_in_view_as(right)?;
            }
            // Scalar subquery handled by check_no_subquery_in_view_as
            Expr::Subquery(_) | Expr::InSubquery { .. } => {}
            // Leaves — nothing to recurse into
            Expr::Literal(_) | Expr::Column { .. } | Expr::Wildcard => {}
        }
        Ok(())
    }

    /// Recursively check that `expr` contains no scalar sub-selects.
    fn check_no_subquery_in_view_as(&self, expr: &Expr) -> Result<(), ValidationError> {
        match expr {
            Expr::Subquery(_) | Expr::InSubquery { .. } => {
                Err(ValidationError::ViewAsSubqueryNotAllowed)
            }
            Expr::BinaryOp { left, right, .. } => {
                self.check_no_subquery_in_view_as(left)?;
                self.check_no_subquery_in_view_as(right)?;
                Ok(())
            }
            Expr::UnaryOp { expr, .. } => self.check_no_subquery_in_view_as(expr),
            Expr::Function { args, .. } => {
                for a in args {
                    self.check_no_subquery_in_view_as(a)?;
                }
                Ok(())
            }
            Expr::Cast { expr, .. } => self.check_no_subquery_in_view_as(expr),
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(op) = operand {
                    self.check_no_subquery_in_view_as(op)?;
                }
                for (cond, then) in conditions {
                    self.check_no_subquery_in_view_as(cond)?;
                    self.check_no_subquery_in_view_as(then)?;
                }
                if let Some(e) = else_result {
                    self.check_no_subquery_in_view_as(e)?;
                }
                Ok(())
            }
            Expr::IsNull { expr, .. } => self.check_no_subquery_in_view_as(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                self.check_no_subquery_in_view_as(expr)?;
                self.check_no_subquery_in_view_as(low)?;
                self.check_no_subquery_in_view_as(high)
            }
            Expr::InList { expr, list, .. } => {
                self.check_no_subquery_in_view_as(expr)?;
                for e in list {
                    self.check_no_subquery_in_view_as(e)?;
                }
                Ok(())
            }
            Expr::Substring {
                expr,
                from_pos,
                len,
            } => {
                self.check_no_subquery_in_view_as(expr)?;
                if let Some(fp) = from_pos {
                    self.check_no_subquery_in_view_as(fp)?;
                }
                if let Some(l) = len {
                    self.check_no_subquery_in_view_as(l)?;
                }
                Ok(())
            }
            Expr::Position { substr, in_expr } => {
                self.check_no_subquery_in_view_as(substr)?;
                self.check_no_subquery_in_view_as(in_expr)
            }
            Expr::Trim {
                expr, trim_what, ..
            } => {
                self.check_no_subquery_in_view_as(expr)?;
                if let Some(tw) = trim_what {
                    self.check_no_subquery_in_view_as(tw)?;
                }
                Ok(())
            }
            Expr::MatchAgainst { match_value, .. } => {
                self.check_no_subquery_in_view_as(match_value)
            }
            Expr::ArrayOp { expr, right, .. } => {
                self.check_no_subquery_in_view_as(expr)?;
                self.check_no_subquery_in_view_as(right)
            }
            // Leaves
            Expr::Literal(_) | Expr::Column { .. } | Expr::Wildcard => Ok(()),
        }
    }

    // ── INSERT ────────────────────────────────────────────────────────────────

    fn validate_insert(&self, ins: &InsertStatement) -> Result<(), ValidationError> {
        let schema = self.get_required_table(&ins.table)?;

        // Resolve the effective target columns for this INSERT.
        // If no explicit column list is provided, SQL treats VALUES as mapping
        // to all table columns in schema order.
        let effective_columns = if ins.columns.is_empty() {
            schema
                .columns
                .iter()
                .map(|col| col.name.clone())
                .collect::<Vec<_>>()
        } else {
            for col in &ins.columns {
                self.require_column(schema, col)?;
            }
            ins.columns.clone()
        };

        for row in &ins.values {
            // Validate expressions in VALUES
            for val in row {
                self.validate_expr(val, Some(&ins.table))?;
            }

            // Enforce NOT NULL constraints for all target columns. When the
            // column list is implicit, a short row means trailing columns are
            // omitted; that still violates NOT NULL for any required column.
            for (i, col_name) in effective_columns.iter().enumerate() {
                if let Some(col_schema) = schema.get_column(col_name) {
                    if !col_schema.nullable {
                        match row.get(i) {
                            Some(Expr::Literal(crate::sql::ast::Value::Null)) | None => {
                                return Err(ValidationError::NullConstraintViolation {
                                    table: ins.table.clone(),
                                    column: col_name.clone(),
                                });
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // ── UPDATE ────────────────────────────────────────────────────────────────

    fn validate_update(&self, upd: &UpdateStatement) -> Result<(), ValidationError> {
        let schema = self.get_required_table(&upd.table)?;

        for (col_name, expr) in &upd.assignments {
            self.require_column(schema, col_name)?;

            // Check NOT NULL violation
            let col_schema =
                schema
                    .get_column(col_name)
                    .ok_or_else(|| ValidationError::ColumnNotFound {
                        table: schema.name.clone(),
                        column: col_name.clone(),
                    })?;
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
            // Verify that any EnumRef column points to a type in the catalog.
            if let DataType::EnumRef(type_name) = &col.data_type {
                if !self.catalog.type_exists(type_name) {
                    return Err(ValidationError::TypeNotFound(type_name.clone()));
                }
            }
            seen_names.push(lower);
        }

        Ok(())
    }

    // ── CREATE ENUM / DROP ENUM ───────────────────────────────────────────────

    fn validate_create_enum(
        &self,
        ce: &crate::sql::ast::CreateEnumStatement,
    ) -> Result<(), ValidationError> {
        if !ce.if_not_exists && self.catalog.type_exists(&ce.name) {
            return Err(ValidationError::ConstraintViolation(format!(
                "enum type '{}' already exists",
                ce.name
            )));
        }
        if ce.variants.is_empty() {
            return Err(ValidationError::ConstraintViolation(format!(
                "enum type '{}' must have at least one variant",
                ce.name
            )));
        }
        // Duplicate variant name check (case-insensitive).
        let mut seen: Vec<String> = Vec::new();
        for v in &ce.variants {
            let lower = v.name.to_lowercase();
            if seen.contains(&lower) {
                return Err(ValidationError::ConstraintViolation(format!(
                    "duplicate variant '{}' in CREATE ENUM '{}'",
                    v.name, ce.name
                )));
            }
            seen.push(lower);
        }
        // FLAG enums can have at most one NONE variant.
        let none_count = ce.variants.iter().filter(|v| v.is_none).count();
        if none_count > 1 {
            return Err(ValidationError::ConstraintViolation(format!(
                "enum type '{}' has more than one NONE variant",
                ce.name
            )));
        }
        Ok(())
    }

    fn validate_drop_enum(
        &self,
        de: &crate::sql::ast::DropEnumStatement,
    ) -> Result<(), ValidationError> {
        if !de.if_exists && !self.catalog.type_exists(&de.name) {
            return Err(ValidationError::TypeNotFound(de.name.clone()));
        }
        if self.catalog.is_type_in_use(&de.name) {
            return Err(ValidationError::TypeInUse(de.name.clone()));
        }
        Ok(())
    }

    // ── ALTER TABLE ───────────────────────────────────────────────────────────

    fn validate_alter_table(&self, alt: &AlterTableStatement) -> Result<(), ValidationError> {
        let schema = self.get_required_table(&alt.table)?;

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
            Expr::Between {
                expr, low, high, ..
            } => {
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
            Expr::ArrayOp { expr, right, .. } => {
                self.validate_expr(expr, table)?;
                self.validate_expr(right, table)
            }
            Expr::Substring {
                expr,
                from_pos,
                len,
            } => {
                self.validate_expr(expr, table)?;
                if let Some(e) = from_pos {
                    self.validate_expr(e, table)?;
                }
                if let Some(e) = len {
                    self.validate_expr(e, table)?;
                }
                Ok(())
            }
            Expr::Position { substr, in_expr } => {
                self.validate_expr(substr, table)?;
                self.validate_expr(in_expr, table)
            }
            Expr::Trim {
                expr, trim_what, ..
            } => {
                self.validate_expr(expr, table)?;
                if let Some(e) = trim_what {
                    self.validate_expr(e, table)?;
                }
                Ok(())
            }
            Expr::MatchAgainst { match_value, .. } => self.validate_expr(match_value, table),
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
            Expr::Between {
                expr, low, high, ..
            } => {
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

    fn get_required_table<'b>(&'b self, name: &str) -> Result<&'b TableSchema, ValidationError> {
        self.catalog
            .get_table(name)
            .ok_or_else(|| ValidationError::TableNotFound {
                table: name.to_string(),
            })
    }

    fn require_table(&self, name: &str) -> Result<(), ValidationError> {
        if !self.catalog.table_exists(name) {
            return Err(ValidationError::TableNotFound {
                table: name.to_string(),
            });
        }
        Ok(())
    }

    fn require_column(&self, schema: &TableSchema, col: &str) -> Result<(), ValidationError> {
        if schema.get_column(col).is_none() {
            return Err(ValidationError::ColumnNotFound {
                table: schema.name.clone(),
                column: col.to_string(),
            });
        }
        Ok(())
    }
}

// ── Aggregate detection helper ─────────────────────────────────────────────────

/// Returns `true` if `name` (uppercase) is a standard aggregate function.
fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "ARRAY_AGG" | "STRING_AGG"
    )
}

// ── Apply DDL to catalog ───────────────────────────────────────────────────────

/// Apply a [`CreateTableStatement`] to a [`Catalog`].
///
/// Call this after validating and executing a DDL statement so that subsequent
/// DML statements can be validated against the updated schema.
pub fn apply_create_table(catalog: &mut Catalog, stmt: &CreateTableStatement) {
    let columns = stmt.columns.iter().map(column_def_to_schema).collect();
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
