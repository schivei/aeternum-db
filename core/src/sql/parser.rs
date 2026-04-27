//! SQL parser for AeternumDB.
//!
//! This module provides [`SqlParser`], the main entry point for parsing SQL
//! strings into an internal [`Statement`] AST.  It wraps the
//! [`sqlparser`](::sqlparser) crate and converts its AST into the internal
//! representation defined in [`crate::sql::ast`].
//!
//! # Example
//!
//! ```rust
//! use aeternumdb_core::sql::parser::SqlParser;
//! use aeternumdb_core::sql::ast::Statement;
//!
//! let parser = SqlParser::new();
//! let stmts = parser.parse("SELECT id, name FROM users WHERE age > 18").unwrap();
//! assert_eq!(stmts.len(), 1);
//! assert!(matches!(stmts[0], Statement::Select(_)));
//! ```

use sqlparser::parser::Parser as SpParser;
use sqlparser::parser::ParserError;

use crate::sql::ast::{AstError, Statement};
use crate::sql::dialect::AeternumDialect;

// ── Error type ────────────────────────────────────────────────────────────────

/// Errors returned by [`SqlParser`].
#[derive(Debug, Clone, PartialEq)]
pub enum SqlError {
    /// A syntax error produced by the underlying sqlparser-rs lexer/parser.
    ParseError {
        message: String,
        /// 1-based line number, if available.
        line: Option<usize>,
        /// 1-based column number, if available.
        col: Option<usize>,
    },
    /// The SQL was parsed successfully, but an unsupported or invalid
    /// construct was encountered during conversion to the internal AST.
    AstError(AstError),
    /// Empty input was provided.
    EmptyInput,
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlError::ParseError { message, line, col } => {
                write!(f, "SQL parse error")?;
                if let (Some(l), Some(c)) = (line, col) {
                    write!(f, " at line {l}, col {c}")?;
                } else if let Some(l) = line {
                    write!(f, " at line {l}")?;
                }
                write!(f, ": {message}")
            }
            SqlError::AstError(e) => write!(f, "AST conversion error: {e}"),
            SqlError::EmptyInput => write!(f, "empty SQL input"),
        }
    }
}

impl std::error::Error for SqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SqlError::AstError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<AstError> for SqlError {
    fn from(e: AstError) -> Self {
        SqlError::AstError(e)
    }
}

/// Convert a [`ParserError`] from sqlparser-rs into a [`SqlError`], attempting
/// to extract line/column information from the error message.
fn parse_error_to_sql_error(e: ParserError) -> SqlError {
    let message = match &e {
        ParserError::TokenizerError(s) | ParserError::ParserError(s) => s.clone(),
        ParserError::RecursionLimitExceeded => "recursion limit exceeded".to_string(),
    };

    // sqlparser embeds "at line N, column M" in some error messages.
    let (line, col) = extract_line_col(&message);

    SqlError::ParseError { message, line, col }
}

/// Parse `"... at line N, column M"` from a sqlparser error string.
fn extract_line_col(msg: &str) -> (Option<usize>, Option<usize>) {
    // Pattern: "at line N, column M" or "Line: N, Column: M"
    let line = extract_after(msg, "at line ").or_else(|| extract_after(msg, "Line: "));
    let col = extract_after(msg, "column ").or_else(|| extract_after(msg, "Column: "));
    (line, col)
}

fn extract_after(haystack: &str, needle: &str) -> Option<usize> {
    let pos = haystack.find(needle)?;
    let rest = &haystack[pos + needle.len()..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse::<usize>().ok()
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// The AeternumDB SQL parser.
///
/// Wraps [`sqlparser::parser::Parser`] with the [`AeternumDialect`] and
/// converts the resulting AST into the internal [`Statement`] representation.
///
/// Create a single instance and reuse it — construction is cheap.
pub struct SqlParser {
    dialect: AeternumDialect,
}

impl SqlParser {
    /// Create a new [`SqlParser`] using the [`AeternumDialect`].
    pub fn new() -> Self {
        SqlParser {
            dialect: AeternumDialect,
        }
    }

    /// Parse one or more SQL statements separated by semicolons.
    ///
    /// Returns a `Vec` containing one [`Statement`] per SQL statement.
    ///
    /// # Errors
    ///
    /// Returns a [`SqlError`] if the input is syntactically invalid or
    /// contains an unsupported SQL construct.
    ///
    /// # Example
    ///
    /// ```rust
    /// use aeternumdb_core::sql::parser::SqlParser;
    /// use aeternumdb_core::sql::ast::Statement;
    ///
    /// let parser = SqlParser::new();
    /// let stmts = parser.parse("SELECT 1; SELECT 2").unwrap();
    /// assert_eq!(stmts.len(), 2);
    /// ```
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, SqlError> {
        if sql.trim().is_empty() {
            return Err(SqlError::EmptyInput);
        }

        let sp_stmts = SpParser::parse_sql(&self.dialect, sql).map_err(parse_error_to_sql_error)?;

        if sp_stmts.is_empty() {
            return Err(SqlError::EmptyInput);
        }

        sp_stmts
            .into_iter()
            .map(|s| Statement::try_from(s).map_err(SqlError::AstError))
            .collect()
    }

    /// Parse a single SQL statement and return it.
    ///
    /// Convenience wrapper around [`parse`](SqlParser::parse) that returns
    /// an error if the input contains more than one statement.
    ///
    /// # Errors
    ///
    /// Returns a [`SqlError`] if the SQL is invalid, contains unsupported
    /// constructs, or contains more than one statement.
    pub fn parse_one(&self, sql: &str) -> Result<Statement, SqlError> {
        let mut stmts = self.parse(sql)?;
        if stmts.len() != 1 {
            return Err(SqlError::ParseError {
                message: format!("expected exactly one statement, got {}", stmts.len()),
                line: None,
                col: None,
            });
        }
        Ok(stmts.remove(0))
    }

    /// Parse a SQL expression (not a full statement).
    ///
    /// Useful for parsing `WHERE` conditions, computed column expressions,
    /// etc. in isolation.
    ///
    /// # Example
    ///
    /// ```rust
    /// use aeternumdb_core::sql::parser::SqlParser;
    /// use aeternumdb_core::sql::ast::Expr;
    ///
    /// let parser = SqlParser::new();
    /// let expr = parser.parse_expr("age > 18 AND name IS NOT NULL").unwrap();
    /// assert!(matches!(expr, Expr::BinaryOp { .. }));
    /// ```
    pub fn parse_expr(&self, expr_sql: &str) -> Result<crate::sql::ast::Expr, SqlError> {
        if expr_sql.trim().is_empty() {
            return Err(SqlError::EmptyInput);
        }

        // Wrap in a SELECT so we can reuse the full SQL parser.
        let wrapped = format!("SELECT {expr_sql}");
        let stmts = self.parse(&wrapped)?;
        match stmts.into_iter().next() {
            Some(Statement::Select(mut sel)) => {
                if sel.columns.len() == 1 {
                    use crate::sql::ast::SelectItem;
                    match sel.columns.remove(0) {
                        SelectItem::Expr { expr, .. } => Ok(expr),
                        SelectItem::Wildcard => Ok(crate::sql::ast::Expr::Wildcard),
                        SelectItem::QualifiedWildcard(t) => Err(SqlError::ParseError {
                            message: format!(
                                "qualified wildcard is not a scalar expression: {}.*",
                                t
                            ),
                            line: None,
                            col: None,
                        }),
                    }
                } else {
                    Err(SqlError::ParseError {
                        message: "expected a single expression".to_string(),
                        line: None,
                        col: None,
                    })
                }
            }
            _ => Err(SqlError::ParseError {
                message: "failed to parse expression".to_string(),
                line: None,
                col: None,
            }),
        }
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}
