//! SQL parsing and validation subsystem for AeternumDB.
//!
//! This module integrates the [`sqlparser`] crate and provides:
//!
//! - [`parser::SqlParser`] — the main entry point for parsing SQL strings.
//! - [`ast`] — the internal AST types that the query planner consumes.
//! - [`validator`] — semantic validation against an in-memory schema catalog.
//! - [`dialect`] — the AeternumDB SQL dialect definition.
//!
//! # Quick start
//!
//! ```rust
//! use aeternumdb_core::sql::parser::SqlParser;
//! use aeternumdb_core::sql::ast::Statement;
//!
//! let parser = SqlParser::new();
//!
//! // Parse a SELECT statement
//! let stmts = parser.parse("SELECT id, name FROM users WHERE age > 18 LIMIT 10").unwrap();
//! assert!(matches!(stmts[0], Statement::Select(_)));
//!
//! // Parse a CREATE TABLE statement
//! let stmts = parser.parse(
//!     "CREATE TABLE products (
//!         id INTEGER PRIMARY KEY,
//!         name VARCHAR(255) NOT NULL,
//!         price DECIMAL(10, 2)
//!     )"
//! ).unwrap();
//! assert!(matches!(stmts[0], Statement::CreateTable(_)));
//! ```
//!
//! # With semantic validation
//!
//! ```rust
//! use aeternumdb_core::sql::parser::SqlParser;
//! use aeternumdb_core::sql::ast::DataType;
//! use aeternumdb_core::sql::validator::{Catalog, ColumnSchema, TableSchema, Validator};
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
//! validator.validate(&stmt).unwrap();
//! ```

pub mod ast;
pub mod dialect;
pub mod parser;
pub mod validator;

// Convenience re-exports of the most frequently used types.
pub use ast::Statement;
pub use parser::{SqlError, SqlParser};
pub use validator::{Catalog, ValidationError, Validator};
