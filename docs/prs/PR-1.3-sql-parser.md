# PR 1.3: SQL Parser Integration — AeternumDB Extended Dialect

> **Status**: ✅ **COMPLETE** — 107 tests pass, `cargo clippy` and `cargo fmt` clean.
> This file documents what was implemented.  Future SQL execution work lives in
> PRs 1.4 (planner), 1.5 (executor), and 1.6 (catalog).

## 📋 Overview

**PR Number:** 1.3
**Phase:** 1 - Core Foundation
**Priority:** 🔴 Critical
**Effort:** ~12 days (expanded scope)
**Dependencies:** None (independent)

---

## 🎯 What Was Built

The SQL module (`core/src/sql/`) wraps `sqlparser` 0.61 with an AeternumDB-specific
dialect, a rich internal AST, and a semantic validator backed by an in-memory catalog.

### Module Structure

| File | Purpose |
|------|---------|
| `core/src/sql/mod.rs` | Public re-exports |
| `core/src/sql/dialect.rs` | `AeternumDialect` (backtick identifiers + extensions) |
| `core/src/sql/ast.rs` | Internal AST + `TryFrom<sqlparser::ast::Statement>` |
| `core/src/sql/parser.rs` | `SqlParser` wrapper with `parse` / `parse_one` |
| `core/src/sql/validator.rs` | `Catalog` + `Validator` — semantic checks |
| `core/tests/sql_parser_tests.rs` | 107 comprehensive tests |
| `docs/sql-reference.md` | Full SQL reference (AeternumDB dialect) |

---

## ✅ Implemented Features

### Identifier Handling
- **Case-insensitive**: All identifiers normalized to lowercase during parsing.
  `SELECT UserId FROM MyTable` ≡ `select userid from mytable`.
- **Backtick quoting** (`` `name` ``) and double-quote quoting (`"name"`) for
  reserved-word identifiers.

### Multi-Database / Multi-Schema
- **Databases**: `CREATE DATABASE`, `DROP DATABASE`, `USE [DATABASE] name`.
  Cross-database joins are **not supported** — all tables in a query must
  belong to the same database.
- **Schemas**: `CREATE SCHEMA [db.]name`, `DROP SCHEMA`.  Default schema is
  `app`.  Reserved schemas: `sys`, `information_schema`, `pg_catalog`.
- **Three-part names**: `db.schema.table` fully qualified; `schema.table`;
  bare `table` (resolved to active DB + `app` schema).
- `TableReference::Named` carries `database: Option<String>` and
  `schema: Option<String>`.

### Data Types
#### Integer Types (all with UNSIGNED variants)
`TINYINT`, `SMALLINT`, `MEDIUMINT`, `INTEGER`/`INT`, `BIGINT`.

#### Floating-Point (signed only — no UNSIGNED)
`FLOAT`/`REAL`, `DOUBLE`/`DOUBLE PRECISION`.
`FLOAT UNSIGNED`, `DOUBLE UNSIGNED`, `DECIMAL UNSIGNED` → **parse error**
with suggestion to use a `CHECK` constraint.

#### Fixed-Precision
`DECIMAL(p,s)` / `NUMERIC(p,s)`.

#### Character
`CHAR(n)`, `VARCHAR(n)`, `TEXT`, `TINYTEXT`, `MEDIUMTEXT`, `LONGTEXT`.

#### Binary
`BINARY(n)`, `VARBINARY(n)`, `BLOB(n)`, `TINYBLOB`, `MEDIUMBLOB`, `LONGBLOB`.

#### Other
`BOOLEAN`/`BOOL`, `DATE`, `DATETIME`, `TIMESTAMP [WITH TIME ZONE]`,
`TIME [WITH TIME ZONE]`, `UUID`, `ENUM('a','b',...)`.

#### AeternumDB Extensions
- **Reference types**: `table_name` (single ref), `[table_name]` (array ref),
  `~table_name(col)` (virtual ref), `~[table_name](col)` (virtual array ref).
  Resolved via `objid` at execution time — no `FOREIGN KEY` constraints.
- **Vector types**: `ARRAY<T>` / `T[]` → `DataType::Vector(Box<DataType>)`.
- **FLAG Enum** (AST scaffolding): `DataType::Enum { variants: Vec<EnumVariant>, flag: bool }`.
  Each variant has `name`, `value: Option<u64>`, `is_none: bool`.
  Execution-layer bitmask semantics is a future phase.

### DML Statements
| Statement | Notes |
|-----------|-------|
| `SELECT` | DISTINCT, aliases, JOINs, subqueries, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, CTEs |
| `INSERT` | Single/multi-row VALUES |
| `UPDATE` | SET … WHERE |
| `DELETE` | WHERE |

### DDL Statements
| Statement | Notes |
|-----------|-------|
| `CREATE TABLE [schema.]name` | Schema/DB qualifier, columns, constraints, INHERITS, VERSIONED, FLAT flag (AST) |
| `DROP TABLE` | IF EXISTS |
| `ALTER TABLE` | ADD/DROP/RENAME COLUMN, RENAME TABLE |
| `CREATE DATABASE` | IF NOT EXISTS |
| `DROP DATABASE` | IF EXISTS |
| `USE [DATABASE] name` | |
| `CREATE SCHEMA` | IF NOT EXISTS, db-qualified |
| `DROP SCHEMA` | IF EXISTS |
| `CREATE INDEX` | UNIQUE, IF NOT EXISTS, table + columns |
| `DROP INDEX` | IF EXISTS |
| `CREATE MATERIALIZED VIEW` | IF NOT EXISTS, OR REPLACE |
| `CREATE USER` | WITH PASSWORD (scaffolding) |
| `DROP USER` | (scaffolding) |
| `CREATE TYPE … AS ENUM(…)` | (scaffolding) |

### Table Constraints (table-level)
- `PRIMARY KEY (col1, col2, ...)` — composite primary key.
- `UNIQUE (cols)`.
- `CHECK (expr)`.
- ~~`FOREIGN KEY`~~ → **rejected** with message to use reference column types.

### Column Extensions
- `DEFAULT expr` — default value.
- `CHECK (expr)` — column-level check constraint.
- **ON COMMIT** (for temporary tables): `PRESERVE ROWS`, `DELETE ROWS`, `DROP`.
- **Text Directive** — `ColumnDef::text_directive: Option<TextDirective>` —
  multilingual text with `default_locale`.
- **Terms Directives** — `ColumnDef::terms_directives: Vec<TermsDirective>` —
  named term sets with `kind` (TEXT, INTEGER, FLOAT, BOOLEAN, ENUM).

### FLAT Tables
`CreateTableStatement::flat: bool` marks a table as a FLAT (fast-read, no-join,
no-versioning, no-reference) table.  The `FLAT` keyword is set programmatically
from the AST; SQL-level parsing is a Phase 4 task.

### Versioned Tables
`CreateTableStatement::versioned: bool` — activates system versioning (temporal
row history).  `FOR SYSTEM_TIME AS OF` queries are a future execution phase.

### objid
Reserved system-assigned cluster-unique row identifier.  Not user-definable in
SQL.  Cluster-wide generation is a Phase 3 (Distribution) task.

### Joins — `filter_by` (replaces `ON`)
`TableReference::Join::filter_by: Option<Expr>` — optional predicate that
narrows the join result.  Standard SQL `ON` clauses are mapped to `filter_by`
for backwards compatibility.  The `FILTER BY` keyword is a Phase 4 SQL
extension.

### Path/Chain Joins (AST scaffolding)
`TableReference::Path { parts: Vec<String>, alias: Option<String> }` — a
multi-hop reference chain like `app.my_table.my_refs.their_col`.  The planner
(PR 1.4) will expand these into the equivalent sequence of joins.

### Expression: `Expr::Unnest`
`Expr::Unnest(Box<Expr>)` — unpivots / expands a vector column into individual
values.  Mapped from `UNNEST(col)` or `UNPIVOT(col)` function names.  Full
execution is a Phase 4 task.

### Expression: `Expr::Path`
`Expr::Path(Vec<String>)` — a reference chain used in SELECT expressions, e.g.
`my_table.my_refs.their_name`.  Planner resolves to the appropriate join chain.

### Bitwise Operators
`BinaryOperator::{BitwiseAnd, BitwiseOr, BitwiseXor, BitwiseShiftLeft, BitwiseShiftRight}`
and `UnaryOperator::BitwiseNot`.  Used primarily with FLAG enums and integer
columns.  Mapped from sqlparser operators `&`, `|`, `^`, `~`, `<<`, `>>`.

### DCL (scaffolding)
`GRANT SELECT [(col,...)] ON table TO role`,
`REVOKE … FROM role` — `GrantStatement::columns` / `RevokeStatement::columns`
carry column-level permission lists.  Enforcement is a Phase 6 task.

### Transaction Control
`BEGIN [TRANSACTION]`, `COMMIT [WORK]`, `ROLLBACK [WORK]`, `SAVEPOINT`,
`RELEASE SAVEPOINT` — parsed to AST; execution is PR 1.7.

### Semantic Validator (in-memory)
`Validator` + `Catalog` check:
- Table existence (case-insensitive).
- Column existence.
- Duplicate columns in CREATE TABLE.
- NOT NULL insert violations.
- Aggregate functions not allowed in WHERE clause.

---

## 🔍 Acceptance Criteria Met

- [x] All basic SQL statements parse correctly
- [x] AeternumDB extensions (reference types, FLAG enums, FLAT, versioned) in AST
- [x] Multi-database / multi-schema qualified names
- [x] FOREIGN KEY rejected with helpful message
- [x] All identifiers normalized to lowercase
- [x] Syntax errors with helpful messages
- [x] 107 tests pass
- [x] No clippy warnings; formatted with rustfmt

---

## 📚 Documentation

- `docs/sql-reference.md` — complete SQL reference for the AeternumDB dialect

---

## 🔗 What Comes Next

| Work Item | Target PR |
|-----------|-----------|
| Query planning for path joins, UNNEST, FLAT, filter_by | PR 1.4 |
| Execution of all statement types | PR 1.5 |
| Persistent catalog with multi-DB/schema, objid | PR 1.6 |
| FLAG enum bitmask storage, bitwise operator evaluation | PR 1.9 |
| `FILTER BY` keyword in SQL grammar | Phase 4 |
| `CREATE FLAT TABLE` keyword in SQL grammar | Phase 4 |
| `FOR SYSTEM_TIME AS OF` historical queries | Phase 5 |
| GRANT/REVOKE enforcement | Phase 6 |
| Cluster-wide objid generation | Phase 3 |
