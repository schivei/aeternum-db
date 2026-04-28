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
  `app`.  Reserved schemas: `sys`, `information_schema`, `adb_metadata`.
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
| `SELECT` | DISTINCT, aliases, `EXPAND ref_col [AS prefix]`, JOINs, subqueries, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, CTEs, `VIEW AS (expr AS alias, ...)` |
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

### `SelectItem::Expand` — Reference Column Expansion
`SelectItem::Expand { expr: Box<Expr>, alias: Option<String> }` — expands
**all columns** of the table referenced by a reference-typed column.  When the
column is a **vector reference** (multi-valued), the expansion also
auto-unnests the reference so each referenced row becomes its own result row.

Mapped from a function call `EXPAND(col)` or `EXPAND(col) AS alias` in the
SQL text (via `convert_select_item`).  The planner (PR 1.4) resolves the full
column list, rewrites alias-prefixed column references, and injects the unnest
step when needed.

#### Alias as namespace prefix
When `alias` is `Some(name)`, the alias is a **namespace prefix** for all
expanded columns.  The prefixed names (`alias.column`) are then usable in
`GROUP BY`, `HAVING`, and `VIEW AS`:

```sql
-- EXPAND(my_refs) AS mr  → columns mr.col_a, mr.col_b, …
SELECT u.name, EXPAND(u.order_ref) AS o FROM users u;
-- Columns produced: u.name, o.id, o.total, o.status

-- Access in GROUP BY / HAVING
GROUP BY o.status
HAVING COUNT(*) > 2

-- Access in VIEW AS
VIEW AS (
    UPPER(o.status) AS status_label,
    o.total * 1.2   AS total_with_tax
)
```

### `SelectStatement::view_as` — Result Transformation Clause
`SelectStatement::view_as: Option<Vec<ViewAsItem>>` — an optional
post-result projection applied after all filtering, grouping, ordering, and
limiting.  Each `ViewAsItem { expr, alias }` transforms an output column using
a **primitive expression**.

Semantic restrictions (enforced by the validator):
- `ValidationError::ViewAsAggregateNotAllowed(func_name)` — aggregate
  functions are not allowed.
- `ValidationError::ViewAsSubqueryNotAllowed` — sub-selects are not allowed.

```sql
SELECT id, score FROM users
VIEW AS (
    score * 100  AS pct_score,
    UPPER(name)  AS display_name
);
```

> **Parsing note**: `VIEW AS` is a Phase 4 custom-grammar extension.  The AST
> field (`view_as`) is available for programmatic construction; sqlparser
> lowering sets it to `None`.

### Bitwise Operators
`BinaryOperator::{BitwiseAnd, BitwiseOr, BitwiseXor, BitwiseShiftLeft, BitwiseShiftRight}`
and `UnaryOperator::BitwiseNot`.  Used primarily with FLAG enums and integer
columns.  Mapped from sqlparser operators `&`, `|`, `^`, `~`, `<<`, `>>`.

### Regex Operators
- `BinaryOperator::Regexp` / `NotRegexp` — case-sensitive POSIX regex match (`REGEXP`).
- `BinaryOperator::RegexpIMatch` / `NotRegexpIMatch` — case-insensitive regex
  (`REGEXP ~*`).
- `BinaryOperator::ILike` / `NotILike` — case-insensitive `LIKE` (`ILIKE`).
- `BinaryOperator::SimilarTo` / `NotSimilarTo` — SQL-standard regex (`SIMILAR TO`).

### REVLIKE — Reverse Pattern Matching
`BinaryOperator::RevLike` / `NotRevLike` — the **left operand is the pattern**
and the **right operand is the value** being tested.  Useful when patterns are
stored in columns.

```sql
-- pattern column on the left, tested against a fixed value on the right
SELECT rule_name FROM filter_rules WHERE pattern REVLIKE 'error_404';
```

### Array Quantifier Operators — `LIKE ANY` / `LIKE ALL` / `REVLIKE ANY`
`Expr::ArrayOp { expr, op, quantifier, right }` where `quantifier` is
`ArrayQuantifier::Any` or `ArrayQuantifier::All`.

```sql
expr  LIKE ANY  [pat1, pat2, ...]   -- matches at least one pattern
expr  LIKE ALL  [pat1, pat2, ...]   -- matches every pattern
'val' REVLIKE ANY vec_col           -- at least one stored pattern matches the value
```

### Full-Text Search — MATCH … AGAINST
`Expr::MatchAgainst { columns, match_value, modifier }` with modifiers:
- `TextSearchModifier::NaturalLanguage`
- `TextSearchModifier::Boolean`
- `TextSearchModifier::WithQueryExpansion`

Requires `FULLTEXT` or `TRIGRAM` index on the target columns.

### Text Functions
`Expr::Substring`, `Expr::Position`, `Expr::Trim`, `Expr::Replace`,
`Expr::Overlay` — all standard SQL text functions with optional regex support.

### String Concatenation
`BinaryOperator::StringConcat` (`||`) — SQL-standard string concatenation operator.

### DCL (scaffolding)
`GRANT SELECT [(col,...)] ON table TO role`,
`REVOKE … FROM role` — `GrantStatement::columns` / `RevokeStatement::columns`
carry column-level permission lists.  Enforcement is a Phase 6 task.

### Transaction Control — Named / Nested Transactions
`BEGIN [TRANSACTION [name]]`, `COMMIT [TRANSACTION [name] | ALL]`,
`ROLLBACK [TRANSACTION [name] | ALL | TO SAVEPOINT name]`,
`COMMIT AND CHAIN`, `ROLLBACK AND CHAIN`, `SAVEPOINT`, `RELEASE SAVEPOINT`
— all parsed to AST; execution is PR 1.7.

Nested transaction AST nodes:
- `BeginTransactionStatement { name: Option<String>, chain: bool, … }`
- `CommitStatement { scope: CommitScope, chain: bool }` where `CommitScope` is
  `CurrentTransaction | Named(String) | All`.
- `RollbackStatement { scope: RollbackScope, chain: bool }` where `RollbackScope`
  is `CurrentTransaction | Named(String) | All | ToSavepoint(String)`.

Semantic validator enforces LIFO nesting order at parse time:
- `TransactionNestingViolation` — attempt to close a transaction that has inner
  open transactions.
- `TransactionNameConflict` — reuse of an active transaction name.
- `TransactionNotFound` — commit/rollback of an unknown transaction name.

### CREATE ENUM / DROP ENUM
`CREATE ENUM [FLAG] [IF NOT EXISTS] name (variant, ...)` →
`CreateEnumStatement { name, variants: Vec<EnumVariant>, flag, if_not_exists }`.
`DROP ENUM [IF EXISTS] name` → `DropEnumStatement { name, if_exists }`.

Named enums are catalog objects and **cannot be dropped while any column
references them** — the validator returns `EnumInUse`.

### ON UPDATE / ON DELETE for Reference Columns
`ReferentialAction` enum: `Cascade | SetNull | SetDefault | Restrict | NoAction`.
`ColumnDef` carries `on_update: Option<ReferentialAction>` and
`on_delete: Option<ReferentialAction>`.

Actions are parsed from a column-level `REFERENCES table ON DELETE … ON UPDATE …`
clause.  Specifying referential actions on a non-reference column returns
`AstError::Invalid`.  Cascade execution is PR 1.5.


### Semantic Validator (in-memory)
`Validator` + `Catalog` check:
- Table existence (case-insensitive).
- Column existence.
- Duplicate columns in CREATE TABLE.
- NOT NULL insert violations.
- Aggregate functions not allowed in WHERE clause.
- `VIEW AS` restrictions: `ValidationError::ViewAsAggregateNotAllowed` and
  `ValidationError::ViewAsSubqueryNotAllowed` — ensures only primitive
  expressions are used in `VIEW AS` items.
- `EXPAND` items in SELECT list: the inner column expression is validated like
  any other column reference.

---

## 🔍 Acceptance Criteria Met

- [x] All basic SQL statements parse correctly
- [x] AeternumDB extensions (reference types, FLAG enums, FLAT, versioned) in AST
- [x] Multi-database / multi-schema qualified names
- [x] FOREIGN KEY rejected with helpful message
- [x] All identifiers normalized to lowercase
- [x] Syntax errors with helpful messages
- [x] Named / nested transactions with LIFO validator enforcement
- [x] CREATE ENUM / DROP ENUM with EnumInUse guard
- [x] ON UPDATE / ON DELETE on reference columns (`ReferentialAction`)
- [x] EXPAND operator with alias namespace prefix
- [x] VIEW AS clause with aggregate/subquery guard
- [x] 117 tests pass
- [x] No clippy warnings; formatted with rustfmt

---

## 📚 Documentation

- `docs/sql-reference.md` — complete SQL reference for the AeternumDB dialect

---

## 🔗 What Comes Next

| Work Item | Target PR |
|-----------|-----------|
| Query planning for path joins, UNNEST, FLAT, filter_by | PR 1.4 |
| EXPAND / VIEW AS resolution in query planner | PR 1.4 |
| Execution of all statement types | PR 1.5 |
| `ON UPDATE` / `ON DELETE` referential action enforcement | PR 1.5 |
| `ON UPDATE` / `ON DELETE` cascade execution | PR 1.5 |
| Persistent catalog with multi-DB/schema, objid | PR 1.6 |
| `CREATE ENUM` / `DROP ENUM` catalog storage | PR 1.6 |
| `EnumRef` type resolution at runtime | PR 1.6 |
| FLAG enum bitmask storage, bitwise operator evaluation | PR 1.9 |
| `FILTER BY` keyword in SQL grammar | Phase 4 |
| `CREATE FLAT TABLE` keyword in SQL grammar | Phase 4 |
| `FOR SYSTEM_TIME AS OF` historical queries | Phase 5 |
| GRANT/REVOKE enforcement | Phase 6 |
| Cluster-wide objid generation | Phase 3 |
