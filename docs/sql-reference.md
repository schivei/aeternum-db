# AeternumDB SQL Reference

This document describes the SQL dialect supported by AeternumDB.  The
implementation is based on **SQL-92** with a subset of common SQL extensions.

---

## Table of Contents

1. [Data Types](#data-types)
2. [Identifier Quoting](#identifier-quoting)
3. [Case Insensitivity](#case-insensitivity)
4. [Databases and Schemas](#databases-and-schemas)
5. [System Identifier (objid)](#system-identifier-objid)
6. [Operators](#operators)
   - [Arithmetic](#arithmetic)
   - [Comparison](#comparison)
   - [Logical](#logical)
   - [Bitwise](#bitwise)
   - [String / Pattern](#string--pattern)
   - [Regex Operators](#regex-operators)
   - [REVLIKE — Reverse Pattern Matching](#revlike--reverse-pattern-matching)
   - [Array Quantifier Operators](#array-quantifier-operators--like-any--like-all--revlike-any)
   - [Other](#other)
7. [Functions](#functions)
8. [Statements](#statements)
   - [SELECT](#select)
      - [JOIN](#join)
      - [Subqueries](#subqueries)
      - [EXPAND](#expand)
      - [VIEW AS](#view-as)
   - [INSERT](#insert)
   - [UPDATE](#update)
   - [DELETE](#delete)
   - [CREATE DATABASE](#create-database)
   - [DROP DATABASE](#drop-database)
   - [USE DATABASE](#use-database)
   - [CREATE SCHEMA](#create-schema)
   - [DROP SCHEMA](#drop-schema)
   - [CREATE TABLE](#create-table)
   - [CREATE FLAT TABLE](#create-flat-table)
   - [CREATE TEMPORARY TABLE](#create-temporary-table)
   - [CREATE MATERIALIZED VIEW](#create-materialized-view)
   - [DROP TABLE](#drop-table)
   - [ALTER TABLE](#alter-table)
   - [CREATE INDEX](#create-index)
   - [DROP INDEX](#drop-index)
   - [CREATE USER](#create-user)
   - [DROP USER](#drop-user)
   - [CREATE TYPE](#create-type)
   - [GRANT / REVOKE](#grant--revoke)
9. [Transaction Control](#transaction-control)
10. [Column Extensions](#column-extensions)
    - [Text Directive (Multilingual)](#text-directive-multilingual)
    - [Terms Directives](#terms-directives)
    - [Vector Columns](#vector-columns)
    - [Reference Column Types](#reference-column-types)
    - [Versioned / Temporal Tables](#versioned--temporal-tables)
11. [Joins and FILTER BY](#joins-and-filter-by)
12. [Expressions](#expressions)
13. [Limitations and Future Enhancements](#limitations-and-future-enhancements)

---

## Data Types

### Integer Types

| SQL Type                        | Notes                                    |
|---------------------------------|------------------------------------------|
| `TINYINT`                       | 8-bit signed integer (−128 … 127)       |
| `TINYINT UNSIGNED`              | 8-bit unsigned integer (0 … 255)        |
| `SMALLINT`                      | 16-bit signed integer                    |
| `SMALLINT UNSIGNED`             | 16-bit unsigned integer                  |
| `MEDIUMINT`                     | 24-bit signed integer (MySQL)            |
| `MEDIUMINT UNSIGNED`            | 24-bit unsigned integer (MySQL)          |
| `INTEGER` / `INT`               | 32-bit signed integer                    |
| `INTEGER UNSIGNED` / `INT UNSIGNED` | 32-bit unsigned integer              |
| `BIGINT`                        | 64-bit signed integer                    |
| `BIGINT UNSIGNED`               | 64-bit unsigned integer                  |

### Floating-Point Types

| SQL Type                          | Notes                      |
|-----------------------------------|----------------------------|
| `FLOAT` / `REAL`                  | 32-bit signed IEEE 754     |
| `DOUBLE` / `DOUBLE PRECISION`     | 64-bit signed IEEE 754     |

> **Note**: `FLOAT UNSIGNED` and `DOUBLE UNSIGNED` are **not supported**.
> IEEE 754 floating-point types are always signed.  Use a `CHECK (col >= 0)`
> constraint if you need to restrict values to non-negative numbers.

### Fixed-Precision Types

| SQL Type                             | Notes                                              |
|--------------------------------------|----------------------------------------------------|
| `DECIMAL(p, s)` / `NUMERIC(p, s)`   | Fixed-point with precision `p` and scale `s`       |

> **Note**: `DECIMAL UNSIGNED` is **not supported**.  Use a `CHECK (col >= 0)`
> table constraint to enforce non-negative decimal values.

### Character Types

| SQL Type              | Notes                                  |
|-----------------------|----------------------------------------|
| `CHAR(n)`             | Fixed-length string, padded to `n`     |
| `VARCHAR(n)`          | Variable-length string, max `n` chars  |
| `TEXT`                | Unbounded string                       |
| `TINYTEXT`            | Small text (max 255 chars)             |
| `MEDIUMTEXT`          | Medium text (max 16 MB)                |
| `LONGTEXT`            | Large text (max 4 GB)                  |

### Binary Types

| SQL Type              | Notes                                              |
|-----------------------|----------------------------------------------------|
| `BINARY(n)`           | Fixed-length binary string (`n` bytes)             |
| `VARBINARY(n)`        | Variable-length binary string (max `n` bytes)      |
| `BLOB` / `BLOB(n)`    | Binary large object                                |
| `TINYBLOB`            | Small binary object (max 255 bytes, MySQL)         |
| `MEDIUMBLOB`          | Medium binary object (max 16 MB, MySQL)            |
| `LONGBLOB`            | Large binary object (max 4 GB, MySQL)              |

### Date / Time Types

| SQL Type                          | Notes                              |
|-----------------------------------|------------------------------------|
| `DATE`                            | Calendar date without time         |
| `TIME`                            | Time of day without date           |
| `TIME WITH TIME ZONE`             | Time of day with timezone offset   |
| `DATETIME`                        | Date and time (no timezone)        |
| `TIMESTAMP`                       | Date + time (no timezone)          |
| `TIMESTAMP WITH TIME ZONE`        | Date + time with timezone offset   |

### Special Types

| SQL Type              | Notes                                  |
|-----------------------|----------------------------------------|
| `BOOLEAN` / `BOOL`    | `TRUE` / `FALSE`                       |
| `UUID` / `GUID`       | 128-bit universally-unique identifier  |
| `ENUM('a', 'b', ...)` | Enumerated string values               |

### Vector Types

Any base type can be wrapped in a **vector** — an ordered, variable-length
sequence of values of that element type.  Vector columns support specialised
insert, update, delete, and select operations (execution-layer; future phase).

| SQL Syntax       | Meaning                                   |
|------------------|-------------------------------------------|
| `[INTEGER]`      | Vector of 32-bit integers                 |
| `[VARCHAR(100)]` | Vector of variable-length strings         |
| `[DECIMAL(10,2)]`| Vector of fixed-point decimals            |
| `[table_name]`   | Vector / array of row references (OO)     |

```sql
CREATE TABLE product_tags (
  id       INTEGER PRIMARY KEY AUTO_INCREMENT,
  tags     [VARCHAR(50)],   -- vector of tag strings
  scores   [DECIMAL(5,2)]   -- vector of rating scores
);
```

### Reference Types (OO / Graph Paradigm)

AeternumDB extends the SQL type system with reference types for object-oriented
and graph-style data modelling. These are **future parsing/execution
scaffolding** — the syntax below is documented for planned support, but it is
not yet fully lowered into dedicated internal reference-type variants. Storage,
semantic enforcement, and query execution will be implemented in later phases.

| SQL Syntax                  | Internal type         | Meaning                                      |
|-----------------------------|-----------------------|----------------------------------------------|
| `table_name`                | `Reference`           | Single-row reference (foreign key)           |
| `[table_name]`              | `ReferenceArray`      | Array of references (one-to-many)            |
| `~table_name(column)`       | `VirtualReference`    | Computed inverse reference (read-only)       |
| `~[table_name](column)`     | `VirtualReferenceArray` | Computed inverse array (read-only)         |

**Constraints for reference columns:**

| Constraint      | Applies to         | Description                            |
|-----------------|--------------------|----------------------------------------|
| `MIN_LENGTH n`  | array references   | Minimum number of references required  |
| `MAX_LENGTH n`  | array references   | Maximum number of references allowed   |
| `UNIQUES`       | array references   | All referenced rows must be distinct   |
| `AUTO_INCREMENT`| integer / PK       | Auto-generate next integer value       |

**Example — bidirectional object references:**

```sql
CREATE TABLE sample1 (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  has   ~sample2(belongs_to)  NULL,         -- virtual inverse reference
  slaves ~[sample2](master)   MIN_LENGTH 1  -- virtual inverse array
);

CREATE TABLE sample2 (
  id         INTEGER PRIMARY KEY AUTO_INCREMENT,
  `belongs_to` [sample1]   MIN_LENGTH 0 UNIQUES, -- reference array
  `master`     sample1     NOT NULL,              -- direct reference
  `parent`     sample2     NULL,                  -- self-reference
  `children`  ~[sample2](parent) MIN_LENGTH 0     -- virtual inverse array
);
```

---

## System Identifier (objid)

Every row stored in AeternumDB automatically receives a system-assigned
**`objid`** — a cluster-wide unique identifier that:

- Is unique across all **nodes**, **databases**, **schemas**, and **tables**.
- Can be used in queries exactly like a user-defined primary key.
- Is **read-only from SQL** — users cannot `INSERT` or `UPDATE` it.
  (Backup restoration is the only exception.)
- Survives `UPDATE` operations — the row keeps its `objid` when columns change.

### Versioned / Temporal Rows

When a table is created with `WITH SYSTEM VERSIONING`, every mutation creates a
new *version* of the row.  All versions share the same `objid`; the version is
distinguished by implicit `valid_from` / `valid_to` system columns.

```sql
-- Query the current state (default)
SELECT * FROM orders WHERE objid = '01920e3b-…';

-- Query a historical snapshot (future-phase feature)
SELECT * FROM orders FOR SYSTEM_TIME AS OF '2025-01-01'
WHERE objid = '01920e3b-…';
```

> **Execution status**: `objid` generation and temporal row storage are
> execution-layer features planned for a future phase.  The parser and AST
> already reserve `objid` as a system column identifier.

---

## Identifier Quoting

Identifiers (table names, column names) that clash with SQL keywords can be
quoted using:

- **Backticks** (`` `name` ``) — MySQL-compatible style, preferred:
  ```sql
  SELECT `select`, `from` FROM `table`;
  ```
- **Double-quotes** (`"name"`) — SQL-92 standard style:
  ```sql
  SELECT "select", "from" FROM "table";
  ```

Unquoted identifiers are matched case-insensitively (for example, `users`,
`Users`, and `USERS` refer to the same table or column). Use quoting when you
need to write an identifier exactly as defined or when it would otherwise
conflict with a SQL keyword.

---

## Case Insensitivity

**All SQL keywords and identifiers are case-insensitive in AeternumDB.**

```sql
-- All of the following are equivalent:
SELECT id FROM users;
select id from users;
SELECT ID FROM USERS;
Select Id From Users;
```

Identifiers (table names, column names, schema names, aliases) are **normalized
to lowercase** during parsing.  This means that whether you write `Users`,
`USERS`, or `users`, the engine stores and looks up the name as `users`.
String *literals* (`'Hello World'`) preserve their original casing.

> **Tip**: For maximum clarity and portability, use lowercase identifiers and
> uppercase SQL keywords in your SQL source.

---

## Databases and Schemas

AeternumDB supports a two-level namespace below the connection: **databases**
contain **schemas**, and schemas contain **tables**.

### Multi-Database Architecture

A running AeternumDB cluster can host multiple databases.  Each database is an
independent unit of storage and access control.

- **Cross-database joins are not supported.**  All tables referenced in a
  single query must belong to the same database.
- The active database for a connection is set with `USE [DATABASE] name`.
- Fully qualified names have the form `database.schema.table`.

### Schemas

A schema is a namespace that groups related tables within a database.
Cross-schema joins **are** supported within the same database.

| Schema name         | Purpose                                         |
|---------------------|-------------------------------------------------|
| `app`               | **Default application schema** (used when no schema is specified) |
| `sys`               | System metadata and internal catalog tables (reserved) |
| `information_schema`| SQL-standard information schema views (reserved) |
| `adb_metadata`      | AeternumDB internal metadata and compatibility views (reserved) |

> **Note**: Reserved schemas (`sys`, `information_schema`, `adb_metadata`) cannot
> be created or dropped by users.  Enforcement is a future execution-layer task.

### Three-Part Identifier Syntax

```sql
-- Unqualified: uses the active database and default schema (app)
SELECT * FROM orders;

-- Schema-qualified: explicit schema in the active database
SELECT * FROM reporting.sales;

-- Fully qualified: explicit database, schema, and table
SELECT * FROM myapp.reporting.sales;
```

### CREATE DATABASE

```sql
CREATE DATABASE [IF NOT EXISTS] database_name;
```

Creates a new database.  Reserved database names are `sys` and `aeternumdb`.

### DROP DATABASE

```sql
DROP DATABASE [IF EXISTS] database_name;
```

Drops a database and all objects it contains.

### USE DATABASE

```sql
USE [DATABASE] database_name;
USE database_name;
```

Switches the active database for the current connection.  Cross-database joins
are not allowed; all tables in a query must reside in the active database.

### CREATE SCHEMA

```sql
CREATE SCHEMA [IF NOT EXISTS] [database_name.]schema_name;
```

Creates a new schema in the specified (or active) database.  Reserved schema
names (`sys`, `information_schema`, `adb_metadata`) cannot be used.

### DROP SCHEMA

```sql
DROP SCHEMA [IF EXISTS] [database_name.]schema_name;
```

Drops a schema and all tables it contains.

---

## Operators

### Arithmetic

| Operator | Description    |
|----------|----------------|
| `+`      | Addition       |
| `-`      | Subtraction    |
| `*`      | Multiplication |
| `/`      | Division       |
| `%`      | Modulo         |

### Comparison

| Operator   | Description             |
|------------|-------------------------|
| `=`        | Equal                   |
| `!=`, `<>` | Not equal               |
| `<`        | Less than               |
| `<=`       | Less than or equal      |
| `>`        | Greater than            |
| `>=`       | Greater than or equal   |

### Logical

| Operator | Description  |
|----------|--------------|
| `AND`    | Logical AND  |
| `OR`     | Logical OR   |
| `NOT`    | Logical NOT  |

### Bitwise

| Operator | Description                    |
|----------|--------------------------------|
| `&`      | Bitwise AND                    |
| `\|`     | Bitwise OR                     |
| `^`      | Bitwise XOR                    |
| `~expr`  | Bitwise NOT (unary)            |
| `<<`     | Bit-shift left                 |
| `>>`     | Bit-shift right                |

Bitwise operators are especially useful for filtering FLAG enum columns:

```sql
-- Rows where the Permission column has the Read bit (1) set
SELECT name FROM roles WHERE permissions & 1 = 1;

-- Rows with Read (1) OR Write (2) bits set
SELECT name FROM roles WHERE permissions & 3 != 0;
```

### String / Pattern

| Operator          | Description                                                |
|-------------------|------------------------------------------------------------|
| `\|\|`            | String concatenation                                       |
| `LIKE pat`        | Wildcard pattern match (`%` = any chars, `_` = one char)  |
| `NOT LIKE pat`    | Negated wildcard match                                     |
| `ILIKE pat`       | Case-insensitive `LIKE`                                    |
| `NOT ILIKE pat`   | Negated case-insensitive `LIKE`                            |
| `SIMILAR TO pat`  | SQL-standard regex pattern match                           |
| `NOT SIMILAR TO`  | Negated `SIMILAR TO`                                       |

### Regex Operators

AeternumDB supports POSIX-style regular expression operators:

| Operator             | Description                                         |
|----------------------|-----------------------------------------------------|
| `REGEXP pat`         | Case-sensitive regex match                          |
| `NOT REGEXP pat`     | Negated case-sensitive regex match                  |
| `REGEXP ~* pat`      | Case-insensitive regex match                        |
| `NOT REGEXP ~* pat`  | Negated case-insensitive regex match                |

```sql
-- Rows where email matches a regex
SELECT name FROM users WHERE email REGEXP '^[a-z]+@example\\.com$';

-- Case-insensitive regex
SELECT name FROM users WHERE name REGEXP ~* '^alice';
```

### REVLIKE — Reverse Pattern Matching

`REVLIKE` is the mirror of `LIKE`: the **left side is the pattern** and the
**right side is the value being tested**. This is useful when the pattern
itself is stored in a column.

| Operator            | Description                                              |
|---------------------|----------------------------------------------------------|
| `pat REVLIKE col`   | Pattern (left) matches value in column (right)           |
| `pat NOT REVLIKE col` | Negated reverse LIKE                                   |
| `pat REVILIKE col`  | Case-insensitive reverse LIKE                            |
| `pat REVREGEXP col` | Pattern (left) is a regex, matched against column value  |

```sql
-- Check whether a stored pattern column matches a fixed string
SELECT rule_name FROM filter_rules WHERE pattern REVLIKE 'hello world';

-- The rule table stores LIKE-patterns; test a given value against all of them
SELECT rule_name FROM filter_rules WHERE '%hello%' REVLIKE value_col;
```

### Array Quantifier Operators — LIKE ANY / LIKE ALL / REVLIKE ANY

These operators let you test a value against a list or vector column using
any pattern operator (`LIKE`, `ILIKE`, `REGEXP`, `REVLIKE`, …).

| Syntax                           | True when…                                          |
|----------------------------------|-----------------------------------------------------|
| `col LIKE ANY [pat1, pat2, …]`   | Column matches **at least one** pattern in the list |
| `col LIKE ALL [pat1, pat2, …]`   | Column matches **every** pattern in the list        |
| `'val' REVLIKE ANY vec_col`      | At least one pattern in vector column matches value |
| `'val' REVLIKE ALL vec_col`      | Every pattern in vector column matches the value    |

```sql
-- Rows where name matches any of the supplied patterns
SELECT id FROM users WHERE name LIKE ANY ['%alice%', '%bob%'];

-- Rows where description satisfies all keyword patterns
SELECT id FROM docs WHERE body LIKE ALL ['%security%', '%audit%'];

-- Rows where at least one stored pattern matches a fixed string
SELECT rule_id FROM rules WHERE 'error_404' REVLIKE ANY pattern_col;
```

### Other

| Operator / Syntax             | Description                             |
|-------------------------------|-----------------------------------------|
| `IS NULL`                     | Test for NULL                           |
| `IS NOT NULL`                 | Test for non-NULL                       |
| `BETWEEN low AND high`        | Range test (inclusive)                  |
| `NOT BETWEEN low AND high`    | Negated range test                      |
| `IN (v1, v2, ...)`            | Membership test                         |
| `NOT IN (v1, v2, ...)`        | Negated membership                      |
| `IN (subquery)`               | Subquery membership test                |
| `CAST(expr AS type)`          | Type conversion                         |

---

## Functions

### Aggregate Functions

Aggregate functions operate on a set of rows and return a single value.
They are valid in the `SELECT` list and `HAVING` clause, but **not** in
`WHERE`.

| Function               | Description                              |
|------------------------|------------------------------------------|
| `COUNT(*)`             | Count all rows                           |
| `COUNT(expr)`          | Count non-NULL values of `expr`          |
| `COUNT(DISTINCT expr)` | Count distinct non-NULL values           |
| `SUM(expr)`            | Sum of non-NULL numeric values           |
| `AVG(expr)`            | Average of non-NULL numeric values       |
| `MIN(expr)`            | Minimum value                            |
| `MAX(expr)`            | Maximum value                            |

---

## Statements

### SELECT

```sql
[WITH cte_name [(column, ...)] AS (subquery), ...]
SELECT [DISTINCT]
    { * | column [AS alias] | expr [AS alias] | EXPAND ref_col [AS prefix] }, ...
FROM table_reference
[WHERE condition]
[GROUP BY expr, ...]
[HAVING condition]
[ORDER BY expr [ASC | DESC], ...]
[LIMIT n [OFFSET m]]
[VIEW AS (
    expr AS alias [, expr AS alias ...]
)]
```

**Common Table Expressions (WITH clause):**

WITH clauses allow you to define named temporary result sets (CTEs) that can be
referenced in the main query:

```sql
WITH regional_sales AS (
  SELECT region, SUM(amount) AS total_sales
  FROM orders
  GROUP BY region
)
SELECT * FROM regional_sales WHERE total_sales > 1000;
```

**Examples:**

```sql
-- Simple column selection
SELECT id, name FROM users;

-- Wildcard
SELECT * FROM users;

-- Filtering
SELECT id, name FROM users WHERE age > 18;

-- Ordering and limiting
SELECT id, name FROM users ORDER BY name ASC LIMIT 10 OFFSET 20;

-- Aggregation
SELECT age, COUNT(*) AS cnt FROM users GROUP BY age HAVING COUNT(*) > 1;

-- Aliasing
SELECT id AS user_id, name AS full_name FROM users;

-- DISTINCT
SELECT DISTINCT age FROM users;
```

#### JOIN

AeternumDB joins are **reference-driven**, not condition-driven.  Instead of
listing a second table in a `JOIN` clause, you navigate through a **reference
column** that already encodes the relationship at schema-definition time.
Rows are linked via `objid` — no explicit `ON` clause is required or supported.

> **The only supported join form is chain navigation starting from the `FROM`
> table.**  Direct `INNER JOIN other_table` syntax is not the intended usage;
> all traversal starts from the root `FROM` table and walks reference columns.

**Chain / path navigation syntax:**

```
schema.table.ref_column.target_column
```

```sql
-- Navigate from users → through the order_ref reference column → to total
SELECT u.name, u.order_ref.total
FROM users u;

-- Deep chain: user → order → line_item → product name
SELECT u.name, u.order_ref.line_ref.product_name
FROM app.users u;

-- Schema-qualified chain root
SELECT u.name, u.order_ref.total
FROM app.users u;
```

**Optional `FILTER BY` predicate** further narrows the traversal result
(analogous to SQL's `ON` but semantically distinct — it is a post-join filter
on the already-resolved reference):

```sql
-- Traverse order_ref but only keep rows where total > 100
SELECT u.name, u.order_ref.total
FROM users u
FILTER BY u.order_ref.total > 100;

-- Deep chain with filter
SELECT u.name, u.order_ref.line_ref.qty
FROM app.users u
FILTER BY u.order_ref.status = 'shipped';
```

> **Cross-database joins are not supported.**  All tables in a query must
> belong to the same database.  Cross-schema joins within the same database
> are fully supported.

> **Note**: `NATURAL JOIN`, `USING`, and direct `INNER JOIN table` syntax are
> not the AeternumDB model.  `FLAT` tables cannot participate in joins.

#### Subqueries

Subqueries can appear:
- In the `FROM` clause (derived tables, must be aliased):
  ```sql
  SELECT sub.id FROM (SELECT id FROM users WHERE age > 18) AS sub;
  ```
- In a `WHERE IN (subquery)`:
  ```sql
  SELECT id FROM users WHERE id IN (SELECT user_id FROM orders);
  ```
- As a scalar subquery in expressions:
  ```sql
  SELECT id, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) AS cnt
  FROM users;
  ```

#### EXPAND

`EXPAND(ref_col) [AS alias]` extracts **all columns** from the table referenced
by a reference-typed column into the result set as individual top-level columns.
If the reference is a **vector** (multi-valued), `EXPAND` also automatically
**unnests** the reference so each referenced row becomes its own result row.

`EXPAND` may only appear in the `SELECT` list.

##### Alias as a namespace prefix

When an alias is attached to `EXPAND`, it becomes a **namespace prefix** for
every expanded column.  The prefixed names are then accessible by that dotted
path in `GROUP BY`, `HAVING`, and `VIEW AS` clauses:

```sql
-- EXPAND(my_refs) AS mr  →  mr.col_a, mr.col_b, …
SELECT u.name, EXPAND(u.order_ref) AS o
FROM users u;
-- Expanded columns: o.id, o.total, o.status

-- GROUP BY using the expanded namespace
SELECT u.name, EXPAND(u.order_ref) AS o
FROM users u
GROUP BY o.status;

-- HAVING using the expanded namespace
SELECT u.name, EXPAND(u.order_ref) AS o
FROM users u
GROUP BY o.status
HAVING COUNT(*) > 2;

-- VIEW AS referencing the expanded namespace
SELECT u.name, EXPAND(u.order_ref) AS o
FROM users u
VIEW AS (
    UPPER(o.status)  AS status_label,
    o.total * 1.2    AS total_with_tax
);
```

Without an alias the expanded columns carry their original names (unqualified),
but an alias is **strongly recommended** when multiple `EXPAND` items are
present in the same query to avoid name collisions.

```sql
-- Without alias — original column names exposed directly
SELECT u.name, EXPAND(u.order_ref)
FROM users u;
-- Columns: u.name, id, total, status  (could collide with other columns)

-- With alias — safe, unambiguous
SELECT u.name, EXPAND(u.order_ref) AS o
FROM users u;
-- Columns: u.name, o.id, o.total, o.status

-- Vector reference (multi-valued): each referenced row becomes its own result row
SELECT u.name, EXPAND(u.tag_refs) AS t
FROM users u;
-- tag_refs is a [TagRef] vector; EXPAND auto-unnests it.
-- Columns: u.name, t.id, t.label, t.color  (one row per tag)
```

> **Execution note**: `EXPAND` is resolved by the query planner (PR 1.4).  The
> planner looks up the target table schema via the reference column type,
> rewrites the alias prefix into qualified column references, and injects an
> implicit `UNNEST` step when the cardinality is > 1.

#### VIEW AS

`VIEW AS (expr AS alias, ...)` is a **post-result transformation clause**
applied after all filtering, grouping, ordering, and limiting.  Each item
projects a new column by applying a primitive expression to the output row.

**Restrictions** (enforced by the semantic validator):
- **No aggregate functions** (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, …).
- **No sub-selects** (scalar or `IN (subquery)` forms).
- Only primitive expressions: arithmetic, comparisons, `CASE`, `CAST`,
  string functions (`UPPER`, `LOWER`, `SUBSTRING`, `TRIM`, `REPLACE`, …),
  and column references.

**Referencing `EXPAND` columns**: when the `SELECT` list contains
`EXPAND(ref_col) AS alias`, the expanded columns are available in `VIEW AS`
via the `alias.column_name` dotted path.

```sql
-- Scale score to percentage and display name in upper-case
SELECT id, name, score FROM users
VIEW AS (
    score * 100  AS pct_score,
    UPPER(name)  AS display_name
);

-- Combine multiple columns with a separator
SELECT first_name, last_name, birth_year FROM people
VIEW AS (
    first_name || ' ' || last_name AS full_name,
    2025 - birth_year              AS age
);

-- Conditional transformation
SELECT id, status_code FROM orders
VIEW AS (
    CASE status_code
        WHEN 1 THEN 'pending'
        WHEN 2 THEN 'shipped'
        ELSE       'unknown'
    END AS status_label
);

-- Combining EXPAND alias with VIEW AS
-- EXPAND(u.order_ref) AS o  →  o.id, o.total, o.status
SELECT u.name, EXPAND(u.order_ref) AS o
FROM users u
VIEW AS (
    UPPER(o.status)  AS status_label,
    o.total * 1.2    AS total_with_tax
);
```

> **Parsing note**: `VIEW AS` is an AeternumDB-specific extension.  Parsing
> from raw SQL strings is a Phase 4 custom-grammar task.  The AST scaffolding
> (`SelectStatement::view_as`) is available now for programmatic construction
> and testing.

---

### INSERT

```sql
INSERT INTO table_name [(column1, column2, ...)]
VALUES (value1, value2, ...) [, (value1, value2, ...) ...]
```

**Examples:**

```sql
-- Single row
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);

-- Multiple rows
INSERT INTO users (id, name, age)
VALUES (2, 'Bob', 25), (3, 'Carol', 35);
```

---

### UPDATE

```sql
UPDATE table_name
SET column1 = expr1 [, column2 = expr2 ...]
[WHERE condition]
```

**Example:**

```sql
UPDATE users SET name = 'Dave', age = 40 WHERE id = 1;
```

---

### DELETE

```sql
DELETE FROM table_name [WHERE condition]
```

**Examples:**

```sql
-- Delete with condition
DELETE FROM users WHERE age < 18;

-- Delete all rows
DELETE FROM users;
```

---

### CREATE TABLE

```sql
CREATE [IF NOT EXISTS] TABLE [[database.]schema.]table_name (
    column_name data_type [NOT NULL | NULL] [PRIMARY KEY] [UNIQUE]
                          [DEFAULT expr] [AUTO_INCREMENT]
                          [MIN_LENGTH n] [MAX_LENGTH n] [UNIQUES],
    ...
)
[INHERITS (parent_table [, ...])]
[VERSIONED]
```

Schema and database qualifiers default to `app` and the active database
respectively when omitted.

**Full-featured example with reference types and inheritance:**

```sql
CREATE TABLE person (
  id      INTEGER     PRIMARY KEY AUTO_INCREMENT,
  name    VARCHAR(100) NOT NULL,
  email   VARCHAR(255) NOT NULL,
  dob     DATE
);

CREATE TABLE employee (
  salary  DECIMAL(10,2) NOT NULL,
  dept    VARCHAR(50)
) INHERITS (person);
```

**Example with reference types (instead of FOREIGN KEY):**

```sql
CREATE TABLE sample2 (
  id         INTEGER PRIMARY KEY AUTO_INCREMENT,
  `master`   sample1  NOT NULL,
  `parent`   sample2  NULL,
  `children` ~[sample2](parent) MIN_LENGTH 0
);
```

---

### CREATE FLAT TABLE

A **FLAT** table is a write-optimised, schema-simple table intended for
high-throughput sequential reads (event logs, metrics, audit trails, etc.).

```sql
CREATE [IF NOT EXISTS] FLAT TABLE [[database.]schema.]table_name (
    column_name data_type [NOT NULL | NULL] [DEFAULT expr],
    ...
);
```

**Restrictions** — a FLAT table:

- Does **not** support JOINs or the `FILTER BY` clause.
- Does **not** support reference column types (`table_name`, `[table_name]`,
  `~table_name(col)`).
- Does **not** support `VERSIONED` / temporal row history.
- Does **not** support `INHERITS`.
- Does **not** carry an `objid` — rows are addressed by physical position only.

> **Note**: The `FLAT` keyword is an AeternumDB extension.  The current parser
> represents the flag as `CreateTableStatement::flat: bool`; the `FLAT`
> keyword is not yet parsed directly from standard SQL — it is set
> programmatically at the execution layer.  Full SQL syntax support is planned
> for a future phase.

**Example:**

```sql
CREATE FLAT TABLE metrics (
  recorded_at TIMESTAMP NOT NULL,
  sensor_id   INTEGER   NOT NULL,
  value       FLOAT     NOT NULL
);
```

---

### CREATE TEMPORARY TABLE

```sql
CREATE [TEMPORARY | TEMP] TABLE table_name (
    column_name data_type [column_options],
    ...
)
[ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP }]
```

#### Temporary Table Lifecycle

Temporary tables are automatically cleaned up by the execution engine (future
implementation):

| When                                      | What happens                                      |
|-------------------------------------------|---------------------------------------------------|
| `ON COMMIT PRESERVE ROWS` (default)       | Rows persist across commits; table dropped at **session end** |
| `ON COMMIT DELETE ROWS`                   | Rows truncated on each `COMMIT`; table persists for the session |
| `ON COMMIT DROP`                          | Table **dropped on `COMMIT`** (or `ROLLBACK`)     |
| Session / connection closed (no explicit `DROP`) | Table and all its rows are automatically dropped |

> **Note:** If a temporary table is created **inside an open transaction** and no
> `ON COMMIT` clause is specified, the default behavior is `PRESERVE ROWS` —
> the table persists until the session closes.  Use `ON COMMIT DROP` to have
> the table discarded automatically when the transaction ends.

**Examples:**

```sql
-- Session-scoped temporary table (dropped when connection closes)
CREATE TEMPORARY TABLE tmp_data (
  id    INTEGER PRIMARY KEY,
  value TEXT
);

-- Dropped automatically when the current transaction commits or rolls back
CREATE TEMPORARY TABLE tmp_session (
  id    INTEGER PRIMARY KEY,
  value TEXT
) ON COMMIT DROP;

-- Rows are deleted on each commit, but structure survives the session
CREATE TEMPORARY TABLE tmp_batch (
  id    INTEGER PRIMARY KEY,
  value TEXT
) ON COMMIT DELETE ROWS;
```

---

### CREATE MATERIALIZED VIEW

```sql
CREATE [OR REPLACE] [IF NOT EXISTS]
MATERIALIZED VIEW view_name AS
SELECT ...
```

A materialized view stores the result of a query physically and can be queried
like a table.  Refreshing stale data is a **future-phase feature**.

**Example:**

```sql
CREATE MATERIALIZED VIEW mv_monthly_sales AS
SELECT region, SUM(amount) AS total
FROM orders
GROUP BY region;
```

---

### DROP TABLE

```sql
DROP TABLE [IF EXISTS] table_name [, table_name ...]
```

**Example:**

```sql
DROP TABLE IF EXISTS old_products;
```

---

### ALTER TABLE

```sql
ALTER TABLE table_name
  { ADD [COLUMN] column_name data_type [column_options]
  | DROP [COLUMN] [IF EXISTS] column_name
  | RENAME COLUMN old_name TO new_name
  | RENAME TO new_table_name
  }
```

**Examples:**

```sql
-- Add a column
ALTER TABLE users ADD COLUMN email VARCHAR(255);

-- Drop a column
ALTER TABLE users DROP COLUMN email;

-- Rename a column
ALTER TABLE users RENAME COLUMN name TO full_name;

-- Rename the table
ALTER TABLE users RENAME TO customers;
```

---

### CREATE INDEX

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (col1 [ASC|DESC], ...)
```

**Examples:**

```sql
-- Single-column index
CREATE INDEX idx_users_email ON users (email);

-- Unique index
CREATE UNIQUE INDEX idx_users_username ON users (username);

-- Composite index
CREATE INDEX idx_orders_user_date ON orders (user_id, created_at DESC);
```

---

### DROP INDEX

```sql
DROP INDEX [IF EXISTS] index_name ON table_name
```

**Example:**

```sql
DROP INDEX IF EXISTS idx_users_email ON users;
```

---

### Table-Level Constraints

Constraints can be declared at the table level to support composite keys and cross-column rules.

> **Note**: `FOREIGN KEY` constraints are **not supported** in AeternumDB.
> Use [reference column types](#reference-column-types) instead.
> Relationships are resolved via `objid` at execution time, which is more
> consistent across distributed nodes than traditional FK constraints.

```sql
CREATE TABLE order_items (
  order_id   INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  quantity   INTEGER NOT NULL,
  price      DECIMAL(10,2) NOT NULL,

  -- Composite primary key
  PRIMARY KEY (order_id, product_id),

  -- Composite unique constraint
  UNIQUE (order_id, product_id),

  -- CHECK constraint
  CHECK (quantity > 0),
  CHECK (price >= 0)
);
```

If you need to model a foreign-key style relationship, use a reference column type:

```sql
CREATE TABLE order_items (
  id         INTEGER  PRIMARY KEY AUTO_INCREMENT,
  order_ref  orders   NOT NULL,    -- reference to orders table (resolved via objid)
  product_ref products NOT NULL,   -- reference to products table
  quantity   INTEGER  NOT NULL CHECK (quantity > 0)
);
```
```

### Column-Level CHECK Constraint

```sql
CREATE TABLE products (
  id    INTEGER PRIMARY KEY AUTO_INCREMENT,
  price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
  stock INTEGER       NOT NULL CHECK (stock >= 0)
);
```

---

### CREATE USER

> **Scaffolding** — parsed; execution is a future phase.

```sql
CREATE USER username [WITH PASSWORD 'secret']
```

**Example:**

```sql
CREATE USER alice;
CREATE USER bob WITH PASSWORD 'hunter2';
```

---

### DROP USER

> **Scaffolding** — parsed; execution is a future phase.

```sql
DROP USER [IF EXISTS] username [, username2, ...]
```

---

### CREATE TYPE

**User Type Definitions (UTD)** allow DBAs to define composite types with optional
read/write/anonymization restrictions that apply per user or group.

> **Scaffolding** — parsed; execution and restrictions are a future phase.

```sql
CREATE TYPE contact_info AS (
  phone   VARCHAR(20),
  email   VARCHAR(255)
);
```

---

### GRANT / REVOKE

Column-level grants are supported:

```sql
-- Grant SELECT on all columns
GRANT SELECT ON employees TO analyst_role;

-- Grant SELECT on specific columns only (column-level restriction)
GRANT SELECT (id, name, department) ON employees TO hr_readonly;

-- Revoke column-level permission
REVOKE SELECT (salary) ON employees FROM hr_readonly;
```

> **Scaffolding** — parsed; execution is a future phase.

---

## Transaction Control

AeternumDB supports standard SQL transaction control statements (scaffolding for future execution):

### BEGIN TRANSACTION / START TRANSACTION

```sql
BEGIN [TRANSACTION] [READ ONLY]
  [ISOLATION LEVEL { READ UNCOMMITTED | READ COMMITTED
                   | REPEATABLE READ  | SERIALIZABLE }];
START TRANSACTION [READ ONLY] [ISOLATION LEVEL ...];
```

### COMMIT

```sql
COMMIT [TRANSACTION];
```

### ROLLBACK

```sql
ROLLBACK [TRANSACTION] [TO SAVEPOINT savepoint_name];
```

### SAVEPOINT

```sql
SAVEPOINT savepoint_name;
RELEASE SAVEPOINT savepoint_name;
```

**Example:**

```sql
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

CREATE TEMPORARY TABLE work_items (
  id    INTEGER PRIMARY KEY,
  value TEXT
) ON COMMIT DROP;               -- dropped automatically on COMMIT / ROLLBACK

INSERT INTO accounts (id, balance) VALUES (1, 1000);
SAVEPOINT sp1;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
-- Can rollback to sp1 if needed
COMMIT;
-- work_items is automatically dropped here (ON COMMIT DROP)
```

---

## Column Extensions

AeternumDB extends standard SQL column definitions with three powerful features
for multilingual content, typed metadata, and ordered value sequences.

### Text Directive (Multilingual)

A **text-directive** column stores a map of locale → value rather than a
single value.  Clients access a specific locale using the `column@'locale'`
accessor syntax.  When the requested locale is absent the column's
`DEFAULT_DIRECTIVE` locale is returned.

```sql
CREATE TABLE articles (
  id      INTEGER PRIMARY KEY AUTO_INCREMENT,
  -- Multilingual title; fall back to 'en' when locale is missing
  title   VARCHAR(255) NOT NULL DEFAULT_DIRECTIVE 'en',
  summary TEXT         DEFAULT_DIRECTIVE 'en'
);
```

**Accessor syntax (future query-layer feature):**

```sql
-- Retrieve English title (default)
SELECT title FROM articles WHERE id = 1;

-- Retrieve Brazilian Portuguese translation
SELECT title@'pt-BR' FROM articles WHERE id = 1;

-- Insert with locale
INSERT INTO articles (title@'en', title@'pt-BR') VALUES ('Hello', 'Olá');
```

> **Note**: `DEFAULT_DIRECTIVE` parsing is captured in the AST
> (`TextDirective { default_locale }`).  Locale-keyed storage and retrieval
> are execution-layer features planned for a future phase.

---

### Terms Directives

A **terms directive** attaches a named, strictly-typed metadata slot to each
cell value without adding extra rows.  Possible kinds are `TEXT`, `INTEGER`,
`FLOAT`, `BOOLEAN`, and `ENUM(...)`.

```sql
CREATE TABLE prices (
  id       INTEGER PRIMARY KEY AUTO_INCREMENT,
  amount   DECIMAL(12,4) NOT NULL
             TERMS currency TEXT,    -- e.g. 'USD', 'EUR'
             TERMS precision INTEGER -- significant digits stored
);
```

> **Scaffolding** — Terms directive metadata is captured in the AST
> (`TermsDirective { name, kind }`).  Storage and retrieval are planned
> for a future phase.

---

### Vector Columns

Any base data type can be declared as a **vector** — an ordered
variable-length sequence — by wrapping it in square brackets:

```sql
CREATE TABLE embeddings (
  id     INTEGER PRIMARY KEY AUTO_INCREMENT,
  tags   [VARCHAR(50)],       -- vector of text labels
  scores [FLOAT],             -- similarity scores
  bits   [BINARY(8)]          -- fixed-width bit vectors
);
```

Vector columns support element-level insert, update, delete, and aggregation
operations.  They can also be used as a JOIN source (similar to
`ReferenceArray` for scalar data).

> **Scaffolding** — the `DataType::Vector(Box<DataType>)` type is fully
> parsed.  Element-level DML and query planning are execution-layer features
> for a future phase.

---

### Versioned / Temporal Tables

Tables created with `WITH SYSTEM VERSIONING` retain the full history of every
row.  Each row version shares the same `objid` (see
[System Identifier](#system-identifier-objid)).

```sql
CREATE TABLE salary_history (
  id         INTEGER PRIMARY KEY AUTO_INCREMENT,
  employee_id INTEGER NOT NULL,
  amount     DECIMAL(10,2) NOT NULL
) WITH SYSTEM VERSIONING;
```

**Historical queries (future-phase):**

```sql
-- View salary as it was on a specific date
SELECT * FROM salary_history
FOR SYSTEM_TIME AS OF '2024-01-01'
WHERE employee_id = 42;

-- View all versions of a row
SELECT * FROM salary_history
FOR SYSTEM_TIME ALL
WHERE objid = '01920e3b-…';
```

> **Scaffolding** — `versioned: bool` is captured in
> `CreateTableStatement`.  The system-versioning mechanics (period columns,
> history table, `FOR SYSTEM_TIME` queries) are execution-layer features
> planned for a future phase.

---

---

## Joins and FILTER BY

### How Joins Work in AeternumDB

AeternumDB joins are **reference-driven**, not condition-driven.  Relationships
are encoded in the schema at table-definition time: a reference-type column
(e.g. `order_ref orders`) stores the `objid` of the related row.  At query
time the engine resolves the reference without an explicit `ON` clause.

**All join traversal must start from the `FROM` table and walk reference
columns.**  You do not list the target table separately; you navigate to it
through the reference column chain.

### Chain / Path Navigation

```
schema.table.ref_column.target_column
```

```sql
-- Simple one-hop: users → orders via order_ref
SELECT u.name, u.order_ref.total
FROM users u;

-- Two-hop chain: users → orders → line items → product
SELECT u.name, u.order_ref.line_ref.product_name
FROM app.users u;

-- Cross-schema traversal (same database)
SELECT u.name, u.report_ref.summary
FROM app.users u;
```

### FILTER BY

`FILTER BY` adds an **optional extra predicate** that further narrows the
traversal result after the reference has been resolved:

```sql
-- Only rows where the referenced order total > 100
SELECT u.name, u.order_ref.total
FROM users u
FILTER BY u.order_ref.total > 100;

-- Deep chain with filter
SELECT u.name, u.order_ref.line_ref.qty
FROM app.users u
FILTER BY u.order_ref.status = 'shipped';
```

### Cross-Database vs. Cross-Schema

| Scope           | Joins supported? |
|-----------------|-----------------|
| Same schema     | ✅ Yes |
| Cross-schema    | ✅ Yes (same database) |
| Cross-database  | ❌ No — connection-level routing only |

### FLAT Tables and Joins

`FLAT` tables **cannot participate in JOINs**.  Attempting to join a FLAT table
at execution time will raise an error.

---

## Expressions

### Literals

| Example      | Type    |
|--------------|---------|
| `42`         | Integer |
| `3.14`       | Float   |
| `'hello'`    | String  |
| `TRUE`, `FALSE` | Boolean |
| `NULL`       | Null    |

### Column References

```sql
-- Simple column
column_name

-- Table-qualified column
table_name.column_name

-- Chain / path navigation through reference columns
table_alias.ref_column.target_column
schema.table.ref_column.target_column
```

### CASE Expression

```sql
-- Searched CASE
CASE WHEN condition THEN result [...] [ELSE default] END

-- Simple CASE
CASE expr WHEN value THEN result [...] [ELSE default] END
```

### Text Functions

| Function                                   | Description                                 |
|--------------------------------------------|---------------------------------------------|
| `SUBSTRING(str FROM pos [FOR len])`        | Extract substring                           |
| `SUBSTRING(str FROM regex)`                | Regex-based extraction                      |
| `POSITION(needle IN haystack)`             | Position of substring (1-based)             |
| `TRIM([BOTH\|LEADING\|TRAILING] ch FROM s)`| Remove characters from string ends          |
| `REPLACE(str, from, to)`                   | Replace all occurrences                     |
| `OVERLAY(str PLACING repl FROM pos [FOR n])`| Replace substring at position             |

### Full-Text Search — MATCH … AGAINST

Full-text search uses the `MATCH … AGAINST` syntax and requires a `FULLTEXT`
or `TRIGRAM` index on the target columns.

```sql
MATCH (col1 [, col2, ...]) AGAINST (expr [search_modifier])
```

**Search modifiers:**

| Modifier                    | Description                                          |
|-----------------------------|------------------------------------------------------|
| *(none)*                    | Natural language mode (default)                      |
| `IN NATURAL LANGUAGE MODE`  | Natural language mode (explicit)                     |
| `IN BOOLEAN MODE`           | Boolean mode — supports `+`, `-`, `*` operators      |
| `WITH QUERY EXPANSION`      | Two-pass relevance search                            |

```sql
-- Natural language search
SELECT id, title
FROM articles
WHERE MATCH (title, body) AGAINST ('database performance');

-- Boolean mode search
SELECT id, title
FROM articles
WHERE MATCH (title, body) AGAINST ('+performance -slow' IN BOOLEAN MODE);

-- With query expansion
SELECT id, title
FROM articles
WHERE MATCH (title, body) AGAINST ('database' WITH QUERY EXPANSION);
```

Relevant index types:

| Index type | Use case                                          |
|------------|---------------------------------------------------|
| `FULLTEXT` | Full-text search with stop-words and stemming     |
| `TRIGRAM`  | Fuzzy / partial-word search using 3-gram tokens   |
| `BRIN`     | Range-based index for large ordered columns       |
| `GIN`      | Inverted index for vector/array columns           |
| `GiST`     | Generalized search tree (geometric, range types)  |

### UNNEST — Expand Vector Columns

`UNNEST` expands a vector (array) column into individual rows:

```sql
SELECT id, UNNEST(tags) AS tag FROM articles;
```

This produces one output row per element in the `tags` vector column.

---

## Limitations and Future Enhancements

The following SQL features are **not yet supported** or are **partially implemented**:

| Feature                                     | Status / Planned Phase           |
|---------------------------------------------|----------------------------------|
| Window functions (`OVER (...)`)             | Phase 5                          |
| Recursive queries (`WITH RECURSIVE`)        | Phase 5                          |
| `UNION` / `INTERSECT` / `EXCEPT`            | Phase 4                          |
| `GRANT` / `REVOKE` execution               | Phase 6                          |
| Column-level permission enforcement         | Future execution layer           |
| Transaction execution                       | Parsed; not yet executed         |
| Temporary table auto-drop enforcement       | Future execution layer           |
| Materialized view refresh                  | Future execution layer           |
| Reference type storage + querying           | Future execution layer           |
| `objid` generation (cluster-unique ID)      | Future execution layer           |
| Temporal / versioned table mechanics        | Future execution layer           |
| `FOR SYSTEM_TIME` historical queries        | Future execution layer           |
| Backup / restore execution                  | Future execution layer           |
| `CREATE USER` / `DROP USER` execution       | Future execution layer           |
| `CREATE TYPE` (UTD) execution               | Future execution layer           |
| Text Directive locale-keyed storage         | Future execution layer           |
| Terms Directive storage + retrieval         | Future execution layer           |
| Vector element-level DML                    | Future execution layer           |
| Full-text search syntax                     | Extension                        |
| JSON path expressions                       | Extension                        |
| `RETURNING` clause                          | Phase 3                          |
| `ON CONFLICT` / `UPSERT`                    | Phase 3                          |
| `PIVOT` / `UNPIVOT`                         | Phase 6                          |
| COUNT optimization (index metadata)         | Future optimization              |
| `CREATE DATABASE` / `DROP DATABASE` execution | Future execution layer         |
| `CREATE SCHEMA` / `DROP SCHEMA` execution  | Future execution layer           |
| `USE DATABASE` connection routing           | Future execution layer           |
| Reserved schema enforcement (`sys`, etc.)  | Future execution layer           |
| `FLAT` keyword in SQL (`CREATE FLAT TABLE`)| Phase 4 (parsed programmatically now) |
| `FILTER BY` keyword in SQL JOIN             | Phase 4 (ON mapped to filter_by now)  |
| FLAT table join enforcement                 | Future execution layer           |
| Cross-database join rejection               | Future execution layer           |

### Known Dialect Edge Cases

- **Case insensitivity**: All SQL keywords and identifiers are fully
  case-insensitive.  Identifiers are normalized to lowercase during parsing.
  String *literals* preserve their original casing.
- **Quoted identifiers**: Backticks (`` `name` ``) or double-quotes (`"name"`)
  allow reserved words as identifiers.
- **Comments**: Both `-- single-line` and `/* block */` comments are
  supported, including nested block comments.
- **Multiple statements**: Semicolon-separated multi-statement strings are
  accepted by `SqlParser::parse`.
- **UNSIGNED floats/decimals**: `FLOAT UNSIGNED`, `DOUBLE UNSIGNED`, and
  `DECIMAL UNSIGNED` are rejected with a helpful error.  Use a `CHECK` constraint
  to enforce non-negative values.
- **FOREIGN KEY**: `FOREIGN KEY` table constraints are **not supported**.
  Use reference column types to model relationships.

---

*This document covers PR 1.3. See `docs/prs/PR-1.3-sql-parser.md`
for the full design specification.*
