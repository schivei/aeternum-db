# AeternumDB SQL Reference

This document describes the SQL dialect supported by AeternumDB.  The
implementation is based on **SQL-92** with a subset of common SQL extensions.

---

## Table of Contents

1. [Data Types](#data-types)
2. [Identifier Quoting](#identifier-quoting)
3. [Operators](#operators)
4. [Functions](#functions)
5. [Statements](#statements)
   - [SELECT](#select)
   - [INSERT](#insert)
   - [UPDATE](#update)
   - [DELETE](#delete)
   - [CREATE TABLE](#create-table)
   - [CREATE TEMPORARY TABLE](#create-temporary-table)
   - [CREATE MATERIALIZED VIEW](#create-materialized-view)
   - [DROP TABLE](#drop-table)
   - [ALTER TABLE](#alter-table)
6. [Transaction Control](#transaction-control)
7. [Expressions](#expressions)
8. [Limitations and Future Enhancements](#limitations-and-future-enhancements)

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

| SQL Type                          | Notes                                                                 |
|-----------------------------------|-----------------------------------------------------------------------|
| `FLOAT` / `REAL`                  | 32-bit signed IEEE 754                                                |
| `FLOAT UNSIGNED`                  | MySQL-specific; restricts values to non-negative (≥ 0); not standard IEEE 754 unsigned |
| `DOUBLE` / `DOUBLE PRECISION`     | 64-bit signed IEEE 754                                                |
| `DOUBLE UNSIGNED`                 | MySQL-specific; restricts values to non-negative (≥ 0); not standard IEEE 754 unsigned |

### Fixed-Precision Types

| SQL Type                             | Notes                                              |
|--------------------------------------|----------------------------------------------------|
| `DECIMAL(p, s)` / `NUMERIC(p, s)`   | Fixed-point with precision `p` and scale `s`       |
| `DECIMAL(p, s) UNSIGNED`            | Unsigned fixed-point (MySQL; deprecated in MySQL 8+) |

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

### Reference Types (OO / Graph Paradigm)

AeternumDB extends the SQL type system with reference types for object-oriented
and graph-style data modelling.  These are **future-execution scaffolding** —
the parser captures them fully; storage and query execution will be implemented
in later phases.

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

Unquoted identifiers are case-sensitive.

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

### String

| Operator   | Description         |
|------------|---------------------|
| `LIKE`     | Pattern matching    |
| `NOT LIKE` | Negative pattern    |

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
    { * | column [AS alias] | expr [AS alias] }, ...
FROM table_reference
[WHERE condition]
[GROUP BY expr, ...]
[HAVING condition]
[ORDER BY expr [ASC | DESC], ...]
[LIMIT n [OFFSET m]]
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

```sql
SELECT u.name, o.total
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE o.total > 100;
```

Supported JOIN types: `INNER JOIN`, `LEFT JOIN`, `LEFT OUTER JOIN`,
`RIGHT JOIN`, `RIGHT OUTER JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`.

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
CREATE [IF NOT EXISTS] TABLE table_name (
    column_name data_type [NOT NULL | NULL] [PRIMARY KEY] [UNIQUE]
                          [DEFAULT expr] [AUTO_INCREMENT]
                          [MIN_LENGTH n] [MAX_LENGTH n] [UNIQUES],
    ...
)
[INHERITS (parent_table [, ...])]
```

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

**Example with reference types:**

```sql
CREATE TABLE sample2 (
  id         INTEGER PRIMARY KEY AUTO_INCREMENT,
  `master`   sample1  NOT NULL,
  `parent`   sample2  NULL,
  `children` ~[sample2](parent) MIN_LENGTH 0
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
column_name
table_name.column_name
```

### CASE Expression

```sql
-- Searched CASE
CASE WHEN condition THEN result [...] [ELSE default] END

-- Simple CASE
CASE expr WHEN value THEN result [...] [ELSE default] END
```

---

## Limitations and Future Enhancements

The following SQL features are **not yet supported** or are **partially implemented**:

| Feature                                  | Status / Planned Phase |
|------------------------------------------|----------------------|
| Window functions (`OVER (...)`)          | Phase 5              |
| Recursive queries (`WITH RECURSIVE`)     | Phase 5              |
| `UNION` / `INTERSECT` / `EXCEPT`         | Phase 4              |
| `GRANT` / `REVOKE` execution             | Phase 6              |
| Transaction execution                    | Parsed; not yet executed |
| Temporary table auto-drop enforcement    | Future execution layer |
| Materialized view refresh               | Future execution layer |
| Reference type storage + querying        | Future execution layer |
| Full-text search syntax                  | Extension            |
| JSON path expressions                    | Extension            |
| `RETURNING` clause                       | Phase 3              |
| `ON CONFLICT` / `UPSERT`                 | Phase 3              |
| `PIVOT` / `UNPIVOT`                      | Phase 6              |
| COUNT optimization (index metadata)      | Future optimization  |

### Known Dialect Edge Cases

- **Case sensitivity**: SQL keywords are case-insensitive; identifiers are
  case-sensitive by default.
- **Quoted identifiers**: Backticks (`` `name` ``) or double-quotes (`"name"`)
  allow reserved words as identifiers.
- **Comments**: Both `-- single-line` and `/* block */` comments are
  supported, including nested block comments.
- **Multiple statements**: Semicolon-separated multi-statement strings are
  accepted by `SqlParser::parse`.

---

*This document covers PR 1.3. See `docs/prs/PR-1.3-sql-parser.md`
for the full design specification.*

