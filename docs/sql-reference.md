# AeternumDB SQL Reference

This document describes the SQL dialect supported by AeternumDB.  The
implementation is based on **SQL-92** with a subset of common SQL extensions.

---

## Table of Contents

1. [Data Types](#data-types)
2. [Operators](#operators)
3. [Functions](#functions)
4. [Statements](#statements)
   - [SELECT](#select)
   - [INSERT](#insert)
   - [UPDATE](#update)
   - [DELETE](#delete)
   - [CREATE TABLE](#create-table)
   - [DROP TABLE](#drop-table)
   - [ALTER TABLE](#alter-table)
5. [Expressions](#expressions)
6. [Limitations and Future Enhancements](#limitations-and-future-enhancements)

---

## Data Types

| SQL Type              | Internal type | Notes                                  |
|-----------------------|---------------|----------------------------------------|
| `INTEGER`, `INT`, `BIGINT`, `SMALLINT`, `TINYINT` | Integer | 64-bit signed integer |
| `FLOAT`, `REAL`, `DOUBLE`, `DOUBLE PRECISION` | Float | 64-bit IEEE 754 float |
| `VARCHAR(n)`, `CHAR(n)` | Varchar(n) | Variable-length string, max `n` chars |
| `TEXT`                | Varchar(∞)   | Unbounded string                       |
| `BOOLEAN`, `BOOL`     | Boolean      | `TRUE` / `FALSE`                       |
| `DATE`                | Date         | Calendar date without time             |
| `TIMESTAMP`           | Timestamp    | Date and time                          |
| `DECIMAL(p, s)`, `NUMERIC(p, s)` | Decimal | Precision `p`, scale `s`  |
| `table_name`          | Reference     | Single row reference (foreign key)    |
| `[table_name]`        | ReferenceArray | Array of row references               |
| `~table_name(column)` | VirtualReference | Computed inverse reference (read-only) |
| `~[table_name](column)` | VirtualReferenceArray | Computed inverse array (read-only) |

All other types are accepted as-is and forwarded to the query planner as the
string representation of the type name.

### Reference Types (OO/Graph Paradigm)

AeternumDB supports advanced reference types for object-oriented and graph-style data modeling:

- **Direct References**: `table_name` creates a foreign key relationship
- **Reference Arrays**: `[table_name]` stores multiple references in an array
- **Virtual References**: `~table_name(column)` provides automatic reverse navigation
- **Virtual Arrays**: `~[table_name](column)` provides reverse one-to-many access

**Constraints for Reference Arrays:**
- `MIN_LENGTH n` - minimum number of references required
- `MAX_LENGTH n` - maximum number of references allowed
- `UNIQUES` - all references must be distinct

**Example:**
```sql
CREATE TABLE sample2 (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  `master` sample1 NOT NULL,
  `parent` sample2 NULL,
  `children` ~[sample2](parent) MIN_LENGTH 0
)
```

---

## Operators

### Arithmetic

| Operator | Description   |
|----------|---------------|
| `+`      | Addition      |
| `-`      | Subtraction   |
| `*`      | Multiplication |
| `/`      | Division      |
| `%`      | Modulo        |

### Comparison

| Operator | Description             |
|----------|-------------------------|
| `=`      | Equal                   |
| `!=`, `<>` | Not equal             |
| `<`      | Less than               |
| `<=`     | Less than or equal      |
| `>`      | Greater than            |
| `>=`     | Greater than or equal   |

### Logical

| Operator | Description |
|----------|-------------|
| `AND`    | Logical AND |
| `OR`     | Logical OR  |
| `NOT`    | Logical NOT |

### String

| Operator   | Description         |
|------------|---------------------|
| `LIKE`     | Pattern matching    |
| `NOT LIKE` | Negative pattern    |

### Other

| Operator/Syntax             | Description                             |
|-----------------------------|-----------------------------------------|
| `IS NULL`                   | Test for NULL                           |
| `IS NOT NULL`               | Test for non-NULL                       |
| `BETWEEN low AND high`      | Range test (inclusive)                  |
| `NOT BETWEEN low AND high`  | Negated range test                      |
| `IN (v1, v2, ...)`          | Membership test                         |
| `NOT IN (v1, v2, ...)`      | Negated membership                      |
| `IN (subquery)`             | Subquery membership test                |
| `CAST(expr AS type)`        | Type conversion                         |

---

## Functions

### Aggregate Functions

Aggregate functions operate on a set of rows and return a single value.
They are valid in the `SELECT` list and `HAVING` clause, but **not** in
`WHERE`.

| Function         | Description                                   |
|------------------|-----------------------------------------------|
| `COUNT(*)`       | Count all rows                                |
| `COUNT(expr)`    | Count non-NULL values of `expr`               |
| `COUNT(DISTINCT expr)` | Count distinct non-NULL values          |
| `SUM(expr)`      | Sum of non-NULL numeric values                |
| `AVG(expr)`      | Average of non-NULL numeric values            |
| `MIN(expr)`      | Minimum value                                 |
| `MAX(expr)`      | Maximum value                                 |

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
    column_name data_type [NOT NULL] [PRIMARY KEY] [UNIQUE] [DEFAULT expr],
    ...
)
```

**Example:**

```sql
CREATE TABLE IF NOT EXISTS products (
    id          INTEGER       PRIMARY KEY,
    name        VARCHAR(255)  NOT NULL,
    price       DECIMAL(10,2),
    in_stock    BOOLEAN       DEFAULT TRUE
);
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

## Expressions

### Literals

| Example     | Type    |
|-------------|---------|
| `42`        | Integer |
| `3.14`      | Float   |
| `'hello'`   | String  |
| `TRUE`, `FALSE` | Boolean |
| `NULL`      | Null    |

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

## Transaction Control

AeternumDB supports standard SQL transaction control statements (scaffolding for future execution):

### BEGIN TRANSACTION / START TRANSACTION

```sql
BEGIN [TRANSACTION] [READ ONLY] [ISOLATION LEVEL { READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE }];
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
INSERT INTO accounts (id, balance) VALUES (1, 1000);
SAVEPOINT sp1;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
-- Can rollback to sp1 if needed
COMMIT;
```

---

## Limitations and Future Enhancements

The following SQL features are **not yet supported** or are **partially implemented**:

| Feature                              | Status / Planned Phase |
|--------------------------------------|----------------------|
| Window functions (`OVER (...)`)      | Phase 5              |
| Recursive queries (`WITH RECURSIVE`) | Phase 5              |
| `UNION` / `INTERSECT` / `EXCEPT`     | Phase 4              |
| `GRANT` / `REVOKE` execution         | Phase 6              |
| Transaction execution                | Recognized but not yet executed |
| Full-text search syntax              | Extension            |
| JSON path expressions                | Extension            |
| `RETURNING` clause                   | Phase 3              |
| `ON CONFLICT` / `UPSERT`            | Phase 3              |
| `PIVOT` / `UNPIVOT`                 | Phase 6              |
| Reference type execution             | Future (parsed but not executed) |
| COUNT optimization (index metadata)  | Future optimization  |

### Known Dialect Edge Cases

- **Case sensitivity**: SQL keywords are case-insensitive; identifiers are
  case-sensitive by default.
- **Quoted identifiers**: Double-quotes (`"name"`) allow reserved words as
  identifiers.
- **Comments**: Both `-- single-line` and `/* block */` comments are
  supported, including nested block comments.
- **Multiple statements**: Semicolon-separated multi-statement strings are
  accepted by `SqlParser::parse`.

---

*This document is generated for PR 1.3. See `docs/prs/PR-1.3-sql-parser.md`
for the full design specification.*
