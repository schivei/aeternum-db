---
sidebar_position: 1
---

# 📝 SQL Reference

[![SQL-92](https://img.shields.io/badge/Standard-SQL--92-blue)](https://en.wikipedia.org/wiki/SQL-92)
[![Extensions](https://img.shields.io/badge/Extensions-AeternumDB-purple)](https://github.com/schivei/aeternum-db)

AeternumDB supports **SQL-92** with a rich set of extensions for multi-paradigm data, vector columns, reference types, and temporal tables.

---

## Data Types

### 🔢 Integer Types

| SQL Type | Range | Notes |
|---|---|---|
| `TINYINT` | −128 … 127 | 8-bit signed |
| `TINYINT UNSIGNED` | 0 … 255 | 8-bit unsigned |
| `SMALLINT` | ±32 767 | 16-bit signed |
| `INTEGER` / `INT` | ±2.1 × 10⁹ | 32-bit signed |
| `BIGINT` | ±9.2 × 10¹⁸ | 64-bit signed |
| `BIGINT UNSIGNED` | 0 … 18.4 × 10¹⁸ | 64-bit unsigned |

### 🌊 Floating-Point Types

| SQL Type | Precision | Notes |
|---|---|---|
| `FLOAT` / `REAL` | 32-bit | IEEE 754 |
| `DOUBLE` / `DOUBLE PRECISION` | 64-bit | IEEE 754 |

### 💰 Fixed-Precision

| SQL Type | Notes |
|---|---|
| `DECIMAL(p, s)` / `NUMERIC(p, s)` | Fixed-point with precision `p` and scale `s` |

### 📜 Character Types

| SQL Type | Notes |
|---|---|
| `CHAR(n)` | Fixed-length, padded to `n` |
| `VARCHAR(n)` | Variable-length, max `n` chars |
| `TEXT` | Unbounded string |

### 📅 Date / Time Types

| SQL Type | Notes |
|---|---|
| `DATE` | Calendar date without time |
| `TIME` | Time of day |
| `TIMESTAMP` | Date + time (no timezone) |
| `TIMESTAMP WITH TIME ZONE` | Date + time with timezone offset |

### 🆔 Special Types

| SQL Type | Notes |
|---|---|
| `BOOLEAN` / `BOOL` | `TRUE` / `FALSE` |
| `UUID` / `GUID` | 128-bit unique identifier |
| `JSON` / `JSONB` | JSON document |

---

## Operators

### Arithmetic

| Operator | Description | Example |
|---|---|---|
| `+` | Addition | `price + tax` |
| `-` | Subtraction | `price - discount` |
| `*` | Multiplication | `qty * price` |
| `/` | Division | `total / qty` |
| `%` | Modulo | `id % 10` |

### Comparison

| Operator | Description |
|---|---|
| `=` | Equal |
| `<>` / `!=` | Not equal |
| `<`, `<=`, `>`, `>=` | Relational |
| `IS NULL` / `IS NOT NULL` | Null check |
| `BETWEEN a AND b` | Range check |
| `IN (…)` | Set membership |
| `LIKE 'pattern'` | Pattern matching (`%` wildcard) |

### Logical

| Operator | Description |
|---|---|
| `AND` | Logical AND |
| `OR` | Logical OR |
| `NOT` | Logical NOT |

---

## Statements

### SELECT

```sql
SELECT [DISTINCT] expression [AS alias], ...
FROM   table_reference [AS alias]
[JOIN  table ON condition]
[WHERE predicate]
[GROUP BY columns]
[HAVING predicate]
[ORDER BY columns [ASC|DESC]]
[LIMIT n [OFFSET m]]
```

**Examples:**

```sql
-- Basic query
SELECT id, name, age FROM users WHERE age > 18;

-- Aggregation
SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
HAVING COUNT(*) > 5
ORDER BY avg_salary DESC;

-- Join
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.total > 100.00
LIMIT 20;
```

### JOIN Types

| Join Type | Syntax | Description |
|---|---|---|
| Inner Join | `JOIN … ON` | Rows matching on both sides |
| Left Outer | `LEFT JOIN … ON` | All left rows + matching right |
| Right Outer | `RIGHT JOIN … ON` | All right rows + matching left |
| Full Outer | `FULL JOIN … ON` | All rows from both sides |
| Cross Join | `CROSS JOIN` | Cartesian product |

:::warning Cross-Database Joins
AeternumDB **rejects** queries that join tables from different databases. All table references in a single query must belong to the same database.
:::

### INSERT

```sql
INSERT INTO table_name (col1, col2, ...)
VALUES (val1, val2, ...);

-- Multi-row insert
INSERT INTO products (name, price)
VALUES ('Widget A', 9.99),
       ('Widget B', 14.99),
       ('Widget C', 4.99);
```

### UPDATE

```sql
UPDATE table_name
SET    col1 = expr1, col2 = expr2
WHERE  predicate;
```

### DELETE

```sql
DELETE FROM table_name
WHERE  predicate;
```

---

## DDL Statements

### CREATE TABLE

```sql
CREATE TABLE users (
    id      BIGINT       NOT NULL PRIMARY KEY,
    email   VARCHAR(255) NOT NULL UNIQUE,
    name    VARCHAR(100) NOT NULL,
    age     INTEGER,
    created TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);
```

### CREATE FLAT TABLE

FLAT tables are optimised for bulk analytical reads and **cannot participate in joins**.

```sql
CREATE FLAT TABLE analytics_events (
    event_id   UUID    NOT NULL,
    event_type VARCHAR(64),
    payload    JSON,
    occurred   TIMESTAMP
);
```

### CREATE INDEX

```sql
CREATE INDEX idx_users_email ON users (email);
CREATE UNIQUE INDEX idx_users_id ON users (id);
```

### Transaction Control

```sql
BEGIN TRANSACTION;
  UPDATE accounts SET balance = balance - 100 WHERE id = 1;
  UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- Rollback on error
BEGIN TRANSACTION;
  DELETE FROM orders WHERE status = 'pending';
ROLLBACK;

-- Savepoints
BEGIN TRANSACTION;
  INSERT INTO log VALUES ('started');
  SAVEPOINT before_risky;
  DELETE FROM temp_data;
  ROLLBACK TO before_risky;  -- only undo the DELETE
COMMIT;
```

---

## 🚀 AeternumDB Extensions

### EXPAND — Array Column Unnesting

```sql
-- Expand a vector reference column into individual rows
SELECT u.id, EXPAND(u.tag_ids) AS tag_id
FROM users u;
```

### VIEW AS — Post-Result Transformation

```sql
SELECT id, raw_score
FROM scores
VIEW AS (
    raw_score * 100.0 AS percentage
);
```

### Vector Columns

```sql
CREATE TABLE products (
    id          BIGINT NOT NULL PRIMARY KEY,
    name        VARCHAR(255),
    -- 128-dimension embedding vector
    embedding   VECTOR(128)
);
```

### Reference Columns

```sql
CREATE TABLE orders (
    id       BIGINT NOT NULL PRIMARY KEY,
    user_id  REFERENCE users(id),   -- typed foreign reference
    items    ARRAY REFERENCE products(id)
);
```

---

## Limitations

| Limitation | Status |
|---|---|
| Correlated subqueries | 🔄 Planned |
| Window functions | 🔄 Planned |
| CTEs (`WITH` clause) | 🔄 Planned |
| `EXPLAIN ANALYZE` (with runtime stats) | 🔄 Planned |
| Cross-database joins | ❌ Not supported (by design) |

---

:::tip
Use `EXPLAIN` to inspect how AeternumDB plans your queries before running them at scale. See [EXPLAIN Reference](./explain.md).
:::
