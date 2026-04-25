# PR 1.3: SQL Parser Integration

## 📋 Overview

**PR Number:** 1.3
**Phase:** 1 - Core Foundation
**Priority:** 🔴 Critical
**Estimated Effort:** 4 days
**Dependencies:** None (independent)

## 🎯 Objectives

Integrate the `sqlparser-rs` library to parse SQL statements and convert them into an Abstract Syntax Tree (AST) that can be used by the query planner and executor. Support common SQL-92 statements with proper error handling and validation.

## 📝 Detailed Prompt for Implementation

```
Integrate SQL parsing into AeternumDB with the following requirements:

1. **SQL Parser Integration**
   - Use `sqlparser-rs` crate (production-ready, well-maintained)
   - Support SQL-92 subset
   - Parse DDL: CREATE TABLE, DROP TABLE, ALTER TABLE
   - Parse DML: SELECT, INSERT, UPDATE, DELETE
   - Parse DCL: GRANT, REVOKE (future)

2. **AST Representation**
   - Convert sqlparser AST to internal AST
   - Type-safe representation
   - Easy to traverse for query planning

3. **Syntax Validation**
   - Catch syntax errors with helpful messages
   - Line and column number reporting
   - Suggest corrections when possible

4. **Semantic Validation**
   - Check table existence
   - Check column existence
   - Type checking
   - Validate constraints

5. **Supported SQL Features**
   - SELECT: columns, WHERE, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT
   - INSERT: single and multi-row
   - UPDATE: with WHERE clause
   - DELETE: with WHERE clause
   - CREATE TABLE: columns, constraints, data types
   - DROP TABLE
   - Basic expressions: arithmetic, comparison, logical
   - Subqueries (SELECT in FROM/WHERE)
   - Aggregate functions: COUNT, SUM, AVG, MIN, MAX

6. **Error Handling**
   - User-friendly error messages
   - Context in error reports
   - Recovery from errors when possible
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/sql/mod.rs`**
   - Public SQL parsing API
   - Re-exports

2. **`core/src/sql/parser.rs`**
   - SQL parser implementation
   - Wrapper around sqlparser-rs
   - Error conversion

3. **`core/src/sql/ast.rs`**
   - Internal AST types
   - Conversion from sqlparser AST
   - AST visitor pattern

4. **`core/src/sql/validator.rs`**
   - Semantic validation
   - Type checking
   - Schema validation

5. **`core/src/sql/dialect.rs`**
   - AeternumDB SQL dialect
   - Extensions beyond SQL-92

### Test Files

6. **`core/tests/sql_parser_tests.rs`**
   - Comprehensive parsing tests

## 🔧 Implementation Details

### Parser API

```rust
pub struct SqlParser {
    dialect: Box<dyn Dialect>,
}

impl SqlParser {
    pub fn new() -> Self {
        SqlParser {
            dialect: Box::new(AeternumDbDialect::default()),
        }
    }

    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, SqlError> {
        // Parse SQL using sqlparser-rs
        // Convert to internal AST
    }

    pub fn parse_expr(&self, expr: &str) -> Result<Expr, SqlError> {
        // Parse single expression
    }
}
```

### Internal AST Types

```rust
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
}

pub struct SelectStatement {
    pub columns: Vec<SelectItem>,
    pub from: Option<TableReference>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

pub enum SelectItem {
    Wildcard,
    Column(String),
    AliasedExpr(Expr, String),
}

pub enum Expr {
    Literal(Value),
    Column(String),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
    Subquery(Box<SelectStatement>),
}

pub enum BinaryOperator {
    Plus, Minus, Multiply, Divide,
    Eq, NotEq, Lt, LtEq, Gt, GtEq,
    And, Or,
}
```

### API Examples

```rust
use aeternumdb::sql::SqlParser;

// Parse a SELECT statement
let parser = SqlParser::new();
let sql = "SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10";
let statements = parser.parse(sql)?;

match &statements[0] {
    Statement::Select(select) => {
        println!("Columns: {:?}", select.columns);
        println!("Table: {:?}", select.from);
    },
    _ => {}
}

// Parse INSERT
let sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)";
let statements = parser.parse(sql)?;

// Parse CREATE TABLE
let sql = "CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2)
)";
let statements = parser.parse(sql)?;

// Error handling
let sql = "SELET * FROM users";  // Typo
match parser.parse(sql) {
    Err(SqlError::ParseError { message, line, col }) => {
        eprintln!("Parse error at line {}, col {}: {}", line, col, message);
    },
    _ => {}
}
```

### Semantic Validation

```rust
pub struct Validator<'a> {
    catalog: &'a Catalog,
}

impl<'a> Validator<'a> {
    pub fn validate(&self, stmt: &Statement) -> Result<(), ValidationError> {
        match stmt {
            Statement::Select(select) => {
                // Check table exists
                if let Some(table) = &select.from {
                    self.check_table_exists(table)?;
                }

                // Check columns exist
                for col in &select.columns {
                    self.check_column_exists(col)?;
                }

                // Type check expressions
                if let Some(where_clause) = &select.where_clause {
                    self.type_check_expr(where_clause)?;
                }
            },
            // ... other statement types
        }
        Ok(())
    }
}
```

## ✅ Tests Required

### Unit Tests

1. **Parser Tests** (`sql_parser_tests.rs`)
   - ✅ Parse SELECT with single column
   - ✅ Parse SELECT with multiple columns
   - ✅ Parse SELECT with wildcard (*)
   - ✅ Parse SELECT with WHERE clause
   - ✅ Parse SELECT with JOIN
   - ✅ Parse SELECT with GROUP BY
   - ✅ Parse SELECT with ORDER BY
   - ✅ Parse SELECT with LIMIT/OFFSET
   - ✅ Parse INSERT single row
   - ✅ Parse INSERT multiple rows
   - ✅ Parse UPDATE with WHERE
   - ✅ Parse DELETE with WHERE
   - ✅ Parse CREATE TABLE
   - ✅ Parse DROP TABLE

2. **Expression Tests**
   - ✅ Parse arithmetic expressions (1 + 2 * 3)
   - ✅ Parse comparison expressions (age > 18)
   - ✅ Parse logical expressions (a AND b OR c)
   - ✅ Parse function calls (COUNT(*), SUM(price))
   - ✅ Parse subqueries

3. **Error Handling Tests**
   - ✅ Syntax errors caught
   - ✅ Error messages are helpful
   - ✅ Line/column numbers reported
   - ✅ Invalid identifiers rejected
   - ✅ Unclosed quotes detected

4. **Validation Tests** (`validator_tests.rs`)
   - ✅ Non-existent table detected
   - ✅ Non-existent column detected
   - ✅ Type mismatches caught
   - ✅ Invalid aggregate usage detected

### Integration Tests

5. **Complex Queries**
   - ✅ Parse TPC-H Query 1
   - ✅ Parse TPC-H Query 3
   - ✅ Parse nested subqueries
   - ✅ Parse complex JOINs

## 📊 Performance Targets

| Operation | Target | Measurement |
|-----------|--------|-------------|
| Parse simple SELECT | <100μs | Single query |
| Parse complex query | <1ms | TPC-H Q1 |
| Parse error recovery | <500μs | Syntax error |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Rustdoc for all public APIs
   - Examples in doc comments

2. **SQL Reference** (`docs/sql-reference.md`)
   - Supported SQL syntax
   - Data types
   - Functions
   - Operators
   - Limitations

3. **Examples** (`examples/sql_parsing.rs`)
   - Basic query parsing
   - Error handling
   - AST traversal

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] All basic SQL statements parse correctly
- [ ] AST is correct and type-safe
- [ ] Syntax errors caught with helpful messages
- [ ] Semantic validation works
- [ ] Complex queries supported (JOINs, subqueries)

### Quality Requirements
- [ ] All tests pass
- [ ] Code coverage >85%
- [ ] No clippy warnings
- [ ] Code formatted

### Performance Requirements
- [ ] Parse performance meets targets
- [ ] Low memory overhead

### Documentation Requirements
- [ ] SQL reference complete
- [ ] Examples provided
- [ ] API documented

## 🔗 Related Files

- `Cargo.toml` - Add sqlparser dependency
- `core/src/lib.rs` - Export SQL module

## 📦 Dependencies to Add

```toml
[dependencies]
sqlparser = "0.40"
```

## 🚀 Implementation Steps

1. **Day 1: Basic Parser Setup**
   - Add sqlparser dependency
   - Create SQL module structure
   - Wrap sqlparser-rs
   - Parse simple SELECT

2. **Day 2: AST Conversion**
   - Define internal AST types
   - Convert sqlparser AST to internal AST
   - Parse all statement types

3. **Day 3: Validator**
   - Implement semantic validator
   - Table and column existence checks
   - Type checking

4. **Day 4: Testing & Documentation**
   - Comprehensive tests
   - Error handling improvements
   - Documentation
   - Examples

## 🐛 Known Edge Cases to Handle

1. **Case sensitivity**: SQL is case-insensitive for keywords
2. **Quoted identifiers**: Handle `"table name"` vs `table_name`
3. **String literals**: Single quotes `'string'`
4. **Comments**: `-- comment` and `/* comment */`
5. **Multiple statements**: Separated by semicolons
6. **Unicode in identifiers**: Support or reject?
7. **Reserved keywords as identifiers**: How to handle?

## 💡 Future Enhancements (Out of Scope)

- Window functions → Phase 5
- Common Table Expressions (CTEs) → Phase 5
- Recursive queries → Phase 5
- Full-text search syntax → Extension
- JSON path queries → Extension

## 🏁 Definition of Done

This PR is complete when:
1. All code implemented
2. All tests pass
3. SQL reference documented
4. Examples provided
5. CI/CD passes
6. Code reviewed
7. No known bugs

---

**Ready to implement?** Parse away! 🚀
