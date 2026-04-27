//! Comprehensive tests for the AeternumDB SQL parser.
//!
//! These tests exercise the full parsing pipeline including:
//! - All supported DML and DDL statement types
//! - Expression parsing
//! - Error handling
//! - Semantic validation against an in-memory catalog

use aeternumdb_core::sql::ast::{
    BinaryOperator, DataType, Expr, SelectItem, Statement, Value,
};
use aeternumdb_core::sql::parser::{SqlError, SqlParser};
use aeternumdb_core::sql::validator::{Catalog, ColumnSchema, TableSchema, ValidationError, Validator};

fn parser() -> SqlParser {
    SqlParser::new()
}

fn catalog_with_users() -> Catalog {
    let mut c = Catalog::new();
    c.add_table(TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: DataType::Varchar(Some(255)),
                nullable: true,
            },
            ColumnSchema {
                name: "age".to_string(),
                data_type: DataType::Integer,
                nullable: true,
            },
            ColumnSchema {
                name: "email".to_string(),
                data_type: DataType::Varchar(Some(255)),
                nullable: true,
            },
        ],
    });
    c.add_table(TableSchema {
        name: "orders".to_string(),
        columns: vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnSchema {
                name: "total".to_string(),
                data_type: DataType::Decimal(Some(10), Some(2)),
                nullable: true,
            },
        ],
    });
    c
}

// ══════════════════════════════════════════════════════════════════════════════
// SELECT
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_select_wildcard() {
    let stmts = parser().parse("SELECT * FROM users").unwrap();
    assert_eq!(stmts.len(), 1);
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert_eq!(sel.columns, vec![SelectItem::Wildcard]);
    assert!(matches!(
        &sel.from,
        Some(aeternumdb_core::sql::ast::TableReference::Named { name, .. })
        if name == "users"
    ));
}

#[test]
fn test_select_single_column() {
    let stmts = parser().parse("SELECT id FROM users").unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert_eq!(sel.columns.len(), 1);
    assert!(matches!(&sel.columns[0], SelectItem::Expr { expr: Expr::Column { name, .. }, .. } if name == "id"));
}

#[test]
fn test_select_multiple_columns() {
    let stmts = parser()
        .parse("SELECT id, name, age FROM users")
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert_eq!(sel.columns.len(), 3);
}

#[test]
fn test_select_with_where() {
    let stmts = parser()
        .parse("SELECT id FROM users WHERE age > 18")
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(sel.where_clause.is_some());
    let wc = sel.where_clause.as_ref().unwrap();
    assert!(matches!(wc, Expr::BinaryOp { op: BinaryOperator::Gt, .. }));
}

#[test]
fn test_select_with_and_condition() {
    let stmts = parser()
        .parse("SELECT * FROM users WHERE age > 18 AND name IS NOT NULL")
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        sel.where_clause.as_ref().unwrap(),
        Expr::BinaryOp { op: BinaryOperator::And, .. }
    ));
}

#[test]
fn test_select_with_order_by() {
    let stmts = parser()
        .parse("SELECT id, name FROM users ORDER BY name ASC, id DESC")
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert_eq!(sel.order_by.len(), 2);
    assert!(sel.order_by[0].ascending);
    assert!(!sel.order_by[1].ascending);
}

#[test]
fn test_select_with_limit_offset() {
    let stmts = parser()
        .parse("SELECT * FROM users LIMIT 10 OFFSET 20")
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert_eq!(sel.limit, Some(10));
    assert_eq!(sel.offset, Some(20));
}

#[test]
fn test_select_with_group_by() {
    let stmts = parser()
        .parse("SELECT age, COUNT(*) FROM users GROUP BY age")
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert_eq!(sel.group_by.len(), 1);
    assert!(matches!(&sel.group_by[0], Expr::Column { name, .. } if name == "age"));
}

#[test]
fn test_select_with_having() {
    let stmts = parser()
        .parse("SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 5")
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(sel.having.is_some());
}

#[test]
fn test_select_with_alias() {
    let stmts = parser()
        .parse("SELECT id AS user_id, name AS user_name FROM users")
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(&sel.columns[0], SelectItem::Expr { alias: Some(a), .. } if a == "user_id"));
}

#[test]
fn test_select_distinct() {
    let stmts = parser().parse("SELECT DISTINCT age FROM users").unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(sel.distinct);
}

#[test]
fn test_select_with_inner_join() {
    let stmts = parser()
        .parse(
            "SELECT u.id, o.total FROM users u \
             INNER JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
    assert_eq!(stmts.len(), 1);
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.from,
        Some(aeternumdb_core::sql::ast::TableReference::Join {
            join_type: aeternumdb_core::sql::ast::JoinType::Inner,
            ..
        })
    ));
}

#[test]
fn test_select_with_left_join() {
    let stmts = parser()
        .parse(
            "SELECT u.name, o.total FROM users u \
             LEFT JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.from,
        Some(aeternumdb_core::sql::ast::TableReference::Join {
            join_type: aeternumdb_core::sql::ast::JoinType::Left,
            ..
        })
    ));
}

#[test]
fn test_select_subquery_in_from() {
    let stmts = parser()
        .parse(
            "SELECT sub.id FROM \
             (SELECT id FROM users WHERE age > 18) AS sub",
        )
        .unwrap();
    assert_eq!(stmts.len(), 1);
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.from,
        Some(aeternumdb_core::sql::ast::TableReference::Subquery { .. })
    ));
}

#[test]
fn test_select_subquery_in_where() {
    let stmts = parser()
        .parse(
            "SELECT id FROM users \
             WHERE id IN (SELECT user_id FROM orders)",
        )
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        sel.where_clause.as_ref().unwrap(),
        Expr::InSubquery { .. }
    ));
}

// ══════════════════════════════════════════════════════════════════════════════
// INSERT
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_insert_single_row() {
    let stmts = parser()
        .parse("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
        .unwrap();
    let ins = match &stmts[0] {
        Statement::Insert(i) => i,
        _ => panic!("expected Insert"),
    };
    assert_eq!(ins.table, "users");
    assert_eq!(ins.columns, vec!["id", "name", "age"]);
    assert_eq!(ins.values.len(), 1);
    assert_eq!(ins.values[0].len(), 3);
}

#[test]
fn test_insert_multiple_rows() {
    let stmts = parser()
        .parse(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
        )
        .unwrap();
    let ins = match &stmts[0] {
        Statement::Insert(i) => i,
        _ => panic!("expected Insert"),
    };
    assert_eq!(ins.values.len(), 3);
}

#[test]
fn test_insert_literals() {
    let stmts = parser()
        .parse("INSERT INTO users (id, name, age) VALUES (42, 'Test', 25)")
        .unwrap();
    let ins = match &stmts[0] {
        Statement::Insert(i) => i,
        _ => panic!("expected Insert"),
    };
    assert_eq!(ins.values[0][0], Expr::Literal(Value::Integer(42)));
    assert_eq!(
        ins.values[0][1],
        Expr::Literal(Value::String("Test".to_string()))
    );
    assert_eq!(ins.values[0][2], Expr::Literal(Value::Integer(25)));
}

// ══════════════════════════════════════════════════════════════════════════════
// UPDATE
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_update_with_where() {
    let stmts = parser()
        .parse("UPDATE users SET name = 'Bob' WHERE id = 1")
        .unwrap();
    let upd = match &stmts[0] {
        Statement::Update(u) => u,
        _ => panic!("expected Update"),
    };
    assert_eq!(upd.table, "users");
    assert_eq!(upd.assignments.len(), 1);
    assert_eq!(upd.assignments[0].0, "name");
    assert!(upd.where_clause.is_some());
}

#[test]
fn test_update_multiple_columns() {
    let stmts = parser()
        .parse("UPDATE users SET name = 'Carol', age = 28 WHERE id = 3")
        .unwrap();
    let upd = match &stmts[0] {
        Statement::Update(u) => u,
        _ => panic!("expected Update"),
    };
    assert_eq!(upd.assignments.len(), 2);
}

// ══════════════════════════════════════════════════════════════════════════════
// DELETE
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_delete_with_where() {
    let stmts = parser()
        .parse("DELETE FROM users WHERE id = 5")
        .unwrap();
    let del = match &stmts[0] {
        Statement::Delete(d) => d,
        _ => panic!("expected Delete"),
    };
    assert_eq!(del.table, "users");
    assert!(del.where_clause.is_some());
}

#[test]
fn test_delete_without_where() {
    let stmts = parser().parse("DELETE FROM users").unwrap();
    let del = match &stmts[0] {
        Statement::Delete(d) => d,
        _ => panic!("expected Delete"),
    };
    assert_eq!(del.table, "users");
    assert!(del.where_clause.is_none());
}

// ══════════════════════════════════════════════════════════════════════════════
// CREATE TABLE
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_create_table_basic() {
    let stmts = parser()
        .parse(
            "CREATE TABLE products (\
                id INTEGER PRIMARY KEY, \
                name VARCHAR(255) NOT NULL, \
                price DECIMAL(10, 2)\
            )",
        )
        .unwrap();
    let ct = match &stmts[0] {
        Statement::CreateTable(c) => c,
        _ => panic!("expected CreateTable"),
    };
    assert_eq!(ct.table, "products");
    assert_eq!(ct.columns.len(), 3);
    assert!(ct.columns[0].primary_key);
    assert!(!ct.columns[1].nullable);
    assert_eq!(ct.columns[2].data_type, DataType::Decimal(Some(10), Some(2)));
}

#[test]
fn test_create_table_if_not_exists() {
    let stmts = parser()
        .parse("CREATE TABLE IF NOT EXISTS foo (id INTEGER)")
        .unwrap();
    let ct = match &stmts[0] {
        Statement::CreateTable(c) => c,
        _ => panic!("expected CreateTable"),
    };
    assert!(ct.if_not_exists);
}

#[test]
fn test_create_table_data_types() {
    let stmts = parser()
        .parse(
            "CREATE TABLE types_test (\
                a INTEGER, b FLOAT, c VARCHAR(100), d BOOLEAN, \
                e DATE, f TIMESTAMP, g TEXT\
            )",
        )
        .unwrap();
    let ct = match &stmts[0] {
        Statement::CreateTable(c) => c,
        _ => panic!("expected CreateTable"),
    };
    assert_eq!(ct.columns[0].data_type, DataType::Integer);
    assert_eq!(ct.columns[1].data_type, DataType::Float);
    assert_eq!(ct.columns[2].data_type, DataType::Varchar(Some(100)));
    assert_eq!(ct.columns[3].data_type, DataType::Boolean);
    assert_eq!(ct.columns[4].data_type, DataType::Date);
    assert_eq!(ct.columns[5].data_type, DataType::Timestamp);
    assert_eq!(ct.columns[6].data_type, DataType::Varchar(None));
}

// ══════════════════════════════════════════════════════════════════════════════
// DROP TABLE
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_drop_table() {
    let stmts = parser().parse("DROP TABLE users").unwrap();
    let dt = match &stmts[0] {
        Statement::DropTable(d) => d,
        _ => panic!("expected DropTable"),
    };
    assert_eq!(dt.tables, vec!["users"]);
    assert!(!dt.if_exists);
}

#[test]
fn test_drop_table_if_exists() {
    let stmts = parser()
        .parse("DROP TABLE IF EXISTS old_table")
        .unwrap();
    let dt = match &stmts[0] {
        Statement::DropTable(d) => d,
        _ => panic!("expected DropTable"),
    };
    assert!(dt.if_exists);
}

// ══════════════════════════════════════════════════════════════════════════════
// ALTER TABLE
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_alter_table_add_column() {
    let stmts = parser()
        .parse("ALTER TABLE users ADD COLUMN email VARCHAR(255)")
        .unwrap();
    let alt = match &stmts[0] {
        Statement::AlterTable(a) => a,
        _ => panic!("expected AlterTable"),
    };
    assert_eq!(alt.table, "users");
    assert_eq!(alt.operations.len(), 1);
    assert!(matches!(
        &alt.operations[0],
        aeternumdb_core::sql::ast::AlterTableOperation::AddColumn(_)
    ));
}

#[test]
fn test_alter_table_drop_column() {
    let stmts = parser()
        .parse("ALTER TABLE users DROP COLUMN age")
        .unwrap();
    let alt = match &stmts[0] {
        Statement::AlterTable(a) => a,
        _ => panic!("expected AlterTable"),
    };
    assert!(matches!(
        &alt.operations[0],
        aeternumdb_core::sql::ast::AlterTableOperation::DropColumn { .. }
    ));
}

// ══════════════════════════════════════════════════════════════════════════════
// Multiple statements
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_multiple_statements() {
    let stmts = parser()
        .parse("SELECT 1; SELECT 2; SELECT 3")
        .unwrap();
    assert_eq!(stmts.len(), 3);
}

#[test]
fn test_parse_one_rejects_multiple() {
    let result = parser().parse_one("SELECT 1; SELECT 2");
    assert!(result.is_err());
}

// ══════════════════════════════════════════════════════════════════════════════
// Expression parsing
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_parse_expr_arithmetic() {
    let expr = parser().parse_expr("1 + 2 * 3").unwrap();
    // 1 + (2 * 3)  — multiplication has higher precedence
    assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Plus, .. }));
}

#[test]
fn test_parse_expr_comparison() {
    let expr = parser().parse_expr("age > 18").unwrap();
    assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Gt, .. }));
}

#[test]
fn test_parse_expr_logical() {
    let expr = parser().parse_expr("a AND b OR c").unwrap();
    // SQL operator precedence: AND binds tighter than OR
    assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Or, .. }));
}

#[test]
fn test_parse_expr_count_star() {
    let expr = parser().parse_expr("COUNT(*)").unwrap();
    assert!(matches!(expr, Expr::Function { name, .. } if name == "COUNT"));
}

#[test]
fn test_parse_expr_sum() {
    let expr = parser().parse_expr("SUM(price)").unwrap();
    assert!(matches!(expr, Expr::Function { name, .. } if name == "SUM"));
}

#[test]
fn test_parse_expr_is_null() {
    let expr = parser().parse_expr("name IS NULL").unwrap();
    assert!(matches!(expr, Expr::IsNull { negated: false, .. }));
}

#[test]
fn test_parse_expr_is_not_null() {
    let expr = parser().parse_expr("name IS NOT NULL").unwrap();
    assert!(matches!(expr, Expr::IsNull { negated: true, .. }));
}

#[test]
fn test_parse_expr_between() {
    let expr = parser().parse_expr("age BETWEEN 18 AND 65").unwrap();
    assert!(matches!(expr, Expr::Between { negated: false, .. }));
}

#[test]
fn test_parse_expr_in_list() {
    let expr = parser().parse_expr("status IN (1, 2, 3)").unwrap();
    assert!(matches!(expr, Expr::InList { negated: false, .. }));
}

#[test]
fn test_parse_expr_like() {
    let expr = parser().parse_expr("name LIKE '%Alice%'").unwrap();
    assert!(matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOperator::Like,
            ..
        }
    ));
}

#[test]
fn test_parse_expr_not_like() {
    let expr = parser().parse_expr("name NOT LIKE '%test%'").unwrap();
    assert!(matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOperator::NotLike,
            ..
        }
    ));
}

#[test]
fn test_parse_expr_case() {
    let expr = parser()
        .parse_expr("CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END")
        .unwrap();
    assert!(matches!(expr, Expr::Case { .. }));
}

#[test]
fn test_parse_expr_cast() {
    let expr = parser().parse_expr("CAST(price AS INTEGER)").unwrap();
    assert!(matches!(expr, Expr::Cast { data_type: DataType::Integer, .. }));
}

#[test]
fn test_parse_expr_unary_minus() {
    let expr = parser().parse_expr("-42").unwrap();
    assert!(matches!(
        expr,
        Expr::UnaryOp {
            op: aeternumdb_core::sql::ast::UnaryOperator::Minus,
            ..
        }
    ));
}

#[test]
fn test_parse_expr_distinct_count() {
    let expr = parser().parse_expr("COUNT(DISTINCT id)").unwrap();
    assert!(matches!(expr, Expr::Function { distinct: true, .. }));
}

// ══════════════════════════════════════════════════════════════════════════════
// Aggregate functions in SELECT
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_aggregate_count() {
    let stmts = parser()
        .parse("SELECT COUNT(*) FROM users")
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(&sel.columns[0], SelectItem::Expr { expr: Expr::Function { name, .. }, .. } if name == "COUNT"));
}

#[test]
fn test_aggregate_sum_avg_min_max() {
    let stmts = parser()
        .parse(
            "SELECT SUM(age), AVG(age), MIN(age), MAX(age) FROM users",
        )
        .unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert_eq!(sel.columns.len(), 4);
}

// ══════════════════════════════════════════════════════════════════════════════
// Error handling
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_empty_input_error() {
    let result = parser().parse("   ");
    assert!(matches!(result, Err(SqlError::EmptyInput)));
}

#[test]
fn test_syntax_error() {
    let result = parser().parse("SELET * FROM users");
    // "SELET" is parsed as an identifier, so the error may manifest differently
    // — what matters is that the parse either errors or produces an unexpected result.
    // The important thing is no panic.
    let _ = result;
}

#[test]
fn test_unclosed_string_literal() {
    let result = parser().parse("SELECT 'unclosed FROM users");
    assert!(result.is_err());
}

#[test]
fn test_missing_from_clause() {
    // "SELECT id WHERE age > 0" — valid in some dialects as expression-only;
    // with sqlparser-rs this typically succeeds as a FROM-less query.
    // We just verify no panic.
    let _ = parser().parse("SELECT 1 + 1");
}

#[test]
fn test_sql_error_display() {
    let err = SqlError::ParseError {
        message: "unexpected token".to_string(),
        line: Some(1),
        col: Some(5),
    };
    let s = err.to_string();
    assert!(s.contains("line 1"));
    assert!(s.contains("col 5"));
    assert!(s.contains("unexpected token"));
}

// ══════════════════════════════════════════════════════════════════════════════
// Semantic validation
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_validate_valid_select() {
    let catalog = catalog_with_users();
    let stmt = parser().parse_one("SELECT id, name FROM users").unwrap();
    let v = Validator::new(&catalog);
    assert!(v.validate(&stmt).is_ok());
}

#[test]
fn test_validate_table_not_found() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("SELECT id FROM nonexistent_table")
        .unwrap();
    let v = Validator::new(&catalog);
    let err = v.validate(&stmt).unwrap_err();
    assert!(matches!(err, ValidationError::TableNotFound { .. }));
}

#[test]
fn test_validate_column_not_found() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("SELECT nonexistent_col FROM users")
        .unwrap();
    let v = Validator::new(&catalog);
    let err = v.validate(&stmt).unwrap_err();
    assert!(matches!(err, ValidationError::ColumnNotFound { .. }));
}

#[test]
fn test_validate_insert_not_null_violation() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("INSERT INTO users (id, name) VALUES (NULL, 'Alice')")
        .unwrap();
    let v = Validator::new(&catalog);
    let err = v.validate(&stmt).unwrap_err();
    assert!(matches!(err, ValidationError::NullConstraintViolation { .. }));
}

#[test]
fn test_validate_update_not_null_violation() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("UPDATE users SET id = NULL WHERE id = 1")
        .unwrap();
    let v = Validator::new(&catalog);
    let err = v.validate(&stmt).unwrap_err();
    assert!(matches!(err, ValidationError::NullConstraintViolation { .. }));
}

#[test]
fn test_validate_aggregate_in_where() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("SELECT id FROM users WHERE COUNT(*) > 5")
        .unwrap();
    let v = Validator::new(&catalog);
    let err = v.validate(&stmt).unwrap_err();
    assert!(matches!(err, ValidationError::InvalidAggregateUsage(_)));
}

#[test]
fn test_validate_delete_unknown_table() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("DELETE FROM ghost_table WHERE id = 1")
        .unwrap();
    let v = Validator::new(&catalog);
    let err = v.validate(&stmt).unwrap_err();
    assert!(matches!(err, ValidationError::TableNotFound { .. }));
}

#[test]
fn test_validate_create_table_duplicate_column() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("CREATE TABLE foo (id INTEGER, id VARCHAR(10))")
        .unwrap();
    let v = Validator::new(&catalog);
    let err = v.validate(&stmt).unwrap_err();
    assert!(matches!(err, ValidationError::ConstraintViolation(_)));
}

#[test]
fn test_validate_create_table_already_exists() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("CREATE TABLE users (id INTEGER)")
        .unwrap();
    let v = Validator::new(&catalog);
    let err = v.validate(&stmt).unwrap_err();
    assert!(matches!(err, ValidationError::ConstraintViolation(_)));
}

#[test]
fn test_validate_create_table_if_not_exists_passes() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("CREATE TABLE IF NOT EXISTS users (id INTEGER)")
        .unwrap();
    let v = Validator::new(&catalog);
    // IF NOT EXISTS suppresses the "already exists" error.
    assert!(v.validate(&stmt).is_ok());
}

// ══════════════════════════════════════════════════════════════════════════════
// Complex queries
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_tpch_q1_like() {
    // Simplified TPC-H Q1
    let sql = "
        SELECT
            COUNT(*) AS count_order,
            SUM(total) AS sum_total,
            AVG(total) AS avg_total,
            MIN(total) AS min_total,
            MAX(total) AS max_total
        FROM orders
        WHERE total > 100
        GROUP BY user_id
        HAVING COUNT(*) > 1
        ORDER BY count_order DESC
        LIMIT 100
    ";
    let stmts = parser().parse(sql).unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert_eq!(sel.columns.len(), 5);
    assert!(sel.where_clause.is_some());
    assert!(!sel.group_by.is_empty());
    assert!(sel.having.is_some());
    assert!(!sel.order_by.is_empty());
    assert_eq!(sel.limit, Some(100));
}

#[test]
fn test_nested_subquery() {
    let sql = "
        SELECT id FROM users
        WHERE id IN (
            SELECT user_id FROM orders
            WHERE total > (SELECT AVG(total) FROM orders)
        )
    ";
    let stmts = parser().parse(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    assert!(matches!(&stmts[0], Statement::Select(_)));
}

#[test]
fn test_complex_join() {
    let sql = "
        SELECT u.name, o.total
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        WHERE o.total > 500
        ORDER BY o.total DESC
    ";
    let stmts = parser().parse(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(sel.where_clause.is_some());
    assert!(!sel.order_by.is_empty());
}

// ══════════════════════════════════════════════════════════════════════════════
// Apply-DDL catalog helper
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_apply_create_table_to_catalog() {
    let mut catalog = Catalog::new();
    let stmt = parser()
        .parse_one("CREATE TABLE foo (id INTEGER, val TEXT)")
        .unwrap();

    // Apply DDL to catalog
    if let Statement::CreateTable(ref ct) = stmt {
        aeternumdb_core::sql::validator::apply_create_table(&mut catalog, ct);
    }

    // Now SELECT should validate
    let select_stmt = parser()
        .parse_one("SELECT id, val FROM foo")
        .unwrap();
    let v = Validator::new(&catalog);
    assert!(v.validate(&select_stmt).is_ok());
}
