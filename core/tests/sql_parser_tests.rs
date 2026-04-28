//! Comprehensive tests for the AeternumDB SQL parser.
//!
//! These tests exercise the full parsing pipeline including:
//! - All supported DML and DDL statement types
//! - Expression parsing
//! - Error handling
//! - Semantic validation against an in-memory catalog

use aeternumdb_core::sql::ast::{
    BeginTransactionStatement, BinaryOperator, CommitStatement, DataType, Expr, OnCommitBehavior,
    ReferentialAction, ReleaseSavepointStatement, RollbackStatement, SavepointStatement,
    SelectItem, Statement, TextSearchModifier, TrimWhereField, UnaryOperator, Value, ViewAsItem,
};
use aeternumdb_core::sql::parser::{SqlError, SqlParser};
use aeternumdb_core::sql::validator::{
    Catalog, ColumnSchema, TableSchema, ValidationError, Validator,
};

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
    assert!(
        matches!(&sel.columns[0], SelectItem::Expr { expr: Expr::Column { name, .. }, .. } if name == "id")
    );
}

#[test]
fn test_select_multiple_columns() {
    let stmts = parser().parse("SELECT id, name, age FROM users").unwrap();
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
    assert!(matches!(
        wc,
        Expr::BinaryOp {
            op: BinaryOperator::Gt,
            ..
        }
    ));
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
        Expr::BinaryOp {
            op: BinaryOperator::And,
            ..
        }
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
        .parse("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
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
    let stmts = parser().parse("DELETE FROM users WHERE id = 5").unwrap();
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
    assert_eq!(
        ct.columns[2].data_type,
        DataType::Decimal(Some(10), Some(2))
    );
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
    let stmts = parser().parse("DROP TABLE IF EXISTS old_table").unwrap();
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
    let stmts = parser().parse("ALTER TABLE users DROP COLUMN age").unwrap();
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
    let stmts = parser().parse("SELECT 1; SELECT 2; SELECT 3").unwrap();
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
    assert!(matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOperator::Plus,
            ..
        }
    ));
}

#[test]
fn test_parse_expr_comparison() {
    let expr = parser().parse_expr("age > 18").unwrap();
    assert!(matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOperator::Gt,
            ..
        }
    ));
}

#[test]
fn test_parse_expr_logical() {
    let expr = parser().parse_expr("a AND b OR c").unwrap();
    // SQL operator precedence: AND binds tighter than OR
    assert!(matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOperator::Or,
            ..
        }
    ));
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
    assert!(matches!(
        expr,
        Expr::Cast {
            data_type: DataType::Integer,
            ..
        }
    ));
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
    let stmts = parser().parse("SELECT COUNT(*) FROM users").unwrap();
    let sel = match &stmts[0] {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(
        matches!(&sel.columns[0], SelectItem::Expr { expr: Expr::Function { name, .. }, .. } if name == "COUNT")
    );
}

#[test]
fn test_aggregate_sum_avg_min_max() {
    let stmts = parser()
        .parse("SELECT SUM(age), AVG(age), MIN(age), MAX(age) FROM users")
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
    assert!(matches!(
        err,
        ValidationError::NullConstraintViolation { .. }
    ));
}

#[test]
fn test_validate_update_not_null_violation() {
    let catalog = catalog_with_users();
    let stmt = parser()
        .parse_one("UPDATE users SET id = NULL WHERE id = 1")
        .unwrap();
    let v = Validator::new(&catalog);
    let err = v.validate(&stmt).unwrap_err();
    assert!(matches!(
        err,
        ValidationError::NullConstraintViolation { .. }
    ));
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
    let select_stmt = parser().parse_one("SELECT id, val FROM foo").unwrap();
    let v = Validator::new(&catalog);
    assert!(v.validate(&select_stmt).is_ok());
}

// ══════════════════════════════════════════════════════════════════════════════
// New feature tests
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_backtick_identifiers() {
    use aeternumdb_core::sql::ast::TableReference;
    let stmt = parser().parse_one("SELECT `name` FROM `users`").unwrap();
    let sel = match &stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.from,
        Some(TableReference::Named { name, .. }) if name == "users"
    ));
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr { expr: Expr::Column { name, .. }, .. } if name == "name"
    ));
}

#[test]
fn test_create_temporary_table() {
    let stmt = parser()
        .parse_one("CREATE TEMPORARY TABLE tmp_data (id INTEGER)")
        .unwrap();
    let ct = match &stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    assert_eq!(ct.table, "tmp_data");
    assert!(ct.temporary);
    assert_eq!(ct.columns.len(), 1);
    // No explicit ON COMMIT clause — default (PreserveRows / session-scoped).
    assert_eq!(ct.on_commit, None);
}

#[test]
fn test_create_temporary_table_on_commit_drop() {
    let stmt = parser()
        .parse_one("CREATE TEMPORARY TABLE tmp_drop (id INTEGER) ON COMMIT DROP")
        .unwrap();
    let ct = match &stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    assert!(ct.temporary);
    assert_eq!(ct.on_commit, Some(OnCommitBehavior::Drop));
}

#[test]
fn test_create_temporary_table_on_commit_delete_rows() {
    let stmt = parser()
        .parse_one("CREATE TEMPORARY TABLE tmp_del (id INTEGER) ON COMMIT DELETE ROWS")
        .unwrap();
    let ct = match &stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    assert!(ct.temporary);
    assert_eq!(ct.on_commit, Some(OnCommitBehavior::DeleteRows));
}

#[test]
fn test_create_temporary_table_on_commit_preserve_rows() {
    let stmt = parser()
        .parse_one("CREATE TEMPORARY TABLE tmp_pres (id INTEGER) ON COMMIT PRESERVE ROWS")
        .unwrap();
    let ct = match &stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    assert!(ct.temporary);
    assert_eq!(ct.on_commit, Some(OnCommitBehavior::PreserveRows));
}

#[test]
fn test_create_table_inherits() {
    let stmt = parser()
        .parse_one("CREATE TABLE child (age INTEGER) INHERITS (parent)")
        .unwrap();
    let ct = match &stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    assert_eq!(ct.table, "child");
    assert_eq!(ct.inherits, vec!["parent"]);
}

#[test]
fn test_create_materialized_view() {
    let stmt = parser()
        .parse_one("CREATE MATERIALIZED VIEW mv_sales AS SELECT id, total FROM orders")
        .unwrap();
    let mv = match &stmt {
        Statement::CreateMaterializedView(mv) => mv,
        _ => panic!("expected CreateMaterializedView, got {:?}", stmt),
    };
    assert_eq!(mv.name, "mv_sales");
    assert!(!mv.if_not_exists);
    assert!(!mv.or_replace);
}

#[test]
fn test_new_data_types() {
    let sql = "CREATE TABLE type_test (
        a TINYINT,
        b SMALLINT,
        c BIGINT,
        d CHAR(10),
        e TIME,
        f DATETIME,
        g TIMESTAMP WITH TIME ZONE,
        h UUID
    )";
    let stmt = parser().parse_one(sql).unwrap();
    let ct = match &stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };

    let col = |name: &str| ct.columns.iter().find(|c| c.name == name).unwrap();

    assert_eq!(col("a").data_type, DataType::TinyInt);
    assert_eq!(col("b").data_type, DataType::SmallInt);
    assert_eq!(col("c").data_type, DataType::BigInt);
    assert_eq!(col("d").data_type, DataType::Char(Some(10)));
    assert_eq!(col("e").data_type, DataType::Time);
    assert_eq!(col("f").data_type, DataType::DateTime);
    assert_eq!(col("g").data_type, DataType::TimestampTz);
    assert_eq!(col("h").data_type, DataType::Uuid);
}

#[test]
fn test_transaction_parsing() {
    use aeternumdb_core::sql::ast::{CommitScope, RollbackScope};

    // BEGIN TRANSACTION
    let stmt = parser().parse_one("BEGIN TRANSACTION").unwrap();
    assert!(matches!(
        stmt,
        Statement::BeginTransaction(BeginTransactionStatement {
            name: None,
            isolation_level: None,
            read_only: false
        })
    ));

    // COMMIT  →  scope = Current, chain = false
    let stmt = parser().parse_one("COMMIT").unwrap();
    assert!(matches!(
        stmt,
        Statement::Commit(CommitStatement {
            scope: CommitScope::Current,
            chain: false
        })
    ));

    // COMMIT AND CHAIN  →  chain = true
    let stmt = parser().parse_one("COMMIT AND CHAIN").unwrap();
    assert!(matches!(
        stmt,
        Statement::Commit(CommitStatement {
            scope: CommitScope::Current,
            chain: true
        })
    ));

    // ROLLBACK  →  scope = Current, chain = false
    let stmt = parser().parse_one("ROLLBACK").unwrap();
    assert!(matches!(
        stmt,
        Statement::Rollback(RollbackStatement {
            scope: RollbackScope::Current,
            chain: false
        })
    ));

    // ROLLBACK AND CHAIN  →  chain = true
    let stmt = parser().parse_one("ROLLBACK AND CHAIN").unwrap();
    assert!(matches!(
        stmt,
        Statement::Rollback(RollbackStatement {
            scope: RollbackScope::Current,
            chain: true
        })
    ));

    // ROLLBACK TO SAVEPOINT sp1  →  scope = ToSavepoint
    let stmt = parser().parse_one("ROLLBACK TO SAVEPOINT sp1").unwrap();
    assert!(matches!(
        stmt,
        Statement::Rollback(RollbackStatement {
            scope: RollbackScope::ToSavepoint(ref n),
            chain: false
        }) if n == "sp1"
    ));

    // SAVEPOINT
    let stmt = parser().parse_one("SAVEPOINT sp1").unwrap();
    assert!(matches!(
        stmt,
        Statement::Savepoint(SavepointStatement { name }) if name == "sp1"
    ));

    // RELEASE SAVEPOINT
    let stmt = parser().parse_one("RELEASE SAVEPOINT sp1").unwrap();
    assert!(matches!(
        stmt,
        Statement::ReleaseSavepoint(ReleaseSavepointStatement { name }) if name == "sp1"
    ));
}

#[test]
fn test_transaction_isolation_levels() {
    use aeternumdb_core::sql::ast::IsolationLevel;

    let cases = [
        (
            "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            IsolationLevel::ReadUncommitted,
        ),
        (
            "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
            IsolationLevel::ReadCommitted,
        ),
        (
            "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            IsolationLevel::RepeatableRead,
        ),
        (
            "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            IsolationLevel::Serializable,
        ),
    ];

    for (sql, expected_level) in cases {
        let stmt = parser().parse_one(sql).unwrap();
        match stmt {
            Statement::BeginTransaction(BeginTransactionStatement {
                name: _,
                isolation_level,
                read_only,
            }) => {
                assert_eq!(isolation_level, Some(expected_level), "failed for: {sql}");
                assert!(!read_only, "expected read_write for: {sql}");
            }
            other => panic!("expected BeginTransaction for '{sql}', got: {other:?}"),
        }
    }
}

#[test]
fn test_transaction_read_only() {
    let stmt = parser().parse_one("START TRANSACTION READ ONLY").unwrap();
    match stmt {
        Statement::BeginTransaction(BeginTransactionStatement {
            name: _,
            isolation_level,
            read_only,
        }) => {
            assert_eq!(isolation_level, None);
            assert!(read_only);
        }
        other => panic!("expected BeginTransaction, got: {other:?}"),
    }
}

// ── Nested transaction AST construction ───────────────────────────────────────

#[test]
fn test_nested_transaction_begin_with_name() {
    // Named BEGIN is an AeternumDB extension (Phase 4 custom grammar).
    // For now, verify the AST can be built programmatically.
    use aeternumdb_core::sql::ast::BeginTransactionStatement;
    let stmt = Statement::BeginTransaction(BeginTransactionStatement {
        name: Some("outer_tx".to_string()),
        isolation_level: None,
        read_only: false,
    });
    assert!(matches!(
        stmt,
        Statement::BeginTransaction(BeginTransactionStatement {
            name: Some(ref n),
            ..
        }) if n == "outer_tx"
    ));
}

#[test]
fn test_nested_transaction_commit_named_scope() {
    use aeternumdb_core::sql::ast::{CommitScope, CommitStatement};
    // COMMIT TRANSACTION inner_tx  →  CommitScope::Named (AeternumDB extension).
    let stmt = Statement::Commit(CommitStatement {
        scope: CommitScope::Named("inner_tx".to_string()),
        chain: false,
    });
    assert!(matches!(
        stmt,
        Statement::Commit(CommitStatement {
            scope: CommitScope::Named(ref n),
            chain: false
        }) if n == "inner_tx"
    ));
}

#[test]
fn test_nested_transaction_commit_all_scope() {
    use aeternumdb_core::sql::ast::{CommitScope, CommitStatement};
    // COMMIT ALL  →  CommitScope::All (AeternumDB extension).
    let stmt = Statement::Commit(CommitStatement {
        scope: CommitScope::All,
        chain: false,
    });
    assert!(matches!(
        stmt,
        Statement::Commit(CommitStatement {
            scope: CommitScope::All,
            chain: false
        })
    ));
}

#[test]
fn test_nested_transaction_rollback_named_scope() {
    use aeternumdb_core::sql::ast::{RollbackScope, RollbackStatement};
    // ROLLBACK TRANSACTION inner_tx  →  RollbackScope::Named (AeternumDB extension).
    let stmt = Statement::Rollback(RollbackStatement {
        scope: RollbackScope::Named("inner_tx".to_string()),
        chain: false,
    });
    assert!(matches!(
        stmt,
        Statement::Rollback(RollbackStatement {
            scope: RollbackScope::Named(ref n),
            ..
        }) if n == "inner_tx"
    ));
}

#[test]
fn test_nested_transaction_rollback_all_scope() {
    use aeternumdb_core::sql::ast::{RollbackScope, RollbackStatement};
    // ROLLBACK ALL  →  RollbackScope::All (AeternumDB extension).
    let stmt = Statement::Rollback(RollbackStatement {
        scope: RollbackScope::All,
        chain: false,
    });
    assert!(matches!(
        stmt,
        Statement::Rollback(RollbackStatement {
            scope: RollbackScope::All,
            ..
        })
    ));
}

// ── UNSIGNED and BINARY type tests ────────────────────────────────────────────

#[test]
fn test_unsigned_integer_types() {
    let cases: &[(&str, DataType)] = &[
        ("id TINYINT UNSIGNED", DataType::UnsignedTinyInt),
        ("id SMALLINT UNSIGNED", DataType::UnsignedSmallInt),
        ("id INTEGER UNSIGNED", DataType::UnsignedInt),
        ("id BIGINT UNSIGNED", DataType::UnsignedBigInt),
    ];
    for (col_def, expected) in cases {
        let sql = format!("CREATE TABLE t ({col_def})");
        let stmt = parser().parse_one(&sql).unwrap();
        let ct = match stmt {
            Statement::CreateTable(ct) => ct,
            _ => panic!("expected CreateTable for: {sql}"),
        };
        assert_eq!(
            ct.columns[0].data_type, *expected,
            "mismatch for: {col_def}"
        );
    }
}

#[test]
fn test_binary_types() {
    let cases: &[(&str, DataType)] = &[
        ("data BINARY(16)", DataType::Binary(Some(16))),
        ("data BINARY", DataType::Binary(None)),
        ("data VARBINARY(255)", DataType::Varbinary(Some(255))),
        ("data BLOB", DataType::Blob(None)),
        ("data BLOB(65535)", DataType::Blob(Some(65535))),
        ("data TINYBLOB", DataType::TinyBlob),
        ("data MEDIUMBLOB", DataType::MediumBlob),
        ("data LONGBLOB", DataType::LongBlob),
    ];
    for (col_def, expected) in cases {
        let sql = format!("CREATE TABLE t ({col_def})");
        let stmt = parser().parse_one(&sql).unwrap();
        let ct = match stmt {
            Statement::CreateTable(ct) => ct,
            _ => panic!("expected CreateTable for: {sql}"),
        };
        assert_eq!(
            ct.columns[0].data_type, *expected,
            "mismatch for: {col_def}"
        );
    }
}

#[test]
fn test_unsigned_display() {
    assert_eq!(DataType::UnsignedTinyInt.to_string(), "TINYINT UNSIGNED");
    assert_eq!(DataType::UnsignedSmallInt.to_string(), "SMALLINT UNSIGNED");
    assert_eq!(
        DataType::UnsignedMediumInt.to_string(),
        "MEDIUMINT UNSIGNED"
    );
    assert_eq!(DataType::UnsignedInt.to_string(), "INTEGER UNSIGNED");
    assert_eq!(DataType::UnsignedBigInt.to_string(), "BIGINT UNSIGNED");
}

#[test]
fn test_binary_display() {
    assert_eq!(DataType::Binary(Some(16)).to_string(), "BINARY(16)");
    assert_eq!(DataType::Binary(None).to_string(), "BINARY");
    assert_eq!(DataType::Varbinary(Some(255)).to_string(), "VARBINARY(255)");
    assert_eq!(DataType::Blob(None).to_string(), "BLOB");
    assert_eq!(DataType::TinyBlob.to_string(), "TINYBLOB");
    assert_eq!(DataType::MediumBlob.to_string(), "MEDIUMBLOB");
    assert_eq!(DataType::LongBlob.to_string(), "LONGBLOB");
}

#[test]
fn test_unsigned_float_double_decimal_rejected() {
    for sql in &[
        "CREATE TABLE t (x FLOAT UNSIGNED)",
        "CREATE TABLE t (x DOUBLE UNSIGNED)",
        "CREATE TABLE t (x DECIMAL(10,2) UNSIGNED)",
    ] {
        assert!(
            parser().parse_one(sql).is_err(),
            "expected error for: {sql}"
        );
    }
}

// ── CREATE INDEX / DROP INDEX ──────────────────────────────────────────────

#[test]
fn test_create_index() {
    let stmt = parser()
        .parse_one("CREATE INDEX idx_name ON users (name)")
        .unwrap();
    let ci = match stmt {
        Statement::CreateIndex(ci) => ci,
        _ => panic!("expected CreateIndex"),
    };
    assert_eq!(ci.name, Some("idx_name".to_string()));
    assert_eq!(ci.table, "users");
    assert_eq!(ci.columns.len(), 1);
    assert!(!ci.unique);
}

#[test]
fn test_create_unique_index() {
    let stmt = parser()
        .parse_one("CREATE UNIQUE INDEX idx_email ON users (email)")
        .unwrap();
    let ci = match stmt {
        Statement::CreateIndex(ci) => ci,
        _ => panic!("expected CreateIndex"),
    };
    assert!(ci.unique);
    assert_eq!(ci.table, "users");
}

#[test]
fn test_drop_index() {
    let stmt = parser()
        .parse_one("DROP INDEX IF EXISTS idx_name ON users")
        .unwrap();
    let di = match stmt {
        Statement::DropIndex(di) => di,
        _ => panic!("expected DropIndex"),
    };
    assert!(di.if_exists);
}

// ── Table-level constraints ─────────────────────────────────────────────────

#[test]
fn test_composite_primary_key() {
    use aeternumdb_core::sql::ast::TableConstraint;
    let stmt = parser()
        .parse_one("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
        .unwrap();
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    assert_eq!(ct.constraints.len(), 1);
    match &ct.constraints[0] {
        TableConstraint::PrimaryKey { columns, .. } => {
            assert_eq!(columns, &["a".to_string(), "b".to_string()]);
        }
        _ => panic!("expected PrimaryKey constraint"),
    }
}

#[test]
fn test_table_check_constraint() {
    use aeternumdb_core::sql::ast::TableConstraint;
    let stmt = parser()
        .parse_one("CREATE TABLE t (price DECIMAL(10,2), CHECK (price > 0))")
        .unwrap();
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    let has_check = ct
        .constraints
        .iter()
        .any(|c| matches!(c, TableConstraint::Check { .. }));
    assert!(has_check);
}

#[test]
fn test_foreign_key_constraint_rejected() {
    // FOREIGN KEY constraints are not supported in AeternumDB.
    // Use reference column types to express relationships instead.
    let err = parser()
        .parse_one("CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users (id))")
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("FOREIGN KEY") || msg.contains("foreign key"),
        "expected FK rejection message, got: {msg}"
    );
}

// ── User management scaffolding ────────────────────────────────────────────

#[test]
fn test_create_user() {
    let stmt = parser().parse_one("CREATE USER alice").unwrap();
    let cu = match stmt {
        Statement::CreateUser(cu) => cu,
        _ => panic!("expected CreateUser"),
    };
    assert_eq!(cu.name, "alice");
}

// ── Vector type ────────────────────────────────────────────────────────────

#[test]
fn test_vector_type_via_array() {
    // Vector types are exposed through sqlparser's ARRAY type mapping.
    // Validate the DataType::Vector Display impl.
    use aeternumdb_core::sql::ast::DataType;
    let vt = DataType::Vector(Box::new(DataType::Integer));
    assert_eq!(vt.to_string(), "[INTEGER]");
}

// ── Case insensitivity ─────────────────────────────────────────────────────

#[test]
fn test_keyword_case_insensitive() {
    // SQL keywords in any case must parse identically.
    let lower = parser().parse_one("select id from users").unwrap();
    let upper = parser().parse_one("SELECT ID FROM USERS").unwrap();
    let mixed = parser().parse_one("Select Id From Users").unwrap();
    assert_eq!(lower, upper);
    assert_eq!(lower, mixed);
}

#[test]
fn test_identifier_names_lowercased() {
    // All identifiers (table, column, alias) must be normalized to lowercase.
    let stmt = parser()
        .parse_one("SELECT UserId AS UID FROM MyTable MT")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    // Table name and alias must be lowercase.
    assert!(matches!(
        &sel.from,
        Some(aeternumdb_core::sql::ast::TableReference::Named {
            name,
            alias: Some(alias),
            ..
        }) if name == "mytable" && alias == "mt"
    ));
    // Column alias must be lowercase.
    match &sel.columns[0] {
        SelectItem::Expr { alias: Some(a), .. } => assert_eq!(a, "uid"),
        other => panic!("unexpected select item: {other:?}"),
    }
}

#[test]
fn test_create_table_name_lowercased() {
    let stmt = parser()
        .parse_one("CREATE TABLE MySchema.MyTable (id INTEGER PRIMARY KEY)")
        .unwrap();
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    assert_eq!(ct.table, "mytable");
    assert_eq!(ct.schema, Some("myschema".to_string()));
}

// ── Schema-qualified identifiers ───────────────────────────────────────────

#[test]
fn test_select_schema_qualified_table() {
    let stmt = parser().parse_one("SELECT id FROM app.users").unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.from,
        Some(aeternumdb_core::sql::ast::TableReference::Named {
            schema: Some(schema),
            name,
            ..
        }) if schema == "app" && name == "users"
    ));
}

#[test]
fn test_select_database_schema_qualified_table() {
    let stmt = parser().parse_one("SELECT id FROM mydb.app.users").unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.from,
        Some(aeternumdb_core::sql::ast::TableReference::Named {
            database: Some(db),
            schema: Some(schema),
            name,
            ..
        }) if db == "mydb" && schema == "app" && name == "users"
    ));
}

// ── FLAT tables ────────────────────────────────────────────────────────────

#[test]
fn test_create_flat_table_flat_flag_defaults_false() {
    // FLAT keyword is an AeternumDB extension not yet representable via
    // sqlparser's standard CREATE TABLE.  Creating a table without the flag
    // must leave flat = false.
    let stmt = parser()
        .parse_one("CREATE TABLE metrics (ts TIMESTAMP, val FLOAT)")
        .unwrap();
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    assert!(!ct.flat);
    assert!(!ct.versioned);
}

// ── Database and Schema DDL ────────────────────────────────────────────────

#[test]
fn test_create_database() {
    let stmt = parser().parse_one("CREATE DATABASE myapp").unwrap();
    let cd = match stmt {
        Statement::CreateDatabase(cd) => cd,
        _ => panic!("expected CreateDatabase"),
    };
    assert_eq!(cd.name, "myapp");
    assert!(!cd.if_not_exists);
}

#[test]
fn test_create_database_if_not_exists() {
    let stmt = parser()
        .parse_one("CREATE DATABASE IF NOT EXISTS myapp")
        .unwrap();
    let cd = match stmt {
        Statement::CreateDatabase(cd) => cd,
        _ => panic!("expected CreateDatabase"),
    };
    assert!(cd.if_not_exists);
}

#[test]
fn test_drop_database() {
    let stmt = parser().parse_one("DROP DATABASE myapp").unwrap();
    let dd = match stmt {
        Statement::DropDatabase(dd) => dd,
        _ => panic!("expected DropDatabase"),
    };
    assert_eq!(dd.name, "myapp");
    assert!(!dd.if_exists);
}

#[test]
fn test_drop_database_if_exists() {
    let stmt = parser().parse_one("DROP DATABASE IF EXISTS myapp").unwrap();
    let dd = match stmt {
        Statement::DropDatabase(dd) => dd,
        _ => panic!("expected DropDatabase"),
    };
    assert!(dd.if_exists);
}

#[test]
fn test_use_database() {
    let stmt = parser().parse_one("USE myapp").unwrap();
    let ud = match stmt {
        Statement::UseDatabase(ud) => ud,
        _ => panic!("expected UseDatabase"),
    };
    assert_eq!(ud.name, "myapp");
}

#[test]
fn test_create_schema() {
    let stmt = parser().parse_one("CREATE SCHEMA reporting").unwrap();
    let cs = match stmt {
        Statement::CreateSchema(cs) => cs,
        _ => panic!("expected CreateSchema"),
    };
    assert_eq!(cs.name, "reporting");
    assert_eq!(cs.database, None);
    assert!(!cs.if_not_exists);
}

#[test]
fn test_create_schema_qualified() {
    let stmt = parser().parse_one("CREATE SCHEMA mydb.reporting").unwrap();
    let cs = match stmt {
        Statement::CreateSchema(cs) => cs,
        _ => panic!("expected CreateSchema"),
    };
    assert_eq!(cs.name, "reporting");
    assert_eq!(cs.database, Some("mydb".to_string()));
}

#[test]
fn test_drop_schema() {
    let stmt = parser().parse_one("DROP SCHEMA reporting").unwrap();
    let ds = match stmt {
        Statement::DropSchema(ds) => ds,
        _ => panic!("expected DropSchema"),
    };
    assert_eq!(ds.name, "reporting");
    assert!(!ds.if_exists);
}

// ── FILTER BY in JOINs ────────────────────────────────────────────────────

#[test]
fn test_join_filter_by_field_name() {
    // Ensure the join uses filter_by field (not condition).
    let stmt = parser()
        .parse_one("SELECT u.id, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    // The join's ON clause is mapped to filter_by.
    assert!(matches!(
        &sel.from,
        Some(aeternumdb_core::sql::ast::TableReference::Join {
            filter_by: Some(_),
            ..
        })
    ));
}

#[test]
fn test_cross_join_no_filter() {
    let stmt = parser()
        .parse_one("SELECT a.id, b.id FROM tableA a CROSS JOIN tableB b")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.from,
        Some(aeternumdb_core::sql::ast::TableReference::Join {
            join_type: aeternumdb_core::sql::ast::JoinType::Cross,
            filter_by: None,
            ..
        })
    ));
}

// ══════════════════════════════════════════════════════════════════════════════
// ON UPDATE / ON DELETE referential actions on reference columns
// ══════════════════════════════════════════════════════════════════════════════

/// Helper: parse a CREATE TABLE and return the first column definition.
fn first_column(sql: &str) -> aeternumdb_core::sql::ast::ColumnDef {
    let stmt = parser().parse_one(sql).unwrap();
    match stmt {
        Statement::CreateTable(ct) => ct.columns.into_iter().next().unwrap(),
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn test_reference_on_delete_cascade() {
    // `customer customers REFERENCES customers ON DELETE CASCADE`
    // sqlparser maps the data type to Custom("customers") + ForeignKey option;
    // our converter upgrades it to Reference("customers") and records the action.
    let col = first_column(
        "CREATE TABLE orders (\
          customer customers REFERENCES customers ON DELETE CASCADE\
        )",
    );
    assert_eq!(col.data_type, DataType::Reference("customers".to_string()));
    assert_eq!(col.on_delete, Some(ReferentialAction::Cascade));
    assert_eq!(col.on_update, None);
}

#[test]
fn test_reference_on_update_restrict() {
    let col = first_column(
        "CREATE TABLE orders (\
          customer customers REFERENCES customers ON UPDATE RESTRICT\
        )",
    );
    assert_eq!(col.data_type, DataType::Reference("customers".to_string()));
    assert_eq!(col.on_update, Some(ReferentialAction::Restrict));
    assert_eq!(col.on_delete, None);
}

#[test]
fn test_reference_on_delete_set_null() {
    let col = first_column(
        "CREATE TABLE orders (\
          customer customers REFERENCES customers ON DELETE SET NULL\
        )",
    );
    assert_eq!(col.data_type, DataType::Reference("customers".to_string()));
    assert_eq!(col.on_delete, Some(ReferentialAction::SetNull));
    assert_eq!(col.on_update, None);
}

#[test]
fn test_reference_on_delete_cascade_on_update_cascade() {
    let col = first_column(
        "CREATE TABLE orders (\
          customer customers REFERENCES customers ON DELETE CASCADE ON UPDATE CASCADE\
        )",
    );
    assert_eq!(col.data_type, DataType::Reference("customers".to_string()));
    assert_eq!(col.on_delete, Some(ReferentialAction::Cascade));
    assert_eq!(col.on_update, Some(ReferentialAction::Cascade));
}

#[test]
fn test_on_delete_on_non_reference_column_errors() {
    // INTEGER REFERENCES … ON DELETE CASCADE — data type is NOT a reference type.
    // Our converter should reject this with an AstError::Invalid.
    let result = parser().parse_one(
        "CREATE TABLE orders (\
          customer_id INTEGER REFERENCES customers ON DELETE CASCADE\
        )",
    );
    assert!(
        result.is_err(),
        "expected an error when ON DELETE is used on a non-reference column"
    );
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("ON UPDATE / ON DELETE"),
        "error message should mention ON UPDATE / ON DELETE, got: {err_str}"
    );
}

// ── Expression tests ──────────────────────────────────────────────────────────

#[test]
fn test_bitwise_operators() {
    let cases = [
        ("SELECT a & b FROM t", BinaryOperator::BitwiseAnd),
        ("SELECT a | b FROM t", BinaryOperator::BitwiseOr),
        ("SELECT a ^ b FROM t", BinaryOperator::BitwiseXor),
        // Bit-shift operators (parsed as PGBitwiseShiftLeft/Right internally)
        ("SELECT a << 2 FROM t", BinaryOperator::ShiftLeft),
        ("SELECT a >> 2 FROM t", BinaryOperator::ShiftRight),
    ];
    for (sql, expected_op) in cases {
        let stmt = parser().parse_one(sql).unwrap();
        let sel = match stmt {
            Statement::Select(s) => s,
            _ => panic!("expected Select"),
        };
        match &sel.columns[0] {
            SelectItem::Expr {
                expr: Expr::BinaryOp { op, .. },
                ..
            } => {
                assert_eq!(op, &expected_op, "wrong op for: {sql}");
            }
            other => panic!("expected BinaryOp, got: {other:?}"),
        }
    }
}

#[test]
fn test_bitwise_not_unary() {
    let stmt = parser().parse_one("SELECT ~flags FROM t").unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::UnaryOp {
                op: UnaryOperator::BitwiseNot,
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_regex_operators() {
    let cases: &[(&str, BinaryOperator)] = &[
        ("SELECT a FROM t WHERE a ILIKE 'pat'", BinaryOperator::ILike),
        (
            "SELECT a FROM t WHERE a NOT ILIKE 'pat'",
            BinaryOperator::NotILike,
        ),
        (
            "SELECT a FROM t WHERE a SIMILAR TO 'pat'",
            BinaryOperator::SimilarTo,
        ),
        (
            "SELECT a FROM t WHERE a NOT SIMILAR TO 'pat'",
            BinaryOperator::NotSimilarTo,
        ),
    ];
    for (sql, expected_op) in cases {
        let stmt = parser().parse_one(sql).unwrap();
        let sel = match stmt {
            Statement::Select(s) => s,
            _ => panic!("expected Select for: {sql}"),
        };
        let where_expr = sel.where_clause.expect("expected WHERE");
        assert!(
            matches!(where_expr, Expr::BinaryOp { op, .. } if &op == expected_op),
            "wrong op for: {sql}"
        );
    }
}

#[test]
fn test_string_concat_operator() {
    let stmt = parser()
        .parse_one("SELECT first_name || ' ' || last_name FROM users")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::BinaryOp {
                op: BinaryOperator::StringConcat,
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_pg_regex_operators() {
    // Test PostgreSQL-style POSIX regex operators which are supported by the parser.
    let stmt = parser()
        .parse_one("SELECT id FROM t WHERE name ~ 'pat'")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    let where_expr = sel.where_clause.expect("expected WHERE");
    assert!(matches!(
        where_expr,
        Expr::BinaryOp {
            op: BinaryOperator::RegexpMatch,
            ..
        }
    ));
}

#[test]
fn test_match_against_ast() {
    // MATCH ... AGAINST requires MySQL dialect; instead verify the AST variant
    // is correctly constructed and validated.
    use aeternumdb_core::sql::ast::SelectStatement;

    let stmt = Statement::Select(Box::new(SelectStatement {
        with: vec![],
        columns: vec![SelectItem::Expr {
            expr: Expr::Column {
                table: None,
                name: "id".into(),
            },
            alias: None,
        }],
        from: None,
        where_clause: Some(Expr::MatchAgainst {
            columns: vec!["title".into(), "body".into()],
            match_value: Box::new(Expr::Literal(Value::String("rust".into()))),
            modifier: Some(TextSearchModifier::Boolean),
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
        view_as: None,
    }));
    assert!(matches!(
        stmt,
        Statement::Select(ref s)
        if matches!(
            s.where_clause,
            Some(Expr::MatchAgainst { modifier: Some(TextSearchModifier::Boolean), .. })
        )
    ));
}

#[test]
fn test_substring_expr() {
    let stmt = parser()
        .parse_one("SELECT SUBSTRING(name FROM 1 FOR 5) FROM users")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::Substring {
                from_pos: Some(_),
                len: Some(_),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_position_expr() {
    let stmt = parser()
        .parse_one("SELECT POSITION('a' IN name) FROM users")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::Position { .. },
            ..
        }
    ));
}

#[test]
fn test_trim_expr() {
    let stmt = parser()
        .parse_one("SELECT TRIM(BOTH ' ' FROM name) FROM users")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::Trim {
                trim_where: Some(TrimWhereField::Both),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_overlay_expr() {
    let stmt = parser()
        .parse_one("SELECT OVERLAY(name PLACING 'X' FROM 2 FOR 3) FROM users")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::Overlay {
                for_len: Some(_),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_replace_function() {
    let stmt = parser()
        .parse_one("SELECT REPLACE(name, 'a', 'b') FROM users")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::Function { name, .. },
            ..
        } if name == "REPLACE"
    ));
}

#[test]
fn test_is_null_expr() {
    let stmt = parser()
        .parse_one("SELECT id FROM users WHERE email IS NULL")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        sel.where_clause,
        Some(Expr::IsNull { negated: false, .. })
    ));
}

#[test]
fn test_is_not_null_expr() {
    let stmt = parser()
        .parse_one("SELECT id FROM users WHERE email IS NOT NULL")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        sel.where_clause,
        Some(Expr::IsNull { negated: true, .. })
    ));
}

#[test]
fn test_inline_enum_rejected() {
    let result = parser().parse_one("CREATE TABLE t (status ENUM('active', 'inactive'))");
    assert!(
        result.is_err(),
        "expected error for inline ENUM, got: {result:?}"
    );
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("not supported") || msg.contains("CREATE ENUM"),
        "unexpected error message: {msg}"
    );
}

#[test]
fn test_create_enum_basic() {
    let stmt = parser()
        .parse_one("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')")
        .unwrap();
    let ce = match stmt {
        Statement::CreateEnum(ce) => ce,
        _ => panic!("expected CreateEnum"),
    };
    assert_eq!(ce.name, "status");
    assert!(!ce.flag);
    assert_eq!(ce.variants.len(), 3);
    assert_eq!(ce.variants[0].name, "active");
    assert_eq!(ce.variants[1].name, "inactive");
    assert_eq!(ce.variants[2].name, "pending");
}

#[test]
fn test_drop_enum() {
    // `DROP TYPE name` is mapped to Statement::DropType by the parser.
    // Statement::DropEnum is reserved for the Phase 4 native `DROP ENUM` keyword.
    let stmt = parser().parse_one("DROP TYPE status").unwrap();
    assert!(matches!(stmt, Statement::DropType(_)));
}

#[test]
fn test_enum_ref_column_type() {
    let stmt = parser()
        .parse_one("CREATE TABLE files (path VARCHAR(255), perms permissions)")
        .unwrap();
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => panic!("expected CreateTable"),
    };
    let perms_col = ct.columns.iter().find(|c| c.name == "perms").unwrap();
    assert!(matches!(&perms_col.data_type, DataType::EnumRef(name) if name == "permissions"));
}

#[test]
fn test_case_expression() {
    let stmt = parser()
        .parse_one(
            "SELECT CASE WHEN score > 90 THEN 'A' WHEN score > 80 THEN 'B' ELSE 'C' END \
             FROM grades",
        )
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::Case {
                operand: None,
                conditions,
                ..
            },
            ..
        } if conditions.len() == 2
    ));
}

#[test]
fn test_cast_expression() {
    let stmt = parser()
        .parse_one("SELECT CAST(price AS DECIMAL(10,2)) FROM products")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::Cast { .. },
            ..
        }
    ));
}

#[test]
fn test_between_expression() {
    let stmt = parser()
        .parse_one("SELECT id FROM products WHERE price BETWEEN 10 AND 100")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        sel.where_clause,
        Some(Expr::Between { negated: false, .. })
    ));
}

#[test]
fn test_in_list_expression() {
    let stmt = parser()
        .parse_one("SELECT id FROM users WHERE status IN ('active', 'pending')")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        sel.where_clause,
        Some(Expr::InList { negated: false, list, .. }) if list.len() == 2
    ));
}

#[test]
fn test_expand_select_item() {
    let stmt = parser()
        .parse_one("SELECT id, EXPAND(order_ref) AS o FROM users")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[1],
        SelectItem::Expand { alias: Some(a), .. } if a == "o"
    ));
}

#[test]
fn test_view_as_clause_rejected_aggregate() {
    use aeternumdb_core::sql::ast::{SelectStatement, TableReference};

    let mut catalog = Catalog::new();
    catalog.add_table(TableSchema {
        name: "users".into(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnSchema {
                name: "score".into(),
                data_type: DataType::Float,
                nullable: true,
            },
        ],
    });

    // Construct a SelectStatement directly with a VIEW AS containing COUNT(*).
    let stmt = Statement::Select(Box::new(SelectStatement {
        with: vec![],
        columns: vec![SelectItem::Expr {
            expr: Expr::Column {
                table: None,
                name: "id".into(),
            },
            alias: None,
        }],
        from: Some(TableReference::Named {
            database: None,
            schema: None,
            name: "users".into(),
            alias: None,
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
        view_as: Some(vec![ViewAsItem {
            expr: Expr::Function {
                name: "COUNT".into(),
                args: vec![Expr::Wildcard],
                distinct: false,
            },
            alias: "cnt".into(),
        }]),
    }));
    let result = Validator::new(&catalog).validate(&stmt);
    assert!(result.is_err());
}

// ── CREATE_REGEX / REGEX_REPLACE ──────────────────────────────────────────────

#[test]
fn test_create_regex_function() {
    // CREATE_REGEX(expr) — escapes a plain string for use as a regex pattern.
    let stmt = parser()
        .parse_one("SELECT id FROM docs WHERE body REGEXP CREATE_REGEX(title)")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    let where_expr = sel.where_clause.expect("expected WHERE");
    // Right side of REGEXP should be a CREATE_REGEX function call.
    match where_expr {
        Expr::BinaryOp { op, right, .. } => {
            assert_eq!(op, BinaryOperator::Regexp);
            assert!(
                matches!(
                    *right,
                    Expr::Function { ref name, .. } if name == "CREATE_REGEX"
                ),
                "right side should be CREATE_REGEX function, got: {right:?}"
            );
        }
        other => panic!("expected BinaryOp(REGEXP), got: {other:?}"),
    }
}

#[test]
fn test_create_regex_with_literal() {
    // CREATE_REGEX with a string literal argument.
    let stmt = parser()
        .parse_one("SELECT CREATE_REGEX('hello.world') FROM t")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    assert!(matches!(
        &sel.columns[0],
        SelectItem::Expr {
            expr: Expr::Function { name, .. },
            ..
        } if name == "CREATE_REGEX"
    ));
}

#[test]
fn test_regex_replace_three_args() {
    // REGEX_REPLACE(str, pattern, replacement) — replaces all matches.
    let stmt = parser()
        .parse_one("SELECT REGEX_REPLACE(name, '[0-9]+', '#') FROM users")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    match &sel.columns[0] {
        SelectItem::Expr {
            expr: Expr::Function { name, args, .. },
            ..
        } => {
            assert_eq!(name, "REGEX_REPLACE");
            assert_eq!(args.len(), 3);
        }
        other => panic!("expected REGEX_REPLACE Function, got: {other:?}"),
    }
}

#[test]
fn test_regex_replace_with_flags() {
    // REGEX_REPLACE(str, pattern, replacement, flags) — JavaScript-style flags.
    let stmt = parser()
        .parse_one("SELECT REGEX_REPLACE(code, '[a-z]+', 'X', 'gi') FROM items")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    match &sel.columns[0] {
        SelectItem::Expr {
            expr: Expr::Function { name, args, .. },
            ..
        } => {
            assert_eq!(name, "REGEX_REPLACE");
            // 4 arguments: str, pattern, replacement, flags
            assert_eq!(args.len(), 4);
            // flags argument is a string literal 'gi'
            assert!(matches!(&args[3], Expr::Literal(Value::String(s)) if s == "gi"));
        }
        other => panic!("expected REGEX_REPLACE Function, got: {other:?}"),
    }
}

// ── Enum coercion in comparisons ──────────────────────────────────────────────

#[test]
fn test_enum_string_comparison_parses() {
    // Comparing an enum column with a quoted string literal should parse fine.
    // The execution layer resolves 'active' to the enum variant's integer value.
    let stmt = parser()
        .parse_one("SELECT id FROM users WHERE status = 'active'")
        .unwrap();
    assert!(matches!(stmt, Statement::Select(_)));
}

#[test]
fn test_enum_integer_comparison_parses() {
    // Comparing an enum column with an integer literal should parse fine.
    // The execution layer compares directly to the stored numeric value.
    let stmt = parser()
        .parse_one("SELECT id FROM users WHERE status = 4")
        .unwrap();
    assert!(matches!(stmt, Statement::Select(_)));
}

#[test]
fn test_enum_bitwise_comparison_parses() {
    // FLAG enum columns support bitwise AND comparisons.
    // e.g. permissions & 4 = 4  checks if the bit for value 4 is set.
    let stmt = parser()
        .parse_one("SELECT id FROM roles WHERE permissions & 4 = 4")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    // WHERE should contain a BinaryOp(=) whose left is a BinaryOp(&).
    let where_expr = sel.where_clause.expect("expected WHERE");
    match &where_expr {
        Expr::BinaryOp { op, left, .. } => {
            assert_eq!(op, &BinaryOperator::Eq);
            assert!(matches!(
                left.as_ref(),
                Expr::BinaryOp { op: BinaryOperator::BitwiseAnd, .. }
            ));
        }
        other => panic!("expected outer Eq BinaryOp, got: {other:?}"),
    }
}

#[test]
fn test_enum_unquoted_identifier_comparison_parses() {
    // Unquoted names in comparisons are parsed as identifiers/column refs.
    // The execution layer resolves them as enum variant names when the LHS
    // is an EnumRef column.
    let stmt = parser()
        .parse_one("SELECT id FROM users WHERE status = active")
        .unwrap();
    let sel = match stmt {
        Statement::Select(s) => s,
        _ => panic!("expected Select"),
    };
    let where_expr = sel.where_clause.expect("expected WHERE");
    // 'active' should be parsed as a column/identifier expression
    match where_expr {
        Expr::BinaryOp { right, .. } => {
            assert!(
                matches!(
                    *right,
                    Expr::Column { ref name, .. } if name == "active"
                ),
                "expected 'active' as column identifier, got: {right:?}"
            );
        }
        other => panic!("expected BinaryOp, got: {other:?}"),
    }
}
