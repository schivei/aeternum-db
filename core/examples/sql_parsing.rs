//! SQL parsing examples for AeternumDB.
//!
//! Run with:
//! ```
//! cargo run --example sql_parsing
//! ```

use aeternumdb_core::sql::ast::{DataType, Statement};
use aeternumdb_core::sql::parser::SqlParser;
use aeternumdb_core::sql::validator::{
    apply_create_table, Catalog, ColumnSchema, TableSchema, Validator,
};

fn main() {
    println!("=== AeternumDB SQL Parser Examples ===\n");

    basic_select_example();
    insert_example();
    update_delete_example();
    ddl_example();
    expression_parsing_example();
    error_handling_example();
    semantic_validation_example();
    complex_query_example();
}

fn basic_select_example() {
    println!("--- Basic SELECT ---");
    let parser = SqlParser::new();

    let sql = "SELECT id, name, age FROM users WHERE age > 18 ORDER BY name LIMIT 10";
    let stmts = parser.parse(sql).expect("parse failed");

    match &stmts[0] {
        Statement::Select(select) => {
            println!("Columns: {}", select.columns.len());
            println!("Has WHERE: {}", select.where_clause.is_some());
            println!("Has ORDER BY: {}", !select.order_by.is_empty());
            println!("LIMIT: {:?}", select.limit);
        }
        _ => unreachable!(),
    }
    println!();
}

fn insert_example() {
    println!("--- INSERT ---");
    let parser = SqlParser::new();

    // Single row
    let sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)";
    let stmts = parser.parse(sql).expect("parse failed");
    match &stmts[0] {
        Statement::Insert(ins) => {
            println!("Table: {}", ins.table);
            println!("Columns: {:?}", ins.columns);
            println!("Rows: {}", ins.values.len());
        }
        _ => unreachable!(),
    }

    // Multi-row
    let sql = "INSERT INTO users (id, name) VALUES (2, 'Bob'), (3, 'Carol')";
    let stmts = parser.parse(sql).expect("parse failed");
    match &stmts[0] {
        Statement::Insert(ins) => {
            println!("Multi-row INSERT: {} rows", ins.values.len());
        }
        _ => unreachable!(),
    }
    println!();
}

fn update_delete_example() {
    println!("--- UPDATE / DELETE ---");
    let parser = SqlParser::new();

    let sql = "UPDATE users SET name = 'Dave', age = 25 WHERE id = 1";
    let stmts = parser.parse(sql).expect("parse failed");
    match &stmts[0] {
        Statement::Update(upd) => {
            println!("UPDATE table: {}", upd.table);
            println!("Assignments: {}", upd.assignments.len());
            for (col, _) in &upd.assignments {
                println!("  SET {col} = ...");
            }
        }
        _ => unreachable!(),
    }

    let sql = "DELETE FROM users WHERE age < 18";
    let stmts = parser.parse(sql).expect("parse failed");
    match &stmts[0] {
        Statement::Delete(del) => {
            println!("DELETE FROM {}", del.table);
            println!("Has WHERE: {}", del.where_clause.is_some());
        }
        _ => unreachable!(),
    }
    println!();
}

fn ddl_example() {
    println!("--- DDL ---");
    let parser = SqlParser::new();

    // CREATE TABLE
    let sql = "
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price DECIMAL(10, 2),
            in_stock BOOLEAN
        )
    ";
    let stmts = parser.parse(sql).expect("parse failed");
    match &stmts[0] {
        Statement::CreateTable(ct) => {
            println!("CREATE TABLE: {}", ct.table);
            for col in &ct.columns {
                println!(
                    "  {} {} (nullable={}, pk={})",
                    col.name, col.data_type, col.nullable, col.primary_key
                );
            }
        }
        _ => unreachable!(),
    }

    // DROP TABLE
    let stmts = parser
        .parse("DROP TABLE IF EXISTS old_table")
        .expect("parse failed");
    match &stmts[0] {
        Statement::DropTable(dt) => {
            println!("DROP TABLE IF EXISTS: {:?}", dt.tables);
        }
        _ => unreachable!(),
    }

    // ALTER TABLE
    let stmts = parser
        .parse("ALTER TABLE users ADD COLUMN email VARCHAR(255)")
        .expect("parse failed");
    match &stmts[0] {
        Statement::AlterTable(alt) => {
            println!("ALTER TABLE {}: {} operation(s)", alt.table, alt.operations.len());
        }
        _ => unreachable!(),
    }
    println!();
}

fn expression_parsing_example() {
    println!("--- Expression Parsing ---");
    let parser = SqlParser::new();

    let expressions = [
        "1 + 2 * 3",
        "age > 18 AND name IS NOT NULL",
        "COUNT(DISTINCT id)",
        "SUM(price)",
        "CAST(price AS INTEGER)",
        "status IN (1, 2, 3)",
        "age BETWEEN 18 AND 65",
        "CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END",
    ];

    for expr_str in &expressions {
        match parser.parse_expr(expr_str) {
            Ok(expr) => println!("  ✓ {expr_str:?} → {expr:?}"),
            Err(e) => println!("  ✗ {expr_str:?} → {e}"),
        }
    }
    println!();
}

fn error_handling_example() {
    println!("--- Error Handling ---");
    let parser = SqlParser::new();

    // Typo in keyword
    let sql = "SELET * FROM users";
    match parser.parse(sql) {
        Ok(_) => println!("  Note: 'SELET' was accepted (treated as identifier)"),
        Err(e) => println!("  Error: {e}"),
    }

    // Unclosed string
    let sql = "SELECT 'unclosed FROM users";
    match parser.parse(sql) {
        Ok(_) => println!("  Note: unclosed string accepted"),
        Err(e) => println!("  Error (unclosed string): {e}"),
    }

    // Empty input
    match parser.parse("") {
        Ok(_) => println!("  Note: empty accepted"),
        Err(e) => println!("  Error (empty input): {e}"),
    }

    println!();
}

fn semantic_validation_example() {
    println!("--- Semantic Validation ---");

    // Build catalog
    let mut catalog = Catalog::new();
    catalog.add_table(TableSchema {
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
        ],
    });

    let parser = SqlParser::new();
    let validator = Validator::new(&catalog);

    // Valid query
    let stmt = parser.parse_one("SELECT id, name FROM users").unwrap();
    match validator.validate(&stmt) {
        Ok(()) => println!("  ✓ Valid query passed validation"),
        Err(e) => println!("  ✗ Unexpected error: {e}"),
    }

    // Unknown table
    let stmt = parser.parse_one("SELECT id FROM ghost_table").unwrap();
    match validator.validate(&stmt) {
        Ok(()) => println!("  ✗ Should have failed"),
        Err(e) => println!("  ✓ Caught: {e}"),
    }

    // Unknown column
    let stmt = parser.parse_one("SELECT bogus FROM users").unwrap();
    match validator.validate(&stmt) {
        Ok(()) => println!("  ✗ Should have failed"),
        Err(e) => println!("  ✓ Caught: {e}"),
    }

    // NOT NULL violation
    let stmt = parser
        .parse_one("INSERT INTO users (id) VALUES (NULL)")
        .unwrap();
    match validator.validate(&stmt) {
        Ok(()) => println!("  ✗ Should have failed"),
        Err(e) => println!("  ✓ Caught: {e}"),
    }

    // Aggregate in WHERE
    let stmt = parser
        .parse_one("SELECT id FROM users WHERE COUNT(*) > 5")
        .unwrap();
    match validator.validate(&stmt) {
        Ok(()) => println!("  ✗ Should have failed"),
        Err(e) => println!("  ✓ Caught: {e}"),
    }

    println!();
}

fn complex_query_example() {
    println!("--- Complex Query (TPC-H style) ---");
    let parser = SqlParser::new();

    let sql = "
        SELECT
            user_id,
            COUNT(*) AS order_count,
            SUM(total) AS total_sum,
            AVG(total) AS avg_total
        FROM orders
        WHERE total > 100
        GROUP BY user_id
        HAVING COUNT(*) > 2
        ORDER BY total_sum DESC
        LIMIT 20 OFFSET 0
    ";

    match parser.parse(sql) {
        Ok(stmts) => {
            if let Statement::Select(sel) = &stmts[0] {
                println!("  Columns:  {}", sel.columns.len());
                println!("  GROUP BY: {}", sel.group_by.len());
                println!("  HAVING:   {}", sel.having.is_some());
                println!("  ORDER BY: {}", sel.order_by.len());
                println!("  LIMIT:    {:?}", sel.limit);
                println!("  OFFSET:   {:?}", sel.offset);
            }
        }
        Err(e) => println!("  Error: {e}"),
    }

    // Nested subquery
    let sql = "
        SELECT id, name FROM users
        WHERE id IN (
            SELECT user_id FROM orders
            WHERE total > (SELECT AVG(total) FROM orders)
        )
    ";
    match parser.parse(sql) {
        Ok(_) => println!("  ✓ Nested subquery parsed successfully"),
        Err(e) => println!("  ✗ Error: {e}"),
    }

    println!();
}

/// Demonstrate building a catalog by parsing and executing DDL statements.
#[allow(dead_code)]
fn catalog_from_ddl_example() {
    let parser = SqlParser::new();
    let mut catalog = Catalog::new();

    let ddl_statements = [
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL)",
        "CREATE TABLE invoices (id INTEGER PRIMARY KEY, customer_id INTEGER, amount DECIMAL(12,2))",
    ];

    for sql in &ddl_statements {
        let stmt = parser.parse_one(sql).expect("DDL parse failed");
        if let Statement::CreateTable(ref ct) = stmt {
            apply_create_table(&mut catalog, ct);
            println!("Registered table: {}", ct.table);
        }
    }

    // Now validate a SELECT
    let select_stmt = parser
        .parse_one("SELECT c.name, i.amount FROM customers c INNER JOIN invoices i ON c.id = i.customer_id")
        .expect("SELECT parse failed");
    let validator = Validator::new(&catalog);
    match validator.validate(&select_stmt) {
        Ok(()) => println!("JOIN query validated successfully"),
        Err(e) => println!("Validation error: {e}"),
    }
}
