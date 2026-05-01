//! DML execution (INSERT, UPDATE, DELETE) with ACL and referential integrity.

use super::record_batch::Value;
use super::{ExecutionContext, ExecutorError, Result};
use std::collections::HashMap;

/// Execute an INSERT statement.
pub async fn execute_insert(
    ctx: &ExecutionContext,
    table: &str,
    columns: &[String],
    values: Vec<Vec<Value>>,
) -> Result<usize> {
    ctx.check_privilege(table, "INSERT")?;

    let schema = ctx.table_provider.schema(table)?;
    let mut rows = Vec::new();

    for value_row in values {
        let mut row = super::record_batch::Row::new();
        for (i, col_name) in columns.iter().enumerate() {
            if i < value_row.len() {
                row.insert(col_name.clone(), value_row[i].clone());
            }
        }

        for meta in &schema {
            if !row.columns.contains_key(&meta.name) {
                row.insert(meta.name.clone(), Value::Null);
            }
        }

        rows.push(row);
    }

    ctx.table_provider.insert(table, rows).await
}

/// Execute an UPDATE statement.
pub async fn execute_update(
    ctx: &ExecutionContext,
    table: &str,
    updates: HashMap<String, Value>,
) -> Result<usize> {
    ctx.check_privilege(table, "UPDATE")?;

    ctx.table_provider.update(table, updates).await
}

/// Execute a DELETE statement.
pub async fn execute_delete(ctx: &ExecutionContext, table: &str) -> Result<usize> {
    ctx.check_privilege(table, "DELETE")?;

    ctx.table_provider.delete(table).await
}

/// Execute a GRANT statement.
pub fn execute_grant(
    ctx: &ExecutionContext,
    user: &str,
    object: &str,
    privilege: &str,
) -> Result<()> {
    let mut acl = ctx.acl.lock().unwrap();
    acl.grant(user, object, privilege);
    Ok(())
}

/// Execute a REVOKE statement.
pub fn execute_revoke(
    ctx: &ExecutionContext,
    user: &str,
    object: &str,
    privilege: &str,
) -> Result<()> {
    let mut acl = ctx.acl.lock().unwrap();
    acl.revoke(user, object, privilege);
    Ok(())
}

/// Check referential integrity for a foreign key constraint.
pub async fn check_referential_integrity(
    ctx: &ExecutionContext,
    parent_table: &str,
    parent_column: &str,
    child_value: &Value,
) -> Result<bool> {
    let rows = ctx.table_provider.scan(parent_table).await?;

    for row in rows {
        if let Some(val) = row.get(parent_column) {
            if val == child_value {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// Apply referential action (CASCADE, SET NULL, SET DEFAULT, RESTRICT).
pub async fn apply_referential_action(
    ctx: &ExecutionContext,
    action: &str,
    table: &str,
    column: &str,
) -> Result<()> {
    match action.to_uppercase().as_str() {
        "CASCADE" => {
            execute_delete(ctx, table).await?;
            Ok(())
        }
        "SET NULL" => {
            let mut updates = HashMap::new();
            updates.insert(column.to_string(), Value::Null);
            execute_update(ctx, table, updates).await?;
            Ok(())
        }
        "SET DEFAULT" => {
            let mut updates = HashMap::new();
            updates.insert(column.to_string(), Value::Null);
            execute_update(ctx, table, updates).await?;
            Ok(())
        }
        "RESTRICT" | "NO ACTION" => Err(ExecutorError::ReferentialIntegrityViolation(format!(
            "Cannot delete/update due to foreign key constraint on {}",
            table
        ))),
        _ => Ok(()),
    }
}
