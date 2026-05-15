//! Execution context and table provider abstractions.

use super::record_batch::{Row, Value};
use super::{ExecutorError, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Metadata for a single column, including its name, type, and optional
/// inner element count for array and indexed columns.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMeta {
    /// Column name (lowercased).
    pub name: String,
    /// SQL type name string (e.g. `"integer"`, `"varchar"`, `"array"`).
    pub type_name: String,
    /// For array-typed columns: the total count of inner elements across
    /// all rows currently stored.  For indexed columns: the number of
    /// indexed entries.  `None` when the column is neither an array nor
    /// indexed.
    pub inner_count: Option<usize>,
}

impl ColumnMeta {
    /// Create a new `ColumnMeta` with no inner count.
    pub fn new(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            inner_count: None,
        }
    }

    /// Create a new `ColumnMeta` with a known inner count.
    pub fn with_inner_count(
        name: impl Into<String>,
        type_name: impl Into<String>,
        inner_count: usize,
    ) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            inner_count: Some(inner_count),
        }
    }

    /// Returns true if this column is array-typed (type_name contains "array" or "vector").
    pub fn is_array(&self) -> bool {
        let t = self.type_name.to_lowercase();
        t.contains("array") || t.contains("vector")
    }
}

/// Trait for providing table data to the executor.
///
/// This abstraction allows tests to use an in-memory row store
/// while Phase 1 runs without a full catalog.
#[async_trait]
pub trait TableProvider: Send + Sync {
    /// Scan a table and return all rows.
    async fn scan(&self, table: &str) -> Result<Vec<Row>>;

    /// Get the schema for a table as column metadata (name, type, and inner count).
    fn schema(&self, table: &str) -> Result<Vec<ColumnMeta>>;

    /// Insert rows into a table. Returns the number of rows inserted.
    async fn insert(&self, table: &str, rows: Vec<Row>) -> Result<usize>;

    /// Update rows matching a predicate. Returns the number of rows updated.
    async fn update(&self, table: &str, updates: HashMap<String, Value>) -> Result<usize>;

    /// Delete rows from a table. Returns the number of rows deleted.
    async fn delete(&self, table: &str) -> Result<usize>;

    /// Check if a table exists.
    fn table_exists(&self, table: &str) -> bool;
}

/// Type alias for the in-memory table storage map.
type TableStorage = Arc<Mutex<HashMap<String, (Vec<ColumnMeta>, Vec<Row>)>>>;

/// Simple in-memory table provider for testing.
pub struct InMemoryTableProvider {
    tables: TableStorage,
}

impl InMemoryTableProvider {
    /// Create a new empty in-memory provider.
    pub fn new() -> Self {
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add a table with schema.
    pub fn add_table(&self, name: &str, schema: Vec<(String, String)>) {
        let meta: Vec<ColumnMeta> = schema
            .into_iter()
            .map(|(n, t)| ColumnMeta::new(n, t))
            .collect();
        let mut tables = self.tables.lock().unwrap();
        tables.insert(name.to_string(), (meta, Vec::new()));
    }

    /// Add a table with full column metadata including inner counts.
    pub fn add_table_with_meta(&self, name: &str, schema: Vec<ColumnMeta>) {
        let mut tables = self.tables.lock().unwrap();
        tables.insert(name.to_string(), (schema, Vec::new()));
    }

    /// Set the inner count for a specific column (used after bulk loading).
    pub fn set_column_inner_count(&self, table: &str, column: &str, count: usize) {
        let mut tables = self.tables.lock().unwrap();
        if let Some((schema, _)) = tables.get_mut(table) {
            if let Some(meta) = schema.iter_mut().find(|m| m.name == column) {
                meta.inner_count = Some(count);
            }
        }
    }

    /// Add rows to a table.
    pub fn add_rows(&self, name: &str, rows: Vec<Row>) {
        let mut tables = self.tables.lock().unwrap();
        if let Some((_, table_rows)) = tables.get_mut(name) {
            table_rows.extend(rows);
        }
    }
}

impl Default for InMemoryTableProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProvider for InMemoryTableProvider {
    async fn scan(&self, table: &str) -> Result<Vec<Row>> {
        let tables = self.tables.lock().unwrap();
        if let Some((_, rows)) = tables.get(table) {
            Ok(rows.clone())
        } else {
            Err(ExecutorError::TableNotFound(table.to_string()))
        }
    }

    fn schema(&self, table: &str) -> Result<Vec<ColumnMeta>> {
        let tables = self.tables.lock().unwrap();
        if let Some((schema, _)) = tables.get(table) {
            Ok(schema.clone())
        } else {
            Err(ExecutorError::TableNotFound(table.to_string()))
        }
    }

    async fn insert(&self, table: &str, rows: Vec<Row>) -> Result<usize> {
        let mut tables = self.tables.lock().unwrap();
        if let Some((schema, table_rows)) = tables.get_mut(table) {
            let count = rows.len();
            // Update inner_count for array-typed columns.
            for row in &rows {
                for meta in schema.iter_mut() {
                    if meta.is_array() {
                        if let Some(Value::Array(arr)) = row.get(&meta.name) {
                            meta.inner_count = Some(meta.inner_count.unwrap_or(0) + arr.len());
                        }
                    }
                }
            }
            table_rows.extend(rows);
            Ok(count)
        } else {
            Err(ExecutorError::TableNotFound(table.to_string()))
        }
    }

    async fn update(&self, table: &str, updates: HashMap<String, Value>) -> Result<usize> {
        let mut tables = self.tables.lock().unwrap();
        if let Some((_, rows)) = tables.get_mut(table) {
            let count = rows.len();
            for row in rows.iter_mut() {
                for (col, val) in &updates {
                    row.insert(col.clone(), val.clone());
                }
            }
            Ok(count)
        } else {
            Err(ExecutorError::TableNotFound(table.to_string()))
        }
    }

    async fn delete(&self, table: &str) -> Result<usize> {
        let mut tables = self.tables.lock().unwrap();
        if let Some((schema, rows)) = tables.get_mut(table) {
            let count = rows.len();
            rows.clear();
            // Reset inner_count for array columns since all rows are removed.
            for meta in schema.iter_mut() {
                if meta.is_array() {
                    meta.inner_count = Some(0);
                }
            }
            Ok(count)
        } else {
            Err(ExecutorError::TableNotFound(table.to_string()))
        }
    }

    fn table_exists(&self, table: &str) -> bool {
        let tables = self.tables.lock().unwrap();
        tables.contains_key(table)
    }
}

/// Access control list for privilege checking.
#[derive(Debug, Clone)]
pub struct ACL {
    grants: HashMap<(String, String), Vec<String>>,
}

impl ACL {
    /// Create a new empty ACL.
    pub fn new() -> Self {
        Self {
            grants: HashMap::new(),
        }
    }

    /// Grant a privilege to a user on an object.
    pub fn grant(&mut self, user: &str, object: &str, privilege: &str) {
        let key = (user.to_string(), object.to_string());
        self.grants
            .entry(key)
            .or_default()
            .push(privilege.to_string());
    }

    /// Revoke a privilege from a user on an object.
    pub fn revoke(&mut self, user: &str, object: &str, privilege: &str) {
        let key = (user.to_string(), object.to_string());
        if let Some(privs) = self.grants.get_mut(&key) {
            privs.retain(|p| p != privilege);
        }
    }

    /// Check if a user has a privilege on an object.
    pub fn check(&self, user: &str, object: &str, privilege: &str) -> bool {
        let key = (user.to_string(), object.to_string());
        if let Some(privs) = self.grants.get(&key) {
            privs.contains(&privilege.to_string())
        } else {
            false
        }
    }
}

impl Default for ACL {
    fn default() -> Self {
        Self::new()
    }
}

/// Object ID generator for new database objects.
pub trait ObjIdGenerator: Send + Sync {
    /// Generate a new unique object ID.
    fn next_id(&self) -> u64;
}

/// Simple atomic counter-based ID generator.
pub struct AtomicIdGenerator {
    counter: Arc<Mutex<u64>>,
}

impl AtomicIdGenerator {
    /// Create a new generator starting from the given initial value.
    pub fn new(start: u64) -> Self {
        Self {
            counter: Arc::new(Mutex::new(start)),
        }
    }
}

impl Default for AtomicIdGenerator {
    fn default() -> Self {
        Self::new(1)
    }
}

impl ObjIdGenerator for AtomicIdGenerator {
    fn next_id(&self) -> u64 {
        let mut counter = self.counter.lock().unwrap();
        let id = *counter;
        *counter += 1;
        id
    }
}

/// Execution context holding table provider, ACL, and objid generator.
pub struct ExecutionContext {
    /// Table provider for data access.
    pub table_provider: Arc<dyn TableProvider>,
    /// Access control list.
    pub acl: Arc<Mutex<ACL>>,
    /// Object ID generator.
    pub objid_gen: Arc<dyn ObjIdGenerator>,
    /// Current user (for ACL checks).
    pub current_user: String,
}

impl ExecutionContext {
    /// Create a new execution context.
    pub fn new(
        table_provider: Arc<dyn TableProvider>,
        acl: Arc<Mutex<ACL>>,
        objid_gen: Arc<dyn ObjIdGenerator>,
        current_user: String,
    ) -> Self {
        Self {
            table_provider,
            acl,
            objid_gen,
            current_user,
        }
    }

    /// Create a default context for testing with in-memory tables.
    pub fn default_test() -> Self {
        Self {
            table_provider: Arc::new(InMemoryTableProvider::new()),
            acl: Arc::new(Mutex::new(ACL::new())),
            objid_gen: Arc::new(AtomicIdGenerator::default()),
            current_user: "test_user".to_string(),
        }
    }

    /// Check if the current user has a privilege on an object.
    pub fn check_privilege(&self, object: &str, privilege: &str) -> Result<()> {
        let acl = self.acl.lock().unwrap();
        if acl.check(&self.current_user, object, privilege) {
            Ok(())
        } else {
            Err(ExecutorError::PermissionDenied(format!(
                "User {} lacks {} privilege on {}",
                self.current_user, privilege, object
            )))
        }
    }
}
