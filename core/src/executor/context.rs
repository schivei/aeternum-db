//! Execution context and table provider abstractions.

use super::record_batch::{Row, Value};
use super::{ExecutorError, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Trait for providing table data to the executor.
///
/// This abstraction allows tests to use an in-memory row store
/// while Phase 1 runs without a full catalog.
#[async_trait]
pub trait TableProvider: Send + Sync {
    /// Scan a table and return all rows.
    async fn scan(&self, table: &str) -> Result<Vec<Row>>;

    /// Get the schema for a table (column names and type names).
    fn schema(&self, table: &str) -> Result<Vec<(String, String)>>;

    /// Insert rows into a table. Returns the number of rows inserted.
    async fn insert(&self, table: &str, rows: Vec<Row>) -> Result<usize>;

    /// Update rows matching a predicate. Returns the number of rows updated.
    async fn update(&self, table: &str, updates: HashMap<String, Value>) -> Result<usize>;

    /// Delete rows from a table. Returns the number of rows deleted.
    async fn delete(&self, table: &str) -> Result<usize>;

    /// Check if a table exists.
    fn table_exists(&self, table: &str) -> bool;
}

/// Simple in-memory table provider for testing.
pub struct InMemoryTableProvider {
    tables: Arc<Mutex<HashMap<String, (Vec<(String, String)>, Vec<Row>)>>>,
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
        let mut tables = self.tables.lock().unwrap();
        tables.insert(name.to_string(), (schema, Vec::new()));
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

    fn schema(&self, table: &str) -> Result<Vec<(String, String)>> {
        let tables = self.tables.lock().unwrap();
        if let Some((schema, _)) = tables.get(table) {
            Ok(schema.clone())
        } else {
            Err(ExecutorError::TableNotFound(table.to_string()))
        }
    }

    async fn insert(&self, table: &str, rows: Vec<Row>) -> Result<usize> {
        let mut tables = self.tables.lock().unwrap();
        if let Some((_, table_rows)) = tables.get_mut(table) {
            let count = rows.len();
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
        if let Some((_, rows)) = tables.get_mut(table) {
            let count = rows.len();
            rows.clear();
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
            .or_insert_with(Vec::new)
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
