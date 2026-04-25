# PR 1.6: Table Catalog & Schema Management

## 📋 Overview

**PR Number:** 1.6
**Phase:** 1 - Core Foundation
**Priority:** 🟡 High
**Estimated Effort:** 4 days
**Dependencies:** PR 1.1 (Storage Engine)

## 🎯 Objectives

Implement a comprehensive system catalog for managing database metadata including tables, columns, indexes, and schema evolution. This includes:

- Table and column metadata management
- Schema registry and persistence
- Primary key and constraint definitions
- Index registry
- Schema versioning and evolution
- System catalog tables

## 📝 Detailed Prompt for Implementation

```
Implement a complete catalog system for AeternumDB with the following requirements:

1. **System Catalog Tables**
   - TABLES: Stores table metadata
   - COLUMNS: Stores column definitions
   - INDEXES: Stores index metadata
   - CONSTRAINTS: Stores constraint definitions
   - SCHEMAS: Stores schema versions

2. **Table Management**
   - Create/drop tables
   - Alter table operations (add/drop columns)
   - Table metadata: name, schema, creation time, row count
   - Table versioning for schema changes

3. **Column Management**
   - Column definitions: name, type, nullable, default value
   - Type validation
   - Column ordering
   - Column metadata updates

4. **Constraint Management**
   - Primary keys
   - Foreign keys (basic support)
   - Unique constraints
   - Check constraints
   - Not null constraints

5. **Index Registry**
   - Index metadata: name, table, columns, type
   - Index creation and deletion tracking
   - Multiple indexes per table

6. **Schema Evolution**
   - Track schema versions
   - Backward compatibility
   - Migration support
   - Schema change validation

7. **Persistence**
   - Catalog stored in special pages
   - Transactional catalog updates
   - Recovery after crash

8. **Performance Requirements**
   - Catalog lookup: <1ms
   - Schema cache in memory
   - Support 10,000+ tables

Use Rust best practices, comprehensive error handling, and include detailed documentation.
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/catalog/mod.rs`**
   - Public API for catalog
   - Catalog manager implementation
   - CatalogConfig structure

2. **`core/src/catalog/table.rs`**
   - TableMetadata structure
   - Table creation and management
   - Table registry

3. **`core/src/catalog/column.rs`**
   - ColumnDefinition structure
   - Column types and validation
   - Column metadata

4. **`core/src/catalog/schema.rs`**
   - Schema structure
   - Schema versioning
   - Schema validation

5. **`core/src/catalog/constraint.rs`**
   - Constraint definitions
   - Constraint validation
   - Constraint management

6. **`core/src/catalog/index_registry.rs`**
   - Index metadata
   - Index registration
   - Index lookup

7. **`core/src/catalog/persistence.rs`**
   - Catalog serialization
   - Catalog recovery
   - System catalog tables

### Test Files

8. **`core/tests/catalog_tests.rs`**
   - Integration tests for catalog

## 🔧 Implementation Details

### Table Metadata Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub id: TableId,
    pub name: String,
    pub schema: Arc<Schema>,
    pub schema_version: u32,
    pub created_at: Timestamp,
    pub modified_at: Timestamp,
    pub row_count: u64,
    pub storage_info: StorageInfo,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Option<Vec<String>>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default_value: Option<Value>,
    pub position: usize,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Decimal { precision: u8, scale: u8 },
    Varchar { max_length: Option<usize> },
    Text,
    Timestamp,
    Json,
}
```

### Catalog Manager

```rust
pub struct CatalogManager {
    tables: RwLock<HashMap<String, Arc<TableMetadata>>>,
    indexes: RwLock<HashMap<String, Arc<IndexMetadata>>>,
    storage: Arc<StorageEngine>,
    cache: Arc<CatalogCache>,
}

impl CatalogManager {
    pub fn new(storage: Arc<StorageEngine>) -> Result<Self> {
        let catalog = Self {
            tables: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            storage,
            cache: Arc::new(CatalogCache::new()),
        };

        // Load catalog from storage
        catalog.load_from_storage()?;

        Ok(catalog)
    }

    pub async fn create_table(
        &self,
        name: String,
        schema: Schema,
    ) -> Result<TableId> {
        // Validate schema
        self.validate_schema(&schema)?;

        // Check if table already exists
        if self.table_exists(&name)? {
            return Err(Error::TableAlreadyExists(name));
        }

        // Allocate table ID
        let table_id = self.allocate_table_id().await?;

        // Create table metadata
        let metadata = TableMetadata {
            id: table_id,
            name: name.clone(),
            schema: Arc::new(schema),
            schema_version: 1,
            created_at: Timestamp::now(),
            modified_at: Timestamp::now(),
            row_count: 0,
            storage_info: StorageInfo::default(),
        };

        // Persist to storage
        self.persist_table_metadata(&metadata).await?;

        // Add to in-memory catalog
        let mut tables = self.tables.write();
        tables.insert(name, Arc::new(metadata));

        Ok(table_id)
    }

    pub async fn drop_table(&self, name: &str) -> Result<()> {
        // Get table metadata
        let metadata = self.get_table(name)?;

        // Drop all indexes on this table
        self.drop_all_indexes(metadata.id).await?;

        // Remove from storage
        self.delete_table_metadata(metadata.id).await?;

        // Remove from in-memory catalog
        let mut tables = self.tables.write();
        tables.remove(name);

        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<Arc<TableMetadata>> {
        let tables = self.tables.read();
        tables.get(name)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    pub async fn alter_table(
        &self,
        name: &str,
        operation: AlterTableOp,
    ) -> Result<()> {
        let mut metadata = (*self.get_table(name)?).clone();

        match operation {
            AlterTableOp::AddColumn(col_def) => {
                // Validate column doesn't exist
                if metadata.schema.has_column(&col_def.name) {
                    return Err(Error::ColumnAlreadyExists(col_def.name));
                }

                // Add column to schema
                let mut schema = (*metadata.schema).clone();
                col_def.position = schema.columns.len();
                schema.columns.push(col_def);

                metadata.schema = Arc::new(schema);
                metadata.schema_version += 1;
                metadata.modified_at = Timestamp::now();
            }
            AlterTableOp::DropColumn(col_name) => {
                // Validate column exists
                let mut schema = (*metadata.schema).clone();
                let pos = schema.columns.iter()
                    .position(|c| c.name == col_name)
                    .ok_or_else(|| Error::ColumnNotFound(col_name))?;

                schema.columns.remove(pos);

                // Update positions
                for (i, col) in schema.columns.iter_mut().enumerate() {
                    col.position = i;
                }

                metadata.schema = Arc::new(schema);
                metadata.schema_version += 1;
                metadata.modified_at = Timestamp::now();
            }
        }

        // Persist changes
        self.persist_table_metadata(&metadata).await?;

        // Update in-memory catalog
        let mut tables = self.tables.write();
        tables.insert(name.to_string(), Arc::new(metadata));

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum AlterTableOp {
    AddColumn(ColumnDefinition),
    DropColumn(String),
}
```

### Index Registry

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub id: IndexId,
    pub name: String,
    pub table_id: TableId,
    pub table_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub is_unique: bool,
    pub created_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
}

impl CatalogManager {
    pub async fn create_index(
        &self,
        name: String,
        table_name: String,
        columns: Vec<String>,
        index_type: IndexType,
        is_unique: bool,
    ) -> Result<IndexId> {
        // Verify table exists
        let table = self.get_table(&table_name)?;

        // Verify columns exist
        for col in &columns {
            if !table.schema.has_column(col) {
                return Err(Error::ColumnNotFound(col.clone()));
            }
        }

        // Check if index already exists
        if self.index_exists(&name)? {
            return Err(Error::IndexAlreadyExists(name));
        }

        // Allocate index ID
        let index_id = self.allocate_index_id().await?;

        // Create index metadata
        let metadata = IndexMetadata {
            id: index_id,
            name: name.clone(),
            table_id: table.id,
            table_name,
            columns,
            index_type,
            is_unique,
            created_at: Timestamp::now(),
        };

        // Persist to storage
        self.persist_index_metadata(&metadata).await?;

        // Add to in-memory catalog
        let mut indexes = self.indexes.write();
        indexes.insert(name, Arc::new(metadata));

        Ok(index_id)
    }

    pub fn get_table_indexes(
        &self,
        table_id: TableId,
    ) -> Result<Vec<Arc<IndexMetadata>>> {
        let indexes = self.indexes.read();
        let result = indexes.values()
            .filter(|idx| idx.table_id == table_id)
            .cloned()
            .collect();
        Ok(result)
    }
}
```

### API Examples

```rust
use aeternumdb::catalog::{CatalogManager, Schema, ColumnDefinition, DataType};

// Create catalog manager
let catalog = CatalogManager::new(storage.clone())?;

// Create a table
let schema = Schema {
    columns: vec![
        ColumnDefinition {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            default_value: None,
            position: 0,
        },
        ColumnDefinition {
            name: "name".to_string(),
            data_type: DataType::Varchar { max_length: Some(255) },
            nullable: false,
            default_value: None,
            position: 1,
        },
        ColumnDefinition {
            name: "email".to_string(),
            data_type: DataType::Varchar { max_length: Some(255) },
            nullable: true,
            default_value: None,
            position: 2,
        },
    ],
    primary_key: Some(vec!["id".to_string()]),
    constraints: vec![],
};

let table_id = catalog.create_table("users".to_string(), schema).await?;

// Create an index
let index_id = catalog.create_index(
    "users_email_idx".to_string(),
    "users".to_string(),
    vec!["email".to_string()],
    IndexType::BTree,
    false, // not unique
).await?;

// Get table metadata
let table = catalog.get_table("users")?;
println!("Table: {}, Columns: {}", table.name, table.schema.columns.len());

// Alter table - add column
catalog.alter_table(
    "users",
    AlterTableOp::AddColumn(ColumnDefinition {
        name: "created_at".to_string(),
        data_type: DataType::Timestamp,
        nullable: false,
        default_value: Some(Value::CurrentTimestamp),
        position: 0, // Will be set automatically
    })
).await?;

// Drop table
catalog.drop_table("users").await?;
```

## ✅ Tests Required

### Unit Tests

1. **Table Management Tests** (`table.rs`)
   - ✅ Create table
   - ✅ Drop table
   - ✅ Get table metadata
   - ✅ Table exists check
   - ✅ Create duplicate table (error)
   - ✅ Drop non-existent table (error)

2. **Column Management Tests** (`column.rs`)
   - ✅ Add column
   - ✅ Drop column
   - ✅ Column type validation
   - ✅ Column default values
   - ✅ Column position tracking

3. **Schema Tests** (`schema.rs`)
   - ✅ Schema validation
   - ✅ Schema versioning
   - ✅ Schema evolution
   - ✅ Backward compatibility

4. **Index Registry Tests** (`index_registry.rs`)
   - ✅ Create index
   - ✅ Drop index
   - ✅ List table indexes
   - ✅ Index metadata lookup

### Integration Tests

5. **Catalog Tests** (`catalog_tests.rs`)
   - ✅ Full table lifecycle
   - ✅ Multiple tables
   - ✅ Concurrent catalog access
   - ✅ Catalog persistence
   - ✅ Catalog recovery after crash
   - ✅ Schema changes
   - ✅ 1000+ tables

## 📊 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Table lookup | <1ms | Benchmark |
| Create table | <10ms | Benchmark |
| Alter table | <20ms | Benchmark |
| Index lookup | <1ms | Benchmark |
| Catalog recovery | <1s | Test |
| Support tables | 10,000+ | Test |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Comprehensive rustdoc comments for all public APIs
   - Example usage in doc comments
   - Schema evolution guide

2. **Catalog Architecture Document** (`docs/catalog-architecture.md`)
   - System catalog design
   - Schema versioning strategy
   - Migration guide
   - Performance considerations

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] Tables can be created and dropped
- [ ] Columns can be added and removed
- [ ] Indexes are tracked in registry
- [ ] Schema changes are versioned
- [ ] Metadata persists across restarts
- [ ] Catalog recovers after crash
- [ ] Concurrent access is safe

### Quality Requirements
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Code coverage >85%
- [ ] No clippy warnings
- [ ] Documentation complete

### Performance Requirements
- [ ] Lookup performance meets targets
- [ ] Supports 10,000+ tables
- [ ] Fast recovery

## 🔗 Dependencies

This PR depends on:
- **PR 1.1**: Storage Engine (for persistence)

This PR is required by:
- **PR 1.3**: SQL Parser (needs schema info)
- **PR 1.4**: Query Planner (needs catalog)
- **PR 1.5**: Query Executor (needs table metadata)

## 📦 Dependencies to Add

```toml
[dependencies]
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"  # For catalog serialization
```

## 🚀 Implementation Steps

### Day 1: Core Structures
- Define TableMetadata, Schema, ColumnDefinition
- Implement basic validation
- Write unit tests

### Day 2: Catalog Manager
- Implement CatalogManager
- Table creation and deletion
- In-memory catalog
- Write tests

### Day 3: Schema Evolution & Index Registry
- Implement alter table operations
- Implement index registry
- Schema versioning
- Write tests

### Day 4: Persistence & Documentation
- Implement catalog persistence
- Implement recovery
- Integration tests
- Write documentation

## 🐛 Known Edge Cases to Handle

1. **Concurrent schema changes**: Use locking appropriately
2. **Invalid type changes**: Validate before applying
3. **Dropping columns with indexes**: Drop indexes first
4. **Foreign key constraints**: Track dependencies
5. **Very long table/column names**: Enforce limits
6. **Reserved keywords**: Validate identifiers

## 💡 Future Enhancements (Out of Scope)

- Views and materialized views → Phase 3
- Stored procedures → Phase 3
- Triggers → Phase 3
- Table partitioning → Phase 3
- Cross-database queries → Phase 5

## 🏁 Definition of Done

This PR is complete when:
1. All code is implemented and tested
2. All acceptance criteria met
3. CI/CD pipeline passes
4. Documentation complete
5. Integration verified

---

**Ready to implement?** Use this document as your complete specification. All details needed are provided above. Good luck! 🚀
