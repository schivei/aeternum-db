# PR 1.12: Configuration System

## 📋 Overview

**PR Number:** 1.12  
**Phase:** 1 - Core Foundation  
**Priority:** 🟢 Medium  
**Estimated Effort:** 3 days  
**Dependencies:** None

## 🎯 Objectives

Implement flexible configuration system with TOML files, environment variables, and runtime validation.

## 📝 Detailed Prompt

```
Implement configuration system with:
1. TOML configuration files
2. Environment variable override
3. Default values for all options
4. Configuration validation
5. Hot reload (where applicable)
6. Configuration sections: storage, network, transaction, query, logging
```

## 🏗️ Files to Create

1. `core/src/config/mod.rs` - Public API
2. `core/src/config/loader.rs` - Config loading
3. `core/src/config/validator.rs` - Validation
4. `core/src/config/defaults.rs` - Default values
5. `core/tests/config_tests.rs` - Tests
6. `aeternumdb.toml.example` - Example config

## 🔧 Implementation Details

### Config Structure
```rust
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub network: NetworkConfig,
    pub transaction: TransactionConfig,
    pub query: QueryConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub data_directory: PathBuf,
    pub page_size: usize,
    pub buffer_pool_size: usize,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)?;
        let mut config: Config = toml::from_str(&contents)?;
        
        // Override with environment variables
        config.apply_env_overrides()?;
        
        // Validate
        config.validate()?;
        
        Ok(config)
    }
    
    pub fn validate(&self) -> Result<()> {
        if self.storage.page_size % 4096 != 0 {
            return Err(Error::InvalidConfig("page_size must be multiple of 4096"));
        }
        Ok(())
    }
}
```

### Example Config File
```toml
[storage]
data_directory = "/var/aeternumdb/data"
page_size = 8192
buffer_pool_size = 1000

[network]
listen_address = "0.0.0.0:5432"
max_connections = 1000

[transaction]
isolation_level = "RepeatableRead"
lock_timeout_ms = 5000

[query]
max_memory_mb = 1024
query_timeout_sec = 300

[logging]
level = "info"
output = "file"
file_path = "/var/log/aeternumdb.log"
```

## ✅ Tests Required

- [ ] Load valid config
- [ ] Invalid config rejected
- [ ] Environment overrides work
- [ ] Default values applied
- [ ] Validation catches errors

## 🚀 Implementation Steps

**Day 1:** Config structures and loading  
**Day 2:** Validation and env overrides  
**Day 3:** Testing and documentation

---

**Ready to implement!** 🚀
