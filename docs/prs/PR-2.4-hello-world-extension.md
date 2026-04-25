# PR 2.4: Example Extension - Hello World

## 📋 Overview

**PR Number:** 2.4  
**Phase:** 2 - Extensibility  
**Priority:** 🟢 Medium  
**Estimated Effort:** 3 days  
**Dependencies:** PR 2.3 (Extension Manager)

## 🎯 Objectives

Create simple "Hello World" extension demonstrating extension development, testing framework, and CI/CD integration.

## 📝 Detailed Prompt

```
Create Hello World extension with:
1. Simple Rust crate compiled to WASM
2. Exports extension entry points
3. Calls host functions (logging, config)
4. Returns data to host
5. Complete test suite
6. CI/CD for building WASM
7. Comprehensive tutorial
```

## 🏗️ Files to Create

1. `extensions/hello_world/Cargo.toml`
2. `extensions/hello_world/src/lib.rs`
3. `extensions/hello_world/extension.toml`
4. `extensions/hello_world/README.md`
5. `extensions/hello_world/tests/integration_tests.rs`
6. `extensions/hello_world/build.sh`

## 🔧 Implementation Details

### Extension Code (lib.rs)
```rust
use aeternumdb_extension::*;

#[extension_init]
fn init() -> Result<(), Error> {
    log::info!("Hello World extension initialized");
    
    let version = get_config("version")?;
    log::info!("AeternumDB version: {}", version);
    
    Ok(())
}

#[extension_function]
fn hello(name: &str) -> Result<String, Error> {
    log::info!("hello() called with name: {}", name);
    Ok(format!("Hello, {}!", name))
}

#[extension_function]
fn count_rows(table_name: &str) -> Result<i64, Error> {
    let sql = format!("SELECT COUNT(*) FROM {}", table_name);
    let result = execute_query(&sql)?;
    
    let count = result.get(0, 0).as_i64()?;
    log::info!("Table {} has {} rows", table_name, count);
    
    Ok(count)
}

#[extension_cleanup]
fn cleanup() -> Result<(), Error> {
    log::info!("Hello World extension cleanup");
    Ok(())
}
```

### Cargo.toml
```toml
[package]
name = "hello_world"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
aeternumdb-extension = { path = "../../sdks/rust-extension" }

[profile.release]
opt-level = "z"
lto = true
```

### extension.toml
```toml
[extension]
name = "hello_world"
version = "0.1.0"
author = "AeternumDB Team"
license = "MIT"

[capabilities]
query_language = false
data_type = false

[resources]
max_memory_mb = 10
max_cpu_percent = 10
```

### Build Script
```bash
#!/bin/bash
set -e

# Build WASM module
cargo build --target wasm32-unknown-unknown --release

# Optimize WASM
wasm-opt -Oz -o hello_world.wasm \
    target/wasm32-unknown-unknown/release/hello_world.wasm

# Copy to extensions directory
cp hello_world.wasm ../../bin/extensions/
cp extension.toml ../../bin/extensions/hello_world.toml

echo "Built hello_world extension"
```

## ✅ Tests Required

- [ ] Extension loads successfully
- [ ] Functions execute correctly
- [ ] Logging works
- [ ] Config access works
- [ ] Query execution works
- [ ] Cleanup runs

## 📚 Documentation

Create comprehensive tutorial:
- Extension development overview
- Building and testing
- Debugging tips
- Best practices

## 🚀 Implementation Steps

**Day 1:** Extension code and structure  
**Day 2:** Build system and testing  
**Day 3:** Documentation and CI/CD

---

**Ready to implement!** 🚀
