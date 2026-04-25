# PR 2.2: Extension API & ABI Design

## 📋 Overview

**PR Number:** 2.2  
**Phase:** 2 - Extensibility  
**Priority:** 🔴 Critical  
**Estimated Effort:** 6 days  
**Dependencies:** PR 2.1 (WASM Runtime)

## 🎯 Objectives

Define stable extension API and ABI for host-guest communication, data marshaling, and lifecycle management.

## 📝 Detailed Prompt

```
Design and implement extension API with:
1. Stable C-compatible ABI
2. Host functions for database access
3. Extension lifecycle hooks (init, execute, cleanup)
4. Type-safe data marshaling between Rust and WASM
5. Error propagation and handling
6. Query execution from extensions
7. Logging from extensions
8. Configuration access
```

## 🏗️ Files to Create

1. `core/src/extensions/api.rs` - Extension API definition
2. `core/src/extensions/abi.rs` - ABI specification
3. `core/src/extensions/host_functions.rs` - Host function implementations
4. `core/src/extensions/marshaling.rs` - Type marshaling
5. `core/src/extensions/lifecycle.rs` - Lifecycle management
6. `sdks/rust-extension/src/lib.rs` - Rust SDK foundations

## 🔧 Implementation Details

### Extension API (Host Side)
```rust
pub trait Extension: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn init(&mut self) -> Result<()>;
    fn execute(&mut self, request: &[u8]) -> Result<Vec<u8>>;
    fn cleanup(&mut self) -> Result<()>;
}

// Host functions exported to WASM
#[no_mangle]
pub extern "C" fn ext_query(sql_ptr: *const u8, sql_len: usize, result_ptr: *mut u8) -> i32 {
    // Execute SQL query from extension
    // Returns: 0 on success, error code otherwise
}

#[no_mangle]
pub extern "C" fn ext_log(level: i32, msg_ptr: *const u8, msg_len: usize) {
    // Log message from extension
}

#[no_mangle]
pub extern "C" fn ext_get_config(key_ptr: *const u8, key_len: usize, value_ptr: *mut u8) -> i32 {
    // Get configuration value
}
```

### Extension API (Guest Side - Rust SDK)
```rust
// In rust-extension SDK
pub mod ext {
    extern "C" {
        pub fn ext_query(sql_ptr: *const u8, sql_len: usize, result_ptr: *mut u8) -> i32;
        pub fn ext_log(level: i32, msg_ptr: *const u8, msg_len: usize);
        pub fn ext_get_config(key_ptr: *const u8, key_len: usize, value_ptr: *mut u8) -> i32;
    }
}

pub fn query(sql: &str) -> Result<ResultSet> {
    unsafe {
        let mut result_buf = vec![0u8; 1024 * 1024]; // 1MB buffer
        let ret = ext::ext_query(
            sql.as_ptr(),
            sql.len(),
            result_buf.as_mut_ptr()
        );
        
        if ret == 0 {
            Ok(ResultSet::from_bytes(&result_buf)?)
        } else {
            Err(Error::from_code(ret))
        }
    }
}

pub fn log(level: LogLevel, message: &str) {
    unsafe {
        ext::ext_log(level as i32, message.as_ptr(), message.len());
    }
}
```

### Type Marshaling
```rust
pub struct Marshaler {
    // Convert between Rust types and WASM linear memory
}

impl Marshaler {
    pub fn write_string(&self, store: &mut Store, s: &str) -> Result<(i32, i32)> {
        // Allocate in WASM memory, return (ptr, len)
    }
    
    pub fn read_string(&self, store: &Store, ptr: i32, len: i32) -> Result<String> {
        // Read from WASM memory
    }
    
    pub fn write_result_set(&self, store: &mut Store, rs: &ResultSet) -> Result<i32> {
        // Serialize result set to WASM memory
    }
}
```

## ✅ Tests Required

- [ ] All host functions work
- [ ] Data marshaling correct
- [ ] Error propagation works
- [ ] Extension lifecycle
- [ ] Type safety
- [ ] Performance overhead

## 📚 Documentation

Create comprehensive API documentation including:
- Host function reference
- ABI specification
- Type marshaling guide
- Error handling
- Best practices

## 🚀 Implementation Steps

**Day 1-2:** API design and ABI definition  
**Day 3:** Host function implementations  
**Day 4:** Data marshaling  
**Day 5-6:** Rust SDK and testing

---

**Ready to implement!** 🚀
