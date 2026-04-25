# PR 2.1: WASM Runtime Integration

## 📋 Overview

**PR Number:** 2.1  
**Phase:** 2 - Extensibility  
**Priority:** 🔴 Critical  
**Estimated Effort:** 5 days  
**Dependencies:** Phase 1 complete

## 🎯 Objectives

Integrate WASM runtime (wasmtime) for loading and executing extension modules with proper sandboxing and resource limits.

## 📝 Detailed Prompt

```
Implement WASM runtime integration with:
1. wasmtime runtime integration
2. Module loading from .wasm files
3. Module instantiation with resource limits
4. Memory isolation between extensions
5. CPU time limits and enforcement
6. WASI support for basic I/O
7. Error handling and recovery
8. Performance: <5% overhead
```

## 🏗️ Files to Create

1. `core/src/extensions/mod.rs` - Public API
2. `core/src/extensions/runtime.rs` - WASM runtime
3. `core/src/extensions/module.rs` - Module management
4. `core/src/extensions/memory.rs` - Memory isolation
5. `core/src/extensions/limits.rs` - Resource limits
6. `core/tests/extensions_tests.rs` - Tests

## 🔧 Implementation Details

### WASM Runtime
```rust
use wasmtime::*;

pub struct WasmRuntime {
    engine: Engine,
    store_limits: StoreLimits,
}

impl WasmRuntime {
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        let mut engine_config = Config::new();
        engine_config
            .consume_fuel(true)
            .max_wasm_stack(config.max_stack_size);
        
        let engine = Engine::new(&engine_config)?;
        
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(config.max_memory)
            .instances(config.max_instances)
            .build();
        
        Ok(Self { engine, store_limits })
    }
    
    pub fn load_module(&self, wasm_bytes: &[u8]) -> Result<Module> {
        Module::new(&self.engine, wasm_bytes)
    }
    
    pub fn instantiate(&self, module: &Module) -> Result<Instance> {
        let mut store = Store::new(&self.engine, ());
        store.limiter(|_| &self.store_limits);
        store.add_fuel(10_000_000)?; // 10M instructions
        
        let instance = Instance::new(&mut store, module, &[])?;
        
        Ok(instance)
    }
}
```

### Extension Module
```rust
pub struct ExtensionModule {
    name: String,
    module: Module,
    runtime: Arc<WasmRuntime>,
}

impl ExtensionModule {
    pub async fn call_function(
        &self,
        func_name: &str,
        args: &[Value],
    ) -> Result<Vec<Value>> {
        let instance = self.runtime.instantiate(&self.module)?;
        
        let func = instance
            .get_func(&mut store, func_name)
            .ok_or(Error::FunctionNotFound)?;
        
        let mut results = vec![Val::I32(0); func.ty(&store).results().len()];
        func.call(&mut store, args, &mut results)?;
        
        Ok(results)
    }
}
```

## ✅ Tests Required

- [ ] Load valid WASM module
- [ ] Reject invalid WASM
- [ ] Memory limits enforced
- [ ] CPU limits enforced
- [ ] Multiple concurrent modules
- [ ] Module isolation
- [ ] Error handling

## 📊 Performance Targets

| Metric | Target |
|--------|--------|
| Module load time | <100ms |
| Function call overhead | <5% |
| Memory per module | <10MB |
| Max concurrent modules | >100 |

## 🚀 Implementation Steps

**Day 1-2:** wasmtime integration and basic loading  
**Day 3:** Resource limits and isolation  
**Day 4:** Error handling and testing  
**Day 5:** Performance optimization

---

**Ready to implement!** 🚀
