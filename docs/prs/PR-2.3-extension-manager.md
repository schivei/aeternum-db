# PR 2.3: Extension Manager & Registry

## 📋 Overview

**PR Number:** 2.3  
**Phase:** 2 - Extensibility  
**Priority:** 🟡 High  
**Estimated Effort:** 4 days  
**Dependencies:** PR 2.2 (Extension API)

## 🎯 Objectives

Implement extension registry, discovery, version management, and dependency resolution with hot-reload capability.

## 📝 Detailed Prompt

```
Implement extension manager with:
1. Extension manifest (TOML format)
2. Extension discovery and loading
3. Version management
4. Dependency resolution
5. Hot reload capability
6. Extension metadata storage
7. Extension lifecycle management
```

## 🏗️ Files to Create

1. `core/src/extensions/manager.rs` - Extension manager
2. `core/src/extensions/registry.rs` - Extension registry
3. `core/src/extensions/manifest.rs` - Manifest parsing
4. `core/src/extensions/loader.rs` - Extension loading
5. `extensions/hello_world/extension.toml` - Example manifest

## 🔧 Implementation Details

### Extension Manifest
```toml
[extension]
name = "graphql-engine"
version = "0.1.0"
author = "AeternumDB Contributors"
license = "MIT"
description = "GraphQL query language extension"

[dependencies]
aeternumdb = ">=0.1.0"

[capabilities]
query_language = true
data_type = false
storage_engine = false

[resources]
max_memory_mb = 100
max_cpu_percent = 50
```

### Extension Manager
```rust
pub struct ExtensionManager {
    registry: Arc<RwLock<ExtensionRegistry>>,
    runtime: Arc<WasmRuntime>,
    extensions_dir: PathBuf,
}

impl ExtensionManager {
    pub async fn load_all(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.extensions_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension() == Some(OsStr::new("wasm")) {
                self.load_extension(&entry.path()).await?;
            }
        }
        
        Ok(())
    }
    
    pub async fn load_extension(&self, path: &Path) -> Result<ExtensionId> {
        // Load manifest
        let manifest_path = path.with_extension("toml");
        let manifest = ExtensionManifest::load(&manifest_path)?;
        
        // Validate dependencies
        self.validate_dependencies(&manifest)?;
        
        // Load WASM module
        let wasm_bytes = fs::read(path).await?;
        let module = self.runtime.load_module(&wasm_bytes)?;
        
        // Create extension
        let extension = WasmExtension {
            manifest,
            module,
            state: ExtensionState::Loaded,
        };
        
        // Register
        let ext_id = self.registry.write().await.register(extension)?;
        
        // Initialize
        self.initialize_extension(ext_id).await?;
        
        Ok(ext_id)
    }
    
    pub async fn unload_extension(&self, ext_id: ExtensionId) -> Result<()> {
        // Cleanup
        if let Some(ext) = self.registry.read().await.get(ext_id) {
            ext.cleanup().await?;
        }
        
        // Unregister
        self.registry.write().await.unregister(ext_id)?;
        
        Ok(())
    }
    
    pub async fn reload_extension(&self, ext_id: ExtensionId) -> Result<()> {
        let ext = self.registry.read().await.get(ext_id)
            .ok_or(Error::ExtensionNotFound)?;
        
        let path = ext.manifest.path.clone();
        
        // Unload
        self.unload_extension(ext_id).await?;
        
        // Reload
        self.load_extension(&path).await?;
        
        Ok(())
    }
}
```

### Extension Registry
```rust
pub struct ExtensionRegistry {
    extensions: HashMap<ExtensionId, WasmExtension>,
    by_name: HashMap<String, ExtensionId>,
    next_id: AtomicU64,
}

impl ExtensionRegistry {
    pub fn register(&mut self, extension: WasmExtension) -> Result<ExtensionId> {
        // Check if already registered
        if self.by_name.contains_key(&extension.manifest.name) {
            return Err(Error::ExtensionAlreadyExists);
        }
        
        // Allocate ID
        let ext_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        
        // Store
        self.by_name.insert(extension.manifest.name.clone(), ext_id);
        self.extensions.insert(ext_id, extension);
        
        Ok(ext_id)
    }
    
    pub fn get(&self, ext_id: ExtensionId) -> Option<&WasmExtension> {
        self.extensions.get(&ext_id)
    }
    
    pub fn get_by_name(&self, name: &str) -> Option<&WasmExtension> {
        self.by_name.get(name)
            .and_then(|id| self.extensions.get(id))
    }
    
    pub fn list(&self) -> Vec<ExtensionInfo> {
        self.extensions.values()
            .map(|ext| ExtensionInfo {
                id: ext.id,
                name: ext.manifest.name.clone(),
                version: ext.manifest.version.clone(),
                state: ext.state,
            })
            .collect()
    }
}
```

## ✅ Tests Required

- [ ] Load extension
- [ ] Manifest parsing
- [ ] Version checking
- [ ] Dependency resolution
- [ ] Hot reload
- [ ] Multiple extensions
- [ ] Extension discovery

## 🚀 Implementation Steps

**Day 1:** Manifest format and parsing  
**Day 2:** Extension manager and registry  
**Day 3:** Dependency resolution and loading  
**Day 4:** Hot reload and testing

---

**Ready to implement!** 🚀
