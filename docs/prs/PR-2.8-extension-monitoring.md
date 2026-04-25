# PR 2.8: Extension Performance & Monitoring

## 📋 Overview

**PR Number:** 2.8  
**Phase:** 2 - Extensibility  
**Priority:** 🟢 Medium  
**Estimated Effort:** 3 days  
**Dependencies:** PR 2.6 (Extension Security)

## 🎯 Objectives

Implement performance monitoring, metrics collection, profiling tools, and performance best practices for extensions.

## 📝 Detailed Prompt

```
Implement extension monitoring with:
1. Execution time tracking
2. Memory usage monitoring
3. Call frequency statistics
4. Slow extension detection
5. Performance reports
6. Profiling integration
7. Performance dashboard
```

## 🏗️ Files to Create

1. `core/src/extensions/metrics.rs` - Metrics collection
2. `core/src/extensions/profiler.rs` - Profiling
3. `core/src/extensions/dashboard.rs` - Monitoring dashboard
4. `core/tests/metrics_tests.rs` - Tests

## 🔧 Implementation

### Metrics Collection
```rust
pub struct ExtensionMetrics {
    extension_id: ExtensionId,
    extension_name: String,
    
    // Counters
    function_calls: HashMap<String, AtomicU64>,
    errors: AtomicU64,
    
    // Timing
    total_execution_time: AtomicU64,
    function_times: HashMap<String, AtomicU64>,
    
    // Resources
    peak_memory: AtomicU64,
    total_cpu_time: AtomicU64,
    
    // Hosts calls
    query_count: AtomicU64,
    log_count: AtomicU64,
}

impl ExtensionMetrics {
    pub fn record_call(&self, function: &str, duration: Duration) {
        self.function_calls
            .entry(function.to_string())
            .or_insert(AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
        
        self.function_times
            .entry(function.to_string())
            .or_insert(AtomicU64::new(0))
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
        
        self.total_execution_time
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }
    
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_memory(&self, bytes: u64) {
        self.peak_memory.fetch_max(bytes, Ordering::Relaxed);
    }
    
    pub fn generate_report(&self) -> PerformanceReport {
        PerformanceReport {
            extension_name: self.extension_name.clone(),
            total_calls: self.function_calls.values()
                .map(|c| c.load(Ordering::Relaxed))
                .sum(),
            total_time_ms: self.total_execution_time.load(Ordering::Relaxed) / 1000,
            error_rate: self.errors.load(Ordering::Relaxed) as f64 
                      / self.total_calls() as f64,
            peak_memory_mb: self.peak_memory.load(Ordering::Relaxed) / (1024 * 1024),
            function_stats: self.collect_function_stats(),
        }
    }
}
```

### Performance Report
```rust
#[derive(Debug, Clone)]
pub struct PerformanceReport {
    pub extension_name: String,
    pub total_calls: u64,
    pub total_time_ms: u64,
    pub error_rate: f64,
    pub peak_memory_mb: u64,
    pub function_stats: Vec<FunctionStats>,
}

#[derive(Debug, Clone)]
pub struct FunctionStats {
    pub name: String,
    pub calls: u64,
    pub total_time_ms: u64,
    pub avg_time_ms: f64,
    pub p50_time_ms: f64,
    pub p99_time_ms: f64,
}

impl PerformanceReport {
    pub fn print(&self) {
        println!("Extension Performance Report: {}", self.extension_name);
        println!("  Total Calls: {}", self.total_calls);
        println!("  Total Time: {} ms", self.total_time_ms);
        println!("  Error Rate: {:.2}%", self.error_rate * 100.0);
        println!("  Peak Memory: {} MB", self.peak_memory_mb);
        println!("\nFunction Statistics:");
        
        for stat in &self.function_stats {
            println!("  {}", stat.name);
            println!("    Calls: {}", stat.calls);
            println!("    Avg Time: {:.2} ms", stat.avg_time_ms);
            println!("    P99 Time: {:.2} ms", stat.p99_time_ms);
        }
    }
}
```

### Slow Extension Detection
```rust
pub struct SlowExtensionDetector {
    threshold_ms: u64,
    violations: Arc<RwLock<Vec<SlowCallViolation>>>,
}

#[derive(Debug, Clone)]
pub struct SlowCallViolation {
    timestamp: Timestamp,
    extension_name: String,
    function_name: String,
    duration_ms: u64,
}

impl SlowExtensionDetector {
    pub fn check(&self, 
                 extension_name: &str,
                 function_name: &str, 
                 duration: Duration) {
        let duration_ms = duration.as_millis() as u64;
        
        if duration_ms > self.threshold_ms {
            warn!("Slow extension call detected: {}.{} took {} ms",
                  extension_name, function_name, duration_ms);
            
            let violation = SlowCallViolation {
                timestamp: Timestamp::now(),
                extension_name: extension_name.to_string(),
                function_name: function_name.to_string(),
                duration_ms,
            };
            
            self.violations.write().push(violation);
        }
    }
}
```

## ✅ Tests Required

- [ ] Metrics collected accurately
- [ ] Low overhead (<2%)
- [ ] Reports generated correctly
- [ ] Slow calls detected
- [ ] Dashboard displays metrics

## 🚀 Implementation Steps

**Day 1:** Metrics collection  
**Day 2:** Performance reports  
**Day 3:** Dashboard and documentation

---

**Ready to implement!** 🚀
