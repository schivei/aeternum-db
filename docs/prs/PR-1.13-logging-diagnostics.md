# PR 1.13: Logging & Diagnostics

## 📋 Overview

**PR Number:** 1.13  
**Phase:** 1 - Core Foundation  
**Priority:** 🟢 Medium  
**Estimated Effort:** 3 days  
**Dependencies:** None

## 🎯 Objectives

Implement structured logging, performance metrics, and debug tracing using the Rust tracing ecosystem.

## 📝 Detailed Prompt

```
Implement logging and diagnostics with:
1. Structured logging using `tracing`
2. Multiple log levels (ERROR, WARN, INFO, DEBUG, TRACE)
3. Performance metrics collection
4. Span tracing for request tracking
5. Output to file/console/both
6. Log rotation
7. Minimal performance overhead (<2%)
```

## 🏗️ Files to Create

1. `core/src/logging/mod.rs` - Logging API
2. `core/src/metrics/mod.rs` - Metrics collection
3. `core/src/metrics/counters.rs` - Performance counters
4. `core/tests/logging_tests.rs` - Tests

## 🔧 Implementation Details

### Logging Setup
```rust
use tracing::{info, warn, error, debug, trace, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_logging(config: &LoggingConfig) -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_level(true)
                .with_file(true)
                .with_line_number(true)
        )
        .with(tracing_subscriber::filter::LevelFilter::from_level(config.level))
        .init();
    
    Ok(())
}

#[instrument(skip(storage))]
pub async fn execute_query(storage: &Storage, sql: &str) -> Result<ResultSet> {
    info!("Executing query: {}", sql);
    
    let start = Instant::now();
    let result = storage.execute(sql).await?;
    let duration = start.elapsed();
    
    info!(duration_ms = duration.as_millis(), rows = result.len(), "Query completed");
    
    Ok(result)
}
```

### Metrics Collection
```rust
use metrics::{counter, histogram, gauge};

pub struct Metrics {
    pub queries_total: Counter,
    pub query_duration: Histogram,
    pub active_connections: Gauge,
}

impl Metrics {
    pub fn record_query(&self, duration: Duration) {
        counter!("queries_total").increment(1);
        histogram!("query_duration_ms").record(duration.as_millis() as f64);
    }
    
    pub fn inc_connections(&self) {
        gauge!("active_connections").increment(1.0);
    }
}
```

## ✅ Tests Required

- [ ] Logging at all levels
- [ ] Structured fields
- [ ] Metrics collection
- [ ] Performance overhead test
- [ ] Log rotation

## 📊 Performance Targets

| Metric | Target |
|--------|--------|
| Logging overhead | <2% |
| Metrics overhead | <1% |

## 🚀 Implementation Steps

**Day 1:** Logging setup and integration  
**Day 2:** Metrics collection  
**Day 3:** Testing and optimization

---

**Ready to implement!** 🚀
