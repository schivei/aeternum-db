---
sidebar_position: 4
---

# 🗺️ Roadmap

AeternumDB is evolving through well-defined phases. Each phase builds on the previous one and is accompanied by documentation and a comprehensive test suite.

---

## ✅ Completed

### Etapa 1 — Storage Engine
- Page-based storage with CRC-32 integrity
- LRU buffer pool with async disk I/O
- Free-space bitmap and file growth strategy
- Full error handling with no panics

### Etapa 2 — SQL Parser
- SQL-92 lexer and parser
- Strongly-typed AST
- Catalog-aware validator

### Etapa 3 — Executor
- Physical plan execution engine
- All DML operations (INSERT, UPDATE, DELETE)
- All join types (Inner, Left, Right, Full, Cross)
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Full Value type system

### Etapa 4 — Query Planner & Optimizer
- Logical plan builder
- Cost-based optimizer (constant folding, predicate pushdown, projection pushdown, join reordering)
- Physical plan selection (SeqScan, IndexScan, NestedLoopJoin, HashJoin, External/InMemory Sort)
- Statistics registry with histogram support
- EXPLAIN output

### Etapa 5 — BTree Index
- Disk-backed B+ tree with node serialization
- Metadata page for crash-safe reopens
- Insert, search, delete, upsert
- Range scans via sibling links
- Bulk load
- Configurable fanout

### Etapa 6 — Tests & Coverage
- xUnit test suite for all modules
- >90% global code coverage
- InternalsVisibleTo for white-box testing
- Coverage settings with ExcludeByFile for generated code

### Etapa 7 — Documentation Website *(this site)*
- Docusaurus-based documentation site
- Full API reference
- Architecture guides
- Getting started guides

---

## 🔄 In Progress

### Etapa 8 — CI/CD Pipeline
- C# build + test pipeline on GitHub Actions
- Coverage gate (>90%)
- Automated documentation deployment to GitHub Pages

---

## 📌 Planned Phases

### Phase 2 — Durability & Concurrency
- **Write-Ahead Log (WAL)** — crash durability across process restarts
- **MVCC** — multi-version concurrency control for concurrent reads/writes
- **EXPLAIN ANALYZE** — with actual runtime statistics
- **Adaptive statistics** — live table stats from the storage engine

### Phase 3 — Distribution & Scalability
- **Native replication** — write replication via WAL streaming
- **Row and column sharding** — beyond the current application-layer sharding
- **Distributed cluster** — fault-tolerant cluster with consensus (Raft/Paxos)
- **gRPC + binary protocol** — for client-server communication
- **Observability** — metrics and distributed tracing

### Phase 4 — Drivers & SDKs
- ODBC driver (32/64 bit, cross-platform)
- JDBC driver
- SDKs: Rust, Python, JavaScript/TypeScript, Go, .NET, Java/Kotlin

### Phase 5 — Performance
- Page compression (LZ4/Zstd)
- Advanced eviction (LRU-K / Clock)
- Partition pruning in the query planner
- Materialized-view rewriting
- Sub-query flattening (correlated → join)
- Parallel query execution

### Phase 6 — Security & Compliance
- Encryption in transit (TLS)
- Encryption at rest (AES-256-GCM)
- Strong authentication (OAuth2, JWT)
- Advanced auditing via extensions
- LGPD/GDPR compliance tools

### Phase 7 — Extensions & Ecosystem
- WASM extension system for custom plugins
- Example extensions: GraphQL engine, Object Layer (OOP)
- Procedural languages (Python, JavaScript, .NET)
- Serverless deployment (AWS Lambda, Azure Functions, GCP Cloud Run)

### Phase 8 — Enterprise Edition
- Commercial licensing option
- Advanced management tools
- SLA and dedicated support
- High-availability configuration guides

---

## 💬 Influencing the Roadmap

The roadmap is **iterative** — phases can evolve in parallel based on community needs.

- **Vote on features** — 👍 react to [GitHub Issues](https://github.com/schivei/aeternum-db/issues)
- **Propose new phases** — open a [Discussion](https://github.com/schivei/aeternum-db/discussions)
- **Contribute** — see the [Contributing Guide](./contributing.md)

---

:::tip
Each milestone is accompanied by documentation updates and must meet the >90% coverage threshold before being considered complete.
:::
