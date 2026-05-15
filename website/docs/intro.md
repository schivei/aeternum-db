---
sidebar_position: 1
---

# 🌀 Introduction to AeternumDB

<div align="center">

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](https://github.com/schivei/aeternum-db/blob/main/LICENSE.md)
[![.NET](https://img.shields.io/badge/.NET-10.0-purple.svg)](https://dotnet.microsoft.com)
[![Status](https://img.shields.io/badge/Status-In%20Development-yellow.svg)](https://github.com/schivei/aeternum-db)
[![Language](https://img.shields.io/badge/Language-C%23-239120.svg)](https://learn.microsoft.com/en-us/dotnet/csharp/)

**The database engine that never stops.** *Aeternum* — Latin for *eternal*.

</div>

---

> **AeternumDB** is a high-performance, extensible, and modular Database Management System (DBMS) built in **C# (.NET 10)** with a focus on correctness, observability, and horizontal scalability.

---

## 🚀 Why AeternumDB?

Most database engines force you to pick one paradigm. **AeternumDB refuses that trade-off.**

| 🏆 Advantage | Description |
|---|---|
| **Multi-paradigm** | Relational, document, graph (GraphQL), and JSON in a single engine |
| **Memory-safe** | Written in modern C# with unsafe code only in hot storage/index paths |
| **Cost-based optimizer** | Rule-driven query optimizer with physical plan selection and EXPLAIN support |
| **Disk-backed BTree** | Persistent B+ tree index with range scans, bulk-load, and crash recovery |
| **ACID Transactions** | Full ACID compliance with multiple isolation levels |
| **Cloud-native** | Horizontal sharding and read replica primitives built in from day one |

---

## ✨ Key Features

### 🔐 Core Database Features

- **ACID Transactions** with multiple isolation levels
- **SQL-92** query language with AeternumDB extensions
- **Decimal Engine** for precise numeric operations
- **Versioning** with full history tracking
- **Multi-paradigm** data models in one engine

### 🌲 Storage & Indexing

- **Page-based storage** — fixed-size pages (4/8/16 KiB) with CRC-32 integrity checks
- **LRU Buffer Pool** — keeps hot pages in memory; async disk I/O for cold pages
- **Disk-backed BTree** — persistent B+ tree with node serialization, metadata page, and sibling links
- **Range Scans** — efficient ascending/descending traversal without revisiting internal nodes
- **Bulk Load** — populate large indexes orders of magnitude faster than single inserts

### ⚡ Query Engine

- **Cost-based optimizer** — constant folding, predicate pushdown, projection pushdown, join reordering
- **Physical plan selection** — SeqScan vs IndexScan, NestedLoopJoin vs HashJoin, InMemory vs ExternalSort
- **Statistics registry** — histogram-based selectivity estimation for accurate cost predictions
- **EXPLAIN** — human-readable plan trees annotated with cost and row estimates

### 🚀 Enterprise Capabilities

- **Horizontal Sharding** — each `StorageEngine` manages one shard; route at the application layer
- **Read Replicas** — open the same file with a read-only engine instance for read scale-out
- **Encryption** in transit and at rest *(roadmap)*
- **Strong Authentication** mechanisms *(roadmap)*
- **High Availability** with fault tolerance *(roadmap)*

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         AeternumDB                              │
│                                                                 │
│  ┌───────────┐   ┌──────────────┐   ┌────────────────────────┐ │
│  │ SQL Parser│──►│  Optimizer   │──►│   Physical Planner     │ │
│  └───────────┘   │ (rules+cost) │   │ (SeqScan/IndexScan/…)  │ │
│                  └──────────────┘   └───────────┬────────────┘ │
│                                                 │              │
│  ┌───────────────────────────────────────────── ▼ ──────────┐  │
│  │                    Executor                              │  │
│  └──────────────────────────────┬───────────────────────────┘  │
│                                 │                               │
│  ┌──────────────────────────────▼───────────────────────────┐  │
│  │                 Storage Engine                           │  │
│  │   BufferPool (LRU)  ◄──► FileManager (async I/O)         │  │
│  └──────────────────────────────┬───────────────────────────┘  │
│                                 │                               │
│  ┌──────────────────────────────▼───────────────────────────┐  │
│  │               BTree Index (B+ tree, disk-backed)         │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## ⚡ Quick Start in 30 Seconds

```csharp
// 1. Open a storage engine
var storage = await StorageEngine.OpenAsync("mydb.adb");

// 2. Create a BTree index
var btree = await BTree.OpenAsync(storage, metaPageId: 1);

// 3. Insert data
await btree.InsertAsync(42L, 1UL);
await btree.InsertAsync(99L, 2UL);

// 4. Search
var result = await btree.SearchAsync(42L);
Console.WriteLine($"Found: {result}");  // Found: 1

// 5. Range scan
await foreach (var (k, v) in btree.RangeScanAsync(1L, 100L))
    Console.WriteLine($"Key={k}, Value={v}");
```

→ See the full [Quick Start Guide](./getting-started/quick-start.md)

---

## 📦 Project Structure

| Module | Path | Description |
|---|---|---|
| **Core Library** | `src/AeternumDB.Core/` | Production C# library |
| **Storage** | `src/AeternumDB.Core/Storage/` | Page I/O, buffer pool, file manager |
| **SQL** | `src/AeternumDB.Core/Sql/` | Lexer, parser, AST, validator |
| **Executor** | `src/AeternumDB.Core/Executor/` | Physical plan execution, DML |
| **Query** | `src/AeternumDB.Core/Query/` | Logical/physical planner, optimizer |
| **Index** | `src/AeternumDB.Core/Index/` | Disk-backed BTree |
| **Tests** | `src/AeternumDB.Core.Tests/` | xUnit test suite (>90% coverage) |

---

## 🤝 Community

| | |
|---|---|
| 🐛 **Issues** | [Report bugs and request features](https://github.com/schivei/aeternum-db/issues) |
| 💬 **Discussions** | [Ask questions and share ideas](https://github.com/schivei/aeternum-db/discussions) |
| 🤝 **Contributing** | [Contribution guide](./community/contributing.md) |
| 📄 **License** | [AGPL-3.0](https://github.com/schivei/aeternum-db/blob/main/LICENSE.md) |

---

:::tip Ready to dive in?
Head over to the [Installation Guide](./getting-started/installation.md) to get AeternumDB set up, or jump straight to the [Quick Start](./getting-started/quick-start.md) for a working example in minutes.
:::
