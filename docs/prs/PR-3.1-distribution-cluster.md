# PR 3.1: Distribution — Cluster-Wide `objid` Generation

## 📋 Overview

**PR Number:** 3.1
**Phase:** 3 — Distribution
**Priority:** 🔴 Critical
**Estimated Effort:** 8 days
**Dependencies:** PR 1.5 (Query Executor), PR 1.6 (Catalog & Schema), PR 1.8 (WAL)

## 🎯 Objectives

Implement the distribution layer for AeternumDB, starting with cluster-wide unique `objid`
generation. Every stored row must carry an `objid` that is:

- Unique across all nodes in the cluster
- Monotonically increasing within a single node
- Compact (128-bit integer represented as `u128`)
- Not user-settable from SQL

This PR also lays the groundwork for the distributed coordination primitives (node registry,
epoch management) that future distribution PRs will build upon.

## 📝 Detailed Prompt for Implementation

```
Implement cluster-wide objid generation for AeternumDB with the following requirements:

1. **objid Structure**
   - 128-bit value: [48-bit timestamp ms | 16-bit node-id | 64-bit sequence]
   - Timestamp ensures rough global ordering
   - Node-id prevents collisions across cluster nodes
   - Sequence ensures uniqueness within a single millisecond on the same node
   - Display as a 32-character hexadecimal string or a u128 integer

2. **Node Registry**
   - Each cluster node registers at startup with a central coordinator (or via Raft consensus)
   - Node receives a unique 16-bit node-id (0–65535 → up to 65536 nodes)
   - Node-id is persisted to the node's local storage so it survives restarts
   - If the coordinator is unavailable, fall back to a cached local node-id

3. **Generation API**
   - `ObjidGenerator::next() -> u128` — generates the next objid for this node
   - `ObjidGenerator::parse(hex: &str) -> Result<u128>` — parses a hex objid
   - `ObjidGenerator::format(id: u128) -> String` — formats as hex string
   - Thread-safe: use an atomic counter for the sequence portion

4. **Integration with Storage Layer**
   - Every `INSERT` automatically appends an `objid` column to the stored tuple
   - `objid` is always the first physical column in the tuple layout (index 0)
   - `objid` is read-only from SQL; attempts to INSERT/UPDATE it are rejected
   - Row lookups by objid are O(1) via a dedicated hash index

5. **Reference Columns & Joins**
   - Reference-type columns store the `objid` of the target row, not a surrogate key
   - Join resolution: given a reference column value, look up the target row by its objid
   - Vector-reference columns store `[objid, ...]` — a list of target objids
   - The lookup index is maintained automatically by the storage layer

6. **SQL Surface**
   - `SELECT objid, ... FROM table` — user can read objid
   - `WHERE objid = '0000…'` — filter by objid (hex or u128 literal)
   - `objid` is a reserved identifier; CREATE TABLE columns cannot be named `objid`
   - `INSERT` ignores any user-supplied `objid` value

7. **Coordinator Bootstrap**
   - Single-node mode: node-id is always 0; no coordinator needed
   - Multi-node mode: first node to start becomes the coordinator
   - Coordinator stores the node registry in a dedicated system table: `adb_metadata.nodes`
   - Node registration uses a simple RPC call (TCP + length-prefixed protobuf)

8. **Performance Requirements**
   - `next()` must be lock-free (atomic) on the hot path
   - Throughput: ≥ 1,000,000 objid/s on a single node
   - Coordinator registration roundtrip: < 50 ms in a LAN cluster
```

## 📁 Files to Create / Modify

```
core/src/objid/
    mod.rs          — ObjidGenerator, ObjidFormat trait
    generator.rs    — atomic counter + node-id logic
    format.rs       — hex encode/decode helpers

core/src/storage/
    tuple.rs        — add objid as first physical column in every tuple
    (modify existing)

core/src/distribution/
    mod.rs          — Distribution module re-exports
    node_registry.rs — NodeRegistry trait + single-node implementation
    coordinator.rs  — Coordinator (Phase 3.2+; stub in this PR)

core/tests/objid_tests.rs
docs/objid-design.md
```

## ✅ Tests Required

- [ ] `test_objid_uniqueness` — 1M generated objids contain no duplicates
- [ ] `test_objid_monotonic` — successive objids are non-decreasing
- [ ] `test_objid_parse_format` — round-trip: `format(parse(s)) == s`
- [ ] `test_objid_reserved_name` — `CREATE TABLE t (objid INT)` returns a parse error
- [ ] `test_objid_insert_ignored` — explicit `objid` value in INSERT is silently ignored
- [ ] `test_objid_select_readable` — `SELECT objid FROM t` returns a hex string
- [ ] `test_reference_col_stores_objid` — reference-column value equals target row objid
- [ ] `test_single_node_mode` — generator works with node-id 0 and no coordinator
- [ ] `test_concurrent_generation` — 16 threads each generate 100k objids; no duplicates

## 📊 Performance Targets

| Metric | Target |
|--------|--------|
| `next()` throughput (single thread) | ≥ 1,000,000 / s |
| `next()` throughput (16 threads) | ≥ 8,000,000 / s |
| Coordinator registration latency | < 50 ms (LAN) |
| Row lookup by objid | < 1 µs (cached) |

## 📚 Documentation Requirements

- `docs/objid-design.md` — objid bit layout, node-id assignment, hex format, SQL surface
- `docs/sql-reference.md` — update "System Columns" section with objid read/filter examples
- `docs/prs/PR-3.1-distribution-cluster.md` — this file

## ✔️ Acceptance Criteria

- [ ] `ObjidGenerator::next()` is lock-free and generates unique values under concurrent load
- [ ] Every `INSERT` stores an auto-generated objid; user-supplied values are rejected
- [ ] `SELECT objid` returns a hex string; `WHERE objid = '…'` filters correctly
- [ ] `CREATE TABLE t (objid INT)` is rejected with a helpful error
- [ ] Single-node mode works without any coordinator; node-id defaults to 0
- [ ] All tests pass; no clippy warnings; cargo fmt clean
- [ ] `docs/objid-design.md` explains the full bit layout and node assignment protocol

## 🔗 What Comes Next

| Work Item | Target PR |
|-----------|-----------|
| Multi-node coordinator (Raft-based node registry) | PR 3.2 |
| Cross-node row lookup via objid routing | PR 3.2 |
| Distributed transaction coordination | PR 3.3 |
| Sharding & data placement policies | PR 3.4 |
