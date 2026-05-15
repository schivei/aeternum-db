---
sidebar_position: 1
---

# 📦 Installation

[![NuGet](https://img.shields.io/badge/NuGet-AeternumDB-blue?logo=nuget)](https://nuget.org)
[![.NET](https://img.shields.io/badge/.NET-10.0-purple.svg)](https://dotnet.microsoft.com)

Get AeternumDB running in your project in under 5 minutes.

---

## Prerequisites

| Requirement | Minimum Version |
|---|---|
| **.NET SDK** | 10.0+ |
| **OS** | Windows, Linux, macOS |
| **Architecture** | x64, ARM64 |

---

## Option 1 — Build from Source

AeternumDB is currently in active development. The recommended approach is to build from source and reference the project directly.

### 1. Clone the Repository

```bash
git clone https://github.com/schivei/aeternum-db.git
cd aeternum-db
```

### 2. Build the Core Library

```bash
cd src
dotnet build AeternumDB.slnx -c Release
```

### 3. Run the Test Suite

```bash
dotnet test AeternumDB.slnx -c Release --no-build
```

You should see output similar to:

```
✅ Passed: 180+ tests
📊 Coverage: >90% global
```

### 4. Reference in Your Project

Add a project reference to your `.csproj`:

```xml
<ItemGroup>
  <ProjectReference Include="../aeternum-db/src/AeternumDB.Core/AeternumDB.Core.csproj" />
</ItemGroup>
```

---

## Option 2 — NuGet Package *(Coming Soon)*

:::info
AeternumDB will be published to NuGet once the API stabilises. Watch the repository for announcements.
:::

```bash
# Future — not yet published
dotnet add package AeternumDB.Core
```

---

## Verifying Your Installation

Create a simple smoke test to verify the storage engine starts correctly:

```csharp
using AeternumDB.Core.Storage;

var storage = await StorageEngine.OpenAsync("smoke-test.adb");
var pageId  = await storage.AllocatePageAsync();

Console.WriteLine($"✅ Storage engine ready. First page: {pageId}");

await storage.DisposeAsync();
```

Run it:

```bash
dotnet run
# ✅ Storage engine ready. First page: 1
```

---

## Next Steps

- [Quick Start](./quick-start.md) — A full working example in minutes
- [Configuration](./configuration.md) — Tune page size, buffer pool, and more
- [SQL Reference](../guides/sql-reference.md) — Full SQL dialect documentation
