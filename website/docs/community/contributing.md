---
sidebar_position: 1
---

# 🤝 Contributing

[![PRs Welcome](https://img.shields.io/badge/PRs-Welcome-brightgreen.svg)](https://github.com/schivei/aeternum-db/pulls)
[![Good First Issue](https://img.shields.io/badge/Issues-Good%20First%20Issue-blue)](https://github.com/schivei/aeternum-db/issues?q=label%3A%22good+first+issue%22)

Thank you for considering contributing to **AeternumDB**! This is an ambitious open-source project and every contribution — from fixing typos to implementing new features — matters.

---

## 🚀 Ways to Contribute

| Type | How |
|---|---|
| 🐛 **Bug Reports** | [Open an issue](https://github.com/schivei/aeternum-db/issues/new) |
| 💡 **Feature Requests** | [Open a discussion](https://github.com/schivei/aeternum-db/discussions) |
| 📝 **Documentation** | Edit files in `website/docs/` or `docs/` |
| 🧪 **Tests** | Add tests to `src/AeternumDB.Core.Tests/` |
| 🔧 **Code** | Fork, implement, and open a PR |

---

## 🏗️ Project Structure

```
aeternum-db/
├── src/
│   ├── AeternumDB.Core/           # Production C# library
│   │   ├── Storage/               # Page I/O, buffer pool
│   │   ├── Sql/                   # Lexer, parser, AST
│   │   ├── Executor/              # Physical plan execution
│   │   ├── Query/                 # Optimizer, planner
│   │   └── Index/                 # Disk-backed BTree
│   └── AeternumDB.Core.Tests/     # xUnit test suite
├── website/                       # Docusaurus documentation site
├── docs/                          # Architecture docs
├── poc/                           # Proof-of-concept code
└── .github/workflows/             # CI/CD pipelines
```

---

## 🔧 Development Setup

### Prerequisites

- [.NET SDK 10.0+](https://dotnet.microsoft.com/download)
- [Node.js 18+](https://nodejs.org) (for website development)
- Git

### Building

```bash
git clone https://github.com/schivei/aeternum-db.git
cd aeternum-db/src
dotnet build AeternumDB.slnx -c Release
```

### Running Tests

```bash
cd src
dotnet test AeternumDB.slnx -c Release --no-build
```

### Coverage Report

```bash
cd src
dotnet test AeternumDB.slnx -c Release \
    --collect:"XPlat Code Coverage" \
    --settings AeternumDB.Core.Tests/coverage.runsettings
```

---

## 📋 Development Workflow

1. **Fork** the repository
2. **Create a branch**: `git checkout -b feature/my-feature`
3. **Write tests first** — all new code requires tests
4. **Implement** your change
5. **Verify coverage** — global coverage must remain >90%
6. **Open a PR** against `main`

---

## ✅ Code Standards

| Standard | Requirement |
|---|---|
| Language | C# 13 / .NET 10 |
| Style | Follow existing patterns in the module you edit |
| Tests | Every new public method must have at least one test |
| Coverage | Global coverage threshold: **>90%** |
| No panics | Production paths must return `Result<T>` — no `throw` for expected errors |
| Internal access | Use `InternalsVisibleTo` for test-only access to internals |

---

## 🔐 Licensing

| Component | License |
|---|---|
| `src/AeternumDB.Core/` | [AGPL-3.0](https://github.com/schivei/aeternum-db/blob/main/LICENSE.md) |
| `extensions/` | MIT |
| `drivers/`, `sdks/` | Apache 2.0 |

By contributing, you agree that your contributions will be licensed under the same license as the component you are contributing to.

---

## 💬 Community

- **GitHub Issues** — bug reports and feature requests
- **GitHub Discussions** — questions, ideas, and announcements

We follow the [Code of Conduct](./code-of-conduct.md). Please be respectful and inclusive.
