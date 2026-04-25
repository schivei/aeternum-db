# 🌀 AeternumDB

<div align="center">

**High-Performance, Extensible Database Management System**

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE.md)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![Status](https://img.shields.io/badge/Status-In%20Development-yellow.svg)]()

*A modular DBMS written in Rust, supporting multiple data paradigms and modern execution environments*

[Features](#-key-features) •
[Installation](#-installation-and-usage) •
[Documentation](#-documentation) •
[Contributing](#-contributing) •
[Roadmap](#-roadmap)

</div>

---

## 📖 Overview

AeternumDB is a high-performance, extensible, and modular Database Management System (DBMS) designed to support multiple data paradigms with a focus on horizontal scalability, fault tolerance, and security.

**Project Status:** 🚧 In active development

### Why AeternumDB?

- **Multi-paradigm Support:** Relationships, objects, graphs (GraphQL), JSON/JSON2
- **Production-Ready:** Built with Rust for memory safety and performance
- **Cloud-Native:** Designed for modern distributed architectures
- **Extensible:** WASM-based plugin system for custom functionality
- **Open Source:** Community-driven with hybrid licensing model

---

## ✨ Key Features

<table>
<tr>
<td width="50%">

### 🔐 Core Database Features
- **ACID Transactions** with multiple isolation levels
- **SQL-like** query language support
- **Decimal Engine** for precise numeric operations
- **Versioning** with full history tracking
- **Multi-paradigm** data models

</td>
<td width="50%">

### 🚀 Enterprise Capabilities
- **Horizontal Scalability** (CockroachDB-style)
- **Replication** and **Sharding** (rows & columns)
- **Encryption** in transit and at rest
- **Strong Authentication** mechanisms
- **High Availability** with fault tolerance

</td>
</tr>
<tr>
<td width="50%">

### 🔌 Extensibility
- **WASM Extensions** for custom data types
- **Plugin System** for procedural languages
- **Custom Indexes** and optimization strategies
- **Integration Connectors** (Kafka, MQTT, etc.)

</td>
<td width="50%">

### 🌐 Deployment Options
- **Lite Mode:** Single local instance
- **Containerized:** Docker & Kubernetes ready
- **Serverless:** AWS Lambda, Azure, GCP
- **Multi-cloud:** Flexible deployment strategies

</td>
</tr>
</table>

### Supported Drivers & SDKs

| Platform | Status | License |
|----------|--------|---------|
| **ODBC** (32/64-bit) | 🚧 Planned | Apache 2.0 |
| **JDBC** | 🚧 Planned | Apache 2.0 |
| **Rust SDK** | 🚧 Planned | Apache 2.0 |
| **Python SDK** | 🚧 Planned | Apache 2.0 |
| **JavaScript/TypeScript** | 🚧 Planned | Apache 2.0 |
| **Go SDK** | 🚧 Planned | Apache 2.0 |
| **Java/Kotlin** | 🚧 Planned | Apache 2.0 |
| **.NET Core** | 🚧 Planned | Apache 2.0 |
| **C++** | 🚧 Planned | Apache 2.0 |

---

## 📂 Repository Structure

```plaintext
aeternumdb/
├── core/               # Main database engine (Rust, AGPLv3.0)
│   ├── src/           # Source code
│   └── tests/         # Unit tests
├── extensions/         # WASM plugins (MIT)
├── drivers/           # Database drivers (Apache 2.0)
│   ├── odbc/         # ODBC driver
│   ├── jdbc/         # JDBC driver
│   ├── grpc/         # gRPC protocol
│   └── binary/       # Binary protocol
├── sdks/              # Client SDKs (Apache 2.0)
│   ├── rust/
│   ├── python/
│   ├── javascript/
│   ├── go/
│   ├── java/
│   ├── dotnet/
│   └── cpp/
├── deployment/        # Deployment configurations
│   ├── lite/         # Single instance mode
│   ├── container/    # Docker & Kubernetes
│   └── serverless/   # Cloud functions
├── docs/              # Documentation
└── tests/             # Integration tests
```

---

## 🚀 Installation and Usage

### Prerequisites

Before you begin, ensure you have the following installed:

- **Rust** (latest stable version)
- **Cargo** for package management
- **Docker** (optional, for containerized deployment)
- **Kubernetes** (optional, for orchestration)

### Quick Start

#### 1. Clone the Repository

```bash
git clone https://github.com/schivei/aeternum-db.git
cd aeternum-db
```

#### 2. Build the Core Engine

```bash
cd core
cargo build --release
```

#### 3. Run in Lite Mode

Start a single local instance:

```bash
./target/release/aeternumdb --lite
```

### Docker Deployment

#### Build Docker Image

```bash
docker build -t aeternumdb:latest -f deployment/container/Dockerfile .
```

#### Run Container

```bash
docker run -d \
  --name aeternumdb \
  -p 5432:5432 \
  aeternumdb:latest
```

### Kubernetes Deployment

```bash
kubectl apply -f deployment/container/kubernetes.yaml
```

---

## 📜 Licensing

AeternumDB uses a **hybrid licensing model** to balance community growth and commercial viability:

| Component | License | Purpose |
|-----------|---------|---------|
| **Core Engine** | [AGPLv3.0](https://www.gnu.org/licenses/agpl-3.0.html) | Ensures SaaS providers share improvements |
| **SDKs & Drivers** | [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) | Corporate-friendly, permissive |
| **WASM Extensions** | [MIT](https://opensource.org/licenses/MIT) | Maximum flexibility for developers |
| **Enterprise Edition** | Commercial | Future monetization (planned) |

📄 See [LICENSE.md](LICENSE.md) for complete details.

---

## 🤝 Contributing

We welcome contributions from the community! AeternumDB is an ambitious project that thrives on collaboration.

### How to Contribute

1. **Fork** the repository
2. **Create** a feature branch (`feature/your-feature-name`)
3. **Follow** our coding standards and guidelines
4. **Write** tests for your changes
5. **Submit** a Pull Request with a clear description

### Important Documents

- 📋 [**CONTRIBUTING.md**](CONTRIBUTING.md) - Contribution guidelines and process
- 📜 [**CODE_OF_CONDUCT.md**](CODE_OF_CONDUCT.md) - Community standards
- 🔒 [**SECURITY.md**](SECURITY.md) - Security policy and vulnerability reporting

### Development Resources

- 📚 [**Implementation Plan**](docs/IMPLEMENTATION_PLAN.md) - Complete project roadmap
- 🔧 [**PR Guide**](docs/PR_GUIDE.md) - How to implement features
- 📊 [**Phase 1 Plan**](docs/phase1-prs.md) - Core foundation details

---

## 📖 Documentation

### Core Documentation

| Document | Description |
|----------|-------------|
| [**COPILOT.md**](COPILOT.md) | Architecture whitepaper and design philosophy |
| [**ROADMAP.md**](ROADMAP.md) | Development phases and milestones |
| [**Implementation Plan**](docs/IMPLEMENTATION_PLAN.md) | Detailed 7-phase development plan |

### Guides & Tutorials

- 🏗️ **Architecture Overview** - Understanding AeternumDB's design
- 🔌 **Extension Development** - Creating WASM plugins
- 🚀 **Deployment Guide** - Production deployment strategies
- 📊 **Performance Tuning** - Optimization best practices

---

## 🌍 Roadmap

### Current Status: Phase 1 - Core Foundation ✅

<details>
<summary><b>Phase 1: Core Foundation</b> (In Progress)</summary>

- [x] ACID transaction engine
- [x] Decimal Engine for precise calculations
- [x] JSON/JSON2 engine with schemas
- [x] Versioning layer
- [ ] Storage engine with B-tree indexes
- [ ] SQL parser and query optimizer
- [ ] Write-Ahead Log (WAL)
- [ ] Network protocol

</details>

<details>
<summary><b>Phase 2: Extensibility</b> (Planned)</summary>

- [ ] WASM runtime integration
- [ ] Extension API and ABI
- [ ] Example extensions (GraphQL, Hello World)
- [ ] Extension security sandboxing
- [ ] Extension SDK for developers

</details>

<details>
<summary><b>Phase 3: Distribution & Scalability</b> (Planned)</summary>

- [ ] Cluster coordination
- [ ] Data sharding strategies
- [ ] Replication protocols
- [ ] Fault tolerance mechanisms
- [ ] gRPC and binary protocols

</details>

<details>
<summary><b>Phase 4: Drivers & SDKs</b> (Planned)</summary>

- [ ] ODBC driver (cross-platform)
- [ ] JDBC driver
- [ ] Native SDKs (Rust, Python, JS, Go, Java, .NET, C++)
- [ ] Connection pooling
- [ ] Async/await support

</details>

<details>
<summary><b>Phases 5-7</b> (Future)</summary>

- **Phase 5:** Production features (auth, encryption, backup)
- **Phase 6:** Security & compliance (GDPR, LGPD, auditing)
- **Phase 7:** Enterprise edition (commercial features)

</details>

📊 **Detailed Roadmap:** See [ROADMAP.md](ROADMAP.md) and [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md)

---

## 🙏 Acknowledgments

AeternumDB is inspired by:
- **CockroachDB** - Distributed architecture patterns
- **PostgreSQL** - SQL standards and reliability
- **MongoDB** - Flexible licensing model
- **Rust Community** - Memory safety and performance

---

## 📧 Contact & Community

- **Issues:** [GitHub Issues](https://github.com/schivei/aeternum-db/issues)
- **Discussions:** [GitHub Discussions](https://github.com/schivei/aeternum-db/discussions)
- **Security:** See [SECURITY.md](SECURITY.md) for vulnerability reporting

---

<div align="center">

**[⬆ back to top](#-aeternumdb)**

Made with ❤️ by the AeternumDB community

</div>
