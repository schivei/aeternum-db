🌀 AeternumDB

AeternumDB is a high-performance, extensible, and modular Database Management System (DBMS) written in Rust, with support for multiple data paradigms and modern execution environments.

The project is open source and combines security, scalability, and extensibility to serve from local instances to distributed cloud architectures.

---

✨ Key Features

• ACID + SQL-like: robust transactional core.
• Multi-paradigm: support for relationships, objects, graphs (GraphQL), JSON/JSON2.
• Decimal Engine: numeric precision to avoid floating-point errors.
• Versioning: history of changes in tables/rows.
• Distribution: replication, sharding by rows and columns.
• Horizontal scalability: distributed cluster, CockroachDB-style.
• Security: encryption in transit and at rest, strong authentication.
• Extensibility via WASM: plugins for new paradigms, functions, and integrations.
• Cross-platform drivers: ODBC (32/64), JDBC, native binary, gRPC.
• Execution environments: Lite, Container, Serverless (multi-cloud).


---

📂 Repository Structure

/aeternumdb
  /core            -> Main engine (Rust, AGPLv3.0)
  /extensions      -> WASM plugins (MIT)
  /drivers         -> ODBC, JDBC, binary, gRPC (Apache 2.0)
  /sdks            -> SDKs in multiple languages (Apache 2.0)
  /deployment      -> Lite, Container, Serverless configurations
  /docs            -> Whitepapers, specifications
  /tests           -> Unit and integration tests


---

🚀 Installation and Usage

Prerequisites

• Rust (latest stable version)
• Cargo for package management
• Docker/Kubernetes (optional, for containerized version)


Core Compilation

cd core
cargo build --release


Running Local Instance (Lite)

./target/release/aeternumdb --lite


Running in Container

docker build -t aeternumdb .
docker run -p 5432:5432 aeternumdb


---

📜 Licensing

• Core Engine → AGPLv3.0
• SDKs and Drivers → Apache 2.0
• WASM Extensions → MIT
• Enterprise Edition → Future commercial license


See [LICENSE.md](LICENSE.md) for details.

---

🤝 Contributing

Contributions are welcome!

• Read [CONTRIBUTING.md](CONTRIBUTING.md) to understand how to participate.
• Respect the [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).


---

📖 Documentation

• [COPILOT.md](COPILOT.md) → complete vision of architecture and objectives.
• [CONTRIBUTING.md](CONTRIBUTING.md) → guide for contributors.
• [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) → community code of conduct.


---

🌍 Roadmap

• Implement ACID core in Rust
• Initial support for JSON/JSON2
• Decimal Engine
• Basic replication and sharding
• Example WASM extensions
• ODBC/JDBC drivers
• SDKs in Rust, Python, JS/TS, Go, Java, .NET Core
• Containerized deployment
• Multi-cloud Serverless version
