🗺️ Roadmap – AeternumDB

This document describes the development phases and main milestones of AeternumDB. The goal is to provide a clear vision of the project's evolution, from the initial core to distributed and serverless versions.

---

📌 Phase 1 – Basic Core

Objective: Create the fundamental database engine.

• Implement ACID engine in Rust
• Initial support for SQL-like queries
• Decimal Engine for numeric precision
• JSON/JSON2 with fixed schema
• Basic table/row versioning
• Unit and initial integration tests


---

📌 Phase 2 – Extensibility

Objective: Enable plugins and extensions.

• WASM extension system
• Example extension: GraphQL Engine
• Example extension: Object Layer (OOP)
• Procedural Languages (Python, JS, .NET Core)
• Documentation for creating plugins


---

📌 Phase 3 – Distribution and Scalability

Objective: Make the database distributed and scalable.

• Native replication
• Row and column sharding
• Distributed cluster with fault tolerance
• Communication via gRPC and native binary protocol
• Observability (monitoring and metrics)


---

📌 Phase 4 – Drivers and SDKs

Objective: Expand integration with different languages and platforms.

• ODBC drivers (32/64 bits, cross-platform)
• JDBC driver
• Rust SDK
• Python SDK
• Java/Kotlin SDK
• .NET Core SDK
• Go SDK
• JS/TS SDK


---

📌 Phase 5 – Execution Environments

Objective: Support different deployment scenarios.

• Lite version (single local instance)
• Containerized version (Docker/Kubernetes)
• Serverless version (AWS Lambda, Azure Functions, GCP Cloud Run)
• Multi-cloud configuration for on-demand scalability


---

📌 Phase 6 – Security and Compliance

Objective: Ensure reliability and conformance.

• Encryption in transit and at rest
• Strong authentication (OAuth2, JWT)
• Advanced auditing via extensions
• Compliance tools (LGPD, GDPR)


---

📌 Phase 7 – Enterprise Edition

Objective: Create commercial version for monetization.

• Exclusive advanced features
• SLA and dedicated support
• Alternative licensing for enterprises
• Corporate administration tools


---

📖 Notes

• The roadmap is iterative: phases can evolve in parallel.
• Each milestone must be accompanied by documentation and tests.
• The community can propose new phases and extensions via Issues.
