🤝 Contributing to AeternumDB

Thank you for considering contributing to AeternumDB! This project is ambitious and depends on community collaboration to evolve.

This document describes how you can participate, which areas are open to contributions, and how licensing applies to each part of the project.

---

📂 Project Structure

The repository is organized into main modules:

• core/ → Main engine in Rust (AGPLv3.0)
• extensions/ → WASM plugins (MIT)
• drivers/ → ODBC, JDBC, binary, gRPC (Apache 2.0)
• sdks/ → SDKs in Rust, C++, Python, JS/TS, Go, Java/Kotlin, .NET Core (Apache 2.0)
• deployment/ → Lite, Container, Serverless configurations
• docs/ → Documentation, whitepapers
• tests/ → Unit and integration tests


---

📜 Licensing and Contribution Areas

• Core Engine (AGPLv3.0)• Contributions must be compatible with AGPLv3.0.
• Any modification or derivative must maintain the same license.
• Ideal for those who want to work on ACID, replication, sharding, versioning, and security.

• SDKs and Drivers (Apache 2.0)• Contributions can be used in commercial environments without obligation to open source.
• Ideal for those who want to create integrations in different languages.

• WASM Extensions (MIT)• Contributions are completely free and flexible.
• Ideal for those who want to create new data paradigms, custom functions, or connectors.



---

🛠️ How to Contribute

1. Fork the repository and create your branch (feature/feature-name).
2. Follow the code standards defined in each module (Rust for core, corresponding language for SDKs).
3. Add tests whenever possible.
4. Open a Pull Request (PR) clearly describing:• The problem or improvement being solved.
• The affected project area (core, SDK, extension, driver).
• Which license applies to your contribution.



---

✅ Best Practices

• Write clean and documented code.
• Keep commits small and descriptive.
• Use automated tests to validate your changes.
• Respect the CODE_OF_CONDUCT.md.


---

🌍 Community

• Discussions and ideas can be opened via Issues.
• Extensions and SDKs may be maintained in sub-repositories in the future.
• We encourage both technical and documentation contributions.


---

💡 Future

The project envisions an Enterprise Edition with commercial licensing. Contributions to this version will be discussed separately, but the community will always have access to the AGPLv3.0 core and open source extensions.
