# 🤝 Contributing to AeternumDB

Thank you for considering contributing to **AeternumDB**! This project is ambitious and depends on community collaboration to evolve.

This document describes how you can participate, which areas are open to contributions, and how licensing applies to each part of the project.

---

## 📋 Table of Contents

- [Project Structure](#-project-structure)
- [Licensing and Contribution Areas](#-licensing-and-contribution-areas)
- [How to Contribute](#%EF%B8%8F-how-to-contribute)
- [Development Workflow](#-development-workflow)
- [Code Standards](#-code-standards)
- [Best Practices](#-best-practices)
- [Community](#-community)
- [Future Plans](#-future-plans)

---

## 📂 Project Structure

The repository is organized into modular components with different licenses:

```plaintext
aeternum-db/
│
├── core/               # Main engine in Rust (AGPLv3.0)
│   ├── src/
│   │   ├── acid.rs           # Transaction management
│   │   ├── decimal.rs        # Decimal arithmetic
│   │   ├── json_engine.rs    # JSON/JSON2 support
│   │   └── versioning.rs     # Data versioning
│   └── tests/
│
├── extensions/         # WASM plugins (MIT)
│   └── README.md
│
├── drivers/           # Database drivers (Apache 2.0)
│   ├── odbc/          # ODBC driver
│   ├── jdbc/          # JDBC driver
│   ├── grpc/          # gRPC protocol
│   └── binary/        # Binary protocol
│
├── sdks/              # Client SDKs (Apache 2.0)
│   ├── rust/
│   ├── python/
│   ├── javascript/
│   ├── go/
│   ├── java/
│   ├── dotnet/
│   └── cpp/
│
├── deployment/        # Deployment configurations
├── docs/              # Documentation
└── tests/             # Integration tests
```

---

## 📜 Licensing and Contribution Areas

AeternumDB uses a **hybrid licensing model** to balance open-source community growth with commercial viability. Understanding which license applies to your contribution is important:

### 🔐 Core Engine (AGPLv3.0)

**Areas:** Transaction management, storage engine, query parser, replication

**License Requirements:**
- ✅ Contributions must be compatible with AGPLv3.0
- ✅ Any modification or derivative must maintain the same license
- ✅ If used as a service (SaaS), source code must be made available

**Ideal For:**
- Developers interested in database internals
- ACID transaction systems
- Distributed systems and replication
- Data versioning and consistency
- Security implementations

### 🔌 SDKs and Drivers (Apache 2.0)

**Areas:** Client libraries, database drivers, connection protocols

**License Requirements:**
- ✅ Permissive license allowing commercial use
- ✅ Can be used in proprietary software without opening source
- ✅ Includes patent grant protection

**Ideal For:**
- Language-specific client development
- Protocol implementations
- Integration with existing systems
- Corporate/enterprise environments

### 🎨 WASM Extensions (MIT)

**Areas:** Custom data types, procedural languages, integration connectors

**License Requirements:**
- ✅ Most permissive - minimal restrictions
- ✅ Complete freedom to use, modify, and redistribute
- ✅ Perfect for community experimentation

**Ideal For:**
- Custom data paradigms (GraphQL, NoSQL interfaces)
- Stored procedures in various languages
- Third-party integrations (Kafka, MQTT, etc.)
- Custom indexing strategies
- Experimental features

---

## 🛠️ How to Contribute

### Step 1: Choose Your Area

1. Browse [open issues](https://github.com/schivei/aeternum-db/issues)
2. Check the [Implementation Plan](docs/IMPLEMENTATION_PLAN.md)
3. Look at [Phase 1 PRs](docs/phase1-prs.md) for detailed tasks

### Step 2: Set Up Development Environment

```bash
# Clone the repository
git clone https://github.com/schivei/aeternum-db.git
cd aeternum-db

# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build the project
cd core
cargo build

# Run tests
cargo test
```

### Step 3: Create Your Feature Branch

```bash
git checkout -b feature/your-feature-name
```

Branch naming conventions:
- `feature/` - New features
- `bugfix/` - Bug fixes
- `docs/` - Documentation updates
- `refactor/` - Code refactoring

### Step 4: Make Your Changes

Follow the guidelines in our [PR Development Guide](docs/PR_GUIDE.md):
- Write clean, documented code
- Add tests for new functionality
- Follow Rust conventions (cargo fmt, cargo clippy)
- Update documentation as needed

### Step 5: Submit a Pull Request

Create a PR with a clear description including:

```markdown
## Description
[What does this PR do?]

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Areas Affected
- [ ] Core (AGPLv3.0)
- [ ] SDKs (Apache 2.0)
- [ ] Drivers (Apache 2.0)
- [ ] Extensions (MIT)

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests passing

## Checklist
- [ ] Code follows project style guidelines
- [ ] Documentation updated
- [ ] LICENSE info correct for modified files
```

---

## 🔄 Development Workflow

### Before Starting

1. **Check existing work:** Search issues and PRs to avoid duplication
2. **Discuss major changes:** Open an issue for discussion before large PRs
3. **Read planning docs:** Review [Implementation Plan](docs/IMPLEMENTATION_PLAN.md)

### During Development

```bash
# Keep your branch updated
git fetch origin
git rebase origin/main

# Run formatting
cargo fmt

# Check for common mistakes
cargo clippy -- -D warnings

# Run tests frequently
cargo test

# Build in release mode
cargo build --release
```

### Code Review Process

1. **Automated checks:** CI/CD runs tests, formatting, and clippy
2. **Peer review:** At least one maintainer review required
3. **Feedback:** Address review comments promptly
4. **Approval:** PR approved once all checks pass

---

## 📐 Code Standards

### Rust Code Style

```rust
// ✅ Good: Well-documented, follows conventions
/// Calculates the sum of two decimal numbers.
///
/// # Arguments
/// * `a` - First decimal number
/// * `b` - Second decimal number
///
/// # Examples
/// ```
/// use aeternumdb::decimal::add;
/// let result = add(Decimal::new(1, 0), Decimal::new(2, 0));
/// assert_eq!(result, Decimal::new(3, 0));
/// ```
pub fn add(a: Decimal, b: Decimal) -> Decimal {
    a + b
}
```

### Testing Requirements

- **Unit tests:** Test individual functions
- **Integration tests:** Test component interactions
- **Coverage:** Aim for >80% code coverage
- **Documentation tests:** Examples in doc comments should compile

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_addition() {
        let result = add(Decimal::new(1, 0), Decimal::new(2, 0));
        assert_eq!(result, Decimal::new(3, 0));
    }

    #[tokio::test]
    async fn test_async_operation() {
        // Async test example
    }
}
```

### Documentation Standards

- Public APIs must have doc comments
- Use `///` for public items, `//` for implementation details
- Include examples in documentation
- Link to related items with `[`Item`]` syntax

---

## ✅ Best Practices

### General Guidelines

- ✅ **Write clean code:** Self-documenting, clear variable names
- ✅ **Small commits:** One logical change per commit
- ✅ **Descriptive messages:** Use [conventional commits](https://www.conventionalcommits.org/)
- ✅ **Test thoroughly:** Cover edge cases and error conditions
- ✅ **Document changes:** Update relevant markdown files

### Commit Message Format

```
type(scope): short description

Longer description if needed.

- Additional details
- References to issues: Fixes #123

Related to PR plan: docs/phase1-prs.md, PR 1.1
```

**Types:**
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation
- `test:` Adding tests
- `refactor:` Code restructuring
- `perf:` Performance improvement
- `chore:` Maintenance

### Performance Considerations

- Benchmark critical paths
- Avoid unnecessary allocations
- Use appropriate data structures
- Profile before optimizing
- Document performance characteristics

---

## 🌍 Community

### Communication Channels

- **Issues:** [GitHub Issues](https://github.com/schivei/aeternum-db/issues) - Bug reports, feature requests
- **Discussions:** [GitHub Discussions](https://github.com/schivei/aeternum-db/discussions) - General questions, ideas
- **Pull Requests:** Code contributions and reviews

### Community Guidelines

- Be respectful and inclusive
- Follow the [Code of Conduct](CODE_OF_CONDUCT.md)
- Help others in discussions
- Share knowledge and expertise
- Give constructive feedback

### Recognition

Contributors are recognized in:
- Git commit history
- Release notes
- Project acknowledgments

---

## 💡 Future Plans

### Extensibility

Extensions and SDKs may be maintained in separate repositories in the future as the project matures. This will:
- Simplify contribution workflows
- Enable independent versioning
- Reduce repository complexity

### Enterprise Edition

The project envisions an **Enterprise Edition** with:
- Commercial licensing option
- Advanced features (SLA, support)
- Additional tools and integrations

**Note:** The community will always have access to the AGPLv3.0 core and open-source extensions.

---

## 🆘 Getting Help

### Resources

- 📚 [Implementation Plan](docs/IMPLEMENTATION_PLAN.md) - Complete roadmap
- 🔧 [PR Guide](docs/PR_GUIDE.md) - Detailed PR instructions
- 📊 [Architecture Overview](COPILOT.md) - Design philosophy

### Questions?

- Check existing [issues](https://github.com/schivei/aeternum-db/issues) and [discussions](https://github.com/schivei/aeternum-db/discussions)
- Open a new discussion for general questions
- Tag maintainers in urgent issues

---

## 📄 License

By contributing to AeternumDB, you agree that your contributions will be licensed under the appropriate license for the component:

- **Core:** AGPLv3.0
- **Drivers/SDKs:** Apache 2.0
- **Extensions:** MIT

See [LICENSE.md](LICENSE.md) for full license texts.

---

<div align="center">

**Thank you for contributing to AeternumDB!** 🙏

[⬆ Back to top](#-contributing-to-aeternumdb)

</div>
