# AeternumDB Tests

Comprehensive test suite for AeternumDB.

## Test Structure

- **Unit tests**: Individual module tests (located in `core/src/`)
- **Integration tests**: Cross-module tests (to be added here)
- **Performance tests**: Benchmarks and load tests
- **Driver tests**: Tests for ODBC, JDBC, gRPC, and binary drivers

## Running Tests

### Core Engine Tests

```bash
cd core
cargo test
```

### With Coverage

```bash
cd core
cargo tarpaulin --out Html
```

## CI/CD

Tests run automatically on every push and pull request via GitHub Actions.

## Status

🚧 Test suite is being actively developed
