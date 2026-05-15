# AeternumDB Tests

Comprehensive test suite for AeternumDB.

## Test Structure

- **Unit tests**: Individual module tests (located in `src/AeternumDB.Core.Tests/`)
- **Integration tests**: Cross-module tests (to be added here)
- **Performance tests**: Benchmarks and load tests
- **Driver tests**: Tests for ODBC, JDBC, gRPC, and binary drivers

## Running Tests

### Core Engine Tests

```bash
cd src
dotnet test AeternumDB.slnx -c Release
```

### With Coverage

```bash
cd src
dotnet test AeternumDB.slnx -c Release \
  --collect:"XPlat Code Coverage" \
  --settings AeternumDB.Core.Tests/coverage.runsettings
```

## CI/CD

Tests run automatically on every push and pull request via GitHub Actions.

## Status

🚧 Test suite is being actively developed
