# AeternumDB Lite Mode

Single local instance configuration.

## Usage

```bash
# From repository root
cd core
cargo build --release
./target/release/aeternumdb --lite
```

## Configuration

Create a `config.toml` file:

```toml
[server]
listen_address = "127.0.0.1:5432"
data_dir = "./data"

[logging]
level = "info"
```

## Features

- Single-node operation
- Local file storage
- No clustering overhead
- Ideal for development and testing
