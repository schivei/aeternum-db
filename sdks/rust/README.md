# AeternumDB Rust SDK

Native Rust SDK for AeternumDB.

## License

Apache 2.0

## Status

🚧 Under development

## Installation

```toml
[dependencies]
aeternumdb-sdk = "0.1"
```

## Example

```rust
use aeternumdb_sdk::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("localhost:5432").await?;

    // Execute query
    let result = client.query("SELECT * FROM users").await?;

    Ok(())
}
```
