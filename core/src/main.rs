// AeternumDB Core Engine
// Licensed under AGPLv3.0

#![allow(dead_code)]

mod acid;
mod decimal;
mod json_engine;
mod versioning;

use std::env;

#[tokio::main]
async fn main() {
    println!("🌀 AeternumDB v{}", env!("CARGO_PKG_VERSION"));
    println!("High-performance, extensible database management system");
    println!();

    // Skip argv[0] (the binary path), which can be set to arbitrary text
    // and must not be relied upon for security or mode-selection purposes.
    let args: Vec<String> = env::args().skip(1).collect();

    if args.first().map(String::as_str) == Some("--lite") {
        println!("Starting in Lite mode (single local instance)...");
        run_lite_mode();
    } else {
        println!("Usage: aeternumdb --lite");
        println!();
        println!("Available modes:");
        println!("  --lite    Run as single local instance");
    }
}

fn run_lite_mode() {
    println!("Lite mode initialized");
    println!("Database ready to accept connections");
    // TODO: Implement actual database initialization
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_lite_mode() {
        // Test that run_lite_mode executes without panic
        run_lite_mode();
    }
}
