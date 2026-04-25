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

    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && args[1] == "--lite" {
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
