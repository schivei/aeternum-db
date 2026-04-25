// AeternumDB Core Engine
// Licensed under AGPLv3.0

#![allow(dead_code)]

mod acid;
mod decimal;
mod json_engine;
mod versioning;

use std::env;
use std::ffi::OsStr;

#[tokio::main]
async fn main() {
    println!("🌀 AeternumDB v{}", env!("CARGO_PKG_VERSION"));
    println!("High-performance, extensible database management system");
    println!();

    // Use args_os() to avoid relying on lossy UTF-8 conversion and to ensure
    // argv[0] (which can be set to arbitrary text) is properly skipped.
    let args: Vec<_> = env::args_os().skip(1).collect();

    if args.first().map(|a| a.as_os_str()) == Some(OsStr::new("--lite")) {
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
