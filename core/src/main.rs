// AeternumDB Core Engine
// Licensed under AGPLv3.0

#![allow(dead_code)]

mod acid;
mod decimal;
mod json_engine;
mod versioning;

use clap::{Parser, Subcommand};

/// AeternumDB — high-performance, extensible database management system
#[derive(Parser)]
#[command(
    name = "aeternumdb",
    version = env!("CARGO_PKG_VERSION"),
    about = "High-performance, extensible database management system"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run as a single local instance (Lite mode)
    Lite,
}

#[tokio::main]
async fn main() {
    println!("🌀 AeternumDB v{}", env!("CARGO_PKG_VERSION"));
    println!("High-performance, extensible database management system");
    println!();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Lite) => {
            println!("Starting in Lite mode (single local instance)...");
            run_lite_mode();
        }
        None => {
            println!("Usage: aeternumdb lite");
            println!();
            println!("Available modes:");
            println!("  lite    Run as single local instance");
        }
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

    #[test]
    fn test_cli_parses_lite_subcommand() {
        let cli =
            Cli::try_parse_from(["aeternumdb", "lite"]).expect("'lite' subcommand must parse");
        assert!(matches!(cli.command, Some(Commands::Lite)));
    }

    #[test]
    fn test_cli_parses_no_subcommand() {
        let cli = Cli::try_parse_from(["aeternumdb"]).expect("no subcommand must parse");
        assert!(cli.command.is_none());
    }

    #[test]
    fn test_cli_rejects_unknown_subcommand() {
        let result = Cli::try_parse_from(["aeternumdb", "unknown-mode"]);
        assert!(result.is_err(), "unknown subcommand must be rejected");
    }
}
