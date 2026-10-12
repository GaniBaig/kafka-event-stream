//! # kafka-rs — CLI binary
//!
//! Wires all crates together. Subcommands:
//! - `produce`     — produce N messages with configurable delivery semantics
//! - `consume`     — run a consumer group until Ctrl-C
//! - `transaction` — run the Consume-Transform-Produce demo
//! - `benchmark`   — produce 10k messages, print p50/p95/p99 latency table

mod args;
mod commands;
mod security;

use anyhow::Result;
use clap::Parser;
use observability::{init_tracing, start_metrics_server, MetricsRegistry};
use tokio_util::sync::CancellationToken;

use args::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialise JSON structured logging (reads RUST_LOG env var).
    init_tracing();

    // 2. Start Prometheus metrics server on port 9898.
    //    Prometheus scrapes http://localhost:9898/metrics every 15s.
    let registry = MetricsRegistry::new()?;
    start_metrics_server(9898, registry.clone());

    // 3. Set up graceful shutdown — CancellationToken cancelled on Ctrl-C.
    //    The consume loop uses `tokio::select!` to race between messages and
    //    this token — idiomatic tokio shutdown without Arc<Mutex<bool>>.
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            tracing::info!("Ctrl-C received, initiating graceful shutdown");
            cancel_clone.cancel();
        }
    });

    // 4. Parse CLI args and dispatch.
    let cli = Cli::parse();
    match cli.command {
        Commands::Produce(args) => commands::produce(args, registry).await?,
        Commands::Consume(args) => commands::consume(args, registry, cancel).await?,
        Commands::Transaction(args) => commands::transaction(args).await?,
        Commands::Benchmark(args) => commands::benchmark(args, registry).await?,
    }

    Ok(())
}
