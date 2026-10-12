//! # observability
//!
//! Shared observability infrastructure:
//! - `ObservabilityHooks` trait — the contract producer/consumer crates depend on
//! - `MetricsRegistry` — Prometheus implementation of the trait
//! - `start_metrics_server` — axum HTTP server serving `/metrics`
//! - `init_tracing` — JSON structured logging setup
//!
//! Producer and consumer crates accept `Arc<dyn ObservabilityHooks + Send + Sync>`
//! so they depend on the trait, never on the concrete Prometheus implementation.
//! This is the `Arc<dyn Trait + Send + Sync>` pattern that demonstrates API design.

pub mod hooks;
pub mod metrics;
pub mod server;
pub mod tracing_init;

pub use hooks::ObservabilityHooks;
pub use metrics::MetricsRegistry;
pub use server::start_metrics_server;
pub use tracing_init::init_tracing;
