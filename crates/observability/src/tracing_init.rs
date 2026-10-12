//! JSON structured tracing initialisation.
//!
//! Call `init_tracing()` exactly once at program startup in `main()`.
//! Log level is controlled by the `RUST_LOG` environment variable.
//! Example: `RUST_LOG=info,rdkafka=warn cargo run -- produce`

use tracing_subscriber::{EnvFilter, fmt};

/// Initialise the global tracing subscriber with JSON output.
///
/// Output goes to stdout. Each log line is a JSON object with fields:
/// `timestamp`, `level`, `target`, `message`, plus any `tracing::instrument` fields.
///
/// # Panics
/// Panics if called more than once (tracing subscriber can only be set once per process).
pub fn init_tracing() {
    fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .with_current_span(true)
        .with_span_list(true)
        .init();
}
