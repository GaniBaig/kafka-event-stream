//! axum HTTP server that serves `GET /metrics` for Prometheus scraping.

use std::sync::Arc;

use axum::{Router, extract::State, response::IntoResponse, routing::get};
use tokio::net::TcpListener;

use crate::MetricsRegistry;

/// Spawn a tokio task serving Prometheus metrics on `127.0.0.1:{port}/metrics`.
///
/// Binding to 127.0.0.1 (not 0.0.0.0) per security policy — metrics are
/// internal-only and must not be exposed on all network interfaces.
pub fn start_metrics_server(port: u16, registry: Arc<MetricsRegistry>) {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(registry);

    tokio::spawn(async move {
        let addr = format!("127.0.0.1:{port}");
        let listener = TcpListener::bind(&addr)
            .await
            .expect("metrics server: failed to bind");
        tracing::info!(address = addr, "metrics server listening");
        axum::serve(listener, app)
            .await
            .expect("metrics server: serve error");
    });
}

async fn metrics_handler(State(registry): State<Arc<MetricsRegistry>>) -> impl IntoResponse {
    registry.render()
}
