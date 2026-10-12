//! `ObservabilityHooks` — the trait producer and consumer crates call.
//!
//! By depending on this trait (not on `MetricsRegistry` directly), library crates
//! remain decoupled from Prometheus. Tests can inject a no-op implementation.
//! The pattern `Arc<dyn ObservabilityHooks + Send + Sync>` is the idiomatic way
//! to share a trait object across tokio tasks.

/// Observability hook contract for Kafka client events.
///
/// Implemented by `MetricsRegistry` (Prometheus) and a `NoopHooks` stub for tests.
pub trait ObservabilityHooks: Send + Sync {
    /// Called after a message is successfully produced.
    ///
    /// `topic` — the target topic name  
    /// `latency_ms` — round-trip time from send() call to broker ack, in milliseconds
    fn record_produce(&self, topic: &str, latency_ms: f64);

    /// Called after a message is successfully consumed and the handler returned `Ok`.
    ///
    /// `topic` — source topic  
    /// `group_id` — consumer group ID  
    /// `latency_ms` — time from message receipt to handler completion
    fn record_consume(&self, topic: &str, group_id: &str, latency_ms: f64);

    /// Called after a consumer offset commit. Updates the lag gauge.
    ///
    /// `lag = high_watermark - committed_offset`
    /// A growing lag signals that consumers are slower than producers.
    fn update_consumer_lag(&self, topic: &str, partition: i32, group_id: &str, lag: i64);

    /// Called whenever any Kafka client error occurs.
    ///
    /// `error_type` — a stable string label, e.g. `"produce"`, `"consume"`, `"tls"`
    fn record_error(&self, error_type: &str);
}

/// No-op implementation — used in unit tests that don't need real metrics.
pub struct NoopHooks;

impl ObservabilityHooks for NoopHooks {
    fn record_produce(&self, _topic: &str, _latency_ms: f64) {}
    fn record_consume(&self, _topic: &str, _group_id: &str, _latency_ms: f64) {}
    fn update_consumer_lag(&self, _topic: &str, _partition: i32, _group_id: &str, _lag: i64) {}
    fn record_error(&self, _error_type: &str) {}
}
