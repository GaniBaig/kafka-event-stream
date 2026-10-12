//! Prometheus `MetricsRegistry` — concrete implementation of `ObservabilityHooks`.

use std::sync::Arc;

use prometheus::{CounterVec, GaugeVec, HistogramVec, Opts, Registry, histogram_opts};

use crate::hooks::ObservabilityHooks;

/// Owns all Prometheus metrics for the Kafka client.
///
/// Pass as `Arc<MetricsRegistry>` to producer/consumer constructors.
/// Implements `ObservabilityHooks` so it satisfies `Arc<dyn ObservabilityHooks + Send + Sync>`.
pub struct MetricsRegistry {
    registry: Registry,
    pub messages_produced: CounterVec,
    pub messages_consumed: CounterVec,
    pub consumer_lag: GaugeVec,
    pub produce_latency: HistogramVec,
    pub consume_latency: HistogramVec,
    pub errors: CounterVec,
}

impl MetricsRegistry {
    /// Create and register all metrics.
    ///
    /// Uses a private `Registry` (not the global default) so tests can create
    /// independent instances without metric name conflicts.
    pub fn new() -> Result<Arc<Self>, prometheus::Error> {
        let registry = Registry::new();

        let messages_produced = CounterVec::new(
            Opts::new(
                "kafka_messages_produced_total",
                "Total number of messages successfully produced, by topic",
            ),
            &["topic"],
        )?;

        let messages_consumed = CounterVec::new(
            Opts::new(
                "kafka_messages_consumed_total",
                "Total number of messages successfully consumed and processed, by topic and group",
            ),
            &["topic", "group_id"],
        )?;

        let consumer_lag = GaugeVec::new(
            Opts::new(
                "kafka_consumer_lag",
                "Current consumer lag (high_watermark - committed_offset) per partition",
            ),
            &["topic", "partition", "group_id"],
        )?;

        let produce_latency = HistogramVec::new(
            histogram_opts!(
                "kafka_produce_latency_ms",
                "Producer round-trip latency from send() to broker ack, in milliseconds",
                vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]
            ),
            &["topic"],
        )?;

        let consume_latency = HistogramVec::new(
            histogram_opts!(
                "kafka_consume_latency_ms",
                "Consumer handler processing latency from message receipt to Ok return, in ms",
                vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]
            ),
            &["topic", "group_id"],
        )?;

        let errors = CounterVec::new(
            Opts::new(
                "kafka_errors_total",
                "Total number of Kafka client errors, by error type",
            ),
            &["error_type"],
        )?;

        // Register all metrics with the private registry.
        registry.register(Box::new(messages_produced.clone()))?;
        registry.register(Box::new(messages_consumed.clone()))?;
        registry.register(Box::new(consumer_lag.clone()))?;
        registry.register(Box::new(produce_latency.clone()))?;
        registry.register(Box::new(consume_latency.clone()))?;
        registry.register(Box::new(errors.clone()))?;

        Ok(Arc::new(Self {
            registry,
            messages_produced,
            messages_consumed,
            consumer_lag,
            produce_latency,
            consume_latency,
            errors,
        }))
    }

    /// Render all metrics in the Prometheus text exposition format.
    /// Called by the axum `/metrics` handler.
    pub fn render(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&metric_families, &mut buf).unwrap_or(());
        String::from_utf8(buf).unwrap_or_default()
    }
}

impl ObservabilityHooks for MetricsRegistry {
    fn record_produce(&self, topic: &str, latency_ms: f64) {
        self.messages_produced
            .with_label_values(&[topic])
            .inc();
        self.produce_latency
            .with_label_values(&[topic])
            .observe(latency_ms);
    }

    fn record_consume(&self, topic: &str, group_id: &str, latency_ms: f64) {
        self.messages_consumed
            .with_label_values(&[topic, group_id])
            .inc();
        self.consume_latency
            .with_label_values(&[topic, group_id])
            .observe(latency_ms);
    }

    fn update_consumer_lag(&self, topic: &str, partition: i32, group_id: &str, lag: i64) {
        self.consumer_lag
            .with_label_values(&[topic, &partition.to_string(), group_id])
            .set(lag as f64);
    }

    fn record_error(&self, error_type: &str) {
        self.errors.with_label_values(&[error_type]).inc();
    }
}
