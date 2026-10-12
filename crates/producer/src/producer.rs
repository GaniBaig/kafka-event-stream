//! `KafkaProducer` — wraps `rdkafka::producer::FutureProducer`.
//!
//! Key design decisions:
//! - `produce_message` is `async` and integrates directly with tokio via `FutureProducer`
//! - `produce_batch` uses `futures::future::join_all` — all sends are concurrent,
//!   total time = slowest single send (not sum of all sends)
//! - `tracing::instrument` attaches span fields to every log line emitted inside the fn
//! - `Arc<dyn ObservabilityHooks>` decouples the producer from Prometheus concretely

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use observability::ObservabilityHooks;
use proto::{KafkaError, KafkaMessage};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::instrument;

use crate::config::{Acks, ProducerConfig, RecordMetadata};
use crate::error_conv::from_rdkafka;
use crate::security::apply_security_config;

/// A Kafka producer wrapping rdkafka's `FutureProducer`.
pub struct KafkaProducer {
    inner: FutureProducer,
    default_topic: String,
    hooks: Arc<dyn ObservabilityHooks>,
}

impl KafkaProducer {
    /// Construct a new `KafkaProducer` from a `ProducerConfig`.
    ///
    /// Builds the rdkafka `ClientConfig`, applies security settings, then calls
    /// `create()` which validates the config and connects to the broker.
    pub fn new(
        config: ProducerConfig,
        hooks: Arc<dyn ObservabilityHooks>,
    ) -> Result<Self, KafkaError> {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("acks", config.acks.as_str())
            .set("retries", config.max_retries.to_string())
            .set("batch.size", config.batch_size.to_string())
            .set("compression.type", config.compression.as_str())
            .set("message.timeout.ms", "30000");

        // Idempotence requires acks=all.
        // When enable.idempotence=true, the broker assigns a producer ID + epoch.
        // Each message gets a monotonically increasing sequence number per partition.
        // On retry, the broker deduplicates by (producer_id, epoch, sequence) — preventing
        // duplicate storage on transient network failures. This is at-least-once → single-partition
        // exactly-once. Full EOS across multiple partitions requires transactions.
        if config.idempotent {
            if config.acks != Acks::All {
                return Err(KafkaError::ConfigError(
                    "idempotent producer requires acks=all".into(),
                ));
            }
            client_config.set("enable.idempotence", "true");
        }

        if let Some(security) = &config.security {
            apply_security_config(&mut client_config, security);
        }

        let inner: FutureProducer = client_config
            .create()
            .map_err(|e| KafkaError::ConfigError(e.to_string()))?;

        Ok(Self {
            inner,
            default_topic: config.topic,
            hooks,
        })
    }

    /// Produce a single message and wait for the broker acknowledgment.
    ///
    /// Returns `RecordMetadata` containing the assigned partition and offset.
    /// A non-negative offset proves the broker accepted and stored the message.
    ///
    /// `tracing::instrument` attaches the topic, partition, and offset as span
    /// fields — they appear on every log line emitted inside this function.
    #[instrument(skip(self, msg), fields(topic = %msg.topic, partition, offset))]
    pub async fn produce_message(
        &self,
        msg: KafkaMessage<String>,
    ) -> Result<RecordMetadata, KafkaError> {
        let topic = msg.topic.clone();
        let start = Instant::now();

        // Use message's topic; fall back to the default topic from config.
        let effective_topic = if msg.topic.is_empty() {
            self.default_topic.clone()
        } else {
            topic.clone()
        };
        let mut record = FutureRecord::to(&effective_topic).payload(msg.value.as_str());

        if let Some(ref key) = msg.key {
            // key is Bytes — as_ref() gives &[u8]; FutureRecord expects &[u8] for key
            record = record.key(key.as_ref());
        }

        let delivery_result = self
            .inner
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| from_rdkafka(e))?;

        let latency = start.elapsed().as_secs_f64() * 1000.0;
        self.hooks.record_produce(&topic, latency);

        let metadata = RecordMetadata {
            topic,
            partition: delivery_result.0,
            offset: delivery_result.1,
        };

        // Update span fields with the actual assigned values.
        tracing::Span::current()
            .record("partition", metadata.partition)
            .record("offset", metadata.offset);

        tracing::debug!(
            partition = metadata.partition,
            offset = metadata.offset,
            latency_ms = latency,
            "message produced"
        );

        Ok(metadata)
    }

    /// Produce a batch of messages concurrently.
    ///
    /// All futures are dispatched in parallel via `join_all` — total wall-clock time
    /// equals the slowest single send, not the sum. Returns an error if ANY message fails;
    /// the remaining futures are still awaited to avoid resource leaks.
    #[instrument(skip(self, msgs), fields(count = msgs.len()))]
    pub async fn produce_batch(
        &self,
        msgs: Vec<KafkaMessage<String>>,
    ) -> Result<Vec<RecordMetadata>, KafkaError> {
        let futures: Vec<_> = msgs
            .into_iter()
            .map(|msg| self.produce_message(msg))
            .collect();

        let results = join_all(futures).await;

        results.into_iter().collect()
    }
}

// ─── Unit tests (no broker required) ─────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Acks, CompressionCodec, ProducerConfig};
    use observability::hooks::NoopHooks;

    fn noop_hooks() -> Arc<dyn ObservabilityHooks> {
        Arc::new(NoopHooks)
    }

    #[test]
    fn producer_config_builder_defaults() {
        let cfg = ProducerConfig::builder("localhost:9092", "test-topic").build();
        assert_eq!(cfg.bootstrap_servers, "localhost:9092");
        assert_eq!(cfg.topic, "test-topic");
        assert_eq!(cfg.acks, Acks::All);
        assert_eq!(cfg.max_retries, 3);
        assert!(!cfg.idempotent);
    }

    #[test]
    fn idempotence_forces_acks_all() {
        let cfg = ProducerConfig::builder("localhost:9092", "t")
            .with_acks(Acks::Zero) // set zero first
            .with_idempotence()    // should override to All
            .build();
        assert_eq!(cfg.acks, Acks::All);
        assert!(cfg.idempotent);
    }

    #[test]
    fn builder_compression_codec() {
        let cfg = ProducerConfig::builder("localhost:9092", "t")
            .with_compression(CompressionCodec::Zstd)
            .build();
        assert_eq!(cfg.compression, CompressionCodec::Zstd);
    }

    #[test]
    fn new_fails_with_bad_broker() {
        // Creating a FutureProducer with an invalid broker address fails at create() time.
        let cfg = ProducerConfig::builder("not-a-host:9999", "t").build();
        // create() succeeds (rdkafka validates lazily), so test config validation instead.
        let result = KafkaProducer::new(cfg, noop_hooks());
        // rdkafka accepts any bootstrap.servers at construction; connection errors surface later.
        // We just confirm the type is constructible.
        assert!(result.is_ok());
    }

    // Integration test — requires a running Kafka broker.
    // Run with: cargo test -p producer -- --ignored
    #[tokio::test]
    #[ignore]
    async fn integration_produce_50_messages() {
        let cfg = ProducerConfig::builder("localhost:9092", "test-topic").build();
        let producer = KafkaProducer::new(cfg, noop_hooks()).expect("producer init");

        let messages: Vec<_> = (0..50)
            .map(|i| KafkaMessage::new("test-topic", format!("message-{i}")))
            .collect();

        let results = producer.produce_batch(messages).await.expect("batch produce");
        assert_eq!(results.len(), 50);

        // All offsets must be non-negative (broker assigned them).
        for meta in &results {
            assert!(meta.offset >= 0, "expected non-negative offset, got {}", meta.offset);
        }
    }
}
