//! `KafkaConsumer` — wraps `rdkafka::consumer::StreamConsumer` with a `RebalanceListener`.
//!
//! ## Commit semantics — "commit after process" (at-least-once)
//!
//! Two options exist for when to commit offsets:
//!
//! **Option A — Commit BEFORE processing:**
//! - Commit offset → process message → crash before processing
//! - Result: message is lost (offset was committed, handler never ran)
//! - This is at-most-once semantics.
//!
//! **Option B — Commit AFTER processing (this implementation):**
//! - Process message → handler returns Ok → commit offset → crash before commit
//! - Result: on restart the message is redelivered (offset was not committed)
//! - Consumers must be idempotent (handle duplicate delivery gracefully)
//! - This is at-least-once semantics — the safer default for most use cases.
//!
//! For exactly-once: use the transactions crate (CTP pattern).

use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use observability::ObservabilityHooks;
use proto::KafkaError;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn};

use crate::config::ConsumerConfig;
use crate::rebalance::RebalanceListener;
use producer::apply_security_config;

/// A Kafka consumer wrapping rdkafka's `StreamConsumer`.
pub struct KafkaConsumer {
    inner: StreamConsumer<RebalanceListener>,
    config: ConsumerConfig,
    hooks: Arc<dyn ObservabilityHooks>,
}

impl KafkaConsumer {
    /// Construct a new `KafkaConsumer` and subscribe to the configured topics.
    pub fn new(
        config: ConsumerConfig,
        hooks: Arc<dyn ObservabilityHooks>,
    ) -> Result<Self, KafkaError> {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &config.broker_addresses)
            .set("group.id", &config.group_id)
            .set("auto.offset.reset", config.offset_reset.as_str())
            // Always disable auto-commit — we commit manually after processing.
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", "10000")
            .set("heartbeat.interval.ms", "3000");

        if let Some(security) = &config.security {
            apply_security_config(&mut client_config, security);
        }

        let inner: StreamConsumer<RebalanceListener> = client_config
            .create_with_context(RebalanceListener)
            .map_err(|e| KafkaError::ConfigError(e.to_string()))?;

        let topics: Vec<&str> = config.topics.iter().map(String::as_str).collect();
        inner
            .subscribe(&topics)
            .map_err(|e| KafkaError::ConsumeError(e.to_string()))?;

        info!(topics = ?config.topics, group_id = %config.group_id, "consumer subscribed");

        Ok(Self { inner, config, hooks })
    }

    /// Run the consume loop until the `cancel` token is cancelled.
    ///
    /// For each message received:
    /// 1. Calls `handler(message_payload)`
    /// 2. On `Ok(())` — commits the offset to the broker (at-least-once)
    /// 3. On `Err` — logs a warning and skips the commit (message will be redelivered)
    ///
    /// `tokio::select!` races two futures:
    /// - `stream.next()` — the next Kafka message
    /// - `cancel.cancelled()` — the shutdown signal
    /// The one that resolves first wins. On shutdown, the loop exits cleanly after
    /// finishing the current message (no mid-message truncation).
    #[instrument(skip(self, handler, cancel), fields(group_id = %self.config.group_id))]
    pub async fn consume_loop<F, Fut>(
        &self,
        handler: F,
        cancel: CancellationToken,
    ) -> Result<(), KafkaError>
    where
        F: Fn(String, String) -> Fut,        // (topic, payload) -> Future
        Fut: std::future::Future<Output = Result<(), KafkaError>>,
    {
        let mut stream = self.inner.stream();

        loop {
            select! {
                // Branch 1: next message available
                maybe_msg = stream.next() => {
                    match maybe_msg {
                        None => {
                            // Stream ended — broker closed connection.
                            info!("message stream ended");
                            break;
                        }
                        Some(Err(e)) => {
                            // rdkafka transport/protocol error — log and continue.
                            warn!(error = %e, "consume stream error");
                            self.hooks.record_error("consume");
                        }
                        Some(Ok(msg)) => {
                            let topic = msg.topic().to_string();
                            let partition = msg.partition();
                            let offset = msg.offset();

                            let payload = match msg.payload_view::<str>() {
                                Some(Ok(s)) => s.to_string(),
                                Some(Err(e)) => {
                                    warn!(error = ?e, "payload UTF-8 error, skipping");
                                    self.hooks.record_error("serialization");
                                    continue;
                                }
                                None => String::new(),
                            };

                            let start = Instant::now();

                            match handler(topic.clone(), payload).await {
                                Ok(()) => {
                                    // Commit after successful processing (at-least-once).
                                    // store_offset + commit: two-step to let librdkafka batch commits.
                                    self.inner
                                        .store_offset(&topic, partition, offset)
                                        .map_err(|e| KafkaError::OffsetCommitError(e.to_string()))?;
                                    self.inner
                                        .commit_consumer_state(CommitMode::Async)
                                        .map_err(|e| KafkaError::OffsetCommitError(e.to_string()))?;

                                    let latency = start.elapsed().as_secs_f64() * 1000.0;
                                    self.hooks.record_consume(&topic, &self.config.group_id, latency);

                                    // Consumer lag = high_watermark - committed_offset.
                                    // Updated after every commit so the Prometheus gauge
                                    // reflects the current state within one poll cycle.
                                    if let Ok((_, high)) = self.inner.fetch_watermarks(
                                        &topic,
                                        partition,
                                        std::time::Duration::from_millis(100),
                                    ) {
                                        let lag = (high - offset - 1).max(0);
                                        self.hooks.update_consumer_lag(
                                            &topic,
                                            partition,
                                            &self.config.group_id,
                                            lag,
                                        );
                                    }
                                }
                                Err(e) => {
                                    // Handler failed — skip commit, message will be redelivered.
                                    warn!(
                                        error = %e,
                                        topic = %topic,
                                        partition,
                                        offset,
                                        "handler error, skipping offset commit"
                                    );
                                    self.hooks.record_error("handler");
                                }
                            }
                        }
                    }
                }

                // Branch 2: cancellation signal received (Ctrl-C or token.cancel())
                _ = cancel.cancelled() => {
                    info!("cancellation received, shutting down consumer loop");
                    break;
                }
            }
        }

        Ok(())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OffsetReset;
    use observability::hooks::NoopHooks;

    fn noop_hooks() -> Arc<dyn ObservabilityHooks> {
        Arc::new(NoopHooks)
    }

    #[test]
    fn consumer_config_defaults() {
        let cfg = ConsumerConfig::new(
            "localhost:9092",
            "test-group",
            vec!["events".into()],
        );
        assert_eq!(cfg.group_id, "test-group");
        assert_eq!(cfg.offset_reset, OffsetReset::Earliest);
        assert!(!cfg.auto_commit);
    }

    // Integration test — requires a running Kafka broker.
    // Run with: cargo test -p consumer -- --ignored
    #[tokio::test]
    #[ignore]
    async fn integration_consume_loop_cancels_cleanly() {
        let cfg = ConsumerConfig::new(
            "localhost:9092",
            "test-group-cancel",
            vec!["test-topic".into()],
        );
        let consumer = KafkaConsumer::new(cfg, noop_hooks()).expect("consumer init");
        let cancel = CancellationToken::new();

        // Cancel immediately — loop should exit on first select! poll.
        let token_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            token_clone.cancel();
        });

        let result = consumer
            .consume_loop(
                |_topic, _payload| async { Ok(()) },
                cancel,
            )
            .await;

        assert!(result.is_ok());
    }
}
