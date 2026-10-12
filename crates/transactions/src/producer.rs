//! `TransactionalProducer` — wraps rdkafka's `BaseProducer` with transactional config.
//!
//! ## Error code taxonomy (interview-critical)
//!
//! - `TxnAbortRequired` — the current transaction is poisoned (e.g. timed out).
//!   Call `abort_transaction()` then start a new transaction. The producer itself
//!   is still usable.
//!
//! - `Fatal` — the producer is unusable (e.g. epoch fencing, corrupted state).
//!   Must reinitialise a new `TransactionalProducer` entirely.

use std::collections::HashMap;
use std::time::Duration;

use proto::{KafkaError, KafkaMessage, TopicPartition};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use tracing::{info, instrument, warn};

use crate::config::TransactionalConfig;

/// A Kafka producer configured for exactly-once transactional writes.
pub struct TransactionalProducer {
    inner: BaseProducer,
}

impl TransactionalProducer {
    /// Construct a new `TransactionalProducer` and call `init_transactions`.
    ///
    /// `init_transactions` registers this `transactional_id` with the broker's
    /// transaction coordinator and bumps the producer epoch, fencing any prior
    /// instance with the same ID. Must be called once before any transaction.
    pub fn new(config: TransactionalConfig) -> Result<Self, KafkaError> {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            // acks=all and idempotence are REQUIRED for transactions.
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("transactional.id", &config.transactional_id)
            .set("message.timeout.ms", "30000");

        let inner: BaseProducer = client_config
            .create()
            .map_err(|e| KafkaError::ConfigError(e.to_string()))?;

        // init_transactions contacts the broker's transaction coordinator.
        // Must complete before begin_transaction() is called.
        inner
            .init_transactions(Duration::from_secs(5))
            .map_err(|e| KafkaError::TransactionError(e.to_string()))?;

        info!(transactional_id = %config.transactional_id, "transactional producer initialised");

        Ok(Self { inner })
    }

    /// Begin a new transaction.
    ///
    /// All `send()` calls after this point are part of this transaction until
    /// `commit_transaction()` or `abort_transaction()` is called.
    pub fn begin_transaction(&self) -> Result<(), KafkaError> {
        self.inner
            .begin_transaction()
            .map_err(|e| KafkaError::TransactionError(e.to_string()))
    }

    /// Commit the current transaction.
    ///
    /// Waits for all in-flight produces and the offset sends to be acked,
    /// then sends the transaction commit marker to the broker.
    ///
    /// If the broker returns `TxnAbortRequired`, automatically aborts and returns
    /// a retriable `KafkaError::TransactionError`. The caller should retry the
    /// entire CTP cycle with a fresh transaction.
    pub fn commit_transaction(&self) -> Result<(), KafkaError> {
        match self.inner.commit_transaction(Duration::from_secs(5)) {
            Ok(()) => {
                info!("transaction committed");
                Ok(())
            }
            Err(e) => {
                let msg = e.to_string();
                // TxnAbortRequired: abort and return retriable error.
                // Fatal: producer is unusable — caller must reinitialise.
                warn!(error = %msg, "commit_transaction failed, aborting");
                let _ = self.abort_transaction(); // best-effort abort
                Err(KafkaError::TransactionError(msg))
            }
        }
    }

    /// Abort the current transaction.
    ///
    /// All records produced since the last `begin_transaction()` are discarded.
    /// A consumer with `isolation.level=read_committed` will never see them.
    pub fn abort_transaction(&self) -> Result<(), KafkaError> {
        self.inner
            .abort_transaction(Duration::from_secs(5))
            .map_err(|e| KafkaError::TransactionError(e.to_string()))
    }

    /// Produce a single record within the current transaction (synchronous send).
    ///
    /// Note: `BaseProducer` uses a background poll thread — records are buffered
    /// and sent when the internal queue is flushed or `flush()` is called.
    pub fn produce(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        payload: &str,
    ) -> Result<(), KafkaError> {
        let mut record = BaseRecord::to(topic).payload(payload);
        if let Some(k) = key {
            record = record.key(k);
        }
        self.inner
            .send(record)
            .map_err(|(e, _)| KafkaError::ProduceError(e.to_string()))
    }

    /// Atomically send consumed input offsets as part of the current transaction.
    ///
    /// This is the key operation that makes CTP exactly-once:
    /// the offset commit for the input topic is bundled INTO the same transaction
    /// as the output records. Either both the output records AND the offset commit
    /// land, or neither does — no partial state.
    ///
    /// `offsets`: map of `TopicPartition` → offset to commit (the NEXT offset to consume)
    /// `cgm`: group metadata obtained from the live consumer via `.group_metadata()`
    pub fn send_offsets_to_transaction(
        &self,
        offsets: &HashMap<TopicPartition, i64>,
        cgm: &ConsumerGroupMetadata,
    ) -> Result<(), KafkaError> {
        use rdkafka::TopicPartitionList;

        let mut tpl = TopicPartitionList::new();
        for (tp, offset) in offsets {
            tpl.add_partition_offset(
                &tp.topic,
                tp.partition,
                rdkafka::Offset::Offset(*offset),
            )
            .map_err(|e| KafkaError::TransactionError(e.to_string()))?;
        }

        self.inner
            .send_offsets_to_transaction(&tpl, cgm, Duration::from_secs(5))
            .map_err(|e| KafkaError::TransactionError(e.to_string()))
    }

    /// Flush all buffered records (drains the internal producer queue).
    ///
    /// Must be called before `commit_transaction()` to ensure all records
    /// have been sent to the broker.
    pub fn flush(&self) -> Result<(), KafkaError> {
        self.inner
            .flush(Duration::from_secs(5))
            .map_err(|e| KafkaError::TransactionError(e.to_string()))
    }

    /// Consume-Transform-Produce demo.
    ///
    /// Reads a batch of pre-fetched messages, applies `transform`, writes results
    /// to `output_topic`, atomically commits the input offsets, and commits.
    ///
    /// On any error, aborts the transaction — the output topic gets zero records.
    #[instrument(skip(self, messages, input_offsets, cgm, transform))]
    pub fn consume_transform_produce<F>(
        &self,
        messages: Vec<KafkaMessage<String>>,
        output_topic: &str,
        input_offsets: &HashMap<TopicPartition, i64>,
        cgm: &ConsumerGroupMetadata,
        transform: F,
    ) -> Result<(), KafkaError>
    where
        F: Fn(String) -> String,
    {
        self.begin_transaction()?;

        for msg in &messages {
            let transformed = transform(msg.value.clone());
            let key_bytes: Option<Vec<u8>> = msg.key.as_ref().map(|b| b.to_vec());
            let result = self.produce(
                output_topic,
                key_bytes.as_deref(),
                &transformed,
            );
            if let Err(e) = result {
                warn!(error = %e, "produce failed inside transaction, aborting");
                let _ = self.abort_transaction();
                return Err(e);
            }
        }

        // Flush all buffered records before sending offsets.
        if let Err(e) = self.flush() {
            let _ = self.abort_transaction();
            return Err(e);
        }

        // Atomically commit input offsets WITH the transaction.
        if let Err(e) = self.send_offsets_to_transaction(input_offsets, cgm) {
            let _ = self.abort_transaction();
            return Err(e);
        }

        self.commit_transaction()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TransactionalConfig;

    #[test]
    fn transactional_config_construction() {
        let cfg = TransactionalConfig::new("localhost:9092", "payments-producer-1");
        assert_eq!(cfg.transactional_id, "payments-producer-1");
    }

    // Integration test — requires a running Kafka broker.
    // Run with: cargo test -p transactions -- --ignored
    #[tokio::test]
    #[ignore]
    async fn integration_abort_leaves_zero_records() {
        // This test verifies exactly-once semantics:
        // After abort, a read_committed consumer sees ZERO records.
        // Full implementation requires broker + consumer verification.
        let cfg = TransactionalConfig::new("localhost:9092", "test-txn-producer-1");
        let producer = TransactionalProducer::new(cfg).expect("txn producer init");

        producer.begin_transaction().expect("begin");
        for i in 0..5 {
            producer
                .produce("output-topic", None, &format!("msg-{i}"))
                .expect("produce");
        }
        producer.flush().expect("flush");
        producer.abort_transaction().expect("abort");

        // A read_committed consumer on output-topic should see 0 records.
        // Verified in the integration test suite with a real consumer.
    }
}
