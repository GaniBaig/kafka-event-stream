//! `RebalanceListener` — custom `ConsumerContext` for logging partition events.
//!
//! A rebalance is triggered when:
//! 1. A consumer joins the group (including first start)
//! 2. A consumer leaves (graceful shutdown or heartbeat timeout)
//! 3. Topic partition count changes
//!
//! On `pre_rebalance` (Assign): the broker is about to assign partitions to this consumer.
//! On `pre_rebalance` (Revoke): partitions are being taken away — commit pending offsets NOW
//!   before the assignment moves to another consumer, or those offsets will be re-processed.

use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::{ClientContext, TopicPartitionList};
use tracing::{info, warn};

/// Custom consumer context that logs rebalance events.
///
/// Passed to `StreamConsumer::with_context()` at construction time.
/// The `ConsumerContext::pre_rebalance` hook fires synchronously in the rdkafka
/// background thread — keep it fast; no blocking I/O.
pub struct RebalanceListener;

impl ClientContext for RebalanceListener {}

impl ConsumerContext for RebalanceListener {
    fn pre_rebalance(&self, _base_consumer: &rdkafka::consumer::BaseConsumer<Self>, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                info!(
                    partitions = ?format_tpl(tpl),
                    "rebalance: partitions ASSIGNED to this consumer"
                );
            }
            Rebalance::Revoke(tpl) => {
                // Partitions are being taken away.
                // If auto_commit=false (our default), offsets must be committed here
                // before the assignment moves. If we don't commit, the new owner will
                // re-process messages from the last committed offset — causing duplicates.
                // This is the "rebalance gap" — a well-known Kafka operational topic.
                warn!(
                    partitions = ?format_tpl(tpl),
                    "rebalance: partitions REVOKED — commit any pending offsets"
                );
            }
            Rebalance::Error(e) => {
                warn!(error = %e, "rebalance error");
            }
        }
    }

    fn post_rebalance(&self, _base_consumer: &rdkafka::consumer::BaseConsumer<Self>, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                info!(
                    partitions = ?format_tpl(tpl),
                    "rebalance complete: consumer is now active on assigned partitions"
                );
            }
            Rebalance::Revoke(tpl) => {
                info!(
                    partitions = ?format_tpl(tpl),
                    "rebalance complete: partitions handed off"
                );
            }
            Rebalance::Error(_) => {}
        }
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(()) => {}
            Err(e) => warn!(error = %e, "offset commit callback error"),
        }
    }
}

fn format_tpl(tpl: &TopicPartitionList) -> Vec<String> {
    tpl.elements()
        .iter()
        .map(|e| format!("{}[{}]", e.topic(), e.partition()))
        .collect()
}
