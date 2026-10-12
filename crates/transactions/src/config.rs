//! `TransactionalConfig` — configuration for the transactional producer.

/// Configuration for the transactional Kafka producer.
///
/// `transactional_id` MUST be stable across producer restarts (not a random UUID).
/// The broker uses it to fence zombie producers: if a producer with the same ID
/// restarts, the broker increments the epoch, making the old instance's sends fail
/// with `TxnAbortRequired`. This prevents duplicate commits from zombie processes.
#[derive(Debug, Clone)]
pub struct TransactionalConfig {
    /// Comma-separated list of `host:port` broker addresses.
    pub bootstrap_servers: String,
    /// Stable, unique identifier for this producer instance.
    /// Example: `"payments-ctp-producer-1"` — not `uuid::Uuid::new_v4()`.
    pub transactional_id: String,
}

impl TransactionalConfig {
    pub fn new(
        bootstrap_servers: impl Into<String>,
        transactional_id: impl Into<String>,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            transactional_id: transactional_id.into(),
        }
    }
}
