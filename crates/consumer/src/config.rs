//! `ConsumerConfig` — configuration for the Kafka consumer.

use proto::SecurityConfig;

/// Where to start consuming when there is no committed offset for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetReset {
    /// Start from the earliest available message.
    /// Use in integration tests to replay all messages from the beginning.
    Earliest,
    /// Start from the next message written after the consumer subscribes.
    Latest,
}

impl OffsetReset {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            OffsetReset::Earliest => "earliest",
            OffsetReset::Latest => "latest",
        }
    }
}

/// Configuration for the Kafka consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Comma-separated list of `host:port` broker addresses.
    pub broker_addresses: String,
    /// Consumer group ID. All consumers sharing the same group ID form a group
    /// and the broker distributes partitions among them.
    pub group_id: String,
    /// List of topics to subscribe to.
    pub topics: Vec<String>,
    /// Where to start if no offset is committed for a partition.
    pub offset_reset: OffsetReset,
    /// Whether to auto-commit offsets. Always `false` in this library —
    /// manual commit after successful processing gives at-least-once semantics.
    pub auto_commit: bool,
    /// Optional security configuration (TLS / SASL).
    pub security: Option<SecurityConfig>,
}

impl ConsumerConfig {
    pub fn new(
        broker_addresses: impl Into<String>,
        group_id: impl Into<String>,
        topics: Vec<String>,
    ) -> Self {
        Self {
            broker_addresses: broker_addresses.into(),
            group_id: group_id.into(),
            topics,
            offset_reset: OffsetReset::Earliest,
            auto_commit: false,
            security: None,
        }
    }

    pub fn with_offset_reset(mut self, reset: OffsetReset) -> Self {
        self.offset_reset = reset;
        self
    }

    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.security = Some(security);
        self
    }
}
