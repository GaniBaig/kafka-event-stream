//! # proto
//!
//! Shared domain types used by every crate in the kafka-event-stream workspace.
//! This is the library's entire public API surface — all types here must remain
//! backward-compatible: adding fields must use `Option<T>` with a default so
//! existing callers are not broken.

use std::{fmt, path::PathBuf};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

// ─── Message envelope ────────────────────────────────────────────────────────

/// A generic Kafka message envelope.
///
/// `T` is the payload type — typically `String` (JSON) or `Bytes` (binary).
/// The `key` is `Option<Bytes>` (reference-counted, zero-copy) rather than
/// `Option<String>` so that cloning a key across tokio tasks is O(1), not O(n).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMessage<T> {
    /// Optional partition key. `None` means the producer uses round-robin assignment.
    /// Using `Bytes` (reference-counted buffer) avoids heap copies when the key is
    /// passed across async task boundaries.
    pub key: Option<Bytes>,
    /// Message payload.
    pub value: T,
    /// Target or source topic name.
    pub topic: String,
    /// Partition index (-1 means "let the broker decide" when producing).
    pub partition: i32,
    /// Offset within the partition (-1 for outbound messages not yet assigned).
    pub offset: i64,
    /// Message timestamp in milliseconds since Unix epoch.
    pub timestamp: i64,
}

impl<T: Default> KafkaMessage<T> {
    /// Construct a minimal outbound message (partition/offset/timestamp assigned by broker).
    pub fn new(topic: impl Into<String>, value: T) -> Self {
        Self {
            key: None,
            value,
            topic: topic.into(),
            partition: -1,
            offset: -1,
            timestamp: -1,
        }
    }

    /// Attach a partition key. Key routing is consistent: same key → same partition.
    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }
}

// ─── Topic + partition coordinate ────────────────────────────────────────────

/// Identifies a specific partition within a topic.
///
/// Used as a `HashMap` key in the transactions crate when sending offset maps
/// to `send_offsets_to_transaction`. Derives `Hash + Eq` for that purpose.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// Formats as `"topic[partition]"` — e.g. `"events[2]"`.
impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.topic, self.partition)
    }
}

// ─── Delivery semantics ───────────────────────────────────────────────────────

/// The three Kafka delivery guarantees. Choosing the right one is a fundamental
/// Kafka interview topic — each variant documents the guarantee and failure mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliverySemantics {
    /// **At-most-once** — messages may be lost but are never duplicated.
    ///
    /// Achieved with `acks=0` (fire-and-forget). The producer does not wait for
    /// broker acknowledgment. If the network drops the request, the message is lost
    /// silently. Use only when loss is acceptable (e.g., metrics, clickstream sampling).
    AtMostOnce,

    /// **At-least-once** — messages are never lost but may be delivered more than once.
    ///
    /// Achieved with `acks=all` + `retries > 0`. If the broker acks but the ACK is
    /// lost in transit, the producer retries — the broker receives and stores the message
    /// a second time. Consumers must handle duplicates (idempotent handlers).
    AtLeastOnce,

    /// **Exactly-once** — messages are delivered exactly once, end-to-end.
    ///
    /// Achieved via the transactional Consume-Transform-Produce (CTP) pattern:
    /// `enable.idempotence=true` + `transactional.id` + `send_offsets_to_transaction`.
    /// The offset commit and the output-topic write share the same transaction — either
    /// both commit or both abort. Requires `isolation.level=read_committed` on consumers.
    ExactlyOnce,
}

// ─── Error model ─────────────────────────────────────────────────────────────

/// Unified error type for all Kafka client operations.
///
/// Uses `thiserror` so each variant gets a human-readable `Display` string and
/// integrates with the `?` operator via auto-generated `From` impls.
/// No `.unwrap()` appears in library code — every error path returns `KafkaError`.
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    /// Failed to send a message to the broker.
    #[error("produce error: {0}")]
    ProduceError(String),

    /// Failed to receive or decode a message from the broker.
    #[error("consume error: {0}")]
    ConsumeError(String),

    /// Failed to commit an offset to the broker.
    #[error("offset commit error: {0}")]
    OffsetCommitError(String),

    /// Partition rebalance failed or produced an unexpected assignment.
    #[error("rebalance error: {0}")]
    RebalanceError(String),

    /// TLS handshake or certificate validation failure.
    /// Triggered when connecting to the SSL listener without valid client certs (mTLS).
    #[error("TLS error: {0}")]
    TlsError(String),

    /// Authentication failure (SASL or mTLS client identity rejected).
    #[error("authentication error: {0}")]
    AuthError(String),

    /// Message payload could not be serialized or deserialized.
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// Invalid or missing configuration value.
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// Transaction-level error (begin / commit / abort / send_offsets failed).
    #[error("transaction error: {0}")]
    TransactionError(String),
}

// ─── TLS mode ─────────────────────────────────────────────────────────────────

/// Controls which TLS variant to use when connecting to the broker.
///
/// Cert paths are `PathBuf` — never inline `String` — so the OS resolves them
/// relative to the working directory and they are never confused with raw cert data.
#[derive(Debug, Clone)]
pub enum TlsMode {
    /// Plaintext — no TLS (only for local development on port 9092).
    Disabled,

    /// One-way TLS: broker presents a certificate; client verifies it against the CA.
    /// The broker does NOT verify the client's identity.
    Tls { ca_cert_path: PathBuf },

    /// Mutual TLS (mTLS): both broker and client present certificates.
    /// The broker verifies the client cert against the CA — proving client identity.
    /// This is the security posture required by the JD ("mTLS client authentication").
    MutualTls {
        ca_cert_path: PathBuf,
        client_cert_path: PathBuf,
        client_key_path: PathBuf,
    },
}

// ─── Security configuration ───────────────────────────────────────────────────

/// Kafka client security configuration.
///
/// Maps directly to rdkafka `ClientConfig` string properties via
/// `apply_security_config()` in the producer and consumer crates.
/// All secret material (passwords) comes from environment variables at runtime —
/// never from hardcoded strings — per IBM security policy.
#[derive(Debug, Clone)]
pub enum SecurityConfig {
    /// No authentication — plaintext on port 9092 (local development only).
    Plain,

    /// TLS or mTLS — encrypted connection on port 9093.
    Ssl(TlsMode),

    /// SASL/PLAIN — username + password over SASL.
    /// Password loaded from `KAFKA_SASL_PASSWORD` env var; not from a CLI positional arg
    /// (would appear in shell history).
    SaslPlain { username: String, password: String },

    /// SASL/SCRAM-SHA-256 — hashed credentials, stronger than PLAIN.
    SaslScramSha256 { username: String, password: String },

    /// SASL/SCRAM-SHA-512 — hashed credentials, strongest SCRAM variant.
    SaslScramSha512 { username: String, password: String },
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn topic_partition_display() {
        let tp = TopicPartition::new("events", 2);
        assert_eq!(tp.to_string(), "events[2]");
    }

    #[test]
    fn topic_partition_is_hashmap_key() {
        let mut map: HashMap<TopicPartition, i64> = HashMap::new();
        let tp = TopicPartition::new("orders", 0);
        map.insert(tp.clone(), 42);
        assert_eq!(map[&tp], 42);
    }

    #[test]
    fn kafka_error_messages() {
        let e = KafkaError::ProduceError("timeout".into());
        assert_eq!(e.to_string(), "produce error: timeout");

        let e = KafkaError::TlsError("certificate expired".into());
        assert_eq!(e.to_string(), "TLS error: certificate expired");

        let e = KafkaError::ConfigError("missing bootstrap.servers".into());
        assert_eq!(e.to_string(), "configuration error: missing bootstrap.servers");
    }

    #[test]
    fn tls_mode_mutual_tls_construction() {
        let mode = TlsMode::MutualTls {
            ca_cert_path: PathBuf::from("certs/ca.crt"),
            client_cert_path: PathBuf::from("certs/client.crt"),
            client_key_path: PathBuf::from("certs/client.key"),
        };
        // Pattern match to confirm variant and fields compile correctly.
        if let TlsMode::MutualTls {
            ca_cert_path,
            client_cert_path,
            client_key_path,
        } = mode
        {
            assert_eq!(ca_cert_path, PathBuf::from("certs/ca.crt"));
            assert_eq!(client_cert_path, PathBuf::from("certs/client.crt"));
            assert_eq!(client_key_path, PathBuf::from("certs/client.key"));
        } else {
            panic!("expected MutualTls variant");
        }
    }

    #[test]
    fn security_config_variants_construct() {
        let _plain = SecurityConfig::Plain;
        let _ssl = SecurityConfig::Ssl(TlsMode::Disabled);
        let _sasl = SecurityConfig::SaslPlain {
            username: "alice".into(),
            password: "secret".into(),
        };
        let _scram256 = SecurityConfig::SaslScramSha256 {
            username: "bob".into(),
            password: "secret".into(),
        };
    }

    #[test]
    fn kafka_message_new_and_with_key() {
        let msg = KafkaMessage::new("topic", "payload".to_string())
            .with_key(Bytes::from_static(b"key-1"));
        assert_eq!(msg.topic, "topic");
        assert_eq!(msg.key, Some(Bytes::from_static(b"key-1")));
        assert_eq!(msg.partition, -1); // not yet assigned
    }

    #[test]
    fn delivery_semantics_all_variants() {
        // Confirm each variant is distinct and Copy (no heap allocation).
        let a = DeliverySemantics::AtMostOnce;
        let b = DeliverySemantics::AtLeastOnce;
        let c = DeliverySemantics::ExactlyOnce;
        assert_ne!(a, b);
        assert_ne!(b, c);
    }
}
