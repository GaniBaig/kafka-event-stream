//! `ProducerConfig` — builder-pattern configuration for the Kafka producer.

use proto::SecurityConfig;

/// Acknowledgement level — controls the at-most/at-least/exactly-once tradeoff.
///
/// **Interview concept — acks + min.insync.replicas durability:**
/// `acks=all` alone is not sufficient if `min.insync.replicas=1` (default).
/// Both must be set: `acks=all` means the leader waits for ALL ISRs to ack;
/// `min.insync.replicas=2` means at least 2 replicas must be in-sync before the
/// write is accepted. Together they guarantee durability across a single broker failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    /// `acks=0` — fire-and-forget. No broker acknowledgment. At-most-once semantics.
    Zero,
    /// `acks=1` — leader acknowledges. Default. Can lose data on leader failover.
    One,
    /// `acks=all` — all in-sync replicas acknowledge. Required for idempotence + transactions.
    All,
}

impl Acks {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Acks::Zero => "0",
            Acks::One => "1",
            Acks::All => "all",
        }
    }
}

/// Compression codec for message batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionCodec {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            CompressionCodec::None => "none",
            CompressionCodec::Gzip => "gzip",
            CompressionCodec::Snappy => "snappy",
            CompressionCodec::Lz4 => "lz4",
            CompressionCodec::Zstd => "zstd",
        }
    }
}

/// Configuration for the Kafka producer.
///
/// Construct via `ProducerConfig::builder()` for ergonomic, named-field construction.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Comma-separated list of `host:port` broker addresses.
    pub bootstrap_servers: String,
    /// Default topic to produce to (can be overridden per message).
    pub topic: String,
    /// Acknowledgement level. Use `Acks::All` for durability.
    pub acks: Acks,
    /// Maximum number of retries on transient errors. 0 = no retries (at-most-once).
    pub max_retries: u32,
    /// Maximum number of messages batched together before sending.
    pub batch_size: u32,
    /// Compression algorithm applied to batches.
    pub compression: CompressionCodec,
    /// When true, enables the idempotent producer (`enable.idempotence=true`).
    /// Requires `acks=all`. Prevents duplicates on retry within a single partition.
    pub idempotent: bool,
    /// Optional security configuration (TLS / SASL).
    pub security: Option<SecurityConfig>,
}

impl ProducerConfig {
    /// Start building a `ProducerConfig`.
    pub fn builder(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> ProducerConfigBuilder {
        ProducerConfigBuilder::new(bootstrap_servers, topic)
    }
}

/// Builder for `ProducerConfig` — enables incremental, named construction.
#[derive(Debug)]
pub struct ProducerConfigBuilder {
    inner: ProducerConfig,
}

impl ProducerConfigBuilder {
    fn new(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            inner: ProducerConfig {
                bootstrap_servers: bootstrap_servers.into(),
                topic: topic.into(),
                acks: Acks::All,
                max_retries: 3,
                batch_size: 1000,
                compression: CompressionCodec::None,
                idempotent: false,
                security: None,
            },
        }
    }

    pub fn with_acks(mut self, acks: Acks) -> Self {
        self.inner.acks = acks;
        self
    }

    pub fn with_retries(mut self, retries: u32) -> Self {
        self.inner.max_retries = retries;
        self
    }

    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.inner.batch_size = batch_size;
        self
    }

    pub fn with_compression(mut self, codec: CompressionCodec) -> Self {
        self.inner.compression = codec;
        self
    }

    pub fn with_idempotence(mut self) -> Self {
        // Idempotence requires acks=all — enforce it here so callers cannot
        // accidentally set idempotence=true with acks < all.
        self.inner.idempotent = true;
        self.inner.acks = Acks::All;
        self
    }

    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.inner.security = Some(security);
        self
    }

    pub fn build(self) -> ProducerConfig {
        self.inner
    }
}

/// Metadata returned after a message is committed to the broker.
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// Topic the record was written to.
    pub topic: String,
    /// Partition the record was assigned to.
    pub partition: i32,
    /// Offset within the partition where the record was stored.
    /// A non-negative offset proves the broker accepted the message.
    pub offset: i64,
}
