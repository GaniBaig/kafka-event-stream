//! CLI argument definitions using `clap` derive macros.
//!
//! `///` doc comments on every field auto-appear in `--help` output.
//! `From<ProduceArgs> for ProducerConfig` keeps CLI types separate from
//! library config types — the CLI crate never leaks into library crates.

use clap::{Parser, Subcommand};

/// Kafka event streaming demo — producer, consumer, transactions, benchmark.
#[derive(Parser)]
#[command(name = "kafka-rs", version, author)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Produce messages to a Kafka topic.
    Produce(ProduceArgs),
    /// Consume messages from a Kafka topic (runs until Ctrl-C).
    Consume(ConsumeArgs),
    /// Run the Consume-Transform-Produce exactly-once demo.
    Transaction(TransactionArgs),
    /// Benchmark produce throughput and print p50/p95/p99 latency.
    Benchmark(BenchmarkArgs),
}

// ─── Produce args ─────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
pub struct ProduceArgs {
    /// Kafka broker bootstrap address (e.g. localhost:9092).
    #[arg(long, default_value = "localhost:9092", env = "KAFKA_BOOTSTRAP")]
    pub bootstrap: String,

    /// Target topic name.
    #[arg(long, default_value = "events")]
    pub topic: String,

    /// Number of messages to produce.
    #[arg(long, default_value_t = 10)]
    pub count: u32,

    /// Delivery semantics: at-most-once | at-least-once | exactly-once.
    #[arg(long, default_value = "at-least-once")]
    pub semantics: String,

    /// Enable TLS connection (uses ca-cert, client-cert, client-key).
    #[arg(long)]
    pub tls: bool,

    /// Path to the CA certificate file (PEM).
    #[arg(long)]
    pub ca_cert: Option<String>,

    /// Path to the client certificate file (PEM) — required for mTLS.
    #[arg(long)]
    pub client_cert: Option<String>,

    /// Path to the client private key file (PEM) — required for mTLS.
    #[arg(long)]
    pub client_key: Option<String>,

    /// SASL username. Password must be in KAFKA_SASL_PASSWORD env var.
    #[arg(long)]
    pub sasl_username: Option<String>,

    /// SASL mechanism: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512.
    #[arg(long)]
    pub sasl_mechanism: Option<String>,
}

// ─── Consume args ─────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
pub struct ConsumeArgs {
    /// Kafka broker bootstrap address.
    #[arg(long, default_value = "localhost:9092", env = "KAFKA_BOOTSTRAP")]
    pub bootstrap: String,

    /// Topic to consume from.
    #[arg(long, default_value = "events")]
    pub topic: String,

    /// Consumer group ID.
    #[arg(long, default_value = "kafka-rs-group", env = "KAFKA_GROUP_ID")]
    pub group_id: String,

    /// Enable TLS connection.
    #[arg(long)]
    pub tls: bool,

    /// Path to the CA certificate file (PEM).
    #[arg(long)]
    pub ca_cert: Option<String>,

    /// Path to the client certificate file (PEM).
    #[arg(long)]
    pub client_cert: Option<String>,

    /// Path to the client private key file (PEM).
    #[arg(long)]
    pub client_key: Option<String>,

    /// SASL username.
    #[arg(long)]
    pub sasl_username: Option<String>,

    /// SASL mechanism.
    #[arg(long)]
    pub sasl_mechanism: Option<String>,
}

// ─── Transaction args ─────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
pub struct TransactionArgs {
    /// Kafka broker bootstrap address.
    #[arg(long, default_value = "localhost:9092", env = "KAFKA_BOOTSTRAP")]
    pub bootstrap: String,

    /// Input topic (messages are consumed from here).
    #[arg(long, default_value = "input-topic")]
    pub input_topic: String,

    /// Output topic (transformed messages are written here).
    #[arg(long, default_value = "output-topic")]
    pub output_topic: String,

    /// Stable transactional producer ID (must be unique per process, not a UUID).
    #[arg(long, default_value = "kafka-rs-ctp-1", env = "KAFKA_TRANSACTIONAL_ID")]
    pub transactional_id: String,
}

// ─── Benchmark args ───────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
pub struct BenchmarkArgs {
    /// Kafka broker bootstrap address.
    #[arg(long, default_value = "localhost:9092", env = "KAFKA_BOOTSTRAP")]
    pub bootstrap: String,

    /// Topic to benchmark against.
    #[arg(long, default_value = "benchmark-topic")]
    pub topic: String,

    /// Number of messages to produce (default 10_000).
    #[arg(long, default_value_t = 10_000)]
    pub count: u32,

    /// Message payload size in bytes.
    #[arg(long, default_value_t = 128)]
    pub payload_size: usize,
}
