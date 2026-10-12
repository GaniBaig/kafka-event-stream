//! # producer
//!
//! Kafka producer wrapping `rdkafka::producer::FutureProducer`.
//!
//! Demonstrates all three delivery semantics:
//! - **At-most-once** — `acks=0`, no retries (fire-and-forget)
//! - **At-least-once** — `acks=all`, `retries > 0` (may duplicate on retry)
//! - **Exactly-once preparation** — `acks=all` + `enable.idempotence=true`
//!   (dedup on single partition; full EOS requires the transactions crate)

mod config;
mod error_conv;
mod producer;
mod security;

pub use config::{Acks, CompressionCodec, ProducerConfig, ProducerConfigBuilder, RecordMetadata};
pub use producer::KafkaProducer;
pub use security::apply_security_config;
