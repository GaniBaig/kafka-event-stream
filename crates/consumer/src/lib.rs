//! # consumer
//!
//! Kafka consumer wrapping `rdkafka::consumer::StreamConsumer`.
//!
//! Demonstrates:
//! - Consumer group coordination (multiple consumers share partitions)
//! - Manual offset commit — "commit after process" semantics (at-least-once)
//! - `RebalanceListener` — logs partition assignments/revocations
//! - `CancellationToken` — graceful shutdown via `tokio::select!`
//! - Consumer lag tracking — `high_watermark - committed_offset` per partition

mod config;
mod consumer;
mod rebalance;

pub use config::{ConsumerConfig, OffsetReset};
pub use consumer::KafkaConsumer;
