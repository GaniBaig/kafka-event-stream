//! Converts `rdkafka::error::KafkaError` into the proto `KafkaError` type.
//!
//! Rust's orphan rule prevents `impl From<rdkafka::Error> for proto::KafkaError`
//! because both types are defined in external crates. We use a conversion function
//! instead — it is called wherever an rdkafka error needs mapping.

use proto::KafkaError;

/// Convert an rdkafka error to the project's unified `KafkaError`.
pub(crate) fn from_rdkafka(e: rdkafka::error::KafkaError) -> KafkaError {
    let msg = e.to_string();
    match e {
        rdkafka::error::KafkaError::MessageProduction(_) => KafkaError::ProduceError(msg),
        rdkafka::error::KafkaError::MessageConsumption(_) => KafkaError::ConsumeError(msg),
        rdkafka::error::KafkaError::OffsetFetch(_) => KafkaError::OffsetCommitError(msg),
        rdkafka::error::KafkaError::Rebalance(_) => KafkaError::RebalanceError(msg),
        _ => KafkaError::ProduceError(msg),
    }
}
