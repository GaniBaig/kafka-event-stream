//! # transactions
//!
//! Exactly-once semantics via the Consume-Transform-Produce (CTP) pattern.
//!
//! ## How exactly-once works in Kafka
//!
//! Idempotent producer alone deduplicates retries on a SINGLE partition.
//! It does NOT coordinate writes across multiple partitions or the offset commit.
//!
//! The transactional CTP pattern does:
//! 1. `begin_transaction()`
//! 2. Produce output records to `output-topic`
//! 3. `send_offsets_to_transaction(input_offsets, consumer_group)` — atomically
//!    bundles the input-topic offset commit INTO the same transaction
//! 4. `commit_transaction()` — broker atomically commits both the output records
//!    AND the input offset. Either both land, or neither does.
//!
//! A consumer with `isolation.level=read_committed` only sees committed records —
//! aborted transactions are invisible, preventing partial reads.

mod config;
mod producer;

pub use config::TransactionalConfig;
pub use producer::TransactionalProducer;
