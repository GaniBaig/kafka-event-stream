//! Command implementations for each CLI subcommand.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use consumer::{ConsumerConfig, KafkaConsumer};
use observability::{MetricsRegistry, ObservabilityHooks};
use producer::{KafkaProducer, ProducerConfig};
use proto::KafkaMessage;
use tokio_util::sync::CancellationToken;
use transactions::{TransactionalConfig, TransactionalProducer};

use crate::args::{BenchmarkArgs, ConsumeArgs, ProduceArgs, TransactionArgs};
use crate::security::{security_from_consume, security_from_produce};

// ─── produce ──────────────────────────────────────────────────────────────────

pub async fn produce(args: ProduceArgs, registry: Arc<MetricsRegistry>) -> Result<()> {
    let hooks: Arc<dyn ObservabilityHooks> = registry;
    let security = security_from_produce(&args);

    let mut config = ProducerConfig::builder(&args.bootstrap, &args.topic);
    if let Some(sec) = security {
        config = config.with_security(sec);
    }
    let config = config.build();

    let producer = KafkaProducer::new(config, hooks).context("failed to create producer")?;

    tracing::info!(count = args.count, topic = %args.topic, "producing messages");

    for i in 0..args.count {
        let msg = KafkaMessage::new(&args.topic, format!("message-{i}"));
        let meta = producer
            .produce_message(msg)
            .await
            .context("produce failed")?;
        tracing::info!(
            partition = meta.partition,
            offset = meta.offset,
            "produced"
        );
    }

    tracing::info!("done producing {} messages", args.count);
    Ok(())
}

// ─── consume ──────────────────────────────────────────────────────────────────

pub async fn consume(
    args: ConsumeArgs,
    registry: Arc<MetricsRegistry>,
    cancel: CancellationToken,
) -> Result<()> {
    let hooks: Arc<dyn ObservabilityHooks> = registry;
    let security = security_from_consume(&args);

    let mut config = ConsumerConfig::new(&args.bootstrap, &args.group_id, vec![args.topic.clone()]);
    if let Some(sec) = security {
        config = config.with_security(sec);
    }

    let consumer = KafkaConsumer::new(config, hooks).context("failed to create consumer")?;

    tracing::info!(topic = %args.topic, group = %args.group_id, "starting consume loop");

    consumer
        .consume_loop(
            |topic, payload| async move {
                println!("[{topic}] {payload}");
                Ok(())
            },
            cancel,
        )
        .await
        .context("consume loop error")?;

    Ok(())
}

// ─── transaction ──────────────────────────────────────────────────────────────

pub async fn transaction(args: TransactionArgs) -> Result<()> {
    let config = TransactionalConfig::new(&args.bootstrap, &args.transactional_id);
    let producer = TransactionalProducer::new(config).context("failed to create txn producer")?;

    // Stub CTP demo: produce 5 messages to output-topic in a single transaction.
    // In a real CTP pipeline, messages would come from a consumer on input-topic
    // and offsets would be sent via send_offsets_to_transaction.
    producer.begin_transaction().context("begin_transaction")?;

    for i in 0..5 {
        producer
            .produce(&args.output_topic, None, &format!("txn-message-{i}"))
            .context("transactional produce")?;
    }

    producer.flush().context("flush")?;
    producer.commit_transaction().context("commit_transaction")?;

    tracing::info!(
        output_topic = %args.output_topic,
        "transaction committed — 5 messages written atomically"
    );
    Ok(())
}

// ─── benchmark ────────────────────────────────────────────────────────────────

pub async fn benchmark(args: BenchmarkArgs, registry: Arc<MetricsRegistry>) -> Result<()> {
    let hooks: Arc<dyn ObservabilityHooks> = registry;

    let config = ProducerConfig::builder(&args.bootstrap, &args.topic).build();
    let producer = KafkaProducer::new(config, hooks).context("failed to create producer")?;

    let payload = "x".repeat(args.payload_size);
    let mut latencies: Vec<Duration> = Vec::with_capacity(args.count as usize);

    tracing::info!(count = args.count, payload_bytes = args.payload_size, "benchmark starting");

    for i in 0..args.count {
        let msg = KafkaMessage::new(&args.topic, payload.clone());
        let start = Instant::now();
        producer
            .produce_message(msg)
            .await
            .context(format!("benchmark produce {i}"))?;
        latencies.push(start.elapsed());
    }

    latencies.sort_unstable();

    let p50 = latencies[latencies.len() * 50 / 100];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];
    let total: Duration = latencies.iter().sum();
    let throughput = args.count as f64 / total.as_secs_f64();

    println!("\n──────────────────────────────────────");
    println!("  Benchmark Results ({} messages)", args.count);
    println!("──────────────────────────────────────");
    println!("  p50 latency : {:>8.2} ms", p50.as_secs_f64() * 1000.0);
    println!("  p95 latency : {:>8.2} ms", p95.as_secs_f64() * 1000.0);
    println!("  p99 latency : {:>8.2} ms", p99.as_secs_f64() * 1000.0);
    println!("  throughput  : {:>8.0} msg/s", throughput);
    println!("──────────────────────────────────────\n");

    Ok(())
}
