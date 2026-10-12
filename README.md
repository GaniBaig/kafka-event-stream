# kafka-event-stream

A production-quality Kafka event streaming prototype built entirely in **Rust**, using `rdkafka` — the same C library that underpins every Confluent non-Java client (Python, Go, .NET).

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Rust (stable) | 1.75+ | `rustup.rs` |
| cmake | 3.x+ | Compiles librdkafka from source |
| Podman / Docker | Any | Runs Kafka + Prometheus + Grafana |

## Quickstart (3 commands)

```bash
# 1. Start Kafka, Prometheus, Grafana
podman-compose -f docker/docker-compose.yml up -d

# 2. Build the workspace
cargo build --workspace

# 3. Produce 10 messages
cargo run -- produce --count 10
```

## Architecture

```
kafka-event-stream/
├── crates/
│   ├── proto/          shared types: KafkaMessage<T>, TopicPartition, KafkaError, SecurityConfig
│   ├── producer/       FutureProducer: at-most-once, at-least-once, idempotent
│   ├── consumer/       StreamConsumer: consumer groups, manual offset commit, RebalanceListener
│   ├── transactions/   TransactionalProducer: Consume-Transform-Produce (exactly-once)
│   ├── observability/  Prometheus metrics, axum /metrics server, JSON tracing
│   └── cli/            kafka-rs binary: produce / consume / transaction / benchmark
├── docker/
│   ├── docker-compose.yml      Kafka KRaft + Prometheus + Grafana
│   ├── docker-compose-ssl.yml  Kafka with SSL listener on port 9093
│   └── prometheus.yml          Scrape config targeting :9898/metrics
└── scripts/
    └── gen-certs.sh            Generates CA, broker cert, client cert for mTLS
```

## CLI Subcommands

```bash
# Produce N messages
cargo run -- produce --bootstrap localhost:9092 --topic events --count 50

# Consume until Ctrl-C
cargo run -- consume --topic events --group-id my-group

# Exactly-once CTP demo
cargo run -- transaction --input-topic input-topic --output-topic output-topic

# Benchmark — prints p50/p95/p99 latency table
cargo run -- benchmark --count 10000 --payload-size 256
```

## Running the mTLS Demo

```bash
# 1. Generate certificates (requires openssl + keytool)
bash scripts/gen-certs.sh

# 2. Start Kafka with SSL listener
podman-compose -f docker/docker-compose-ssl.yml up -d

# 3. Produce over mTLS (port 9093)
cargo run -- produce \
  --bootstrap localhost:9093 \
  --tls \
  --ca-cert certs/ca.crt \
  --client-cert certs/client.crt \
  --client-key certs/client.key

# 4. Verify rejection case — should fail with TlsError
cargo run -- produce --bootstrap localhost:9093
```

## Concept-to-Code Mapping

| Kafka Concept | Where it lives | JD Requirement |
|---|---|---|
| `KafkaMessage<T>`, generics, trait bounds | `crates/proto/src/lib.rs` | Strong Rust proficiency |
| `rdkafka` over librdkafka (C-FFI boundary) | `crates/producer/src/producer.rs` | Protocol-level client work |
| `acks=all`, idempotence, compression | `crates/producer/src/config.rs` | Delivery semantics |
| `RebalanceListener` + `ConsumerContext` | `crates/consumer/src/rebalance.rs` | Consumer groups, rebalancing |
| Manual offset commit after processing | `crates/consumer/src/consumer.rs` | At-least-once, offset management |
| `begin_transaction` → `send_offsets_to_transaction` → `commit` | `crates/transactions/src/producer.rs` | Exactly-once, transactions |
| `TlsMode::MutualTls`, `apply_security_config` | `crates/producer/src/security.rs` | mTLS/SASL security |
| `ObservabilityHooks` trait, `MetricsRegistry` | `crates/observability/src/` | Observability, SLI/SLO |
| `thiserror` error model, no `.unwrap()` | All library crates | Production-quality error handling |
| `CancellationToken` + `tokio::select!` | `crates/consumer/src/consumer.rs` | Production reliability |
| `Arc<dyn ObservabilityHooks + Send + Sync>` | `crates/producer/src/producer.rs` | API design, backward compat |
| GitHub Actions CI + `cargo audit` | `.github/workflows/ci.yml` | Release processes |
| p50/p95/p99 benchmark | `crates/cli/src/commands.rs` | Performance profiling |

## Rust Concepts Demonstrated

1. **Ownership + zero-copy** — `Bytes` for message keys (O(1) clone across tasks)
2. **Async/await + tokio::spawn** — producer and consumer run as independent tasks
3. **No `.unwrap()` in library code** — all errors propagate via `?` → `KafkaError`
4. **Generics + trait bounds** — `KafkaMessage<T>`, `F: Fn(...) -> Fut + Send`
5. **Custom trait impls** — `Display` for `TopicPartition`, `ConsumerContext` for `RebalanceListener`
6. **`tokio::select!`** — consumer loop races message stream vs cancellation signal
7. **`Arc<dyn Trait + Send + Sync>`** — `MetricsRegistry` shared without concrete type coupling
8. **Builder pattern** — `ProducerConfig::builder().with_acks().with_idempotence().build()`

## Observability

| Metric | Type | Labels |
|---|---|---|
| `kafka_messages_produced_total` | Counter | `topic` |
| `kafka_messages_consumed_total` | Counter | `topic`, `group_id` |
| `kafka_consumer_lag` | Gauge | `topic`, `partition`, `group_id` |
| `kafka_produce_latency_ms` | Histogram | `topic` |
| `kafka_consume_latency_ms` | Histogram | `topic`, `group_id` |
| `kafka_errors_total` | Counter | `error_type` |

- Prometheus UI: http://localhost:9090
- Grafana: http://localhost:3000 (admin / admin)
- Metrics endpoint: http://localhost:9898/metrics

## Security

- All cert paths come from CLI flags or `KAFKA_TLS_*` env vars — never hardcoded
- SASL passwords come from `KAFKA_SASL_PASSWORD` env var — never CLI positional args
- `certs/` is git-ignored — private keys are never committed
- TLS 1.2+ enforced via rdkafka's SSL config (`security.protocol=SSL`)
