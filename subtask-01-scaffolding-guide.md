# Sub-Task 1 — Project Scaffolding & Workspace Setup
### A step-by-step learning guide for Rust beginners

> **Goal:** By the end of this guide you will have a compiling Rust workspace with six crates,
> a running local Kafka broker in Docker, and Prometheus reachable at `localhost:9090`.
> Every command is explained so you learn *why*, not just *what*.

---

## 0 — Prerequisites (install these first)

| Tool | Why you need it | Status on your machine |
|---|---|---|
| Rust (stable) | The compiler and toolchain | ✅ `rustc 1.99.0` — installed at `~/.cargo/bin` |
| `cmake` | `rdkafka` compiles librdkafka (C code) from source | ✅ `cmake 4.4.4` |
| Podman + podman-compose | Runs Kafka, Prometheus, Grafana locally (drop-in Docker replacement) | ✅ `podman 5.4.2`, `podman-compose 1.5.0` |
| `git` | Version control | ✅ pre-installed |

**Your PATH fix (already applied):** Rust installs to `~/.cargo/bin` but zsh didn't know about it.
The line `. "$HOME/.cargo/env"` was added to your `~/.zshrc`. Every new terminal will now find `rustc` and `cargo` automatically.

Confirm in a **new terminal tab**:

```bash
rustc --version    # rustc 1.99.0 (...)
cargo --version    # cargo 1.99.0 (...)
cmake --version    # cmake version 4.4.4
podman --version   # podman version 5.x
```

> **Podman vs Docker:** Podman is a daemonless, rootless drop-in replacement for Docker.
> All `docker` commands in this guide become `podman` commands — the flags are identical.
> `docker compose` becomes `podman-compose`.

---

## 1 — What is a Cargo Workspace?

Rust uses **Cargo** as its build tool and package manager (like `npm` for Node, `pip` for Python).

A **workspace** is a single top-level `Cargo.toml` that groups multiple smaller packages called
**crates** so they share a single `target/` build directory and a single `Cargo.lock` file.

```
kafka-event-stream/        ← workspace root
├── Cargo.toml             ← workspace manifest (lists all member crates)
├── Cargo.lock             ← exact dependency versions (auto-generated, commit this)
├── target/                ← all compiled output (git-ignored)
└── crates/
    ├── proto/             ← library crate (shared types)
    ├── producer/          ← library crate (Kafka producer)
    ├── consumer/          ← library crate (Kafka consumer)
    ├── transactions/      ← library crate (exactly-once)
    ├── observability/     ← library crate (metrics + tracing)
    └── cli/               ← binary crate (the runnable program)
```

**Why split into crates?** Each crate compiles independently. If you change `producer/`, Rust only
recompiles `producer/` and anything that depends on it — not the whole project. It also enforces
clean boundaries: `cli` can depend on `producer`, but `producer` cannot accidentally import `cli`.

---

## 2 — Navigate to the project folder

All commands in this guide run from:

```bash
cd /Users/mirzaabdulganibaig/Documents/kafka-event-stream/Project
```

> Think of this as your "workspace root". Every `cargo` command runs here.

---

## 3 — Create the workspace `Cargo.toml`

Create a file called `Cargo.toml` **at the workspace root** with this content:

```bash
cat > Cargo.toml << 'EOF'
[workspace]
resolver = "2"
members = [
    "crates/proto",
    "crates/producer",
    "crates/consumer",
    "crates/transactions",
    "crates/observability",
    "crates/cli",
]

[workspace.dependencies]
tokio       = { version = "1",   features = ["full"] }
serde       = { version = "1",   features = ["derive"] }
serde_json  = "1"
bytes       = "1"
tracing     = "0.1"
thiserror   = "2"
anyhow      = "1"
EOF
```

**What each line means:**

- `[workspace]` — tells Cargo "this file defines a workspace, not a single crate"
- `resolver = "2"` — use the modern dependency resolver (required for edition 2021 workspaces)
- `members = [...]` — the list of crates in this workspace (relative paths)
- `[workspace.dependencies]` — declare shared dependencies **once** here; individual crates
  inherit them with `dependency-name.workspace = true` instead of repeating the version everywhere

---

## 4 — Create the six crates

### 4a — Library crates (`--lib`)

A **library crate** exposes functions and types for other crates to import. It has no `main()`.

```bash
cargo new --lib crates/proto
cargo new --lib crates/producer
cargo new --lib crates/consumer
cargo new --lib crates/transactions
cargo new --lib crates/observability
```

**What `cargo new --lib` does:**
- Creates the directory `crates/<name>/`
- Creates `crates/<name>/Cargo.toml` — the crate's own manifest
- Creates `crates/<name>/src/lib.rs` — the crate's entry point (the root module)

### 4b — Binary crate (`--bin`)

A **binary crate** has a `main()` function and produces an executable you can run.

```bash
cargo new --bin crates/cli
```

This creates `crates/cli/src/main.rs` with a `fn main()`.

### Verify the structure:

```bash
find crates -name "Cargo.toml" | sort
```

Expected output:
```
crates/cli/Cargo.toml
crates/consumer/Cargo.toml
crates/observability/Cargo.toml
crates/producer/Cargo.toml
crates/proto/Cargo.toml
crates/transactions/Cargo.toml
```

---

## 5 — Wire up dependencies in each crate's `Cargo.toml`

Each crate's `Cargo.toml` needs to declare what it depends on.

### 5a — `crates/proto/Cargo.toml`

Replace the `[dependencies]` section:

```toml
[package]
name    = "proto"
version = "0.1.0"
edition = "2021"

[dependencies]
bytes     = { workspace = true }
serde     = { workspace = true }
thiserror = { workspace = true }
```

**Explanation:**
- `{ workspace = true }` means "use the version declared in the root `Cargo.toml`'s
  `[workspace.dependencies]`". You don't repeat the version number here.
- `bytes` — Confluent-style zero-copy byte buffer for message payloads
- `serde` — serialization/deserialization framework (Serialize/Deserialize derives)
- `thiserror` — macro library for clean, ergonomic custom error types

### 5b — `crates/producer/Cargo.toml`

```toml
[package]
name    = "producer"
version = "0.1.0"
edition = "2021"

[dependencies]
proto        = { path = "../proto" }
rdkafka      = { version = "0.37", features = ["cmake-build"] }
tokio        = { workspace = true }
serde        = { workspace = true }
serde_json   = { workspace = true }
bytes        = { workspace = true }
tracing      = { workspace = true }
thiserror    = { workspace = true }
observability = { path = "../observability" }
```

**Key point — `rdkafka = { version = "0.37", features = ["cmake-build"] }`:**
- `rdkafka` is Rust's safe wrapper around librdkafka — the C library that every Confluent
  non-Java client is built on (Python's `confluent-kafka`, Go's `confluent-kafka-go`, etc.)
- `cmake-build` feature tells cargo to compile librdkafka from source using cmake
- This is why cmake is a prerequisite
- `{ path = "../proto" }` — local path dependency, meaning "use the `proto` crate in this same
  workspace"

### 5c — `crates/consumer/Cargo.toml`

```toml
[package]
name    = "consumer"
version = "0.1.0"
edition = "2021"

[dependencies]
proto         = { path = "../proto" }
rdkafka       = { version = "0.37", features = ["cmake-build"] }
tokio         = { workspace = true }
tokio-util    = { version = "0.7" }
tracing       = { workspace = true }
thiserror     = { workspace = true }
observability = { path = "../observability" }
```

**New here — `tokio-util`:**
- Provides `CancellationToken` — the idiomatic way to signal graceful shutdown across async tasks
- `CancellationToken` is in tokio-util's **default feature set** — no extra feature flag is needed
- Better than `Arc<Mutex<bool>>` because it integrates with `tokio::select!` natively

### 5d — `crates/transactions/Cargo.toml`

```toml
[package]
name    = "transactions"
version = "0.1.0"
edition = "2021"

[dependencies]
proto      = { path = "../proto" }
rdkafka    = { version = "0.37", features = ["cmake-build"] }
tokio      = { workspace = true }
tracing    = { workspace = true }
thiserror  = { workspace = true }
```

### 5e — `crates/observability/Cargo.toml`

```toml
[package]
name    = "observability"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio              = { workspace = true }
tracing            = { workspace = true }
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
prometheus         = "0.13"
axum               = "0.8"
```

**New here:**
- `tracing-subscriber` — configures how trace/log events are formatted and filtered at runtime
- `prometheus` — Prometheus metrics library: counters, gauges, histograms
- `axum` — a minimal HTTP server framework (used to serve `/metrics` endpoint)

### 5f — `crates/cli/Cargo.toml`

```toml
[package]
name    = "kafka-rs"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "kafka-rs"
path = "src/main.rs"

[dependencies]
proto         = { path = "../proto" }
producer      = { path = "../producer" }
consumer      = { path = "../consumer" }
transactions  = { path = "../transactions" }
observability = { path = "../observability" }
tokio         = { workspace = true }
tokio-util    = { version = "0.7" }
tracing       = { workspace = true }
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
clap          = { version = "4", features = ["derive"] }
anyhow        = { workspace = true }
serde_json    = { workspace = true }
```

**New here:**
- `clap` — command-line argument parser; the `derive` feature lets you define CLI args as Rust
  structs with `#[derive(Parser)]` — very ergonomic
- The `[[bin]]` section explicitly names the binary `kafka-rs`

---

## 6 — Write minimal stub code so the workspace compiles

Right now `cargo new` puts placeholder code in each crate. The placeholders are fine for library
crates but you need to verify the CLI binary has a valid `main`.

Check `crates/cli/src/main.rs` — it should already contain:

```rust
fn main() {
    println!("Hello, world!");
}
```

That is enough for now. Each library crate's `src/lib.rs` will just have the default empty content.

---

## 7 — First build

This is the moment of truth. Run:

```bash
cargo build --workspace
```

**What happens:**
1. Cargo reads the workspace `Cargo.toml`, discovers all six crates
2. Downloads all declared dependencies from crates.io (internet required first time)
3. Compiles librdkafka from C source using cmake (this takes 2–5 minutes the first time)
4. Compiles all six Rust crates in dependency order: `proto` first, then `producer`/`consumer`/
   `transactions` (which depend on `proto`), then `observability`, then `cli` (which depends on all)

**Expected output (abbreviated):**
```
   Compiling proto v0.1.0 (crates/proto)
   Compiling observability v0.1.0 (crates/observability)
   Compiling producer v0.1.0 (crates/producer)
   Compiling consumer v0.1.0 (crates/consumer)
   Compiling transactions v0.1.0 (crates/transactions)
   Compiling kafka-rs v0.1.0 (crates/cli)
    Finished dev [unoptimized + debuginfo] target(s) in Xs
```

If you see errors, the most common causes are:
- `cmake` not installed → install with `brew install cmake`
- Missing C toolchain → install Xcode Command Line Tools: `xcode-select --install`

---

## 8 — Run the linter and formatter

```bash
# Install the formatter component (one-time)
rustup component add rustfmt clippy

# Check formatting (does NOT modify files — just reports)
cargo fmt --all -- --check

# Run the linter
cargo clippy --workspace -- -D warnings
```

**What these do:**

- **`cargo fmt`** — `rustfmt` formats your code according to the official Rust style guide
  (indentation, spacing, line length). `--check` exits with an error if any file would change,
  which is what CI uses to enforce consistent style.

- **`cargo clippy`** — static analysis (linter) that catches hundreds of common mistakes:
  unused variables, inefficient patterns, potential panics, non-idiomatic code.
  `-D warnings` turns every warning into a build error — a zero-warning policy.

Run `cargo fmt --all` (without `--check`) to auto-format your code:

```bash
cargo fmt --all
```

---

## 9 — Create `.gitignore`

```bash
cat > .gitignore << 'EOF'
# Rust build output
/target/

# Generated certificates (never commit private keys)
/certs/

# Environment variables file
.env
.env.local

# macOS metadata
.DS_Store
EOF
```

**Why `certs/` must be git-ignored:** You will generate TLS certificates with private keys in
Sub-Task 6. Private keys MUST NOT be committed to version control — this is a security
requirement (hardcoded secrets rule).

---

## 10 — Write the Docker Compose file for Kafka

Create the `docker/` directory and the compose file:

```bash
mkdir -p docker
```

Create `docker/docker-compose.yml`:

```yaml
version: "3.8"

services:

  # ─── Kafka broker running in KRaft mode (no ZooKeeper required) ───
  kafka:
    image: confluentinc/cp-kafka:7.8.0
    hostname: kafka
    container_name: kafka
    ports:
      - "9092:9092"       # Client connections (plaintext)
      - "9101:9101"       # JMX (optional monitoring)
    environment:
      # KRaft mode: this node acts as both broker AND controller
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093"

      # Listener configuration:
      # PLAINTEXT     → client-facing port 9092 (what your Rust code connects to)
      # CONTROLLER    → internal port 29093 (KRaft leader election, internal only)
      # PLAINTEXT_HOST → maps internal 29092 to external 9092
      KAFKA_LISTENERS: "PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"

      # Storage and topic defaults
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 4
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"

      # Unique cluster ID (generate once with: docker run --rm confluentinc/cp-kafka:7.8.0 kafka-storage random-uuid)
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    volumes:
      - kafka_data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 30s

  # ─── Prometheus: scrapes metrics from your Rust binary ───
  prometheus:
    image: prom/prometheus:v3.1.0
    container_name: prometheus
    ports:
      - "9090:9090"       # Prometheus UI: http://localhost:9090
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    depends_on:
      - kafka

  # ─── Grafana: visualises Prometheus metrics ───
  grafana:
    image: grafana/grafana:11.4.0
    container_name: grafana
    ports:
      - "3000:3000"       # Grafana UI: http://localhost:3000 (admin/admin)
    environment:
      GF_SECURITY_ADMIN_PASSWORD: "admin"
    depends_on:
      - prometheus

volumes:
  kafka_data:
```

**Key concepts explained:**

- **KRaft mode** — Kafka 3.x+ can run without ZooKeeper. The broker elects its own controller
  using the Raft consensus algorithm. `KAFKA_PROCESS_ROLES=broker,controller` means this single
  node acts as both.
- **Listeners** — Kafka distinguishes between *internal* listeners (broker-to-broker) and
  *external* listeners (client-to-broker). `PLAINTEXT_HOST://0.0.0.0:9092` is what your Rust
  code connects to from the host machine.
- **`CLUSTER_ID`** — KRaft requires a pre-generated cluster ID. The one above is fixed for
  repeatability in development. In production you'd generate one with
  `kafka-storage random-uuid`.

---

## 11 — Write the Prometheus scrape config

Create `docker/prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "kafka-rs"
    static_configs:
      # host.docker.internal resolves to your Mac's IP from inside Docker
      # Port 9898 is where the cli binary's axum /metrics server will listen
      - targets: ["host.docker.internal:9898"]
```

**Explanation:** Prometheus *pulls* metrics by making HTTP GET requests to `/metrics` endpoints.
This config tells Prometheus "every 15 seconds, call `http://host.docker.internal:9898/metrics`".
Your Rust binary (Sub-Task 7) will serve that endpoint with `axum`.

---

## 12 — Start the Kafka environment (using Podman)

> You have **Podman**, not Docker. Use `podman-compose` everywhere instead of `docker compose`.
> The flags are identical — Podman is a drop-in replacement.

```bash
# Start all three containers in the background (-d = detached)
podman-compose -f docker/docker-compose.yml up -d
```

**Check that everything started:**

```bash
podman-compose -f docker/docker-compose.yml ps
```

Expected output: all three services show `running` or `healthy`.

**Verify Kafka is ready:**

```bash
podman exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

Expected: a long list of Kafka API versions without errors.

**Open Prometheus UI in your browser:**
```
http://localhost:9090
```

You should see the Prometheus interface. The `kafka-rs` target will show as "down" until you
run the Rust binary in Sub-Task 7 — that is expected.

---

## 13 — Commit your work

```bash
# Stage everything
git add .

# Commit with a clear message
git commit -m "feat: Sub-Task 1 — workspace scaffolding, six crate skeletons, docker compose"
```

**Why clear commit messages matter:** The JD asks for "operational readiness" and "documentation".
Every commit message is part of the project's history. Interviewers read commit logs.

**Push to GitHub:**

```bash
git push origin main
```

---

## 14 — Validation checklist

Run these commands and confirm each passes before moving to Sub-Task 2:

```bash
# 1. Workspace compiles
cargo build --workspace

# 2. Linter passes with zero warnings
cargo clippy --workspace -- -D warnings

# 3. All tests pass (only stub tests exist right now)
cargo test --workspace

# 4. Podman services are healthy  (Podman, not Docker)
podman-compose -f docker/docker-compose.yml ps

# 5. Kafka responds
podman exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# 6. Prometheus UI reachable
curl -s http://localhost:9090/-/healthy
# Expected output: Prometheus Server is Healthy.
```

---

## 15 — Rust concepts you practised in this sub-task

| Concept | Where you used it |
|---|---|
| **Cargo workspace** | Root `Cargo.toml` with `[workspace]` and `members = [...]` |
| **Crate types: `--lib` vs `--bin`** | 5 library crates + 1 binary crate |
| **`[workspace.dependencies]`** | Shared dependency versions declared once, inherited with `workspace = true` |
| **Path dependencies** | `proto = { path = "../proto" }` — local crate references within the workspace |
| **Feature flags** | `rdkafka = { features = ["cmake-build"] }`, `tokio = { features = ["full"] }` |
| **`cargo build`, `cargo fmt`, `cargo clippy`** | Build, format, and lint commands |
| **`.gitignore`** | Excluding `target/` and `certs/` from version control |

---

## 16 — What comes next (Sub-Task 2 preview)

Sub-Task 2 implements the `proto` crate — the shared type library that all other crates import.
You will write:

- `KafkaMessage<T>` — a generic struct (teaches Rust generics)
- `KafkaError` — a custom error enum using `thiserror` (teaches Rust error handling)
- `TopicPartition` — a struct with custom `Display` and `Hash` impls (teaches Rust traits)
- `DeliverySemantics` — an enum that encodes protocol guarantees at the type level
- `TlsMode` and `SecurityConfig` — enums covering all Kafka security configurations

These are all patterns that appear in interview code reviews for a senior Rust position.

---

*Guide written for kafka-event-stream Sub-Task 1 — Project Scaffolding.*
*Move to `subtask-02-proto-crate-guide.md` after completing all validation checks above.*
