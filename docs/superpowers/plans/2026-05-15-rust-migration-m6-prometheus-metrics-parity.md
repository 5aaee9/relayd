# Rust Migration M6 Prometheus Metrics Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Rust `/metrics` match Zig Prometheus listener metrics by calculating stateful per-listener rx/tx byte rates for TCP, UDP, and dual-protocol runtime snapshots.

**Architecture:** Extend `src/prometheus.rs` with `ListenerMetricsRate` and `RateCalculator`, preserving `ListenerMetricsSnapshot` as runtime input. Store one shared `RateCalculator` in HTTP `AppState` behind a Tokio mutex and render rate rows from `/metrics`. Runtime modules continue to expose cumulative snapshots; M6 only changes rate calculation/rendering and tests.

**Tech Stack:** Rust 1.95, Tokio `Mutex`, Axum state, existing `RuntimeFacade`, existing real/TCP/UDP runtimes, `cargo test --locked`, `cargo clippy --locked --lib --tests -- -D warnings`, `zig build test`.

---

## File Structure

- Modify: `src/prometheus.rs` — add rate struct/calculator, render rates, unit tests.
- Modify: `src/http/control_plane.rs` — add shared rate calculator to app state and use it in `/metrics` handler.
- Modify: `src/runtime/real.rs` tests if end-to-end real runtime rate coverage is best colocated there, or add HTTP tests in `src/http/control_plane.rs` using seeded runtime rows.
- Modify after implementation: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md` — add M6 implementation status.
- Preserve: `src/runtime/facade.rs`, `src/model.rs`, `/v1/metrics` JSON field definitions.

## Acceptance checklist

- [ ] Independent plan reviewer returns `APPROVED` before implementation.
- [ ] `RateCalculator` is keyed by `(port, protocol)` and stores byte totals plus timestamp.
- [ ] First sample emits zero rates.
- [ ] Positive byte deltas over positive elapsed time emit positive floating-point rates.
- [ ] Same numeric port with `tcp` and `udp` rows keeps independent samples when scrape row order changes.
- [ ] Zero/negative elapsed time emits zero rates.
- [ ] Counter decrease/reset emits zero for that direction.
- [ ] Stale listener keys are removed when absent from current snapshots.
- [ ] Prometheus output keeps exact metric names, `# TYPE` lines, content type, labels, and first-scrape zero-rate behavior through `render_rates`.
- [ ] HTTP `/metrics` uses a shared stateful calculator across requests.
- [ ] `/v1/metrics` JSON compatibility tests remain unchanged and passing.
- [ ] TCP, UDP, and dual-protocol rows have tests proving positive rates after traffic/repeated scrapes.
- [ ] `cargo fmt -- --check` passes.
- [ ] `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked` passes.
- [ ] `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings` passes.
- [ ] `zig build test` passes.
- [ ] Final independent spec-compliance reviewer returns `APPROVED` before docs commit/push.

## Task 1: Add Rust Prometheus rate calculator and rate renderer

**Files:**
- Modify: `src/prometheus.rs`

- [ ] **Step 1: Write failing calculator tests**

Replace/extend the test module in `src/prometheus.rs` with tests that assert calculator behavior before implementation exists:

```rust
#[test]
fn rate_calculator_reports_zero_for_first_sample() {
    let mut calculator = RateCalculator::default();
    let rows = [ListenerMetricsSnapshot {
        port: 7000,
        protocol: Protocol::Tcp,
        connections_current: 2,
        rx_bytes_total: 100,
        tx_bytes_total: 50,
    }];

    let rates = calculator.calculate(&rows, 1_000);

    assert_eq!(rates.len(), 1);
    assert_eq!(rates[0].connections_current, 2);
    assert_eq!(rates[0].rx_bytes_per_second, 0.0);
    assert_eq!(rates[0].tx_bytes_per_second, 0.0);
}

#[test]
fn rate_calculator_reports_positive_delta_rates() {
    let mut calculator = RateCalculator::default();
    let first = [ListenerMetricsSnapshot {
        port: 7000,
        protocol: Protocol::Tcp,
        connections_current: 1,
        rx_bytes_total: 100,
        tx_bytes_total: 50,
    }];
    calculator.calculate(&first, 1_000);

    let second = [ListenerMetricsSnapshot {
        port: 7000,
        protocol: Protocol::Tcp,
        connections_current: 1,
        rx_bytes_total: 300,
        tx_bytes_total: 150,
    }];
    let rates = calculator.calculate(&second, 2_000);

    assert_eq!(rates[0].rx_bytes_per_second, 200.0);
    assert_eq!(rates[0].tx_bytes_per_second, 100.0);
}

#[test]
fn rate_calculator_clamps_zero_elapsed_and_counter_reset_to_zero() {
    let mut calculator = RateCalculator::default();
    calculator.calculate(&[ListenerMetricsSnapshot {
        port: 7000,
        protocol: Protocol::Udp,
        connections_current: 1,
        rx_bytes_total: 300,
        tx_bytes_total: 150,
    }], 1_000);

    let same_time = calculator.calculate(&[ListenerMetricsSnapshot {
        port: 7000,
        protocol: Protocol::Udp,
        connections_current: 1,
        rx_bytes_total: 500,
        tx_bytes_total: 250,
    }], 1_000);
    assert_eq!(same_time[0].rx_bytes_per_second, 0.0);
    assert_eq!(same_time[0].tx_bytes_per_second, 0.0);

    let reset = calculator.calculate(&[ListenerMetricsSnapshot {
        port: 7000,
        protocol: Protocol::Udp,
        connections_current: 1,
        rx_bytes_total: 100,
        tx_bytes_total: 50,
    }], 2_000);
    assert_eq!(reset[0].rx_bytes_per_second, 0.0);
    assert_eq!(reset[0].tx_bytes_per_second, 0.0);
}

#[test]
fn rate_calculator_keeps_same_port_protocol_keys_independent_when_rows_reorder() {
    let mut calculator = RateCalculator::default();
    let first = [
        ListenerMetricsSnapshot { port: 7000, protocol: Protocol::Tcp, connections_current: 1, rx_bytes_total: 100, tx_bytes_total: 50 },
        ListenerMetricsSnapshot { port: 7000, protocol: Protocol::Udp, connections_current: 2, rx_bytes_total: 1000, tx_bytes_total: 500 },
    ];
    calculator.calculate(&first, 1_000);

    let second_reordered = [
        ListenerMetricsSnapshot { port: 7000, protocol: Protocol::Udp, connections_current: 2, rx_bytes_total: 1200, tx_bytes_total: 700 },
        ListenerMetricsSnapshot { port: 7000, protocol: Protocol::Tcp, connections_current: 1, rx_bytes_total: 150, tx_bytes_total: 75 },
    ];
    let rates = calculator.calculate(&second_reordered, 2_000);

    let udp = rates.iter().find(|row| row.protocol == Protocol::Udp).unwrap();
    let tcp = rates.iter().find(|row| row.protocol == Protocol::Tcp).unwrap();
    assert_eq!(udp.rx_bytes_per_second, 200.0);
    assert_eq!(udp.tx_bytes_per_second, 200.0);
    assert_eq!(tcp.rx_bytes_per_second, 50.0);
    assert_eq!(tcp.tx_bytes_per_second, 25.0);
}

#[test]
fn rate_renderer_preserves_type_lines_labels_and_first_sample_zero_rates() {
    let mut calculator = RateCalculator::default();
    let snapshots = [ListenerMetricsSnapshot {
        port: 7000,
        protocol: Protocol::Tcp,
        connections_current: 3,
        rx_bytes_total: 500,
        tx_bytes_total: 250,
    }];
    let rates = calculator.calculate(&snapshots, 1_000);

    let output = render_rates(&rates);

    assert!(output.contains("# TYPE relayd_connections_current gauge\n"));
    assert!(output.contains("# TYPE relayd_rx_bytes_per_second gauge\n"));
    assert!(output.contains("# TYPE relayd_tx_bytes_per_second gauge\n"));
    assert!(output.contains("relayd_connections_current{port=\"7000\",protocol=\"tcp\"} 3\n"));
    assert!(output.contains("relayd_rx_bytes_per_second{port=\"7000\",protocol=\"tcp\"} 0\n"));
    assert!(output.contains("relayd_tx_bytes_per_second{port=\"7000\",protocol=\"tcp\"} 0\n"));
    assert!(!output.contains("{\""));
}

#[test]
fn rate_calculator_removes_stale_listener_keys() {
    let mut calculator = RateCalculator::default();
    let first = [
        ListenerMetricsSnapshot { port: 7000, protocol: Protocol::Tcp, connections_current: 1, rx_bytes_total: 100, tx_bytes_total: 50 },
        ListenerMetricsSnapshot { port: 7001, protocol: Protocol::Udp, connections_current: 1, rx_bytes_total: 200, tx_bytes_total: 75 },
    ];
    calculator.calculate(&first, 1_000);
    assert_eq!(calculator.sample_count(), 2);

    let second = [ListenerMetricsSnapshot {
        port: 7000,
        protocol: Protocol::Tcp,
        connections_current: 1,
        rx_bytes_total: 150,
        tx_bytes_total: 100,
    }];
    calculator.calculate(&second, 2_000);

    assert_eq!(calculator.sample_count(), 1);
}
```

- [ ] **Step 2: Run red tests**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked prometheus::tests::rate_calculator -- --nocapture
```

Expected: FAIL to compile because `RateCalculator` does not exist.

- [ ] **Step 3: Implement calculator and rate renderer**

Update `src/prometheus.rs`:

```rust
use crate::model::Protocol;
use crate::runtime::facade::ListenerMetricsSnapshot;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Debug, Clone, PartialEq)]
pub struct ListenerMetricsRate {
    pub port: u16,
    pub protocol: Protocol,
    pub connections_current: u64,
    pub rx_bytes_per_second: f64,
    pub tx_bytes_per_second: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Key {
    port: u16,
    protocol: Protocol,
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.port.hash(state);
        self.protocol.as_str().hash(state);
    }
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    rx_bytes_total: u64,
    tx_bytes_total: u64,
    timestamp_ms: i64,
}

#[derive(Debug, Default)]
pub struct RateCalculator {
    samples: HashMap<Key, Sample>,
}

impl RateCalculator {
    pub fn calculate(
        &mut self,
        snapshots: &[ListenerMetricsSnapshot],
        timestamp_ms: i64,
    ) -> Vec<ListenerMetricsRate> {
        let mut rates = Vec::with_capacity(snapshots.len());
        let mut current_keys = HashSet::with_capacity(snapshots.len());
        for snapshot in snapshots {
            let key = Key { port: snapshot.port, protocol: snapshot.protocol };
            current_keys.insert(key);
            let previous = self.samples.get(&key).copied();
            rates.push(calculate_rate(snapshot, previous, timestamp_ms));
            self.samples.insert(key, Sample {
                rx_bytes_total: snapshot.rx_bytes_total,
                tx_bytes_total: snapshot.tx_bytes_total,
                timestamp_ms,
            });
        }
        self.samples.retain(|key, _| current_keys.contains(key));
        rates
    }

    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }
}

fn calculate_rate(
    snapshot: &ListenerMetricsSnapshot,
    previous: Option<Sample>,
    timestamp_ms: i64,
) -> ListenerMetricsRate {
    let mut rx_rate = 0.0;
    let mut tx_rate = 0.0;
    if let Some(previous) = previous {
        let elapsed_ms = timestamp_ms.saturating_sub(previous.timestamp_ms);
        if elapsed_ms > 0 {
            let elapsed_seconds = elapsed_ms as f64 / 1000.0;
            rx_rate = bytes_per_second(snapshot.rx_bytes_total, previous.rx_bytes_total, elapsed_seconds);
            tx_rate = bytes_per_second(snapshot.tx_bytes_total, previous.tx_bytes_total, elapsed_seconds);
        }
    }
    ListenerMetricsRate {
        port: snapshot.port,
        protocol: snapshot.protocol,
        connections_current: snapshot.connections_current,
        rx_bytes_per_second: rx_rate,
        tx_bytes_per_second: tx_rate,
    }
}

fn bytes_per_second(current: u64, previous: u64, elapsed_seconds: f64) -> f64 {
    if current <= previous { 0.0 } else { (current - previous) as f64 / elapsed_seconds }
}

pub fn render_rates(rows: &[ListenerMetricsRate]) -> String {
    let mut out = String::new();
    out.push_str("# TYPE relayd_connections_current gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_connections_current{{port=\"{}\",protocol=\"{}\"}} {}\n",
            row.port,
            row.protocol.as_str(),
            row.connections_current
        ));
    }
    out.push_str("# TYPE relayd_rx_bytes_per_second gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_rx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} {}\n",
            row.port,
            row.protocol.as_str(),
            row.rx_bytes_per_second
        ));
    }
    out.push_str("# TYPE relayd_tx_bytes_per_second gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_tx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} {}\n",
            row.port,
            row.protocol.as_str(),
            row.tx_bytes_per_second
        ));
    }
    out
}

pub fn render(rows: &[ListenerMetricsSnapshot]) -> String {
    let mut calculator = RateCalculator::default();
    let rates = calculator.calculate(rows, 0);
    render_rates(&rates)
}
```

- [ ] **Step 4: Run green calculator tests**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked prometheus::tests -- --nocapture
```

Expected: PASS.

## Task 2: Wire `/metrics` to shared stateful rate calculator

**Files:**
- Modify: `src/http/control_plane.rs`
- Modify: `src/prometheus.rs`

- [ ] **Step 1: Add failing HTTP statefulness test with seeded rows**

In `src/http/control_plane.rs`, extend `prometheus_metrics_uses_seeded_rows_text_content_type_labels_and_no_json` or add a new test:

```rust
#[tokio::test]
async fn prometheus_metrics_calculates_rates_across_scrapes() {
    let (app, _, runtime, _, _file) = test_app().await;
    runtime.seed_listener_metrics(vec![crate::runtime::facade::ListenerMetricsSnapshot {
        port: 10000,
        protocol: Protocol::Tcp,
        connections_current: 1,
        rx_bytes_total: 100,
        tx_bytes_total: 50,
    }]);

    let (status, _, first_body) = request(app.clone(), Method::GET, "/metrics", "", Some("Bearer secret-token")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(first_body.contains("relayd_rx_bytes_per_second{port=\"10000\",protocol=\"tcp\"} 0\n"));

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    runtime.seed_listener_metrics(vec![crate::runtime::facade::ListenerMetricsSnapshot {
        port: 10000,
        protocol: Protocol::Tcp,
        connections_current: 1,
        rx_bytes_total: 300,
        tx_bytes_total: 150,
    }]);

    let (status, _, second_body) = request(app, Method::GET, "/metrics", "", Some("Bearer secret-token")).await;
    assert_eq!(status, StatusCode::OK);
    assert_metric_sample_positive(&second_body, "relayd_rx_bytes_per_second", 10000, "tcp");
    assert_metric_sample_positive(&second_body, "relayd_tx_bytes_per_second", 10000, "tcp");
}
```

Add a helper in the test module:

```rust
fn assert_metric_sample_positive(body: &str, name: &str, port: u16, protocol: &str) {
    let prefix = format!("{name}{{port=\"{port}\",protocol=\"{protocol}\"}} ");
    let line = body.lines().find(|line| line.starts_with(&prefix)).expect("metric line missing");
    let value: f64 = line[prefix.len()..].parse().expect("metric sample value parses");
    assert!(value > 0.0, "expected {line} to be positive");
}
```

- [ ] **Step 2: Run red HTTP test**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked http::control_plane::tests::prometheus_metrics_calculates_rates_across_scrapes -- --nocapture
```

Expected: FAIL because current handler uses stateless `render` and always emits zero.

- [ ] **Step 3: Add shared calculator to AppState and handler**

In `src/http/control_plane.rs`, import `RateCalculator`, `render_rates`, and `tokio::sync::Mutex` while preserving the existing public `AppState`, `auth_token`, manual `Clone`, and `AppState::new` constructor shape:

```rust
use crate::prometheus::{RateCalculator, render_rates};
use tokio::sync::Mutex;

pub struct AppState<R: RuntimeFacade> {
    pub service: Arc<Service<R>>,
    pub metrics: Arc<Metrics>,
    pub auth_token: Arc<str>,
    pub prometheus_rates: Arc<Mutex<RateCalculator>>,
}

impl<R: RuntimeFacade> Clone for AppState<R> {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            metrics: Arc::clone(&self.metrics),
            auth_token: Arc::clone(&self.auth_token),
            prometheus_rates: Arc::clone(&self.prometheus_rates),
        }
    }
}

impl<R: RuntimeFacade> AppState<R> {
    pub fn new(
        service: Arc<Service<R>>,
        metrics: Arc<Metrics>,
        auth_token: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            service,
            metrics,
            auth_token: auth_token.into(),
            prometheus_rates: Arc::new(Mutex::new(RateCalculator::default())),
        }
    }
}
```

Update handler:

```rust
async fn prometheus_metrics<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
) -> Response {
    match state.service.snapshot_listener_metrics().await {
        Ok(rows) => {
            let timestamp_ms = current_time_ms();
            let rates = state.prometheus_rates.lock().await.calculate(&rows, timestamp_ms);
            (
                StatusCode::OK,
                [(CONTENT_TYPE, crate::prometheus::CONTENT_TYPE)],
                render_rates(&rates),
            )
                .into_response()
        }
        Err(error) => service_error_response(error),
    }
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
```

- [ ] **Step 4: Run green HTTP tests**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked http::control_plane::tests::prometheus_metrics -- --nocapture
```

Expected: PASS. Existing `/v1/metrics` JSON test must still pass.

## Task 3: Add real-runtime TCP/UDP/dual-protocol positive rate coverage

**Files:**
- Modify: `src/runtime/real.rs`
- Modify: `src/prometheus.rs` if helper access is needed

- [ ] **Step 1: Add test helpers in `src/runtime/real.rs` test module**

Add helpers:

```rust
fn metric_rate(rows: &[crate::prometheus::ListenerMetricsRate], port: u16, protocol: Protocol) -> &crate::prometheus::ListenerMetricsRate {
    rows.iter().find(|row| row.port == port && row.protocol == protocol).expect("rate row missing")
}

async fn calculated_rates_after_traffic(runtime: &RealRuntime, timestamp_ms: i64) -> Vec<crate::prometheus::ListenerMetricsRate> {
    let mut calculator = crate::prometheus::RateCalculator::default();
    let first = runtime.snapshot_listener_metrics().await.unwrap();
    calculator.calculate(&first, timestamp_ms);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = runtime.snapshot_listener_metrics().await.unwrap();
    calculator.calculate(&second, timestamp_ms + 1_000)
}
```

- [ ] **Step 2: Add TCP/UDP/dual positive-rate tests**

Append tests:

```rust
#[tokio::test]
async fn real_runtime_tcp_metrics_feed_positive_prometheus_rates() {
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let relay_port = free_tcp_port().await;
    let (target_port, target_task) = start_tcp_echo_server().await;
    runtime.create(&allocation("tcp-rates", Protocol::Tcp, relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();

    let mut calculator = crate::prometheus::RateCalculator::default();
    calculator.calculate(&runtime.snapshot_listener_metrics().await.unwrap(), 1_000);
    tcp_round_trip(relay_port, b"tcp-rate").await;
    let rates = calculator.calculate(&runtime.snapshot_listener_metrics().await.unwrap(), 2_000);
    let row = metric_rate(&rates, relay_port, Protocol::Tcp);
    assert!(row.rx_bytes_per_second > 0.0);
    assert!(row.tx_bytes_per_second > 0.0);

    runtime.delete("tcp-rates", 500).await.unwrap();
    target_task.abort();
}

#[tokio::test]
async fn real_runtime_udp_metrics_feed_positive_prometheus_rates() {
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let relay_port = free_udp_port().await;
    let (target_port, target_task) = start_udp_echo_server().await;
    runtime.create(&allocation("udp-rates", Protocol::Udp, relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();

    let mut calculator = crate::prometheus::RateCalculator::default();
    calculator.calculate(&runtime.snapshot_listener_metrics().await.unwrap(), 1_000);
    udp_round_trip(relay_port, b"udp-rate").await;
    let rates = calculator.calculate(&runtime.snapshot_listener_metrics().await.unwrap(), 2_000);
    let row = metric_rate(&rates, relay_port, Protocol::Udp);
    assert_eq!(row.connections_current, 1);
    assert!(row.rx_bytes_per_second > 0.0);
    assert!(row.tx_bytes_per_second > 0.0);

    runtime.delete("udp-rates", 500).await.unwrap();
    target_task.abort();
}

#[tokio::test]
async fn real_runtime_both_metrics_feed_separate_tcp_udp_prometheus_rates() {
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let relay_port = free_tcp_udp_port_pair().await;
    let target_port = free_tcp_udp_port_pair().await;
    let tcp_task = start_tcp_echo_server_on(target_port).await;
    let udp_task = start_udp_echo_server_on(target_port).await;
    runtime.create(&allocation("both-rates", Protocol::Both, relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();

    let mut calculator = crate::prometheus::RateCalculator::default();
    calculator.calculate(&runtime.snapshot_listener_metrics().await.unwrap(), 1_000);
    tcp_round_trip(relay_port, b"both-tcp-rate").await;
    udp_round_trip(relay_port, b"both-udp-rate").await;
    let rates = calculator.calculate(&runtime.snapshot_listener_metrics().await.unwrap(), 2_000);
    let tcp = metric_rate(&rates, relay_port, Protocol::Tcp);
    let udp = metric_rate(&rates, relay_port, Protocol::Udp);
    assert!(tcp.rx_bytes_per_second > 0.0);
    assert!(tcp.tx_bytes_per_second > 0.0);
    assert!(udp.rx_bytes_per_second > 0.0);
    assert!(udp.tx_bytes_per_second > 0.0);

    runtime.delete("both-rates", 500).await.unwrap();
    tcp_task.abort();
    udp_task.abort();
}
```

- [ ] **Step 3: Run targeted real-runtime tests**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked runtime::real::tests::real_runtime_ -- --nocapture
```

Expected: PASS.

## Task 4: Update docs/status and run final verification

**Files:**
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`

- [ ] **Step 1: Add M6 ledger status**

Append:

```markdown
## M6 implementation status

- Status: implemented Rust Prometheus rate parity with stateful per-listener TCP/UDP byte-rate calculation, authenticated `/metrics` rendering, stale listener cleanup, and dual-protocol concrete series support.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Scope note: Rust binary production cutover, Docker, and CI migration remain assigned to M7.
```

- [ ] **Step 2: Run full verification**

Run:

```bash
cargo fmt -- --check
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings
zig build test
```

Expected: all commands PASS with no clippy warnings.

- [ ] **Step 3: Request final independent spec-compliance review**

Dispatch an independent reviewer with the M6 spec, plan, changed files, and verification evidence. Fix any `BLOCKED` items and repeat verification/review until the reviewer returns `APPROVED`.

- [ ] **Step 4: Commit and push**

After approval, commit with Lore protocol:

```bash
git add src/prometheus.rs src/http/control_plane.rs src/runtime/real.rs \
  docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md \
  docs/superpowers/specs/2026-05-15-rust-migration-m6-prometheus-metrics-parity.md \
  docs/superpowers/plans/2026-05-15-rust-migration-m6-prometheus-metrics-parity.md

git commit -m "Calculate Prometheus listener rates in Rust" \
  -m "Add stateful per-listener TCP/UDP byte-rate calculation to the Rust Prometheus path so authenticated scrapes report current listener counts and rx/tx bytes-per-second for single and dual-protocol runtimes without changing JSON metrics or API definitions." \
  -m "Constraint: Prometheus metric names, labels, content type, and /v1/metrics JSON fields must remain compatible with Zig." \
  -m "Rejected: Adding new Prometheus counters or labels | M6 is parity for existing listener gauges only." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Directive: Leave Rust production cutover, Docker, and CI migration for M7." \
  -m "Tested: cargo fmt -- --check; TMPDIR=\$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked; TMPDIR=\$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings; zig build test" \
  -m "Not-tested: optional TCP session-model/splice and UDP worker/io_uring/GRO/fast-path lanes" \
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"

git push
```
