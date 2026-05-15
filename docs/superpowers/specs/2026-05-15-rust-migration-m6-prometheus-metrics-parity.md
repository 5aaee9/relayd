# Rust Migration M6 Prometheus Metrics Parity Spec

## Goal

Port relayd's Prometheus listener metrics parity to Rust by adding stateful per-listener byte-rate calculation for `/metrics` while preserving bearer auth, content type, metric names, labels, and `/v1/metrics` JSON compatibility. TCP, UDP, and dual-protocol (`both`) runtime listener snapshots must render concrete `tcp` and `udp` series with current connection/session counts and rx/tx bytes-per-second rates derived from cumulative listener byte totals.

## Source requirements

- Continue the Zig-to-Rust migration with main/default functionality parity; feature-flagged runtime paths may remain deferred.
- API interfaces and definitions must remain exactly aligned with the existing Zig HTTP API.
- This milestone must have a Superpowers-style spec and plan, plan review must return `APPROVED` before implementation, implementation must use Subagent Driven Development, final spec-compliance review must approve, docs must be updated, then commit and push.

## Source of truth

- Zig Prometheus rate calculator/exporter: `src/prometheus_exporter.zig`.
- Zig HTTP Prometheus integration tests: `tests/integration/http_api_test.zig`.
- Rust listener snapshot shape: `src/runtime/facade.rs`.
- Rust Prometheus renderer: `src/prometheus.rs`.
- Rust HTTP metrics route: `src/http/control_plane.rs`.
- Rust concrete runtimes: `src/runtime/tcp.rs`, `src/runtime/udp.rs`, `src/runtime/real.rs`.
- Migration ledger: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`.

## In scope

- Add a Rust rate calculator equivalent to Zig `RateCalculator`:
  - Keyed by `(port, protocol)`.
  - First sample emits zero rx/tx rates.
  - Later samples compute `(current_total - previous_total) / elapsed_seconds`.
  - Non-positive elapsed time emits zero rates.
  - Counter reset/decrease emits zero rate for that direction.
  - Samples for listeners no longer present are removed.
- Change Prometheus rendering to render calculated rates instead of hard-coded zero rates.
- Preserve metric names and labels exactly:
  - `relayd_connections_current{port="<port>",protocol="tcp|udp"}`
  - `relayd_rx_bytes_per_second{port="<port>",protocol="tcp|udp"}`
  - `relayd_tx_bytes_per_second{port="<port>",protocol="tcp|udp"}`
- Keep content type exactly `text/plain; version=0.0.4; charset=utf-8`.
- Keep bearer auth behavior for `/metrics` exactly as M2.
- Keep `/v1/metrics` JSON counters and field names unchanged.
- Wire the HTTP `/metrics` route to a shared, stateful rate calculator so repeated scrapes produce deltas.
- Add tests for TCP, UDP, and dual-protocol concrete rows with positive rates after traffic and repeated scrapes.
- Add unit tests for first sample, positive deltas, zero elapsed, reset/decrease clamp, stale sample removal, same-port TCP/UDP key independence under reordered snapshots, and direct rate-render label/type-line output.

## Out of scope

- Adding new Prometheus metric names or labels.
- Changing `/v1/metrics` JSON fields or route behavior.
- Optional TCP session-model/splice and UDP worker/io_uring/GRO/dataplane/fast-path metrics beyond existing JSON counters staying compatible.
- Rust binary production cutover, Docker, and CI replacement; M7 owns that work.
- Changing runtime forwarding behavior except as needed to expose existing snapshots.

## Required behavior

### Rate calculation

For each scrape, the HTTP handler obtains `Vec<ListenerMetricsSnapshot>` from the runtime. The rate calculator compares each snapshot to its prior sample for the same `(port, protocol)` key:

- If no prior sample exists, rx and tx rates are `0`.
- If `timestamp_ms <= previous.timestamp_ms`, rx and tx rates are `0`.
- If current byte total is less than or equal to the prior byte total, that direction's rate is `0`.
- Otherwise rate is `(current - previous) / elapsed_seconds` as a floating-point value.
- Current connection/session count is copied from the current snapshot.
- After calculating rates, samples for keys absent from the current snapshot set are removed.

### Prometheus output

`/metrics` returns a text body with the existing `# TYPE` lines and samples for every current listener rate row. The exact order may follow input snapshot order, but samples must include correct `port` and concrete `protocol` labels. Floating point values may be rendered with Rust's default formatting as long as they parse as numeric Prometheus samples.

### HTTP statefulness

The Rust HTTP app must hold one shared rate calculator across `/metrics` requests. Repeated scrapes with increasing byte totals and elapsed wall-clock time must produce positive rates. First scrape after startup or after a listener disappears/reappears emits zero for that listener until a new prior sample exists.

### Dual-protocol metrics

A `both` allocation uses `RealRuntime` snapshots to produce separate concrete TCP and UDP rows on the same port. `/metrics` must render current connection/session counts and independent rx/tx rates for both labels.

## Acceptance criteria

- M6 plan is independently reviewed to `APPROVED` before implementation.
- `src/prometheus.rs` or an equivalent Rust module contains a stateful rate calculator with unit tests matching Zig calculator semantics.
- `/metrics` route uses a shared calculator and renders non-zero rx/tx rates after repeated scrapes with traffic.
- `/metrics` auth and content type remain unchanged.
- `/v1/metrics` JSON output remains unchanged.
- TCP real-runtime test proves positive rx and tx rates after TCP traffic and repeated scrapes/calculations.
- UDP real-runtime test proves positive rx and tx rates and current session gauge after UDP traffic and repeated scrapes/calculations.
- Dual-protocol real-runtime test proves a `both` allocation emits separate `tcp` and `udp` rows for the same port with independent positive rates after both protocols carry traffic.
- Stale listener removal test proves deleted listener keys do not keep emitting old rates.
- Same-port key-independence test proves TCP and UDP rows for the same port keep separate samples even when scrape row order changes.
- M6 docs/status are updated after implementation.
- Fresh verification passes: `cargo fmt -- --check`; `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked`; `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Final independent spec-compliance review returns `APPROVED` before commit/push.
