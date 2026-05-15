# Rust Migration Milestones Design

## Source requirement

Migrate `relayd` from Zig to Rust while matching all main functionality. API interfaces and definitions must remain exactly aligned with the existing Zig HTTP API. Functionality hidden behind feature flags or optional fast-path environment gates may be deferred. Every functional slice must be represented by a milestone. Each milestone must have a Superpowers-style spec and implementation plan; the plan must be reviewed by an independent agent until it is approved before implementation starts. Implementation must use Subagent Driven Development. After implementation, an independent agent must review spec compliance until approved, documentation must be updated, and the work must be committed and pushed.

## Current product summary

`relayd` is a Linux-first relay daemon with an authenticated HTTP control plane, SQLite persistence, startup restore, TCP and UDP forwarding, dual-protocol allocation support, and Prometheus listener metrics. The Zig entrypoint (`src/main.zig`, `src/app.zig`) parses environment configuration, opens SQLite, starts the runtime manager, restores persisted allocations, and serves HTTP until process termination.

## Main functionality to port

Main/default functionality is functionality active without enabling optional fast-path feature flags:

1. Configuration parsing from environment:
   - `HTTP_LISTEN` with `:PORT` shorthand defaulting to `127.0.0.1`.
   - `PORT_RANGE` inclusive range parsing.
   - required non-empty `AUTH_TOKEN`.
   - `SQLITE_PATH`, `RUNTIME_APPLY_TIMEOUT_MS`, and `RESTORE_SWEEP_TIMEOUT_MS` defaults.
   - UDP socket buffer defaults because baseline UDP listeners use them.
2. Domain model:
   - protocols `tcp`, `udp`, `both` with case-insensitive parsing.
   - allocation, binding, allocation resource, binding view, aggregate allocation view.
   - runtime status/error values documented in the API.
3. SQLite persistence:
   - `allocations` and `bindings` schema, including legacy binding columns.
   - WAL mode, busy timeout, startup self-check.
   - legacy migration from allocation `target_port`/`host` columns into `bindings`.
   - ordered allocation listing by protocol then port.
4. Allocation service:
   - atomic allocation creation over an inclusive port range.
   - cross-protocol conflict rules for `both`.
   - binding put/delete/update compatibility behavior.
   - delete and restore orchestration.
5. HTTP API:
   - bearer auth for all endpoints.
   - primary allocation and binding endpoints under `/v1`.
   - compatibility `/v1/ports` endpoints.
   - JSON response shapes and plain-text error mapping documented in `docs/API.md`.
   - existing `/v1/metrics` JSON counters.
6. Runtime forwarding:
   - baseline TCP copy forwarding after target binding is configured.
   - baseline UDP listener/session forwarding after target binding is configured.
   - `both` creates one aggregate allocation with TCP and UDP listeners on the same port and one shared binding target.
   - deleting an allocation releases listeners; startup restore recreates listeners.
   - no-host allocations reject traffic consistently with existing runtime state.
7. Prometheus `/metrics`:
   - same bearer auth.
   - `text/plain; version=0.0.4; charset=utf-8` response.
   - `relayd_connections_current`, `relayd_rx_bytes_per_second`, and `relayd_tx_bytes_per_second` labeled by concrete `port` and `protocol`.
   - dual-protocol allocations emit separate `tcp` and `udp` metric series.
8. Packaging and operator docs:
   - README build/run instructions.
   - Dockerfile and CI workflows using Rust tooling.
   - API documentation aligned with the Rust implementation.

## Deferred feature-flag/optional functionality

The following Zig paths are explicitly deferred until after main parity because they require opt-in environment flags or are benchmark lanes, not default behavior:

- `TCP_SESSION_MODEL_ENABLED`, `TCP_SESSION_MODEL_WORKERS`, `TCP_SESSION_MODEL_ACCEPT_BALANCED`, and `TCP_SESSION_MODEL_SHARDED_ACCEPT` runtime modes.
- `TCP_SPLICE_ENABLED` and `FORCE_TCP_COPY_FALLBACK` splice/copy comparison counters.
- `UDP_SESSION_WORKERS` workerized/probe mode beyond baseline UDP session handling.
- `UDP_IO_URING_ENABLED`, `UDP_GRO_ENABLED`, `UDP_DATAPLANE_REDESIGN_ENABLED`, and `UDP_FAST_PATH_ENABLED` paths.
- UDP fast-path segment/GSO tuning except keeping configuration fields harmless for docs compatibility if needed.
- `scripts/ci/e2e_iperf3.sh` benchmark matrix modes that exercise deferred lanes.

## Milestone ledger

### M0 — Rust core foundation, model, config, and SQLite repository

Create a Rust crate beside the existing Zig code without removing Zig. Port configuration parsing, domain model, UUIDv7 generation via a crate, SQLite schema/migration/repository behavior, and unit tests that lock current semantics. No HTTP server or live runtime is required in M0.

### M1 — Allocation service with in-memory runtime facade

Port allocation service behavior on top of the Rust repository. Use a test runtime facade to validate port selection, `both` conflict rules, binding put/delete/update, delete rollback behavior where applicable, and restore orchestration without opening network listeners.

### M2 — Authenticated HTTP control plane

Add the Rust HTTP server and route handlers for allocation, binding, compatibility ports, and JSON metrics endpoints. Keep response shapes, content types, status codes, auth behavior, and error mapping aligned with docs and Zig integration tests. Runtime forwarding can still use the M1 runtime facade.

### M3 — Baseline TCP runtime forwarding

Implement default TCP listener lifecycle and copy forwarding. Cover allocation without binding, binding activation, traffic forwarding, delete release, process shutdown with active sessions, and startup restore for TCP allocations.

### M4 — Baseline UDP runtime forwarding

Implement default UDP listener/session lifecycle. Cover binding activation, traffic forwarding, session cleanup, delete release, process shutdown, and startup restore for UDP allocations. Workerized, io_uring, GRO, and fast-path lanes remain deferred.

### M5 — Dual-protocol runtime parity

Integrate `both` allocations into the real Rust runtime so one allocation creates TCP and UDP listeners on the same port, shares one binding target, reports one API row, releases both listeners, restores both listeners, and emits concrete metrics rows for each protocol.

### M6 — Prometheus metrics parity

Port the authenticated `/metrics` endpoint, listener metric snapshots, active connection/session counts, byte totals, and rate calculation. Keep `/v1/metrics` JSON compatibility. Cover TCP, UDP, and dual-protocol label behavior.

### M7 — Rust binary cutover, docs, Docker, and CI

Make the Rust binary the default build/run artifact, update README/API/operator docs, migrate CI and Docker to Rust tooling, keep the Zig implementation only as historical/reference code or remove it if no longer needed, and run full parity verification.

## Cross-milestone process gates

For each milestone:

1. Save a spec under `docs/superpowers/specs/YYYY-MM-DD-<milestone>.md`.
2. Save a Superpowers-style implementation plan under `docs/superpowers/plans/YYYY-MM-DD-<milestone>.md`.
3. Dispatch an independent plan reviewer. Implementation may begin only after the reviewer returns `APPROVED` or equivalent approval.
4. Execute implementation with Subagent Driven Development.
5. Dispatch an independent spec-compliance reviewer after implementation. Fix and re-review until approved.
6. Update relevant docs.
7. Run fresh verification commands that directly cover the milestone.
8. Commit with the Lore Commit Protocol and push.

## M0 success criteria

M0 is complete when:

- `Cargo.toml` and Rust source/test files exist and build with `cargo test`.
- Rust config parsing matches current Zig tests for HTTP listen, port ranges, and IP literal host validation.
- Rust model parsing supports `tcp`, `udp`, and `both` case-insensitively.
- Rust repository creates the same SQLite tables, migrates legacy binding data, persists/list allocations and bindings, and orders allocations by protocol then port.
- Existing Zig code remains untouched except documentation/workflow artifacts unless required by repository metadata.
- Documentation records the migration milestones and M0 status.

## M0 implementation status

- Status: implemented in Rust foundation crate.
- Verification: `cargo test`.
- Scope note: Zig implementation remains available as parity reference for later milestones.

## M1 implementation status

- Status: implemented in Rust service layer with in-memory runtime facade.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `zig build test`.
- API note: HTTP wire-level compatibility remains M2, but M1 preserves allocation resource, binding view, aggregate allocation view, and compatibility update service definitions.

## M2 implementation status

- Status: implemented in Rust HTTP control-plane router using the M1 in-memory runtime facade.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `zig build test`.
- API note: Allocation, binding, compatibility port, JSON metrics, auth, content type, and error-mapping interfaces are preserved. Real forwarding and full Prometheus rate parity remain M3-M6.

## M3 implementation status

- Status: implemented in Rust TCP runtime with loopback listener lifecycle, default copy forwarding, no-host rejection, delete cleanup, restore, and TCP listener metrics.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Scope note: UDP runtime, dual-protocol real runtime parity, TCP session-model/splice optional lanes, and full Prometheus rate semantics remain assigned to M4-M6.

## M4 implementation status

- Status: implemented in Rust UDP runtime with loopback listener lifecycle, baseline per-client session forwarding, no-host rejection, TTL cleanup, update/delete stale-session cleanup, restore, and UDP listener metrics.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Scope note: Dual-protocol real runtime parity, UDP workerized/io_uring/GRO/fast-path optional lanes, and full Prometheus rate semantics remain assigned to M5-M6.
## M5 implementation status

- Status: implemented in Rust composed real runtime with dual-protocol create/update/delete/restore, shared binding target, TCP+UDP forwarding on the same port, aggregate snapshots, explicit shutdown cleanup, and concrete TCP/UDP listener metrics rows.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Scope note: Full Prometheus rate semantics and Rust binary production cutover remain assigned to M6-M7.
## M6 implementation status

- Status: implemented Rust Prometheus rate parity with stateful per-listener TCP/UDP byte-rate calculation, authenticated `/metrics` rendering, stale listener cleanup, and dual-protocol concrete series support.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Scope note: Rust binary production cutover, Docker, and CI migration remain assigned to M7.
## M7 implementation status

- Status: implemented Rust binary cutover with real startup, Rust-primary Docker/CI/docs, and Zig retained as parity/reference tests.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `cargo build --locked --bin relayd`; `zig build test`.
- Scope note: Optional TCP/UDP feature-flag lanes remain deferred after main/default parity.

