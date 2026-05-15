# Rust Migration M3 Baseline TCP Runtime Spec

## Goal

Port relayd's default TCP runtime forwarding path to Rust so TCP allocations open real listeners, reject traffic while unbound, forward bytes after a binding is configured, expose TCP listener/session metrics, release listeners on delete, and restore persisted TCP allocations on startup. API wire definitions and model fields must remain exactly aligned with the Zig implementation; optional TCP session-model and splice fast paths remain deferred.

## Source requirements

- Continue the Zig-to-Rust migration with main functionality parity; feature-flagged runtime paths may remain deferred.
- API interfaces and definitions must remain exactly aligned with the existing Zig HTTP API.
- This milestone must have a Superpowers-style spec and plan, plan review must return `APPROVED` before implementation, implementation must use Subagent Driven Development, final spec-compliance review must approve, docs must be updated, then commit and push.

## Source of truth

- Zig runtime manager: `src/runtime/manager.zig`.
- Zig service/runtime integration tests: `tests/integration/service_forward_test.zig`.
- Rust runtime facade: `src/runtime/facade.rs`.
- Rust allocation service: `src/service/allocation_service.rs`.
- Rust metrics/prometheus modules: `src/metrics.rs`, `src/prometheus.rs`.
- Rust HTTP/service tests from M1/M2 for expected control-plane shapes and runtime snapshot integration.

## In scope

- A real Rust TCP runtime implementation behind the existing `RuntimeFacade` trait.
- TCP listener lifecycle for `Protocol::Tcp` allocations:
  - `create` binds `127.0.0.1:<allocation.port>` and records a runtime entry.
  - `restore` recreates listeners for persisted TCP allocations.
  - `update` changes the effective target only when both `host` and `target_port` are present.
  - `delete` removes the runtime entry, stops accepting new connections, and releases the TCP port.
- Baseline copy forwarding:
  - Each accepted client connection connects to the current configured IP literal and target port.
  - Bytes flow in both directions until EOF/error using Tokio TCP copy primitives.
  - Deferred session-model and splice paths are not enabled by environment flags in Rust M3.
- No-host behavior:
  - TCP allocations with no `host` or no `target_port` report `runtime_status = rejecting_no_host`.
  - Connections accepted while no effective target exists are closed without forwarding.
  - Updating or deleting a binding closes existing active sessions for that allocation so stale target connections do not continue indefinitely.
- Error/degraded behavior:
  - Bind failure on create returns `RuntimeCreateFailed` so the allocation service can try the next port.
  - Bind failure on restore returns `RuntimeRestoreFailed`, does not retain a runtime entry, and increments both `bind_fail_total` and `restore_failures_total`.
  - Updating a missing runtime entry returns `RuntimeUpdateFailed` and snapshots for missing state still degrade through existing service behavior.
- Metrics:
  - Increment existing Rust `Metrics` counters/gauges for default TCP path: `runtime_apply_total`, `bind_fail_total`, `restore_failures_total`, `rejected_no_host_total`, `tcp_copy_fallback_total`, `tcp_session_create_total`, `tcp_session_close_total`, `tcp_listener_accept_total`, `tcp_upstream_connect_total`, `tcp_upstream_connect_fail_total`, and `tcp_active_sessions`.
  - Maintain per-listener `ListenerMetricsSnapshot` rows for TCP with current connection count and cumulative rx/tx byte totals.
  - Existing `/v1/metrics` and `/metrics` routes must reflect M3 runtime counters/snapshots when used with the real runtime.
- Rust tests with real loopback sockets that cover forwarding, no-host close, binding delete/update behavior, delete port release, startup restore, and TCP metrics.

## Out of scope

- UDP runtime forwarding, UDP sessions, UDP metrics, and UDP startup restore; M4 owns baseline UDP.
- Real `Protocol::Both` runtime parity; M5 owns dual-protocol listeners on one port. M3 may reject or no-op non-TCP real-runtime requests only if existing M1/M2 in-memory tests remain unchanged and TCP behavior is unaffected.
- TCP session-model flags: `TCP_SESSION_MODEL_ENABLED`, workerized session model, sharded accept, accept-balanced mode, and session half-close parity tests tied to that mode.
- TCP splice fast path and forced copy-fallback feature flag semantics. M3 is the default copy path; splice counters remain zero except `tcp_copy_fallback_total` for sessions that use baseline copy mode.
- Rust binary cutover, Docker, and CI replacement; M7 owns production cutover.
- Full Prometheus rate calculation from byte deltas; M6 owns rate semantics. M3 only feeds cumulative snapshot data to the existing M2 renderer.

## Required runtime behavior

### Create

For a `Protocol::Tcp` allocation, the Rust runtime binds a TCP listener on loopback and the allocation's port. Binding failure returns `RuntimeCreateFailed` and increments `bind_fail_total`; service port-selection behavior must continue trying later ports. Successful create records an entry with no effective target unless both `host` and `target_port` are configured, starts an accept loop, and increments `runtime_apply_total`. Successful restore also increments `runtime_apply_total`.

### Update

For an existing TCP runtime entry:

- If `allocation.host` and `allocation.target_port` are both present, set `effective_host`, `effective_target_port`, clear `error_kind`/`last_error`, set `runtime_status = active`, close existing sessions so new traffic uses the new target, and increment `runtime_apply_total`.
- If either value is missing, clear effective target state, set `runtime_status = rejecting_no_host`, close existing sessions, increment `rejected_no_host_total`, and increment `runtime_apply_total`.
- If the runtime entry is absent, return `RuntimeUpdateFailed`.

### Forwarding

When a client connects to an active TCP listener:

1. Increment listener accept counters.
2. Read the current target atomically from the listener entry.
3. If no target exists, close the client and do not create an upstream connection.
4. If a target exists, connect to `<effective_host>:<effective_target_port>`.
5. On upstream connect success, increment session create counters and gauges, copy bytes in both directions, add client-to-upstream bytes to listener `tx_bytes_total`, add upstream-to-client bytes to listener `rx_bytes_total`, then decrement gauges and increment session close counters when the session ends.
6. On upstream connect failure, increment `tcp_upstream_connect_fail_total`, close the client, and keep the listener active.

### Delete and shutdown

Deleting a TCP allocation aborts the accept loop, closes active sessions, removes listener metrics from future snapshots, and releases the bound port. Dropping or shutting down the runtime must not hang when clients are connected.

### Restore

`restore_all` with the real TCP runtime recreates persisted TCP listeners. A restored allocation with a persisted binding should immediately forward traffic to the persisted target. A restored allocation without a complete binding should report `rejecting_no_host` and close incoming clients.

## Acceptance criteria

- M3 plan is independently reviewed to `APPROVED` before implementation.
- Rust has a real TCP runtime module exported by `src/runtime/mod.rs` while keeping `InMemoryRuntime` available for existing tests.
- Baseline TCP forwarding test proves bytes round-trip through a Rust relay after binding to a loopback echo server.
- No-host test proves an unbound TCP allocation closes/rejects client traffic and reports `rejecting_no_host`.
- Binding delete/update tests prove existing sessions are closed, aborted sessions clean up active gauges, no-host rejects new clients, and update-to-new-target makes new sessions use the current effective target.
- Delete test proves the listener port is released and can be rebound or reallocated.
- Restore test proves persisted TCP allocation plus binding recreates a listener and forwards without recreating the database row.
- Metrics tests prove TCP session counters/gauge, cancellation-safe active gauge cleanup, `runtime_apply_total`, restore failure counters, and `ListenerMetricsSnapshot` rx/tx byte totals change as required.
- Existing M1/M2 in-memory runtime tests continue to pass.
- `cargo fmt -- --check` passes.
- `cargo test --locked` passes.
- `cargo clippy --locked --lib --tests -- -D warnings` passes.
- `zig build test` passes to prove the Zig reference implementation remains intact.
- M3 docs/status are updated after implementation, and the final independent spec-compliance review returns `APPROVED` before commit/push.
