# Rust Migration M4 Baseline UDP Runtime Spec

## Goal

Port relayd's default UDP runtime forwarding path to Rust so UDP allocations open real loopback listeners, reject traffic while unbound, forward datagrams through per-client upstream sessions after binding, clean idle sessions, drop stale delayed replies after updates/deletes, release listeners on delete, restore persisted UDP allocations on startup, and expose UDP metrics/listener snapshots. API interfaces and model definitions must remain exactly aligned with the Zig implementation. Workerized UDP, io_uring, GRO, fast-path/GSO, and dual-protocol runtime parity remain deferred.

## Source requirements

- Continue the Zig-to-Rust migration with main functionality parity; feature-flagged runtime paths may remain deferred.
- API interfaces and definitions must remain exactly aligned with the existing Zig HTTP API.
- This milestone must have a Superpowers-style spec and plan, plan review must return `APPROVED` before implementation, implementation must use Subagent Driven Development, final spec-compliance review must approve, docs must be updated, then commit and push.

## Source of truth

- Zig runtime manager UDP implementation: `src/runtime/manager.zig`.
- Zig service/runtime integration tests: `tests/integration/service_forward_test.zig`.
- Rust runtime facade: `src/runtime/facade.rs`.
- Rust allocation service: `src/service/allocation_service.rs`.
- Rust metrics/prometheus modules: `src/metrics.rs`, `src/prometheus.rs`.
- M3 TCP runtime pattern: `src/runtime/tcp.rs`.

## In scope

- A real Rust UDP runtime implementation behind the existing `RuntimeFacade` trait, exported as `runtime::udp` while leaving `runtime::tcp` and `InMemoryRuntime` unchanged.
- UDP listener lifecycle for `Protocol::Udp` allocations:
  - `create` binds a UDP socket on `127.0.0.1:<allocation.port>` and records a runtime entry.
  - `restore` recreates persisted UDP listeners.
  - `update` changes the effective target only when both `host` and `target_port` are present.
  - `delete` removes the runtime entry, stops read/cleanup tasks, closes sessions, and releases the UDP port.
- Baseline per-client UDP sessions:
  - Client identity is the concrete `SocketAddr` that sent to the relay listener.
  - First datagram from a client creates an upstream UDP socket bound to loopback ephemeral port and connected to the effective target.
  - Repeated datagrams from the same client reuse the same upstream socket so the upstream server sees a stable remote port for that client.
  - Datagrams from different clients create independent upstream sessions.
  - Upstream replies are sent back to the original client address through the relay listener.
- Session lifecycle:
  - Sessions track `last_seen` and expire after a configurable TTL; default TTL matches Zig baseline `60_000` ms.
  - Runtime config allows tests to set a short TTL.
  - Updating or deleting a binding closes existing sessions so delayed replies from old targets are dropped and new traffic uses the new target. Each session/reply task must carry a generation/token or equivalent tombstone and verify it is still the current session before sending a reply through the listener; stale generation/session mismatches increment stale/drop metrics and never reach clients.
  - Dropping or deleting the runtime with active UDP sessions must not hang.
- No-host behavior:
  - UDP allocations with no `host` or no `target_port` report `runtime_status = rejecting_no_host`.
  - Incoming datagrams while no effective target exists are dropped without creating an upstream session.
  - `rejected_no_host_total` increments for rejected datagrams and for updates that transition to no-host state, consistent with M3 TCP behavior.
- Error/degraded behavior:
  - Bind failure on create returns `RuntimeCreateFailed`, increments `bind_fail_total`, and lets the allocation service try later ports.
  - Bind failure on restore returns `RuntimeRestoreFailed`, does not retain an entry, and increments both `bind_fail_total` and `restore_failures_total`.
  - Updating a missing runtime entry returns `RuntimeUpdateFailed`.
- Metrics:
  - Increment existing Rust `Metrics` counters/gauges for default UDP path: `runtime_apply_total`, `bind_fail_total`, `restore_failures_total`, `rejected_no_host_total`, `udp_packets_in_total`, `udp_packets_out_total`, `udp_bytes_in_total`, `udp_bytes_out_total`, `udp_recv_errors_total`, `udp_send_errors_total`, `udp_session_create_total`, `udp_session_expire_total`, `udp_drop_total`, `udp_reply_primary_total`, `udp_reply_drop_total`, `udp_reply_stale_total`, and `udp_active_sessions`.
  - Maintain per-listener `ListenerMetricsSnapshot` rows for UDP with current active session count and cumulative rx/tx byte totals.
  - Existing `/v1/metrics` and `/metrics` routes must reflect M4 runtime counters/snapshots when used with the real UDP runtime.
- Rust tests with real loopback UDP sockets covering forwarding, no-host drop, client session reuse, multi-client session split, TTL expiry, update-to-new-target, delayed stale reply drop after update/delete, delete release, restore, and UDP metrics.

## Out of scope

- `Protocol::Both` real runtime parity; M5 owns dual-protocol listeners on the same port and shared binding target.
- UDP workerized paths: `UDP_SESSION_WORKERS` and worker distribution counters.
- UDP io_uring, GRO, dataplane redesign, fast-path/GSO, and associated optional counters beyond staying zero unless already touched by existing metrics snapshots.
- TCP runtime behavior except ensuring M3 tests keep passing.
- Full Prometheus rate calculation from byte deltas; M6 owns rate semantics. M4 feeds cumulative UDP snapshot data to the existing M2 renderer.
- Rust binary cutover, Docker, and CI replacement; M7 owns production cutover.

## Required runtime behavior

### Create

For a `Protocol::Udp` allocation, the Rust runtime binds a UDP socket on loopback and the allocation's port. Binding failure returns `RuntimeCreateFailed` and increments `bind_fail_total`; service port-selection behavior must continue trying later ports. Successful create records an entry, starts a listener receive loop and cleanup loop, sets observed state from the allocation, and increments `runtime_apply_total`.

### Update

For an existing UDP runtime entry:

- If `allocation.host` and `allocation.target_port` are both present, set `effective_host`, `effective_target_port`, clear `error_kind`/`last_error`, set `runtime_status = active`, close existing sessions so new datagrams use the new target, and increment `runtime_apply_total`.
- If either value is missing, clear effective target state, set `runtime_status = rejecting_no_host`, close existing sessions, increment `rejected_no_host_total`, and increment `runtime_apply_total`.
- If the runtime entry is absent, return `RuntimeUpdateFailed`.

### Forwarding

When a datagram arrives on an active UDP listener:

1. Increment listener and global inbound packet/byte counters.
2. Read the current target from listener state.
3. If no target exists, increment `rejected_no_host_total`, drop the datagram, and do not create an upstream socket.
4. If a target exists, find or create a session for the client address.
5. Forward the datagram to the session's connected upstream socket.
6. For upstream replies, forward each reply back to the original client address through the relay listener and increment outbound packet/byte counters plus `udp_reply_primary_total`.
7. If a send/receive error occurs, increment the appropriate error/drop counter and close the session.

### Session cleanup, delete, and shutdown

Sessions expire when `now - last_seen > udp_session_ttl_ms`; expiration decrements active session gauges and increments `udp_session_expire_total`. Session removal must be idempotent across TTL, update, delete, reply error, and task-abort paths so `udp_active_sessions` and listener `connections_current` never underflow or leak. Delete/update closes active sessions and drops delayed replies from previous targets; stale generation/session mismatches increment `udp_reply_stale_total` or `udp_reply_drop_total`. Runtime shutdown or drop must abort tasks and sessions without hanging.

### Restore

`restore_all` with the real UDP runtime recreates persisted UDP listeners. A restored allocation with a persisted binding should immediately forward datagrams to the persisted target. A restored allocation without a complete binding should report `rejecting_no_host` and drop incoming datagrams.

## Acceptance criteria

- M4 plan is independently reviewed to `APPROVED` before implementation.
- Rust has a real UDP runtime module exported by `src/runtime/mod.rs` while keeping `TcpRuntime` and `InMemoryRuntime` available for existing tests.
- Baseline UDP forwarding test proves datagrams round-trip through a Rust relay after binding to a loopback UDP echo server.
- No-host test proves an unbound UDP allocation drops traffic, creates no sessions, increments no-host metrics, and reports `rejecting_no_host`.
- Session reuse test proves repeated packets from one client reuse one upstream remote port and a second client creates a separate session.
- TTL cleanup test proves idle sessions expire, decrement active gauges, and new traffic creates a new session.
- Binding update/delete tests prove existing sessions are closed, delayed stale replies are deterministically released and dropped using barriers/channels, stale/drop metrics increment, active gauges settle to zero without underflow/leak, and new datagrams use the current target.
- Delete test proves the UDP listener port is released and can be rebound or reallocated.
- Restore test proves persisted UDP allocation plus binding recreates a listener and forwards without recreating the database row.
- Bind failure tests prove create skips busy ports and restore reports `RuntimeRestoreFailed` with metrics.
- Metrics tests prove UDP counters/gauge, stale/drop reply counters, idempotent active-session cleanup, and `ListenerMetricsSnapshot` rx/tx byte totals change after forwarded traffic and feed the existing Prometheus renderer.
- Existing M1/M2/M3 tests continue to pass.
- `cargo fmt -- --check` passes.
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked` passes.
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings` passes.
- `zig build test` passes to prove the Zig reference implementation remains intact.
- M4 docs/status are updated after implementation, and the final independent spec-compliance review returns `APPROVED` before commit/push.
