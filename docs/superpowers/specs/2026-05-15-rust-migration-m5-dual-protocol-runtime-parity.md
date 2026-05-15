# Rust Migration M5 Dual-Protocol Runtime Parity Spec

## Goal

Port relayd's default dual-protocol (`both`) runtime behavior to Rust so one allocation with `protocol = both` creates one API allocation row and two concrete loopback listeners on the same port: TCP and UDP. The allocation must share one binding target, restore both listeners on startup, release both listeners on delete, reject both protocols while unbound, forward TCP and UDP traffic after binding, and emit separate concrete listener metric rows labeled `tcp` and `udp`. Existing Zig HTTP API interfaces and Rust model/resource definitions must remain unchanged.

## Source requirements

- Continue the Zig-to-Rust migration with main/default functionality parity; feature-flagged runtime paths may remain deferred.
- API interfaces and definitions must remain exactly aligned with the existing Zig HTTP API.
- This milestone must have a Superpowers-style spec and plan, plan review must return `APPROVED` before implementation, implementation must use Subagent Driven Development, final spec-compliance review must approve, docs must be updated, then commit and push.

## Source of truth

- Zig runtime manager and protocol allocation behavior: `src/runtime/manager.zig`.
- Zig forwarding/service tests: `tests/integration/service_forward_test.zig`.
- Rust allocation service conflict rules: `src/service/allocation_service.rs`.
- Rust runtime facade and in-memory test facade: `src/runtime/facade.rs`.
- Rust concrete runtime modules: `src/runtime/tcp.rs`, `src/runtime/udp.rs`.
- Rust HTTP/control-plane and metrics renderers: `src/http/control_plane.rs`, `src/prometheus.rs`, `src/metrics.rs`.
- Migration ledger: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`.

## In scope

- Add a real Rust runtime facade for default production parity that composes the existing TCP and UDP runtimes.
- For `Protocol::Tcp`, delegate to the TCP runtime with unchanged behavior.
- For `Protocol::Udp`, delegate to the UDP runtime with unchanged behavior.
- For `Protocol::Both`:
  - `create` binds both a TCP listener and a UDP listener on `127.0.0.1:<allocation.port>` using the same allocation id.
  - TCP and UDP may share the same numeric port because they are separate transport protocols.
  - If one side fails during create, the operation returns `RuntimeCreateFailed` and any listener already created for that allocation is cleaned up before returning so the service can try later ports.
  - `update` applies the same binding target (`host`, `target_port`) to both concrete listeners.
  - `delete` removes both concrete listeners and closes active sessions/connections.
  - `restore` recreates both concrete listeners; if either side fails, return `RuntimeRestoreFailed` and clean up any side that was restored.
  - `snapshot` returns one aggregate `ObservedState` for the API row. When both concrete snapshots agree, return that state. If one side is missing or degraded, return a degraded state with an appropriate error kind/last error so the existing service view can expose runtime problems without changing response shape.
  - `snapshot_listener_metrics` returns concrete listener rows for each bound protocol; a `both` allocation must produce one TCP row and one UDP row for the same port.
- Add tests with real loopback sockets proving dual TCP+UDP forwarding through one `both` allocation after one shared binding target is configured.
- Add tests proving no-host rejection, update/delete binding back to no-host, delete release, startup restore, bind-failure cleanup/port-skip behavior, restore-failure cleanup, aggregate degraded snapshot behavior, explicit shutdown cleanup, and concrete metrics rows for `both`.
- Keep all M0-M4 tests passing.

## Out of scope

- New HTTP API fields, protocol enum values, or resource shape changes.
- Full Prometheus byte-rate delta calculation; M6 owns rate semantics. M5 only ensures dual-protocol allocations provide concrete TCP/UDP rows to the existing renderer.
- Rust binary production cutover, Docker, and CI replacement; M7 owns that work.
- Optional TCP session-model/splice lanes and optional UDP worker/io_uring/GRO/dataplane/fast-path lanes.
- Changing Zig source behavior except using it as a parity reference.

## Required runtime behavior

### Single-protocol delegation

`Tcp` and `Udp` allocations must retain M3/M4 behavior exactly. The composed real runtime must not regress existing concrete runtime tests or service semantics.

### Dual-protocol create

For a `Protocol::Both` allocation, the composed runtime creates both concrete listeners on the same numeric port. Tests must use a helper that reserves a TCP+UDP-free relay port pair to avoid flakes from checking only one transport. If TCP creation fails, no UDP listener is attempted. If UDP creation fails after TCP succeeds, TCP is deleted before returning `RuntimeCreateFailed`. The allocation service continues scanning later ports exactly as it does for single-protocol create failures.

### Dual-protocol no-host behavior

Before a binding exists, both concrete listeners report `rejecting_no_host`. TCP clients are accepted and closed consistently with M3. UDP datagrams are dropped without creating sessions consistently with M4. The aggregate API snapshot reports `rejecting_no_host` with no effective host or target.

### Dual-protocol update and forwarding

A single binding update sets the same host/target_port on both listeners. The target can be a TCP and UDP server bound to the same numeric target port. After update:

- TCP traffic to the relay port is copied to the TCP upstream server and replies return to the TCP client.
- UDP datagrams to the relay port are forwarded through per-client UDP sessions and replies return to the UDP client.
- TCP active connection metrics and UDP active session metrics update independently.

If update transitions back to no-host, both sides close stale sessions/connections and reject new traffic consistently with M3/M4. Deleting a binding through the service must drive this same update-back-to-no-host behavior for both concrete listeners.

### Dual-protocol delete and shutdown

Deleting a `both` allocation closes both listeners and active sessions/tasks. The numeric port can then be rebound for both TCP and UDP. Explicitly shutting down the composed runtime must not leak concrete listeners; the composed runtime exposes `shutdown_all()` to release both TCP and UDP listeners deterministically. Best-effort `Drop` safeguards may be added where ownership permits, but deterministic verification targets explicit shutdown.

### Dual-protocol restore

Startup restore recreates both concrete listeners for persisted `both` allocations. A persisted binding immediately enables both TCP and UDP forwarding to the shared target. Restore bind failure on either side returns `RuntimeRestoreFailed`, increments existing bind/restore metrics through concrete runtimes, and leaves no half-restored listener behind. If UDP restore fails after TCP restore succeeds, TCP must be deleted before returning.

### Metrics

`both` allocations emit two `ListenerMetricsSnapshot` rows with the same port: one row labeled `tcp`, one row labeled `udp`. `/v1/metrics` JSON counters remain compatible; `/metrics` continues to render existing text fields from listener rows. Full byte-per-second rate math remains M6.

## Acceptance criteria

- M5 plan is independently reviewed to `APPROVED` before implementation.
- A composed real runtime module exists and is exported without changing `RuntimeFacade` method signatures or API model definitions.
- Single-protocol TCP and UDP paths still pass all M3/M4 tests.
- Dual-protocol create test proves a `both` allocation creates both concrete listeners on one port.
- No-host test proves TCP and UDP traffic are rejected before binding and the aggregate snapshot is `rejecting_no_host`.
- Forwarding test proves one shared binding target enables both TCP and UDP relay traffic through the same relay port.
- Delete/no-host-update tests prove deleting a `both` allocation releases both TCP and UDP listeners, deleting/updating the binding closes both targets and rejects new traffic, and concrete metric rows are cleared where applicable.
- Restore test proves persisted `both` allocation plus binding restores both listeners and forwards both protocols.
- Bind failure tests prove create skips a busy port when either TCP or UDP side cannot bind, and no half-created listener remains.
- Restore failure tests prove TCP-side and UDP-side restore failures return `RuntimeRestoreFailed` and leave no half-restored listener behind.
- Aggregate snapshot tests prove missing TCP, missing UDP, and divergent concrete states surface degraded status/error information without changing API shape.
- Shutdown tests prove explicit `shutdown_all()` releases both concrete listener types.
- Metrics test proves `snapshot_listener_metrics` and Prometheus rendering include separate `tcp` and `udp` rows for the same `both` port.
- M5 docs/status are updated after implementation.
- Fresh verification passes: `cargo fmt -- --check`; `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked`; `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Final independent spec-compliance review returns `APPROVED` before commit/push.
