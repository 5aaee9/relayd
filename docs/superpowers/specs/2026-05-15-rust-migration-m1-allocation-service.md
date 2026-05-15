# Rust Migration M1 Allocation Service Spec

## Goal

Port relayd's allocation-service behavior to Rust on top of the M0 SeaORM/SQLx repository, using an in-memory runtime facade instead of real network listeners. M1 must preserve public API-facing data definitions so M2 can expose HTTP routes with the same request/response semantics as the Zig implementation.

## Source requirements

- Continue the full Zig-to-Rust migration with feature-flagged runtime paths deferred.
- Every milestone must have a Superpowers spec and plan, and the plan must be independently approved before implementation.
- Use Subagent Driven Development for implementation.
- After implementation, independent spec-compliance review must approve before the milestone is considered implemented.
- API interfaces and definitions must remain exactly the same as the existing API; M1 prepares the service data contracts but does not expose HTTP yet.

## In scope

- Rust allocation service module that mirrors `src/service/allocation_service.zig` behavior where no live socket is required.
- Runtime facade trait and in-memory test runtime implementation.
- Service-level methods for:
  - create allocation with optional compatibility `target_port` seed;
  - get/list allocation resources;
  - get/list aggregate allocation views;
  - get/put/delete binding;
  - compatibility set-target/update allocation behavior;
  - delete allocation;
  - restore all persisted allocations;
  - listener metrics snapshot pass-through placeholder for later Prometheus milestone.
- Cross-protocol conflict behavior for `tcp`, `udp`, and `both`.
- Runtime failure behavior:
  - create-time runtime bind failures skip that port and continue scanning;
  - update/delete/restore failures surface as service errors;
  - delete persistence failure path restores runtime state when simulated.
- Host validation through the Rust config parser so only IP literals are accepted.
- Tests that lock service behavior without opening network sockets.

## Out of scope

- HTTP route handling and JSON serialization details beyond using existing Rust model structs.
- Real TCP/UDP listener creation, forwarding, session accounting, and no-host packet behavior.
- Prometheus exposition rendering.
- Multi-process SQLite transaction isolation. M1 still uses SeaORM/SQLx transactions or explicit cleanup to prevent partial allocation+binding persistence inside one service operation; future live milestones may deepen DB transaction behavior if needed by runtime parity.
- Feature-flagged runtime lanes listed as deferred in the milestone ledger.

## Design

### Module layout

Add:

- `src/runtime/mod.rs`
- `src/runtime/facade.rs`
- `src/service/mod.rs`
- `src/service/allocation_service.rs`

Update `src/lib.rs` to export `runtime` and `service`.

### Runtime facade

Define a trait with async methods that match service needs but avoid concrete network listeners:

- `create(allocation, timeout_ms)`
- `update(allocation, timeout_ms)`
- `delete(id, timeout_ms)`
- `restore(allocation, timeout_ms)`
- `snapshot(id)`
- `snapshot_listener_metrics()`

Define `ObservedState` with the same API-facing fields the Zig service reads from runtime: `effective_target_port`, `effective_host`, `runtime_status`, `error_kind`, and `last_error`.

The in-memory runtime stores allocations by ID and records operation calls for tests. It returns `rejecting_no_host` for allocations without binding, `active` for bound allocations, and configurable failures for create/update/delete/restore paths.

### Service behavior

`Service<R: RuntimeFacade>` owns:

- `Repository`
- runtime facade instance
- `PortRange`
- apply timeout
- deterministic UUID generator injectable for tests
- async mutex to serialize service mutations
- failpoints equivalent to Zig tests where useful

Create allocation scans ports from `port_range.start` through `port_range.end`, checks cross-protocol conflicts against existing repository rows, asks runtime to create the candidate, persists allocation, optionally persists a binding when compatibility `target_port` is present, and returns the allocation. Allocation+binding persistence must be atomic from the service caller perspective: if binding persistence fails after allocation insertion, the inserted allocation is removed and runtime state is deleted best-effort before returning the error. Runtime create bind failure skips the port and continues; no available port returns `NoAvailablePort`.

Conflict rules must match Zig:

- requested `tcp` conflicts with existing `tcp` or `both` on same port;
- requested `udp` conflicts with existing `udp` or `both` on same port;
- requested `both` conflicts with any existing allocation on same port.

Binding updates validate host as an IP literal, persist binding columns, update runtime, and return the API-facing binding model. Binding delete clears persistence, updates runtime to no-host state, and returns not-found when no binding exists.

Views combine repository rows and runtime snapshots exactly like Zig service: missing runtime state degrades to `degraded_bind_failed`, `error_kind = bind_failed`, and `last_error = "missing runtime state"`.

### API definition preservation

M1 does not expose HTTP, but returned structs and service method shapes must align with existing Zig API concepts:

- allocation resource: `id`, `protocol`, `port`, `created_at_ms`, `updated_at_ms`;
- binding view: `allocation_id`, `host`, `target_port`, effective target fields, runtime status/error fields, timestamps;
- aggregate allocation view: allocation fields plus binding/effective/runtime fields and `host_configured`.

M2 must be able to serialize these same models without changing API definitions.

## Acceptance criteria

- `cargo test --locked service` passes and covers all M1 service behavior.
- `cargo test --locked` passes.
- `zig build test` passes.
- Service conflict tests cover tcp-vs-both, udp-vs-both, both-vs-existing-tcp, both-vs-existing-udp, and non-conflicting tcp+udp same port.
- Service lifecycle tests cover create/get/list/delete resources and aggregate views.
- Binding tests cover put/get/delete, host validation, missing binding not-found, runtime update call, and no-host state after delete.
- Runtime failure tests cover create bind failure port skip, no available port, update failure, delete runtime failure, create binding persistence cleanup, delete persistence failure restore, and restore-all orchestration.
- API-facing structs remain unchanged from M0 model definitions except for additions required by service/runtime facade internals; compatibility update tests cover target resolution and timestamp preservation.
- M1 docs record that HTTP/API wire compatibility is deferred to M2 but model definitions are preserved.


### Listener metrics pass-through

M1 does not calculate Prometheus metrics, but `Service::snapshot_listener_metrics()` must delegate to the runtime facade unchanged so M6 can attach exporter behavior without altering service API shape. Tests must verify row identity is preserved.
