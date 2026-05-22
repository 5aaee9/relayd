# Tracing Lifecycle Logs Design

## Purpose

Add operator-visible structured logs to relayd for control-plane lifecycle changes using the Rust `tracing` ecosystem. The logs must make it possible to audit when relay allocations are created, when upstream bindings/assignments are created or updated, and when bindings or entire relay allocations are deleted.

## Scope

In scope:

- Initialize a `tracing_subscriber` formatter in the `relayd` binary so logs are emitted without requiring application code changes by callers.
- Add structured `tracing` events for successful lifecycle mutations in `Service`:
  - allocation/relay creation through both `POST /v1/allocations` and compatibility `POST /v1/ports` because both call `Service::create_allocation`;
  - binding/assign creation or replacement through `PUT /v1/allocations/{id}/binding`, `POST /v1/ports/target`, and `POST /v1/ports/{id}` because they call `put_binding`, `set_target`, or `update_allocation`;
  - binding/assign deletion through `DELETE /v1/allocations/{id}/binding`;
  - allocation/relay deletion through both `DELETE /v1/allocations/{id}` and compatibility `DELETE /v1/ports/{id}`.
- Log detailed fields needed for audit/debugging: allocation id, protocol, relay port, configured target port, configured host, and the operation-specific previous/new values where relevant.
- Keep API behavior, response bodies, status codes, persistence, runtime side effects, and metrics unchanged.
- Document logging behavior and runtime configuration in the README.

Out of scope:

- Request/response access logs for every HTTP request.
- Per-packet or per-connection dataplane logs.
- Runtime TCP/UDP forwarding internals beyond lifecycle mutation events.
- New log storage, rotation, or remote log shipping.

## Design

Use `tracing` rather than `log` or `println!`. Add `tracing` and `tracing-subscriber` dependencies. The binary initializes `tracing_subscriber::fmt()` at startup with an `EnvFilter`, defaulting to `info` when `RUST_LOG` is unset. This keeps default lifecycle events visible and lets operators change verbosity with `RUST_LOG`.

Lifecycle events live in `src/service/allocation_service.rs`, next to the state transitions that know the final persisted/runtime allocation data. This avoids duplicating logs across primary and compatibility HTTP handlers and ensures the same event is emitted regardless of which endpoint triggered the mutation.

Events are emitted only after the operation has successfully applied the relevant runtime and persistence changes. Failed operations continue to return existing errors without adding success logs. The delete-allocation log captures details from the allocation loaded before deletion so the deleted relay remains identifiable after the repository row is removed.

Event names and fields:

- `relay_allocation_created`: `allocation_id`, `protocol`, `relay_port`, `target_port`, `host`.
- `relay_binding_assigned`: `allocation_id`, `protocol`, `relay_port`, `target_port`, `host`, `previous_target_port`, `previous_host`.
- `relay_binding_deleted`: `allocation_id`, `protocol`, `relay_port`, `previous_target_port`, `previous_host`.
- `relay_allocation_deleted`: `allocation_id`, `protocol`, `relay_port`, `target_port`, `host`.
- Startup/shutdown support events in the binary may include HTTP listen address, relay bind host, port range, database path, restore completion, and shutdown signal failures.

Field values should be structured tracing fields, not only interpolated message text. Optional values are logged as debug-formatted fields so `None` remains explicit.

## Testing

Add focused service tests that install a test tracing subscriber with a custom writer and assert successful create, binding assign/update, binding delete, and allocation delete operations emit the expected event names and key fields. Tests should avoid depending on wall-clock timestamps or exact formatting beyond stable field snippets.

Run the targeted service tests first, then the full test suite, formatting, and clippy.

## Documentation

Update README with a logging section explaining that relayd writes tracing logs to stderr, default level is `info`, and `RUST_LOG` can adjust filtering, with an example such as `RUST_LOG=relayd=debug,tower_http=warn` or `RUST_LOG=info`.

## Acceptance Criteria

- `cargo test --locked service::allocation_service` passes.
- `cargo test --locked` passes.
- `cargo fmt --check` passes.
- `cargo clippy --locked --lib --tests -- -D warnings` passes.
- README documents logging behavior.
- The final diff contains no API contract changes unrelated to logging.
