# Dual-Protocol Port Allocation Design

## Source requirement

Implement `dual-protocol-port-allocation.md`: `protocol = "both"` creates one logical allocation that owns TCP and UDP listeners on the same relay port, while preserving existing `tcp` and `udp` semantics.

## Goals

- Accept `protocol` values `tcp`, `udp`, and `both` on allocation creation surfaces.
- For `both`, return one allocation object with one `id` and one numeric `port`.
- Reserve the selected port atomically for both protocols: no existing TCP, UDP, or `both` allocation can conflict with it, and future TCP/UDP/`both` allocations cannot reuse either side.
- Runtime creates, updates, restores, and deletes both TCP and UDP listeners for a dual-protocol allocation.
- One binding target applies to both protocol listeners.
- API list/read and compatibility endpoints return one aggregate row with `protocol = "both"`.
- Existing single-protocol behavior remains unchanged.

## Non-goals

- No per-protocol binding targets.
- No automatic pairing of independently created TCP and UDP allocations.
- No Terraform provider code changes in this repository.
- No new dependencies or storage framework.

## Architecture

### Protocol model

Extend `src/model/allocation.zig` `Protocol` with `.both`. `fromString()` accepts case-insensitive `both`, and `asString()` continues to use `@tagName` so JSON encoders emit `"both"`.

### Port reservation

Keep the current single-row storage shape: one `allocations` row with `protocol = "both"` and one `port`. Enforce the cross-protocol invariant with an atomic repository operation: begin an SQLite `IMMEDIATE` transaction, check conflicting rows for the candidate port inside that transaction, insert the allocation before commit, and roll back on any conflict or runtime failure. The service mutex remains useful in-process, but the SQLite transaction is the correctness boundary:

- `tcp` conflicts with rows where `port` matches and `protocol in ('tcp', 'both')`.
- `udp` conflicts with rows where `port` matches and `protocol in ('udp', 'both')`.
- `both` conflicts with any row where `port` matches.

The existing `UNIQUE(protocol, port)` remains a backstop for identical protocol collisions, but it is not sufficient for `both`; tests must lock the invariant.

### Runtime ownership

Generalize runtime listener state so one allocation ID can own more than one listener handle. For `tcp` and `udp`, behavior remains exactly one listener. For `both`, runtime manager creates a primary entry with two protocol listeners on the same numeric port. Runtime event dispatch must route by the concrete event FD protocol, not by the aggregate allocation protocol:

- TCP listener for `(allocation_id, tcp, port)`, and TCP accept handlers must accept from that concrete TCP FD.
- UDP listener for `(allocation_id, udp, port)`, and UDP readable/GRO/io_uring handlers must receive from that concrete UDP FD.

The API-level observed state stays one aggregate state per allocation ID. Status is `active` only when both listeners exist and a binding is configured; without a binding, the aggregate status is `rejecting_no_host`; if only one side binds or applies, status is degraded and `last_error` names the failed protocol.

### Binding semantics

No binding schema change. Existing `bindings` rows remain keyed by `allocation_id`, so one binding applies to both listeners. Runtime tests must use a local upstream that listens on the same numeric target port for TCP and UDP so the shared target semantics are actually verified. `putBinding()` and compatibility updates set the same `host` and `target_port` for TCP and UDP runtime paths.

### HTTP/API behavior

- `POST /v1/allocations {"protocol":"both"}` returns `201` and an allocation resource with no binding fields.
- `POST /v1/ports` accepts `protocol = "both"` and seeds the binding from `target_port` exactly like existing compatibility creation.
- `GET /v1/allocations`, `GET /v1/allocations/{id}`, and `GET /v1/ports` emit one row for a dual allocation.
- Existing invalid protocol response remains `400`.
- No available dual-capable port remains `409` via `error.NoAvailablePort`.

### Startup restore and delete

`Service.restoreAll()` passes persisted `both` allocations to runtime restore. Runtime restore attempts both listeners and records a degraded aggregate if one side fails. `deleteAllocation()` removes both listener handles before removing the persisted row; partial runtime deletion failures continue to map to existing `503` service errors.

## Testing strategy

- Unit: protocol parser accepts `both`; repository/service conflict rules for tcp-vs-both, udp-vs-both, and both-vs-existing single protocol.
- HTTP integration: `POST /v1/allocations` and `POST /v1/ports` accept `both`, return a single row, and list/read endpoints show `protocol":"both"` once.
- Runtime integration: a `both` allocation with one binding forwards TCP and UDP traffic on the same relay port to a TCP+UDP upstream bound on one shared target port; delete releases both listener sockets, proven by subsequent TCP and UDP reuse checks.
- Restore: persisted dual allocation is restored with both listeners and can forward both protocols after restart.
- Regression: existing TCP-only and UDP-only tests continue to pass.

## Documentation

Update `docs/API.md` and `docs/api/http.md` to document `protocol = "both"`, the shared binding target, compatibility endpoint behavior, conflict semantics, and aggregate runtime status semantics. Keep `dual-protocol-port-allocation.md` as the source proposal unless implementation decisions need a short note.

## Success criteria

- All acceptance criteria in `dual-protocol-port-allocation.md` have direct tests or documented verification, including legacy `POST /v1/ports` creation with `protocol = "both"`.
- `zig build test` passes.
- Independent implementation reviewer returns `APPROVE` for spec compliance before the final doc update/commit/push.

## Self-review

- Placeholder scan: no placeholders remain.
- Internal consistency: one-row storage, one binding, atomic conflict checks, concrete-FD runtime dispatch, and aggregate runtime state are used consistently across API, runtime, and docs.
- Scope: focused on relayd implementation only; Terraform provider is explicitly excluded.
- Ambiguity: the open binding decision is resolved for v1 as one shared target for both protocols.
