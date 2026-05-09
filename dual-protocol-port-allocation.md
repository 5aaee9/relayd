# API Change Proposal: dual-protocol port allocation

## Goal

Support reserving the same numeric relay port for both TCP and UDP through a single API operation.

Today an allocation has exactly one `protocol` value, either `tcp` or `udp`. Clients that need both protocols must create two independent allocations, which can return different port numbers. This proposal adds an explicit dual-protocol allocation mode so a client can request one logical allocation that owns both TCP and UDP listeners on the same `port`.

## Non-goals

- Do not change the existing meaning of `protocol = "tcp"` or `protocol = "udp"`.
- Do not require existing clients to migrate immediately.
- Do not make two separately created single-protocol allocations automatically pair with each other.
- Do not allow different target hosts or target ports per protocol in the first version.

## API summary

Extend allocation creation to accept a third protocol value:

```json
{
  "protocol": "both"
}
```

A successful response returns one allocation object:

```json
{
  "id": "018f0d5d-86d6-7e57-8bb5-c0b7c38e2f0e",
  "protocol": "both",
  "port": 10000,
  "created_at_ms": 1712822400000,
  "updated_at_ms": 1712822400000
}
```

The returned `port` means:

- TCP listens on `port`.
- UDP listens on the same `port`.
- The allocation owns both `(tcp, port)` and `(udp, port)` until deleted.

## Endpoint changes

### `POST /v1/allocations`

Current request:

```json
{
  "protocol": "tcp"
}
```

New validation:

- `protocol` must be one of `tcp`, `udp`, or `both`.
- `tcp` keeps current behavior and reserves one TCP relay port.
- `udp` keeps current behavior and reserves one UDP relay port.
- `both` reserves the same numeric port for both TCP and UDP.

Success for `both`:

- `201 Created`
- Body: allocation object with `protocol = "both"`.

Port selection for `both`:

- The server must choose a port from `PORT_RANGE` where neither `(tcp, port)` nor `(udp, port)` is already allocated.
- If no port satisfies both protocols, return `409 Conflict` with the existing no-available-port error body.
- Allocation must be atomic: either both protocol reservations are created, or neither is created.

### `GET /v1/allocations`

Return dual-protocol allocations as single rows:

```json
[
  {
    "id": "018f0d5d-86d6-7e57-8bb5-c0b7c38e2f0e",
    "protocol": "both",
    "port": 10000,
    "created_at_ms": 1712822400000,
    "updated_at_ms": 1712822400000
  }
]
```

Ordering remains by `protocol`, then `port`. Recommended protocol sort order:

1. `tcp`
2. `udp`
3. `both`

If preserving lexical ordering is easier, document that `both`, `tcp`, `udp` is the actual order. The important requirement is stable ordering.

### `GET /v1/allocations/{id}`

Return the allocation object. Dual-protocol allocations return `protocol = "both"`.

### `DELETE /v1/allocations/{id}`

For `protocol = "both"`, delete both protocol reservations and close both runtime listeners.

The operation remains atomic from the API user's perspective:

- Success returns `204 No Content` after both protocol listeners are removed or scheduled consistently through the runtime manager.
- Partial runtime cleanup failures should use the existing `503 Service Unavailable` mapping and leave enough state for retry/reconciliation.

## Binding semantics

Bindings remain attached to the allocation ID:

```http
PUT /v1/allocations/{id}/binding
```

For a dual-protocol allocation, one binding applies to both protocol listeners:

```json
{
  "host": "127.0.0.1",
  "target_port": 8080
}
```

Meaning:

- TCP traffic received on relay `port` forwards to `host:target_port` using TCP.
- UDP traffic received on relay `port` forwards to `host:target_port` using UDP.
- `effective_host`, `effective_target_port`, and `runtime_status` describe the combined allocation.

### Runtime status for partial failures

A dual-protocol allocation can fail independently on TCP or UDP listener setup. The API should expose enough information to diagnose this without breaking existing clients.

Recommended minimal v1 behavior:

- Keep existing `runtime_status` as the aggregate status.
- `active` means both TCP and UDP listeners are active.
- Any partial failure returns a degraded status, preferably the existing `degraded_bind_failed` or `degraded_apply_failed`.
- `last_error` should mention which protocol failed, for example `tcp bind failed: address already in use`.

Optional future extension:

```json
{
  "runtime_status": "degraded_bind_failed",
  "protocol_statuses": {
    "tcp": "active",
    "udp": "degraded_bind_failed"
  }
}
```

Do not add `protocol_statuses` unless clients need machine-readable per-protocol runtime state immediately.

## Compatibility endpoint changes

### `GET /v1/ports`

Return a dual-protocol allocation as one aggregate row with `protocol = "both"`:

```json
{
  "id": "018f0d5d-86d6-7e57-8bb5-c0b7c38e2f0e",
  "protocol": "both",
  "port": 10000,
  "target_port": 8080,
  "host": "127.0.0.1",
  "effective_target_port": 8080,
  "effective_host": "127.0.0.1",
  "host_configured": true,
  "runtime_status": "active",
  "error_kind": null,
  "last_error": null,
  "created_at_ms": 1712822400000,
  "updated_at_ms": 1712822405000
}
```

Rationale: returning two rows with the same ID and port would make compatibility clients likely to double-count or mishandle deletion.

### `POST /v1/ports`

If this legacy endpoint remains a creation surface, allow `protocol = "both"` with the same semantics as `POST /v1/allocations`, then seed the binding from `target_port` as it does today.

New validation:

- `protocol` must be one of `tcp`, `udp`, or `both`.

## Storage model

The implementation can use either storage shape, but the API must expose one logical allocation for `both`.

Recommended storage approach:

- Keep one allocation row with `protocol = "both"` and one `port` value.
- Enforce uniqueness so no single-protocol row can claim either side of a dual-protocol port.

Required uniqueness invariant:

- A `tcp` allocation conflicts with an existing `tcp` or `both` allocation on the same port.
- A `udp` allocation conflicts with an existing `udp` or `both` allocation on the same port.
- A `both` allocation conflicts with any existing `tcp`, `udp`, or `both` allocation on the same port.

If SQLite partial indexes cannot express this cleanly, enforce it in a transaction around port selection and insert, and keep tests for the conflict cases.

## Runtime model

For `protocol = "both"`, the runtime manager should treat one allocation as owning two listener handles:

- TCP listener: `(allocation_id, tcp, port)`
- UDP listener: `(allocation_id, udp, port)`

Apply semantics:

1. Validate that both listeners can be created for the chosen port.
2. Start or update both listeners using the same binding target.
3. Report `active` only when both protocols are active.
4. On rollback or delete, close both listeners.

Startup restore must restore both protocol listeners for a `both` allocation before reporting it active.

## Error mapping

Keep current HTTP status codes:

- `400 Bad Request` — `protocol` is not `tcp`, `udp`, or `both`.
- `409 Conflict` — no port in `PORT_RANGE` is available for all requested protocols.
- `503 Service Unavailable` — runtime apply/delete timeout or failure.

Suggested plain-text error bodies:

- `invalid protocol`
- `no available port`
- `runtime apply failed`

## Terraform provider impact

After relayd supports this API change, the Terraform provider can extend `relayd_port_allocation.protocol` validation from:

```text
tcp | udp
```

to:

```text
tcp | udp | both
```

Example Terraform usage:

```hcl
resource "relayd_port_allocation" "game" {
  protocol = "both"
}

resource "relayd_port_binding" "game" {
  allocation_id = relayd_port_allocation.game.id
  host          = "127.0.0.1"
  target_port   = 25565
}

output "relay_tcp_endpoint" {
  value = "${var.relay_host}:${relayd_port_allocation.game.port}"
}

output "relay_udp_endpoint" {
  value = "${var.relay_host}:${relayd_port_allocation.game.port}"
}
```

No Terraform schema shape change is required if relayd returns `protocol = "both"` in the existing allocation object.

## Acceptance criteria

- `POST /v1/allocations` with `{"protocol":"both"}` returns one allocation with one `id` and one numeric `port`.
- TCP and UDP both listen on the returned `port`.
- `GET /v1/allocations/{id}` returns `protocol = "both"`.
- `GET /v1/allocations` returns one row for the dual-protocol allocation.
- `GET /v1/ports` returns one compatibility row for the dual-protocol allocation.
- A `tcp` allocation cannot later reserve the same port as an existing `both` allocation.
- A `udp` allocation cannot later reserve the same port as an existing `both` allocation.
- A `both` allocation cannot reserve a port already used by either TCP or UDP.
- Deleting the dual-protocol allocation releases both TCP and UDP listeners.
- Startup restore recreates both listeners for persisted dual-protocol allocations.
- Existing `tcp` and `udp` API behavior remains unchanged.

## Open decision

Should `protocol = "both"` use one shared binding target for both protocols, or should relayd eventually allow per-protocol targets?

This proposal recommends one shared target for the first version because it preserves the current binding schema and keeps the Terraform provider change small. If per-protocol targets become necessary later, add a new binding shape rather than overloading this initial change.
