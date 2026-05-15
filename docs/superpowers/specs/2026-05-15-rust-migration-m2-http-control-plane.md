# Rust Migration M2 Authenticated HTTP Control Plane Spec

## Goal

Port relayd's authenticated HTTP control-plane API to Rust on top of the M1 allocation service and in-memory runtime facade. M2 must preserve the existing Zig HTTP API interfaces, JSON field definitions, status codes, bearer-auth behavior, and plain-text error bodies for allocation, binding, compatibility port, and JSON metrics routes. Real TCP/UDP forwarding remains deferred to later runtime milestones.

## Source requirements

- Continue the Zig-to-Rust migration with main functionality parity; feature-flagged runtime paths may remain deferred.
- API interfaces and definitions must remain exactly aligned with the existing Zig HTTP API.
- This milestone must have a Superpowers-style spec and plan, plan review must return `APPROVED` before implementation, implementation must use Subagent Driven Development, final spec-compliance review must approve, docs must be updated, then commit and push.

## Source of truth

- Zig HTTP server: `src/http/server.zig`.
- API documentation: `docs/API.md`.
- Existing Zig integration tests: `tests/integration/http_api_test.zig`.
- Rust service/model foundation: `src/model.rs`, `src/service/allocation_service.rs`, `src/runtime/facade.rs`, `src/storage/sqlite.rs`.

## In scope

- Rust HTTP module and router construction usable by tests and later binary startup.
- Bearer-token authentication for every route, including `/metrics`.
- Primary allocation routes:
  - `POST /v1/allocations`
  - `GET /v1/allocations`
  - `GET /v1/allocations/{id}`
  - `DELETE /v1/allocations/{id}`
- Binding routes:
  - `PUT /v1/allocations/{id}/binding`
  - `GET /v1/allocations/{id}/binding`
  - `DELETE /v1/allocations/{id}/binding`
- Compatibility routes:
  - `POST /v1/ports`
  - `POST /v1/ports/target`
  - `POST /v1/ports/{id}`
  - `DELETE /v1/ports/{id}`
  - `GET /v1/ports`
- JSON metrics route:
  - `GET /v1/metrics`
- Authenticated Prometheus route presence:
  - `GET /metrics` must require the same bearer token and return Prometheus text using current runtime facade listener snapshots. Full live byte-rate semantics remain M6, but M2 must not omit the route.
- Rust integration-style HTTP tests that drive the router through HTTP requests and assert status codes, content types, JSON shapes, auth failures, compatibility behavior, and service error mapping.

## Out of scope

- Real TCP/UDP forwarding and live socket listener runtime behavior.
- Full Prometheus rate calculation parity and live active connection/session counters; M6 owns parity for scrape-rate semantics and concrete runtime metrics. M2 only exposes the authenticated route with facade snapshot rows and zero speed samples.
- Rust binary cutover and production daemon lifecycle; M7 owns default build/run replacement. M2 may add a reusable router/server module but need not replace the Zig runtime entrypoint.
- Feature-flagged metrics values behind deferred runtime modes. The JSON metrics object must include those fields with current Rust counters, which will be zero until the deferred features are ported.

## Required API behavior

### Authentication

All endpoints require `Authorization: Bearer <AUTH_TOKEN>`. Missing headers, malformed bearer values, or wrong tokens return:

- status: `401 Unauthorized`
- body: `unauthorized`
- no JSON body requirement

Token comparison must avoid early success on prefix-only values. Header name matching should accept normal HTTP case-insensitive handling.

### Content types and bodies

- Successful `POST`, `PUT`, and `GET` JSON routes return `Content-Type: application/json`.
- Successful `DELETE` routes return `204 No Content` with an empty body.
- Error responses are plain text.
- `/metrics` returns `Content-Type: text/plain; version=0.0.4; charset=utf-8`.

### Primary allocation API

- `POST /v1/allocations` accepts body `{ "protocol": "tcp|udp|both" }`, creates an unbound allocation through `Service::create_allocation(protocol, None)`, and returns `201 Created` with an allocation resource object only: `id`, `protocol`, `port`, `created_at_ms`, `updated_at_ms`. It must not include `target_port` or aggregate runtime fields.
- `GET /v1/allocations` returns `200 OK` with an array of allocation resource objects ordered by the repository/service order.
- `GET /v1/allocations/{id}` returns `200 OK` with an allocation resource or `404 Not Found` with body `NotFound` when missing, matching Zig.
- `DELETE /v1/allocations/{id}` deletes the allocation and binding via service and returns `204 No Content`.

### Binding API

- `PUT /v1/allocations/{id}/binding` accepts `{ "host": "IP literal", "target_port": <u16> }`, calls `put_binding`, then returns `200 OK` with the binding view from `get_binding_view`.
- `GET /v1/allocations/{id}/binding` returns `200 OK` with binding view or `404 Not Found` with body `NotFound` when no binding exists.
- `DELETE /v1/allocations/{id}/binding` deletes the binding only and returns `204 No Content`.

### Compatibility API

- `POST /v1/ports` accepts `{ "protocol": "tcp|udp|both", "target_port": <u16> }`, calls `create_allocation(protocol, Some(target_port))`, then returns `201 Created` with aggregate allocation view.
- `POST /v1/ports/target` accepts `{ "id": "...", "host": "IP literal" }`, calls `set_target`, then returns `200 OK` with aggregate allocation view.
- `POST /v1/ports/{id}` accepts any non-empty combination of `target_port` and `host`; `{}` returns `400 Bad Request` with body `empty update`. Success calls `update_allocation` and returns `200 OK` with aggregate allocation view.
- `DELETE /v1/ports/{id}` is the same delete operation as `DELETE /v1/allocations/{id}` and returns `204 No Content`.
- `GET /v1/ports` returns `200 OK` with aggregate allocation views. `protocol = "both"` appears once with one `id` and one `port`.

### Error mapping

HTTP handlers must map Rust service/model/body errors to Zig/API-compatible plain-text responses:

- invalid protocol: `400 Bad Request`, body `invalid protocol`
- malformed JSON/body extraction failures: `400 Bad Request`, body `invalid request body`
- invalid host: `400 Bad Request`, body `InvalidHost`
- invalid port/body port range: `400 Bad Request`, body `invalid port`
- empty legacy update: `400 Bad Request`, body `empty update`
- missing allocation/binding: `404 Not Found`, body `NotFound`
- no available port: `409 Conflict`, body `NoAvailablePort`
- runtime timeout/update/delete/restore failures: `503 Service Unavailable`, body `Timeout`, `RuntimeUpdateFailed`, `RuntimeDeleteFailed`, or `RuntimeRestoreFailed` matching the underlying Rust service/runtime error
- unexpected internal failures: `500 Internal Server Error`, body `internal server error`
- unknown route: `404 Not Found`, body `not found`

### JSON metrics API

`GET /v1/metrics` returns a JSON object with the exact field names documented in `docs/API.md` and present in Zig `JsonMetrics`, including deferred feature counters. M2 may initialize all counters and gauges to zero except values explicitly incremented by M2 HTTP/server behavior. The object must remain forward-compatible with later runtime milestones.

### Prometheus route presence

`GET /metrics` must be authenticated and return valid text exposition with these metric families:

- `relayd_connections_current`
- `relayd_rx_bytes_per_second`
- `relayd_tx_bytes_per_second`

For each `ListenerMetricsSnapshot` row from the M1 runtime facade, M2 emits one `connections_current` sample and zero-valued speed samples. Full delta-rate calculation is deferred to M6.

## Design

Use `axum` for the Rust HTTP module because it integrates directly with Tokio, serde JSON extractors, and tower-based router tests. Keep HTTP code separate from service code:

- `src/http/mod.rs` exports the HTTP module.
- `src/http/control_plane.rs` owns router construction, request/response DTOs, auth middleware/extractor, route handlers, error mapping, and tests.
- `src/metrics.rs` owns Rust metric counters/gauges and the JSON metrics struct with exact API field names.
- `src/prometheus.rs` owns the M2 minimal Prometheus text renderer over `ListenerMetricsSnapshot` rows.

The router state should wrap `Service<R>` in `Arc` and carry an auth token plus metrics state. Tests should instantiate `Service<InMemoryRuntime>` with temp SQLite repositories, deterministic port ranges/IDs where needed, and drive `Router` with `tower::ServiceExt::oneshot` so no network sockets are required.

## Acceptance criteria

- M2 plan is independently reviewed to `APPROVED` before implementation.
- HTTP tests cover auth success/failure on JSON and `/metrics` routes.
- HTTP tests cover allocation create/list/get/delete, and assert primary allocation JSON does not contain compatibility aggregate fields.
- HTTP tests cover binding missing/put/get/delete and allocation survives binding delete.
- HTTP tests cover compatibility create/set-target/update/delete/list including empty update `400` and `both` aggregate appears once.
- HTTP tests cover service error mapping for invalid protocol, malformed JSON, invalid host, invalid port/body port range, missing resource, no available port, distinct runtime failures, and timeout through M1 failpoints/runtime failpoints.
- HTTP tests cover `GET /v1/metrics` exact documented field names.
- HTTP tests cover `GET /metrics` auth and minimal text exposition from seeded listener metric snapshots.
- `cargo fmt -- --check` passes.
- `cargo test --locked` passes.
- `zig build test` passes to prove existing Zig implementation remains intact.
- M2 docs/status are updated after implementation, and the final independent spec-compliance review returns `APPROVED` before commit/push.
