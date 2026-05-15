# Rust Migration M2 Authenticated HTTP Control Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the Rust authenticated HTTP control plane for relayd's allocation, binding, compatibility port, JSON metrics, and minimal Prometheus routes while preserving the Zig API interface and definitions.

**Architecture:** Use an Axum router with shared `Arc` state containing the M1 `Service<R>`, auth token, and Rust metrics object. Keep serialization/error mapping in `src/http/control_plane.rs`, exact JSON metric fields in `src/metrics.rs`, and M2 minimal Prometheus rendering in `src/prometheus.rs`; tests drive the router with Tower `oneshot` requests and an in-memory runtime facade.

**Tech Stack:** Rust 1.95, Tokio, Axum 0.8, Tower test utilities, serde/serde_json, M1 SeaORM/SQLx repository and service, `cargo test --locked`, `zig build test`.

---

## File Structure

- Modify: `Cargo.toml` — add `axum`, `http-body-util`, `serde_json`, and `tower` with util features for router tests.
- Modify: `Cargo.lock` — lock dependency changes.
- Modify: `src/lib.rs` — export `http`, `metrics`, and `prometheus` modules.
- Create: `src/metrics.rs` — Rust metric counter/gauge primitives and exact `JsonMetrics` API object.
- Create: `src/prometheus.rs` — M2 minimal Prometheus text renderer over listener metric snapshots.
- Create: `src/http/mod.rs` — HTTP module declaration.
- Create: `src/http/control_plane.rs` — Axum app state, router, auth, DTOs, handlers, error mapping, and HTTP tests.
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md` — update M2 status after implementation.
- Existing source references: `src/http/server.zig`, `docs/API.md`, `tests/integration/http_api_test.zig`.

## Acceptance checklist

- [ ] Independent plan reviewer returns `APPROVED` before implementation.
- [ ] Router requires bearer auth on `/v1/*` and `/metrics`.
- [ ] Allocation routes match Zig/API status codes, content types, bodies, and primary resource shape.
- [ ] Binding routes match Zig/API lifecycle behavior and binding view shape.
- [ ] Compatibility `/v1/ports*` routes match Zig/API aggregate behavior and error handling.
- [ ] `/v1/metrics` returns the exact documented JSON field names.
- [ ] `/metrics` exists, is authenticated, returns Prometheus text content type, and emits M2 snapshot-based samples.
- [ ] Error mapping tests cover invalid protocol, malformed JSON/body extraction, invalid host, invalid port/body port range, missing resources, no available port, distinct runtime failures, and timeout.
- [ ] `cargo fmt -- --check` passes.
- [ ] `cargo test --locked` passes.
- [ ] `zig build test` passes.
- [ ] Final independent spec-compliance reviewer returns `APPROVED` before docs commit/push.

## Task 1: Add HTTP dependencies and metrics primitives

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Create: `src/metrics.rs`
- Create: `src/prometheus.rs`

- [ ] **Step 1: Add dependencies**

Add to `[dependencies]` in `Cargo.toml`:

```toml
axum = "0.8"
http-body-util = "0.1"
serde_json = "1"
tower = { version = "0.5", features = ["util"] }
```

Keep existing dependencies unchanged. Run `cargo update -p axum -p tower -p http-body-util -p serde_json` only if Cargo.lock is not updated by the first test build.

- [ ] **Step 2: Export new modules**

Append these exports to `src/lib.rs`:

```rust
pub mod http;
pub mod metrics;
pub mod prometheus;
```

- [ ] **Step 3: Implement exact JSON metrics fields**

Create `src/metrics.rs` with atomic counter/gauge wrappers and a serializable `JsonMetrics` containing every Zig field from `src/http/server.zig::JsonMetrics`:

```rust
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) { self.add(1); }
    pub fn add(&self, amount: u64) { self.0.fetch_add(amount, Ordering::Relaxed); }
    pub fn load(&self) -> u64 { self.0.load(Ordering::Relaxed) }
}

#[derive(Debug, Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    pub fn inc(&self) { self.0.fetch_add(1, Ordering::Relaxed); }
    pub fn dec(&self) { self.0.fetch_sub(1, Ordering::Relaxed); }
    pub fn load(&self) -> u64 { self.0.load(Ordering::Relaxed) }
}

#[derive(Debug, Default)]
pub struct Metrics {
    pub allocations_total: Counter,
    pub runtime_apply_total: Counter,
    pub restore_failures_total: Counter,
    pub rejected_no_host_total: Counter,
    pub bind_fail_total: Counter,
    pub tcp_splice_fast_path_total: Counter,
    pub tcp_copy_fallback_total: Counter,
    pub tcp_splice_attempt_total: Counter,
    pub tcp_splice_success_total: Counter,
    pub tcp_splice_fallback_total: Counter,
    pub tcp_splice_hard_failure_total: Counter,
    pub tcp_splice_fallback_forced_total: Counter,
    pub tcp_splice_fallback_unsupported_total: Counter,
    pub tcp_splice_fallback_runtime_error_total: Counter,
    pub tcp_session_create_total: Counter,
    pub tcp_session_close_total: Counter,
    pub tcp_session_event_total: Counter,
    pub tcp_session_worker_dispatch_total: Counter,
    pub tcp_session_worker0_dispatch_total: Counter,
    pub tcp_session_worker1_dispatch_total: Counter,
    pub tcp_accept_handoff_total: Counter,
    pub tcp_accept_handoff_worker0_total: Counter,
    pub tcp_accept_handoff_worker1_total: Counter,
    pub tcp_accept_handoff_worker2_total: Counter,
    pub tcp_accept_handoff_worker3_total: Counter,
    pub tcp_listener_accept_total: Counter,
    pub tcp_listener_accept_worker0_total: Counter,
    pub tcp_listener_accept_worker1_total: Counter,
    pub tcp_listener_accept_worker2_total: Counter,
    pub tcp_listener_accept_worker3_total: Counter,
    pub tcp_upstream_connect_total: Counter,
    pub tcp_upstream_connect_fail_total: Counter,
    pub tcp_active_sessions: Gauge,
    pub udp_packets_in_total: Counter,
    pub udp_packets_out_total: Counter,
    pub udp_bytes_in_total: Counter,
    pub udp_bytes_out_total: Counter,
    pub udp_recv_errors_total: Counter,
    pub udp_send_errors_total: Counter,
    pub udp_session_create_total: Counter,
    pub udp_session_expire_total: Counter,
    pub udp_batch_calls_total: Counter,
    pub udp_batch_messages_total: Counter,
    pub udp_drop_total: Counter,
    pub udp_reply_primary_total: Counter,
    pub udp_reply_drop_total: Counter,
    pub udp_reply_stale_total: Counter,
    pub udp_worker_packets_in_total: Counter,
    pub udp_worker_packets_out_total: Counter,
    pub udp_worker0_packets_in_total: Counter,
    pub udp_worker1_packets_in_total: Counter,
    pub udp_worker2_packets_in_total: Counter,
    pub udp_worker3_packets_in_total: Counter,
    pub udp_worker0_packets_out_total: Counter,
    pub udp_worker1_packets_out_total: Counter,
    pub udp_worker2_packets_out_total: Counter,
    pub udp_worker3_packets_out_total: Counter,
    pub udp_dataplane_redesign_packets_in_total: Counter,
    pub udp_dataplane_redesign_packets_out_total: Counter,
    pub udp_io_uring_submit_total: Counter,
    pub udp_io_uring_cqe_total: Counter,
    pub udp_io_uring_multishot_total: Counter,
    pub udp_io_uring_buf_release_total: Counter,
    pub udp_io_uring_fallback_total: Counter,
    pub udp_fast_path_packets_in_total: Counter,
    pub udp_fast_path_packets_out_total: Counter,
    pub udp_fast_path_gso_send_total: Counter,
    pub udp_fast_path_gro_recv_total: Counter,
    pub udp_fast_path_fallback_total: Counter,
    pub udp_fast_path_drop_total: Counter,
    pub udp_active_sessions: Gauge,
    pub restore_timeout_total: Counter,
    pub http_non_loopback_bind_total: Counter,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct JsonMetrics {
    pub allocations_total: u64,
    pub runtime_apply_total: u64,
    pub restore_failures_total: u64,
    pub rejected_no_host_total: u64,
    pub bind_fail_total: u64,
    pub tcp_splice_fast_path_total: u64,
    pub tcp_copy_fallback_total: u64,
    pub tcp_splice_attempt_total: u64,
    pub tcp_splice_success_total: u64,
    pub tcp_splice_fallback_total: u64,
    pub tcp_splice_hard_failure_total: u64,
    pub tcp_splice_fallback_forced_total: u64,
    pub tcp_splice_fallback_unsupported_total: u64,
    pub tcp_splice_fallback_runtime_error_total: u64,
    pub tcp_session_create_total: u64,
    pub tcp_session_close_total: u64,
    pub tcp_session_event_total: u64,
    pub tcp_session_worker_dispatch_total: u64,
    pub tcp_session_worker0_dispatch_total: u64,
    pub tcp_session_worker1_dispatch_total: u64,
    pub tcp_accept_handoff_total: u64,
    pub tcp_accept_handoff_worker0_total: u64,
    pub tcp_accept_handoff_worker1_total: u64,
    pub tcp_accept_handoff_worker2_total: u64,
    pub tcp_accept_handoff_worker3_total: u64,
    pub tcp_listener_accept_total: u64,
    pub tcp_listener_accept_worker0_total: u64,
    pub tcp_listener_accept_worker1_total: u64,
    pub tcp_listener_accept_worker2_total: u64,
    pub tcp_listener_accept_worker3_total: u64,
    pub tcp_upstream_connect_total: u64,
    pub tcp_upstream_connect_fail_total: u64,
    pub tcp_active_sessions: u64,
    pub udp_packets_in_total: u64,
    pub udp_packets_out_total: u64,
    pub udp_bytes_in_total: u64,
    pub udp_bytes_out_total: u64,
    pub udp_recv_errors_total: u64,
    pub udp_send_errors_total: u64,
    pub udp_session_create_total: u64,
    pub udp_session_expire_total: u64,
    pub udp_batch_calls_total: u64,
    pub udp_batch_messages_total: u64,
    pub udp_drop_total: u64,
    pub udp_reply_primary_total: u64,
    pub udp_reply_drop_total: u64,
    pub udp_reply_stale_total: u64,
    pub udp_worker_packets_in_total: u64,
    pub udp_worker_packets_out_total: u64,
    pub udp_worker0_packets_in_total: u64,
    pub udp_worker1_packets_in_total: u64,
    pub udp_worker2_packets_in_total: u64,
    pub udp_worker3_packets_in_total: u64,
    pub udp_worker0_packets_out_total: u64,
    pub udp_worker1_packets_out_total: u64,
    pub udp_worker2_packets_out_total: u64,
    pub udp_worker3_packets_out_total: u64,
    pub udp_dataplane_redesign_packets_in_total: u64,
    pub udp_dataplane_redesign_packets_out_total: u64,
    pub udp_io_uring_submit_total: u64,
    pub udp_io_uring_cqe_total: u64,
    pub udp_io_uring_multishot_total: u64,
    pub udp_io_uring_buf_release_total: u64,
    pub udp_io_uring_fallback_total: u64,
    pub udp_fast_path_packets_in_total: u64,
    pub udp_fast_path_packets_out_total: u64,
    pub udp_fast_path_gso_send_total: u64,
    pub udp_fast_path_gro_recv_total: u64,
    pub udp_fast_path_fallback_total: u64,
    pub udp_fast_path_drop_total: u64,
    pub udp_active_sessions: u64,
    pub restore_timeout_total: u64,
    pub http_non_loopback_bind_total: u64,
}
```

Then add `impl Metrics { pub fn snapshot(&self) -> JsonMetrics { ... } }` mapping every field one-to-one with `.load()`.

- [ ] **Step 4: Implement minimal Prometheus renderer**

Create `src/prometheus.rs`:

```rust
use crate::runtime::facade::ListenerMetricsSnapshot;

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

pub fn render(rows: &[ListenerMetricsSnapshot]) -> String {
    let mut out = String::new();
    out.push_str("# TYPE relayd_connections_current gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_connections_current{{port=\"{}\",protocol=\"{}\"}} {}\n",
            row.port,
            row.protocol.as_str(),
            row.connections_current
        ));
    }
    out.push_str("# TYPE relayd_rx_bytes_per_second gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_rx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} 0\n",
            row.port,
            row.protocol.as_str()
        ));
    }
    out.push_str("# TYPE relayd_tx_bytes_per_second gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_tx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} 0\n",
            row.port,
            row.protocol.as_str()
        ));
    }
    out
}
```

- [ ] **Step 5: Add unit tests for exact metrics field names and Prometheus labels**

In `src/metrics.rs`, add a test that serializes `Metrics::default().snapshot()` with `serde_json::to_value` and asserts all field names listed in `docs/API.md` exist.

In `src/prometheus.rs`, add a test with TCP and UDP rows asserting the three `# TYPE` lines and labels such as `port="10000",protocol="tcp"` exist and that the output does not contain `{\"` JSON fragments.

- [ ] **Step 6: Verify Task 1**

Run:

```bash
cargo fmt -- --check
CARGO_TARGET_DIR=/tmp/relayd-m2-task1-target cargo test --locked metrics
CARGO_TARGET_DIR=/tmp/relayd-m2-task1-target cargo test --locked prometheus
```

Expected: format check passes and both targeted test filters pass.

## Task 2: Build authenticated Axum router skeleton

**Files:**
- Create: `src/http/mod.rs`
- Create: `src/http/control_plane.rs`

- [ ] **Step 1: Create module declaration**

Create `src/http/mod.rs`:

```rust
pub mod control_plane;
```

- [ ] **Step 2: Define router state and constructor**

Create `src/http/control_plane.rs` with state and route registration:

```rust
use crate::metrics::Metrics;
use crate::runtime::facade::RuntimeFacade;
use crate::service::allocation_service::Service;
use axum::{Router, routing::{delete, get, post, put}};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState<R: RuntimeFacade> {
    pub service: Arc<Service<R>>,
    pub metrics: Arc<Metrics>,
    pub auth_token: Arc<str>,
}

impl<R: RuntimeFacade> AppState<R> {
    pub fn new(service: Arc<Service<R>>, metrics: Arc<Metrics>, auth_token: impl Into<Arc<str>>) -> Self {
        Self { service, metrics, auth_token: auth_token.into() }
    }
}

pub fn router<R: RuntimeFacade + Clone + 'static>(state: AppState<R>) -> Router {
    Router::new()
        .route("/metrics", get(prometheus_metrics::<R>))
        .route("/v1/metrics", get(json_metrics::<R>))
        .route("/v1/allocations", get(list_allocations::<R>).post(create_allocation::<R>))
        .route("/v1/allocations/{id}", get(get_allocation::<R>).delete(delete_allocation::<R>))
        .route("/v1/allocations/{id}/binding", get(get_binding::<R>).put(put_binding::<R>).delete(delete_binding::<R>))
        .route("/v1/ports", get(list_ports::<R>).post(create_port::<R>))
        .route("/v1/ports/target", post(set_target::<R>))
        .route("/v1/ports/{id}", post(update_port::<R>).delete(delete_allocation::<R>))
        .fallback(not_found)
        .with_state(state)
}
```

Add placeholder async handlers returning `StatusCode::NOT_IMPLEMENTED` only inside this step, then replace them in Tasks 3-5 before any acceptance review.

- [ ] **Step 3: Implement auth extractor**

In `src/http/control_plane.rs`, implement an `Authed` extractor using `FromRequestParts<AppState<R>>`:

```rust
use axum::extract::FromRequestParts;
use axum::http::{header::AUTHORIZATION, request::Parts, StatusCode};

struct Authed;

impl<R: RuntimeFacade> FromRequestParts<AppState<R>> for Authed {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, state: &AppState<R>) -> Result<Self, Self::Rejection> {
        let Some(value) = parts.headers.get(AUTHORIZATION).and_then(|value| value.to_str().ok()) else {
            return Err((StatusCode::UNAUTHORIZED, "unauthorized"));
        };
        let Some(provided) = value.strip_prefix("Bearer ") else {
            return Err((StatusCode::UNAUTHORIZED, "unauthorized"));
        };
        if constant_time_eq(provided.as_bytes(), state.auth_token.as_bytes()) {
            Ok(Self)
        } else {
            Err((StatusCode::UNAUTHORIZED, "unauthorized"))
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() { return false; }
    let mut diff = 0_u8;
    for (&x, &y) in a.iter().zip(b.iter()) { diff |= x ^ y; }
    diff == 0
}
```

Every handler signature in later tasks must include `_auth: Authed` as the first extractor so all routes enforce auth.

- [ ] **Step 4: Add router test harness**

Add test helpers in `src/http/control_plane.rs` under `#[cfg(test)]`:

```rust
use crate::config::PortRange;
use crate::runtime::facade::InMemoryRuntime;
use crate::storage::sqlite::Repository;
use axum::body::Body;
use axum::http::{Method, Request};
use http_body_util::BodyExt;
use std::sync::{Arc, Mutex as StdMutex};
use tempfile::NamedTempFile;
use tower::ServiceExt;

async fn test_app() -> (Router, Arc<Service<InMemoryRuntime>>, InMemoryRuntime, Arc<Metrics>, NamedTempFile) {
    let file = NamedTempFile::new().unwrap();
    let repo = Repository::open(file.path()).await.unwrap();
    let runtime = InMemoryRuntime::default();
    let metrics = Arc::new(Metrics::default());
    let next_id = Arc::new(StdMutex::new(0_u64));
    let service = Arc::new(Service::with_id_generator(
        repo,
        runtime.clone(),
        PortRange { start: 10000, end: 10002 },
        500,
        Arc::new(move || {
            let mut next_id = next_id.lock().unwrap();
            *next_id += 1;
            format!("alloc-{next_id}")
        }),
    ));
    let app = router(AppState::new(service.clone(), metrics.clone(), "secret-token"));
    (app, service, runtime, metrics, file)
}

async fn request(app: Router, method: Method, uri: &str, body: &str, auth: Option<&str>) -> (StatusCode, Option<String>, String) {
    let mut builder = Request::builder().method(method).uri(uri).header("content-type", "application/json");
    if let Some(auth) = auth { builder = builder.header("authorization", auth); }
    let response = app.oneshot(builder.body(Body::from(body.to_owned())).unwrap()).await.unwrap();
    let status = response.status();
    let content_type = response.headers().get("content-type").and_then(|value| value.to_str().ok()).map(str::to_owned);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, content_type, String::from_utf8(bytes.to_vec()).unwrap())
}
```

- [ ] **Step 5: Add auth tests**

Add tests:

```rust
#[tokio::test]
async fn auth_is_required_for_json_and_prometheus_routes() {
    let (app, _, _, _, _file) = test_app().await;
    let (status, _, body) = request(app.clone(), Method::GET, "/v1/allocations", "", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, "unauthorized");

    let (status, _, body) = request(app.clone(), Method::GET, "/v1/allocations", "", Some("Bearer wrong")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, "unauthorized");

    let (status, _, body) = request(app, Method::GET, "/metrics", "", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, "unauthorized");
}
```

After Task 2, this test should pass once placeholder handlers include `_auth: Authed` and return anything other than 401 for correct auth. Full route behavior is implemented in later tasks.

- [ ] **Step 6: Verify Task 2**

Run:

```bash
cargo fmt -- --check
CARGO_TARGET_DIR=/tmp/relayd-m2-task2-target cargo test --locked http::control_plane::tests::auth_is_required_for_json_and_prometheus_routes
```

Expected: targeted auth test passes.

## Task 3: Implement allocation and binding routes

**Files:**
- Modify: `src/http/control_plane.rs`

- [ ] **Step 1: Add request DTOs and response helpers**

Add request DTOs:

```rust
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AllocationCreateRequest { protocol: String }

#[derive(Debug, Deserialize)]
struct BindingPutRequest { host: String, target_port: u32 }
```

Add response helpers:

```rust
use axum::{
    body::Body,
    extract::{Path, State},
    http::Request,
    response::{IntoResponse, Response},
    Json,
};
use crate::config::parse_port;
use crate::model::{AllocationResource, BindingView, Protocol};
use crate::runtime::facade::RuntimeError;
use crate::service::allocation_service::ServiceError;
use std::str::FromStr;

fn json_response<T: serde::Serialize>(status: StatusCode, value: T) -> Response {
    (status, Json(value)).into_response()
}

fn text_response(status: StatusCode, body: &'static str) -> Response {
    (status, body).into_response()
}

fn parse_protocol(value: &str) -> Result<Protocol, Response> {
    Protocol::from_str(value).map_err(|_| text_response(StatusCode::BAD_REQUEST, "invalid protocol"))
}

fn service_error_response(error: ServiceError) -> Response {
    match error {
        ServiceError::NotFound => text_response(StatusCode::NOT_FOUND, "NotFound"),
        ServiceError::NoAvailablePort => text_response(StatusCode::CONFLICT, "NoAvailablePort"),
        ServiceError::InvalidHost => text_response(StatusCode::BAD_REQUEST, "InvalidHost"),
        ServiceError::Timeout => text_response(StatusCode::SERVICE_UNAVAILABLE, "Timeout"),
        ServiceError::Runtime(RuntimeError::RuntimeCreateFailed) => text_response(StatusCode::SERVICE_UNAVAILABLE, "RuntimeCreateFailed"),
        ServiceError::Runtime(RuntimeError::RuntimeUpdateFailed) => text_response(StatusCode::SERVICE_UNAVAILABLE, "RuntimeUpdateFailed"),
        ServiceError::Runtime(RuntimeError::RuntimeDeleteFailed) => text_response(StatusCode::SERVICE_UNAVAILABLE, "RuntimeDeleteFailed"),
        ServiceError::Runtime(RuntimeError::RuntimeRestoreFailed) => text_response(StatusCode::SERVICE_UNAVAILABLE, "RuntimeRestoreFailed"),
        ServiceError::Runtime(RuntimeError::Timeout) => text_response(StatusCode::SERVICE_UNAVAILABLE, "Timeout"),
        ServiceError::DeletePersistenceFailed => text_response(StatusCode::INTERNAL_SERVER_ERROR, "DeletePersistenceFailed"),
        ServiceError::Repository(_) => text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error"),
    }
}

async fn read_json<T: serde::de::DeserializeOwned>(request: Request<Body>) -> Result<T, Response> {
    let bytes = axum::body::to_bytes(request.into_body(), 4096)
        .await
        .map_err(|_| text_response(StatusCode::BAD_REQUEST, "invalid request body"))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| text_response(StatusCode::BAD_REQUEST, "invalid request body"))
}

fn parse_request_port(port: u32) -> Result<u16, Response> {
    parse_port(&port.to_string()).map_err(|_| text_response(StatusCode::BAD_REQUEST, "invalid port"))
}
```

If a later compile shows non-update runtime variants need distinct strings, keep status `503` and choose stable bodies matching tests.

- [ ] **Step 2: Implement primary allocation handlers**

Replace placeholders with:

```rust
async fn create_allocation<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    request: Request<Body>,
) -> Response {
    let body: AllocationCreateRequest = match read_json(request).await { Ok(value) => value, Err(response) => return response };
    let protocol = match parse_protocol(&body.protocol) { Ok(protocol) => protocol, Err(response) => return response };
    let allocation = match state.service.create_allocation(protocol, None).await { Ok(value) => value, Err(error) => return service_error_response(error) };
    match state.service.get_allocation_resource(&allocation.id).await {
        Ok(Some(resource)) => json_response(StatusCode::CREATED, resource),
        Ok(None) => text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error"),
        Err(error) => service_error_response(error),
    }
}

async fn list_allocations<R: RuntimeFacade>(_auth: Authed, State(state): State<AppState<R>>) -> Response {
    match state.service.list_allocation_resources().await {
        Ok(resources) => json_response(StatusCode::OK, resources),
        Err(error) => service_error_response(error),
    }
}

async fn get_allocation<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
) -> Response {
    match state.service.get_allocation_resource(&id).await {
        Ok(Some(resource)) => json_response(StatusCode::OK, resource),
        Ok(None) => text_response(StatusCode::NOT_FOUND, "NotFound"),
        Err(error) => service_error_response(error),
    }
}

async fn delete_allocation<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
) -> Response {
    match state.service.delete_allocation(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => service_error_response(error),
    }
}
```

- [ ] **Step 3: Implement binding handlers**

```rust
async fn put_binding<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    let body: BindingPutRequest = match read_json(request).await { Ok(value) => value, Err(response) => return response };
    let target_port = match parse_request_port(body.target_port) { Ok(value) => value, Err(response) => return response };
    if let Err(error) = state.service.put_binding(&id, &body.host, target_port).await {
        return service_error_response(error);
    }
    match state.service.get_binding_view(&id).await {
        Ok(Some(view)) => json_response(StatusCode::OK, view),
        Ok(None) => text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error"),
        Err(error) => service_error_response(error),
    }
}

async fn get_binding<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
) -> Response {
    match state.service.get_binding_view(&id).await {
        Ok(Some(view)) => json_response(StatusCode::OK, view),
        Ok(None) => text_response(StatusCode::NOT_FOUND, "NotFound"),
        Err(error) => service_error_response(error),
    }
}

async fn delete_binding<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
) -> Response {
    match state.service.delete_binding(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => service_error_response(error),
    }
}
```

- [ ] **Step 4: Add allocation/binding lifecycle tests**

Add tests that:

1. `POST /v1/allocations` with `{"protocol":"tcp"}` returns `201`, JSON content type, id `alloc-1`, protocol `tcp`, port `10000`, and no `target_port` key.
2. `GET /v1/allocations/{id}` and `GET /v1/allocations` return the same id and no aggregate-only fields.
3. missing `GET /v1/allocations/missing` returns `404` and `NotFound`.
4. missing binding returns `404` and `NotFound`.
5. `PUT /v1/allocations/{id}/binding` returns binding view with `allocation_id`, `host`, `target_port`, `runtime_status = active`.
6. `GET` binding returns `effective_target_port`.
7. `DELETE` binding returns `204`, subsequent binding get is `404`, and allocation get remains `200`.
8. `DELETE /v1/allocations/{id}` returns `204` and subsequent binding get is `404`.

Use `serde_json::Value` assertions rather than string-order assertions for JSON bodies.

- [ ] **Step 5: Add primary route error tests**

Add tests for:

- invalid protocol body on `POST /v1/allocations` returns `400` and `invalid protocol`.
- malformed JSON such as `{` on `POST /v1/allocations` returns `400` plain text `invalid request body`.
- invalid binding host returns `400`.
- invalid port/body port range such as `target_port: 0` or `target_port: 70000` in a binding or compatibility body route returns `400` plain text `invalid port`; DTOs use `u32` for request ports so out-of-`u16` values reach `parse_request_port` instead of becoming generic JSON extractor failures.
- all ports unavailable: create three conflicting allocations through HTTP/service in range `10000..=10002`, then next `POST /v1/allocations` returns `409`.
- runtime update failure on binding returns `503` with body `RuntimeUpdateFailed` by calling `runtime.fail_update_id("alloc-1")` before `PUT`.
- timeout mapping returns `503` with body `Timeout` by setting `service.set_failpoints(Failpoints { update_timeout: true, ..Default::default() })` before a binding or compatibility update.
- runtime delete failure returns `503` with body `RuntimeDeleteFailed` by calling `runtime.fail_delete_id("alloc-1")` before `DELETE /v1/allocations/alloc-1`.

- [ ] **Step 6: Verify Task 3**

Run:

```bash
cargo fmt -- --check
CARGO_TARGET_DIR=/tmp/relayd-m2-task3-target cargo test --locked http::control_plane
```

Expected: HTTP control-plane allocation/binding tests pass.

## Task 4: Implement compatibility ports, JSON metrics, and Prometheus routes

**Files:**
- Modify: `src/http/control_plane.rs`

- [ ] **Step 1: Add compatibility request DTOs**

```rust
#[derive(Debug, Deserialize)]
struct PortCreateRequest { protocol: String, target_port: u32 }

#[derive(Debug, Deserialize)]
struct TargetRequest { id: String, host: String }

#[derive(Debug, Deserialize)]
struct UpdateRequest { target_port: Option<u32>, host: Option<String> }
```

- [ ] **Step 2: Implement aggregate helper and compatibility handlers**

Add helper:

```rust
async fn aggregate_view<R: RuntimeFacade>(state: &AppState<R>, id: &str, status: StatusCode) -> Response {
    match state.service.get_allocation_view(id).await {
        Ok(Some(view)) => json_response(status, view),
        Ok(None) => text_response(StatusCode::NOT_FOUND, "NotFound"),
        Err(error) => service_error_response(error),
    }
}
```

Implement:

```rust
async fn create_port<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    request: Request<Body>,
) -> Response {
    let body: PortCreateRequest = match read_json(request).await { Ok(value) => value, Err(response) => return response };
    let protocol = match parse_protocol(&body.protocol) { Ok(protocol) => protocol, Err(response) => return response };
    let target_port = match parse_request_port(body.target_port) { Ok(value) => value, Err(response) => return response };
    let allocation = match state.service.create_allocation(protocol, Some(target_port)).await { Ok(value) => value, Err(error) => return service_error_response(error) };
    aggregate_view(&state, &allocation.id, StatusCode::CREATED).await
}

async fn set_target<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    request: Request<Body>,
) -> Response {
    let body: TargetRequest = match read_json(request).await { Ok(value) => value, Err(response) => return response };
    let allocation = match state.service.set_target(&body.id, &body.host).await { Ok(value) => value, Err(error) => return service_error_response(error) };
    aggregate_view(&state, &allocation.id, StatusCode::OK).await
}

async fn update_port<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    let body: UpdateRequest = match read_json(request).await { Ok(value) => value, Err(response) => return response };
    let target_port = match body.target_port {
        Some(port) => Some(match parse_request_port(port) { Ok(value) => value, Err(response) => return response }),
        None => None,
    };
    if target_port.is_none() && body.host.is_none() {
        return text_response(StatusCode::BAD_REQUEST, "empty update");
    }
    let allocation = match state.service.update_allocation(&id, target_port, body.host.as_deref()).await { Ok(value) => value, Err(error) => return service_error_response(error) };
    aggregate_view(&state, &allocation.id, StatusCode::OK).await
}

async fn list_ports<R: RuntimeFacade>(_auth: Authed, State(state): State<AppState<R>>) -> Response {
    match state.service.list_allocations().await {
        Ok(views) => json_response(StatusCode::OK, views),
        Err(error) => service_error_response(error),
    }
}
```

- [ ] **Step 3: Implement metrics routes**

```rust
use axum::http::header::CONTENT_TYPE;
use crate::prometheus;

async fn json_metrics<R: RuntimeFacade>(_auth: Authed, State(state): State<AppState<R>>) -> Response {
    json_response(StatusCode::OK, state.metrics.snapshot())
}

async fn prometheus_metrics<R: RuntimeFacade>(_auth: Authed, State(state): State<AppState<R>>) -> Response {
    match state.service.snapshot_listener_metrics().await {
        Ok(rows) => ([ (CONTENT_TYPE, prometheus::CONTENT_TYPE) ], prometheus::render(&rows)).into_response(),
        Err(error) => service_error_response(error),
    }
}

async fn not_found() -> Response {
    text_response(StatusCode::NOT_FOUND, "not found")
}
```

- [ ] **Step 4: Add compatibility route tests**

Add tests that:

1. `POST /v1/ports` returns `201` aggregate view with `target_port` and runtime status present.
2. `POST /v1/ports/target` sets host and returns `200` aggregate view with `host_configured: true` and `runtime_status: active`.
3. `POST /v1/ports/{id}` with `{}` returns `400` and `empty update`.
4. target-only update preserves host; host-only update preserves target port.
5. `GET /v1/ports` returns aggregate array and `both` allocation appears once.
6. `DELETE /v1/ports/{id}` returns `204` and removes allocation.

- [ ] **Step 5: Add metrics route tests**

Add tests that:

1. `GET /v1/metrics` returns `200`, JSON content type, and every field name from `Metrics::snapshot()`.
2. Mutating the `Arc<Metrics>` returned by `test_app()` with `metrics.http_non_loopback_bind_total.inc()` before sending the request is reflected in JSON.
3. Seed runtime listener metrics with TCP and UDP rows, then `GET /metrics` returns `200`, exact Prometheus content type, the three `# TYPE` lines, concrete labels, current connection values, and no JSON fragments.
4. `GET /unknown` returns `404` and `not found`.

- [ ] **Step 6: Verify Task 4**

Run:

```bash
cargo fmt -- --check
CARGO_TARGET_DIR=/tmp/relayd-m2-task4-target cargo test --locked http::control_plane
CARGO_TARGET_DIR=/tmp/relayd-m2-task4-target cargo test --locked metrics
CARGO_TARGET_DIR=/tmp/relayd-m2-task4-target cargo test --locked prometheus
```

Expected: targeted HTTP and metrics/prometheus test filters pass.

## Task 5: Final M2 verification, docs update, and review handoff

**Files:**
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`
- Optionally modify: `docs/API.md` only if implementation exposes a documented M2 limitation that must be stated without changing API definitions.

- [ ] **Step 1: Update milestone ledger status**

Append or update this block in `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`:

```markdown
## M2 implementation status

- Status: implemented in Rust HTTP control-plane router using the M1 in-memory runtime facade.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `zig build test`.
- API note: Allocation, binding, compatibility port, JSON metrics, auth, content type, and error-mapping interfaces are preserved. Real forwarding and full Prometheus rate parity remain M3-M6.
```

- [ ] **Step 2: Run final verification**

Run:

```bash
cargo fmt -- --check
CARGO_TARGET_DIR=/tmp/relayd-m2-final-target cargo test --locked
zig build test
```

Expected: all commands exit 0.

- [ ] **Step 3: Independent final spec-compliance review**

Dispatch an independent reviewer with this prompt:

```text
Work in /home/indexyz/relayd. Review M2 final implementation against docs/superpowers/specs/2026-05-15-rust-migration-m2-http-control-plane.md and docs/superpowers/plans/2026-05-15-rust-migration-m2-http-control-plane.md. Compare HTTP API behavior against src/http/server.zig, docs/API.md, and relevant tests/integration/http_api_test.zig cases. Verify auth, route set, status codes, content types, JSON shapes, error mapping, JSON metrics fields, minimal /metrics route presence, tests, and docs status. Return exactly APPROVED if compliant, otherwise CHANGES_REQUESTED with file/line evidence. Do not edit files.
```

Fix any `CHANGES_REQUESTED` issues and repeat review until `APPROVED`.

- [ ] **Step 4: Commit and push after approval**

After approval, commit using Lore protocol:

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/http src/metrics.rs src/prometheus.rs docs/superpowers/specs/2026-05-15-rust-migration-m2-http-control-plane.md docs/superpowers/plans/2026-05-15-rust-migration-m2-http-control-plane.md docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md
git commit -m "Expose Rust HTTP control-plane parity" \
  -m "Rust needs the documented authenticated API surface before live runtime forwarding can be cut over. This adds the allocation, binding, compatibility, JSON metrics, and minimal Prometheus route layer on top of the M1 service facade."
git push origin main
```

The actual commit message must include Lore trailers for `Constraint`, `Rejected`, `Confidence`, `Scope-risk`, `Directive`, `Tested`, `Not-tested`, and `Co-authored-by: OmX <omx@oh-my-codex.dev>`.
