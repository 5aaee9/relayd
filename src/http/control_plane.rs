use crate::config::parse_port;
use crate::metrics::Metrics;
use crate::model::Protocol;
use crate::runtime::facade::{RuntimeError, RuntimeFacade};
use crate::service::allocation_service::{Service, ServiceError};
use axum::body::Body;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::{
    Request, StatusCode,
    header::{AUTHORIZATION, CONTENT_TYPE},
    request::Parts,
};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;

pub struct AppState<R: RuntimeFacade> {
    pub service: Arc<Service<R>>,
    pub metrics: Arc<Metrics>,
    pub auth_token: Arc<str>,
}

impl<R: RuntimeFacade> Clone for AppState<R> {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            metrics: Arc::clone(&self.metrics),
            auth_token: Arc::clone(&self.auth_token),
        }
    }
}

impl<R: RuntimeFacade> AppState<R> {
    pub fn new(
        service: Arc<Service<R>>,
        metrics: Arc<Metrics>,
        auth_token: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            service,
            metrics,
            auth_token: auth_token.into(),
        }
    }
}

pub fn router<R: RuntimeFacade + 'static>(state: AppState<R>) -> Router {
    Router::new()
        .route("/metrics", get(prometheus_metrics::<R>))
        .route("/v1/metrics", get(json_metrics::<R>))
        .route(
            "/v1/allocations",
            get(list_allocations::<R>).post(create_allocation::<R>),
        )
        .route(
            "/v1/allocations/{id}",
            get(get_allocation::<R>).delete(delete_allocation::<R>),
        )
        .route(
            "/v1/allocations/{id}/binding",
            get(get_binding::<R>)
                .put(put_binding::<R>)
                .delete(delete_binding::<R>),
        )
        .route("/v1/ports", get(list_ports::<R>).post(create_port::<R>))
        .route("/v1/ports/target", post(set_target::<R>))
        .route(
            "/v1/ports/{id}",
            post(update_port::<R>).delete(delete_port::<R>),
        )
        .fallback(not_found::<R>)
        .method_not_allowed_fallback(not_found::<R>)
        .with_state(state)
}

struct Authed;

impl<R: RuntimeFacade> FromRequestParts<AppState<R>> for Authed {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState<R>,
    ) -> Result<Self, Self::Rejection> {
        let Some(value) = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        else {
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
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Debug, Deserialize)]
struct AllocationCreateRequest {
    protocol: String,
}

#[derive(Debug, Deserialize)]
struct BindingPutRequest {
    host: String,
    target_port: u32,
}

#[derive(Debug, Deserialize)]
struct PortCreateRequest {
    protocol: String,
    target_port: u32,
}

#[derive(Debug, Deserialize)]
struct TargetRequest {
    id: String,
    host: String,
}

#[derive(Debug, Deserialize)]
struct UpdateRequest {
    target_port: Option<u32>,
    host: Option<String>,
}

fn json_response<T: serde::Serialize>(status: StatusCode, value: T) -> Response {
    (status, Json(value)).into_response()
}

fn text_response(status: StatusCode, body: &'static str) -> Response {
    (status, body).into_response()
}

#[derive(Debug, Clone, Copy)]
struct HttpTextError {
    status: StatusCode,
    body: &'static str,
}

impl HttpTextError {
    fn into_response(self) -> Response {
        text_response(self.status, self.body)
    }
}

fn parse_protocol(value: &str) -> Result<Protocol, HttpTextError> {
    Protocol::from_str(value).map_err(|_| HttpTextError {
        status: StatusCode::BAD_REQUEST,
        body: "invalid protocol",
    })
}

fn parse_request_port(port: u32) -> Result<u16, HttpTextError> {
    parse_port(&port.to_string()).map_err(|_| HttpTextError {
        status: StatusCode::BAD_REQUEST,
        body: "invalid port",
    })
}

async fn read_json<T: serde::de::DeserializeOwned>(
    request: Request<Body>,
) -> Result<T, HttpTextError> {
    let bytes = axum::body::to_bytes(request.into_body(), 4096)
        .await
        .map_err(|_| HttpTextError {
            status: StatusCode::BAD_REQUEST,
            body: "invalid request body",
        })?;
    serde_json::from_slice(&bytes).map_err(|_| HttpTextError {
        status: StatusCode::BAD_REQUEST,
        body: "invalid request body",
    })
}

fn service_error_response(error: ServiceError) -> Response {
    match error {
        ServiceError::NotFound => text_response(StatusCode::NOT_FOUND, "NotFound"),
        ServiceError::NoAvailablePort => text_response(StatusCode::CONFLICT, "NoAvailablePort"),
        ServiceError::InvalidHost => text_response(StatusCode::BAD_REQUEST, "InvalidHost"),
        ServiceError::Timeout => text_response(StatusCode::SERVICE_UNAVAILABLE, "Timeout"),
        ServiceError::Runtime(RuntimeError::RuntimeCreateFailed) => {
            text_response(StatusCode::SERVICE_UNAVAILABLE, "RuntimeCreateFailed")
        }
        ServiceError::Runtime(RuntimeError::RuntimeUpdateFailed) => {
            text_response(StatusCode::SERVICE_UNAVAILABLE, "RuntimeUpdateFailed")
        }
        ServiceError::Runtime(RuntimeError::RuntimeDeleteFailed) => {
            text_response(StatusCode::SERVICE_UNAVAILABLE, "RuntimeDeleteFailed")
        }
        ServiceError::Runtime(RuntimeError::RuntimeRestoreFailed) => {
            text_response(StatusCode::SERVICE_UNAVAILABLE, "RuntimeRestoreFailed")
        }
        ServiceError::Runtime(RuntimeError::Timeout) => {
            text_response(StatusCode::SERVICE_UNAVAILABLE, "Timeout")
        }
        ServiceError::DeletePersistenceFailed => {
            text_response(StatusCode::INTERNAL_SERVER_ERROR, "DeletePersistenceFailed")
        }
        ServiceError::Repository(_) => {
            text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
        }
    }
}

async fn prometheus_metrics<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
) -> Response {
    match state.service.snapshot_listener_metrics().await {
        Ok(rows) => (
            StatusCode::OK,
            [(CONTENT_TYPE, crate::prometheus::CONTENT_TYPE)],
            crate::prometheus::render(&rows),
        )
            .into_response(),
        Err(error) => service_error_response(error),
    }
}

async fn json_metrics<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
) -> Response {
    json_response(StatusCode::OK, state.metrics.snapshot())
}

async fn list_allocations<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
) -> Response {
    match state.service.list_allocation_resources().await {
        Ok(resources) => json_response(StatusCode::OK, resources),
        Err(error) => service_error_response(error),
    }
}

async fn create_allocation<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    request: Request<Body>,
) -> Response {
    let body: AllocationCreateRequest = match read_json(request).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let protocol = match parse_protocol(&body.protocol) {
        Ok(protocol) => protocol,
        Err(error) => return error.into_response(),
    };
    let allocation = match state.service.create_allocation(protocol, None).await {
        Ok(value) => value,
        Err(error) => return service_error_response(error),
    };
    match state.service.get_allocation_resource(&allocation.id).await {
        Ok(Some(resource)) => json_response(StatusCode::CREATED, resource),
        Ok(None) => text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error"),
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

async fn put_binding<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    let body: BindingPutRequest = match read_json(request).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let target_port = match parse_request_port(body.target_port) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = state
        .service
        .put_binding(&id, &body.host, target_port)
        .await
    {
        return service_error_response(error);
    }
    match state.service.get_binding_view(&id).await {
        Ok(Some(view)) => json_response(StatusCode::OK, view),
        Ok(None) => text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error"),
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

async fn aggregate_view<R: RuntimeFacade>(
    state: &AppState<R>,
    id: &str,
    status: StatusCode,
) -> Response {
    match state.service.get_allocation_view(id).await {
        Ok(Some(view)) => json_response(status, view),
        Ok(None) => text_response(StatusCode::NOT_FOUND, "NotFound"),
        Err(error) => service_error_response(error),
    }
}

async fn list_ports<R: RuntimeFacade>(_auth: Authed, State(state): State<AppState<R>>) -> Response {
    match state.service.list_allocations().await {
        Ok(views) => json_response(StatusCode::OK, views),
        Err(error) => service_error_response(error),
    }
}

async fn create_port<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    request: Request<Body>,
) -> Response {
    let body: PortCreateRequest = match read_json(request).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let protocol = match parse_protocol(&body.protocol) {
        Ok(protocol) => protocol,
        Err(error) => return error.into_response(),
    };
    let target_port = match parse_request_port(body.target_port) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let allocation = match state
        .service
        .create_allocation(protocol, Some(target_port))
        .await
    {
        Ok(value) => value,
        Err(error) => return service_error_response(error),
    };
    aggregate_view(&state, &allocation.id, StatusCode::CREATED).await
}

async fn set_target<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    request: Request<Body>,
) -> Response {
    let body: TargetRequest = match read_json(request).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let allocation = match state.service.set_target(&body.id, &body.host).await {
        Ok(value) => value,
        Err(error) => return service_error_response(error),
    };
    aggregate_view(&state, &allocation.id, StatusCode::OK).await
}

async fn update_port<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    let body: UpdateRequest = match read_json(request).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let target_port = match body.target_port {
        Some(port) => match parse_request_port(port) {
            Ok(value) => Some(value),
            Err(error) => return error.into_response(),
        },
        None => None,
    };
    if target_port.is_none() && body.host.is_none() {
        return text_response(StatusCode::BAD_REQUEST, "empty update");
    }
    let allocation = match state
        .service
        .update_allocation(&id, target_port, body.host.as_deref())
        .await
    {
        Ok(value) => value,
        Err(error) => return service_error_response(error),
    };
    aggregate_view(&state, &allocation.id, StatusCode::OK).await
}

async fn delete_port<R: RuntimeFacade>(
    _auth: Authed,
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
) -> Response {
    match state.service.delete_allocation(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => service_error_response(error),
    }
}

async fn not_found<R: RuntimeFacade>(_auth: Authed, State(_state): State<AppState<R>>) -> Response {
    text_response(StatusCode::NOT_FOUND, "not found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PortRange;
    use crate::runtime::facade::InMemoryRuntime;
    use crate::storage::sqlite::Repository;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use std::sync::{Arc, Mutex as StdMutex};
    use tempfile::NamedTempFile;
    use tower::ServiceExt;

    async fn test_app() -> (
        Router,
        Arc<Service<InMemoryRuntime>>,
        InMemoryRuntime,
        Arc<Metrics>,
        NamedTempFile,
    ) {
        let file = NamedTempFile::new().unwrap();
        let repo = Repository::open(file.path()).await.unwrap();
        let runtime = InMemoryRuntime::default();
        let metrics = Arc::new(Metrics::default());
        let next_id = Arc::new(StdMutex::new(0_u64));
        let service = Arc::new(Service::with_id_generator(
            repo,
            runtime.clone(),
            PortRange {
                start: 10000,
                end: 10002,
            },
            500,
            Arc::new(move || {
                let mut next_id = next_id.lock().unwrap();
                *next_id += 1;
                format!("alloc-{next_id}")
            }),
        ));
        let app = router(AppState::new(
            service.clone(),
            metrics.clone(),
            "secret-token",
        ));
        (app, service, runtime, metrics, file)
    }

    async fn request(
        app: Router,
        method: Method,
        uri: &str,
        body: &str,
        auth: Option<&str>,
    ) -> (StatusCode, Option<String>, String) {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(auth) = auth {
            builder = builder.header("authorization", auth);
        }
        let response = app
            .oneshot(builder.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            content_type,
            String::from_utf8(bytes.to_vec()).unwrap(),
        )
    }

    #[tokio::test]
    async fn auth_is_required_for_json_and_prometheus_routes() {
        let (app, _, _, _, _file) = test_app().await;
        let registered_routes = [
            (Method::GET, "/metrics", ""),
            (Method::GET, "/v1/metrics", ""),
            (Method::GET, "/v1/allocations", ""),
            (Method::POST, "/v1/allocations", "{}"),
            (Method::GET, "/v1/allocations/alloc-1", ""),
            (Method::DELETE, "/v1/allocations/alloc-1", ""),
            (Method::GET, "/v1/allocations/alloc-1/binding", ""),
            (Method::PUT, "/v1/allocations/alloc-1/binding", "{}"),
            (Method::DELETE, "/v1/allocations/alloc-1/binding", ""),
            (Method::GET, "/v1/ports", ""),
            (Method::POST, "/v1/ports", "{}"),
            (Method::POST, "/v1/ports/target", "{}"),
            (Method::POST, "/v1/ports/alloc-1", "{}"),
            (Method::DELETE, "/v1/ports/alloc-1", ""),
        ];

        for (method, uri, body) in registered_routes {
            let (status, _, response_body) =
                request(app.clone(), method.clone(), uri, body, None).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "missing auth for {method} {uri}"
            );
            assert_eq!(response_body, "unauthorized");

            let (status, _, response_body) =
                request(app.clone(), method.clone(), uri, body, Some("Bearer wrong")).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "wrong auth for {method} {uri}"
            );
            assert_eq!(response_body, "unauthorized");
        }
    }

    #[tokio::test]
    async fn compatibility_ports_lifecycle_matches_legacy_api_shape() {
        let (app, _, _, _, _file) = test_app().await;

        let (status, content_type, body) = request(
            app.clone(),
            Method::POST,
            "/v1/ports",
            r#"{"protocol":"both","target_port":8080}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_json_content_type(content_type);
        let created: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(created["id"], "alloc-1");
        assert_eq!(created["protocol"], "both");
        assert_eq!(created["port"], 10000);
        assert_eq!(created["target_port"], 8080);
        assert_eq!(created["runtime_status"], "rejecting_no_host");

        let (status, content_type, body) = request(
            app.clone(),
            Method::POST,
            "/v1/ports/target",
            r#"{"id":"alloc-1","host":"127.0.0.1"}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_json_content_type(content_type);
        let targeted: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(targeted["id"], "alloc-1");
        assert_eq!(targeted["host"], "127.0.0.1");
        assert_eq!(targeted["host_configured"], true);

        let (status, content_type, body) = request(
            app.clone(),
            Method::POST,
            "/v1/ports/alloc-1",
            r#"{"target_port":9090}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_json_content_type(content_type);
        let updated: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(updated["target_port"], 9090);
        assert_eq!(updated["host"], "127.0.0.1");

        let (status, _, body) = request(
            app.clone(),
            Method::POST,
            "/v1/ports/alloc-1",
            "{}",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "empty update");

        let (status, content_type, body) = request(
            app.clone(),
            Method::GET,
            "/v1/ports",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_json_content_type(content_type);
        let listed: Value = serde_json::from_str(&body).unwrap();
        let listed = listed.as_array().unwrap();
        assert_eq!(
            listed.len(),
            1,
            "protocol=both allocation should appear once"
        );
        assert_eq!(listed[0]["protocol"], "both");

        let (status, _, body) = request(
            app.clone(),
            Method::DELETE,
            "/v1/ports/alloc-1",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(body.is_empty());

        let (status, content_type, body) = request(
            app,
            Method::GET,
            "/v1/ports",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_json_content_type(content_type);
        let listed: Value = serde_json::from_str(&body).unwrap();
        assert!(listed.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn json_metrics_uses_snapshot_field_names_and_reflects_counters() {
        let (app, _, _, metrics, _file) = test_app().await;
        metrics.allocations_total.add(2);
        metrics.tcp_session_create_total.inc();
        metrics.udp_packets_in_total.add(7);

        let (status, content_type, body) = request(
            app,
            Method::GET,
            "/v1/metrics",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_json_content_type(content_type);
        let metrics: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(metrics["allocations_total"], 2);
        assert_eq!(metrics["tcp_session_create_total"], 1);
        assert_eq!(metrics["udp_packets_in_total"], 7);
        assert!(metrics.get("runtime_apply_total").is_some());
        assert!(metrics.get("http_non_loopback_bind_total").is_some());
    }

    #[tokio::test]
    async fn prometheus_metrics_uses_seeded_rows_text_content_type_labels_and_no_json() {
        let (app, _, runtime, _, _file) = test_app().await;
        runtime.seed_listener_metrics(vec![
            crate::runtime::facade::ListenerMetricsSnapshot {
                port: 10000,
                protocol: Protocol::Tcp,
                connections_current: 4,
                rx_bytes_total: 10,
                tx_bytes_total: 20,
            },
            crate::runtime::facade::ListenerMetricsSnapshot {
                port: 10001,
                protocol: Protocol::Udp,
                connections_current: 5,
                rx_bytes_total: 30,
                tx_bytes_total: 40,
            },
        ]);

        let (status, content_type, body) = request(
            app,
            Method::GET,
            "/metrics",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            content_type.as_deref(),
            Some(crate::prometheus::CONTENT_TYPE)
        );
        assert!(body.contains("# TYPE relayd_connections_current gauge\n"));
        assert!(body.contains("# TYPE relayd_rx_bytes_per_second gauge\n"));
        assert!(body.contains("# TYPE relayd_tx_bytes_per_second gauge\n"));
        assert!(body.contains("relayd_connections_current{port=\"10000\",protocol=\"tcp\"} 4\n"));
        assert!(body.contains("relayd_connections_current{port=\"10001\",protocol=\"udp\"} 5\n"));
        assert!(!body.contains("{\""));
    }

    #[tokio::test]
    async fn unknown_and_wrong_method_routes_auth_first_then_return_not_found_body() {
        let (app, _, _, _, _file) = test_app().await;

        let (status, _, body) = request(app.clone(), Method::GET, "/v1/unknown", "", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body, "unauthorized");

        let (status, content_type, body) = request(
            app.clone(),
            Method::GET,
            "/v1/unknown",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(content_type.as_deref(), Some("text/plain; charset=utf-8"));
        assert_eq!(body, "not found");

        let (status, content_type, body) = request(
            app.clone(),
            Method::GET,
            "/v1/ports/target",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(content_type.as_deref(), Some("text/plain; charset=utf-8"));
        assert_eq!(body, "not found");

        let (status, content_type, body) =
            request(app, Method::GET, "/nope", "", Some("Bearer secret-token")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(content_type.as_deref(), Some("text/plain; charset=utf-8"));
        assert_eq!(body, "not found");
    }

    fn assert_json_content_type(content_type: Option<String>) {
        assert!(
            content_type
                .as_deref()
                .is_some_and(|value| value.starts_with("application/json")),
            "content-type should be JSON, got {content_type:?}"
        );
    }

    #[tokio::test]
    async fn allocation_and_binding_lifecycle_matches_primary_api_shape() {
        let (app, _, _, _, _file) = test_app().await;

        let (status, content_type, body) = request(
            app.clone(),
            Method::POST,
            "/v1/allocations",
            r#"{"protocol":"tcp"}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_json_content_type(content_type);
        let created: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(created["id"], "alloc-1");
        assert_eq!(created["protocol"], "tcp");
        assert_eq!(created["port"], 10000);
        assert!(created.get("target_port").is_none());

        let (status, content_type, body) = request(
            app.clone(),
            Method::GET,
            "/v1/allocations/alloc-1",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_json_content_type(content_type);
        let fetched: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(fetched["id"], "alloc-1");
        assert!(fetched.get("host_configured").is_none());
        assert!(fetched.get("runtime_status").is_none());

        let (status, content_type, body) = request(
            app.clone(),
            Method::GET,
            "/v1/allocations",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_json_content_type(content_type);
        let listed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(listed.as_array().unwrap().len(), 1);
        assert_eq!(listed[0]["id"], "alloc-1");
        assert!(listed[0].get("host_configured").is_none());
        assert!(listed[0].get("runtime_status").is_none());

        let (status, _, body) = request(
            app.clone(),
            Method::GET,
            "/v1/allocations/missing",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, "NotFound");

        let (status, _, body) = request(
            app.clone(),
            Method::GET,
            "/v1/allocations/alloc-1/binding",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, "NotFound");

        let (status, content_type, body) = request(
            app.clone(),
            Method::PUT,
            "/v1/allocations/alloc-1/binding",
            r#"{"host":"127.0.0.1","target_port":8080}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_json_content_type(content_type);
        let binding: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(binding["allocation_id"], "alloc-1");
        assert_eq!(binding["host"], "127.0.0.1");
        assert_eq!(binding["target_port"], 8080);
        assert_eq!(binding["runtime_status"], "active");

        let (status, content_type, body) = request(
            app.clone(),
            Method::GET,
            "/v1/allocations/alloc-1/binding",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_json_content_type(content_type);
        let binding: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(binding["effective_target_port"], 8080);

        let (status, _, body) = request(
            app.clone(),
            Method::DELETE,
            "/v1/allocations/alloc-1/binding",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(body.is_empty());

        let (status, _, body) = request(
            app.clone(),
            Method::GET,
            "/v1/allocations/alloc-1/binding",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, "NotFound");

        let (status, _, _) = request(
            app.clone(),
            Method::GET,
            "/v1/allocations/alloc-1",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _, body) = request(
            app.clone(),
            Method::PUT,
            "/v1/allocations/alloc-1/binding",
            r#"{"host":"127.0.0.1","target_port":8080}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains(r#""allocation_id":"alloc-1""#));
        let (status, _, body) = request(
            app.clone(),
            Method::DELETE,
            "/v1/allocations/alloc-1",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(body.is_empty());

        let (status, _, body) = request(
            app,
            Method::GET,
            "/v1/allocations/alloc-1/binding",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, "NotFound");
    }

    #[tokio::test]
    async fn primary_routes_report_validation_and_service_errors() {
        let (app, service, runtime, _, _file) = test_app().await;

        let (status, _, body) = request(
            app.clone(),
            Method::POST,
            "/v1/allocations",
            r#"{"protocol":"http"}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "invalid protocol");

        let (status, content_type, body) = request(
            app.clone(),
            Method::POST,
            "/v1/allocations",
            "{",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(content_type.as_deref(), Some("text/plain; charset=utf-8"));
        assert_eq!(body, "invalid request body");

        request(
            app.clone(),
            Method::POST,
            "/v1/allocations",
            r#"{"protocol":"tcp"}"#,
            Some("Bearer secret-token"),
        )
        .await;

        let (status, _, body) = request(
            app.clone(),
            Method::PUT,
            "/v1/allocations/alloc-1/binding",
            r#"{"host":"localhost","target_port":8080}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "InvalidHost");

        for port in [0_u32, 70000_u32] {
            let (status, _, body) = request(
                app.clone(),
                Method::PUT,
                "/v1/allocations/alloc-1/binding",
                &format!(r#"{{"host":"127.0.0.1","target_port":{port}}}"#),
                Some("Bearer secret-token"),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "port {port}");
            assert_eq!(body, "invalid port", "port {port}");
        }

        request(
            app.clone(),
            Method::POST,
            "/v1/allocations",
            r#"{"protocol":"tcp"}"#,
            Some("Bearer secret-token"),
        )
        .await;
        request(
            app.clone(),
            Method::POST,
            "/v1/allocations",
            r#"{"protocol":"tcp"}"#,
            Some("Bearer secret-token"),
        )
        .await;
        let (status, _, body) = request(
            app.clone(),
            Method::POST,
            "/v1/allocations",
            r#"{"protocol":"tcp"}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body, "NoAvailablePort");

        runtime.fail_update_id("alloc-1");
        let (status, _, body) = request(
            app.clone(),
            Method::PUT,
            "/v1/allocations/alloc-1/binding",
            r#"{"host":"127.0.0.1","target_port":8080}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "RuntimeUpdateFailed");
        runtime.clear_failures();

        service
            .set_failpoints(crate::service::allocation_service::Failpoints {
                update_timeout: true,
                ..Default::default()
            })
            .await;
        let (status, _, body) = request(
            app.clone(),
            Method::PUT,
            "/v1/allocations/alloc-1/binding",
            r#"{"host":"127.0.0.1","target_port":8080}"#,
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "Timeout");
        service.set_failpoints(Default::default()).await;

        runtime.fail_delete_id("alloc-1");
        let (status, _, body) = request(
            app,
            Method::DELETE,
            "/v1/allocations/alloc-1",
            "",
            Some("Bearer secret-token"),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "RuntimeDeleteFailed");
    }
}
