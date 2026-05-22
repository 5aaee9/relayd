use clap::Parser;
use relayd::config::Config;
use relayd::http::control_plane::{AppState, router};
use relayd::metrics::Metrics;
use relayd::runtime::real::{RealRuntime, RealRuntimeConfig};
use relayd::service::allocation_service::Service;
use relayd::storage::sqlite::Repository;
use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    init_tracing();
    if let Err(error) = run().await {
        error!(%error, "relayd exited with error");
        eprintln!("relayd: {error}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = config_from_cli_and_env(Cli::parse(), std::env::vars().collect())?;
    run_with_config(config).await
}

#[derive(Debug, Parser)]
#[command(
    name = "relayd",
    version,
    about = "Authenticated TCP/UDP relay daemon with SQLite persistence",
    long_about = "relayd starts an authenticated HTTP control plane, persists allocations in SQLite, restores allocations at startup, and forwards TCP/UDP traffic for allocated ports. CLI options override the matching environment variables."
)]
struct Cli {
    #[arg(
        long,
        value_name = "ADDR",
        help = "HTTP control-plane listen address (env: HTTP_LISTEN). Use :PORT to bind 127.0.0.1; IPv6 literals use [::1]:PORT."
    )]
    http_listen: Option<String>,

    #[arg(
        long,
        value_name = "HOST",
        help = "TCP/UDP relay listen host and UDP forwarding session source bind host (env: PROXY_LISTEN_HOST). Default: 0.0.0.0."
    )]
    proxy_listen_host: Option<String>,

    #[arg(
        long,
        value_name = "START-END",
        help = "Inclusive relay port allocation range (env: PORT_RANGE). Example: 10000-30000."
    )]
    port_range: Option<String>,

    #[arg(
        long,
        value_name = "TOKEN",
        help = "Required bearer token for authenticated HTTP API requests (env: AUTH_TOKEN)."
    )]
    auth_token: Option<String>,

    #[arg(
        long,
        value_name = "PATH",
        help = "SQLite database path for persisted allocations (env: SQLITE_PATH)."
    )]
    sqlite_path: Option<String>,

    #[arg(
        long,
        value_name = "MILLISECONDS",
        help = "Timeout for applying runtime allocation changes (env: RUNTIME_APPLY_TIMEOUT_MS)."
    )]
    runtime_apply_timeout_ms: Option<String>,

    #[arg(
        long,
        value_name = "MILLISECONDS",
        help = "Timeout for startup restore sweep operations (env: RESTORE_SWEEP_TIMEOUT_MS)."
    )]
    restore_sweep_timeout_ms: Option<String>,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Enable the compatibility TCP session model flag (env: TCP_SESSION_MODEL_ENABLED)."
    )]
    tcp_session_model_enabled: bool,

    #[arg(
        long,
        value_name = "COUNT",
        help = "Compatibility TCP session worker count parsed by config (env: TCP_SESSION_MODEL_WORKERS)."
    )]
    tcp_session_model_workers: Option<String>,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Enable balanced TCP accept compatibility flag (env: TCP_SESSION_MODEL_ACCEPT_BALANCED)."
    )]
    tcp_session_model_accept_balanced: bool,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Enable sharded TCP accept compatibility flag (env: TCP_SESSION_MODEL_SHARDED_ACCEPT)."
    )]
    tcp_session_model_sharded_accept: bool,

    #[arg(
        long,
        value_name = "COUNT",
        help = "Maximum active compatibility TCP sessions (env: TCP_SESSION_MODEL_MAX_ACTIVE)."
    )]
    tcp_session_model_max_active: Option<String>,

    #[arg(
        long,
        value_name = "COUNT",
        help = "Compatibility UDP session worker count parsed by config (env: UDP_SESSION_WORKERS)."
    )]
    udp_session_workers: Option<String>,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Enable UDP io_uring compatibility flag parsed by config (env: UDP_IO_URING_ENABLED)."
    )]
    udp_io_uring_enabled: bool,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Enable UDP GRO compatibility flag parsed by config (env: UDP_GRO_ENABLED)."
    )]
    udp_gro_enabled: bool,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Enable UDP dataplane redesign compatibility flag parsed by config (env: UDP_DATAPLANE_REDESIGN_ENABLED)."
    )]
    udp_dataplane_redesign_enabled: bool,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Enable UDP fast-path compatibility flag parsed by config (env: UDP_FAST_PATH_ENABLED)."
    )]
    udp_fast_path_enabled: bool,

    #[arg(
        long,
        value_name = "BYTES",
        help = "UDP fast-path segment size parsed by config (env: UDP_FAST_PATH_SEGMENT_SIZE)."
    )]
    udp_fast_path_segment_size: Option<String>,

    #[arg(
        long,
        value_name = "COUNT",
        help = "UDP fast-path GSO burst size parsed by config (env: UDP_FAST_PATH_GSO_BURST)."
    )]
    udp_fast_path_gso_burst: Option<String>,

    #[arg(
        long,
        value_name = "BYTES",
        help = "UDP socket receive buffer size parsed for compatibility (env: UDP_SOCKET_RCVBUF_BYTES)."
    )]
    udp_socket_rcvbuf_bytes: Option<String>,

    #[arg(
        long,
        value_name = "BYTES",
        help = "UDP socket send buffer size parsed for compatibility (env: UDP_SOCKET_SNDBUF_BYTES)."
    )]
    udp_socket_sndbuf_bytes: Option<String>,
}

impl Cli {
    fn apply_to_env(self, env: &mut HashMap<String, String>) {
        fn insert_if_present(
            env: &mut HashMap<String, String>,
            name: &'static str,
            value: Option<String>,
        ) {
            if let Some(value) = value {
                env.insert(name.to_owned(), value);
            }
        }
        fn insert_flag(env: &mut HashMap<String, String>, name: &'static str, enabled: bool) {
            if enabled {
                env.insert(name.to_owned(), "true".to_owned());
            }
        }

        insert_if_present(env, "HTTP_LISTEN", self.http_listen);
        insert_if_present(env, "PROXY_LISTEN_HOST", self.proxy_listen_host);
        insert_if_present(env, "PORT_RANGE", self.port_range);
        insert_if_present(env, "AUTH_TOKEN", self.auth_token);
        insert_if_present(env, "SQLITE_PATH", self.sqlite_path);
        insert_if_present(
            env,
            "RUNTIME_APPLY_TIMEOUT_MS",
            self.runtime_apply_timeout_ms,
        );
        insert_if_present(
            env,
            "RESTORE_SWEEP_TIMEOUT_MS",
            self.restore_sweep_timeout_ms,
        );
        insert_flag(
            env,
            "TCP_SESSION_MODEL_ENABLED",
            self.tcp_session_model_enabled,
        );
        insert_if_present(
            env,
            "TCP_SESSION_MODEL_WORKERS",
            self.tcp_session_model_workers,
        );
        insert_flag(
            env,
            "TCP_SESSION_MODEL_ACCEPT_BALANCED",
            self.tcp_session_model_accept_balanced,
        );
        insert_flag(
            env,
            "TCP_SESSION_MODEL_SHARDED_ACCEPT",
            self.tcp_session_model_sharded_accept,
        );
        insert_if_present(
            env,
            "TCP_SESSION_MODEL_MAX_ACTIVE",
            self.tcp_session_model_max_active,
        );
        insert_if_present(env, "UDP_SESSION_WORKERS", self.udp_session_workers);
        insert_flag(env, "UDP_IO_URING_ENABLED", self.udp_io_uring_enabled);
        insert_flag(env, "UDP_GRO_ENABLED", self.udp_gro_enabled);
        insert_flag(
            env,
            "UDP_DATAPLANE_REDESIGN_ENABLED",
            self.udp_dataplane_redesign_enabled,
        );
        insert_flag(env, "UDP_FAST_PATH_ENABLED", self.udp_fast_path_enabled);
        insert_if_present(
            env,
            "UDP_FAST_PATH_SEGMENT_SIZE",
            self.udp_fast_path_segment_size,
        );
        insert_if_present(env, "UDP_FAST_PATH_GSO_BURST", self.udp_fast_path_gso_burst);
        insert_if_present(env, "UDP_SOCKET_RCVBUF_BYTES", self.udp_socket_rcvbuf_bytes);
        insert_if_present(env, "UDP_SOCKET_SNDBUF_BYTES", self.udp_socket_sndbuf_bytes);
    }
}

fn config_from_cli_and_env(
    cli: Cli,
    mut env: HashMap<String, String>,
) -> Result<Config, relayd::config::ConfigError> {
    cli.apply_to_env(&mut env);
    Config::from_env_map(&env)
}

async fn run_with_config(config: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let host: IpAddr = config
        .http_listen_host
        .parse()
        .map_err(|_| relayd::config::ConfigError::InvalidHttpListen)?;
    let addr = SocketAddr::new(host, config.http_listen_port);
    let listener = TcpListener::bind(addr).await?;
    run_with_listener(config, listener).await
}

async fn run_with_listener(
    config: Config,
    listener: TcpListener,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(
        http_listen = %listener.local_addr()?,
        proxy_listen_host = %config.proxy_listen_host,
        port_range_start = config.port_range.start,
        port_range_end = config.port_range.end,
        sqlite_path = %config.db_path,
        "starting relayd"
    );
    let metrics = Arc::new(Metrics::default());
    let repo = Repository::open(&config.db_path).await?;
    let runtime = RealRuntime::new(RealRuntimeConfig::with_bind_host(
        config.proxy_listen_host.clone(),
        metrics.clone(),
    ));
    let service = Arc::new(Service::new(
        repo,
        runtime,
        config.port_range,
        config.runtime_apply_timeout_ms,
    ));
    service.restore_all(config.restore_sweep_timeout_ms).await?;
    info!("restored persisted relay allocations");
    let state = AppState::new(service, metrics, config.auth_token);
    serve_listener(listener, state).await
}

async fn serve_listener(
    listener: TcpListener,
    state: AppState<RealRuntime>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    serve_listener_until_shutdown(listener, state, shutdown_signal()).await
}

async fn serve_listener_until_shutdown(
    listener: TcpListener,
    state: AppState<RealRuntime>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "failed to listen for shutdown signal");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn cli_help_documents_runtime_options() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("relayd starts an authenticated HTTP control plane"));
        assert!(help.contains("--http-listen <ADDR>"));
        assert!(help.contains("env: HTTP_LISTEN"));
        assert!(help.contains("--proxy-listen-host <HOST>"));
        assert!(help.contains("env: PROXY_LISTEN_HOST"));
        assert!(help.contains("UDP forwarding session source bind host"));
        assert!(help.contains("--port-range <START-END>"));
        assert!(help.contains("--auth-token <TOKEN>"));
        assert!(help.contains("--sqlite-path <PATH>"));
        assert!(help.contains("--runtime-apply-timeout-ms <MILLISECONDS>"));
        assert!(help.contains("--udp-socket-rcvbuf-bytes <BYTES>"));
    }

    #[test]
    fn cli_options_override_environment_config() {
        let cli = Cli::try_parse_from([
            "relayd",
            "--http-listen",
            "127.0.0.1:19080",
            "--proxy-listen-host",
            "127.0.0.1",
            "--port-range",
            "20000-20005",
            "--auth-token",
            "cli-token",
            "--sqlite-path",
            "cli.sqlite3",
            "--runtime-apply-timeout-ms",
            "1500",
            "--tcp-session-model-enabled",
            "--udp-socket-rcvbuf-bytes",
            "4096",
        ])
        .unwrap();
        let env = HashMap::from([
            ("HTTP_LISTEN".to_owned(), ":8080".to_owned()),
            ("PROXY_LISTEN_HOST".to_owned(), "0.0.0.0".to_owned()),
            ("PORT_RANGE".to_owned(), "10000-10010".to_owned()),
            ("AUTH_TOKEN".to_owned(), "env-token".to_owned()),
            ("SQLITE_PATH".to_owned(), "env.sqlite3".to_owned()),
        ]);

        let config = config_from_cli_and_env(cli, env).unwrap();

        assert_eq!(config.http_listen_host, "127.0.0.1");
        assert_eq!(config.http_listen_port, 19080);
        assert_eq!(config.proxy_listen_host, "127.0.0.1");
        assert_eq!(config.port_range.start, 20000);
        assert_eq!(config.port_range.end, 20005);
        assert_eq!(config.auth_token, "cli-token");
        assert_eq!(config.db_path, "cli.sqlite3");
        assert_eq!(config.runtime_apply_timeout_ms, 1500);
        assert!(config.tcp_session_model_enabled);
        assert_eq!(config.udp_socket_recv_buffer_bytes, 4096);
    }

    #[tokio::test]
    async fn startup_config_requires_auth_token() {
        let env = HashMap::from([
            ("HTTP_LISTEN".to_owned(), ":8080".to_owned()),
            ("PORT_RANGE".to_owned(), "20000-20010".to_owned()),
        ]);
        let error = Config::from_env_map(&env).unwrap_err();
        assert_eq!(error, relayd::config::ConfigError::MissingEnv("AUTH_TOKEN"));
    }

    #[test]
    fn run_with_config_accepts_parsed_ipv6_http_listen() {
        let env = HashMap::from([
            ("HTTP_LISTEN".to_owned(), "[::1]:8080".to_owned()),
            ("PORT_RANGE".to_owned(), "20000-20010".to_owned()),
            ("AUTH_TOKEN".to_owned(), "secret-token".to_owned()),
        ]);
        let config = Config::from_env_map(&env).unwrap();
        let host: IpAddr = config.http_listen_host.parse().unwrap();
        let addr = SocketAddr::new(host, config.http_listen_port);
        assert_eq!(addr.to_string(), "[::1]:8080");
    }

    #[tokio::test]
    async fn serve_listener_exits_after_shutdown_signal() {
        let parent = std::env::current_dir()
            .unwrap()
            .join("target/relayd-test-dbs");
        std::fs::create_dir_all(&parent).unwrap();
        let dir = tempfile::tempdir_in(parent).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let env = HashMap::from([
            ("HTTP_LISTEN".to_owned(), "127.0.0.1:18080".to_owned()),
            ("PORT_RANGE".to_owned(), "24100-24110".to_owned()),
            ("AUTH_TOKEN".to_owned(), "secret-token".to_owned()),
            (
                "SQLITE_PATH".to_owned(),
                dir.path().join("relayd.sqlite").display().to_string(),
            ),
            ("RUNTIME_APPLY_TIMEOUT_MS".to_owned(), "500".to_owned()),
            ("RESTORE_SWEEP_TIMEOUT_MS".to_owned(), "500".to_owned()),
        ]);
        let config = Config::from_env_map(&env).unwrap();
        let metrics = Arc::new(Metrics::default());
        let repo = Repository::open(&config.db_path).await.unwrap();
        let runtime = RealRuntime::new(RealRuntimeConfig::with_bind_host(
            config.proxy_listen_host.clone(),
            metrics.clone(),
        ));
        let service = Arc::new(Service::new(
            repo,
            runtime,
            config.port_range,
            config.runtime_apply_timeout_ms,
        ));
        let state = AppState::new(service, metrics, config.auth_token);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_listener_until_shutdown(listener, state, async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        shutdown_tx.send(()).unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .expect("server should exit promptly after shutdown signal")
            .expect("server task should not panic")
            .expect("server shutdown should succeed");
    }

    #[tokio::test]
    async fn startup_path_serves_authenticated_metrics_from_temp_sqlite() {
        let parent = std::env::current_dir()
            .unwrap()
            .join("target/relayd-test-dbs");
        std::fs::create_dir_all(&parent).unwrap();
        let dir = tempfile::tempdir_in(parent).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let env = HashMap::from([
            (
                "HTTP_LISTEN".to_owned(),
                format!("127.0.0.1:{}", addr.port()),
            ),
            ("PORT_RANGE".to_owned(), "24000-24010".to_owned()),
            ("AUTH_TOKEN".to_owned(), "secret-token".to_owned()),
            (
                "SQLITE_PATH".to_owned(),
                dir.path().join("relayd.sqlite").display().to_string(),
            ),
            ("RUNTIME_APPLY_TIMEOUT_MS".to_owned(), "500".to_owned()),
            ("RESTORE_SWEEP_TIMEOUT_MS".to_owned(), "500".to_owned()),
        ]);
        let config = Config::from_env_map(&env).unwrap();
        let server =
            tokio::spawn(async move { run_with_listener(config, listener).await.unwrap() });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer secret-token\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut body = String::new();
        stream.read_to_string(&mut body).await.unwrap();
        assert!(body.starts_with("HTTP/1.1 200 OK"));
        assert!(body.contains("allocations_total"));

        server.abort();
    }
}
