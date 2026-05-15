use relayd::config::Config;
use relayd::http::control_plane::{AppState, router};
use relayd::metrics::Metrics;
use relayd::runtime::real::{RealRuntime, RealRuntimeConfig};
use relayd::service::allocation_service::Service;
use relayd::storage::sqlite::Repository;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("relayd: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = config_from_env()?;
    run_with_config(config).await
}

fn config_from_env() -> Result<Config, relayd::config::ConfigError> {
    let env: HashMap<String, String> = std::env::vars().collect();
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
    let metrics = Arc::new(Metrics::default());
    let repo = Repository::open(&config.db_path).await?;
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let service = Arc::new(Service::new(
        repo,
        runtime,
        config.port_range,
        config.runtime_apply_timeout_ms,
    ));
    service.restore_all(config.restore_sweep_timeout_ms).await?;
    let state = AppState::new(service, metrics, config.auth_token);
    serve_listener(listener, state).await
}

async fn serve_listener(
    listener: TcpListener,
    state: AppState<RealRuntime>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    axum::serve(listener, router(state)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
