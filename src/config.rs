use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpListen {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortRange {
    pub start: u16,
    pub end: u16,
}

impl PortRange {
    pub fn contains(self, port: u16) -> bool {
        port >= self.start && port <= self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub http_listen_host: String,
    pub http_listen_port: u16,
    pub proxy_listen_host: String,
    pub port_range: PortRange,
    pub auth_token: String,
    pub tcp_session_model_enabled: bool,
    pub tcp_session_model_workers: u32,
    pub tcp_session_model_accept_balanced: bool,
    pub tcp_session_model_sharded_accept: bool,
    pub tcp_session_model_max_active: u32,
    pub udp_session_workers: u32,
    pub udp_io_uring_enabled: bool,
    pub udp_gro_enabled: bool,
    pub udp_dataplane_redesign_enabled: bool,
    pub udp_fast_path_enabled: bool,
    pub udp_fast_path_segment_size: u32,
    pub udp_fast_path_gso_burst: u32,
    pub udp_max_sessions: usize,
    pub udp_socket_recv_buffer_bytes: u32,
    pub udp_socket_send_buffer_bytes: u32,
    pub runtime_apply_timeout_ms: u32,
    pub restore_sweep_timeout_ms: u32,
    pub db_path: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("missing environment variable: {0}")]
    MissingEnv(&'static str),
    #[error("required environment variable AUTH_TOKEN must not be empty")]
    EmptyAuthToken,
    #[error("invalid HTTP_LISTEN")]
    InvalidHttpListen,
    #[error("invalid PORT_RANGE")]
    InvalidPortRange,
    #[error("invalid port")]
    InvalidPort,
    #[error("invalid host")]
    InvalidHost,
    #[error("invalid integer for {0}")]
    InvalidInteger(&'static str),
}

impl Config {
    pub fn from_env_map(env: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let listen = parse_http_listen(
            env.get("HTTP_LISTEN")
                .map(String::as_str)
                .unwrap_or(":8080"),
        )?;
        let proxy_listen_host = env
            .get("PROXY_LISTEN_HOST")
            .map(String::as_str)
            .unwrap_or("0.0.0.0");
        parse_ip_literal(proxy_listen_host)?;
        let port_range = parse_port_range(
            env.get("PORT_RANGE")
                .map(String::as_str)
                .unwrap_or("10000-30000"),
        )?;
        let auth_token = env
            .get("AUTH_TOKEN")
            .ok_or(ConfigError::MissingEnv("AUTH_TOKEN"))?
            .clone();
        if auth_token.is_empty() {
            return Err(ConfigError::EmptyAuthToken);
        }
        Ok(Self {
            http_listen_host: listen.host,
            http_listen_port: listen.port,
            proxy_listen_host: proxy_listen_host.to_owned(),
            port_range,
            auth_token,
            tcp_session_model_enabled: env_bool(env, "TCP_SESSION_MODEL_ENABLED"),
            tcp_session_model_workers: env_u32(env, "TCP_SESSION_MODEL_WORKERS", 0)?,
            tcp_session_model_accept_balanced: env_bool(env, "TCP_SESSION_MODEL_ACCEPT_BALANCED"),
            tcp_session_model_sharded_accept: env_bool(env, "TCP_SESSION_MODEL_SHARDED_ACCEPT"),
            tcp_session_model_max_active: env_u32(env, "TCP_SESSION_MODEL_MAX_ACTIVE", 256)?,
            udp_session_workers: env_u32(env, "UDP_SESSION_WORKERS", 0)?,
            udp_io_uring_enabled: env_bool(env, "UDP_IO_URING_ENABLED"),
            udp_gro_enabled: env_bool(env, "UDP_GRO_ENABLED"),
            udp_dataplane_redesign_enabled: env_bool(env, "UDP_DATAPLANE_REDESIGN_ENABLED"),
            udp_fast_path_enabled: env_bool(env, "UDP_FAST_PATH_ENABLED"),
            udp_fast_path_segment_size: env_u32(env, "UDP_FAST_PATH_SEGMENT_SIZE", 1472)?,
            udp_fast_path_gso_burst: env_u32(env, "UDP_FAST_PATH_GSO_BURST", 16)?,
            udp_max_sessions: env_nonzero_usize(env, "UDP_MAX_SESSIONS", 65_536)?,
            udp_socket_recv_buffer_bytes: env_u32(env, "UDP_SOCKET_RCVBUF_BYTES", 8 * 1024 * 1024)?,
            udp_socket_send_buffer_bytes: env_u32(env, "UDP_SOCKET_SNDBUF_BYTES", 8 * 1024 * 1024)?,
            runtime_apply_timeout_ms: env_u32(env, "RUNTIME_APPLY_TIMEOUT_MS", 2000)?,
            restore_sweep_timeout_ms: env_u32(env, "RESTORE_SWEEP_TIMEOUT_MS", 30000)?,
            db_path: env
                .get("SQLITE_PATH")
                .cloned()
                .unwrap_or_else(|| "relayd.sqlite3".to_owned()),
        })
    }
}

pub fn parse_http_listen(raw: &str) -> Result<HttpListen, ConfigError> {
    if raw.is_empty() {
        return Err(ConfigError::InvalidHttpListen);
    }
    if let Some(port) = raw.strip_prefix(':') {
        return Ok(HttpListen {
            host: "127.0.0.1".to_owned(),
            port: parse_port(port)?,
        });
    }
    if raw.starts_with('[') {
        let end = raw.find(']').ok_or(ConfigError::InvalidHttpListen)?;
        if raw.as_bytes().get(end + 1) != Some(&b':') {
            return Err(ConfigError::InvalidHttpListen);
        }
        let host = &raw[1..end];
        parse_ip_literal(host)?;
        return Ok(HttpListen {
            host: host.to_owned(),
            port: parse_port(&raw[end + 2..])?,
        });
    }
    let idx = raw.rfind(':').ok_or(ConfigError::InvalidHttpListen)?;
    let host = &raw[..idx];
    if host.is_empty() {
        return Err(ConfigError::InvalidHttpListen);
    }
    parse_ip_literal(host)?;
    Ok(HttpListen {
        host: host.to_owned(),
        port: parse_port(&raw[idx + 1..])?,
    })
}

pub fn parse_port_range(raw: &str) -> Result<PortRange, ConfigError> {
    let (start, end) = raw.split_once('-').ok_or(ConfigError::InvalidPortRange)?;
    let start = parse_port(start).map_err(|_| ConfigError::InvalidPortRange)?;
    let end = parse_port(end).map_err(|_| ConfigError::InvalidPortRange)?;
    if start > end {
        return Err(ConfigError::InvalidPortRange);
    }
    Ok(PortRange { start, end })
}

pub fn parse_port(raw: &str) -> Result<u16, ConfigError> {
    if raw.is_empty() {
        return Err(ConfigError::InvalidPort);
    }
    let parsed = raw.parse::<u32>().map_err(|_| ConfigError::InvalidPort)?;
    if parsed == 0 || parsed > u16::MAX as u32 {
        return Err(ConfigError::InvalidPort);
    }
    Ok(parsed as u16)
}

pub fn parse_ip_literal(host: &str) -> Result<IpAddr, ConfigError> {
    IpAddr::from_str(host).map_err(|_| ConfigError::InvalidHost)
}

fn env_bool(env: &HashMap<String, String>, name: &'static str) -> bool {
    env.get(name).is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn env_u32(
    env: &HashMap<String, String>,
    name: &'static str,
    default_value: u32,
) -> Result<u32, ConfigError> {
    match env.get(name) {
        Some(value) => value
            .parse::<u32>()
            .map_err(|_| ConfigError::InvalidInteger(name)),
        None => Ok(default_value),
    }
}

fn env_nonzero_usize(
    env: &HashMap<String, String>,
    name: &'static str,
    default_value: usize,
) -> Result<usize, ConfigError> {
    match env.get(name) {
        Some(value) => value
            .parse::<usize>()
            .ok()
            .filter(|parsed| *parsed > 0)
            .ok_or(ConfigError::InvalidInteger(name)),
        None => Ok(default_value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_with_token() -> HashMap<String, String> {
        HashMap::from([("AUTH_TOKEN".to_owned(), "secret".to_owned())])
    }

    #[test]
    fn parse_http_listen_defaults_colon_form_to_loopback() {
        assert_eq!(
            parse_http_listen(":8080").unwrap(),
            HttpListen {
                host: "127.0.0.1".to_owned(),
                port: 8080
            }
        );
    }

    #[test]
    fn parse_http_listen_accepts_ip_literals_only() {
        assert_eq!(
            parse_http_listen("127.0.0.1:8081").unwrap().host,
            "127.0.0.1"
        );
        assert_eq!(parse_http_listen("[::1]:8082").unwrap().host, "::1");
        assert!(matches!(
            parse_http_listen("localhost:8080"),
            Err(ConfigError::InvalidHost)
        ));
    }

    #[test]
    fn parse_port_range_accepts_ordered_inclusive_ranges() {
        let range = parse_port_range("10000-30000").unwrap();
        assert_eq!(
            range,
            PortRange {
                start: 10000,
                end: 30000
            }
        );
        assert!(range.contains(10000));
        assert!(range.contains(30000));
        assert!(!range.contains(9999));
        assert!(matches!(
            parse_port_range("30000-10000"),
            Err(ConfigError::InvalidPortRange)
        ));
    }

    #[test]
    fn parse_port_rejects_zero_empty_and_out_of_range() {
        assert!(matches!(parse_port(""), Err(ConfigError::InvalidPort)));
        assert!(matches!(parse_port("0"), Err(ConfigError::InvalidPort)));
        assert!(matches!(parse_port("65536"), Err(ConfigError::InvalidPort)));
    }

    #[test]
    fn config_from_env_map_applies_defaults_and_requires_auth_token() {
        assert!(matches!(
            Config::from_env_map(&HashMap::new()),
            Err(ConfigError::MissingEnv("AUTH_TOKEN"))
        ));
        let mut env = env_with_token();
        env.insert("TCP_SESSION_MODEL_ENABLED".to_owned(), "yes".to_owned());
        env.insert("RUNTIME_APPLY_TIMEOUT_MS".to_owned(), "1234".to_owned());
        let cfg = Config::from_env_map(&env).unwrap();
        assert_eq!(cfg.http_listen_host, "127.0.0.1");
        assert_eq!(cfg.http_listen_port, 8080);
        assert_eq!(cfg.proxy_listen_host, "0.0.0.0");
        assert_eq!(
            cfg.port_range,
            PortRange {
                start: 10000,
                end: 30000
            }
        );
        assert_eq!(cfg.db_path, "relayd.sqlite3");
        assert!(cfg.tcp_session_model_enabled);
        assert_eq!(cfg.runtime_apply_timeout_ms, 1234);
        assert_eq!(cfg.tcp_session_model_max_active, 256);
        assert_eq!(cfg.udp_fast_path_segment_size, 1472);
        assert_eq!(cfg.udp_socket_recv_buffer_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn config_from_env_map_defaults_udp_max_sessions_to_65536() {
        let cfg = Config::from_env_map(&env_with_token()).unwrap();
        assert_eq!(cfg.udp_max_sessions, 65_536);
    }

    #[test]
    fn config_from_env_map_parses_udp_max_sessions_override() {
        let mut env = env_with_token();
        env.insert("UDP_MAX_SESSIONS".to_owned(), "12345".to_owned());
        let cfg = Config::from_env_map(&env).unwrap();
        assert_eq!(cfg.udp_max_sessions, 12_345);
    }

    #[test]
    fn config_from_env_map_rejects_invalid_udp_max_sessions() {
        let mut env = env_with_token();
        env.insert("UDP_MAX_SESSIONS".to_owned(), "bad".to_owned());
        assert!(matches!(
            Config::from_env_map(&env),
            Err(ConfigError::InvalidInteger("UDP_MAX_SESSIONS"))
        ));

        let mut env = env_with_token();
        env.insert("UDP_MAX_SESSIONS".to_owned(), "0".to_owned());
        assert!(matches!(
            Config::from_env_map(&env),
            Err(ConfigError::InvalidInteger("UDP_MAX_SESSIONS"))
        ));
    }

    #[test]
    fn config_from_env_map_parses_proxy_listen_host() {
        let mut env = env_with_token();
        env.insert("PROXY_LISTEN_HOST".to_owned(), "127.0.0.1".to_owned());
        let cfg = Config::from_env_map(&env).unwrap();
        assert_eq!(cfg.proxy_listen_host, "127.0.0.1");

        env.insert("PROXY_LISTEN_HOST".to_owned(), "::1".to_owned());
        let cfg = Config::from_env_map(&env).unwrap();
        assert_eq!(cfg.proxy_listen_host, "::1");

        env.insert("PROXY_LISTEN_HOST".to_owned(), "localhost".to_owned());
        assert!(matches!(
            Config::from_env_map(&env),
            Err(ConfigError::InvalidHost)
        ));
    }

    #[test]
    fn config_from_env_map_parses_custom_tcp_session_model_max_active() {
        let mut env = env_with_token();
        env.insert("TCP_SESSION_MODEL_MAX_ACTIVE".to_owned(), "17".to_owned());
        let cfg = Config::from_env_map(&env).unwrap();
        assert_eq!(cfg.tcp_session_model_max_active, 17);
    }

    #[test]
    fn config_surface_does_not_expose_tcp_splice_activation_flags() {
        let mut env = env_with_token();
        env.insert("TCP_SPLICE_ENABLED".to_owned(), "true".to_owned());
        env.insert("FORCE_TCP_COPY_FALLBACK".to_owned(), "true".to_owned());
        Config::from_env_map(&env).expect("obsolete splice flags should be ignored");

        let cfg = Config {
            http_listen_host: "127.0.0.1".to_owned(),
            http_listen_port: 8080,
            proxy_listen_host: "0.0.0.0".to_owned(),
            port_range: PortRange {
                start: 10000,
                end: 30000,
            },
            auth_token: "secret".to_owned(),
            tcp_session_model_enabled: false,
            tcp_session_model_workers: 0,
            tcp_session_model_accept_balanced: false,
            tcp_session_model_sharded_accept: false,
            tcp_session_model_max_active: 256,
            udp_session_workers: 0,
            udp_io_uring_enabled: false,
            udp_gro_enabled: false,
            udp_dataplane_redesign_enabled: false,
            udp_fast_path_enabled: false,
            udp_fast_path_segment_size: 1472,
            udp_fast_path_gso_burst: 16,
            udp_max_sessions: 65_536,
            udp_socket_recv_buffer_bytes: 8 * 1024 * 1024,
            udp_socket_send_buffer_bytes: 8 * 1024 * 1024,
            runtime_apply_timeout_ms: 2000,
            restore_sweep_timeout_ms: 30000,
            db_path: "relayd.sqlite3".to_owned(),
        };

        assert_eq!(cfg.auth_token, "secret");
    }

    #[test]
    fn config_from_env_map_rejects_empty_auth_token_and_bad_integer() {
        assert!(matches!(
            Config::from_env_map(&HashMap::from([("AUTH_TOKEN".to_owned(), "".to_owned())])),
            Err(ConfigError::EmptyAuthToken)
        ));
        let mut env = env_with_token();
        env.insert("UDP_SESSION_WORKERS".to_owned(), "nope".to_owned());
        assert!(matches!(
            Config::from_env_map(&env),
            Err(ConfigError::InvalidInteger("UDP_SESSION_WORKERS"))
        ));

        let mut env = env_with_token();
        env.insert("TCP_SESSION_MODEL_MAX_ACTIVE".to_owned(), "bad".to_owned());
        assert!(matches!(
            Config::from_env_map(&env),
            Err(ConfigError::InvalidInteger("TCP_SESSION_MODEL_MAX_ACTIVE"))
        ));
    }
}
