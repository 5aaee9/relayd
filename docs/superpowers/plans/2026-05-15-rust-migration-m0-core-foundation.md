# Rust Migration M0 Core Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Rust crate that ports relayd's configuration parsing, domain model, UUID generation, and SQLite repository behavior without changing the existing Zig runtime.

**Architecture:** Introduce a library-first Rust crate with focused modules: `config` for environment and parser semantics, `model` for API/domain structs, `storage::sqlite` for persistence, and `uuid` for UUIDv7 generation. Tests mirror the existing Zig unit tests and repository migration behavior so later milestones can build service, HTTP, and runtime layers on stable Rust foundations.

**Tech Stack:** Rust 1.95, Cargo, SeaORM 1.1.x, SQLx 0.8.x with SQLite/Tokio runtime, `serde`, `thiserror`, `uuid` with v7 support, standard `std::net` IP parsing, and `cargo test`.

---

## File Structure

- Create: `Cargo.toml` — Rust package metadata, binary/lib targets, and dependencies.
- Create: `src/lib.rs` — public module declarations for the Rust implementation.
- Create: `src/bin/relayd.rs` — placeholder binary entrypoint for M0 that prints a clear scaffold message.
- Create: `src/config.rs` — config structs and parser helpers matching Zig `src/config.zig` default semantics.
- Create: `src/model.rs` — domain enums/structs matching Zig `src/model/allocation.zig`.
- Create: `src/uuid.rs` — UUIDv7 helper wrapper.
- Create: `src/storage/mod.rs` — storage module declaration.
- Create: `src/storage/sqlite.rs` — SQLx-backed SQLite repository with SeaORM entities/ActiveModels for CRUD/list behavior, schema/migration setup, and async tests.
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md` — append M0 implementation status after verification.
- Existing: `docs/superpowers/specs/2026-05-15-rust-migration-m0-core-foundation.md` — focused M0 spec derived from the milestone ledger and approved before implementation.

## M0 acceptance checklist

- [ ] `cargo test` compiles the Rust crate and passes all M0 tests.
- [ ] `Protocol::parse` accepts `tcp`, `udp`, `both` case-insensitively and rejects unknown strings.
- [ ] `parse_http_listen(":8080")` returns host `127.0.0.1` and port `8080`.
- [ ] IPv4 and IPv6 HTTP listen forms parse only IP literals; hostname forms are rejected.
- [ ] `parse_port_range("10000-30000")` accepts inclusive ordered ranges and rejects reversed or invalid ports.
- [ ] `Config::from_env_map` requires non-empty `AUTH_TOKEN`, applies Zig-compatible defaults including `TCP_SESSION_MODEL_MAX_ACTIVE = 256`, and parses numeric options.
- [ ] SQLite repository opens with WAL and 5000 ms busy timeout, creates `allocations`/`bindings`, and can self-check with direct tests for WAL and busy timeout PRAGMAs.
- [ ] Repository inserts allocations and bindings, directly verifies legacy binding columns are updated on binding writes, directly verifies they are cleared on binding deletes, and directly verifies deleting an allocation removes its binding.
- [ ] Repository migrates legacy allocation binding columns into the `bindings` table at open.
- [ ] Repository lists allocations ordered by protocol then port (`both`, `tcp`, `udp`) and hydrates binding data through the left join.
- [ ] M0 docs are updated before commit.

## Task 1: Create Rust crate skeleton and domain model

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/bin/relayd.rs`
- Create: `src/model.rs`
- Create: `src/uuid.rs`

- [ ] **Step 1: Write Cargo manifest**

Create `Cargo.toml` and expect Cargo to generate and commit `Cargo.lock` for this binary crate:

```toml
[package]
name = "relayd"
version = "0.1.0"
edition = "2024"
rust-version = "1.95"

[lib]
name = "relayd"
path = "src/lib.rs"

[[bin]]
name = "relayd"
path = "src/bin/relayd.rs"

[dependencies]
sea-orm = { version = "1.1", default-features = false, features = ["macros", "sqlx-sqlite", "runtime-tokio-rustls"] }
serde = { version = "1", features = ["derive"] }
sqlx = { version = "0.8", default-features = false, features = ["sqlite", "runtime-tokio-rustls"] }
thiserror = "2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
uuid = { version = "1", features = ["v7", "serde"] }

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 2: Create module root**

Create `src/lib.rs`:

```rust
pub mod config;
pub mod model;
pub mod storage;
pub mod uuid;
```

- [ ] **Step 3: Create placeholder binary**

Create `src/bin/relayd.rs`:

```rust
fn main() {
    eprintln!("relayd Rust migration scaffold: runtime starts in a later milestone");
}
```

- [ ] **Step 4: Implement model types and parser tests**

Create `src/model.rs`:

```rust
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Udp,
    Both,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
            Self::Both => "both",
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Protocol {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "tcp" => Ok(Self::Tcp),
            "udp" => Ok(Self::Udp),
            "both" => Ok(Self::Both),
            _ => Err(ModelError::InvalidProtocol(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStatus {
    Active,
    RejectingNoHost,
    DegradedBindFailed,
    DegradedApplyFailed,
    DegradedCloseFailed,
}

impl RuntimeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::RejectingNoHost => "rejecting_no_host",
            Self::DegradedBindFailed => "degraded_bind_failed",
            Self::DegradedApplyFailed => "degraded_apply_failed",
            Self::DegradedCloseFailed => "degraded_close_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    BindFailed,
    ApplyFailed,
    CloseFailed,
    RestoreFailed,
    InvalidInput,
}

impl ErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BindFailed => "bind_failed",
            Self::ApplyFailed => "apply_failed",
            Self::CloseFailed => "close_failed",
            Self::RestoreFailed => "restore_failed",
            Self::InvalidInput => "invalid_input",
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ModelError {
    #[error("invalid protocol: {0}")]
    InvalidProtocol(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    pub id: String,
    pub protocol: Protocol,
    pub port: u16,
    pub target_port: Option<u16>,
    pub host: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub allocation_id: String,
    pub target_port: u16,
    pub host: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AllocationResource {
    pub id: String,
    pub protocol: Protocol,
    pub port: u16,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BindingView {
    pub allocation_id: String,
    pub host: Option<String>,
    pub target_port: u16,
    pub effective_target_port: Option<u16>,
    pub effective_host: Option<String>,
    pub runtime_status: RuntimeStatus,
    pub error_kind: Option<ErrorKind>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AllocationView {
    pub id: String,
    pub protocol: Protocol,
    pub port: u16,
    pub target_port: Option<u16>,
    pub host: Option<String>,
    pub effective_target_port: Option<u16>,
    pub effective_host: Option<String>,
    pub host_configured: bool,
    pub runtime_status: RuntimeStatus,
    pub error_kind: Option<ErrorKind>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

pub fn is_host_configured(host: Option<&str>) -> bool {
    host.is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_parser_accepts_current_values_case_insensitively() {
        assert_eq!("tcp".parse::<Protocol>().unwrap(), Protocol::Tcp);
        assert_eq!("UDP".parse::<Protocol>().unwrap(), Protocol::Udp);
        assert_eq!("Both".parse::<Protocol>().unwrap(), Protocol::Both);
        assert!("http".parse::<Protocol>().is_err());
    }

    #[test]
    fn host_configured_helper_matches_zig_semantics() {
        assert!(!is_host_configured(None));
        assert!(!is_host_configured(Some("")));
        assert!(is_host_configured(Some("127.0.0.1")));
    }
}
```

- [ ] **Step 5: Implement UUIDv7 wrapper and test**

Create `src/uuid.rs`:

```rust
pub fn generate_uuid_v7() -> String {
    ::uuid::Uuid::now_v7().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_v7_format_has_version_seven() {
        let id = generate_uuid_v7();
        assert_eq!(id.len(), 36);
        assert_eq!(id.as_bytes()[14], b'7');
        assert_eq!(id.as_bytes()[8], b'-');
        assert_eq!(id.as_bytes()[13], b'-');
        assert_eq!(id.as_bytes()[18], b'-');
        assert_eq!(id.as_bytes()[23], b'-');
    }
}
```

- [ ] **Step 6: Run tests for expected compile failures before remaining modules**

Run: `cargo test --lib`

Expected: fails because `config` and `storage` modules are declared but not created yet. Continue to Task 2.

## Task 2: Implement configuration parsing

**Files:**
- Create: `src/config.rs`

- [ ] **Step 1: Add config module implementation**

Create `src/config.rs`:

```rust
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
    pub port_range: PortRange,
    pub auth_token: String,
    pub tcp_session_model_enabled: bool,
    pub tcp_session_model_workers: u32,
    pub tcp_session_model_accept_balanced: bool,
    pub tcp_session_model_sharded_accept: bool,
    pub tcp_session_model_max_active: u32,
    pub tcp_splice_enabled: bool,
    pub force_tcp_copy_fallback: bool,
    pub udp_session_workers: u32,
    pub udp_io_uring_enabled: bool,
    pub udp_gro_enabled: bool,
    pub udp_dataplane_redesign_enabled: bool,
    pub udp_fast_path_enabled: bool,
    pub udp_fast_path_segment_size: u32,
    pub udp_fast_path_gso_burst: u32,
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
        let listen = parse_http_listen(env.get("HTTP_LISTEN").map(String::as_str).unwrap_or(":8080"))?;
        let port_range = parse_port_range(env.get("PORT_RANGE").map(String::as_str).unwrap_or("10000-30000"))?;
        let auth_token = env.get("AUTH_TOKEN").ok_or(ConfigError::MissingEnv("AUTH_TOKEN"))?.clone();
        if auth_token.is_empty() {
            return Err(ConfigError::EmptyAuthToken);
        }
        Ok(Self {
            http_listen_host: listen.host,
            http_listen_port: listen.port,
            port_range,
            auth_token,
            tcp_session_model_enabled: env_bool(env, "TCP_SESSION_MODEL_ENABLED"),
            tcp_session_model_workers: env_u32(env, "TCP_SESSION_MODEL_WORKERS", 0)?,
            tcp_session_model_accept_balanced: env_bool(env, "TCP_SESSION_MODEL_ACCEPT_BALANCED"),
            tcp_session_model_sharded_accept: env_bool(env, "TCP_SESSION_MODEL_SHARDED_ACCEPT"),
            tcp_session_model_max_active: env_u32(env, "TCP_SESSION_MODEL_MAX_ACTIVE", 256)?,
            tcp_splice_enabled: env_bool(env, "TCP_SPLICE_ENABLED"),
            force_tcp_copy_fallback: env_bool(env, "FORCE_TCP_COPY_FALLBACK"),
            udp_session_workers: env_u32(env, "UDP_SESSION_WORKERS", 0)?,
            udp_io_uring_enabled: env_bool(env, "UDP_IO_URING_ENABLED"),
            udp_gro_enabled: env_bool(env, "UDP_GRO_ENABLED"),
            udp_dataplane_redesign_enabled: env_bool(env, "UDP_DATAPLANE_REDESIGN_ENABLED"),
            udp_fast_path_enabled: env_bool(env, "UDP_FAST_PATH_ENABLED"),
            udp_fast_path_segment_size: env_u32(env, "UDP_FAST_PATH_SEGMENT_SIZE", 1472)?,
            udp_fast_path_gso_burst: env_u32(env, "UDP_FAST_PATH_GSO_BURST", 16)?,
            udp_socket_recv_buffer_bytes: env_u32(env, "UDP_SOCKET_RCVBUF_BYTES", 8 * 1024 * 1024)?,
            udp_socket_send_buffer_bytes: env_u32(env, "UDP_SOCKET_SNDBUF_BYTES", 8 * 1024 * 1024)?,
            runtime_apply_timeout_ms: env_u32(env, "RUNTIME_APPLY_TIMEOUT_MS", 2000)?,
            restore_sweep_timeout_ms: env_u32(env, "RESTORE_SWEEP_TIMEOUT_MS", 30000)?,
            db_path: env.get("SQLITE_PATH").cloned().unwrap_or_else(|| "relayd.sqlite3".to_owned()),
        })
    }
}

pub fn parse_http_listen(raw: &str) -> Result<HttpListen, ConfigError> {
    if raw.is_empty() {
        return Err(ConfigError::InvalidHttpListen);
    }
    if let Some(port) = raw.strip_prefix(':') {
        return Ok(HttpListen { host: "127.0.0.1".to_owned(), port: parse_port(port)? });
    }
    if raw.starts_with('[') {
        let end = raw.find(']').ok_or(ConfigError::InvalidHttpListen)?;
        if raw.as_bytes().get(end + 1) != Some(&b':') {
            return Err(ConfigError::InvalidHttpListen);
        }
        let host = &raw[1..end];
        parse_ip_literal(host)?;
        return Ok(HttpListen { host: host.to_owned(), port: parse_port(&raw[end + 2..])? });
    }
    let idx = raw.rfind(':').ok_or(ConfigError::InvalidHttpListen)?;
    let host = &raw[..idx];
    if host.is_empty() {
        return Err(ConfigError::InvalidHttpListen);
    }
    parse_ip_literal(host)?;
    Ok(HttpListen { host: host.to_owned(), port: parse_port(&raw[idx + 1..])? })
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
    env.get(name).is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

fn env_u32(env: &HashMap<String, String>, name: &'static str, default_value: u32) -> Result<u32, ConfigError> {
    match env.get(name) {
        Some(value) => value.parse::<u32>().map_err(|_| ConfigError::InvalidInteger(name)),
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
        assert_eq!(parse_http_listen(":8080").unwrap(), HttpListen { host: "127.0.0.1".to_owned(), port: 8080 });
    }

    #[test]
    fn parse_http_listen_accepts_ip_literals_only() {
        assert_eq!(parse_http_listen("127.0.0.1:8081").unwrap().host, "127.0.0.1");
        assert_eq!(parse_http_listen("[::1]:8082").unwrap().host, "::1");
        assert!(matches!(parse_http_listen("localhost:8080"), Err(ConfigError::InvalidHost)));
    }

    #[test]
    fn parse_port_range_accepts_ordered_inclusive_ranges() {
        let range = parse_port_range("10000-30000").unwrap();
        assert_eq!(range, PortRange { start: 10000, end: 30000 });
        assert!(range.contains(10000));
        assert!(range.contains(30000));
        assert!(!range.contains(9999));
        assert!(matches!(parse_port_range("30000-10000"), Err(ConfigError::InvalidPortRange)));
    }

    #[test]
    fn parse_port_rejects_zero_empty_and_out_of_range() {
        assert!(matches!(parse_port(""), Err(ConfigError::InvalidPort)));
        assert!(matches!(parse_port("0"), Err(ConfigError::InvalidPort)));
        assert!(matches!(parse_port("65536"), Err(ConfigError::InvalidPort)));
    }

    #[test]
    fn config_from_env_map_applies_defaults_and_requires_auth_token() {
        assert!(matches!(Config::from_env_map(&HashMap::new()), Err(ConfigError::MissingEnv("AUTH_TOKEN"))));
        let mut env = env_with_token();
        env.insert("TCP_SESSION_MODEL_ENABLED".to_owned(), "yes".to_owned());
        env.insert("RUNTIME_APPLY_TIMEOUT_MS".to_owned(), "1234".to_owned());
        let cfg = Config::from_env_map(&env).unwrap();
        assert_eq!(cfg.http_listen_host, "127.0.0.1");
        assert_eq!(cfg.http_listen_port, 8080);
        assert_eq!(cfg.port_range, PortRange { start: 10000, end: 30000 });
        assert_eq!(cfg.db_path, "relayd.sqlite3");
        assert!(cfg.tcp_session_model_enabled);
        assert_eq!(cfg.runtime_apply_timeout_ms, 1234);
        assert_eq!(cfg.tcp_session_model_max_active, 256);
        assert_eq!(cfg.udp_fast_path_segment_size, 1472);
        assert_eq!(cfg.udp_socket_recv_buffer_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn config_from_env_map_rejects_empty_auth_token_and_bad_integer() {
        assert!(matches!(Config::from_env_map(&HashMap::from([("AUTH_TOKEN".to_owned(), "".to_owned())])), Err(ConfigError::EmptyAuthToken)));
        let mut env = env_with_token();
        env.insert("UDP_SESSION_WORKERS".to_owned(), "nope".to_owned());
        assert!(matches!(Config::from_env_map(&env), Err(ConfigError::InvalidInteger("UDP_SESSION_WORKERS"))));
    }
}
```

- [ ] Add a config test assertion for custom `TCP_SESSION_MODEL_MAX_ACTIVE=17` returning `tcp_session_model_max_active == 17`, and invalid `TCP_SESSION_MODEL_MAX_ACTIVE` returning `InvalidInteger("TCP_SESSION_MODEL_MAX_ACTIVE")`.

- [ ] **Step 2: Run config/model tests**

Run: `cargo test --lib`

Expected: still fails because `storage` module is declared but missing. Continue to Task 3.

## Task 3: Implement SQLite repository with SeaORM + SQLx

**Files:**
- Create: `src/storage/mod.rs`
- Create: `src/storage/sqlite.rs`
- Modify: `Cargo.toml` and `Cargo.lock` if Task 1 still contains the pre-R1 dependency set.

- [ ] **Step 1: Create storage module declaration**

Create `src/storage/mod.rs`:

```rust
pub mod sqlite;
```

- [ ] **Step 2: Update dependencies if needed**

Ensure `Cargo.toml` contains SeaORM + SQLx and does not contain `rusqlite`:

```toml
sea-orm = { version = "1.1", default-features = false, features = ["macros", "sqlx-sqlite", "runtime-tokio-rustls"] }
sqlx = { version = "0.8", default-features = false, features = ["sqlite", "runtime-tokio-rustls"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Run `cargo update` or `cargo test` to regenerate `Cargo.lock`. Confirm `Cargo.lock` has no `rusqlite` package entry.

- [ ] **Step 3: Implement SeaORM entity layout**

In `src/storage/sqlite.rs`, define internal entity modules with SeaORM derives:

- `allocation_entity` maps table `allocations` with fields `id: String` primary key, `protocol: String`, `port: i32`, `target_port: i32`, `host: Option<String>`, `created_at_ms: i64`, `updated_at_ms: i64`.
- `binding_entity` maps table `bindings` with fields `allocation_id: String` primary key, `target_port: i32`, `host: Option<String>`, `created_at_ms: i64`, `updated_at_ms: i64`.
- Both entities have empty `Relation` enums and `impl ActiveModelBehavior`.

Use repository helper functions to convert SeaORM models into public `crate::model::Allocation` and `crate::model::Binding`, including:

- parse protocol strings through `Protocol::from_str`;
- convert SQLite integer ports to `u16`;
- map allocation `target_port <= 0` to `None`;
- use binding values to hydrate allocation target/host when present, matching Zig's `LEFT JOIN` + `COALESCE`.

- [ ] **Step 4: Implement SQLx-backed connection setup**

`Repository` must store both:

```rust
pool: sqlx::SqlitePool,
db: sea_orm::DatabaseConnection,
```

`Repository::open(path)` becomes `pub async fn open(path: impl AsRef<std::path::Path>) -> Result<Self>`. It must:

1. Build a `SqliteConnectOptions` from the path.
2. Set `.create_if_missing(true)`, `.journal_mode(SqliteJournalMode::Wal)`, and `.busy_timeout(Duration::from_millis(5000))`.
3. Connect a small `SqlitePool` with max one connection for M0 deterministic tests.
4. Create `db` via `sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool.clone())`.
5. Run schema setup and legacy migration before returning.

- [ ] **Step 5: Implement schema setup and legacy migration**

Use SeaORM/SeaQuery builders for `CREATE TABLE IF NOT EXISTS` and the unique `(protocol, port)` index. Raw SQL is allowed only for SQLite-specific PRAGMA/introspection and for the legacy set-based migration if SeaQuery cannot express `INSERT INTO ... SELECT ... WHERE NOT EXISTS` cleanly. The migration must preserve Zig behavior:

```sql
INSERT INTO bindings(allocation_id, target_port, host, created_at_ms, updated_at_ms)
SELECT id, target_port, host, created_at_ms, updated_at_ms FROM allocations
WHERE target_port > 0
  AND NOT EXISTS (SELECT 1 FROM bindings WHERE bindings.allocation_id = allocations.id);
```

- [ ] **Step 6: Implement async repository API**

Keep public method names, but make them async:

```rust
pub async fn self_check(&self) -> Result<()>;
pub async fn insert_allocation(&self, allocation: &Allocation) -> Result<()>;
pub async fn put_binding(&self, binding: &Binding) -> Result<()>;
pub async fn delete_binding(&self, allocation_id: &str, updated_at_ms: i64) -> Result<bool>;
pub async fn delete_allocation(&self, id: &str) -> Result<bool>;
pub async fn get_binding(&self, allocation_id: &str) -> Result<Option<Binding>>;
pub async fn get_allocation(&self, id: &str) -> Result<Option<Allocation>>;
pub async fn list_allocations(&self) -> Result<Vec<Allocation>>;
```

Implementation requirements:

- Use SeaORM `ActiveModel::insert`, `Entity::find_by_id`, `Entity::find().order_by_asc(...)`, `ActiveModel::update`, and `Entity::delete_by_id` for CRUD.
- `put_binding` may use find-then-insert/update with `ActiveModel`; after binding write, update the matching allocation legacy columns through SeaORM.
- `delete_binding` deletes the binding with SeaORM and clears legacy allocation columns only if a row was deleted.
- `delete_allocation` deletes the binding row first with SeaORM, then deletes the allocation row and returns whether the allocation existed.
- `get_allocation` and `list_allocations` may fetch allocations and bindings separately and merge in Rust instead of handwritten join SQL. This preserves left-join/COALESCE semantics without raw CRUD SQL.
- M0 intentionally omits transaction-control helpers to avoid handwritten transaction SQL; M1 will introduce SeaORM/SQLx transaction handling when allocation service parity needs it.

- [ ] **Step 7: Add async repository tests**

Convert repository tests to `#[tokio::test]`. Keep all existing assertions, updated with `.await`. Add explicit assertions that:

- `repo.journal_mode().await` returns `wal` through SQLx PRAGMA query.
- `repo.busy_timeout_ms().await` returns `5000` through SQLx PRAGMA query.
- `Cargo.lock`/`cargo tree -i rusqlite` or a package-lock scan shows no `rusqlite` dependency.
- Legacy binding columns update/clear are verified through SQLx scalar queries against the pool.
- Delete allocation removes the binding row.
- Legacy migration can open a pre-existing DB created with SQLx raw DDL and migrates binding columns into the SeaORM binding entity.

- [ ] **Step 8: Run repository tests**

Run: `CARGO_TARGET_DIR=/tmp/relayd-seaorm-task3-target cargo test storage::sqlite`

Expected: all repository tests pass.

## Task 4: Update docs and run full verification

**Files:**
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-m0-core-foundation.md` if implementation evidence changes acceptance wording.
- Modify: `.gitignore` to ignore `target/` while keeping `Cargo.lock` tracked.

- [ ] **Step 1: Update `.gitignore` for Rust build artifacts**

Add this line to `.gitignore` if absent:

```gitignore
target/
```

Do not ignore `Cargo.lock`; commit it for the binary crate.

- [ ] **Step 2: Append M0 status note to the milestone design**

Append this section to `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md` after implementation and verification:

```markdown

## M0 implementation status

- Status: implemented in Rust foundation crate.
- Verification: `cargo test`.
- Scope note: Zig implementation remains available as parity reference for later milestones.
```

- [ ] **Step 3: Run full Rust verification**

Run: `cargo test`

Expected: all Rust tests pass.

- [ ] **Step 4: Run existing Zig regression suite**

Run: `zig build test`

Expected: existing Zig tests pass because M0 does not change Zig source.

- [ ] **Step 5: Prepare commit after independent spec-compliance approval**

After implementation and independent spec-compliance approval, commit with the Lore Commit Protocol. Use an intent-focused message such as:

```text
Establish a Rust foundation before porting runtime behavior

Constraint: Keep the Zig implementation intact while building parity slices.
Rejected: Big-bang rewrite | too risky to verify against existing behavior.
Confidence: high
Scope-risk: narrow
Directive: Preserve M0 module boundaries for service, HTTP, and runtime milestones.
Tested: cargo test; zig build test
Not-tested: Live HTTP/runtime parity, deferred to later milestones.
```
