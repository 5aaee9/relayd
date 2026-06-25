# Runtime Mode Netlink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `RuntimeMode` so relayd can keep the default proxy runtime or install nftables DNAT rules through libnftnl in `netlink` mode.

**Architecture:** Add config and CLI selection first, then extend `RuntimeFacade` with an initialization hook and a non-retryable apply error. Implement `NetlinkRuntime` behind the existing runtime facade with an in-memory test backend and a production `nftnl` backend. Startup dispatch remains statically typed by making listener serving generic and branching before service construction.

**Tech Stack:** Rust 2024, tokio, async-trait, clap, nftnl 0.9.2, mnl 0.3.0, nftnl_sys through nftnl for missing low-level nftables expressions/messages, SQLite tests through existing repository helpers.

---

## File Structure

- Modify `Cargo.toml` and `Cargo.lock`: add `nftnl = "0.9.2"` and `mnl = "0.3.0"`.
- Modify `src/config.rs`: add `RuntimeMode`, nftables table/chain fields, parsing, validation, and unit tests.
- Modify `src/bin/relayd.rs`: add CLI flags, runtime-mode dispatch, generic serving helpers, startup tests, and help text assertions.
- Modify `src/runtime/facade.rs`: add `RuntimeApplyFailed` and `initialize`.
- Modify `src/runtime/mod.rs`: expose the new `netlink` module.
- Create `src/runtime/netlink.rs`: implement `NetlinkRuntime`, `NftBackend`, test backend, rule projection, production backend shell, and unit tests.
- Modify `src/service/allocation_service.rs`: call `runtime.initialize()` during restore and treat `RuntimeApplyFailed` as non-retryable in create.
- Modify `src/http/control_plane.rs`: map `RuntimeApplyFailed` to HTTP 503 text response.
- Modify `README.md` and `docs/architecture/port-forwarder.md`: document runtime modes and nftables ownership.

## Task 1: Config And CLI Surface

**Files:**
- Modify: `src/config.rs`
- Modify: `src/bin/relayd.rs`

- [ ] **Step 1: Add failing config tests**

Add tests to `src/config.rs`:

```rust
#[test]
fn config_from_env_map_defaults_runtime_mode_and_nftables_names() {
    let cfg = Config::from_env_map(&env_with_token()).unwrap();
    assert_eq!(cfg.runtime_mode, RuntimeMode::Proxy);
    assert_eq!(cfg.nftables_table, "relayd");
    assert_eq!(cfg.nftables_chain, "mapping");
}

#[test]
fn config_from_env_map_parses_netlink_runtime_mode_and_nftables_names() {
    let mut env = env_with_token();
    env.insert("RELAYD_RUNTIME_MODE".to_owned(), "NeTlInK".to_owned());
    env.insert("RELAYD_NFTABLES_TABLE".to_owned(), "custom_table".to_owned());
    env.insert("RELAYD_NFTABLES_CHAIN".to_owned(), "custom_chain".to_owned());

    let cfg = Config::from_env_map(&env).unwrap();

    assert_eq!(cfg.runtime_mode, RuntimeMode::Netlink);
    assert_eq!(cfg.nftables_table, "custom_table");
    assert_eq!(cfg.nftables_chain, "custom_chain");
}

#[test]
fn config_from_env_map_rejects_invalid_runtime_mode_and_empty_nftables_names() {
    let mut env = env_with_token();
    env.insert("RELAYD_RUNTIME_MODE".to_owned(), "bad".to_owned());
    assert!(matches!(
        Config::from_env_map(&env),
        Err(ConfigError::InvalidRuntimeMode)
    ));

    let mut env = env_with_token();
    env.insert("RELAYD_NFTABLES_TABLE".to_owned(), "".to_owned());
    assert!(matches!(
        Config::from_env_map(&env),
        Err(ConfigError::InvalidNftablesName("RELAYD_NFTABLES_TABLE"))
    ));

    let mut env = env_with_token();
    env.insert("RELAYD_NFTABLES_CHAIN".to_owned(), "".to_owned());
    assert!(matches!(
        Config::from_env_map(&env),
        Err(ConfigError::InvalidNftablesName("RELAYD_NFTABLES_CHAIN"))
    ));
}
```

- [ ] **Step 2: Run failing config tests**

Run:

```bash
cargo test --locked config::tests::config_from_env_map_defaults_runtime_mode_and_nftables_names
cargo test --locked config::tests::config_from_env_map_parses_netlink_runtime_mode_and_nftables_names
cargo test --locked config::tests::config_from_env_map_rejects_invalid_runtime_mode_and_empty_nftables_names
```

Expected: FAIL because `RuntimeMode` and the new config fields do not exist.

- [ ] **Step 3: Implement config parsing**

In `src/config.rs`, add:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMode {
    Proxy,
    Netlink,
}

impl RuntimeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Proxy => "proxy",
            Self::Netlink => "netlink",
        }
    }
}

impl FromStr for RuntimeMode {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "proxy" => Ok(Self::Proxy),
            "netlink" => Ok(Self::Netlink),
            _ => Err(ConfigError::InvalidRuntimeMode),
        }
    }
}
```

Extend `Config`:

```rust
pub runtime_mode: RuntimeMode,
pub nftables_table: String,
pub nftables_chain: String,
```

Extend `ConfigError`:

```rust
#[error("invalid RELAYD_RUNTIME_MODE")]
InvalidRuntimeMode,
#[error("invalid nftables name for {0}")]
InvalidNftablesName(&'static str),
```

Add helper:

```rust
fn env_nonempty_string(
    env: &HashMap<String, String>,
    name: &'static str,
    default_value: &'static str,
) -> Result<String, ConfigError> {
    let value = env.get(name).map(String::as_str).unwrap_or(default_value);
    if value.is_empty() {
        return Err(ConfigError::InvalidNftablesName(name));
    }
    Ok(value.to_owned())
}
```

In `Config::from_env_map`, parse:

```rust
runtime_mode: env
    .get("RELAYD_RUNTIME_MODE")
    .map(String::as_str)
    .unwrap_or("proxy")
    .parse()?,
nftables_table: env_nonempty_string(env, "RELAYD_NFTABLES_TABLE", "relayd")?,
nftables_chain: env_nonempty_string(env, "RELAYD_NFTABLES_CHAIN", "mapping")?,
```

Update the literal `Config { ... }` in `config_surface_does_not_expose_tcp_splice_activation_flags` with defaults.

- [ ] **Step 4: Add failing CLI tests**

In `src/bin/relayd.rs`, update `cli_help_documents_runtime_options` with:

```rust
assert!(help.contains("--runtime-mode <MODE>"));
assert!(help.contains("env: RELAYD_RUNTIME_MODE"));
assert!(help.contains("--nftables-table <NAME>"));
assert!(help.contains("env: RELAYD_NFTABLES_TABLE"));
assert!(help.contains("--nftables-chain <NAME>"));
assert!(help.contains("env: RELAYD_NFTABLES_CHAIN"));
assert!(help.contains("relayd-owned"));
assert!(help.contains("flushed"));
```

Update `cli_options_override_environment_config` input with:

```rust
"--runtime-mode",
"netlink",
"--nftables-table",
"cli_table",
"--nftables-chain",
"cli_chain",
```

Add env values that should be overridden:

```rust
("RELAYD_RUNTIME_MODE".to_owned(), "proxy".to_owned()),
("RELAYD_NFTABLES_TABLE".to_owned(), "env_table".to_owned()),
("RELAYD_NFTABLES_CHAIN".to_owned(), "env_chain".to_owned()),
```

Assert:

```rust
assert_eq!(config.runtime_mode, relayd::config::RuntimeMode::Netlink);
assert_eq!(config.nftables_table, "cli_table");
assert_eq!(config.nftables_chain, "cli_chain");
```

- [ ] **Step 5: Run failing CLI tests**

Run:

```bash
cargo test --locked --bin relayd cli_help_documents_runtime_options
cargo test --locked --bin relayd cli_options_override_environment_config
```

Expected: FAIL because CLI flags do not exist.

- [ ] **Step 6: Implement CLI flags**

In `Cli`, add:

```rust
#[arg(
    long,
    value_name = "MODE",
    help = "Runtime forwarding mode: proxy or netlink (env: RELAYD_RUNTIME_MODE). Default: proxy."
)]
runtime_mode: Option<String>,

#[arg(
    long,
    value_name = "NAME",
    help = "relayd-owned nftables table name for netlink runtime mode (env: RELAYD_NFTABLES_TABLE). Default: relayd."
)]
nftables_table: Option<String>,

#[arg(
    long,
    value_name = "NAME",
    help = "relayd-owned nftables chain name for netlink runtime mode; startup and runtime changes flush and rewrite this chain (env: RELAYD_NFTABLES_CHAIN). Default: mapping."
)]
nftables_chain: Option<String>,
```

In `Cli::apply_to_env`, insert:

```rust
insert_if_present(env, "RELAYD_RUNTIME_MODE", self.runtime_mode);
insert_if_present(env, "RELAYD_NFTABLES_TABLE", self.nftables_table);
insert_if_present(env, "RELAYD_NFTABLES_CHAIN", self.nftables_chain);
```

- [ ] **Step 7: Run Task 1 tests**

Run:

```bash
cargo test --locked config::tests::config_from_env_map_defaults_runtime_mode_and_nftables_names
cargo test --locked config::tests::config_from_env_map_parses_netlink_runtime_mode_and_nftables_names
cargo test --locked config::tests::config_from_env_map_rejects_invalid_runtime_mode_and_empty_nftables_names
cargo test --locked --bin relayd cli_help_documents_runtime_options
cargo test --locked --bin relayd cli_options_override_environment_config
```

Expected: PASS.

## Task 2: Runtime Facade Lifecycle And Non-Retryable Apply Error

**Files:**
- Modify: `src/runtime/facade.rs`
- Modify: `src/runtime/real.rs`
- Modify: `src/runtime/tcp.rs`
- Modify: `src/runtime/udp.rs`
- Modify: `src/service/allocation_service.rs`
- Modify: `src/http/control_plane.rs`

- [ ] **Step 1: Add failing service/runtime tests**

In `src/runtime/facade.rs`, extend `InMemoryRuntime` state:

```rust
initialize_fail: bool,
initialize_calls: u32,
apply_fail_ports: HashSet<u16>,
```

Add helper methods:

```rust
pub fn fail_initialize(&self) {
    self.state.lock().unwrap().initialize_fail = true;
}

pub fn fail_apply_port(&self, port: u16) {
    self.state.lock().unwrap().apply_fail_ports.insert(port);
}

pub fn initialize_calls(&self) -> u32 {
    self.state.lock().unwrap().initialize_calls
}
```

Add tests in `src/service/allocation_service.rs`:

```rust
#[tokio::test]
async fn restore_all_initializes_runtime_even_with_no_allocations() {
    let repo = temp_repo().await;
    let runtime = InMemoryRuntime::default();
    let svc = service(repo, runtime.clone());

    svc.restore_all(500).await.unwrap();

    assert_eq!(runtime.initialize_calls(), 1);
}

#[tokio::test]
async fn create_allocation_does_not_retry_non_retryable_runtime_apply_failed() {
    let repo = temp_repo().await;
    let runtime = InMemoryRuntime::default();
    runtime.fail_apply_port(10000);
    let svc = service_with_range(repo, runtime.clone(), 10000, 10002);

    let error = svc.create_allocation(Protocol::Tcp, None).await.unwrap_err();

    assert!(matches!(
        error,
        ServiceError::Runtime(RuntimeError::RuntimeApplyFailed)
    ));
    assert_eq!(runtime.calls().create, vec!["alloc-1"]);
    assert!(svc.repo().list_allocations().await.unwrap().is_empty());
}
```

Add HTTP mapping test near existing runtime failure tests:

```rust
#[tokio::test]
async fn runtime_apply_failed_maps_to_503() {
    let (app, _, runtime, _, _file) = test_app().await;
    runtime.fail_apply_port(10000);

    let response = request(
        &app,
        Method::POST,
        "/v1/allocations",
        r#"{"protocol":"tcp"}"#,
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"RuntimeApplyFailed");
}
```

- [ ] **Step 2: Run failing lifecycle/error tests**

Run:

```bash
cargo test --locked service::allocation_service::tests::restore_all_initializes_runtime_even_with_no_allocations
cargo test --locked service::allocation_service::tests::create_allocation_does_not_retry_non_retryable_runtime_apply_failed
cargo test --locked http::control_plane::tests::runtime_apply_failed_maps_to_503
```

Expected: FAIL because `initialize` and `RuntimeApplyFailed` do not exist.

- [ ] **Step 3: Extend facade and in-memory runtime**

In `RuntimeError`, add:

```rust
#[error("runtime apply failed")]
RuntimeApplyFailed,
```

In `RuntimeFacade`, add:

```rust
async fn initialize(&self) -> Result<(), RuntimeError>;
```

Implement no-op `initialize` in `RealRuntime`, `TcpRuntime`, and `UdpRuntime`.

Implement `initialize` in `InMemoryRuntime`:

```rust
async fn initialize(&self) -> Result<(), RuntimeError> {
    let mut state = self.state.lock().unwrap();
    state.initialize_calls += 1;
    if state.initialize_fail {
        return Err(RuntimeError::RuntimeApplyFailed);
    }
    Ok(())
}
```

In `InMemoryRuntime::create`, before retryable create failure handling, return `RuntimeApplyFailed` when `apply_fail_ports` contains the allocation port.

- [ ] **Step 4: Wire service and HTTP mapping**

In `Service::restore_all`, call:

```rust
self.runtime.initialize().await?;
```

before listing/restoring allocations.

In `Service::create_allocation`, ensure only `RuntimeCreateFailed` continues to the next port. `RuntimeApplyFailed` must fall through the existing `Err(error) => return Err(ServiceError::Runtime(error))` branch.

In `service_error_response`, add:

```rust
ServiceError::Runtime(RuntimeError::RuntimeApplyFailed) => {
    text_response(StatusCode::SERVICE_UNAVAILABLE, "RuntimeApplyFailed")
}
```

- [ ] **Step 5: Run Task 2 tests**

Run:

```bash
cargo test --locked runtime::facade::tests
cargo test --locked service::allocation_service::tests::restore_all_initializes_runtime_even_with_no_allocations
cargo test --locked service::allocation_service::tests::create_allocation_does_not_retry_non_retryable_runtime_apply_failed
cargo test --locked http::control_plane::tests::runtime_apply_failed_maps_to_503
```

Expected: PASS.

## Task 3: Netlink Runtime With In-Memory Backend

**Files:**
- Create: `src/runtime/netlink.rs`
- Modify: `src/runtime/mod.rs`
- Modify: `src/model.rs`

- [ ] **Step 1: Create module skeleton and failing tests**

In `src/runtime/mod.rs`, add:

```rust
pub mod netlink;
```

`src/model.rs` already contains `RuntimeStatus::DegradedApplyFailed` and `ErrorKind::ApplyFailed`; use those existing variants in netlink tests and snapshot implementation. Do not add duplicate enum variants.

Create `src/runtime/netlink.rs` with tests first. Define test helpers:

```rust
fn allocation(id: &str, protocol: Protocol, port: u16, target_port: Option<u16>, host: Option<&str>) -> Allocation {
    Allocation {
        id: id.to_owned(),
        protocol,
        port,
        target_port,
        host: host.map(str::to_owned),
        created_at_ms: 1000,
        updated_at_ms: 1000,
    }
}
```

Add tests:

```rust
#[tokio::test]
async fn netlink_initialize_replaces_empty_ruleset_with_configured_topology() {
    let backend = Arc::new(RecordingBackend::default());
    let runtime = NetlinkRuntime::with_backend(
        NetlinkRuntimeConfig::new("relayd", "mapping"),
        backend.clone(),
    );

    runtime.initialize().await.unwrap();

    assert_eq!(
        backend.calls(),
        vec![ReplaceCall {
            table: "relayd".to_owned(),
            chain: "mapping".to_owned(),
            rules: vec![],
        }]
    );
}

#[tokio::test]
async fn netlink_unbound_allocation_installs_no_rules_and_reports_rejecting_no_host() {
    let backend = Arc::new(RecordingBackend::default());
    let runtime = NetlinkRuntime::with_backend(
        NetlinkRuntimeConfig::new("relayd", "mapping"),
        backend.clone(),
    );

    runtime
        .create(&allocation("a1", Protocol::Tcp, 10000, None, None), 500)
        .await
        .unwrap();

    assert_eq!(backend.last_rules(), Vec::<NftDnatRule>::new());
    let observed = runtime.snapshot("a1").await.unwrap().unwrap();
    assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
    assert_eq!(observed.effective_host, None);
    assert_eq!(observed.effective_target_port, None);
    assert_eq!(observed.error_kind, None);
    assert_eq!(observed.last_error, None);
}

#[tokio::test]
async fn netlink_bound_tcp_and_udp_allocations_project_dnat_rules() {
    let backend = Arc::new(RecordingBackend::default());
    let runtime = NetlinkRuntime::with_backend(
        NetlinkRuntimeConfig::new("relayd", "mapping"),
        backend.clone(),
    );

    runtime
        .create(
            &allocation("tcp", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
            500,
        )
        .await
        .unwrap();
    runtime
        .create(
            &allocation("udp", Protocol::Udp, 10001, Some(8081), Some("::1")),
            500,
        )
        .await
        .unwrap();

    assert_eq!(
        backend.last_rules(),
        vec![
            NftDnatRule {
                protocol: Protocol::Tcp,
                relay_port: 10000,
                target_addr: "127.0.0.1".parse().unwrap(),
                target_port: 8080,
                family: NftIpFamily::Ipv4,
            },
            NftDnatRule {
                protocol: Protocol::Udp,
                relay_port: 10001,
                target_addr: "::1".parse().unwrap(),
                target_port: 8081,
                family: NftIpFamily::Ipv6,
            },
        ]
    );
}

#[tokio::test]
async fn netlink_both_allocation_projects_tcp_and_udp_rules() {
    let backend = Arc::new(RecordingBackend::default());
    let runtime = NetlinkRuntime::with_backend(
        NetlinkRuntimeConfig::new("relayd", "mapping"),
        backend.clone(),
    );

    runtime
        .create(
            &allocation("both", Protocol::Both, 10000, Some(8080), Some("127.0.0.1")),
            500,
        )
        .await
        .unwrap();

    assert_eq!(
        backend.last_rules(),
        vec![
            NftDnatRule {
                protocol: Protocol::Tcp,
                relay_port: 10000,
                target_addr: "127.0.0.1".parse().unwrap(),
                target_port: 8080,
                family: NftIpFamily::Ipv4,
            },
            NftDnatRule {
                protocol: Protocol::Udp,
                relay_port: 10000,
                target_addr: "127.0.0.1".parse().unwrap(),
                target_port: 8080,
                family: NftIpFamily::Ipv4,
            },
        ]
    );
}

#[tokio::test]
async fn netlink_delete_and_unbind_rewrite_rules_without_deleted_or_unbound_allocations() {
    let backend = Arc::new(RecordingBackend::default());
    let runtime = NetlinkRuntime::with_backend(
        NetlinkRuntimeConfig::new("relayd", "mapping"),
        backend.clone(),
    );

    runtime
        .create(
            &allocation("tcp", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
            500,
        )
        .await
        .unwrap();
    runtime
        .update(&allocation("tcp", Protocol::Tcp, 10000, None, None), 500)
        .await
        .unwrap();
    assert_eq!(backend.last_rules(), Vec::<NftDnatRule>::new());
    assert_eq!(
        runtime
            .snapshot("tcp")
            .await
            .unwrap()
            .unwrap()
            .runtime_status,
        RuntimeStatus::RejectingNoHost
    );

    runtime
        .update(
            &allocation("tcp", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
            500,
        )
        .await
        .unwrap();
    runtime.delete("tcp", 500).await.unwrap();
    assert_eq!(backend.last_rules(), Vec::<NftDnatRule>::new());
    assert_eq!(runtime.snapshot("tcp").await.unwrap(), None);
}

#[tokio::test]
async fn netlink_apply_failure_state_matches_service_persistence_order() {
    let backend = Arc::new(RecordingBackend::default());
    let runtime = NetlinkRuntime::with_backend(
        NetlinkRuntimeConfig::new("relayd", "mapping"),
        backend.clone(),
    );

    backend.fail_next("create failed");
    assert_eq!(
        runtime
            .create(
                &allocation("create", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
                500,
            )
            .await,
        Err(RuntimeError::RuntimeApplyFailed)
    );
    assert_eq!(runtime.snapshot("create").await.unwrap(), None);

    runtime
        .create(
            &allocation("update", Protocol::Tcp, 10001, Some(8081), Some("127.0.0.1")),
            500,
        )
        .await
        .unwrap();
    backend.fail_next("update failed");
    assert_eq!(
        runtime
            .update(
                &allocation("update", Protocol::Tcp, 10001, Some(9090), Some("127.0.0.1")),
                500,
            )
            .await,
        Err(RuntimeError::RuntimeApplyFailed)
    );
    let observed = runtime.snapshot("update").await.unwrap().unwrap();
    assert_eq!(observed.runtime_status, RuntimeStatus::DegradedApplyFailed);
    assert_eq!(observed.effective_target_port, Some(9090));

    backend.fail_next("delete failed");
    assert_eq!(
        runtime.delete("update", 500).await,
        Err(RuntimeError::RuntimeApplyFailed)
    );
    let observed = runtime.snapshot("update").await.unwrap().unwrap();
    assert_eq!(observed.runtime_status, RuntimeStatus::DegradedApplyFailed);
    assert_eq!(observed.effective_target_port, Some(9090));
}

#[tokio::test]
async fn netlink_restore_repopulates_rules_and_restore_failure_commits_stale_state() {
    let backend = Arc::new(RecordingBackend::default());
    let runtime = NetlinkRuntime::with_backend(
        NetlinkRuntimeConfig::new("relayd", "mapping"),
        backend.clone(),
    );

    runtime.initialize().await.unwrap();
    runtime
        .restore(
            &allocation("restored", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
            500,
        )
        .await
        .unwrap();

    assert_eq!(
        backend.last_rules(),
        vec![NftDnatRule {
            protocol: Protocol::Tcp,
            relay_port: 10000,
            target_addr: "127.0.0.1".parse().unwrap(),
            target_port: 8080,
            family: NftIpFamily::Ipv4,
        }]
    );

    backend.fail_next("restore failed");
    assert_eq!(
        runtime
            .restore(
                &allocation("stale", Protocol::Udp, 10001, Some(8081), Some("::1")),
                500,
            )
            .await,
        Err(RuntimeError::RuntimeApplyFailed)
    );

    let observed = runtime.snapshot("stale").await.unwrap().unwrap();
    assert_eq!(observed.runtime_status, RuntimeStatus::DegradedApplyFailed);
    assert_eq!(observed.effective_host.as_deref(), Some("::1"));
    assert_eq!(observed.effective_target_port, Some(8081));
    assert_eq!(observed.error_kind, Some(ErrorKind::ApplyFailed));
}

#[tokio::test]
async fn netlink_create_does_not_bind_relay_tcp_socket() {
    let busy_relay = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let relay_port = busy_relay.local_addr().unwrap().port();
    let backend = Arc::new(RecordingBackend::default());
    let runtime = NetlinkRuntime::with_backend(
        NetlinkRuntimeConfig::new("relayd", "mapping"),
        backend.clone(),
    );

    runtime
        .create(
            &allocation("tcp", Protocol::Tcp, relay_port, Some(8080), Some("127.0.0.1")),
            500,
        )
        .await
        .unwrap();

    assert_eq!(
        backend.last_rules(),
        vec![NftDnatRule {
            protocol: Protocol::Tcp,
            relay_port,
            target_addr: "127.0.0.1".parse().unwrap(),
            target_port: 8080,
            family: NftIpFamily::Ipv4,
        }]
    );
    drop(busy_relay);
}
```

- [ ] **Step 2: Run failing netlink runtime tests**

Run:

```bash
cargo test --locked runtime::netlink
```

Expected: FAIL because the module/types are incomplete.

- [ ] **Step 3: Implement netlink runtime types**

In `src/runtime/netlink.rs`, define:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NftIpFamily {
    Ipv4,
    Ipv6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NftDnatRule {
    pub protocol: Protocol,
    pub relay_port: u16,
    pub target_addr: IpAddr,
    pub target_port: u16,
    pub family: NftIpFamily,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[error("{message}")]
pub(crate) struct NftBackendError {
    message: String,
}

pub(crate) trait NftBackend: Send + Sync {
    fn replace_ruleset(
        &self,
        table: &str,
        chain: &str,
        rules: &[NftDnatRule],
    ) -> Result<(), NftBackendError>;
}

#[derive(Clone)]
pub struct NetlinkRuntimeConfig {
    table: String,
    chain: String,
}

#[derive(Clone)]
pub struct NetlinkRuntime<B> {
    config: NetlinkRuntimeConfig,
    backend: Arc<B>,
    state: Arc<Mutex<NetlinkState>>,
}
```

State:

```rust
#[derive(Default)]
struct NetlinkState {
    allocations: HashMap<String, Allocation>,
    stale_error: Option<String>,
    initialized: bool,
}
```

Add constructors:

```rust
impl NetlinkRuntimeConfig {
    pub fn new(table: impl Into<String>, chain: impl Into<String>) -> Self { ... }
}

impl<B: NftBackend + 'static> NetlinkRuntime<B> {
    pub(crate) fn with_backend(config: NetlinkRuntimeConfig, backend: Arc<B>) -> Self { ... }
}
```

Task 3 intentionally does not define the production default `NetlinkRuntime<LibNftnlBackend>` constructor. It only needs the generic runtime and `RecordingBackend` tests. Task 4 adds `LibNftnlBackend` and the production `NetlinkRuntime::new` constructor after dependencies exist.

- [ ] **Step 4: Implement rule projection**

Implement:

```rust
fn rules_for_allocations(allocations: &HashMap<String, Allocation>) -> Vec<NftDnatRule>
```

Rules:

- Skip allocations missing `host` or `target_port`.
- Parse host as `IpAddr`; service validation should make this valid, but invalid parse should be treated as no rule in tests only or returned as backend apply failure before production use.
- `Protocol::Tcp` -> one TCP rule.
- `Protocol::Udp` -> one UDP rule.
- `Protocol::Both` -> TCP then UDP rules.
- Sort deterministic by `(relay_port, protocol.as_str(), target_addr.to_string(), target_port)`.

- [ ] **Step 5: Implement `RuntimeFacade` for `NetlinkRuntime<B>`**

Implement `initialize`, `create`, `update`, `delete`, `restore`, `snapshot`, and `snapshot_listener_metrics` per spec:

- `initialize` applies empty current map and commits `initialized = true` only on success.
- `create` applies candidate map with new allocation; commit candidate only on success.
- `update` applies candidate map with updated allocation; commit attempted candidate even on failure.
- `delete` applies candidate map without allocation; keep old map on failure.
- `restore` applies candidate map with restored allocation; commit attempted candidate even on failure.
- Backend errors map to `RuntimeError::RuntimeApplyFailed`.
- `snapshot_listener_metrics` returns `Ok(Vec::new())`.
- stale bound allocations report `degraded_apply_failed`, `Some(ApplyFailed)`, and backend error.

- [ ] **Step 6: Implement in-memory backend for tests**

Under `#[cfg(test)]`, add:

```rust
#[derive(Clone, Default)]
struct RecordingBackend {
    calls: Arc<std::sync::Mutex<Vec<ReplaceCall>>>,
    fail_next: Arc<std::sync::Mutex<Option<String>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplaceCall {
    table: String,
    chain: String,
    rules: Vec<NftDnatRule>,
}
```

`replace_ruleset` records every call unless `fail_next` is set, in which case it returns `NftBackendError` and clears the failpoint.

- [ ] **Step 7: Run Task 3 tests**

Run:

```bash
cargo test --locked runtime::netlink
```

Expected: PASS.

## Task 4: Production nftnl Backend

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/runtime/netlink.rs`

- [ ] **Step 1: Inspect nftnl and mnl APIs before coding**

Run:

```bash
cargo info nftnl
cargo info mnl
```

Then inspect the downloaded sources:

```bash
NFTNL_SRC="$(find ~/.cargo/registry/src -maxdepth 3 -type d -name 'nftnl-0.9.2' | head -1)"
MNL_SRC="$(find ~/.cargo/registry/src -maxdepth 3 -type d -name 'mnl-0.3.0' | head -1)"
sed -n '1,220p' "$NFTNL_SRC/src/lib.rs"
sed -n '1,260p' "$NFTNL_SRC/src/batch.rs"
sed -n '1,260p' "$NFTNL_SRC/src/chain.rs"
sed -n '1,260p' "$NFTNL_SRC/src/rule.rs"
sed -n '1,220p' "$NFTNL_SRC/src/expr/nat.rs"
sed -n '1,260p' "$NFTNL_SRC/examples/add-rules.rs"
sed -n '1,220p' "$MNL_SRC/src/lib.rs"
```

Before coding the backend, add an implementation note comment near the future `LibNftnlBackend` section in `src/runtime/netlink.rs` with the compile-grounded API findings. The note must list:

- confirmed safe wrapper symbols, such as `nftnl::Batch`, `nftnl::Table`, `nftnl::Chain`, `nftnl::Rule`, `nftnl::expr::Nat`, `mnl::Socket`, and `mnl::cb_run`;
- safe wrapper gaps, including whether chain flush and `fib daddr type local` are missing;
- exact `nftnl::nftnl_sys` constants/functions that will be used for chain flush;
- exact `nftnl::nftnl_sys` constants/functions that will be used for the FIB expression;
- any required adjustment to the plan before backend coding.

If the inspection shows a planned symbol does not exist, revise this plan section before implementing `LibNftnlBackend`; do not guess through compilation errors.

- [ ] **Step 2: Add dependencies and regenerate lockfile**

In `Cargo.toml`, add:

```toml
nftnl = "0.9.2"
mnl = "0.3.0"
```

Run:

```bash
cargo check --locked
```

Expected: FAIL because `Cargo.lock` needs updates for new dependencies.

Then populate the lockfile with an unlocked check:

```bash
cargo check
```

Expected: `Cargo.lock` includes `nftnl`, `nftnl-sys`, `mnl`, and their transitive dependencies.

If Cargo resolves a different `nftnl` or `mnl` version than requested, pin the dependency with:

```bash
cargo update -p nftnl --precise 0.9.2
cargo update -p mnl --precise 0.3.0
```

Inspect lockfile changes:

```bash
git diff -- Cargo.lock
```

Expected: diff adds only the new dependency closure required by `nftnl` and `mnl`; unrelated existing packages are not version-changed.

- [ ] **Step 3: Add backend conversion tests**

In `src/runtime/netlink.rs`, add unit tests that do not require root:

```rust
#[test]
fn libnftnl_backend_accepts_c_string_safe_names() {
    assert!(cstring_name("relayd").is_ok());
    assert!(cstring_name("mapping").is_ok());
    assert!(cstring_name("").is_err());
    assert!(cstring_name("bad\0name").is_err());
}

#[test]
fn htons_encodes_ports_for_transport_payload_comparisons() {
    assert_eq!(network_port_bytes(80), 80_u16.to_be_bytes());
}
```

- [ ] **Step 4: Implement `LibNftnlBackend` structure**

Add:

```rust
#[derive(Clone, Default)]
pub struct LibNftnlBackend;
```

Implement `NftBackend for LibNftnlBackend`.

Add production constructor:

```rust
impl NetlinkRuntime<LibNftnlBackend> {
    pub fn new(config: NetlinkRuntimeConfig) -> Self {
        Self::with_backend(config, Arc::new(LibNftnlBackend))
    }
}
```

Build `replace_ruleset` as one `nftnl::Batch` using symbols confirmed in Step 1:

1. `Table::new(table_cstr, ProtoFamily::Inet)`, `batch.add(&table, MsgType::Add)`.
2. `Chain::new(chain_cstr, &table)`, set type `ChainType::Nat`, hook `Hook::PreRouting` with destination NAT priority, policy accept, `batch.add(&chain, MsgType::Add)`.
3. Add a chain flush message for the configured chain using `nftnl::nftnl_sys` because the safe wrapper does not expose flush. If the API does not expose a flush message helper directly, use a documented local wrapper around `nftnl_chain` serialization with `NFTNL_CMD_FLUSH`/flush-equivalent constants found in `nftnl_sys`; keep this wrapper private to `src/runtime/netlink.rs`.
4. Add one `Rule` per `NftDnatRule`.
5. Finalize and send through `mnl::Socket::new(mnl::Bus::Netfilter)`.
6. Process ACKs with `mnl::cb_run` for each sequence number.

Map all I/O, C string, and netlink callback errors to `NftBackendError { message }`.

- [ ] **Step 5: Implement rule expression builder**

For each DNAT rule, add expressions equivalent to:

```text
meta l4proto == tcp|udp
payload transport dport == relay_port
fib daddr type local == local
immediate target address register
immediate target port register
counter
dnat to target address:target port
```

Use safe `nftnl` expressions where available and verify each expression compiles immediately after adding it:

- `nft_expr!(meta l4proto)` and `nft_expr!(cmp == protocol_number)`
- `expr::Payload::Transport(TransportHeaderField::Tcp(TcpHeaderField::Dport))` or UDP Dport, followed by `Cmp::new(CmpOp::Eq, u16::from_be_bytes(...))` if needed after compile verification.
- `expr::Immediate` for destination address and port registers.
- `expr::Nat { nat_type: NatType::DNat, family: ProtoFamily::Ipv4|Ipv6, ip_register, port_register: Some(...) }`

Use `nftnl::nftnl_sys` inside small local expression wrappers for:

- `fib daddr type local` when not present in safe wrapper.
- chain flush when not present in safe wrapper.

Keep all unsafe code local to `src/runtime/netlink.rs`, documented with short safety comments.

- [ ] **Step 6: Compile backend**

Run:

```bash
cargo check --locked
```

Expected: PASS if system `libnftnl`/`libmnl` development libraries are installed. If it fails due to missing system libraries or `pkg-config`, record the exact error and continue only with tests that do not require linking impossible dependencies.

- [ ] **Step 7: Run backend unit tests**

Run:

```bash
cargo test --locked runtime::netlink::tests::libnftnl_backend_accepts_c_string_safe_names
cargo test --locked runtime::netlink::tests::htons_encodes_ports_for_transport_payload_comparisons
```

Expected: PASS when dependencies link.

## Task 5: Startup Runtime Selection

**Files:**
- Modify: `src/bin/relayd.rs`

- [ ] **Step 1: Add startup wiring tests**

Add tests:

```rust
#[test]
fn runtime_mode_selection_builds_proxy_runtime_config_without_netlink_settings() {
    let config = Config::from_env_map(&HashMap::from([
        ("AUTH_TOKEN".to_owned(), "secret".to_owned()),
        ("RELAYD_RUNTIME_MODE".to_owned(), "proxy".to_owned()),
        ("RELAYD_NFTABLES_TABLE".to_owned(), "custom".to_owned()),
        ("RELAYD_NFTABLES_CHAIN".to_owned(), "custom_chain".to_owned()),
    ])).unwrap();

    assert_eq!(config.runtime_mode, relayd::config::RuntimeMode::Proxy);
    let runtime_config = real_runtime_config_from_config(&config, Arc::new(Metrics::default()));
    assert_eq!(runtime_config.udp_max_sessions(), 65_536);
}

#[test]
fn netlink_runtime_config_from_config_carries_table_and_chain() {
    let config = Config::from_env_map(&HashMap::from([
        ("AUTH_TOKEN".to_owned(), "secret".to_owned()),
        ("RELAYD_RUNTIME_MODE".to_owned(), "netlink".to_owned()),
        ("RELAYD_NFTABLES_TABLE".to_owned(), "custom".to_owned()),
        ("RELAYD_NFTABLES_CHAIN".to_owned(), "custom_chain".to_owned()),
    ])).unwrap();

    let runtime_config = netlink_runtime_config_from_config(&config);
    assert_eq!(runtime_config.table(), "custom");
    assert_eq!(runtime_config.chain(), "custom_chain");
}
```

The backend-independent no-bind assertion is covered in `runtime::netlink::tests::netlink_create_does_not_bind_relay_tcp_socket`, where the library unit test can use the local `RecordingBackend` without crossing crate visibility boundaries. Do not add a binary test that imports `RecordingBackend`; `src/bin/relayd.rs` tests compile as a separate crate and cannot access library `#[cfg(test)]` items or `pub(crate)` internals.

- [ ] **Step 2: Run failing startup wiring tests**

Run:

```bash
cargo test --locked --bin relayd runtime_mode_selection_builds_proxy_runtime_config_without_netlink_settings
cargo test --locked --bin relayd netlink_runtime_config_from_config_carries_table_and_chain
```

Expected: config helper tests FAIL until helper/getters exist.

- [ ] **Step 3: Genericize serving and branch in startup**

Change imports:

```rust
use relayd::config::{Config, RuntimeMode};
use relayd::runtime::facade::RuntimeFacade;
use relayd::runtime::netlink::{NetlinkRuntime, NetlinkRuntimeConfig};
```

Add helper:

```rust
fn netlink_runtime_config_from_config(config: &Config) -> NetlinkRuntimeConfig {
    NetlinkRuntimeConfig::new(config.nftables_table.clone(), config.nftables_chain.clone())
}
```

Change `run_with_listener`:

```rust
match config.runtime_mode {
    RuntimeMode::Proxy => {
        let runtime = RealRuntime::new(real_runtime_config_from_config(&config, metrics.clone()));
        run_with_runtime(config, listener, runtime, metrics).await
    }
    RuntimeMode::Netlink => {
        let runtime = NetlinkRuntime::new(netlink_runtime_config_from_config(&config));
        run_with_runtime(config, listener, runtime, metrics).await
    }
}
```

Extract generic helper:

```rust
async fn run_with_runtime<R: RuntimeFacade + 'static>(
    config: Config,
    listener: TcpListener,
    runtime: R,
    metrics: Arc<Metrics>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { ... }
```

Make `serve_listener` and `serve_listener_until_shutdown` generic:

```rust
async fn serve_listener<R: RuntimeFacade + 'static>(
    listener: TcpListener,
    state: AppState<R>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
```

- [ ] **Step 4: Run startup tests**

Run:

```bash
cargo test --locked --bin relayd runtime_mode_selection_builds_proxy_runtime_config_without_netlink_settings
cargo test --locked --bin relayd netlink_runtime_config_from_config_carries_table_and_chain
cargo test --locked runtime::netlink::tests::netlink_create_does_not_bind_relay_tcp_socket
cargo test --locked --bin relayd serve_listener_exits_after_shutdown_signal
cargo test --locked --bin relayd startup_path_serves_authenticated_metrics_from_temp_sqlite
```

Expected: PASS.

## Task 6: Documentation

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture/port-forwarder.md`

- [ ] **Step 1: Update README**

Add runtime configuration bullets near existing environment variable list:

```markdown
- `RELAYD_RUNTIME_MODE` - runtime forwarding mode, `proxy` by default. `proxy` starts relayd-owned TCP/UDP forwarding sockets; `netlink` installs nftables DNAT rules instead.
- `RELAYD_NFTABLES_TABLE` - nftables table for `netlink` mode, default `relayd`.
- `RELAYD_NFTABLES_CHAIN` - nftables chain for `netlink` mode, default `mapping`.
```

Add operational note:

```markdown
In `netlink` runtime mode, relayd manages an `inet` nftables table and a `nat`/`prerouting`/destination-NAT chain. The configured chain is exclusively owned by relayd: startup creates it if needed, flushes it, and rewrites rules from SQLite allocations. This mode requires a binary built with `--features netlink`, nftables/netlink privileges, and system libnftnl/libmnl support. Listener metrics are empty in this mode because forwarding happens in the kernel rather than relayd-owned sockets.
```

Update CLI example with:

```bash
cargo run --locked --bin relayd -- --runtime-mode netlink --nftables-table relayd --nftables-chain mapping --auth-token mytoken
```

- [ ] **Step 2: Update architecture doc**

In `docs/architecture/port-forwarder.md`, add:

```markdown
- Runtime mode `proxy` is the default and owns TCP/UDP listener sockets.
- Runtime mode `netlink` owns an nftables `inet` table/chain and rewrites DNAT rules from allocation state. The chain is relayd-owned and flushed on startup, including when there are no persisted allocations.
- Netlink rule updates are whole-chain replacements; apply failures are non-retryable runtime apply failures and snapshots report `degraded_apply_failed` for bound allocations until a later successful replace.
```

- [ ] **Step 3: Verify docs mention required strings**

Run:

```bash
rg -n "RELAYD_RUNTIME_MODE|--runtime-mode|RELAYD_NFTABLES_TABLE|RELAYD_NFTABLES_CHAIN|netlink|relayd-owned|flushed" README.md docs/architecture/port-forwarder.md
```

Expected: output includes all new config names and ownership/flush behavior.

## Task 7: Full Verification And Review Prep

**Files:**
- All changed files

- [ ] **Step 1: Run targeted test suite**

Run:

```bash
cargo test --locked config::tests
cargo test --locked runtime::facade::tests
cargo test --locked runtime::netlink
cargo test --locked service::allocation_service::tests::restore_all_initializes_runtime_even_with_no_allocations
cargo test --locked service::allocation_service::tests::create_allocation_does_not_retry_non_retryable_runtime_apply_failed
cargo test --locked http::control_plane::tests::runtime_apply_failed_maps_to_503
cargo test --locked --bin relayd cli_help_documents_runtime_options
cargo test --locked --bin relayd cli_options_override_environment_config
cargo test --locked --bin relayd netlink_runtime_config_from_config_carries_table_and_chain
```

Expected: PASS, except commands that cannot link due to missing system `libnftnl`/`libmnl` must be reported with exact linker/build output.

- [ ] **Step 2: Run broad verification**

Run:

```bash
cargo test --locked
```

Expected: PASS when system libnftnl/libmnl build prerequisites are present. If the environment lacks those prerequisites, report the exact failure and keep targeted non-linking evidence where possible.

- [ ] **Step 3: Inspect final diff**

Run:

```bash
git status --short
git diff -- src/config.rs src/bin/relayd.rs src/runtime/facade.rs src/runtime/mod.rs src/runtime/netlink.rs src/service/allocation_service.rs src/http/control_plane.rs Cargo.toml Cargo.lock README.md docs/architecture/port-forwarder.md docs/superpowers/specs/2026-06-25-runtime-mode-netlink-design.md docs/superpowers/plans/2026-06-25-runtime-mode-netlink.md
```

Expected: only intended feature, docs, spec, and plan files changed.

## Self-Review

- Spec coverage: RuntimeMode config, nftables names, default proxy behavior, netlink backend, inet topology, startup initialize/empty flush, whole-chain rewrite, both TCP+UDP rules, IPv4/IPv6, non-retryable apply failure, stale snapshot semantics, metrics behavior, docs, and verification are each covered by tasks.
- Placeholder scan: no TBD/TODO/fill-in placeholders remain; low-level nftnl expression details are constrained to the production backend task with explicit allowed `nftnl_sys` usage.
- Type consistency: `RuntimeMode`, `RuntimeApplyFailed`, `NetlinkRuntimeConfig`, `NetlinkRuntime`, `NftBackend`, `NftDnatRule`, and `NftBackendError` names are consistent across tasks.
