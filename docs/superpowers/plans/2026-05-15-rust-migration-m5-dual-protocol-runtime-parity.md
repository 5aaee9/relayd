# Rust Migration M5 Dual-Protocol Runtime Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a composed real Rust runtime that gives `Protocol::Both` allocations TCP and UDP listeners on the same port with one shared binding target and separate concrete metrics rows.

**Architecture:** Keep `RuntimeFacade` stable. Add `src/runtime/real.rs` with `RealRuntime` and `RealRuntimeConfig` that own a `TcpRuntime` and `UdpRuntime`, delegate single-protocol calls unchanged, and expand `Both` allocations into concrete TCP and UDP allocation copies with the same id/port/host/target. Use compensating cleanup for partial create/restore failures and aggregate snapshots for one API row.

**Tech Stack:** Rust 1.95, Tokio TCP/UDP loopback sockets, existing `RuntimeFacade`, existing `TcpRuntime`/`UdpRuntime`, SeaORM/SQLx repository/service tests, `cargo test --locked`, `cargo clippy --locked --lib --tests -- -D warnings`, `zig build test`.

---

## File Structure

- Modify: `src/runtime/mod.rs` — export `real` runtime module.
- Create: `src/runtime/real.rs` — composed runtime facade, concrete allocation conversion helpers, aggregate snapshot logic, and dual-protocol runtime/service tests.
- Modify after implementation: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md` — add M5 implementation status.
- Preserve: `src/runtime/tcp.rs`, `src/runtime/udp.rs`, `src/runtime/facade.rs`, `src/model.rs`, and HTTP resource definitions unless tests expose a necessary bug fix.

## Acceptance checklist

- [ ] Independent plan reviewer returns `APPROVED` before implementation.
- [ ] `src/runtime/real.rs` defines `RealRuntimeConfig` and `RealRuntime` implementing `RuntimeFacade`.
- [ ] `Protocol::Tcp` and `Protocol::Udp` delegate to existing concrete runtimes without behavior changes.
- [ ] `Protocol::Both` create binds TCP and UDP listeners on the same port and cleans up partial failure.
- [ ] `Protocol::Both` update applies one binding target to both listeners.
- [ ] `Protocol::Both` delete closes both listeners/sessions and releases both transports.
- [ ] `Protocol::Both` restore recreates both listeners and cleans up partial failure.
- [ ] Aggregate `snapshot` preserves one API row for a `both` allocation while surfacing missing/degraded concrete state.
- [ ] `snapshot_listener_metrics` returns separate TCP and UDP rows for `both` ports.
- [ ] Dual-protocol tests cover no-host rejection, forwarding, delete release, restore, bind failure, and Prometheus rows.
- [ ] `cargo fmt -- --check` passes.
- [ ] `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked` passes.
- [ ] `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings` passes.
- [ ] `zig build test` passes.
- [ ] Final independent spec-compliance reviewer returns `APPROVED` before docs commit/push.

## Task 1: Add composed runtime skeleton and single-protocol delegation

**Files:**
- Modify: `src/runtime/mod.rs`
- Create: `src/runtime/real.rs`

- [ ] **Step 1: Write failing delegation tests**

Create `src/runtime/real.rs` with tests that use real loopback listeners and prove TCP and UDP single-protocol allocations still work through the composed runtime:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::model::{Allocation, Protocol};
    use crate::runtime::facade::RuntimeFacade;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream, UdpSocket};
    use tokio::task::JoinHandle;
    use tokio::time::{timeout, Duration};

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

    async fn free_tcp_port() -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        listener.local_addr().unwrap().port()
    }

    async fn free_udp_port() -> u16 {
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        socket.local_addr().unwrap().port()
    }

    async fn start_tcp_echo_server() -> (u16, JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 { break; }
                        let _ = socket.write_all(&buf[..n]).await;
                    }
                });
            }
        });
        (port, task)
    }

    async fn start_udp_echo_server_on(port: u16) -> JoinHandle<()> {
        let socket = UdpSocket::bind(("127.0.0.1", port)).await.unwrap();
        tokio::spawn(async move {
            let mut buf = [0_u8; 2048];
            while let Ok((n, peer)) = socket.recv_from(&mut buf).await {
                let _ = socket.send_to(&buf[..n], peer).await;
            }
        })
    }

    async fn start_tcp_echo_server_on(port: u16) -> JoinHandle<()> {
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 { break; }
                        let _ = socket.write_all(&buf[..n]).await;
                    }
                });
            }
        })
    }

    async fn start_udp_echo_server() -> (u16, JoinHandle<()>) {
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let mut buf = [0_u8; 2048];
            while let Ok((n, peer)) = socket.recv_from(&mut buf).await {
                let _ = socket.send_to(&buf[..n], peer).await;
            }
        });
        (port, task)
    }

    async fn tcp_round_trip(relay_port: u16, payload: &[u8]) {
        let mut stream = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
        stream.write_all(payload).await.unwrap();
        let mut buf = vec![0_u8; payload.len()];
        timeout(Duration::from_secs(1), stream.read_exact(&mut buf)).await.unwrap().unwrap();
        assert_eq!(buf, payload);
    }

    async fn udp_round_trip(relay_port: u16, payload: &[u8]) {
        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        client.send_to(payload, ("127.0.0.1", relay_port)).await.unwrap();
        let mut buf = [0_u8; 2048];
        let (n, _) = timeout(Duration::from_secs(1), client.recv_from(&mut buf)).await.unwrap().unwrap();
        assert_eq!(&buf[..n], payload);
    }

    #[tokio::test]
    async fn real_runtime_delegates_single_protocol_tcp_and_udp_forwarding() {
        let metrics = Arc::new(Metrics::default());
        let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics));
        let tcp_relay = free_tcp_port().await;
        let udp_relay = free_udp_port().await;
        let (tcp_target, tcp_task) = start_tcp_echo_server().await;
        let (udp_target, udp_task) = start_udp_echo_server().await;

        runtime.create(&allocation("tcp", Protocol::Tcp, tcp_relay, Some(tcp_target), Some("127.0.0.1")), 500).await.unwrap();
        runtime.create(&allocation("udp", Protocol::Udp, udp_relay, Some(udp_target), Some("127.0.0.1")), 500).await.unwrap();

        tcp_round_trip(tcp_relay, b"tcp-only").await;
        udp_round_trip(udp_relay, b"udp-only").await;

        runtime.delete("tcp", 500).await.unwrap();
        runtime.delete("udp", 500).await.unwrap();
        tcp_task.abort();
        udp_task.abort();
    }
}
```

- [ ] **Step 2: Run red test**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked runtime::real::tests::real_runtime_delegates_single_protocol_tcp_and_udp_forwarding -- --nocapture
```

Expected: FAIL to compile because `runtime::real`, `RealRuntime`, or `RealRuntimeConfig` does not exist/export yet.

- [ ] **Step 3: Export module and implement minimal composed runtime**

Update `src/runtime/mod.rs`:

```rust
pub mod facade;
pub mod real;
pub mod tcp;
pub mod udp;
```

Implement `src/runtime/real.rs`:

```rust
use crate::metrics::Metrics;
use crate::model::{Allocation, ErrorKind, Protocol, RuntimeStatus};
use crate::runtime::facade::{ListenerMetricsSnapshot, ObservedState, RuntimeError, RuntimeFacade};
use crate::runtime::tcp::{TcpRuntime, TcpRuntimeConfig};
use crate::runtime::udp::{UdpRuntime, UdpRuntimeConfig};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;

#[derive(Clone)]
pub struct RealRuntimeConfig {
    metrics: Arc<Metrics>,
    udp_session_ttl: Duration,
}

impl RealRuntimeConfig {
    pub fn loopback(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            udp_session_ttl: Duration::from_millis(60_000),
        }
    }

    pub fn with_udp_session_ttl(mut self, ttl: Duration) -> Self {
        self.udp_session_ttl = ttl;
        self
    }
}

#[derive(Clone)]
pub struct RealRuntime {
    tcp: TcpRuntime,
    udp: UdpRuntime,
    protocols: Arc<Mutex<HashMap<String, Protocol>>>,
}

impl RealRuntime {
    pub fn new(config: RealRuntimeConfig) -> Self {
        let tcp = TcpRuntime::new(TcpRuntimeConfig::loopback(config.metrics.clone()));
        let udp = UdpRuntime::new(
            UdpRuntimeConfig::loopback(config.metrics.clone()).with_session_ttl(config.udp_session_ttl),
        );
        Self {
            tcp,
            udp,
            protocols: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn as_protocol(allocation: &Allocation, protocol: Protocol) -> Allocation {
        Allocation { protocol, ..allocation.clone() }
    }
}

#[async_trait]
impl RuntimeFacade for RealRuntime {
    async fn create(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError> {
        match allocation.protocol {
            Protocol::Tcp => {
                self.tcp.create(allocation, timeout_ms).await?;
                self.protocols.lock().await.insert(allocation.id.clone(), Protocol::Tcp);
                Ok(())
            }
            Protocol::Udp => {
                self.udp.create(allocation, timeout_ms).await?;
                self.protocols.lock().await.insert(allocation.id.clone(), Protocol::Udp);
                Ok(())
            }
            Protocol::Both => {
                let tcp = Self::as_protocol(allocation, Protocol::Tcp);
                let udp = Self::as_protocol(allocation, Protocol::Udp);
                self.tcp.create(&tcp, timeout_ms).await?;
                if let Err(error) = self.udp.create(&udp, timeout_ms).await {
                    let _ = self.tcp.delete(&allocation.id, timeout_ms).await;
                    return Err(match error {
                        RuntimeError::RuntimeCreateFailed => RuntimeError::RuntimeCreateFailed,
                        other => other,
                    });
                }
                self.protocols.lock().await.insert(allocation.id.clone(), Protocol::Both);
                Ok(())
            }
        }
    }

    async fn update(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError> {
        match allocation.protocol {
            Protocol::Tcp => self.tcp.update(allocation, timeout_ms).await,
            Protocol::Udp => self.udp.update(allocation, timeout_ms).await,
            Protocol::Both => {
                self.tcp.update(&Self::as_protocol(allocation, Protocol::Tcp), timeout_ms).await?;
                self.udp.update(&Self::as_protocol(allocation, Protocol::Udp), timeout_ms).await
            }
        }
    }

    async fn delete(&self, id: &str, timeout_ms: u32) -> Result<(), RuntimeError> {
        self.protocols.lock().await.remove(id);
        let tcp = self.tcp.delete(id, timeout_ms).await;
        let udp = self.udp.delete(id, timeout_ms).await;
        tcp.and(udp)
    }

    async fn restore(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError> {
        match allocation.protocol {
            Protocol::Tcp => {
                self.tcp.restore(allocation, timeout_ms).await?;
                self.protocols.lock().await.insert(allocation.id.clone(), Protocol::Tcp);
                Ok(())
            }
            Protocol::Udp => {
                self.udp.restore(allocation, timeout_ms).await?;
                self.protocols.lock().await.insert(allocation.id.clone(), Protocol::Udp);
                Ok(())
            }
            Protocol::Both => {
                self.tcp.restore(&Self::as_protocol(allocation, Protocol::Tcp), timeout_ms).await?;
                if let Err(error) = self.udp.restore(&Self::as_protocol(allocation, Protocol::Udp), timeout_ms).await {
                    let _ = self.tcp.delete(&allocation.id, timeout_ms).await;
                    return Err(match error {
                        RuntimeError::RuntimeRestoreFailed => RuntimeError::RuntimeRestoreFailed,
                        other => other,
                    });
                }
                self.protocols.lock().await.insert(allocation.id.clone(), Protocol::Both);
                Ok(())
            }
        }
    }

    async fn snapshot(&self, id: &str) -> Result<Option<ObservedState>, RuntimeError> {
        match self.protocols.lock().await.get(id).copied() {
            Some(Protocol::Tcp) => self.tcp.snapshot(id).await,
            Some(Protocol::Udp) => self.udp.snapshot(id).await,
            Some(Protocol::Both) => {
                let tcp = self.tcp.snapshot(id).await?;
                let udp = self.udp.snapshot(id).await?;
                Ok(aggregate_snapshot(tcp, udp))
            }
            None => Ok(None),
        }
    }

    async fn snapshot_listener_metrics(&self) -> Result<Vec<ListenerMetricsSnapshot>, RuntimeError> {
        let mut rows = self.tcp.snapshot_listener_metrics().await?;
        rows.extend(self.udp.snapshot_listener_metrics().await?);
        rows.sort_by_key(|row| (row.port, row.protocol.as_str()));
        Ok(rows)
    }
}

fn aggregate_snapshot(tcp: Option<ObservedState>, udp: Option<ObservedState>) -> Option<ObservedState> {
    match (tcp, udp) {
        (Some(tcp), Some(udp)) if tcp == udp => Some(tcp),
        (Some(tcp), None) => Some(degraded_from(tcp, "missing udp runtime state")),
        (None, Some(udp)) => Some(degraded_from(udp, "missing tcp runtime state")),
        (Some(tcp), Some(udp)) => {
            if tcp == udp { Some(tcp) }
            else { Some(degraded_from(tcp, "divergent tcp/udp runtime state")) }
        }
        (None, None) => None,
    }
}

fn degraded_from(mut state: ObservedState, message: &str) -> ObservedState {
    state.runtime_status = RuntimeStatus::DegradedApplyFailed;
    state.error_kind = Some(ErrorKind::ApplyFailed);
    state.last_error = Some(message.to_owned());
    state
}
```

- [ ] **Step 4: Run green test**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked runtime::real::tests::real_runtime_delegates_single_protocol_tcp_and_udp_forwarding -- --nocapture
```

Expected: PASS.

## Task 2: Add dual-protocol no-host, create, forwarding, and concrete metrics

**Files:**
- Modify: `src/runtime/real.rs`

- [ ] **Step 1: Add failing dual-protocol forwarding test**

Append this test to `src/runtime/real.rs` tests:

```rust
#[tokio::test]
async fn real_runtime_both_allocation_forwards_tcp_and_udp_on_same_port_and_reports_rows() {
    let metrics = Arc::new(Metrics::default());
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_tcp_port().await;
    let target_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let target_port = target_listener.local_addr().unwrap().port();
    drop(target_listener);
    let tcp_task = start_tcp_echo_server_on(target_port).await;
    let udp_task = start_udp_echo_server_on(target_port).await;

    runtime.create(&allocation("both", Protocol::Both, relay_port, None, None), 500).await.unwrap();
    let observed = runtime.snapshot("both").await.unwrap().unwrap();
    assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
    assert_eq!(observed.effective_host, None);
    assert_eq!(observed.effective_target_port, None);

    runtime.update(&allocation("both", Protocol::Both, relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();
    tcp_round_trip(relay_port, b"both-tcp").await;
    udp_round_trip(relay_port, b"both-udp").await;

    let rows = runtime.snapshot_listener_metrics().await.unwrap();
    let tcp_row = rows.iter().find(|row| row.port == relay_port && row.protocol == Protocol::Tcp).unwrap();
    let udp_row = rows.iter().find(|row| row.port == relay_port && row.protocol == Protocol::Udp).unwrap();
    assert!(tcp_row.connections_current <= 1);
    assert!(tcp_row.rx_bytes_total > 0 || tcp_row.tx_bytes_total > 0);
    assert_eq!(udp_row.connections_current, 1);
    assert!(udp_row.rx_bytes_total > 0);
    assert!(udp_row.tx_bytes_total > 0);

    let rendered = crate::prometheus::render(&rows);
    assert!(rendered.contains(&format!("relayd_connections_current{{port=\"{}\",protocol=\"tcp\"}}", relay_port)));
    assert!(rendered.contains(&format!("relayd_connections_current{{port=\"{}\",protocol=\"udp\"}}", relay_port)));

    runtime.delete("both", 500).await.unwrap();
    tcp_task.abort();
    udp_task.abort();
}
```

- [ ] **Step 2: Run test and fix compile imports**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked runtime::real::tests::real_runtime_both_allocation_forwards_tcp_and_udp_on_same_port_and_reports_rows -- --nocapture
```

Expected: FAIL if imports for `RuntimeStatus` or helper visibility are missing. Add `RuntimeStatus` to the test imports and keep helper functions inside the same test module.

- [ ] **Step 3: Adjust aggregate snapshot if needed**

If the test fails because aggregate snapshot reports degraded while both concrete states are semantically equal, normalize the aggregate comparison to compare `effective_target_port`, `effective_host`, `runtime_status`, `error_kind`, and `last_error`. Keep no API shape changes.

- [ ] **Step 4: Run green test**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked runtime::real::tests::real_runtime_both_allocation_forwards_tcp_and_udp_on_same_port_and_reports_rows -- --nocapture
```

Expected: PASS.

## Task 3: Add dual-protocol delete release and service port-skip behavior

**Files:**
- Modify: `src/runtime/real.rs`

- [ ] **Step 1: Add failing delete release and busy-port tests**

Append tests:

```rust
async fn reserve_tcp_udp_port_pair() -> (TcpListener, UdpSocket, u16) {
    for _ in 0..200 {
        let tcp = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = tcp.local_addr().unwrap().port();
        if let Ok(udp) = UdpSocket::bind(("127.0.0.1", port)).await {
            return (tcp, udp, port);
        }
    }
    panic!("could not reserve tcp+udp port pair");
}

#[tokio::test]
async fn real_runtime_both_delete_releases_tcp_and_udp_ports() {
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let relay_port = free_tcp_port().await;
    runtime.create(&allocation("both-delete", Protocol::Both, relay_port, None, None), 500).await.unwrap();

    runtime.delete("both-delete", 500).await.unwrap();

    let tcp_rebound = TcpListener::bind(("127.0.0.1", relay_port)).await.unwrap();
    let udp_rebound = UdpSocket::bind(("127.0.0.1", relay_port)).await.unwrap();
    assert_eq!(tcp_rebound.local_addr().unwrap().port(), relay_port);
    assert_eq!(udp_rebound.local_addr().unwrap().port(), relay_port);
    assert!(runtime.snapshot_listener_metrics().await.unwrap().is_empty());
}

#[tokio::test]
async fn real_runtime_both_create_cleans_partial_tcp_when_udp_port_is_busy() {
    let metrics = Arc::new(Metrics::default());
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let tcp_probe = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = tcp_probe.local_addr().unwrap().port();
    drop(tcp_probe);
    let busy_udp = UdpSocket::bind(("127.0.0.1", port)).await.unwrap();

    let error = runtime.create(&allocation("both-busy-udp", Protocol::Both, port, None, None), 500).await.unwrap_err();
    assert_eq!(error, RuntimeError::RuntimeCreateFailed);
    drop(busy_udp);

    let tcp_rebound = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let udp_rebound = UdpSocket::bind(("127.0.0.1", port)).await.unwrap();
    assert_eq!(tcp_rebound.local_addr().unwrap().port(), port);
    assert_eq!(udp_rebound.local_addr().unwrap().port(), port);
    assert!(runtime.snapshot("both-busy-udp").await.unwrap().is_none());
    assert!(metrics.bind_fail_total.load() >= 1);
}
```

- [ ] **Step 2: Run red/green target**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked runtime::real::tests::real_runtime_both_ -- --nocapture
```

Expected: PASS for the dual-protocol tests added so far. If cleanup is incomplete, fix `RealRuntime::create` to delete the TCP side after UDP create failure and re-run.

## Task 4: Add dual-protocol restore behavior with persisted service state

**Files:**
- Modify: `src/runtime/real.rs`

- [ ] **Step 1: Add test repository helpers and failing restore test**

Add imports in the test module:

```rust
use crate::config::PortRange;
use crate::service::allocation_service::Service;
use crate::storage::sqlite::Repository;
use std::path::PathBuf;
```

Add helpers:

```rust
fn temp_db_path() -> PathBuf {
    let parent = std::env::current_dir().unwrap().join("target/relayd-test-dbs");
    std::fs::create_dir_all(&parent).unwrap();
    let dir = tempfile::tempdir_in(parent).unwrap().keep();
    dir.join("relayd.sqlite")
}

async fn temp_repo_with_path() -> (Repository, PathBuf) {
    let path = temp_db_path();
    let repo = Repository::open(&path).await.unwrap();
    (repo, path)
}

fn service(repo: Repository, runtime: RealRuntime, start: u16, end: u16) -> Service<RealRuntime> {
    Service::new(repo, runtime, PortRange { start, end }, 500)
}
```

Append restore test:

```rust
#[tokio::test]
async fn real_runtime_both_restore_recreates_tcp_and_udp_forwarding() {
    let relay_port = free_tcp_port().await;
    let target_probe = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let target_port = target_probe.local_addr().unwrap().port();
    drop(target_probe);
    let tcp_task = start_tcp_echo_server_on(target_port).await;
    let udp_task = start_udp_echo_server_on(target_port).await;
    let (repo, path) = temp_repo_with_path().await;
    let runtime_one = RealRuntime::new(RealRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let svc_one = service(repo, runtime_one.clone(), relay_port, relay_port);
    let alloc = svc_one.create_allocation(Protocol::Both, Some(target_port)).await.unwrap();
    svc_one.set_target(&alloc.id, "127.0.0.1").await.unwrap();
    runtime_one.delete(&alloc.id, 500).await.unwrap();
    drop(svc_one);

    let repo_two = Repository::open(&path).await.unwrap();
    let runtime_two = RealRuntime::new(RealRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let svc_two = service(repo_two, runtime_two.clone(), relay_port, relay_port);
    svc_two.restore_all(500).await.unwrap();

    tcp_round_trip(relay_port, b"restore-tcp").await;
    udp_round_trip(relay_port, b"restore-udp").await;

    runtime_two.delete(&alloc.id, 500).await.unwrap();
    tcp_task.abort();
    udp_task.abort();
}
```

- [ ] **Step 2: Run restore test**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked runtime::real::tests::real_runtime_both_restore_recreates_tcp_and_udp_forwarding -- --nocapture
```

Expected: PASS after Task 1 restore logic. If partial restore cleanup fails, fix `RealRuntime::restore` to delete the TCP side after UDP restore failure and return `RuntimeRestoreFailed`.

## Task 5: Add no-host rejection and final docs/status

**Files:**
- Modify: `src/runtime/real.rs`
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`

- [ ] **Step 1: Add dual no-host rejection test**

Append test:

```rust
#[tokio::test]
async fn real_runtime_both_no_host_rejects_tcp_and_udp_without_sessions() {
    let metrics = Arc::new(Metrics::default());
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_tcp_port().await;
    runtime.create(&allocation("both-no-host", Protocol::Both, relay_port, None, None), 500).await.unwrap();

    let mut tcp = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
    tcp.write_all(b"drop-tcp").await.unwrap();
    let mut tcp_buf = [0_u8; 16];
    assert!(timeout(Duration::from_millis(200), tcp.read(&mut tcp_buf)).await.is_err()
        || tcp.read(&mut tcp_buf).await.unwrap_or(0) == 0);

    let udp = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    udp.send_to(b"drop-udp", ("127.0.0.1", relay_port)).await.unwrap();
    let mut udp_buf = [0_u8; 16];
    assert!(timeout(Duration::from_millis(150), udp.recv_from(&mut udp_buf)).await.is_err());

    let observed = runtime.snapshot("both-no-host").await.unwrap().unwrap();
    assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
    assert_eq!(observed.effective_host, None);
    assert_eq!(observed.effective_target_port, None);
    assert_eq!(metrics.udp_session_create_total.load(), 0);
    assert_eq!(metrics.udp_active_sessions.load(), 0);
    assert!(metrics.rejected_no_host_total.load() >= 1);

    runtime.delete("both-no-host", 500).await.unwrap();
}
```

- [ ] **Step 2: Run all real runtime tests**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked runtime::real::tests -- --nocapture
```

Expected: all M5 real runtime tests PASS.

- [ ] **Step 3: Update milestone ledger**

Append to `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`:

```markdown
## M5 implementation status

- Status: implemented in Rust composed real runtime with dual-protocol create/update/delete/restore, shared binding target, TCP+UDP forwarding on the same port, aggregate snapshots, and concrete TCP/UDP listener metrics rows.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Scope note: Full Prometheus rate semantics and Rust binary production cutover remain assigned to M6-M7.
```

- [ ] **Step 4: Run full verification**

Run:

```bash
cargo fmt -- --check
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings
zig build test
```

Expected: all commands PASS with no clippy warnings.

- [ ] **Step 5: Request final independent spec-compliance review**

Dispatch an independent reviewer with the M5 spec, plan, changed files, and verification evidence. Fix any `BLOCKED` items and repeat verification/review until the reviewer returns `APPROVED`.

- [ ] **Step 6: Commit and push**

After approval, commit with Lore protocol:

```bash
git add src/runtime/mod.rs src/runtime/real.rs \
  docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md \
  docs/superpowers/specs/2026-05-15-rust-migration-m5-dual-protocol-runtime-parity.md \
  docs/superpowers/plans/2026-05-15-rust-migration-m5-dual-protocol-runtime-parity.md

git commit -m "Make dual-protocol allocations real in Rust runtime" \
  -m "Compose the TCP and UDP runtimes so a both allocation owns concrete listeners for both transports on the same port, shares one binding target, restores and deletes both sides together, and exposes separate listener metrics rows without changing API definitions." \
  -m "Constraint: Existing Zig HTTP API/resource definitions and single-protocol runtime behavior must remain unchanged." \
  -m "Rejected: Folding TCP and UDP implementations into one monolithic runtime | Composition preserves M3/M4 behavior and keeps dual-protocol orchestration isolated." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Directive: Leave Prometheus byte-rate calculation and Rust production cutover for M6-M7." \
  -m "Tested: cargo fmt -- --check; TMPDIR=\$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked; TMPDIR=\$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings; zig build test" \
  -m "Not-tested: optional TCP session-model/splice and UDP worker/io_uring/GRO/fast-path lanes" \
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"

git push
```

## Plan Review Required Revisions (integrate before implementation)

The independent plan review blocked the first M5 plan. The following revisions are mandatory parts of implementation and verification; workers must treat them as acceptance criteria, not optional extras.

### Revision A: Non-flaky dual-protocol port helper

For every test that creates a `Protocol::Both` relay listener, use this helper instead of `free_tcp_port()`:

```rust
async fn free_tcp_udp_port_pair() -> u16 {
    for _ in 0..200 {
        let tcp = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = tcp.local_addr().unwrap().port();
        if let Ok(udp) = UdpSocket::bind(("127.0.0.1", port)).await {
            drop(udp);
            drop(tcp);
            return port;
        }
    }
    panic!("could not find free tcp+udp port pair");
}
```

Replace `let relay_port = free_tcp_port().await;` in `Protocol::Both` tests with:

```rust
let relay_port = free_tcp_udp_port_pair().await;
```

### Revision B: Update-back-to-no-host and service delete-binding coverage

Add this test to `src/runtime/real.rs` after the forwarding test:

```rust
#[tokio::test]
async fn real_runtime_both_update_back_to_no_host_closes_sessions_and_rejects_new_traffic() {
    let metrics = Arc::new(Metrics::default());
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_tcp_udp_port_pair().await;
    let target_probe = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let target_port = target_probe.local_addr().unwrap().port();
    drop(target_probe);
    let tcp_task = start_tcp_echo_server_on(target_port).await;
    let udp_task = start_udp_echo_server_on(target_port).await;

    runtime.create(&allocation("both-nohost-update", Protocol::Both, relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();
    tcp_round_trip(relay_port, b"tcp-before").await;
    udp_round_trip(relay_port, b"udp-before").await;

    runtime.update(&allocation("both-nohost-update", Protocol::Both, relay_port, None, None), 500).await.unwrap();
    let observed = runtime.snapshot("both-nohost-update").await.unwrap().unwrap();
    assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
    assert_eq!(observed.effective_host, None);
    assert_eq!(observed.effective_target_port, None);

    let mut tcp = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
    tcp.write_all(b"tcp-after").await.unwrap();
    let mut tcp_buf = [0_u8; 16];
    let tcp_read = timeout(Duration::from_millis(250), tcp.read(&mut tcp_buf)).await;
    assert!(tcp_read.is_err() || tcp_read.unwrap().unwrap_or(0) == 0);

    let udp = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    udp.send_to(b"udp-after", ("127.0.0.1", relay_port)).await.unwrap();
    let mut udp_buf = [0_u8; 16];
    assert!(timeout(Duration::from_millis(150), udp.recv_from(&mut udp_buf)).await.is_err());
    assert_eq!(metrics.udp_active_sessions.load(), 0);
    assert!(metrics.rejected_no_host_total.load() >= 2);

    runtime.delete("both-nohost-update", 500).await.unwrap();
    tcp_task.abort();
    udp_task.abort();
}
```

Add this service-level delete-binding test after repository helpers from Task 4 are available:

```rust
#[tokio::test]
async fn real_service_both_delete_binding_drives_both_listeners_back_to_no_host() {
    let metrics = Arc::new(Metrics::default());
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_tcp_udp_port_pair().await;
    let target_probe = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let target_port = target_probe.local_addr().unwrap().port();
    drop(target_probe);
    let tcp_task = start_tcp_echo_server_on(target_port).await;
    let udp_task = start_udp_echo_server_on(target_port).await;
    let (repo, _path) = temp_repo_with_path().await;
    let svc = service(repo, runtime.clone(), relay_port, relay_port);

    let alloc = svc.create_allocation(Protocol::Both, Some(target_port)).await.unwrap();
    svc.set_target(&alloc.id, "127.0.0.1").await.unwrap();
    tcp_round_trip(relay_port, b"tcp-bound").await;
    udp_round_trip(relay_port, b"udp-bound").await;

    svc.delete_binding(&alloc.id).await.unwrap();
    let observed = runtime.snapshot(&alloc.id).await.unwrap().unwrap();
    assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
    assert_eq!(observed.effective_host, None);
    assert_eq!(observed.effective_target_port, None);

    let udp = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    udp.send_to(b"udp-after-delete-binding", ("127.0.0.1", relay_port)).await.unwrap();
    let mut udp_buf = [0_u8; 32];
    assert!(timeout(Duration::from_millis(150), udp.recv_from(&mut udp_buf)).await.is_err());
    assert_eq!(metrics.udp_active_sessions.load(), 0);

    svc.delete_allocation(&alloc.id).await.unwrap();
    tcp_task.abort();
    udp_task.abort();
}
```

### Revision C: Complete create bind-failure and service port-skip coverage

Add these tests in Task 3 in addition to the UDP-busy partial-cleanup test:

```rust
#[tokio::test]
async fn real_runtime_both_create_with_busy_tcp_does_not_bind_udp() {
    let metrics = Arc::new(Metrics::default());
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let busy_tcp = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = busy_tcp.local_addr().unwrap().port();

    let error = runtime.create(&allocation("both-busy-tcp", Protocol::Both, port, None, None), 500).await.unwrap_err();
    assert_eq!(error, RuntimeError::RuntimeCreateFailed);
    assert!(UdpSocket::bind(("127.0.0.1", port)).await.is_ok());
    assert!(runtime.snapshot("both-busy-tcp").await.unwrap().is_none());
    assert!(metrics.bind_fail_total.load() >= 1);
}

#[tokio::test]
async fn real_service_both_create_skips_ports_busy_for_tcp_or_udp() {
    let metrics = Arc::new(Metrics::default());
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let first_busy_tcp = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let start = first_busy_tcp.local_addr().unwrap().port();
    let second = start + 1;
    let third = start + 2;
    let second_tcp_probe = TcpListener::bind(("127.0.0.1", second)).await.unwrap();
    drop(second_tcp_probe);
    let second_busy_udp = UdpSocket::bind(("127.0.0.1", second)).await.unwrap();
    let third_tcp_probe = TcpListener::bind(("127.0.0.1", third)).await.unwrap();
    drop(third_tcp_probe);
    let third_udp_probe = UdpSocket::bind(("127.0.0.1", third)).await.unwrap();
    drop(third_udp_probe);

    let (repo, _path) = temp_repo_with_path().await;
    let svc = service(repo, runtime, start, third);
    let allocation = svc.create_allocation(Protocol::Both, None).await.unwrap();

    assert_eq!(allocation.port, third);
    assert!(metrics.bind_fail_total.load() >= 2);
    drop(second_busy_udp);
}
```

If `start + 2` overflows or the sequential ports are not available, replace the setup with a bounded helper that finds three consecutive ports where the first can hold busy TCP, the second can hold busy UDP, and the third can bind both TCP and UDP.

### Revision D: Restore-failure cleanup coverage

Add tests:

```rust
#[tokio::test]
async fn real_runtime_both_restore_fails_when_tcp_busy_without_binding_udp() {
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let busy_tcp = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = busy_tcp.local_addr().unwrap().port();

    let error = runtime.restore(&allocation("both-restore-tcp-busy", Protocol::Both, port, None, None), 500).await.unwrap_err();
    assert_eq!(error, RuntimeError::RuntimeRestoreFailed);
    assert!(UdpSocket::bind(("127.0.0.1", port)).await.is_ok());
    assert!(runtime.snapshot("both-restore-tcp-busy").await.unwrap().is_none());
}

#[tokio::test]
async fn real_runtime_both_restore_cleans_tcp_when_udp_busy() {
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let port = free_tcp_udp_port_pair().await;
    let busy_udp = UdpSocket::bind(("127.0.0.1", port)).await.unwrap();

    let error = runtime.restore(&allocation("both-restore-udp-busy", Protocol::Both, port, None, None), 500).await.unwrap_err();
    assert_eq!(error, RuntimeError::RuntimeRestoreFailed);
    drop(busy_udp);
    assert!(TcpListener::bind(("127.0.0.1", port)).await.is_ok());
    assert!(UdpSocket::bind(("127.0.0.1", port)).await.is_ok());
    assert!(runtime.snapshot("both-restore-udp-busy").await.unwrap().is_none());
}
```

`RealRuntime::restore` must map any concrete create-side restore error to `RuntimeRestoreFailed` for `Protocol::Both` and delete partial concrete state.

### Revision E: Aggregate snapshot degraded-state coverage

Add test-only constructors or helper methods if necessary. Do not change `RuntimeFacade`. Preferred helper in `impl RealRuntime` behind `#[cfg(test)]`:

```rust
#[cfg(test)]
async fn remember_protocol_for_test(&self, id: &str, protocol: Protocol) {
    self.protocols.lock().await.insert(id.to_owned(), protocol);
}
```

Add tests for pure aggregate helper and protocol registry behavior:

```rust
#[test]
fn aggregate_snapshot_reports_degraded_for_missing_or_divergent_concrete_state() {
    let active = ObservedState {
        effective_target_port: Some(8080),
        effective_host: Some("127.0.0.1".to_owned()),
        runtime_status: RuntimeStatus::Active,
        error_kind: None,
        last_error: None,
    };
    let rejecting = ObservedState {
        effective_target_port: None,
        effective_host: None,
        runtime_status: RuntimeStatus::RejectingNoHost,
        error_kind: None,
        last_error: None,
    };

    let missing_udp = aggregate_snapshot(Some(active.clone()), None).unwrap();
    assert_eq!(missing_udp.runtime_status, RuntimeStatus::DegradedApplyFailed);
    assert_eq!(missing_udp.error_kind, Some(ErrorKind::ApplyFailed));
    assert_eq!(missing_udp.last_error.as_deref(), Some("missing udp runtime state"));

    let missing_tcp = aggregate_snapshot(None, Some(active.clone())).unwrap();
    assert_eq!(missing_tcp.runtime_status, RuntimeStatus::DegradedApplyFailed);
    assert_eq!(missing_tcp.error_kind, Some(ErrorKind::ApplyFailed));
    assert_eq!(missing_tcp.last_error.as_deref(), Some("missing tcp runtime state"));

    let divergent = aggregate_snapshot(Some(active), Some(rejecting)).unwrap();
    assert_eq!(divergent.runtime_status, RuntimeStatus::DegradedApplyFailed);
    assert_eq!(divergent.error_kind, Some(ErrorKind::ApplyFailed));
    assert_eq!(divergent.last_error.as_deref(), Some("divergent tcp/udp runtime state"));
}
```

### Revision F: Explicit shutdown cleanup

Add an explicit shutdown method to `RealRuntime`:

```rust
impl RealRuntime {
    pub async fn shutdown_all(&self) {
        self.protocols.lock().await.clear();
        let tcp = self.tcp.shutdown_all();
        let udp = self.udp.shutdown_all();
        tokio::join!(tcp, udp);
    }
}
```

If `TcpRuntime` lacks `shutdown_all`, add it using the same pattern as `UdpRuntime::shutdown_all`: drain entries and stop accept/session tasks. The required deterministic API is explicit `shutdown_all()`. Do not claim drop cleanup unless a separate deterministic test is added and passing; M5 acceptance is explicit shutdown cleanup.

Add test:

```rust
#[tokio::test]
async fn real_runtime_shutdown_all_releases_both_tcp_and_udp_listeners() {
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let relay_port = free_tcp_udp_port_pair().await;
    runtime.create(&allocation("both-shutdown", Protocol::Both, relay_port, None, None), 500).await.unwrap();

    runtime.shutdown_all().await;

    assert!(TcpListener::bind(("127.0.0.1", relay_port)).await.is_ok());
    assert!(UdpSocket::bind(("127.0.0.1", relay_port)).await.is_ok());
    assert!(runtime.snapshot_listener_metrics().await.unwrap().is_empty());
}
```

### Revision G: Full verification and review gate remains unchanged

After applying all revised tasks, run:

```bash
cargo fmt -- --check
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings
zig build test
```

Then dispatch final independent spec-compliance review. Do not commit or push M5 until the final reviewer returns `APPROVED`.
