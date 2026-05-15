# Rust Migration M3 Baseline TCP Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the real Rust baseline TCP runtime so TCP allocations bind loopback listeners, reject while unbound, forward bytes after binding, expose TCP listener metrics, delete cleanly, and restore from SQLite.

**Architecture:** Keep the existing `RuntimeFacade` trait stable and add a `src/runtime/tcp.rs` implementation using Tokio `TcpListener`, per-allocation shared state, cancellation tokens, and per-session `JoinHandle`s. The allocation service remains the owner of persistence/orchestration; the TCP runtime owns only socket lifecycle, observed runtime state, session closure, and listener metric snapshots.

**Tech Stack:** Rust 1.95, Tokio `net`/`io-util`/`sync`/`time`, existing `async-trait`, SeaORM/SQLx repository, Axum M2 HTTP modules, `cargo test --locked`, `cargo clippy --locked --lib --tests -- -D warnings`, `zig build test`.

---

## File Structure

- Modify: `Cargo.toml` — expand Tokio features from `macros`, `rt-multi-thread` to include `net`, `io-util`, `sync`, and `time`.
- Modify: `src/runtime/mod.rs` — export the new TCP runtime module.
- Modify: `src/metrics.rs` — add `Gauge::set_zero_for_test_or_shutdown` only if task implementation needs a saturating reset; otherwise leave metrics API unchanged.
- Create: `src/runtime/tcp.rs` — `TcpRuntime`, `TcpRuntimeConfig`, listener entry state, session tracking, copy loop, and real-runtime tests.
- Modify: `src/prometheus.rs` only if the test needs to assert nonzero rows from real runtime; renderer behavior must stay M2-compatible.
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md` after implementation — add M3 status block.
- Existing source references: `src/runtime/manager.zig`, `tests/integration/service_forward_test.zig`, `src/service/allocation_service.rs`, `src/http/control_plane.rs`.

## Acceptance checklist

- [ ] Independent plan reviewer returns `APPROVED` before implementation.
- [ ] `TcpRuntime` implements `RuntimeFacade` without changing public API/resource definitions.
- [ ] TCP create binds loopback listener and create bind failure lets service try later ports.
- [ ] No-host allocations report `rejecting_no_host` and close incoming TCP clients.
- [ ] Binding activation forwards bytes through the relay to a loopback echo server.
- [ ] Binding delete/update closes stale sessions and new clients use the current state.
- [ ] Delete releases listener port and removes listener metric rows.
- [ ] Restore recreates persisted TCP listeners and forwards when binding is persisted.
- [ ] TCP counters/gauge/snapshot byte totals update after forwarded traffic, including `runtime_apply_total` on create/update/restore and restore failure counters on restore bind failure.
- [ ] Binding update-to-new-target closes stale sessions and new clients reach the new target.
- [ ] Active-session cleanup is cancellation-safe so aborted sessions cannot leave global or listener active gauges stale.
- [ ] Busy-port tests use deterministic reserved free-port ranges and never rely on `port + 1` being available.
- [ ] `cargo fmt -- --check` passes.
- [ ] `cargo test --locked` passes.
- [ ] `cargo clippy --locked --lib --tests -- -D warnings` passes.
- [ ] `zig build test` passes.
- [ ] Final independent spec-compliance reviewer returns `APPROVED` before docs commit/push.

## Task 1: Add TCP runtime skeleton and no-host listener behavior

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/runtime/mod.rs`
- Create: `src/runtime/tcp.rs`

- [ ] **Step 1: Write failing no-host runtime tests**

Create `src/runtime/tcp.rs` with the initial module and these tests first. The implementation types referenced in the tests are intentionally missing at red time:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PortRange;
    use crate::metrics::Metrics;
    use crate::model::{Allocation, Protocol, RuntimeStatus};
    use crate::runtime::facade::RuntimeFacade;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{timeout, Duration};

    fn allocation(id: &str, port: u16, target_port: Option<u16>, host: Option<&str>) -> Allocation {
        Allocation {
            id: id.to_owned(),
            protocol: Protocol::Tcp,
            port,
            target_port,
            host: host.map(str::to_owned),
            created_at_ms: 1000,
            updated_at_ms: 1000,
        }
    }

    async fn free_port() -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        listener.local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn tcp_runtime_create_without_binding_reports_rejecting_no_host_and_closes_clients() {
        let metrics = Arc::new(Metrics::default());
        let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
        let port = free_port().await;
        let alloc = allocation("alloc-no-host", port, None, None);

        runtime.create(&alloc, 500).await.unwrap();

        let observed = runtime.snapshot("alloc-no-host").await.unwrap().unwrap();
        assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
        assert_eq!(observed.effective_host, None);
        assert_eq!(observed.effective_target_port, None);

        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut buf = [0_u8; 4];
        let read = timeout(Duration::from_millis(500), client.read(&mut buf)).await.unwrap().unwrap();
        assert_eq!(read, 0);
        assert_eq!(metrics.rejected_no_host_total.load(), 1);

        runtime.delete("alloc-no-host", 500).await.unwrap();
    }
}
```

- [ ] **Step 2: Run red test and verify it fails because `TcpRuntime` is missing**

Run:

```bash
CARGO_TARGET_DIR=/tmp/relayd-m3-task1-red cargo test --locked runtime::tcp::tests::tcp_runtime_create_without_binding_reports_rejecting_no_host_and_closes_clients
```

Expected: FAIL to compile with unresolved `TcpRuntime` / `TcpRuntimeConfig` or equivalent missing implementation errors.

- [ ] **Step 3: Add Tokio features and module export**

Change `Cargo.toml` Tokio dependency to:

```toml
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net", "io-util", "sync", "time"] }
```

Change `src/runtime/mod.rs` to:

```rust
pub mod facade;
pub mod tcp;
```

- [ ] **Step 4: Implement minimal `TcpRuntime` create/snapshot/delete/no-host close**

Replace the top of `src/runtime/tcp.rs` before the test module with this implementation shape:

```rust
use crate::metrics::Metrics;
use crate::model::{Allocation, ErrorKind, Protocol, RuntimeStatus};
use crate::runtime::facade::{ListenerMetricsSnapshot, ObservedState, RuntimeError, RuntimeFacade};
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock, watch};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct TcpRuntimeConfig {
    bind_host: String,
    metrics: Arc<Metrics>,
}

impl TcpRuntimeConfig {
    pub fn loopback(metrics: Arc<Metrics>) -> Self {
        Self { bind_host: "127.0.0.1".to_owned(), metrics }
    }
}

#[derive(Clone)]
pub struct TcpRuntime {
    config: TcpRuntimeConfig,
    entries: Arc<Mutex<HashMap<String, Arc<ListenerEntry>>>>,
}

struct ListenerEntry {
    id: String,
    port: u16,
    state: RwLock<EntryState>,
    metrics: ListenerMetrics,
    shutdown: watch::Sender<bool>,
    accept_task: Mutex<Option<JoinHandle<()>>>,
    sessions: Mutex<Vec<JoinHandle<()>>>,
    global_metrics: Arc<Metrics>,
}

#[derive(Clone, Debug)]
struct EntryState {
    effective_host: Option<String>,
    effective_target_port: Option<u16>,
    runtime_status: RuntimeStatus,
    error_kind: Option<ErrorKind>,
    last_error: Option<String>,
}

#[derive(Default)]
struct ListenerMetrics {
    active: std::sync::atomic::AtomicU64,
    rx: std::sync::atomic::AtomicU64,
    tx: std::sync::atomic::AtomicU64,
}
```

Then implement `TcpRuntime::new`, `entry_state_for`, `bind_addr`, `spawn_accept_loop`, `close_sessions`, and `RuntimeFacade` methods so the red test passes. The accept loop must select on `shutdown_rx.changed()` and `listener.accept()`. For Task 1, accepted clients may be closed immediately when there is no effective target; forwarding is added in Task 2.

- [ ] **Step 5: Run green test**

Run:

```bash
CARGO_TARGET_DIR=/tmp/relayd-m3-task1-green cargo test --locked runtime::tcp::tests::tcp_runtime_create_without_binding_reports_rejecting_no_host_and_closes_clients
```

Expected: PASS.

## Task 2: Add baseline TCP copy forwarding and listener metrics

**Files:**
- Modify: `src/runtime/tcp.rs`

- [ ] **Step 1: Write failing forwarding and metrics tests**

Append these tests inside `src/runtime/tcp.rs` test module:

```rust
async fn start_echo_server() -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else { break; };
            tokio::spawn(async move {
                let (mut rd, mut wr) = socket.split();
                let _ = tokio::io::copy(&mut rd, &mut wr).await;
            });
        }
    });
    (port, task)
}

async fn start_prefix_echo_server(prefix: &'static [u8]) -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else { break; };
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let Ok(n) = socket.read(&mut buf).await else { break; };
                    if n == 0 { break; }
                    if socket.write_all(prefix).await.is_err() { break; }
                    if socket.write_all(&buf[..n]).await.is_err() { break; }
                }
            });
        }
    });
    (port, task)
}

#[tokio::test]
async fn tcp_runtime_forwards_bytes_after_binding_and_records_metrics() {
    let metrics = Arc::new(Metrics::default());
    let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_port().await;
    let (target_port, target_task) = start_echo_server().await;
    let alloc = allocation("alloc-active", relay_port, None, None);
    runtime.create(&alloc, 500).await.unwrap();
    runtime.update(&allocation("alloc-active", relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();

    let mut client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
    client.write_all(b"hello tcp").await.unwrap();
    let mut buf = [0_u8; 9];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello tcp");
    drop(client);

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = runtime.snapshot_listener_metrics().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].port, relay_port);
    assert_eq!(rows[0].protocol, Protocol::Tcp);
    assert!(rows[0].rx_bytes_total >= 9);
    assert!(rows[0].tx_bytes_total >= 9);
    assert_eq!(metrics.tcp_listener_accept_total.load(), 1);
    assert_eq!(metrics.tcp_upstream_connect_total.load(), 1);
    assert_eq!(metrics.tcp_copy_fallback_total.load(), 1);
    assert_eq!(metrics.tcp_session_create_total.load(), 1);
    assert_eq!(metrics.tcp_session_close_total.load(), 1);
    assert_eq!(metrics.tcp_active_sessions.load(), 0);
    assert!(metrics.runtime_apply_total.load() >= 2);

    runtime.delete("alloc-active", 500).await.unwrap();
    target_task.abort();
}
```

- [ ] **Step 2: Run red test and verify forwarding is not implemented**

Run:

```bash
CARGO_TARGET_DIR=/tmp/relayd-m3-task2-red cargo test --locked runtime::tcp::tests::tcp_runtime_forwards_bytes_after_binding_and_records_metrics
```

Expected: FAIL by timing out, connection reset, or missing metric increments because Task 1 closed clients without forwarding.

- [ ] **Step 3: Implement session forwarding**

In `spawn_accept_loop`, when an accepted connection has a configured target, spawn a session task that:

```rust
let upstream = tokio::net::TcpStream::connect((host.as_str(), target_port)).await;
match upstream {
    Ok(mut upstream) => {
        entry.global_metrics.tcp_upstream_connect_total.inc();
        entry.global_metrics.tcp_copy_fallback_total.inc();
        let active_guard = ActiveSessionGuard::new(entry.clone());
        let (from_client, from_upstream) = tokio::io::copy_bidirectional(&mut client, &mut upstream).await.unwrap_or((0, 0));
        entry.metrics.tx.fetch_add(from_client, std::sync::atomic::Ordering::Relaxed);
        entry.metrics.rx.fetch_add(from_upstream, std::sync::atomic::Ordering::Relaxed);
        drop(active_guard);
    }
    Err(error) => {
        entry.global_metrics.tcp_upstream_connect_fail_total.inc();
        let mut state = entry.state.write().await;
        state.error_kind = Some(ErrorKind::ApplyFailed);
        state.last_error = Some(error.to_string());
    }
}
```

Store each session `JoinHandle` in `entry.sessions`; `close_sessions(entry).await` aborts and awaits all handles. Implement an `ActiveSessionGuard` whose constructor increments listener/global active gauges plus session-create counters and whose `Drop` decrements listener/global active gauges and increments `tcp_session_close_total`. This guard must live inside the spawned session task so normal completion, connection errors, and abort cancellation all leave gauges consistent.

- [ ] **Step 4: Implement listener metric snapshots**

Implement `snapshot_listener_metrics` to return one row per current TCP entry:

```rust
ListenerMetricsSnapshot {
    port: entry.port,
    protocol: Protocol::Tcp,
    connections_current: entry.metrics.active.load(std::sync::atomic::Ordering::Relaxed),
    rx_bytes_total: entry.metrics.rx.load(std::sync::atomic::Ordering::Relaxed),
    tx_bytes_total: entry.metrics.tx.load(std::sync::atomic::Ordering::Relaxed),
}
```

- [ ] **Step 5: Run green test**

Run:

```bash
CARGO_TARGET_DIR=/tmp/relayd-m3-task2-green cargo test --locked runtime::tcp::tests::tcp_runtime_forwards_bytes_after_binding_and_records_metrics
```

Expected: PASS.

## Task 3: Add service-level restore, delete-release, and binding state tests

**Files:**
- Modify: `src/runtime/tcp.rs`

- [ ] **Step 1: Write failing integration-style runtime/service tests**

Append these tests inside the `src/runtime/tcp.rs` test module. Include local temp repository helpers so tests exercise `Service<TcpRuntime>` instead of only direct runtime calls:

```rust
use crate::service::allocation_service::Service;
use crate::storage::sqlite::Repository;
use tempfile::NamedTempFile;

async fn temp_repo_with_file() -> (Repository, NamedTempFile) {
    let file = NamedTempFile::new().unwrap();
    let repo = Repository::open(file.path()).await.unwrap();
    (repo, file)
}

fn service(repo: Repository, runtime: TcpRuntime, start: u16, end: u16) -> Service<TcpRuntime> {
    Service::new(repo, runtime, PortRange { start, end }, 500)
}

#[tokio::test]
async fn tcp_service_delete_releases_listener_port() {
    let metrics = Arc::new(Metrics::default());
    let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics));
    let port = free_port().await;
    let (repo, _file) = temp_repo_with_file().await;
    let svc = service(repo, runtime.clone(), port, port);
    let alloc = svc.create_allocation(Protocol::Tcp, None).await.unwrap();

    svc.delete_allocation(&alloc.id).await.unwrap();

    let rebound = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    assert_eq!(rebound.local_addr().unwrap().port(), port);
}

#[tokio::test]
async fn tcp_service_restore_recreates_listener_and_forwards_persisted_binding() {
    let port = free_port().await;
    let (target_port, target_task) = start_echo_server().await;
    let (repo, file) = temp_repo_with_file().await;
    let runtime_one = TcpRuntime::new(TcpRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let svc_one = service(repo, runtime_one.clone(), port, port);
    let alloc = svc_one.create_allocation(Protocol::Tcp, Some(target_port)).await.unwrap();
    svc_one.set_target(&alloc.id, "127.0.0.1").await.unwrap();
    drop(svc_one);
    runtime_one.delete(&alloc.id, 500).await.unwrap();

    let repo_two = Repository::open(file.path()).await.unwrap();
    let runtime_two = TcpRuntime::new(TcpRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let svc_two = service(repo_two, runtime_two.clone(), port, port);
    svc_two.restore_all(500).await.unwrap();

    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    client.write_all(b"restore").await.unwrap();
    let mut buf = [0_u8; 7];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"restore");

    runtime_two.delete(&alloc.id, 500).await.unwrap();
    target_task.abort();
}

#[tokio::test]
async fn tcp_runtime_update_to_new_target_closes_stale_session_and_uses_new_target() {
    let metrics = Arc::new(Metrics::default());
    let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics));
    let relay_port = free_port().await;
    let (target_one_port, target_one_task) = start_prefix_echo_server(b"one:").await;
    let (target_two_port, target_two_task) = start_prefix_echo_server(b"two:").await;
    runtime.create(&allocation("alloc-retarget", relay_port, Some(target_one_port), Some("127.0.0.1")), 500).await.unwrap();

    let mut old_client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
    old_client.write_all(b"before").await.unwrap();
    let mut old_buf = [0_u8; 10];
    old_client.read_exact(&mut old_buf).await.unwrap();
    assert_eq!(&old_buf, b"one:before");

    runtime.update(&allocation("alloc-retarget", relay_port, Some(target_two_port), Some("127.0.0.1")), 500).await.unwrap();
    let mut after_update_read = [0_u8; 1];
    let old_closed = timeout(Duration::from_millis(500), old_client.read(&mut after_update_read)).await.unwrap().unwrap();
    assert_eq!(old_closed, 0);

    let mut new_client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
    new_client.write_all(b"after").await.unwrap();
    let mut new_buf = [0_u8; 9];
    new_client.read_exact(&mut new_buf).await.unwrap();
    assert_eq!(&new_buf, b"two:after");

    runtime.delete("alloc-retarget", 500).await.unwrap();
    target_one_task.abort();
    target_two_task.abort();
}

#[tokio::test]
async fn tcp_runtime_aborted_active_session_decrements_global_and_listener_gauges() {
    let metrics = Arc::new(Metrics::default());
    let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_port().await;
    let (target_port, target_task) = start_echo_server().await;
    runtime.create(&allocation("alloc-abort", relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();

    let _client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(metrics.tcp_active_sessions.load(), 1);
    assert_eq!(runtime.snapshot_listener_metrics().await.unwrap()[0].connections_current, 1);

    runtime.update(&allocation("alloc-abort", relay_port, None, None), 500).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(metrics.tcp_active_sessions.load(), 0);
    assert_eq!(runtime.snapshot_listener_metrics().await.unwrap()[0].connections_current, 0);

    runtime.delete("alloc-abort", 500).await.unwrap();
    target_task.abort();
}

#[tokio::test]
async fn tcp_runtime_binding_delete_closes_existing_session_and_rejects_new_clients() {
    let metrics = Arc::new(Metrics::default());
    let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics));
    let relay_port = free_port().await;
    let (target_port, target_task) = start_echo_server().await;
    let alloc = allocation("alloc-close", relay_port, Some(target_port), Some("127.0.0.1"));
    runtime.create(&alloc, 500).await.unwrap();

    let client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
    runtime.update(&allocation("alloc-close", relay_port, None, None), 500).await.unwrap();
    drop(client);

    let observed = runtime.snapshot("alloc-close").await.unwrap().unwrap();
    assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
    let mut new_client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
    new_client.write_all(b"stale").await.unwrap();
    let mut buf = [0_u8; 5];
    let read = timeout(Duration::from_millis(500), new_client.read(&mut buf)).await.unwrap().unwrap();
    assert_eq!(read, 0);

    runtime.delete("alloc-close", 500).await.unwrap();
    target_task.abort();
}
```

- [ ] **Step 2: Run red tests**

Run:

```bash
CARGO_TARGET_DIR=/tmp/relayd-m3-task3-red cargo test --locked runtime::tcp::tests::tcp_
```

Expected: at least one FAIL if delete/restore/session abort/update-retarget semantics are incomplete.

- [ ] **Step 3: Implement delete, restore, and update session cleanup semantics**

Ensure these code paths are present:

```rust
async fn remove_entry(&self, id: &str) -> Option<Arc<ListenerEntry>> {
    self.entries.lock().await.remove(id)
}

async fn stop_entry(entry: Arc<ListenerEntry>) {
    let _ = entry.shutdown.send(true);
    if let Some(handle) = entry.accept_task.lock().await.take() {
        handle.abort();
        let _ = handle.await;
    }
    close_sessions(&entry).await;
}
```

`delete` must call `remove_entry` then `stop_entry`. `restore` must call the same internal bind path as `create` but map errors to `RuntimeRestoreFailed` and increment `restore_failures_total` on failure. `update` must call `close_sessions(&entry).await` after replacing effective state.

- [ ] **Step 4: Run green tests**

Run:

```bash
CARGO_TARGET_DIR=/tmp/relayd-m3-task3-green cargo test --locked runtime::tcp::tests::tcp_
```

Expected: PASS.

## Task 4: Add bind-failure, service port-selection, and HTTP/Prometheus smoke coverage

**Files:**
- Modify: `src/runtime/tcp.rs`
- Modify: `src/prometheus.rs` only if required by compiler/test imports

- [ ] **Step 1: Write failing bind-failure and service metrics tests**

Append these tests inside `src/runtime/tcp.rs` test module:

```rust
async fn reserve_two_port_range() -> (TcpListener, u16, u16) {
    for start in 20000_u16..60000_u16 {
        let Ok(first) = TcpListener::bind(("127.0.0.1", start)).await else { continue; };
        if let Ok(second) = TcpListener::bind(("127.0.0.1", start + 1)).await {
            drop(second);
            return (first, start, start + 1);
        }
    }
    panic!("could not reserve a two-port range for test");
}

#[tokio::test]
async fn tcp_service_skips_busy_port_after_runtime_create_bind_failure() {
    let (_busy, busy_port, next_port) = reserve_two_port_range().await;
    let metrics = Arc::new(Metrics::default());
    let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
    let (repo, _file) = temp_repo_with_file().await;
    let svc = service(repo, runtime, busy_port, next_port);

    let alloc = svc.create_allocation(Protocol::Tcp, None).await.unwrap();

    assert_eq!(alloc.port, next_port);
    assert!(metrics.bind_fail_total.load() >= 1);
    assert!(metrics.runtime_apply_total.load() >= 1);
}

#[tokio::test]
async fn tcp_runtime_restore_bind_failure_reports_restore_error_and_metrics() {
    let busy = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let busy_port = busy.local_addr().unwrap().port();
    let metrics = Arc::new(Metrics::default());
    let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));

    let error = runtime.restore(&allocation("alloc-restore-fail", busy_port, None, None), 500).await.unwrap_err();

    assert_eq!(error, crate::runtime::facade::RuntimeError::RuntimeRestoreFailed);
    assert_eq!(metrics.bind_fail_total.load(), 1);
    assert_eq!(metrics.restore_failures_total.load(), 1);
    assert!(runtime.snapshot("alloc-restore-fail").await.unwrap().is_none());
}

#[tokio::test]
async fn tcp_runtime_snapshot_rows_feed_prometheus_renderer() {
    let metrics = Arc::new(Metrics::default());
    let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics));
    let relay_port = free_port().await;
    let (target_port, target_task) = start_echo_server().await;
    runtime.create(&allocation("alloc-prom", relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();

    let mut client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
    client.write_all(b"prom").await.unwrap();
    let mut buf = [0_u8; 4];
    client.read_exact(&mut buf).await.unwrap();
    drop(client);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let rows = runtime.snapshot_listener_metrics().await.unwrap();
    let rendered = crate::prometheus::render(&rows);
    assert!(rendered.contains(&format!("relayd_connections_current{{port=\"{}\",protocol=\"tcp\"}}", relay_port)));

    runtime.delete("alloc-prom", 500).await.unwrap();
    target_task.abort();
}
```

- [ ] **Step 2: Run red tests**

Run:

```bash
CARGO_TARGET_DIR=/tmp/relayd-m3-task4-red cargo test --locked runtime::tcp::tests::tcp_
```

Expected: FAIL if bind failure mapping/metrics, restore failure metrics, or snapshot rendering integration is incomplete.

- [ ] **Step 3: Implement bind-failure mapping and metric increments**

In the internal bind/create function, map bind errors as follows:

```rust
match TcpListener::bind(bind_addr).await {
    Ok(listener) => listener,
    Err(_) if is_restore => {
        self.config.metrics.bind_fail_total.inc();
        self.config.metrics.restore_failures_total.inc();
        return Err(RuntimeError::RuntimeRestoreFailed);
    }
    Err(_) => {
        self.config.metrics.bind_fail_total.inc();
        return Err(RuntimeError::RuntimeCreateFailed);
    }
}
```

Do not persist any entry on bind failure. Keep service retry behavior unchanged.

- [ ] **Step 4: Run green tests**

Run:

```bash
CARGO_TARGET_DIR=/tmp/relayd-m3-task4-green cargo test --locked runtime::tcp::tests::tcp_
```

Expected: PASS.

## Task 5: Final verification, docs status, independent spec review, commit, and push

**Files:**
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-m3-baseline-tcp-runtime.md` only if implementation reveals approved scope clarifications
- Modify: `docs/superpowers/plans/2026-05-15-rust-migration-m3-baseline-tcp-runtime.md` checkbox statuses if desired

- [ ] **Step 1: Run full formatting and Rust tests**

Run:

```bash
cargo fmt -- --check
CARGO_TARGET_DIR=/tmp/relayd-m3-final-target cargo test --locked
```

Expected: both PASS.

- [ ] **Step 2: Run lint and Zig reference tests**

Run:

```bash
cargo clippy --locked --lib --tests -- -D warnings
zig build test
```

Expected: both PASS.

- [ ] **Step 3: Update milestone ledger with M3 status**

Append this block after the existing M2 implementation status in `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`:

```markdown
## M3 implementation status

- Status: implemented in Rust TCP runtime with loopback listener lifecycle, default copy forwarding, no-host rejection, delete cleanup, restore, and TCP listener metrics.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Scope note: UDP runtime, dual-protocol real runtime parity, TCP session-model/splice optional lanes, and full Prometheus rate semantics remain assigned to M4-M6.
```

- [ ] **Step 4: Dispatch independent spec-compliance reviewer**

Ask a fresh reviewer to compare the final diff against `docs/superpowers/specs/2026-05-15-rust-migration-m3-baseline-tcp-runtime.md`. Required final response must contain `APPROVED` or concrete blocking gaps. Fix blocking gaps and re-review until `APPROVED`.

- [ ] **Step 5: Commit and push**

Run:

```bash
git status --short
git add Cargo.toml Cargo.lock src/runtime/mod.rs src/runtime/tcp.rs docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md docs/superpowers/specs/2026-05-15-rust-migration-m3-baseline-tcp-runtime.md docs/superpowers/plans/2026-05-15-rust-migration-m3-baseline-tcp-runtime.md
git commit -m "Bring default TCP forwarding into Rust runtime" \
  -m "Add a real Tokio TCP runtime behind the existing facade so the Rust service can bind loopback listeners, reject unbound traffic, forward bound sessions, restore persisted TCP allocations, and report TCP listener metrics without changing the HTTP API definitions." \
  -m "Constraint: Zig API interfaces and definitions must remain unchanged during migration." \
  -m "Rejected: Porting TCP session-model or splice lanes in M3 | They are optional feature-flagged paths deferred by the migration ledger." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Directive: Keep UDP, both-protocol runtime parity, and Prometheus rate semantics in their assigned M4-M6 milestones." \
  -m "Tested: cargo fmt -- --check; cargo test --locked; cargo clippy --locked --lib --tests -- -D warnings; zig build test" \
  -m "Not-tested: Optional TCP session-model/splice paths, UDP forwarding, dual-protocol real runtime, production Rust binary cutover" \
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"
git push
```

Expected: commit and push succeed.
