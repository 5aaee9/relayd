# Rust Migration M4 Baseline UDP Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the real Rust baseline UDP runtime so UDP allocations bind loopback listeners, reject while unbound, forward datagrams through per-client upstream sessions after binding, clean idle sessions, delete/restore cleanly, and expose UDP listener metrics.

**Architecture:** Keep the existing `RuntimeFacade` trait stable and add `src/runtime/udp.rs` using Tokio `UdpSocket`, one listener receive task per UDP allocation, one upstream reply task per client session, a cleanup task for TTL expiry, and shared `Arc` entry state. The allocation service remains responsible for persistence and orchestration; the UDP runtime owns socket lifecycle, session maps, observed runtime state, and UDP listener metric snapshots.

**Tech Stack:** Rust 1.95, Tokio `net`/`sync`/`time`, existing `async-trait`, SeaORM/SQLx repository and service, M2 metrics/prometheus modules, `cargo test --locked`, `cargo clippy --locked --lib --tests -- -D warnings`, `zig build test`.

---

## File Structure

- Modify: `src/runtime/mod.rs` — export the new UDP runtime module.
- Create: `src/runtime/udp.rs` — `UdpRuntime`, `UdpRuntimeConfig`, listener entry/session state, receive loop, reply loop, TTL cleanup, and UDP runtime tests.
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md` after implementation — add M4 status block.
- Existing source references: `src/runtime/manager.zig`, `tests/integration/service_forward_test.zig`, `src/runtime/tcp.rs`, `src/service/allocation_service.rs`, `src/prometheus.rs`.

## Acceptance checklist

- [ ] Independent plan reviewer returns `APPROVED` before implementation.
- [ ] `UdpRuntime` implements `RuntimeFacade` without changing public API/resource definitions.
- [ ] UDP create binds loopback listener and create bind failure lets service try later ports.
- [ ] No-host allocations report `rejecting_no_host`, drop datagrams, create no sessions, and increment no-host metrics.
- [ ] Binding activation forwards datagrams through the relay to a loopback echo server.
- [ ] Repeated datagrams from one client reuse one upstream session; a second client creates a second session.
- [ ] Idle sessions expire by TTL and active gauges/snapshots settle to zero.
- [ ] Binding update/delete closes stale sessions; deterministic delayed old replies are dropped via generation/tombstone checks; stale/drop counters increment; new datagrams use current state.
- [ ] Delete releases listener port and removes listener metric rows.
- [ ] Restore recreates persisted UDP listener and forwards when binding is persisted.
- [ ] UDP counters/gauge/snapshot byte totals update after forwarded traffic, stale/drop reply counters are covered, active gauges cannot underflow/leak, and Prometheus renderer receives UDP rows.
- [ ] `cargo fmt -- --check` passes.
- [ ] `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked` passes.
- [ ] `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings` passes.
- [ ] `zig build test` passes.
- [ ] Final independent spec-compliance reviewer returns `APPROVED` before docs commit/push.

## Task 1: Add UDP runtime skeleton and no-host datagram behavior

**Files:**
- Modify: `src/runtime/mod.rs`
- Create: `src/runtime/udp.rs`

- [ ] **Step 1: Write failing no-host UDP runtime test**

Create `src/runtime/udp.rs` with the initial test module first. The implementation types are intentionally missing at red time:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::model::{Allocation, Protocol, RuntimeStatus};
    use crate::runtime::facade::RuntimeFacade;
    use std::sync::Arc;
    use tokio::net::UdpSocket;
    use tokio::time::{timeout, Duration};

    fn allocation(id: &str, port: u16, target_port: Option<u16>, host: Option<&str>) -> Allocation {
        Allocation {
            id: id.to_owned(),
            protocol: Protocol::Udp,
            port,
            target_port,
            host: host.map(str::to_owned),
            created_at_ms: 1000,
            updated_at_ms: 1000,
        }
    }

    async fn free_udp_port() -> u16 {
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        socket.local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn udp_runtime_create_without_binding_reports_rejecting_no_host_and_drops_datagrams() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_udp_port().await;
        runtime.create(&allocation("alloc-no-host", relay_port, None, None), 500).await.unwrap();

        let observed = runtime.snapshot("alloc-no-host").await.unwrap().unwrap();
        assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
        assert_eq!(observed.effective_host, None);
        assert_eq!(observed.effective_target_port, None);

        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        client.send_to(b"drop-me", ("127.0.0.1", relay_port)).await.unwrap();
        let mut buf = [0_u8; 16];
        assert!(timeout(Duration::from_millis(150), client.recv_from(&mut buf)).await.is_err());

        let rows = runtime.snapshot_listener_metrics().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].protocol, Protocol::Udp);
        assert_eq!(rows[0].connections_current, 0);
        assert_eq!(metrics.rejected_no_host_total.load(), 1);
        assert_eq!(metrics.udp_session_create_total.load(), 0);
        assert_eq!(metrics.udp_active_sessions.load(), 0);

        runtime.delete("alloc-no-host", 500).await.unwrap();
    }
}
```

- [ ] **Step 2: Run red test**

Run:

```bash
CARGO_TARGET_DIR=target cargo test --locked runtime::udp::tests::udp_runtime_create_without_binding_reports_rejecting_no_host_and_drops_datagrams
```

Expected: FAIL to compile with unresolved `UdpRuntime` / `UdpRuntimeConfig` or missing module export.

- [ ] **Step 3: Export module and implement minimal skeleton**

Change `src/runtime/mod.rs` to include:

```rust
pub mod facade;
pub mod tcp;
pub mod udp;
```

Implement the top of `src/runtime/udp.rs` with these types and the no-host receive loop:

```rust
use crate::metrics::Metrics;
use crate::model::{Allocation, ErrorKind, Protocol, RuntimeStatus};
use crate::runtime::facade::{ListenerMetricsSnapshot, ObservedState, RuntimeError, RuntimeFacade};
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, RwLock, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

#[derive(Clone)]
pub struct UdpRuntimeConfig {
    bind_host: String,
    metrics: Arc<Metrics>,
    session_ttl: Duration,
    max_sessions: usize,
}

impl UdpRuntimeConfig {
    pub fn loopback(metrics: Arc<Metrics>) -> Self {
        Self {
            bind_host: "127.0.0.1".to_owned(),
            metrics,
            session_ttl: Duration::from_millis(60_000),
            max_sessions: 4096,
        }
    }

    pub fn with_session_ttl(mut self, ttl: Duration) -> Self {
        self.session_ttl = ttl;
        self
    }
}

#[derive(Clone)]
pub struct UdpRuntime {
    config: UdpRuntimeConfig,
    entries: Arc<Mutex<HashMap<String, Arc<ListenerEntry>>>>,
}
```

Add `ListenerEntry`, `EntryState`, `ListenerMetrics`, `UdpSession`, `SessionGuard`, `UdpRuntime::new`, `entry_state_for`, `bind_addr`, `spawn_receive_loop`, `close_sessions`, `stop_entry`, and `RuntimeFacade` methods. For Task 1, `handle_datagram` may only record inbound packet/bytes and drop while no target exists; forwarding is Task 2. `snapshot_listener_metrics` must return UDP rows.

- [ ] **Step 4: Run green test**

Run:

```bash
CARGO_TARGET_DIR=target cargo test --locked runtime::udp::tests::udp_runtime_create_without_binding_reports_rejecting_no_host_and_drops_datagrams
```

Expected: PASS.

## Task 2: Add baseline UDP forwarding, session reuse, and listener metrics

**Files:**
- Modify: `src/runtime/udp.rs`

- [ ] **Step 1: Add forwarding/session tests**

Append these tests and helpers to `src/runtime/udp.rs`:

```rust
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

async fn start_udp_capture_server(expected: usize) -> (u16, Arc<Mutex<Vec<(u16, Vec<u8>)>>>, JoinHandle<()>) {
    let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let records = Arc::new(Mutex::new(Vec::new()));
    let task_records = records.clone();
    let task = tokio::spawn(async move {
        let mut buf = [0_u8; 2048];
        while task_records.lock().await.len() < expected {
            let Ok((n, peer)) = socket.recv_from(&mut buf).await else { break; };
            task_records.lock().await.push((peer.port(), buf[..n].to_vec()));
            let _ = socket.send_to(&buf[..n], peer).await;
        }
    });
    (port, records, task)
}

async fn send_udp_and_expect(client: &UdpSocket, relay_port: u16, payload: &[u8]) {
    client.send_to(payload, ("127.0.0.1", relay_port)).await.unwrap();
    let mut buf = [0_u8; 2048];
    let (n, _) = timeout(Duration::from_secs(1), client.recv_from(&mut buf)).await.unwrap().unwrap();
    assert_eq!(&buf[..n], payload);
}

async fn wait_for_udp_metrics(runtime: &UdpRuntime, metrics: &Metrics, packets_in: u64, packets_out: u64) {
    timeout(Duration::from_secs(2), async {
        loop {
            let rows = runtime.snapshot_listener_metrics().await.unwrap();
            if metrics.udp_packets_in_total.load() == packets_in
                && metrics.udp_packets_out_total.load() == packets_out
                && !rows.is_empty()
                && rows[0].rx_bytes_total > 0
                && rows[0].tx_bytes_total > 0
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("udp metrics did not settle");
}

#[tokio::test]
async fn udp_runtime_forwards_datagrams_after_binding_and_records_metrics() {
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_udp_port().await;
    let (target_port, target_task) = start_udp_echo_server().await;
    runtime.create(&allocation("alloc-active", relay_port, None, None), 500).await.unwrap();
    runtime.update(&allocation("alloc-active", relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();

    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    send_udp_and_expect(&client, relay_port, b"pong").await;
    wait_for_udp_metrics(&runtime, &metrics, 1, 1).await;

    let rows = runtime.snapshot_listener_metrics().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].protocol, Protocol::Udp);
    assert_eq!(rows[0].connections_current, 1);
    assert_eq!(rows[0].rx_bytes_total, 4);
    assert_eq!(rows[0].tx_bytes_total, 4);
    assert_eq!(metrics.udp_session_create_total.load(), 1);
    assert_eq!(metrics.udp_active_sessions.load(), 1);

    runtime.delete("alloc-active", 500).await.unwrap();
    target_task.abort();
}

#[tokio::test]
async fn udp_runtime_reuses_session_for_same_client_and_splits_second_client() {
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_udp_port().await;
    let (target_port, records, target_task) = start_udp_capture_server(3).await;
    runtime.create(&allocation("alloc-sessions", relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();

    let client_one = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let client_two = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    send_udp_and_expect(&client_one, relay_port, b"one-a").await;
    send_udp_and_expect(&client_one, relay_port, b"one-b").await;
    send_udp_and_expect(&client_two, relay_port, b"two-a").await;

    target_task.await.unwrap();
    let records = records.lock().await.clone();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].0, records[1].0);
    assert_ne!(records[0].0, records[2].0);
    assert_eq!(metrics.udp_session_create_total.load(), 2);
    assert_eq!(metrics.udp_active_sessions.load(), 2);

    runtime.delete("alloc-sessions", 500).await.unwrap();
}
```

- [ ] **Step 2: Run red tests**

Run:

```bash
CARGO_TARGET_DIR=target cargo test --locked runtime::udp::tests::udp_runtime_
```

Expected: FAIL until forwarding/session code is implemented.

- [ ] **Step 3: Implement session forwarding**

Implement `handle_datagram` for active entries:

- Increment `udp_packets_in_total`, `udp_bytes_in_total`, and listener rx bytes for every inbound datagram.
- Read state and drop/no-host if target is absent.
- Lock sessions map; create a session if needed and `sessions.len() < max_sessions`.
- A new session binds `UdpSocket::bind(("127.0.0.1", 0))`, connects to target, starts a reply task, increments `udp_session_create_total`, `udp_active_sessions`, and listener active sessions through a `SessionGuard`.
- Send inbound payload through the session upstream socket with `send`; on error increment `udp_send_errors_total` and `udp_drop_total` and close the session.
- Reply task receives from the upstream socket, then verifies the session is still current before sending through the listener: the task must carry a session generation/token and compare it with the entry's current session map (or an equivalent tombstone). On generation/session mismatch, increment `udp_reply_stale_total` and drop the reply without sending to the client. On valid current session, send replies through the listener socket to the original client address and increment `udp_reply_primary_total`, outbound packet/byte counters, listener tx bytes, and `last_seen`.

- [ ] **Step 4: Run green tests**

Run:

```bash
CARGO_TARGET_DIR=target cargo test --locked runtime::udp::tests::udp_runtime_
```

Expected: PASS for Task 1 and Task 2 UDP tests.

## Task 3: Add TTL cleanup, update/delete stale reply behavior, and active gauge cleanup

**Files:**
- Modify: `src/runtime/udp.rs`

- [ ] **Step 1: Add cleanup/update/delete tests**

Append tests covering TTL, retarget, delayed stale replies, and delete release:

```rust
async fn start_udp_fixed_reply_server(reply: &'static [u8]) -> (u16, JoinHandle<()>) {
    let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let mut buf = [0_u8; 2048];
        while let Ok((_n, peer)) = socket.recv_from(&mut buf).await {
            let _ = socket.send_to(reply, peer).await;
        }
    });
    (port, task)
}

async fn wait_for_active_sessions(runtime: &UdpRuntime, metrics: &Metrics, expected: u64) {
    timeout(Duration::from_secs(2), async {
        loop {
            let rows = runtime.snapshot_listener_metrics().await.unwrap();
            if metrics.udp_active_sessions.load() == expected
                && !rows.is_empty()
                && rows[0].connections_current == expected
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("udp active sessions did not settle");
}

#[tokio::test]
async fn udp_runtime_expires_idle_sessions_and_recreates_on_new_traffic() {
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()).with_session_ttl(Duration::from_millis(25)));
    let relay_port = free_udp_port().await;
    let (target_port, target_task) = start_udp_echo_server().await;
    runtime.create(&allocation("alloc-ttl", relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();

    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    send_udp_and_expect(&client, relay_port, b"ttl1").await;
    wait_for_active_sessions(&runtime, &metrics, 1).await;
    wait_for_active_sessions(&runtime, &metrics, 0).await;
    send_udp_and_expect(&client, relay_port, b"ttl2").await;
    wait_for_active_sessions(&runtime, &metrics, 1).await;

    assert_eq!(metrics.udp_session_create_total.load(), 2);
    assert!(metrics.udp_session_expire_total.load() >= 1);
    runtime.delete("alloc-ttl", 500).await.unwrap();
    target_task.abort();
}

#[tokio::test]
async fn udp_runtime_update_to_new_target_closes_stale_session_and_uses_new_target() {
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_udp_port().await;
    let (target_one_port, target_one_task) = start_udp_fixed_reply_server(b"one").await;
    let (target_two_port, target_two_task) = start_udp_fixed_reply_server(b"two").await;
    runtime.create(&allocation("alloc-retarget", relay_port, Some(target_one_port), Some("127.0.0.1")), 500).await.unwrap();

    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    send_udp_and_expect(&client, relay_port, b"first").await;
    runtime.update(&allocation("alloc-retarget", relay_port, Some(target_two_port), Some("127.0.0.1")), 500).await.unwrap();
    wait_for_active_sessions(&runtime, &metrics, 0).await;

    client.send_to(b"second", ("127.0.0.1", relay_port)).await.unwrap();
    let mut buf = [0_u8; 16];
    let (n, _) = timeout(Duration::from_secs(1), client.recv_from(&mut buf)).await.unwrap().unwrap();
    assert_eq!(&buf[..n], b"two");

    runtime.delete("alloc-retarget", 500).await.unwrap();
    target_one_task.abort();
    target_two_task.abort();
}

#[tokio::test]
async fn udp_runtime_delete_releases_listener_port_and_clears_sessions() {
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_udp_port().await;
    let (target_port, target_task) = start_udp_echo_server().await;
    runtime.create(&allocation("alloc-delete", relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();
    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    send_udp_and_expect(&client, relay_port, b"delete").await;
    wait_for_active_sessions(&runtime, &metrics, 1).await;

    runtime.delete("alloc-delete", 500).await.unwrap();
    assert_eq!(metrics.udp_active_sessions.load(), 0);
    let rebound = UdpSocket::bind(("127.0.0.1", relay_port)).await.unwrap();
    assert_eq!(rebound.local_addr().unwrap().port(), relay_port);
    assert!(runtime.snapshot_listener_metrics().await.unwrap().is_empty());
    target_task.abort();
}
```


Also append deterministic delayed-reply helpers and tests before running the red command:

```rust
struct DelayedUdpServer {
    port: u16,
    received: tokio::sync::oneshot::Receiver<()>,
    release: tokio::sync::oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_delayed_udp_reply_server(reply: &'static [u8]) -> DelayedUdpServer {
    let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut buf = [0_u8; 2048];
        let Ok((_n, peer)) = socket.recv_from(&mut buf).await else { return; };
        let _ = received_tx.send(());
        let _ = release_rx.await;
        let _ = socket.send_to(reply, peer).await;
    });
    DelayedUdpServer { port, received: received_rx, release: release_tx, task }
}

async fn expect_no_udp_response(client: &UdpSocket) {
    let mut buf = [0_u8; 64];
    assert!(timeout(Duration::from_millis(150), client.recv_from(&mut buf)).await.is_err());
}

#[tokio::test]
async fn udp_runtime_delayed_reply_from_replaced_target_is_dropped_and_new_target_replies() {
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_udp_port().await;
    let old = start_delayed_udp_reply_server(b"old").await;
    let (new_port, new_task) = start_udp_fixed_reply_server(b"new").await;
    runtime.create(&allocation("alloc-stale-update", relay_port, Some(old.port), Some("127.0.0.1")), 500).await.unwrap();

    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    client.send_to(b"before", ("127.0.0.1", relay_port)).await.unwrap();
    old.received.await.unwrap();
    wait_for_active_sessions(&runtime, &metrics, 1).await;

    runtime.update(&allocation("alloc-stale-update", relay_port, Some(new_port), Some("127.0.0.1")), 500).await.unwrap();
    wait_for_active_sessions(&runtime, &metrics, 0).await;
    let _ = old.release.send(());
    expect_no_udp_response(&client).await;

    client.send_to(b"after", ("127.0.0.1", relay_port)).await.unwrap();
    let mut buf = [0_u8; 16];
    let (n, _) = timeout(Duration::from_secs(1), client.recv_from(&mut buf)).await.unwrap().unwrap();
    assert_eq!(&buf[..n], b"new");
    assert!(metrics.udp_reply_stale_total.load() + metrics.udp_reply_drop_total.load() >= 1);

    runtime.delete("alloc-stale-update", 500).await.unwrap();
    old.task.abort();
    new_task.abort();
}

#[tokio::test]
async fn udp_runtime_delayed_reply_after_delete_is_dropped_without_leaking_session() {
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_udp_port().await;
    let old = start_delayed_udp_reply_server(b"old-delete").await;
    runtime.create(&allocation("alloc-stale-delete", relay_port, Some(old.port), Some("127.0.0.1")), 500).await.unwrap();

    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    client.send_to(b"before-delete", ("127.0.0.1", relay_port)).await.unwrap();
    old.received.await.unwrap();
    wait_for_active_sessions(&runtime, &metrics, 1).await;

    runtime.delete("alloc-stale-delete", 500).await.unwrap();
    assert_eq!(metrics.udp_active_sessions.load(), 0);
    assert!(runtime.snapshot_listener_metrics().await.unwrap().is_empty());
    let _ = old.release.send(());
    expect_no_udp_response(&client).await;
    assert!(metrics.udp_reply_stale_total.load() + metrics.udp_reply_drop_total.load() >= 1);
    old.task.abort();
}
```

- [ ] **Step 2: Run red tests**

Run:

```bash
CARGO_TARGET_DIR=target cargo test --locked runtime::udp::tests::udp_runtime_
```

Expected: FAIL until TTL cleanup and update/delete cleanup are complete.

- [ ] **Step 3: Implement cleanup semantics**

Implement:

- `spawn_cleanup_loop(entry)` with `tokio::time::interval(min(session_ttl, 50ms))` that removes expired sessions.
- `remove_session(entry, client_addr, reason)` that is idempotent: if the session was already removed/tombstoned, it must not decrement gauges twice. On first removal it marks the session tombstoned, aborts or signals the reply task, removes the map entry only if the generation still matches, decrements active gauges via `SessionGuard` drop, increments `udp_session_expire_total` for expiry and `udp_reply_drop_total` when update/delete deliberately drops outstanding replies.
- `close_sessions(entry, count_as_reply_drop)` used by `update`, `delete`, and `stop_entry`; it must leave `udp_active_sessions == 0` and listener `connections_current == 0` after completion.
- `update` must call `close_sessions(..., true)` after replacing target/no-host state.
- Reply tasks must check generation/tombstone before client send and increment `udp_reply_stale_total` on mismatch. This is required even if update/delete also aborts tasks, because a delayed reply can race with removal.

- [ ] **Step 4: Run green tests**

Run:

```bash
CARGO_TARGET_DIR=target cargo test --locked runtime::udp::tests::udp_runtime_
```

Expected: PASS.

## Task 4: Add service restore, bind-failure, Prometheus, and full verification docs

**Files:**
- Modify: `src/runtime/udp.rs`
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`

- [ ] **Step 1: Add service/bind/restore/prometheus tests**

Append tests:

```rust
use crate::config::PortRange;
use crate::service::allocation_service::Service;
use crate::storage::sqlite::Repository;
use std::path::PathBuf;

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

fn service(repo: Repository, runtime: UdpRuntime, start: u16, end: u16) -> Service<UdpRuntime> {
    Service::new(repo, runtime, PortRange { start, end }, 500)
}

async fn reserve_two_udp_port_range() -> (UdpSocket, u16, u16) {
    for start in 20000_u16..60000_u16 {
        let Ok(first) = UdpSocket::bind(("127.0.0.1", start)).await else { continue; };
        if let Ok(second) = UdpSocket::bind(("127.0.0.1", start + 1)).await {
            drop(second);
            return (first, start, start + 1);
        }
    }
    panic!("could not reserve two UDP ports");
}

#[tokio::test]
async fn udp_service_skips_busy_port_after_runtime_create_bind_failure() {
    let (_busy, busy_port, next_port) = reserve_two_udp_port_range().await;
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
    let (repo, _path) = temp_repo_with_path().await;
    let svc = service(repo, runtime, busy_port, next_port);

    let alloc = svc.create_allocation(Protocol::Udp, None).await.unwrap();
    assert_eq!(alloc.port, next_port);
    assert_eq!(metrics.bind_fail_total.load(), 1);
    assert_eq!(metrics.runtime_apply_total.load(), 1);
}

#[tokio::test]
async fn udp_service_restore_recreates_listener_and_forwards_persisted_binding() {
    let relay_port = free_udp_port().await;
    let (target_port, target_task) = start_udp_echo_server().await;
    let (repo, path) = temp_repo_with_path().await;
    let runtime_one = UdpRuntime::new(UdpRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let svc_one = service(repo, runtime_one.clone(), relay_port, relay_port);
    let alloc = svc_one.create_allocation(Protocol::Udp, Some(target_port)).await.unwrap();
    svc_one.set_target(&alloc.id, "127.0.0.1").await.unwrap();
    runtime_one.delete(&alloc.id, 500).await.unwrap();
    drop(svc_one);

    let repo_two = Repository::open(&path).await.unwrap();
    let runtime_two = UdpRuntime::new(UdpRuntimeConfig::loopback(Arc::new(Metrics::default())));
    let svc_two = service(repo_two, runtime_two.clone(), relay_port, relay_port);
    svc_two.restore_all(500).await.unwrap();

    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    send_udp_and_expect(&client, relay_port, b"restore").await;

    runtime_two.delete(&alloc.id, 500).await.unwrap();
    target_task.abort();
}

#[tokio::test]
async fn udp_runtime_restore_bind_failure_reports_restore_error_and_metrics() {
    let busy = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let busy_port = busy.local_addr().unwrap().port();
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));

    let error = runtime.restore(&allocation("alloc-restore-fail", busy_port, None, None), 500).await.unwrap_err();
    assert_eq!(error, crate::runtime::facade::RuntimeError::RuntimeRestoreFailed);
    assert_eq!(metrics.bind_fail_total.load(), 1);
    assert_eq!(metrics.restore_failures_total.load(), 1);
    assert!(runtime.snapshot("alloc-restore-fail").await.unwrap().is_none());
}

#[tokio::test]
async fn udp_runtime_snapshot_rows_feed_prometheus_renderer() {
    let metrics = Arc::new(Metrics::default());
    let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
    let relay_port = free_udp_port().await;
    let (target_port, target_task) = start_udp_echo_server().await;
    runtime.create(&allocation("alloc-prom", relay_port, Some(target_port), Some("127.0.0.1")), 500).await.unwrap();
    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    send_udp_and_expect(&client, relay_port, b"prom").await;
    wait_for_udp_metrics(&runtime, &metrics, 1, 1).await;

    let rows = runtime.snapshot_listener_metrics().await.unwrap();
    assert_eq!(rows[0].protocol, Protocol::Udp);
    assert_eq!(rows[0].rx_bytes_total, 4);
    assert_eq!(rows[0].tx_bytes_total, 4);
    let rendered = crate::prometheus::render(&rows);
    assert!(rendered.contains(&format!("relayd_connections_current{{port=\"{}\",protocol=\"udp\"}} 1\n", relay_port)));
    assert!(rendered.contains(&format!("relayd_rx_bytes_per_second{{port=\"{}\",protocol=\"udp\"}} 0\n", relay_port)));
    assert!(rendered.contains(&format!("relayd_tx_bytes_per_second{{port=\"{}\",protocol=\"udp\"}} 0\n", relay_port)));

    runtime.delete("alloc-prom", 500).await.unwrap();
    target_task.abort();
}
```

- [ ] **Step 2: Run red tests**

Run:

```bash
CARGO_TARGET_DIR=target cargo test --locked runtime::udp::tests::udp_
```

Expected: FAIL until bind/restore/service behavior is complete.

- [ ] **Step 3: Implement bind/restore/service fixes**

Ensure create bind failure returns `RuntimeCreateFailed`; restore bind failure returns `RuntimeRestoreFailed`; successful restore increments `runtime_apply_total`; service tests pass without changing service API. `snapshot_listener_metrics` must produce UDP rows with active sessions and rx/tx totals.

- [ ] **Step 4: Run M4 and full verification**

Run:

```bash
cargo fmt -- --check
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings
zig build test
```

Expected: all PASS.

- [ ] **Step 5: Update milestone ledger with M4 status**

Append this block after M3 status in `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`:

```markdown
## M4 implementation status

- Status: implemented in Rust UDP runtime with loopback listener lifecycle, baseline per-client session forwarding, no-host rejection, TTL cleanup, update/delete stale-session cleanup, restore, and UDP listener metrics.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `zig build test`.
- Scope note: Dual-protocol real runtime parity, UDP workerized/io_uring/GRO/fast-path optional lanes, and full Prometheus rate semantics remain assigned to M5-M6.
```

- [ ] **Step 6: Dispatch final independent spec-compliance reviewer**

Ask a fresh reviewer to compare the final diff against `docs/superpowers/specs/2026-05-15-rust-migration-m4-baseline-udp-runtime.md`. Required final response must contain `APPROVED` or concrete blocking gaps. Fix blocking gaps and re-review until `APPROVED`.

- [ ] **Step 7: Commit and push**

Run:

```bash
git status --short
git add src/runtime/mod.rs src/runtime/udp.rs docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md docs/superpowers/specs/2026-05-15-rust-migration-m4-baseline-udp-runtime.md docs/superpowers/plans/2026-05-15-rust-migration-m4-baseline-udp-runtime.md
git commit -m "Bring default UDP forwarding into Rust runtime" \
  -m "Add a real Tokio UDP runtime behind the existing facade so the Rust service can bind loopback UDP listeners, reject unbound datagrams, forward per-client sessions, clean idle sessions, restore persisted UDP allocations, and report UDP listener metrics without changing HTTP API definitions." \
  -m "Constraint: Zig API interfaces and definitions must remain unchanged during migration." \
  -m "Rejected: Porting UDP workerized/io_uring/GRO/fast-path lanes in M4 | They are optional feature-flagged paths deferred by the migration ledger." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Directive: Keep both-protocol runtime parity and Prometheus rate semantics in their assigned M5-M6 milestones." \
  -m "Tested: cargo fmt -- --check; cargo test --locked; cargo clippy --locked --lib --tests -- -D warnings; zig build test" \
  -m "Not-tested: UDP workerized/io_uring/GRO/fast-path paths, dual-protocol real runtime, production Rust binary cutover" \
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"
git push
```

Expected: commit and push succeed.
