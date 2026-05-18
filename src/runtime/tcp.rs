use crate::metrics::Metrics;
use crate::model::{Allocation, ErrorKind, Protocol, RuntimeStatus};
use crate::runtime::facade::{ListenerMetricsSnapshot, ObservedState, RuntimeError, RuntimeFacade};
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};

#[derive(Clone)]
pub struct TcpRuntimeConfig {
    bind_host: String,
    metrics: Arc<Metrics>,
}

impl TcpRuntimeConfig {
    pub fn loopback(metrics: Arc<Metrics>) -> Self {
        Self::with_bind_host("127.0.0.1", metrics)
    }

    pub fn with_bind_host(bind_host: impl Into<String>, metrics: Arc<Metrics>) -> Self {
        Self {
            bind_host: bind_host.into(),
            metrics,
        }
    }
}

#[derive(Clone)]
pub struct TcpRuntime {
    config: TcpRuntimeConfig,
    entries: Arc<Mutex<HashMap<String, Arc<ListenerEntry>>>>,
}

struct ListenerEntry {
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
    active: AtomicU64,
    rx: AtomicU64,
    tx: AtomicU64,
}

struct ActiveSessionGuard {
    entry: Arc<ListenerEntry>,
}

impl ActiveSessionGuard {
    fn new(entry: Arc<ListenerEntry>) -> Self {
        entry.global_metrics.tcp_session_create_total.inc();
        entry.global_metrics.tcp_active_sessions.inc();
        entry.metrics.active.fetch_add(1, Ordering::Relaxed);
        Self { entry }
    }
}

impl Drop for ActiveSessionGuard {
    fn drop(&mut self) {
        self.entry.metrics.active.fetch_sub(1, Ordering::Relaxed);
        self.entry.global_metrics.tcp_active_sessions.dec();
        self.entry.global_metrics.tcp_session_close_total.inc();
    }
}

impl TcpRuntime {
    pub fn new(config: TcpRuntimeConfig) -> Self {
        Self {
            config,
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn entry_state_for(allocation: &Allocation) -> EntryState {
        match (&allocation.host, allocation.target_port) {
            (Some(host), Some(target_port)) => EntryState {
                effective_host: Some(host.clone()),
                effective_target_port: Some(target_port),
                runtime_status: RuntimeStatus::Active,
                error_kind: None,
                last_error: None,
            },
            _ => EntryState {
                effective_host: None,
                effective_target_port: None,
                runtime_status: RuntimeStatus::RejectingNoHost,
                error_kind: None,
                last_error: None,
            },
        }
    }

    fn bind_addr(&self, port: u16) -> Result<SocketAddr, RuntimeError> {
        let host: IpAddr = self
            .config
            .bind_host
            .parse()
            .map_err(|_| RuntimeError::RuntimeCreateFailed)?;
        Ok(SocketAddr::new(host, port))
    }

    async fn spawn_accept_loop(entry: Arc<ListenerEntry>, listener: TcpListener) {
        let mut shutdown_rx = entry.shutdown.subscribe();
        let task_entry = entry.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_ok() && *shutdown_rx.borrow() {
                            break;
                        }
                        if changed.is_err() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { break; };
                        task_entry.global_metrics.tcp_listener_accept_total.inc();
                        Self::handle_client(task_entry.clone(), client).await;
                    }
                }
            }
        });
        *entry.accept_task.lock().await = Some(task);
    }

    async fn handle_client(entry: Arc<ListenerEntry>, mut client: TcpStream) {
        let mut sessions = entry.sessions.lock().await;
        let state = entry.state.read().await.clone();
        let (Some(host), Some(target_port)) = (state.effective_host, state.effective_target_port)
        else {
            drop(sessions);
            entry.global_metrics.rejected_no_host_total.inc();
            let mut discard = [0_u8; 1024];
            let _ = timeout(Duration::from_millis(50), client.read(&mut discard)).await;
            let _ = client.shutdown().await;
            return;
        };

        let session_entry = entry.clone();
        let session = tokio::spawn(async move {
            match TcpStream::connect((host.as_str(), target_port)).await {
                Ok(mut upstream) => {
                    session_entry
                        .global_metrics
                        .tcp_upstream_connect_total
                        .inc();
                    session_entry.global_metrics.tcp_copy_fallback_total.inc();
                    let _active_session = ActiveSessionGuard::new(session_entry.clone());
                    match tokio::io::copy_bidirectional(&mut client, &mut upstream).await {
                        Ok((from_client, from_upstream)) => {
                            session_entry
                                .metrics
                                .tx
                                .fetch_add(from_client, Ordering::Relaxed);
                            session_entry
                                .metrics
                                .rx
                                .fetch_add(from_upstream, Ordering::Relaxed);
                        }
                        Err(error) => {
                            let mut state = session_entry.state.write().await;
                            state.error_kind = Some(ErrorKind::ApplyFailed);
                            state.last_error = Some(error.to_string());
                        }
                    }
                }
                Err(error) => {
                    session_entry
                        .global_metrics
                        .tcp_upstream_connect_fail_total
                        .inc();
                    let mut state = session_entry.state.write().await;
                    state.error_kind = Some(ErrorKind::ApplyFailed);
                    state.last_error = Some(error.to_string());
                    let _ = client.shutdown().await;
                }
            }
        });
        sessions.push(session);
    }

    async fn close_sessions(entry: &ListenerEntry) {
        let sessions = {
            let mut sessions = entry.sessions.lock().await;
            sessions.drain(..).collect::<Vec<_>>()
        };
        for session in sessions {
            session.abort();
            let _ = session.await;
        }
    }

    async fn stop_entry(entry: Arc<ListenerEntry>) {
        let _ = entry.shutdown.send(true);
        if let Some(task) = entry.accept_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        Self::close_sessions(&entry).await;
    }

    pub async fn shutdown_all(&self) {
        let entries = {
            let mut entries = self.entries.lock().await;
            entries.drain().map(|(_, entry)| entry).collect::<Vec<_>>()
        };
        for entry in entries {
            Self::stop_entry(entry).await;
        }
    }

    async fn bind_entry(
        &self,
        allocation: &Allocation,
    ) -> Result<Arc<ListenerEntry>, RuntimeError> {
        let addr = self.bind_addr(allocation.port)?;
        let listener = TcpListener::bind(addr).await.map_err(|_| {
            self.config.metrics.bind_fail_total.inc();
            RuntimeError::RuntimeCreateFailed
        })?;
        let (shutdown, _shutdown_rx) = watch::channel(false);
        let entry = Arc::new(ListenerEntry {
            port: allocation.port,
            state: RwLock::new(Self::entry_state_for(allocation)),
            metrics: ListenerMetrics::default(),
            shutdown,
            accept_task: Mutex::new(None),
            sessions: Mutex::new(Vec::new()),
            global_metrics: self.config.metrics.clone(),
        });
        Self::spawn_accept_loop(entry.clone(), listener).await;
        Ok(entry)
    }
}

#[async_trait]
impl RuntimeFacade for TcpRuntime {
    async fn create(&self, allocation: &Allocation, _timeout_ms: u32) -> Result<(), RuntimeError> {
        if allocation.protocol != Protocol::Tcp {
            return Ok(());
        }
        let entry = self.bind_entry(allocation).await?;
        self.entries
            .lock()
            .await
            .insert(allocation.id.clone(), entry);
        self.config.metrics.runtime_apply_total.inc();
        Ok(())
    }

    async fn update(&self, allocation: &Allocation, _timeout_ms: u32) -> Result<(), RuntimeError> {
        let entry = self
            .entries
            .lock()
            .await
            .get(&allocation.id)
            .cloned()
            .ok_or(RuntimeError::RuntimeUpdateFailed)?;
        let new_state = Self::entry_state_for(allocation);
        if new_state.runtime_status == RuntimeStatus::RejectingNoHost {
            self.config.metrics.rejected_no_host_total.inc();
        }
        *entry.state.write().await = new_state;
        Self::close_sessions(&entry).await;
        self.config.metrics.runtime_apply_total.inc();
        Ok(())
    }

    async fn delete(&self, id: &str, _timeout_ms: u32) -> Result<(), RuntimeError> {
        let Some(entry) = self.entries.lock().await.remove(id) else {
            return Ok(());
        };
        Self::stop_entry(entry).await;
        Ok(())
    }

    async fn restore(&self, allocation: &Allocation, _timeout_ms: u32) -> Result<(), RuntimeError> {
        if allocation.protocol != Protocol::Tcp {
            return Ok(());
        }
        let addr = self
            .bind_addr(allocation.port)
            .map_err(|_| RuntimeError::RuntimeRestoreFailed)?;
        let listener = TcpListener::bind(addr).await.map_err(|_| {
            self.config.metrics.bind_fail_total.inc();
            self.config.metrics.restore_failures_total.inc();
            RuntimeError::RuntimeRestoreFailed
        })?;
        let (shutdown, _shutdown_rx) = watch::channel(false);
        let entry = Arc::new(ListenerEntry {
            port: allocation.port,
            state: RwLock::new(Self::entry_state_for(allocation)),
            metrics: ListenerMetrics::default(),
            shutdown,
            accept_task: Mutex::new(None),
            sessions: Mutex::new(Vec::new()),
            global_metrics: self.config.metrics.clone(),
        });
        Self::spawn_accept_loop(entry.clone(), listener).await;
        self.entries
            .lock()
            .await
            .insert(allocation.id.clone(), entry);
        self.config.metrics.runtime_apply_total.inc();
        Ok(())
    }

    async fn snapshot(&self, id: &str) -> Result<Option<ObservedState>, RuntimeError> {
        let entry = self.entries.lock().await.get(id).cloned();
        let Some(entry) = entry else {
            return Ok(None);
        };
        let state = entry.state.read().await;
        Ok(Some(ObservedState {
            effective_target_port: state.effective_target_port,
            effective_host: state.effective_host.clone(),
            runtime_status: state.runtime_status,
            error_kind: state.error_kind,
            last_error: state.last_error.clone(),
        }))
    }

    async fn snapshot_listener_metrics(
        &self,
    ) -> Result<Vec<ListenerMetricsSnapshot>, RuntimeError> {
        let entries: Vec<Arc<ListenerEntry>> =
            self.entries.lock().await.values().cloned().collect();
        Ok(entries
            .into_iter()
            .map(|entry| ListenerMetricsSnapshot {
                port: entry.port,
                protocol: Protocol::Tcp,
                connections_current: entry.metrics.active.load(Ordering::Relaxed),
                rx_bytes_total: entry.metrics.rx.load(Ordering::Relaxed),
                tx_bytes_total: entry.metrics.tx.load(Ordering::Relaxed),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PortRange;
    use crate::metrics::Metrics;
    use crate::model::{Allocation, Protocol, RuntimeStatus};
    use crate::runtime::facade::RuntimeFacade;
    use crate::service::allocation_service::Service;
    use crate::storage::sqlite::Repository;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{Duration, timeout};

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

    async fn reserve_two_port_range() -> (TcpListener, u16, u16) {
        for start in 20000_u16..60000_u16 {
            let Ok(first) = TcpListener::bind(("127.0.0.1", start)).await else {
                continue;
            };
            if let Ok(second) = TcpListener::bind(("127.0.0.1", start + 1)).await {
                drop(second);
                return (first, start, start + 1);
            }
        }
        panic!("could not reserve a two-port range for test");
    }

    async fn start_echo_server() -> (u16, JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
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
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        if socket.write_all(prefix).await.is_err() {
                            break;
                        }
                        if socket.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        (port, task)
    }

    async fn temp_repo_with_dir() -> (Repository, TempDir) {
        let parent = std::env::current_dir()
            .unwrap()
            .join("target/relayd-test-dbs");
        std::fs::create_dir_all(&parent).unwrap();
        let dir = tempfile::tempdir_in(parent).unwrap();
        let path = dir.path().join("relayd.sqlite");
        let repo = Repository::open(&path).await.unwrap();
        (repo, dir)
    }

    fn service(repo: Repository, runtime: TcpRuntime, start: u16, end: u16) -> Service<TcpRuntime> {
        Service::new(repo, runtime, PortRange { start, end }, 500)
    }

    async fn wait_for_forwarding_metrics(runtime: &TcpRuntime, metrics: &Metrics, bytes: u64) {
        timeout(Duration::from_secs(2), async {
            loop {
                let rows = runtime.snapshot_listener_metrics().await.unwrap();
                if metrics.tcp_session_close_total.load() == 1
                    && metrics.tcp_active_sessions.load() == 0
                    && rows.len() == 1
                    && rows[0].rx_bytes_total >= bytes
                    && rows[0].tx_bytes_total >= bytes
                {
                    return rows;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("forwarding metrics did not settle");
    }

    async fn wait_for_active_sessions(runtime: &TcpRuntime, metrics: &Metrics, expected: u64) {
        timeout(Duration::from_secs(2), async {
            loop {
                let rows = runtime.snapshot_listener_metrics().await.unwrap();
                if metrics.tcp_active_sessions.load() == expected
                    && rows.len() == 1
                    && rows[0].connections_current == expected
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("active session gauges did not settle");
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
        let read = timeout(Duration::from_millis(500), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, 0);
        assert_eq!(metrics.rejected_no_host_total.load(), 1);

        runtime.delete("alloc-no-host", 500).await.unwrap();
    }

    #[tokio::test]
    async fn tcp_runtime_forwards_bytes_after_binding_and_records_metrics() {
        let metrics = Arc::new(Metrics::default());
        let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_port().await;
        let (target_port, target_task) = start_echo_server().await;
        let alloc = allocation("alloc-active", relay_port, None, None);
        runtime.create(&alloc, 500).await.unwrap();
        runtime
            .update(
                &allocation(
                    "alloc-active",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        let mut client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
        client.write_all(b"hello tcp").await.unwrap();
        let mut buf = [0_u8; 9];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello tcp");
        drop(client);

        wait_for_forwarding_metrics(&runtime, &metrics, 9).await;
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

    #[tokio::test]
    async fn tcp_service_delete_releases_listener_port() {
        let metrics = Arc::new(Metrics::default());
        let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
        let port = free_port().await;
        let (repo, _file) = temp_repo_with_dir().await;
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
        let (repo, dir) = temp_repo_with_dir().await;
        let runtime_one = TcpRuntime::new(TcpRuntimeConfig::loopback(Arc::new(Metrics::default())));
        let svc_one = service(repo, runtime_one.clone(), port, port);
        let alloc = svc_one
            .create_allocation(Protocol::Tcp, Some(target_port))
            .await
            .unwrap();
        svc_one.set_target(&alloc.id, "127.0.0.1").await.unwrap();
        drop(svc_one);
        runtime_one.delete(&alloc.id, 500).await.unwrap();

        let repo_two = Repository::open(dir.path().join("relayd.sqlite"))
            .await
            .unwrap();
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
        let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_port().await;
        let (target_one_port, target_one_task) = start_prefix_echo_server(b"one:").await;
        let (target_two_port, target_two_task) = start_prefix_echo_server(b"two:").await;
        runtime
            .create(
                &allocation(
                    "alloc-retarget",
                    relay_port,
                    Some(target_one_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        let mut old_client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
        old_client.write_all(b"before").await.unwrap();
        let mut old_buf = [0_u8; 10];
        old_client.read_exact(&mut old_buf).await.unwrap();
        assert_eq!(&old_buf, b"one:before");

        runtime
            .update(
                &allocation(
                    "alloc-retarget",
                    relay_port,
                    Some(target_two_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();
        let mut after_update_read = [0_u8; 1];
        let old_closed = timeout(
            Duration::from_millis(500),
            old_client.read(&mut after_update_read),
        )
        .await
        .unwrap()
        .unwrap();
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
        runtime
            .create(
                &allocation(
                    "alloc-abort",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        let _client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
        wait_for_active_sessions(&runtime, &metrics, 1).await;

        runtime
            .update(&allocation("alloc-abort", relay_port, None, None), 500)
            .await
            .unwrap();
        wait_for_active_sessions(&runtime, &metrics, 0).await;

        runtime.delete("alloc-abort", 500).await.unwrap();
        target_task.abort();
    }

    #[tokio::test]
    async fn tcp_runtime_binding_delete_closes_existing_session_and_rejects_new_clients() {
        let metrics = Arc::new(Metrics::default());
        let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_port().await;
        let (target_port, target_task) = start_echo_server().await;
        let alloc = allocation(
            "alloc-close",
            relay_port,
            Some(target_port),
            Some("127.0.0.1"),
        );
        runtime.create(&alloc, 500).await.unwrap();

        let mut client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
        client.write_all(b"before-detach").await.unwrap();
        let mut before_buf = [0_u8; 13];
        client.read_exact(&mut before_buf).await.unwrap();
        assert_eq!(&before_buf, b"before-detach");
        wait_for_active_sessions(&runtime, &metrics, 1).await;

        runtime
            .update(&allocation("alloc-close", relay_port, None, None), 500)
            .await
            .unwrap();
        wait_for_active_sessions(&runtime, &metrics, 0).await;
        let mut stale_buf = [0_u8; 1];
        let stale_read = timeout(Duration::from_millis(500), client.read(&mut stale_buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stale_read, 0);

        let observed = runtime.snapshot("alloc-close").await.unwrap().unwrap();
        assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
        assert_eq!(metrics.rejected_no_host_total.load(), 1);
        let mut new_client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
        new_client.write_all(b"stale").await.unwrap();
        let mut buf = [0_u8; 5];
        let read = timeout(Duration::from_millis(500), new_client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, 0);

        runtime.delete("alloc-close", 500).await.unwrap();
        target_task.abort();
    }

    #[tokio::test]
    async fn tcp_service_skips_busy_port_after_runtime_create_bind_failure() {
        let (_busy, busy_port, next_port) = reserve_two_port_range().await;
        let metrics = Arc::new(Metrics::default());
        let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
        let (repo, _dir) = temp_repo_with_dir().await;
        let svc = service(repo, runtime, busy_port, next_port);

        let alloc = svc.create_allocation(Protocol::Tcp, None).await.unwrap();

        assert_eq!(alloc.port, next_port);
        assert_eq!(metrics.bind_fail_total.load(), 1);
        assert_eq!(metrics.runtime_apply_total.load(), 1);
    }

    #[tokio::test]
    async fn tcp_runtime_restore_bind_failure_reports_restore_error_and_metrics() {
        let busy = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let busy_port = busy.local_addr().unwrap().port();
        let metrics = Arc::new(Metrics::default());
        let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));

        let error = runtime
            .restore(
                &allocation("alloc-restore-fail", busy_port, None, None),
                500,
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            crate::runtime::facade::RuntimeError::RuntimeRestoreFailed
        );
        assert_eq!(metrics.bind_fail_total.load(), 1);
        assert_eq!(metrics.restore_failures_total.load(), 1);
        assert!(
            runtime
                .snapshot("alloc-restore-fail")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn tcp_runtime_snapshot_rows_feed_prometheus_renderer() {
        let metrics = Arc::new(Metrics::default());
        let runtime = TcpRuntime::new(TcpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_port().await;
        let (target_port, target_task) = start_echo_server().await;
        runtime
            .create(
                &allocation(
                    "alloc-prom",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        let mut client = TcpStream::connect(("127.0.0.1", relay_port)).await.unwrap();
        client.write_all(b"prom").await.unwrap();
        let mut buf = [0_u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        drop(client);
        wait_for_forwarding_metrics(&runtime, &metrics, 4).await;
        let rows = runtime.snapshot_listener_metrics().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].port, relay_port);
        assert_eq!(rows[0].protocol, Protocol::Tcp);
        assert_eq!(rows[0].connections_current, 0);
        assert_eq!(rows[0].rx_bytes_total, 4);
        assert_eq!(rows[0].tx_bytes_total, 4);
        let rendered = crate::prometheus::render(&rows);
        assert!(rendered.contains(&format!(
            "relayd_connections_current{{port=\"{}\",protocol=\"tcp\"}} 0\n",
            relay_port
        )));
        assert!(rendered.contains(&format!(
            "relayd_rx_bytes_per_second{{port=\"{}\",protocol=\"tcp\"}} 0\n",
            relay_port
        )));
        assert!(rendered.contains(&format!(
            "relayd_tx_bytes_per_second{{port=\"{}\",protocol=\"tcp\"}} 0\n",
            relay_port
        )));

        runtime.delete("alloc-prom", 500).await.unwrap();
        target_task.abort();
    }
}
