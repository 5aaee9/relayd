use crate::metrics::Metrics;
use crate::model::{Allocation, ErrorKind, Protocol, RuntimeStatus};
use crate::runtime::facade::{ListenerMetricsSnapshot, ObservedState, RuntimeError, RuntimeFacade};
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
        Self::with_bind_host("127.0.0.1", metrics)
    }

    pub fn with_bind_host(bind_host: impl Into<String>, metrics: Arc<Metrics>) -> Self {
        Self {
            bind_host: bind_host.into(),
            metrics,
            session_ttl: Duration::from_millis(60_000),
            max_sessions: 65_536,
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

struct ListenerEntry {
    port: u16,
    listener_socket: Arc<UdpSocket>,
    state: RwLock<EntryState>,
    metrics: ListenerMetrics,
    shutdown: watch::Sender<bool>,
    receive_task: Mutex<Option<JoinHandle<()>>>,
    cleanup_task: Mutex<Option<JoinHandle<()>>>,
    sessions: Mutex<HashMap<SocketAddr, Arc<UdpSession>>>,
    generation: AtomicU64,
    max_sessions: usize,
    global_metrics: Arc<Metrics>,
}

#[derive(Clone, Debug)]
struct EntryState {
    effective_host: Option<String>,
    effective_target_port: Option<u16>,
    runtime_status: RuntimeStatus,
    error_kind: Option<ErrorKind>,
    last_error: Option<String>,
    generation: u64,
}

#[derive(Default)]
struct ListenerMetrics {
    active: AtomicU64,
    rx: AtomicU64,
    tx: AtomicU64,
}

struct UdpSession {
    generation: u64,
    client_addr: SocketAddr,
    upstream: Arc<UdpSocket>,
    closed: AtomicBool,
    last_seen: Mutex<Instant>,
    reply_task: Mutex<Option<JoinHandle<()>>>,
    _guard: SessionGuard,
}

#[derive(Clone, Copy)]
enum SessionRemovalReason {
    Expired,
    DropReply,
    Error,
}

struct SessionGuard {
    entry: Arc<ListenerEntry>,
    active: bool,
}

impl SessionGuard {
    fn new(entry: Arc<ListenerEntry>) -> Self {
        entry.global_metrics.udp_session_create_total.inc();
        entry.global_metrics.udp_active_sessions.inc();
        entry.metrics.active.fetch_add(1, Ordering::Relaxed);
        Self {
            entry,
            active: true,
        }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        if self.active {
            self.entry.metrics.active.fetch_sub(1, Ordering::Relaxed);
            self.entry.global_metrics.udp_active_sessions.dec();
        }
    }
}

impl UdpRuntime {
    pub fn new(config: UdpRuntimeConfig) -> Self {
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
                generation: 0,
            },
            _ => EntryState {
                effective_host: None,
                effective_target_port: None,
                runtime_status: RuntimeStatus::RejectingNoHost,
                error_kind: None,
                last_error: None,
                generation: 0,
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

    async fn spawn_receive_loop(entry: Arc<ListenerEntry>) {
        let mut shutdown_rx = entry.shutdown.subscribe();
        let task_entry = entry.clone();
        let task = tokio::spawn(async move {
            let mut buf = [0_u8; 2048];
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    received = task_entry.listener_socket.recv_from(&mut buf) => {
                        match received {
                            Ok((n, peer)) => {
                                Self::handle_datagram(task_entry.clone(), &buf[..n], peer).await;
                            }
                            Err(_) => {
                                task_entry.global_metrics.udp_recv_errors_total.inc();
                                break;
                            }
                        }
                    }
                }
            }
        });
        *entry.receive_task.lock().await = Some(task);
    }

    async fn spawn_cleanup_loop(entry: Arc<ListenerEntry>, session_ttl: Duration) {
        let mut shutdown_rx = entry.shutdown.subscribe();
        let task_entry = entry.clone();
        let task = tokio::spawn(async move {
            let interval_duration = session_ttl.min(Duration::from_millis(50));
            let mut interval = tokio::time::interval(interval_duration);
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        Self::expire_idle_sessions(&task_entry, session_ttl).await;
                    }
                }
            }
        });
        *entry.cleanup_task.lock().await = Some(task);
    }

    async fn spawn_reply_loop(entry: Arc<ListenerEntry>, session: Arc<UdpSession>) {
        if session.closed.load(Ordering::Relaxed) {
            return;
        }
        let task_session = session.clone();
        let task_entry = entry.clone();
        let task = tokio::spawn(async move {
            let mut buf = [0_u8; 2048];
            loop {
                if task_session.closed.load(Ordering::Relaxed) {
                    break;
                }
                match task_session.upstream.recv(&mut buf).await {
                    Ok(n) => {
                        let current = {
                            let sessions = task_entry.sessions.lock().await;
                            sessions
                                .get(&task_session.client_addr)
                                .is_some_and(|current| {
                                    Arc::ptr_eq(current, &task_session)
                                        && current.generation
                                            == task_entry.generation.load(Ordering::Relaxed)
                                })
                        };

                        if !current {
                            task_session.closed.store(true, Ordering::Relaxed);
                            task_entry.global_metrics.udp_reply_stale_total.inc();
                            break;
                        }

                        match task_entry
                            .listener_socket
                            .send_to(&buf[..n], task_session.client_addr)
                            .await
                        {
                            Ok(sent) => {
                                task_entry.global_metrics.udp_reply_primary_total.inc();
                                task_entry.global_metrics.udp_packets_out_total.inc();
                                task_entry
                                    .global_metrics
                                    .udp_bytes_out_total
                                    .add(sent as u64);
                                task_entry
                                    .metrics
                                    .tx
                                    .fetch_add(sent as u64, Ordering::Relaxed);
                                *task_session.last_seen.lock().await = Instant::now();
                            }
                            Err(_) => {
                                task_entry.global_metrics.udp_send_errors_total.inc();
                                task_entry.global_metrics.udp_reply_drop_total.inc();
                                Self::remove_session(
                                    &task_entry,
                                    task_session.client_addr,
                                    &task_session,
                                    SessionRemovalReason::Error,
                                )
                                .await;
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        task_entry.global_metrics.udp_recv_errors_total.inc();
                        Self::remove_session(
                            &task_entry,
                            task_session.client_addr,
                            &task_session,
                            SessionRemovalReason::Error,
                        )
                        .await;
                        break;
                    }
                }
            }
        });
        *session.reply_task.lock().await = Some(task);
    }

    async fn create_session(
        entry: Arc<ListenerEntry>,
        client_addr: SocketAddr,
        host: &str,
        target_port: u16,
        generation: u64,
    ) -> Option<Arc<UdpSession>> {
        let upstream = match Self::bind_upstream_socket().await {
            Ok(socket) => Arc::new(socket),
            Err(_) => {
                entry.global_metrics.udp_send_errors_total.inc();
                entry.global_metrics.udp_drop_total.inc();
                return None;
            }
        };

        if upstream.connect((host, target_port)).await.is_err() {
            entry.global_metrics.udp_send_errors_total.inc();
            entry.global_metrics.udp_drop_total.inc();
            return None;
        }

        let session = Arc::new(UdpSession {
            generation,
            client_addr,
            upstream,
            closed: AtomicBool::new(false),
            last_seen: Mutex::new(Instant::now()),
            reply_task: Mutex::new(None),
            _guard: SessionGuard::new(entry.clone()),
        });
        Some(session)
    }

    async fn bind_upstream_socket() -> std::io::Result<UdpSocket> {
        UdpSocket::bind(("0.0.0.0", 0)).await
    }

    async fn session_for(
        entry: Arc<ListenerEntry>,
        client_addr: SocketAddr,
        host: &str,
        target_port: u16,
        generation: u64,
    ) -> Option<Arc<UdpSession>> {
        {
            let sessions = entry.sessions.lock().await;
            if let Some(session) = sessions.get(&client_addr)
                && session.generation == generation
            {
                return Some(session.clone());
            }
            if sessions.len() >= entry.max_sessions {
                entry.global_metrics.udp_drop_total.inc();
                return None;
            }
        }

        let session =
            Self::create_session(entry.clone(), client_addr, host, target_port, generation).await?;
        let replaced = {
            let mut sessions = entry.sessions.lock().await;
            if let Some(existing) = sessions.get(&client_addr)
                && existing.generation == generation
            {
                return Some(existing.clone());
            }
            if entry.generation.load(Ordering::Relaxed) != generation {
                entry.global_metrics.udp_reply_stale_total.inc();
                return None;
            }
            sessions.insert(client_addr, session.clone())
        };
        if let Some(replaced) = replaced {
            replaced.closed.store(true, Ordering::Relaxed);
            entry.global_metrics.udp_reply_drop_total.inc();
            Self::abort_reply_task(&replaced, true).await;
        }

        Self::spawn_reply_loop(entry, session.clone()).await;
        Some(session)
    }

    async fn abort_reply_task(session: &Arc<UdpSession>, await_abort: bool) {
        if let Some(task) = session.reply_task.lock().await.take() {
            task.abort();
            if await_abort {
                let _ = task.await;
            }
        }
    }

    fn record_removal(entry: &ListenerEntry, reason: SessionRemovalReason) {
        match reason {
            SessionRemovalReason::Expired => entry.global_metrics.udp_session_expire_total.inc(),
            SessionRemovalReason::DropReply => entry.global_metrics.udp_reply_drop_total.inc(),
            SessionRemovalReason::Error => {}
        }
    }

    async fn remove_session(
        entry: &Arc<ListenerEntry>,
        client_addr: SocketAddr,
        expected: &Arc<UdpSession>,
        reason: SessionRemovalReason,
    ) {
        let session = {
            let mut sessions = entry.sessions.lock().await;
            if sessions
                .get(&client_addr)
                .is_some_and(|current| Arc::ptr_eq(current, expected))
            {
                sessions.remove(&client_addr)
            } else {
                None
            }
        };
        if let Some(session) = session {
            session.closed.store(true, Ordering::Relaxed);
            Self::record_removal(entry, reason);
            Self::abort_reply_task(&session, false).await;
        }
    }

    async fn expire_idle_sessions(entry: &Arc<ListenerEntry>, session_ttl: Duration) {
        let sessions = {
            let sessions = entry.sessions.lock().await;
            sessions
                .iter()
                .map(|(client_addr, session)| (*client_addr, session.clone()))
                .collect::<Vec<_>>()
        };
        let now = Instant::now();
        for (client_addr, session) in sessions {
            let last_seen = *session.last_seen.lock().await;
            if now.duration_since(last_seen) > session_ttl {
                Self::remove_session(entry, client_addr, &session, SessionRemovalReason::Expired)
                    .await;
            }
        }
    }

    async fn handle_datagram(entry: Arc<ListenerEntry>, payload: &[u8], peer: SocketAddr) {
        entry.global_metrics.udp_packets_in_total.inc();
        entry
            .global_metrics
            .udp_bytes_in_total
            .add(payload.len() as u64);
        entry
            .metrics
            .rx
            .fetch_add(payload.len() as u64, Ordering::Relaxed);

        let state = entry.state.read().await.clone();
        let generation = state.generation;
        let (Some(host), Some(target_port)) = (state.effective_host, state.effective_target_port)
        else {
            entry.global_metrics.rejected_no_host_total.inc();
            return;
        };

        let Some(session) =
            Self::session_for(entry.clone(), peer, &host, target_port, generation).await
        else {
            return;
        };
        if entry.generation.load(Ordering::Relaxed) != generation
            || session.generation != generation
            || session.closed.load(Ordering::Relaxed)
        {
            session.closed.store(true, Ordering::Relaxed);
            entry.global_metrics.udp_reply_stale_total.inc();
            return;
        }

        *session.last_seen.lock().await = Instant::now();
        if session.upstream.send(payload).await.is_err() {
            entry.global_metrics.udp_send_errors_total.inc();
            entry.global_metrics.udp_drop_total.inc();
            Self::remove_session(&entry, peer, &session, SessionRemovalReason::Error).await;
        }
    }

    async fn close_sessions(entry: &ListenerEntry, count_as_reply_drop: bool) {
        let sessions = {
            let mut sessions = entry.sessions.lock().await;
            sessions
                .drain()
                .map(|(_, session)| session)
                .collect::<Vec<_>>()
        };
        for session in sessions {
            session.closed.store(true, Ordering::Relaxed);
            if count_as_reply_drop {
                Self::record_removal(entry, SessionRemovalReason::DropReply);
            }
            Self::abort_reply_task(&session, true).await;
        }
    }

    async fn stop_entry(entry: Arc<ListenerEntry>, count_as_reply_drop: bool) {
        let _ = entry.shutdown.send(true);
        if let Some(task) = entry.receive_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = entry.cleanup_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        Self::close_sessions(&entry, count_as_reply_drop).await;
    }

    pub async fn shutdown_all(&self) {
        let entries = {
            let mut entries = self.entries.lock().await;
            entries.drain().map(|(_, entry)| entry).collect::<Vec<_>>()
        };
        for entry in entries {
            Self::stop_entry(entry, false).await;
        }
    }

    fn stop_entry_best_effort(entry: &Arc<ListenerEntry>) {
        let _ = entry.shutdown.send(true);
        if let Ok(mut task) = entry.receive_task.try_lock()
            && let Some(task) = task.take()
        {
            task.abort();
        }
        if let Ok(mut task) = entry.cleanup_task.try_lock()
            && let Some(task) = task.take()
        {
            task.abort();
        }
        if let Ok(mut sessions) = entry.sessions.try_lock() {
            for (_, session) in sessions.drain() {
                session.closed.store(true, Ordering::Relaxed);
                if let Ok(mut task) = session.reply_task.try_lock()
                    && let Some(task) = task.take()
                {
                    task.abort();
                }
            }
        }
    }

    fn shutdown_all_best_effort(&self) {
        if let Ok(mut entries) = self.entries.try_lock() {
            for (_, entry) in entries.drain() {
                Self::stop_entry_best_effort(&entry);
            }
        }
    }

    async fn bind_entry(
        &self,
        allocation: &Allocation,
        restore: bool,
    ) -> Result<Arc<ListenerEntry>, RuntimeError> {
        let addr = self.bind_addr(allocation.port).map_err(|_| {
            if restore {
                RuntimeError::RuntimeRestoreFailed
            } else {
                RuntimeError::RuntimeCreateFailed
            }
        })?;
        let socket = Arc::new(UdpSocket::bind(addr).await.map_err(|_| {
            self.config.metrics.bind_fail_total.inc();
            if restore {
                self.config.metrics.restore_failures_total.inc();
                RuntimeError::RuntimeRestoreFailed
            } else {
                RuntimeError::RuntimeCreateFailed
            }
        })?);
        let (shutdown, _shutdown_rx) = watch::channel(false);
        let entry = Arc::new(ListenerEntry {
            port: allocation.port,
            listener_socket: socket,
            state: RwLock::new(Self::entry_state_for(allocation)),
            metrics: ListenerMetrics::default(),
            shutdown,
            receive_task: Mutex::new(None),
            cleanup_task: Mutex::new(None),
            sessions: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
            max_sessions: self.config.max_sessions,
            global_metrics: self.config.metrics.clone(),
        });
        Self::spawn_receive_loop(entry.clone()).await;
        Self::spawn_cleanup_loop(entry.clone(), self.config.session_ttl).await;
        Ok(entry)
    }
}

impl Drop for UdpRuntime {
    fn drop(&mut self) {
        if Arc::strong_count(&self.entries) == 1 {
            self.shutdown_all_best_effort();
        }
    }
}

#[async_trait]
impl RuntimeFacade for UdpRuntime {
    async fn create(&self, allocation: &Allocation, _timeout_ms: u32) -> Result<(), RuntimeError> {
        if allocation.protocol != Protocol::Udp {
            return Ok(());
        }
        let entry = self.bind_entry(allocation, false).await?;
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
        let mut new_state = Self::entry_state_for(allocation);
        if new_state.runtime_status == RuntimeStatus::RejectingNoHost {
            self.config.metrics.rejected_no_host_total.inc();
        }
        {
            let mut state = entry.state.write().await;
            new_state.generation = state.generation.saturating_add(1);
            entry
                .generation
                .store(new_state.generation, Ordering::Relaxed);
            *state = new_state;
        }
        Self::close_sessions(&entry, true).await;
        self.config.metrics.runtime_apply_total.inc();
        Ok(())
    }

    async fn delete(&self, id: &str, _timeout_ms: u32) -> Result<(), RuntimeError> {
        let Some(entry) = self.entries.lock().await.remove(id) else {
            return Ok(());
        };
        Self::stop_entry(entry, true).await;
        Ok(())
    }

    async fn restore(&self, allocation: &Allocation, _timeout_ms: u32) -> Result<(), RuntimeError> {
        if allocation.protocol != Protocol::Udp {
            return Ok(());
        }
        let entry = self.bind_entry(allocation, true).await?;
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
                protocol: Protocol::Udp,
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
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::net::UdpSocket;
    use tokio::time::{Duration, timeout};

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

    #[test]
    fn udp_runtime_config_defaults_to_65536_max_sessions() {
        let config = UdpRuntimeConfig::loopback(Arc::new(Metrics::default()));

        assert_eq!(config.max_sessions, 65_536);
    }

    #[tokio::test]
    async fn udp_upstream_socket_binds_unspecified_addr_for_non_loopback_targets() {
        let socket = UdpRuntime::bind_upstream_socket().await.unwrap();

        assert_eq!(
            socket.local_addr().unwrap().ip(),
            std::net::Ipv4Addr::UNSPECIFIED
        );
    }

    fn temp_db_path() -> PathBuf {
        let parent = std::env::current_dir()
            .unwrap()
            .join("target/relayd-test-dbs");
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
            let Ok(first) = UdpSocket::bind(("127.0.0.1", start)).await else {
                continue;
            };
            if let Ok(second) = UdpSocket::bind(("127.0.0.1", start + 1)).await {
                drop(second);
                return (first, start, start + 1);
            }
        }
        panic!("could not reserve two UDP ports");
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

    async fn start_udp_capture_server(
        expected: usize,
    ) -> (u16, Arc<Mutex<Vec<(u16, Vec<u8>)>>>, JoinHandle<()>) {
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let records = Arc::new(Mutex::new(Vec::new()));
        let task_records = records.clone();
        let task = tokio::spawn(async move {
            let mut buf = [0_u8; 2048];
            while task_records.lock().await.len() < expected {
                let Ok((n, peer)) = socket.recv_from(&mut buf).await else {
                    break;
                };
                task_records
                    .lock()
                    .await
                    .push((peer.port(), buf[..n].to_vec()));
                let _ = socket.send_to(&buf[..n], peer).await;
            }
        });
        (port, records, task)
    }

    async fn send_udp_and_expect(client: &UdpSocket, relay_port: u16, payload: &[u8]) {
        client
            .send_to(payload, ("127.0.0.1", relay_port))
            .await
            .unwrap();
        let mut buf = [0_u8; 2048];
        let (n, _) = timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], payload);
    }

    async fn wait_for_udp_metrics(
        runtime: &UdpRuntime,
        metrics: &Metrics,
        packets_in: u64,
        packets_out: u64,
    ) {
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
        })
        .await
        .expect("udp metrics did not settle");
    }

    #[tokio::test]
    async fn udp_runtime_create_without_binding_reports_rejecting_no_host_and_drops_datagrams() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_udp_port().await;
        runtime
            .create(&allocation("alloc-no-host", relay_port, None, None), 500)
            .await
            .unwrap();

        let observed = runtime.snapshot("alloc-no-host").await.unwrap().unwrap();
        assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
        assert_eq!(observed.effective_host, None);
        assert_eq!(observed.effective_target_port, None);

        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        client
            .send_to(b"drop-me", ("127.0.0.1", relay_port))
            .await
            .unwrap();
        let mut buf = [0_u8; 16];
        assert!(
            timeout(Duration::from_millis(150), client.recv_from(&mut buf))
                .await
                .is_err()
        );

        let rows = runtime.snapshot_listener_metrics().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].protocol, Protocol::Udp);
        assert_eq!(rows[0].connections_current, 0);
        assert_eq!(metrics.rejected_no_host_total.load(), 1);
        assert_eq!(metrics.udp_session_create_total.load(), 0);
        assert_eq!(metrics.udp_active_sessions.load(), 0);

        runtime.delete("alloc-no-host", 500).await.unwrap();
    }

    #[tokio::test]
    async fn udp_runtime_forwards_datagrams_after_binding_and_records_metrics() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_udp_port().await;
        let (target_port, target_task) = start_udp_echo_server().await;
        runtime
            .create(&allocation("alloc-active", relay_port, None, None), 500)
            .await
            .unwrap();
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
        runtime
            .create(
                &allocation(
                    "alloc-sessions",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

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
                let snapshot_matches = if expected == 0 && rows.is_empty() {
                    true
                } else {
                    !rows.is_empty() && rows[0].connections_current == expected
                };
                if metrics.udp_active_sessions.load() == expected && snapshot_matches {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("udp active sessions did not settle");
    }

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
            let Ok((_n, peer)) = socket.recv_from(&mut buf).await else {
                return;
            };
            let _ = received_tx.send(());
            let _ = release_rx.await;
            let _ = socket.send_to(reply, peer).await;
        });
        DelayedUdpServer {
            port,
            received: received_rx,
            release: release_tx,
            task,
        }
    }

    async fn expect_no_udp_response(client: &UdpSocket) {
        let mut buf = [0_u8; 64];
        assert!(
            timeout(Duration::from_millis(150), client.recv_from(&mut buf))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn udp_runtime_expires_idle_sessions_and_recreates_on_new_traffic() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(
            UdpRuntimeConfig::loopback(metrics.clone()).with_session_ttl(Duration::from_millis(25)),
        );
        let relay_port = free_udp_port().await;
        let (target_port, target_task) = start_udp_echo_server().await;
        runtime
            .create(
                &allocation(
                    "alloc-ttl",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

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

        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        client
            .send_to(b"first", ("127.0.0.1", relay_port))
            .await
            .unwrap();
        let mut buf = [0_u8; 16];
        let (n, _) = timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"one");
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
        wait_for_active_sessions(&runtime, &metrics, 0).await;

        client
            .send_to(b"second", ("127.0.0.1", relay_port))
            .await
            .unwrap();
        let (n, _) = timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
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
        runtime
            .create(
                &allocation(
                    "alloc-delete",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();
        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        send_udp_and_expect(&client, relay_port, b"delete").await;
        wait_for_active_sessions(&runtime, &metrics, 1).await;

        runtime.delete("alloc-delete", 500).await.unwrap();
        assert_eq!(metrics.udp_active_sessions.load(), 0);
        let rebound = UdpSocket::bind(("127.0.0.1", relay_port)).await.unwrap();
        assert_eq!(rebound.local_addr().unwrap().port(), relay_port);
        assert!(
            runtime
                .snapshot_listener_metrics()
                .await
                .unwrap()
                .is_empty()
        );
        target_task.abort();
    }

    #[tokio::test]
    async fn udp_runtime_delayed_reply_from_replaced_target_is_dropped_and_new_target_replies() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_udp_port().await;
        let old = start_delayed_udp_reply_server(b"old").await;
        let (new_port, new_task) = start_udp_fixed_reply_server(b"new").await;
        runtime
            .create(
                &allocation(
                    "alloc-stale-update",
                    relay_port,
                    Some(old.port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        client
            .send_to(b"before", ("127.0.0.1", relay_port))
            .await
            .unwrap();
        old.received.await.unwrap();
        wait_for_active_sessions(&runtime, &metrics, 1).await;

        runtime
            .update(
                &allocation(
                    "alloc-stale-update",
                    relay_port,
                    Some(new_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();
        wait_for_active_sessions(&runtime, &metrics, 0).await;
        let _ = old.release.send(());
        expect_no_udp_response(&client).await;

        client
            .send_to(b"after", ("127.0.0.1", relay_port))
            .await
            .unwrap();
        let mut buf = [0_u8; 16];
        let (n, _) = timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
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
        runtime
            .create(
                &allocation(
                    "alloc-stale-delete",
                    relay_port,
                    Some(old.port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        client
            .send_to(b"before-delete", ("127.0.0.1", relay_port))
            .await
            .unwrap();
        old.received.await.unwrap();
        wait_for_active_sessions(&runtime, &metrics, 1).await;

        runtime.delete("alloc-stale-delete", 500).await.unwrap();
        assert_eq!(metrics.udp_active_sessions.load(), 0);
        assert!(
            runtime
                .snapshot_listener_metrics()
                .await
                .unwrap()
                .is_empty()
        );
        let _ = old.release.send(());
        expect_no_udp_response(&client).await;
        assert!(metrics.udp_reply_stale_total.load() + metrics.udp_reply_drop_total.load() >= 1);
        old.task.abort();
    }

    #[tokio::test]
    async fn udp_runtime_shutdown_without_delete_releases_listener_sessions_and_drops_late_replies()
    {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_udp_port().await;
        let delayed = start_delayed_udp_reply_server(b"after-shutdown").await;
        runtime
            .create(
                &allocation(
                    "alloc-shutdown",
                    relay_port,
                    Some(delayed.port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        client
            .send_to(b"before-shutdown", ("127.0.0.1", relay_port))
            .await
            .unwrap();
        delayed.received.await.unwrap();
        wait_for_active_sessions(&runtime, &metrics, 1).await;

        runtime.shutdown_all().await;
        wait_for_active_sessions(&runtime, &metrics, 0).await;
        let rebound = UdpSocket::bind(("127.0.0.1", relay_port)).await.unwrap();
        assert_eq!(rebound.local_addr().unwrap().port(), relay_port);
        let _ = delayed.release.send(());
        expect_no_udp_response(&client).await;
        delayed.task.abort();
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
        let alloc = svc_one
            .create_allocation(Protocol::Udp, Some(target_port))
            .await
            .unwrap();
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
    async fn udp_runtime_snapshot_rows_feed_prometheus_renderer() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(UdpRuntimeConfig::loopback(metrics.clone()));
        let relay_port = free_udp_port().await;
        let (target_port, target_task) = start_udp_echo_server().await;
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
        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        send_udp_and_expect(&client, relay_port, b"prom").await;
        wait_for_udp_metrics(&runtime, &metrics, 1, 1).await;

        let rows = runtime.snapshot_listener_metrics().await.unwrap();
        assert_eq!(rows[0].protocol, Protocol::Udp);
        assert_eq!(rows[0].rx_bytes_total, 4);
        assert_eq!(rows[0].tx_bytes_total, 4);
        let rendered = crate::prometheus::render(&rows);
        assert!(rendered.contains(&format!(
            "relayd_connections_current{{port=\"{}\",protocol=\"udp\"}} 1\n",
            relay_port
        )));
        assert!(rendered.contains(&format!(
            "relayd_rx_bytes_per_second{{port=\"{}\",protocol=\"udp\"}} 0\n",
            relay_port
        )));
        assert!(rendered.contains(&format!(
            "relayd_tx_bytes_per_second{{port=\"{}\",protocol=\"udp\"}} 0\n",
            relay_port
        )));

        runtime.delete("alloc-prom", 500).await.unwrap();
        target_task.abort();
    }
}
