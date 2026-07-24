use crate::metrics::Metrics;
use crate::model::{Allocation, ErrorKind, Protocol, RuntimeStatus};
use crate::runtime::facade::{ListenerMetricsSnapshot, ObservedState, RuntimeError, RuntimeFacade};
use async_trait::async_trait;
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(target_os = "linux")]
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, RwLock, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

const MAX_SESSION_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const UDP_DATAGRAM_BUFFER_BYTES: usize = 2048;
#[cfg(target_os = "linux")]
const UDP_INITIAL_RECEIVE_BATCH_SIZE: usize = 16;
#[cfg(target_os = "linux")]
// Start each listener at 32 KiB; saturated listeners grow to at most 128 KiB.
const UDP_MAX_RECEIVE_BATCH_SIZE: usize = 64;
#[cfg(not(target_os = "linux"))]
const UDP_INITIAL_RECEIVE_BATCH_SIZE: usize = 1;
#[cfg(not(target_os = "linux"))]
const UDP_MAX_RECEIVE_BATCH_SIZE: usize = 1;

struct UdpReceiveBatch {
    buffers: Vec<[u8; UDP_DATAGRAM_BUFFER_BYTES]>,
    lengths: [usize; UDP_MAX_RECEIVE_BATCH_SIZE],
    peers: [Option<SocketAddr>; UDP_MAX_RECEIVE_BATCH_SIZE],
}

struct UdpBatchSendError {
    sent: usize,
    error: io::Error,
}

impl UdpReceiveBatch {
    fn new() -> Self {
        Self {
            buffers: vec![[0; UDP_DATAGRAM_BUFFER_BYTES]; UDP_INITIAL_RECEIVE_BATCH_SIZE],
            lengths: [0; UDP_MAX_RECEIVE_BATCH_SIZE],
            peers: [None; UDP_MAX_RECEIVE_BATCH_SIZE],
        }
    }

    fn grow_if_saturated(&mut self, received: usize) {
        let capacity = self.buffers.len();
        if received < capacity || capacity >= UDP_MAX_RECEIVE_BATCH_SIZE {
            return;
        }
        let new_capacity = capacity.saturating_mul(2).min(UDP_MAX_RECEIVE_BATCH_SIZE);
        self.buffers
            .resize(new_capacity, [0; UDP_DATAGRAM_BUFFER_BYTES]);
    }
}

#[derive(Clone)]
pub struct UdpRuntimeConfig {
    bind_host: String,
    metrics: Arc<Metrics>,
    session_ttl: Duration,
    max_sessions: usize,
    udp_batch_enabled: bool,
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
            udp_batch_enabled: cfg!(target_os = "linux"),
        }
    }

    pub fn with_session_ttl(mut self, ttl: Duration) -> Self {
        self.session_ttl = ttl;
        self
    }

    pub fn with_udp_max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = max_sessions;
        self
    }

    pub fn with_udp_batch_enabled(mut self, enabled: bool) -> Self {
        self.udp_batch_enabled = enabled;
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
    bind_host: String,
    listener_socket: Arc<UdpSocket>,
    state: RwLock<EntryState>,
    metrics: ListenerMetrics,
    shutdown: watch::Sender<bool>,
    receive_task: Mutex<Option<JoinHandle<()>>>,
    cleanup_task: Mutex<Option<JoinHandle<()>>>,
    sessions: Mutex<HashMap<SocketAddr, Arc<UdpSession>>>,
    empty_since: Mutex<Option<Instant>>,
    generation: AtomicU64,
    max_sessions: usize,
    udp_batch_enabled: bool,
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
    // This timestamp is never held across an await; avoid async-mutex overhead per batch/reply.
    last_seen: StdMutex<Instant>,
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
    fn session_cleanup_interval(session_ttl: Duration) -> Duration {
        session_ttl.min(MAX_SESSION_CLEANUP_INTERVAL)
    }

    pub fn new(config: UdpRuntimeConfig) -> Self {
        Self {
            config,
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn max_sessions(&self) -> usize {
        self.config.max_sessions
    }

    #[cfg(test)]
    pub(crate) fn batch_enabled(&self) -> bool {
        self.config.udp_batch_enabled
    }

    #[cfg(test)]
    async fn session_table_stats(&self, id: &str) -> Option<(usize, usize)> {
        let entry = self.entries.lock().await.get(id).cloned()?;
        let sessions = entry.sessions.lock().await;
        Some((sessions.len(), sessions.capacity()))
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

    #[cfg(target_os = "linux")]
    fn socket_addr_from_storage(storage: &libc::sockaddr_storage) -> io::Result<SocketAddr> {
        match i32::from(storage.ss_family) {
            libc::AF_INET => {
                // SAFETY: the kernel set ss_family to AF_INET and wrote a sockaddr_in.
                let address = unsafe { &*std::ptr::from_ref(storage).cast::<libc::sockaddr_in>() };
                Ok(SocketAddr::new(
                    IpAddr::V4(std::net::Ipv4Addr::from(
                        address.sin_addr.s_addr.to_ne_bytes(),
                    )),
                    u16::from_be(address.sin_port),
                ))
            }
            libc::AF_INET6 => {
                // SAFETY: the kernel set ss_family to AF_INET6 and wrote a sockaddr_in6.
                let address = unsafe { &*std::ptr::from_ref(storage).cast::<libc::sockaddr_in6>() };
                Ok(SocketAddr::V6(std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::from(address.sin6_addr.s6_addr),
                    u16::from_be(address.sin6_port),
                    u32::from_be(address.sin6_flowinfo),
                    address.sin6_scope_id,
                )))
            }
            family => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported UDP address family {family}"),
            )),
        }
    }

    #[cfg(target_os = "linux")]
    async fn recv_batch(socket: &UdpSocket, batch: &mut UdpReceiveBatch) -> io::Result<usize> {
        let count = Self::recv_one(socket, batch).await?;
        debug_assert_eq!(count, 1);

        match socket.try_io(Interest::READABLE, || {
            let remaining_capacity = batch.buffers.len() - 1;
            let last_buffer_index = batch.buffers.len() - 1;
            let mut addresses =
                // SAFETY: sockaddr_storage is plain data and zero is a valid empty value.
                [unsafe { std::mem::zeroed::<libc::sockaddr_storage>() };
                    UDP_MAX_RECEIVE_BATCH_SIZE];
            let mut iovecs: [libc::iovec; UDP_MAX_RECEIVE_BATCH_SIZE] =
                std::array::from_fn(|position| {
                    let index = (position + 1).min(last_buffer_index);
                    libc::iovec {
                        iov_base: batch.buffers[index].as_mut_ptr().cast(),
                        iov_len: batch.buffers[index].len(),
                    }
                });
            let mut messages: [libc::mmsghdr; UDP_MAX_RECEIVE_BATCH_SIZE] =
                std::array::from_fn(|position| {
                    // SAFETY: mmsghdr is a C socket descriptor whose fields and padding
                    // accept zero initialization; the live buffer pointers are set below.
                    let mut message = unsafe { std::mem::zeroed::<libc::mmsghdr>() };
                    message.msg_hdr.msg_name = std::ptr::from_mut(&mut addresses[position]).cast();
                    message.msg_hdr.msg_namelen =
                        std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                    message.msg_hdr.msg_iov = std::ptr::from_mut(&mut iovecs[position]);
                    message.msg_hdr.msg_iovlen = 1;
                    message
                });

            // SAFETY: every mmsghdr points to a live address and payload buffer for the
            // duration of this nonblocking syscall, and the array has the supplied length.
            let received = unsafe {
                libc::recvmmsg(
                    socket.as_raw_fd(),
                    messages.as_mut_ptr(),
                    remaining_capacity as libc::c_uint,
                    libc::MSG_DONTWAIT as _,
                    std::ptr::null_mut(),
                )
            };
            if received < 0 {
                return Err(io::Error::last_os_error());
            }

            let received = received as usize;
            for position in 0..received {
                let index = position + 1;
                batch.lengths[index] = messages[position].msg_len as usize;
                batch.peers[index] = Some(Self::socket_addr_from_storage(&addresses[position])?);
            }
            Ok(received)
        }) {
            Ok(additional) => Ok(1 + additional),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(1),
            Err(error) => Err(error),
        }
    }

    #[cfg(not(target_os = "linux"))]
    async fn recv_batch(socket: &UdpSocket, batch: &mut UdpReceiveBatch) -> std::io::Result<usize> {
        let (length, peer) = socket.recv_from(&mut batch.buffers[0]).await?;
        batch.lengths[0] = length;
        batch.peers[0] = Some(peer);
        Ok(1)
    }

    async fn recv_one(socket: &UdpSocket, batch: &mut UdpReceiveBatch) -> io::Result<usize> {
        let (length, peer) = socket.recv_from(&mut batch.buffers[0]).await?;
        batch.lengths[0] = length;
        batch.peers[0] = Some(peer);
        Ok(1)
    }

    async fn send_individually(
        socket: &UdpSocket,
        batch: &UdpReceiveBatch,
        indexes: &[usize],
    ) -> Result<(), UdpBatchSendError> {
        for (sent, index) in indexes.iter().copied().enumerate() {
            if let Err(error) = socket
                .send(&batch.buffers[index][..batch.lengths[index]])
                .await
            {
                return Err(UdpBatchSendError { sent, error });
            }
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn send_batch(
        socket: &UdpSocket,
        batch: &UdpReceiveBatch,
        indexes: &[usize],
    ) -> Result<(), UdpBatchSendError> {
        let mut sent = 0;
        while sent < indexes.len() {
            let result = socket
                .async_io(Interest::WRITABLE, || {
                    let remaining = &indexes[sent..];
                    let mut iovecs: [libc::iovec; UDP_MAX_RECEIVE_BATCH_SIZE] =
                        std::array::from_fn(|position| {
                            let index = remaining.get(position).copied().unwrap_or(0);
                            libc::iovec {
                                iov_base: batch.buffers[index].as_ptr().cast_mut().cast(),
                                iov_len: batch.lengths[index],
                            }
                        });
                    // SAFETY: mmsghdr is a C socket descriptor whose fields and padding accept
                    // zero initialization; live buffer pointers are set before the syscall.
                    let mut messages: [libc::mmsghdr; UDP_MAX_RECEIVE_BATCH_SIZE] =
                        unsafe { std::mem::zeroed() };
                    for (message, iovec) in messages.iter_mut().zip(&mut iovecs) {
                        message.msg_hdr.msg_iov = std::ptr::from_mut(iovec);
                        message.msg_hdr.msg_iovlen = 1;
                    }

                    // SAFETY: the initialized headers point to live payload buffers for this
                    // nonblocking syscall, and only the requested prefix is passed to the kernel.
                    let result = unsafe {
                        libc::sendmmsg(
                            socket.as_raw_fd(),
                            messages.as_mut_ptr(),
                            remaining.len() as libc::c_uint,
                            libc::MSG_DONTWAIT as _,
                        )
                    };
                    if result < 0 {
                        Err(io::Error::last_os_error())
                    } else if result == 0 {
                        Err(io::Error::from(io::ErrorKind::WriteZero))
                    } else {
                        Ok(result as usize)
                    }
                })
                .await;
            match result {
                Ok(count) => sent += count,
                Err(error) => return Err(UdpBatchSendError { sent, error }),
            }
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    async fn send_batch(
        socket: &UdpSocket,
        batch: &UdpReceiveBatch,
        indexes: &[usize],
    ) -> Result<(), UdpBatchSendError> {
        for (sent, index) in indexes.iter().copied().enumerate() {
            if let Err(error) = socket
                .send(&batch.buffers[index][..batch.lengths[index]])
                .await
            {
                return Err(UdpBatchSendError { sent, error });
            }
        }
        Ok(())
    }

    async fn spawn_receive_loop(entry: Arc<ListenerEntry>) {
        let mut shutdown_rx = entry.shutdown.subscribe();
        let task_entry = entry.clone();
        let task = tokio::spawn(async move {
            let mut batch = Box::new(UdpReceiveBatch::new());
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    received = async {
                        if task_entry.udp_batch_enabled {
                            Self::recv_batch(&task_entry.listener_socket, &mut batch).await
                        } else {
                            Self::recv_one(&task_entry.listener_socket, &mut batch).await
                        }
                    } => {
                        match received {
                            Ok(count) => {
                                if task_entry.udp_batch_enabled {
                                    task_entry.global_metrics.udp_batch_calls_total.inc();
                                    task_entry
                                        .global_metrics
                                        .udp_batch_messages_total
                                        .add(count as u64);
                                }
                                let received_bytes = batch.lengths[..count]
                                    .iter()
                                    .map(|length| *length as u64)
                                    .sum::<u64>();
                                task_entry
                                    .global_metrics
                                    .udp_packets_in_total
                                    .add(count as u64);
                                task_entry
                                    .global_metrics
                                    .udp_bytes_in_total
                                    .add(received_bytes);
                                task_entry
                                    .metrics
                                    .rx
                                    .fetch_add(received_bytes, Ordering::Relaxed);
                                let sessions =
                                    Self::prepare_datagram_batch(&task_entry, &batch, count).await;

                                let mut grouped = [false; UDP_MAX_RECEIVE_BATCH_SIZE];
                                for first in 0..count {
                                    let Some(session) = sessions[first].as_ref() else {
                                        continue;
                                    };
                                    if grouped[first] {
                                        continue;
                                    }
                                    let mut indexes = [0; UDP_MAX_RECEIVE_BATCH_SIZE];
                                    let mut index_count = 0;
                                    for index in first..count {
                                        if sessions[index]
                                            .as_ref()
                                            .is_some_and(|candidate| Arc::ptr_eq(candidate, session))
                                        {
                                            grouped[index] = true;
                                            indexes[index_count] = index;
                                            index_count += 1;
                                        }
                                    }

                                    let sent = if task_entry.udp_batch_enabled && index_count > 1 {
                                        Self::send_batch(
                                            &session.upstream,
                                            &batch,
                                            &indexes[..index_count],
                                        )
                                        .await
                                    } else {
                                        Self::send_individually(
                                            &session.upstream,
                                            &batch,
                                            &indexes[..index_count],
                                        )
                                        .await
                                    };
                                    if let Err(error) = sent {
                                        task_entry.global_metrics.udp_send_errors_total.inc();
                                        task_entry
                                            .global_metrics
                                            .udp_drop_total
                                            .add((index_count - error.sent) as u64);
                                        tracing::debug!(
                                            error = %error.error,
                                            dropped = index_count - error.sent,
                                            "udp_batch_send_failed"
                                        );
                                        Self::remove_session(
                                            &task_entry,
                                            session.client_addr,
                                            session,
                                            SessionRemovalReason::Error,
                                        )
                                        .await;
                                    }
                                }
                                batch.grow_if_saturated(count);
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
            // Avoid scanning every active session 20 times per second at the default 60s TTL.
            let interval_duration = Self::session_cleanup_interval(session_ttl);
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
                        Self::compact_empty_sessions_after_idle(&task_entry, session_ttl).await;
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
                                *task_session.last_seen.lock().unwrap() = Instant::now();
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
        let upstream = match Self::bind_upstream_socket(&entry.bind_host).await {
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
            last_seen: StdMutex::new(Instant::now()),
            reply_task: Mutex::new(None),
            _guard: SessionGuard::new(entry.clone()),
        });
        Some(session)
    }

    async fn bind_upstream_socket(bind_host: &str) -> std::io::Result<UdpSocket> {
        UdpSocket::bind((bind_host, 0)).await
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
            *entry.empty_since.lock().await = None;
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
            Self::record_empty_since_if_empty(entry).await;
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
            let last_seen = *session.last_seen.lock().unwrap();
            if now.duration_since(last_seen) > session_ttl {
                Self::remove_session(entry, client_addr, &session, SessionRemovalReason::Expired)
                    .await;
            }
        }
    }

    async fn record_empty_since_if_empty(entry: &Arc<ListenerEntry>) {
        if entry.sessions.lock().await.is_empty() {
            let mut empty_since = entry.empty_since.lock().await;
            empty_since.get_or_insert_with(Instant::now);
        }
    }

    async fn compact_empty_sessions_after_idle(entry: &Arc<ListenerEntry>, idle_grace: Duration) {
        let empty_since = *entry.empty_since.lock().await;
        let Some(empty_since) = empty_since else {
            return;
        };
        if Instant::now().duration_since(empty_since) < idle_grace {
            return;
        }

        {
            let mut sessions = entry.sessions.lock().await;
            if sessions.is_empty() {
                sessions.shrink_to_fit();
            }
        }
        *entry.empty_since.lock().await = None;
    }

    async fn prepare_datagram_batch(
        entry: &Arc<ListenerEntry>,
        batch: &UdpReceiveBatch,
        count: usize,
    ) -> [Option<Arc<UdpSession>>; UDP_MAX_RECEIVE_BATCH_SIZE] {
        let mut sessions = std::array::from_fn(|_| None);
        let state = entry.state.read().await.clone();
        let generation = state.generation;
        let (Some(host), Some(target_port)) = (state.effective_host, state.effective_target_port)
        else {
            entry
                .global_metrics
                .rejected_no_host_total
                .add(count as u64);
            return sessions;
        };

        for index in 0..count {
            let peer = batch.peers[index].expect("received UDP datagram must have a peer");
            sessions[index] = (0..index)
                .find(|previous| batch.peers[*previous] == Some(peer))
                .and_then(|previous| sessions[previous].clone());
            if sessions[index].is_none() {
                sessions[index] =
                    Self::session_for(entry.clone(), peer, &host, target_port, generation).await;
            }
        }

        if entry.generation.load(Ordering::Relaxed) != generation {
            let mut stale = 0;
            for session in sessions[..count].iter().flatten() {
                session.closed.store(true, Ordering::Relaxed);
                stale += 1;
            }
            entry.global_metrics.udp_reply_stale_total.add(stale);
            return std::array::from_fn(|_| None);
        }

        for index in 0..count {
            let Some(session) = sessions[index].as_ref() else {
                continue;
            };
            if session.generation != generation || session.closed.load(Ordering::Relaxed) {
                session.closed.store(true, Ordering::Relaxed);
                entry.global_metrics.udp_reply_stale_total.inc();
                sessions[index] = None;
                continue;
            }
            if !(0..index).any(|previous| {
                sessions[previous]
                    .as_ref()
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, session))
            }) {
                *session.last_seen.lock().unwrap() = Instant::now();
            }
        }
        sessions
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
        *entry.empty_since.lock().await = Some(Instant::now());
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
            bind_host: self.config.bind_host.clone(),
            listener_socket: socket,
            state: RwLock::new(Self::entry_state_for(allocation)),
            metrics: ListenerMetrics::default(),
            shutdown,
            receive_task: Mutex::new(None),
            cleanup_task: Mutex::new(None),
            sessions: Mutex::new(HashMap::new()),
            empty_since: Mutex::new(None),
            generation: AtomicU64::new(0),
            max_sessions: self.config.max_sessions,
            udp_batch_enabled: self.config.udp_batch_enabled,
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
    async fn initialize(&self, _timeout_ms: u32) -> Result<(), RuntimeError> {
        Ok(())
    }

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
    fn udp_runtime_config_accepts_custom_max_sessions() {
        let config =
            UdpRuntimeConfig::loopback(Arc::new(Metrics::default())).with_udp_max_sessions(123);

        assert_eq!(config.max_sessions, 123);
    }

    #[test]
    fn udp_runtime_config_defaults_batching_by_platform_and_accepts_overrides() {
        let default = UdpRuntime::new(UdpRuntimeConfig::loopback(Arc::new(Metrics::default())));
        assert_eq!(default.batch_enabled(), cfg!(target_os = "linux"));

        let disabled = UdpRuntime::new(
            UdpRuntimeConfig::loopback(Arc::new(Metrics::default())).with_udp_batch_enabled(false),
        );
        assert!(!disabled.batch_enabled());

        let enabled = UdpRuntime::new(
            UdpRuntimeConfig::loopback(Arc::new(Metrics::default())).with_udp_batch_enabled(true),
        );
        assert!(enabled.batch_enabled());
    }

    #[test]
    fn udp_runtime_config_defaults_to_65536_max_sessions() {
        let config = UdpRuntimeConfig::loopback(Arc::new(Metrics::default()));

        assert_eq!(config.max_sessions, 65_536);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn udp_linux_receive_batch_grows_only_after_saturation() {
        let mut batch = UdpReceiveBatch::new();
        assert_eq!(batch.buffers.len(), 16);

        batch.grow_if_saturated(15);
        assert_eq!(batch.buffers.len(), 16);
        batch.grow_if_saturated(16);
        assert_eq!(batch.buffers.len(), 32);
        batch.grow_if_saturated(32);
        assert_eq!(batch.buffers.len(), 64);
        batch.grow_if_saturated(64);
        assert_eq!(batch.buffers.len(), 64);
    }

    #[test]
    fn udp_session_cleanup_interval_tracks_short_ttls_and_caps_default_sweeps() {
        assert_eq!(
            UdpRuntime::session_cleanup_interval(Duration::from_millis(25)),
            Duration::from_millis(25)
        );
        assert_eq!(
            UdpRuntime::session_cleanup_interval(Duration::from_secs(60)),
            Duration::from_secs(1)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
    async fn udp_last_seen_lock_benchmark() {
        const ITERATIONS: usize = 5_000_000;

        fn measure_std(lock: &StdMutex<Instant>) -> Duration {
            let started = Instant::now();
            for _ in 0..ITERATIONS {
                *lock.lock().unwrap() = Instant::now();
            }
            std::hint::black_box(*lock.lock().unwrap());
            started.elapsed()
        }

        async fn measure_tokio(lock: &Mutex<Instant>) -> Duration {
            let started = Instant::now();
            for _ in 0..ITERATIONS {
                *lock.lock().await = Instant::now();
            }
            std::hint::black_box(*lock.lock().await);
            started.elapsed()
        }

        let std_lock = StdMutex::new(Instant::now());
        let tokio_lock = Mutex::new(Instant::now());
        std::hint::black_box(measure_std(&std_lock));
        std::hint::black_box(measure_tokio(&tokio_lock).await);

        let previous = measure_tokio(&tokio_lock).await;
        let synchronous = measure_std(&std_lock);
        eprintln!(
            "udp last_seen lock: tokio={:.3}ms std={:.3}ms speedup={:.2}x",
            previous.as_secs_f64() * 1_000.0,
            synchronous.as_secs_f64() * 1_000.0,
            previous.as_secs_f64() / synchronous.as_secs_f64(),
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
    fn udp_send_message_header_initialization_benchmark() {
        const ITERATIONS: usize = 500_000;

        fn measure(from_fn: bool) -> Duration {
            // SAFETY: iovec accepts zero initialization and is only used as a pointer target.
            let mut iovecs: [libc::iovec; UDP_MAX_RECEIVE_BATCH_SIZE] =
                unsafe { std::mem::zeroed() };
            let started = Instant::now();
            for _ in 0..ITERATIONS {
                let messages: [libc::mmsghdr; UDP_MAX_RECEIVE_BATCH_SIZE] = if from_fn {
                    std::array::from_fn(|position| {
                        // SAFETY: mmsghdr accepts zero initialization; the live iovec pointer is
                        // set exactly as it was in the previous send path.
                        let mut message = unsafe { std::mem::zeroed::<libc::mmsghdr>() };
                        message.msg_hdr.msg_iov = std::ptr::from_mut(&mut iovecs[position]);
                        message.msg_hdr.msg_iovlen = 1;
                        message
                    })
                } else {
                    // SAFETY: mmsghdr accepts zero initialization; live iovec pointers are set
                    // before the headers could be passed to a syscall.
                    let mut messages: [libc::mmsghdr; UDP_MAX_RECEIVE_BATCH_SIZE] =
                        unsafe { std::mem::zeroed() };
                    for (message, iovec) in messages.iter_mut().zip(&mut iovecs) {
                        message.msg_hdr.msg_iov = std::ptr::from_mut(iovec);
                        message.msg_hdr.msg_iovlen = 1;
                    }
                    messages
                };
                std::hint::black_box(messages);
            }
            started.elapsed()
        }

        std::hint::black_box(measure(true));
        std::hint::black_box(measure(false));
        let previous = measure(true);
        let direct = measure(false);
        eprintln!(
            "udp send message headers: from_fn={:.3}ms direct={:.3}ms speedup={:.2}x",
            previous.as_secs_f64() * 1_000.0,
            direct.as_secs_f64() * 1_000.0,
            previous.as_secs_f64() / direct.as_secs_f64(),
        );
    }

    #[tokio::test]
    async fn udp_upstream_socket_uses_configured_bind_host() {
        let loopback = UdpRuntime::bind_upstream_socket("127.0.0.2").await.unwrap();
        assert_eq!(
            loopback.local_addr().unwrap().ip(),
            std::net::Ipv4Addr::new(127, 0, 0, 2)
        );

        let unspecified = UdpRuntime::bind_upstream_socket("0.0.0.0").await.unwrap();
        assert_eq!(
            unspecified.local_addr().unwrap().ip(),
            std::net::Ipv4Addr::UNSPECIFIED
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn udp_batch_receive_decodes_ipv6_peers() {
        let receiver = UdpSocket::bind(("::1", 0)).await.unwrap();
        let receiver_port = receiver.local_addr().unwrap().port();
        let sender = std::net::UdpSocket::bind(("::1", 0)).unwrap();
        let sender_addr = sender.local_addr().unwrap();
        sender.connect(("::1", receiver_port)).unwrap();
        sender.send(b"first").unwrap();
        sender.send(b"second").unwrap();

        let mut batch = UdpReceiveBatch::new();
        let count = UdpRuntime::recv_batch(&receiver, &mut batch).await.unwrap();

        assert_eq!(count, 2);
        assert_eq!(batch.peers[0], Some(sender_addr));
        assert_eq!(batch.peers[1], Some(sender_addr));
        assert_eq!(&batch.buffers[0][..batch.lengths[0]], b"first");
        assert_eq!(&batch.buffers[1][..batch.lengths[1]], b"second");
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
    async fn udp_runtime_upstream_sessions_use_configured_bind_host() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(UdpRuntimeConfig::with_bind_host(
            "127.0.0.2",
            metrics.clone(),
        ));
        let relay_port = free_udp_port().await;
        let capture = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let target_port = capture.local_addr().unwrap().port();

        runtime
            .create(
                &allocation("alloc-source-bind", relay_port, None, None),
                500,
            )
            .await
            .unwrap();
        runtime
            .update(
                &allocation(
                    "alloc-source-bind",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        client
            .send_to(b"source-bind", ("127.0.0.2", relay_port))
            .await
            .unwrap();

        let mut buf = [0_u8; 64];
        let (n, peer) = timeout(Duration::from_millis(500), capture.recv_from(&mut buf))
            .await
            .expect("capture target did not receive forwarded datagram")
            .unwrap();

        assert_eq!(&buf[..n], b"source-bind");
        assert_eq!(peer.ip(), std::net::Ipv4Addr::new(127, 0, 0, 2));
        assert_eq!(metrics.udp_session_create_total.load(), 1);

        runtime.delete("alloc-source-bind", 500).await.unwrap();
    }

    #[tokio::test]
    async fn udp_runtime_forwards_datagrams_after_binding_and_records_metrics() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(
            UdpRuntimeConfig::loopback(metrics.clone()).with_udp_batch_enabled(false),
        );
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
        assert_eq!(metrics.udp_batch_calls_total.load(), 0);
        assert_eq!(metrics.udp_batch_messages_total.load(), 0);

        runtime.delete("alloc-active", 500).await.unwrap();
        target_task.abort();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn udp_runtime_batches_bursts_without_changing_order_or_metrics() {
        const PACKETS: usize = UDP_MAX_RECEIVE_BATCH_SIZE;

        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(
            UdpRuntimeConfig::loopback(metrics.clone()).with_udp_batch_enabled(true),
        );
        let relay_port = free_udp_port().await;
        let (target_port, records, target_task) = start_udp_capture_server(PACKETS).await;
        runtime
            .create(
                &allocation(
                    "alloc-batch",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        // Queue the complete burst without yielding so the receive loop can drain it together.
        let client = std::net::UdpSocket::bind(("127.0.0.1", 0)).unwrap();
        client.connect(("127.0.0.1", relay_port)).unwrap();
        for sequence in 0..PACKETS as u64 {
            client.send(&sequence.to_ne_bytes()).unwrap();
        }
        client.set_nonblocking(true).unwrap();
        let client = UdpSocket::from_std(client).unwrap();

        let mut reply = [0_u8; 8];
        for sequence in 0..PACKETS as u64 {
            timeout(Duration::from_secs(1), client.recv(&mut reply))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(u64::from_ne_bytes(reply), sequence);
        }

        target_task.await.unwrap();
        let records = records.lock().await;
        assert_eq!(records.len(), PACKETS);
        assert!(records.windows(2).all(|rows| rows[0].0 == rows[1].0));
        drop(records);
        wait_for_udp_metrics(&runtime, &metrics, PACKETS as u64, PACKETS as u64).await;
        assert_eq!(metrics.udp_batch_messages_total.load(), PACKETS as u64);
        assert!(metrics.udp_batch_calls_total.load() < PACKETS as u64);
        assert_eq!(metrics.udp_session_create_total.load(), 1);

        runtime.delete("alloc-batch", 500).await.unwrap();
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

    async fn wait_for_session_table_len(runtime: &UdpRuntime, id: &str, expected: usize) -> usize {
        timeout(Duration::from_secs(2), async {
            loop {
                if let Some((len, capacity)) = runtime.session_table_stats(id).await
                    && len == expected
                {
                    return capacity;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("udp session table length did not settle")
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
    async fn udp_runtime_delays_empty_session_table_compaction_until_idle_grace() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(
            UdpRuntimeConfig::loopback(metrics.clone()).with_session_ttl(Duration::from_millis(40)),
        );
        let relay_port = free_udp_port().await;
        let (target_port, target_task) = start_udp_echo_server().await;
        runtime
            .create(
                &allocation(
                    "alloc-compact",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        for index in 0..16 {
            let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
            let payload = format!("compact-{index}");
            send_udp_and_expect(&client, relay_port, payload.as_bytes()).await;
        }
        wait_for_active_sessions(&runtime, &metrics, 16).await;
        let expanded_capacity = runtime
            .session_table_stats("alloc-compact")
            .await
            .unwrap()
            .1;
        assert!(expanded_capacity >= 16);

        let empty_capacity = wait_for_session_table_len(&runtime, "alloc-compact", 0).await;
        assert_eq!(empty_capacity, expanded_capacity);

        timeout(Duration::from_secs(2), async {
            loop {
                let (len, capacity) = runtime.session_table_stats("alloc-compact").await.unwrap();
                if len == 0 && capacity == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("udp session table was not compacted after idle grace");

        runtime.delete("alloc-compact", 500).await.unwrap();
        target_task.abort();
    }

    #[tokio::test]
    async fn udp_runtime_update_to_new_target_closes_stale_session_and_uses_new_target() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(
            UdpRuntimeConfig::loopback(metrics.clone()).with_udp_batch_enabled(true),
        );
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
        let runtime = UdpRuntime::new(
            UdpRuntimeConfig::loopback(metrics.clone()).with_udp_batch_enabled(true),
        );
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
        let runtime = UdpRuntime::new(
            UdpRuntimeConfig::loopback(metrics.clone()).with_udp_batch_enabled(true),
        );
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
        let runtime = UdpRuntime::new(
            UdpRuntimeConfig::loopback(metrics.clone()).with_udp_batch_enabled(true),
        );
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
