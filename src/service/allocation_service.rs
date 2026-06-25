use crate::config::{PortRange, parse_ip_literal};
use crate::model::{
    self, Allocation, AllocationResource, AllocationView, Binding, BindingView, ErrorKind,
    Protocol, RuntimeStatus,
};
use crate::runtime::facade::{ListenerMetricsSnapshot, ObservedState, RuntimeError, RuntimeFacade};
use crate::storage::sqlite::{Repository, RepositoryError};
use crate::uuid::generate_uuid_v7;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::info;

pub type IdGenerator = Arc<dyn Fn() -> String + Send + Sync>;

#[derive(Debug, Default, Clone)]
pub struct Failpoints {
    pub create_timeout: bool,
    pub update_timeout: bool,
    pub delete_timeout: bool,
    pub delete_db_failure: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("not found")]
    NotFound,
    #[error("no available port")]
    NoAvailablePort,
    #[error("invalid host")]
    InvalidHost,
    #[error("timeout")]
    Timeout,
    #[error("delete persistence failed")]
    DeletePersistenceFailed,
}

pub struct Service<R: RuntimeFacade> {
    repo: Repository,
    runtime: R,
    port_range: PortRange,
    apply_timeout_ms: u32,
    id_generator: IdGenerator,
    failpoints: Mutex<Failpoints>,
    mutation_lock: Mutex<()>,
}

impl<R: RuntimeFacade> Service<R> {
    pub fn new(repo: Repository, runtime: R, port_range: PortRange, apply_timeout_ms: u32) -> Self {
        Self::with_id_generator(
            repo,
            runtime,
            port_range,
            apply_timeout_ms,
            Arc::new(generate_uuid_v7),
        )
    }

    pub fn with_id_generator(
        repo: Repository,
        runtime: R,
        port_range: PortRange,
        apply_timeout_ms: u32,
        id_generator: IdGenerator,
    ) -> Self {
        Self {
            repo,
            runtime,
            port_range,
            apply_timeout_ms,
            id_generator,
            failpoints: Mutex::new(Failpoints::default()),
            mutation_lock: Mutex::new(()),
        }
    }

    pub async fn set_failpoints(&self, failpoints: Failpoints) {
        *self.failpoints.lock().await = failpoints;
    }

    pub fn repo(&self) -> &Repository {
        &self.repo
    }

    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    pub async fn create_allocation(
        &self,
        protocol: Protocol,
        target_port: Option<u16>,
    ) -> Result<Allocation, ServiceError> {
        let _mutation_guard = self.mutation_lock.lock().await;

        if self.failpoints.lock().await.create_timeout {
            return Err(ServiceError::Timeout);
        }

        for port in self.port_range.start..=self.port_range.end {
            let existing_allocations = self.repo.list_allocations().await?;
            if existing_allocations.iter().any(|allocation| {
                allocation.port == port && conflicts(protocol, allocation.protocol)
            }) {
                continue;
            }

            let now = current_time_ms();
            let allocation = Allocation {
                id: (self.id_generator)(),
                protocol,
                port,
                target_port,
                host: None,
                created_at_ms: now,
                updated_at_ms: now,
            };

            match self
                .runtime
                .create(&allocation, self.apply_timeout_ms)
                .await
            {
                Ok(()) => {}
                Err(RuntimeError::RuntimeCreateFailed) => continue,
                Err(error) => return Err(ServiceError::Runtime(error)),
            }

            if let Err(error) = self.persist_created_allocation(&allocation).await {
                let _ = self
                    .runtime
                    .delete(&allocation.id, self.apply_timeout_ms)
                    .await;
                return Err(error);
            }

            info!(
                allocation_id = %allocation.id,
                protocol = %allocation.protocol,
                relay_port = allocation.port,
                target_port = ?allocation.target_port,
                host = ?allocation.host,
                "relay_allocation_created"
            );

            return Ok(allocation);
        }

        Err(ServiceError::NoAvailablePort)
    }

    async fn persist_created_allocation(
        &self,
        allocation: &Allocation,
    ) -> Result<(), ServiceError> {
        if let Err(error) = self.repo.insert_allocation(allocation).await {
            return Err(ServiceError::Repository(error));
        }

        if let Some(target_port) = allocation.target_port {
            let binding = Binding {
                allocation_id: allocation.id.clone(),
                target_port,
                host: allocation.host.clone(),
                created_at_ms: allocation.created_at_ms,
                updated_at_ms: allocation.updated_at_ms,
            };

            if let Err(error) = self.repo.put_binding(&binding).await {
                let _ = self
                    .repo
                    .delete_binding(&allocation.id, allocation.updated_at_ms)
                    .await;
                let _ = self.repo.delete_allocation(&allocation.id).await;
                return Err(ServiceError::Repository(error));
            }
        }

        Ok(())
    }

    pub async fn get_allocation(&self, id: &str) -> Result<Option<Allocation>, ServiceError> {
        Ok(self.repo.get_allocation(id).await?)
    }

    pub async fn list_allocation_resources(&self) -> Result<Vec<AllocationResource>, ServiceError> {
        Ok(self
            .repo
            .list_allocations()
            .await?
            .iter()
            .map(clone_allocation_resource)
            .collect())
    }

    pub async fn get_allocation_resource(
        &self,
        id: &str,
    ) -> Result<Option<AllocationResource>, ServiceError> {
        Ok(self
            .repo
            .get_allocation(id)
            .await?
            .as_ref()
            .map(clone_allocation_resource))
    }

    pub async fn list_allocations(&self) -> Result<Vec<AllocationView>, ServiceError> {
        let allocations = self.repo.list_allocations().await?;
        let mut views = Vec::with_capacity(allocations.len());
        for allocation in allocations {
            let observed = self.runtime.snapshot(&allocation.id).await?;
            views.push(allocation_view(allocation, observed));
        }
        Ok(views)
    }

    pub async fn get_allocation_view(
        &self,
        id: &str,
    ) -> Result<Option<AllocationView>, ServiceError> {
        let Some(allocation) = self.repo.get_allocation(id).await? else {
            return Ok(None);
        };
        let observed = self.runtime.snapshot(id).await?;
        Ok(Some(allocation_view(allocation, observed)))
    }

    pub async fn get_binding_view(&self, id: &str) -> Result<Option<BindingView>, ServiceError> {
        let Some(binding) = self.repo.get_binding(id).await? else {
            return Ok(None);
        };
        let observed = self.runtime.snapshot(id).await?;
        Ok(Some(binding_view(binding, observed)))
    }

    pub async fn put_binding(
        &self,
        id: &str,
        host: &str,
        target_port: u16,
    ) -> Result<Binding, ServiceError> {
        parse_ip_literal(host).map_err(|_| ServiceError::InvalidHost)?;
        let _mutation_guard = self.mutation_lock.lock().await;

        let Some(allocation) = self.repo.get_allocation(id).await? else {
            return Err(ServiceError::NotFound);
        };
        let existing_binding = self.repo.get_binding(id).await?;
        let now = current_time_ms();
        let binding = Binding {
            allocation_id: id.to_owned(),
            target_port,
            host: Some(host.to_owned()),
            created_at_ms: existing_binding
                .as_ref()
                .map(|binding| binding.created_at_ms)
                .unwrap_or(now),
            updated_at_ms: now,
        };

        self.repo.put_binding(&binding).await?;
        let updated_allocation = allocation_with_binding(allocation, &binding);
        if self.failpoints.lock().await.update_timeout {
            return Err(ServiceError::Timeout);
        }
        self.runtime
            .update(&updated_allocation, self.apply_timeout_ms)
            .await?;

        info!(
            allocation_id = %binding.allocation_id,
            protocol = %updated_allocation.protocol,
            relay_port = updated_allocation.port,
            target_port = binding.target_port,
            host = ?binding.host,
            previous_target_port = ?existing_binding.as_ref().map(|binding| binding.target_port),
            previous_host = ?existing_binding.as_ref().and_then(|binding| binding.host.as_deref()),
            "relay_binding_assigned"
        );

        Ok(binding)
    }

    pub async fn delete_binding(&self, id: &str) -> Result<(), ServiceError> {
        let _mutation_guard = self.mutation_lock.lock().await;

        let Some(allocation) = self.repo.get_allocation(id).await? else {
            return Err(ServiceError::NotFound);
        };
        let Some(existing_binding) = self.repo.get_binding(id).await? else {
            return Err(ServiceError::NotFound);
        };

        let now = current_time_ms();
        if !self.repo.delete_binding(id, now).await? {
            return Err(ServiceError::NotFound);
        }

        let updated_allocation = Allocation {
            target_port: None,
            host: None,
            updated_at_ms: now,
            ..allocation
        };
        if self.failpoints.lock().await.update_timeout {
            return Err(ServiceError::Timeout);
        }
        self.runtime
            .update(&updated_allocation, self.apply_timeout_ms)
            .await?;

        info!(
            allocation_id = %id,
            protocol = %updated_allocation.protocol,
            relay_port = updated_allocation.port,
            previous_target_port = existing_binding.target_port,
            previous_host = ?existing_binding.host,
            "relay_binding_deleted"
        );

        Ok(())
    }

    pub async fn set_target(&self, id: &str, host: &str) -> Result<Allocation, ServiceError> {
        self.update_allocation(id, None, Some(host)).await
    }

    pub async fn update_allocation(
        &self,
        id: &str,
        target_port: Option<u16>,
        host: Option<&str>,
    ) -> Result<Allocation, ServiceError> {
        let _mutation_guard = self.mutation_lock.lock().await;

        let Some(allocation) = self.repo.get_allocation(id).await? else {
            return Err(ServiceError::NotFound);
        };
        if let Some(host) = host {
            parse_ip_literal(host).map_err(|_| ServiceError::InvalidHost)?;
        }
        let existing_binding = self.repo.get_binding(id).await?;
        let resolved_target_port = target_port
            .or_else(|| existing_binding.as_ref().map(|binding| binding.target_port))
            .ok_or(ServiceError::NotFound)?;
        let resolved_host = host
            .map(str::to_owned)
            .or_else(|| {
                existing_binding
                    .as_ref()
                    .and_then(|binding| binding.host.clone())
            })
            .or_else(|| allocation.host.clone());
        let now = current_time_ms();
        let binding = Binding {
            allocation_id: id.to_owned(),
            target_port: resolved_target_port,
            host: resolved_host,
            created_at_ms: existing_binding
                .as_ref()
                .map(|binding| binding.created_at_ms)
                .unwrap_or(now),
            updated_at_ms: now,
        };

        self.repo.put_binding(&binding).await?;
        let updated_allocation = allocation_with_binding(allocation, &binding);
        if self.failpoints.lock().await.update_timeout {
            return Err(ServiceError::Timeout);
        }
        self.runtime
            .update(&updated_allocation, self.apply_timeout_ms)
            .await?;

        info!(
            allocation_id = %binding.allocation_id,
            protocol = %updated_allocation.protocol,
            relay_port = updated_allocation.port,
            target_port = binding.target_port,
            host = ?binding.host,
            previous_target_port = ?existing_binding.as_ref().map(|binding| binding.target_port),
            previous_host = ?existing_binding.as_ref().and_then(|binding| binding.host.as_deref()),
            "relay_binding_assigned"
        );

        Ok(updated_allocation)
    }

    pub async fn delete_allocation(&self, id: &str) -> Result<(), ServiceError> {
        let _mutation_guard = self.mutation_lock.lock().await;

        let Some(allocation) = self.repo.get_allocation(id).await? else {
            return Err(ServiceError::NotFound);
        };

        if self.failpoints.lock().await.delete_timeout {
            return Err(ServiceError::Timeout);
        }

        self.runtime.delete(id, self.apply_timeout_ms).await?;

        if self.failpoints.lock().await.delete_db_failure {
            self.runtime
                .restore(&allocation, self.apply_timeout_ms)
                .await?;
            return Err(ServiceError::DeletePersistenceFailed);
        }

        if !self.repo.delete_allocation(id).await? {
            return Err(ServiceError::NotFound);
        }

        info!(
            allocation_id = %allocation.id,
            protocol = %allocation.protocol,
            relay_port = allocation.port,
            target_port = ?allocation.target_port,
            host = ?allocation.host,
            "relay_allocation_deleted"
        );

        Ok(())
    }

    pub async fn restore_all(&self, timeout_ms: u32) -> Result<(), ServiceError> {
        let _mutation_guard = self.mutation_lock.lock().await;

        self.runtime.initialize(timeout_ms).await?;
        for allocation in self.repo.list_allocations().await? {
            self.runtime.restore(&allocation, timeout_ms).await?;
        }

        Ok(())
    }

    pub async fn snapshot_listener_metrics(
        &self,
    ) -> Result<Vec<ListenerMetricsSnapshot>, ServiceError> {
        Ok(self.runtime.snapshot_listener_metrics().await?)
    }
}

fn clone_allocation_resource(allocation: &Allocation) -> AllocationResource {
    AllocationResource {
        id: allocation.id.clone(),
        protocol: allocation.protocol,
        port: allocation.port,
        created_at_ms: allocation.created_at_ms,
        updated_at_ms: allocation.updated_at_ms,
    }
}

fn allocation_with_binding(mut allocation: Allocation, binding: &Binding) -> Allocation {
    allocation.target_port = Some(binding.target_port);
    allocation.host = binding.host.clone();
    allocation.updated_at_ms = binding.updated_at_ms;
    allocation
}

fn missing_runtime_state() -> ObservedState {
    ObservedState {
        effective_target_port: None,
        effective_host: None,
        runtime_status: RuntimeStatus::DegradedBindFailed,
        error_kind: Some(ErrorKind::BindFailed),
        last_error: Some("missing runtime state".to_owned()),
    }
}

fn binding_view(binding: Binding, observed: Option<ObservedState>) -> BindingView {
    let observed = observed.unwrap_or_else(missing_runtime_state);
    BindingView {
        allocation_id: binding.allocation_id,
        host: binding.host,
        target_port: binding.target_port,
        effective_target_port: observed.effective_target_port,
        effective_host: observed.effective_host,
        runtime_status: observed.runtime_status,
        error_kind: observed.error_kind,
        last_error: observed.last_error,
        created_at_ms: binding.created_at_ms,
        updated_at_ms: binding.updated_at_ms,
    }
}

fn allocation_view(allocation: Allocation, observed: Option<ObservedState>) -> AllocationView {
    let observed = observed.unwrap_or_else(missing_runtime_state);
    AllocationView {
        id: allocation.id,
        protocol: allocation.protocol,
        port: allocation.port,
        target_port: allocation.target_port,
        host_configured: model::is_host_configured(allocation.host.as_deref()),
        host: allocation.host,
        effective_target_port: observed.effective_target_port,
        effective_host: observed.effective_host,
        runtime_status: observed.runtime_status,
        error_kind: observed.error_kind,
        last_error: observed.last_error,
        created_at_ms: allocation.created_at_ms,
        updated_at_ms: allocation.updated_at_ms,
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn conflicts(requested: Protocol, existing: Protocol) -> bool {
    match requested {
        Protocol::Tcp => existing == Protocol::Tcp || existing == Protocol::Both,
        Protocol::Udp => existing == Protocol::Udp || existing == Protocol::Both,
        Protocol::Both => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::facade::InMemoryRuntime;
    use sqlx::Executor;
    use std::io;
    use std::path::PathBuf;
    use std::sync::{Mutex as StdMutex, Once, OnceLock};
    use tracing_subscriber::fmt::MakeWriter;

    async fn temp_repo() -> Repository {
        let path = temp_db_path();
        Repository::open(&path).await.unwrap()
    }

    async fn temp_repo_with_path() -> (Repository, PathBuf) {
        let path = temp_db_path();
        let repo = Repository::open(&path).await.unwrap();
        (repo, path)
    }

    fn temp_db_path() -> PathBuf {
        let parent = std::env::current_dir()
            .unwrap()
            .join("target/relayd-test-dbs");
        std::fs::create_dir_all(&parent).unwrap();
        let dir = tempfile::tempdir_in(parent).unwrap().keep();
        dir.join("relayd.sqlite")
    }

    fn service(repo: Repository, runtime: InMemoryRuntime) -> Service<InMemoryRuntime> {
        service_with_range(repo, runtime, 10000, 10010)
    }

    fn service_with_range(
        repo: Repository,
        runtime: InMemoryRuntime,
        start: u16,
        end: u16,
    ) -> Service<InMemoryRuntime> {
        let next_id = Arc::new(StdMutex::new(0_u64));
        Service::with_id_generator(
            repo,
            runtime,
            PortRange { start, end },
            500,
            Arc::new(move || {
                let mut next_id = next_id.lock().unwrap();
                *next_id += 1;
                format!("alloc-{next_id}")
            }),
        )
    }

    fn allocation(
        id: &str,
        protocol: Protocol,
        port: u16,
        target_port: Option<u16>,
        host: Option<&str>,
    ) -> Allocation {
        Allocation {
            id: id.to_owned(),
            protocol,
            port,
            target_port,
            host: host.map(str::to_owned),
            created_at_ms: 1000,
            updated_at_ms: 1100,
        }
    }

    fn binding(
        allocation_id: &str,
        target_port: u16,
        host: Option<&str>,
        created_at_ms: i64,
        updated_at_ms: i64,
    ) -> Binding {
        Binding {
            allocation_id: allocation_id.to_owned(),
            target_port,
            host: host.map(str::to_owned),
            created_at_ms,
            updated_at_ms,
        }
    }

    #[derive(Clone, Default)]
    struct SharedLogWriter {
        buffer: Arc<StdMutex<Vec<u8>>>,
    }

    impl SharedLogWriter {
        fn clear(&self) {
            self.buffer.lock().unwrap().clear();
        }

        fn contents(&self) -> String {
            String::from_utf8(self.buffer.lock().unwrap().clone()).unwrap()
        }
    }

    impl<'a> MakeWriter<'a> for SharedLogWriter {
        type Writer = SharedLogSink;

        fn make_writer(&'a self) -> Self::Writer {
            SharedLogSink {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    struct SharedLogSink {
        buffer: Arc<StdMutex<Vec<u8>>>,
    }

    impl io::Write for SharedLogSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn install_log_capture_subscriber(writer: SharedLogWriter) {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(writer)
                .with_ansi(false)
                .with_target(false)
                .with_level(false)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("test tracing subscriber should install once");
        });
    }

    async fn capture_service_logs<F, Fut>(operation: F) -> String
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        static WRITER: OnceLock<SharedLogWriter> = OnceLock::new();
        static CAPTURE_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

        let writer = WRITER.get_or_init(SharedLogWriter::default).clone();
        install_log_capture_subscriber(writer.clone());
        let _guard = CAPTURE_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;
        writer.clear();
        operation().await;
        writer.contents()
    }

    #[tokio::test]
    async fn create_allocation_emits_lifecycle_log() {
        let svc = service(temp_repo().await, InMemoryRuntime::default());

        let logs = capture_service_logs(|| async {
            let allocation = svc
                .create_allocation(Protocol::Tcp, Some(8080))
                .await
                .unwrap();
            assert_eq!(allocation.id, "alloc-1");
        })
        .await;

        assert!(logs.contains("relay_allocation_created"));
        assert!(logs.contains("allocation_id=alloc-1"));
        assert!(logs.contains("protocol=tcp"));
        assert!(logs.contains("relay_port=10000"));
        assert!(logs.contains("target_port=Some(8080)"));
    }

    #[tokio::test]
    async fn binding_assignment_and_deletion_emit_lifecycle_logs() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("alloc-1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let svc = service(repo, InMemoryRuntime::default());

        let logs = capture_service_logs(|| async {
            svc.put_binding("alloc-1", "127.0.0.1", 8080).await.unwrap();
            svc.put_binding("alloc-1", "127.0.0.2", 9090).await.unwrap();
            svc.delete_binding("alloc-1").await.unwrap();
        })
        .await;

        assert!(logs.matches("relay_binding_assigned").count() >= 2);
        assert!(logs.contains("allocation_id=alloc-1"));
        assert!(logs.contains("relay_port=10000"));
        assert!(logs.contains("target_port=8080"));
        assert!(logs.contains("host=Some(\"127.0.0.1\")"));
        assert!(logs.contains("previous_target_port=Some(8080)"));
        assert!(logs.contains("previous_host=Some(\"127.0.0.1\")"));
        assert!(logs.contains("relay_binding_deleted"));
        assert!(logs.contains("previous_target_port=9090"));
        assert!(logs.contains("previous_host=Some(\"127.0.0.2\")"));
    }

    #[tokio::test]
    async fn update_allocation_and_delete_allocation_emit_lifecycle_logs() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation(
            "alloc-1",
            Protocol::Udp,
            10000,
            Some(5353),
            None,
        ))
        .await
        .unwrap();
        repo.put_binding(&binding("alloc-1", 5353, Some("127.0.0.1"), 1000, 1100))
            .await
            .unwrap();
        let svc = service(repo, InMemoryRuntime::default());

        let logs = capture_service_logs(|| async {
            svc.update_allocation("alloc-1", Some(5354), Some("127.0.0.2"))
                .await
                .unwrap();
            svc.delete_allocation("alloc-1").await.unwrap();
        })
        .await;

        assert!(logs.contains("relay_binding_assigned"));
        assert!(logs.contains("protocol=udp"));
        assert!(logs.contains("target_port=5354"));
        assert!(logs.contains("host=Some(\"127.0.0.2\")"));
        assert!(logs.contains("previous_target_port=Some(5353)"));
        assert!(logs.contains("relay_allocation_deleted"));
        assert!(logs.contains("target_port=Some(5354)"));
        assert!(logs.contains("host=Some(\"127.0.0.2\")"));
    }

    #[tokio::test]
    async fn empty_lists_are_empty() {
        let svc = service(temp_repo().await, InMemoryRuntime::default());

        assert!(svc.list_allocation_resources().await.unwrap().is_empty());
        assert!(svc.list_allocations().await.unwrap().is_empty());
        assert!(svc.snapshot_listener_metrics().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn inserted_allocation_can_be_read_as_resource() {
        let repo = temp_repo().await;
        let inserted = allocation("alloc-1", Protocol::Tcp, 10000, Some(8080), None);
        repo.insert_allocation(&inserted).await.unwrap();
        let svc = service(repo, InMemoryRuntime::default());

        let resources = svc.list_allocation_resources().await.unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].id, "alloc-1");
        assert_eq!(resources[0].protocol, Protocol::Tcp);
        assert_eq!(resources[0].port, 10000);
        assert_eq!(resources[0].created_at_ms, 1000);
        assert_eq!(resources[0].updated_at_ms, 1100);
        assert_eq!(
            svc.get_allocation_resource("alloc-1").await.unwrap(),
            Some(resources[0].clone())
        );
    }

    #[tokio::test]
    async fn missing_runtime_snapshot_degrades_aggregate_view() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation(
            "alloc-1",
            Protocol::Udp,
            10001,
            Some(5353),
            Some("127.0.0.1"),
        ))
        .await
        .unwrap();
        let svc = service(repo, InMemoryRuntime::default());

        let views = svc.list_allocations().await.unwrap();
        assert_eq!(views.len(), 1);
        let view = &views[0];
        assert_eq!(view.id, "alloc-1");
        assert_eq!(view.protocol, Protocol::Udp);
        assert_eq!(view.port, 10001);
        assert_eq!(view.target_port, Some(5353));
        assert_eq!(view.host.as_deref(), Some("127.0.0.1"));
        assert!(view.host_configured);
        assert_eq!(view.effective_target_port, None);
        assert_eq!(view.effective_host, None);
        assert_eq!(view.runtime_status, RuntimeStatus::DegradedBindFailed);
        assert_eq!(view.error_kind, Some(ErrorKind::BindFailed));
        assert_eq!(view.last_error.as_deref(), Some("missing runtime state"));
    }

    #[tokio::test]
    async fn create_allocation_create_timeout_failpoint_returns_timeout_without_runtime_create() {
        let runtime = InMemoryRuntime::default();
        let svc = service_with_range(temp_repo().await, runtime.clone(), 10000, 10000);
        svc.set_failpoints(Failpoints {
            create_timeout: true,
            ..Failpoints::default()
        })
        .await;

        assert!(matches!(
            svc.create_allocation(Protocol::Tcp, None).await,
            Err(ServiceError::Timeout)
        ));
        assert!(runtime.calls().create.is_empty());
        assert!(svc.repo().list_allocations().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_allocation_allows_tcp_and_udp_to_share_a_port() {
        let repo = temp_repo().await;
        let runtime = InMemoryRuntime::default();
        let svc = service_with_range(repo, runtime, 10000, 10000);

        let tcp = svc.create_allocation(Protocol::Tcp, None).await.unwrap();
        let udp = svc.create_allocation(Protocol::Udp, None).await.unwrap();

        assert_eq!(tcp.port, 10000);
        assert_eq!(udp.port, 10000);
        assert_ne!(tcp.id, udp.id);
        assert_eq!(svc.repo().list_allocations().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn create_allocation_existing_both_blocks_tcp_and_udp() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("both", Protocol::Both, 10000, None, None))
            .await
            .unwrap();
        let svc = service_with_range(repo, InMemoryRuntime::default(), 10000, 10000);

        assert!(matches!(
            svc.create_allocation(Protocol::Tcp, None).await,
            Err(ServiceError::NoAvailablePort)
        ));
        assert!(matches!(
            svc.create_allocation(Protocol::Udp, None).await,
            Err(ServiceError::NoAvailablePort)
        ));
    }

    #[tokio::test]
    async fn create_allocation_existing_tcp_or_udp_blocks_both() {
        let tcp_repo = temp_repo().await;
        tcp_repo
            .insert_allocation(&allocation("tcp", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let tcp_svc = service_with_range(tcp_repo, InMemoryRuntime::default(), 10000, 10000);
        assert!(matches!(
            tcp_svc.create_allocation(Protocol::Both, None).await,
            Err(ServiceError::NoAvailablePort)
        ));

        let udp_repo = temp_repo().await;
        udp_repo
            .insert_allocation(&allocation("udp", Protocol::Udp, 10000, None, None))
            .await
            .unwrap();
        let udp_svc = service_with_range(udp_repo, InMemoryRuntime::default(), 10000, 10000);
        assert!(matches!(
            udp_svc.create_allocation(Protocol::Both, None).await,
            Err(ServiceError::NoAvailablePort)
        ));
    }

    #[tokio::test]
    async fn create_allocation_runtime_create_failure_skips_to_next_port() {
        let runtime = InMemoryRuntime::default();
        runtime.fail_create_port(10000);
        let svc = service_with_range(temp_repo().await, runtime.clone(), 10000, 10001);

        let allocation = svc
            .create_allocation(Protocol::Tcp, Some(8080))
            .await
            .unwrap();

        assert_eq!(allocation.port, 10001);
        assert_eq!(allocation.target_port, Some(8080));
        assert!(runtime.contains(&allocation.id));
        assert_eq!(runtime.calls().create.len(), 2);
    }

    #[tokio::test]
    async fn create_allocation_does_not_retry_non_retryable_runtime_apply_failed() {
        let runtime = InMemoryRuntime::default();
        runtime.fail_apply_port(10000);
        let svc = service_with_range(temp_repo().await, runtime.clone(), 10000, 10002);

        assert!(matches!(
            svc.create_allocation(Protocol::Tcp, None).await,
            Err(ServiceError::Runtime(RuntimeError::RuntimeApplyFailed))
        ));
        assert_eq!(runtime.calls().create, vec!["alloc-1".to_owned()]);
        assert!(svc.repo().list_allocations().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_allocation_all_ports_unavailable_returns_no_available_port() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("tcp", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let svc = service_with_range(repo, InMemoryRuntime::default(), 10000, 10000);

        assert!(matches!(
            svc.create_allocation(Protocol::Tcp, None).await,
            Err(ServiceError::NoAvailablePort)
        ));
    }

    #[tokio::test]
    async fn create_allocation_binding_persistence_failure_cleans_allocation_and_runtime() {
        let (repo, file) = temp_repo_with_path().await;
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&file)
            .create_if_missing(false);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        pool.execute(concat!(
            "CREATE TRIGGER fail_binding_insert BEFORE INSERT ON bindings ",
            "BEGIN SELECT RAISE(ABORT, 'binding persistence failed'); END;",
        ))
        .await
        .unwrap();

        let runtime = InMemoryRuntime::default();
        let svc = service_with_range(repo, runtime.clone(), 10000, 10000);

        assert!(matches!(
            svc.create_allocation(Protocol::Tcp, Some(8080)).await,
            Err(ServiceError::Repository(_))
        ));
        assert!(svc.repo().list_allocations().await.unwrap().is_empty());
        assert!(!runtime.contains("alloc-1"));
        assert_eq!(runtime.calls().delete, vec!["alloc-1".to_owned()]);
    }

    #[tokio::test]
    async fn put_binding_returns_api_fields_and_active_runtime_state() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("alloc-1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let runtime = InMemoryRuntime::default();
        let svc = service(repo, runtime.clone());

        let binding = svc.put_binding("alloc-1", "127.0.0.1", 8080).await.unwrap();
        assert_eq!(binding.allocation_id, "alloc-1");
        assert_eq!(binding.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(binding.target_port, 8080);
        assert_eq!(binding.created_at_ms, binding.updated_at_ms);

        let view = svc.get_binding_view("alloc-1").await.unwrap().unwrap();
        assert_eq!(view.allocation_id, "alloc-1");
        assert_eq!(view.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(view.target_port, 8080);
        assert_eq!(view.effective_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(view.effective_target_port, Some(8080));
        assert_eq!(view.runtime_status, RuntimeStatus::Active);
        assert_eq!(view.error_kind, None);
        assert_eq!(view.last_error, None);
        assert_eq!(runtime.calls().update, vec!["alloc-1".to_owned()]);
    }

    #[tokio::test]
    async fn put_binding_invalid_host_returns_invalid_host_without_runtime_update() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("alloc-1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let runtime = InMemoryRuntime::default();
        let svc = service(repo, runtime.clone());

        assert!(matches!(
            svc.put_binding("alloc-1", "localhost", 8080).await,
            Err(ServiceError::InvalidHost)
        ));
        assert!(runtime.calls().update.is_empty());
        assert!(svc.repo().get_binding("alloc-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_binding_clears_host_and_target_and_missing_binding_is_not_found() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation(
            "alloc-1",
            Protocol::Tcp,
            10000,
            Some(8080),
            Some("127.0.0.1"),
        ))
        .await
        .unwrap();
        repo.put_binding(&binding("alloc-1", 8080, Some("127.0.0.1"), 1000, 1100))
            .await
            .unwrap();
        let runtime = InMemoryRuntime::default();
        let svc = service(repo, runtime.clone());

        svc.delete_binding("alloc-1").await.unwrap();

        assert!(svc.repo().get_binding("alloc-1").await.unwrap().is_none());
        let allocation = svc.get_allocation("alloc-1").await.unwrap().unwrap();
        assert_eq!(allocation.target_port, None);
        assert_eq!(allocation.host, None);
        let view = svc.get_allocation_view("alloc-1").await.unwrap().unwrap();
        assert_eq!(view.runtime_status, RuntimeStatus::RejectingNoHost);
        assert_eq!(view.effective_target_port, None);
        assert_eq!(view.effective_host, None);
        assert!(matches!(
            svc.delete_binding("alloc-1").await,
            Err(ServiceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn update_runtime_failure_surfaces_and_leaves_persisted_binding_visible() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("alloc-1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let runtime = InMemoryRuntime::default();
        runtime.fail_update_id("alloc-1");
        let svc = service(repo, runtime);

        assert!(matches!(
            svc.put_binding("alloc-1", "127.0.0.1", 8080).await,
            Err(ServiceError::Runtime(RuntimeError::RuntimeUpdateFailed))
        ));
        let persisted = svc.repo().get_binding("alloc-1").await.unwrap().unwrap();
        assert_eq!(persisted.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(persisted.target_port, 8080);
    }

    #[tokio::test]
    async fn update_timeout_surfaces_after_persisting_binding_without_runtime_update() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("alloc-1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let runtime = InMemoryRuntime::default();
        let svc = service(repo, runtime.clone());
        svc.set_failpoints(Failpoints {
            update_timeout: true,
            ..Failpoints::default()
        })
        .await;

        assert!(matches!(
            svc.put_binding("alloc-1", "127.0.0.1", 8080).await,
            Err(ServiceError::Timeout)
        ));
        let persisted = svc.repo().get_binding("alloc-1").await.unwrap().unwrap();
        assert_eq!(persisted.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(persisted.target_port, 8080);
        assert!(runtime.calls().update.is_empty());
    }

    #[tokio::test]
    async fn set_target_preserves_target_port_and_binding_created_timestamp() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation(
            "alloc-1",
            Protocol::Tcp,
            10000,
            Some(8080),
            Some("127.0.0.1"),
        ))
        .await
        .unwrap();
        repo.put_binding(&binding("alloc-1", 8080, Some("127.0.0.1"), 777, 888))
            .await
            .unwrap();
        let runtime = InMemoryRuntime::default();
        let svc = service(repo, runtime.clone());

        let updated = svc.set_target("alloc-1", "127.0.0.2").await.unwrap();

        assert_eq!(updated.target_port, Some(8080));
        assert_eq!(updated.host.as_deref(), Some("127.0.0.2"));
        let persisted = svc.repo().get_binding("alloc-1").await.unwrap().unwrap();
        assert_eq!(persisted.created_at_ms, 777);
        assert!(persisted.updated_at_ms >= 888);
        assert_eq!(runtime.calls().update, vec!["alloc-1".to_owned()]);
    }

    #[tokio::test]
    async fn update_allocation_target_only_preserves_host_and_binding_created_timestamp() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation(
            "alloc-1",
            Protocol::Tcp,
            10000,
            Some(8080),
            Some("127.0.0.1"),
        ))
        .await
        .unwrap();
        repo.put_binding(&binding("alloc-1", 8080, Some("127.0.0.1"), 777, 888))
            .await
            .unwrap();
        let runtime = InMemoryRuntime::default();
        let svc = service(repo, runtime.clone());

        let updated = svc
            .update_allocation("alloc-1", Some(9090), None)
            .await
            .unwrap();

        assert_eq!(updated.target_port, Some(9090));
        assert_eq!(updated.host.as_deref(), Some("127.0.0.1"));
        let persisted = svc.repo().get_binding("alloc-1").await.unwrap().unwrap();
        assert_eq!(persisted.created_at_ms, 777);
        assert_eq!(persisted.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(runtime.calls().update, vec!["alloc-1".to_owned()]);
    }

    #[tokio::test]
    async fn update_allocation_without_target_or_existing_binding_returns_not_found() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("alloc-1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let svc = service(repo, InMemoryRuntime::default());

        assert!(matches!(
            svc.update_allocation("alloc-1", None, Some("127.0.0.1"))
                .await,
            Err(ServiceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn update_allocation_missing_id_returns_not_found_before_host_validation() {
        let svc = service(temp_repo().await, InMemoryRuntime::default());

        assert!(matches!(
            svc.update_allocation("missing", Some(8080), Some("localhost"))
                .await,
            Err(ServiceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn update_allocation_legacy_target_without_binding_still_returns_not_found() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation(
            "alloc-1",
            Protocol::Tcp,
            10000,
            Some(8080),
            Some("127.0.0.1"),
        ))
        .await
        .unwrap();
        repo.delete_binding("alloc-1", 1200).await.unwrap();
        let runtime = InMemoryRuntime::default();
        let svc = service(repo, runtime.clone());

        assert!(matches!(
            svc.set_target("alloc-1", "127.0.0.2").await,
            Err(ServiceError::NotFound)
        ));
        assert!(runtime.calls().update.is_empty());
    }

    #[tokio::test]
    async fn delete_allocation_runtime_failure_leaves_repository_row_intact() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("alloc-1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let runtime = InMemoryRuntime::default();
        runtime.fail_delete_id("alloc-1");
        let svc = service(repo, runtime);

        assert!(matches!(
            svc.delete_allocation("alloc-1").await,
            Err(ServiceError::Runtime(RuntimeError::RuntimeDeleteFailed))
        ));
        assert!(
            svc.repo()
                .get_allocation("alloc-1")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn delete_persistence_failpoint_restores_runtime_and_returns_error() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation(
            "alloc-1",
            Protocol::Tcp,
            10000,
            Some(8080),
            Some("127.0.0.1"),
        ))
        .await
        .unwrap();
        let runtime = InMemoryRuntime::default();
        let svc = service(repo, runtime.clone());
        svc.set_failpoints(Failpoints {
            delete_db_failure: true,
            ..Failpoints::default()
        })
        .await;

        assert!(matches!(
            svc.delete_allocation("alloc-1").await,
            Err(ServiceError::DeletePersistenceFailed)
        ));
        assert!(runtime.contains("alloc-1"));
        assert_eq!(runtime.calls().delete, vec!["alloc-1".to_owned()]);
        assert_eq!(runtime.calls().restore, vec!["alloc-1".to_owned()]);
        assert!(
            svc.repo()
                .get_allocation("alloc-1")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn delete_persistence_failpoint_surfaces_restore_failure() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("alloc-1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        let runtime = InMemoryRuntime::default();
        runtime.fail_restore_id("alloc-1");
        let svc = service(repo, runtime.clone());
        svc.set_failpoints(Failpoints {
            delete_db_failure: true,
            ..Failpoints::default()
        })
        .await;

        assert!(matches!(
            svc.delete_allocation("alloc-1").await,
            Err(ServiceError::Runtime(RuntimeError::RuntimeRestoreFailed))
        ));
        assert_eq!(runtime.calls().delete, vec!["alloc-1".to_owned()]);
        assert_eq!(runtime.calls().restore, vec!["alloc-1".to_owned()]);
    }

    #[tokio::test]
    async fn restore_all_restores_every_persisted_allocation() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("alloc-1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        repo.insert_allocation(&allocation(
            "alloc-2",
            Protocol::Udp,
            10001,
            Some(5353),
            None,
        ))
        .await
        .unwrap();
        let runtime = InMemoryRuntime::default();
        let svc = service(repo, runtime.clone());

        svc.restore_all(1234).await.unwrap();

        assert_eq!(
            runtime.calls().restore,
            vec!["alloc-1".to_owned(), "alloc-2".to_owned()]
        );
        assert!(runtime.contains("alloc-1"));
        assert!(runtime.contains("alloc-2"));
    }

    #[tokio::test]
    async fn restore_all_initializes_runtime_even_with_no_allocations() {
        let runtime = InMemoryRuntime::default();
        let svc = service(temp_repo().await, runtime.clone());

        svc.restore_all(500).await.unwrap();

        assert_eq!(runtime.initialize_calls(), 1);
        assert!(runtime.calls().restore.is_empty());
    }

    #[tokio::test]
    async fn snapshot_listener_metrics_returns_runtime_rows_unchanged() {
        let runtime = InMemoryRuntime::default();
        let rows = vec![
            ListenerMetricsSnapshot {
                port: 10000,
                protocol: Protocol::Tcp,
                connections_current: 1,
                rx_bytes_total: 2,
                tx_bytes_total: 3,
            },
            ListenerMetricsSnapshot {
                port: 10001,
                protocol: Protocol::Udp,
                connections_current: 4,
                rx_bytes_total: 5,
                tx_bytes_total: 6,
            },
        ];
        runtime.seed_listener_metrics(rows.clone());
        let svc = service(temp_repo().await, runtime);

        assert_eq!(svc.snapshot_listener_metrics().await.unwrap(), rows);
    }

    #[test]
    fn protocol_conflicts_match_allocation_rules() {
        assert!(conflicts(Protocol::Tcp, Protocol::Tcp));
        assert!(!conflicts(Protocol::Tcp, Protocol::Udp));
        assert!(conflicts(Protocol::Tcp, Protocol::Both));
        assert!(!conflicts(Protocol::Udp, Protocol::Tcp));
        assert!(conflicts(Protocol::Udp, Protocol::Udp));
        assert!(conflicts(Protocol::Udp, Protocol::Both));
        assert!(conflicts(Protocol::Both, Protocol::Tcp));
        assert!(conflicts(Protocol::Both, Protocol::Udp));
        assert!(conflicts(Protocol::Both, Protocol::Both));
    }
}
