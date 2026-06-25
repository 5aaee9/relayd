use crate::model::{Allocation, ErrorKind, Protocol, RuntimeStatus};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedState {
    pub effective_target_port: Option<u16>,
    pub effective_host: Option<String>,
    pub runtime_status: RuntimeStatus,
    pub error_kind: Option<ErrorKind>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerMetricsSnapshot {
    pub port: u16,
    pub protocol: Protocol,
    pub connections_current: u64,
    pub rx_bytes_total: u64,
    pub tx_bytes_total: u64,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("runtime create failed")]
    RuntimeCreateFailed,
    #[error("runtime update failed")]
    RuntimeUpdateFailed,
    #[error("runtime delete failed")]
    RuntimeDeleteFailed,
    #[error("runtime restore failed")]
    RuntimeRestoreFailed,
    #[error("runtime apply failed")]
    RuntimeApplyFailed,
    #[error("timeout")]
    Timeout,
}

#[async_trait]
pub trait RuntimeFacade: Send + Sync {
    async fn initialize(&self, timeout_ms: u32) -> Result<(), RuntimeError>;
    async fn create(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError>;
    async fn update(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError>;
    async fn delete(&self, id: &str, timeout_ms: u32) -> Result<(), RuntimeError>;
    async fn restore(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError>;
    async fn snapshot(&self, id: &str) -> Result<Option<ObservedState>, RuntimeError>;
    async fn snapshot_listener_metrics(&self)
    -> Result<Vec<ListenerMetricsSnapshot>, RuntimeError>;
}

#[derive(Default, Debug)]
struct RuntimeState {
    allocations: HashMap<String, Allocation>,
    listener_metrics: Vec<ListenerMetricsSnapshot>,
    initialize_fail: bool,
    create_fail_ports: HashSet<u16>,
    apply_fail_ports: HashSet<u16>,
    update_fail_ids: HashSet<String>,
    delete_fail_ids: HashSet<String>,
    restore_fail_ids: HashSet<String>,
    initialize_calls: u32,
    create_calls: Vec<String>,
    update_calls: Vec<String>,
    delete_calls: Vec<String>,
    restore_calls: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeCalls {
    pub create: Vec<String>,
    pub update: Vec<String>,
    pub delete: Vec<String>,
    pub restore: Vec<String>,
}

#[derive(Clone, Default, Debug)]
pub struct InMemoryRuntime {
    state: Arc<Mutex<RuntimeState>>,
}

impl InMemoryRuntime {
    pub fn fail_initialize(&self) {
        self.state.lock().unwrap().initialize_fail = true;
    }

    pub fn fail_create_port(&self, port: u16) {
        self.state.lock().unwrap().create_fail_ports.insert(port);
    }

    pub fn fail_apply_port(&self, port: u16) {
        self.state.lock().unwrap().apply_fail_ports.insert(port);
    }

    pub fn fail_update_id(&self, id: impl Into<String>) {
        self.state.lock().unwrap().update_fail_ids.insert(id.into());
    }

    pub fn fail_delete_id(&self, id: impl Into<String>) {
        self.state.lock().unwrap().delete_fail_ids.insert(id.into());
    }

    pub fn fail_restore_id(&self, id: impl Into<String>) {
        self.state
            .lock()
            .unwrap()
            .restore_fail_ids
            .insert(id.into());
    }

    pub fn contains(&self, id: &str) -> bool {
        self.state.lock().unwrap().allocations.contains_key(id)
    }

    pub fn initialize_calls(&self) -> u32 {
        self.state.lock().unwrap().initialize_calls
    }

    pub fn calls(&self) -> RuntimeCalls {
        let state = self.state.lock().unwrap();
        RuntimeCalls {
            create: state.create_calls.clone(),
            update: state.update_calls.clone(),
            delete: state.delete_calls.clone(),
            restore: state.restore_calls.clone(),
        }
    }

    pub fn seed_listener_metrics(&self, rows: Vec<ListenerMetricsSnapshot>) {
        self.state.lock().unwrap().listener_metrics = rows;
    }

    pub fn clear_failures(&self) {
        let mut state = self.state.lock().unwrap();
        state.initialize_fail = false;
        state.create_fail_ports.clear();
        state.apply_fail_ports.clear();
        state.update_fail_ids.clear();
        state.delete_fail_ids.clear();
        state.restore_fail_ids.clear();
    }
}

#[async_trait]
impl RuntimeFacade for InMemoryRuntime {
    async fn initialize(&self, _timeout_ms: u32) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().unwrap();
        state.initialize_calls = state.initialize_calls.saturating_add(1);
        if state.initialize_fail {
            return Err(RuntimeError::RuntimeApplyFailed);
        }
        Ok(())
    }

    async fn create(&self, allocation: &Allocation, _timeout_ms: u32) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().unwrap();
        state.create_calls.push(allocation.id.clone());
        if state.create_fail_ports.contains(&allocation.port) {
            return Err(RuntimeError::RuntimeCreateFailed);
        }
        if state.apply_fail_ports.contains(&allocation.port) {
            return Err(RuntimeError::RuntimeApplyFailed);
        }
        state
            .allocations
            .insert(allocation.id.clone(), allocation.clone());
        Ok(())
    }

    async fn update(&self, allocation: &Allocation, _timeout_ms: u32) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().unwrap();
        state.update_calls.push(allocation.id.clone());
        if state.update_fail_ids.contains(&allocation.id) {
            return Err(RuntimeError::RuntimeUpdateFailed);
        }
        state
            .allocations
            .insert(allocation.id.clone(), allocation.clone());
        Ok(())
    }

    async fn delete(&self, id: &str, _timeout_ms: u32) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().unwrap();
        state.delete_calls.push(id.to_owned());
        if state.delete_fail_ids.contains(id) {
            return Err(RuntimeError::RuntimeDeleteFailed);
        }
        state.allocations.remove(id);
        Ok(())
    }

    async fn restore(&self, allocation: &Allocation, _timeout_ms: u32) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().unwrap();
        state.restore_calls.push(allocation.id.clone());
        if state.restore_fail_ids.contains(&allocation.id) {
            return Err(RuntimeError::RuntimeRestoreFailed);
        }
        state
            .allocations
            .insert(allocation.id.clone(), allocation.clone());
        Ok(())
    }

    async fn snapshot(&self, id: &str) -> Result<Option<ObservedState>, RuntimeError> {
        let state = self.state.lock().unwrap();
        Ok(state.allocations.get(id).map(observed_state_for))
    }

    async fn snapshot_listener_metrics(
        &self,
    ) -> Result<Vec<ListenerMetricsSnapshot>, RuntimeError> {
        Ok(self.state.lock().unwrap().listener_metrics.clone())
    }
}

fn observed_state_for(allocation: &Allocation) -> ObservedState {
    let runtime_status = if allocation.host.is_some() && allocation.target_port.is_some() {
        RuntimeStatus::Active
    } else {
        RuntimeStatus::RejectingNoHost
    };
    ObservedState {
        effective_target_port: allocation.target_port,
        effective_host: allocation.host.clone(),
        runtime_status,
        error_kind: None,
        last_error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn in_memory_runtime_reports_active_only_when_host_and_target_port_are_present() {
        let runtime = InMemoryRuntime::default();
        runtime
            .create(
                &allocation("active", 10000, Some(8080), Some("127.0.0.1")),
                500,
            )
            .await
            .unwrap();
        runtime
            .create(&allocation("missing-host", 10001, Some(8081), None), 500)
            .await
            .unwrap();
        runtime
            .create(
                &allocation("missing-port", 10002, None, Some("127.0.0.1")),
                500,
            )
            .await
            .unwrap();

        let active = runtime.snapshot("active").await.unwrap().unwrap();
        assert_eq!(active.runtime_status, RuntimeStatus::Active);
        assert_eq!(active.effective_target_port, Some(8080));
        assert_eq!(active.effective_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(active.error_kind, None);
        assert_eq!(active.last_error, None);

        let missing_host = runtime.snapshot("missing-host").await.unwrap().unwrap();
        assert_eq!(missing_host.runtime_status, RuntimeStatus::RejectingNoHost);
        assert_eq!(missing_host.effective_target_port, Some(8081));
        assert_eq!(missing_host.effective_host, None);
        assert_eq!(missing_host.error_kind, None);
        assert_eq!(missing_host.last_error, None);

        let missing_port = runtime.snapshot("missing-port").await.unwrap().unwrap();
        assert_eq!(missing_port.runtime_status, RuntimeStatus::RejectingNoHost);
        assert_eq!(missing_port.effective_target_port, None);
        assert_eq!(missing_port.effective_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(missing_port.error_kind, None);
        assert_eq!(missing_port.last_error, None);
    }

    #[tokio::test]
    async fn in_memory_runtime_tracks_calls_and_failpoints() {
        let runtime = InMemoryRuntime::default();
        let alloc = allocation("a1", 10000, Some(8080), Some("127.0.0.1"));

        runtime.fail_initialize();
        assert_eq!(
            runtime.initialize(500).await,
            Err(RuntimeError::RuntimeApplyFailed)
        );

        runtime.clear_failures();
        runtime.initialize(500).await.unwrap();
        assert_eq!(runtime.initialize_calls(), 2);

        runtime.fail_apply_port(10000);
        assert_eq!(
            runtime.create(&alloc, 500).await,
            Err(RuntimeError::RuntimeApplyFailed)
        );
        assert!(!runtime.contains("a1"));

        runtime.clear_failures();
        runtime.fail_create_port(10000);
        assert_eq!(
            runtime.create(&alloc, 500).await,
            Err(RuntimeError::RuntimeCreateFailed)
        );
        assert!(!runtime.contains("a1"));

        runtime.clear_failures();
        runtime.create(&alloc, 500).await.unwrap();
        assert!(runtime.contains("a1"));

        runtime.fail_update_id("a1");
        assert_eq!(
            runtime.update(&alloc, 500).await,
            Err(RuntimeError::RuntimeUpdateFailed)
        );

        runtime.clear_failures();
        runtime
            .update(&allocation("a1", 10000, Some(9090), Some("127.0.0.1")), 500)
            .await
            .unwrap();
        assert_eq!(
            runtime
                .snapshot("a1")
                .await
                .unwrap()
                .unwrap()
                .effective_target_port,
            Some(9090)
        );

        runtime.fail_delete_id("a1");
        assert_eq!(
            runtime.delete("a1", 500).await,
            Err(RuntimeError::RuntimeDeleteFailed)
        );
        assert!(runtime.contains("a1"));

        runtime.clear_failures();
        runtime.delete("a1", 500).await.unwrap();
        assert!(!runtime.contains("a1"));

        runtime.fail_restore_id("a1");
        assert_eq!(
            runtime.restore(&alloc, 500).await,
            Err(RuntimeError::RuntimeRestoreFailed)
        );
        runtime.clear_failures();
        runtime.restore(&alloc, 500).await.unwrap();

        let calls = runtime.calls();
        assert_eq!(calls.create, vec!["a1", "a1", "a1"]);
        assert_eq!(calls.update, vec!["a1", "a1"]);
        assert_eq!(calls.delete, vec!["a1", "a1"]);
        assert_eq!(calls.restore, vec!["a1", "a1"]);
    }
}
