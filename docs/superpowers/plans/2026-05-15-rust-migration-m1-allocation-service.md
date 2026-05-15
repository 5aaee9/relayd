# Rust Migration M1 Allocation Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port relayd's allocation-service behavior to Rust using the M0 SeaORM/SQLx repository and an in-memory runtime facade, while preserving API-facing model definitions for exact HTTP API parity in M2.

**Architecture:** Add a `runtime::facade` abstraction with an in-memory implementation for tests, then add `service::allocation_service` that serializes mutations with an async mutex and composes repository + runtime operations. Keep real network behavior out of M1; tests prove service semantics, conflict rules, binding lifecycle, runtime failure handling, and restore orchestration.

**Tech Stack:** Rust 1.95, Tokio, async-trait, SeaORM/SQLx repository from M0, existing Rust model/config modules, `cargo test --locked`, `zig build test`.

---

## File Structure

- Modify: `Cargo.toml` — add direct `async-trait` dependency for object-safe async runtime facade.
- Modify: `Cargo.lock` — lock dependency changes.
- Modify: `src/lib.rs` — export `runtime` and `service` modules.
- Create: `src/runtime/mod.rs` — runtime module declaration.
- Create: `src/runtime/facade.rs` — `RuntimeFacade`, `ObservedState`, `RuntimeError`, `ListenerMetricsSnapshot`, and `InMemoryRuntime` test facade.
- Create: `src/service/mod.rs` — service module declaration.
- Create: `src/service/allocation_service.rs` — Rust service implementation and service tests.
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md` — update M1 status after implementation.
- Existing: `docs/superpowers/specs/2026-05-15-rust-migration-m1-allocation-service.md` — M1 spec.

## Acceptance checklist

- [ ] Plan has independent `APPROVED` review before implementation.
- [ ] `cargo test --locked service` passes.
- [ ] `cargo test --locked` passes.
- [ ] `zig build test` passes.
- [ ] Service conflict tests cover tcp-vs-both, udp-vs-both, both-vs-existing-tcp, both-vs-existing-udp, and non-conflicting tcp+udp same port.
- [ ] Lifecycle tests cover create/get/list/delete allocation resources and aggregate views.
- [ ] Binding tests cover put/get/delete, invalid host, missing binding not-found, runtime update calls, and no-host runtime state after binding delete.
- [ ] Runtime failure tests cover create bind failure port skip, no available port, update failure, delete runtime failure, create binding persistence cleanup, delete persistence failure restore, and restore-all orchestration.
- [ ] M1 does not expose HTTP routes and does not change API-facing model definitions incompatibly.
- [ ] Listener metrics snapshot pass-through test verifies service delegates runtime rows unchanged.
- [ ] Compatibility update tests cover set-target host-only update, target-port-only update, no-resolved-target `NotFound`, timestamp preservation, and runtime update calls.
- [ ] Docs record M1 status and M2 wire/API parity responsibility.

## Task 1: Add runtime facade module

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Create: `src/runtime/mod.rs`
- Create: `src/runtime/facade.rs`

- [ ] **Step 1: Add dependency**

Add to `[dependencies]` in `Cargo.toml`:

```toml
async-trait = "0.1"
```

- [ ] **Step 2: Export runtime module**

Append to `src/lib.rs`:

```rust
pub mod runtime;
```

Create `src/runtime/mod.rs`:

```rust
pub mod facade;
```

- [ ] **Step 3: Define runtime facade contracts**

Create `src/runtime/facade.rs` with:

```rust
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
    #[error("timeout")]
    Timeout,
}

#[async_trait]
pub trait RuntimeFacade: Send + Sync {
    async fn create(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError>;
    async fn update(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError>;
    async fn delete(&self, id: &str, timeout_ms: u32) -> Result<(), RuntimeError>;
    async fn restore(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError>;
    async fn snapshot(&self, id: &str) -> Result<Option<ObservedState>, RuntimeError>;
    async fn snapshot_listener_metrics(&self) -> Result<Vec<ListenerMetricsSnapshot>, RuntimeError>;
}
```

- [ ] **Step 4: Add in-memory runtime for service tests**

In the same file, add `InMemoryRuntime` with shared state:

```rust
#[derive(Default, Debug)]
struct RuntimeState {
    allocations: HashMap<String, Allocation>,
    create_fail_ports: HashSet<u16>,
    update_fail_ids: HashSet<String>,
    delete_fail_ids: HashSet<String>,
    restore_fail_ids: HashSet<String>,
    create_calls: Vec<String>,
    update_calls: Vec<String>,
    delete_calls: Vec<String>,
    restore_calls: Vec<String>,
}

#[derive(Clone, Default)]
pub struct InMemoryRuntime {
    state: Arc<Mutex<RuntimeState>>,
}
```

Implement helpers for tests: `fail_create_port`, `fail_update_id`, `fail_delete_id`, `fail_restore_id`, `contains`, `calls`, and `clear_failures`. `ObservedState` should be derived from an allocation: `active` when both `host` and `target_port` are present; otherwise `rejecting_no_host` with no error.

- [ ] **Step 5: Run targeted compile**

Run: `CARGO_TARGET_DIR=/tmp/relayd-m1-task1-target cargo test --locked runtime::facade`

Expected: compiles; no tests yet or only facade helper tests if added.

## Task 2: Implement allocation service skeleton and read/list methods

**Files:**
- Modify: `src/lib.rs`
- Create: `src/service/mod.rs`
- Create: `src/service/allocation_service.rs`

- [ ] **Step 1: Export service module**

Append to `src/lib.rs`:

```rust
pub mod service;
```

Create `src/service/mod.rs`:

```rust
pub mod allocation_service;
```

- [ ] **Step 2: Define service types**

Create `src/service/allocation_service.rs` with imports and types:

```rust
use crate::config::{parse_ip_literal, PortRange};
use crate::model::{self, Allocation, AllocationResource, AllocationView, Binding, BindingView, ErrorKind, Protocol, RuntimeStatus};
use crate::runtime::facade::{ListenerMetricsSnapshot, ObservedState, RuntimeError, RuntimeFacade};
use crate::storage::sqlite::{Repository, RepositoryError};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

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
```

Implement constructors:

```rust
impl<R: RuntimeFacade> Service<R> {
    pub fn new(repo: Repository, runtime: R, port_range: PortRange, apply_timeout_ms: u32) -> Self { ... }
    pub fn with_id_generator(repo: Repository, runtime: R, port_range: PortRange, apply_timeout_ms: u32, id_generator: IdGenerator) -> Self { ... }
    pub async fn set_failpoints(&self, failpoints: Failpoints) { ... }
    pub fn repo(&self) -> &Repository { ... }
    pub fn runtime(&self) -> &R { ... }
}
```

- [ ] **Step 3: Add view conversion helpers**

Implement helpers equivalent to Zig service:

```rust
fn clone_allocation_resource(allocation: &Allocation) -> AllocationResource { ... }
fn missing_runtime_state() -> ObservedState { ... }
fn binding_view(binding: Binding, observed: Option<ObservedState>) -> BindingView { ... }
fn allocation_view(allocation: Allocation, observed: Option<ObservedState>) -> AllocationView { ... }
fn conflicts(requested: Protocol, existing: Protocol) -> bool { ... }
```

Missing runtime snapshot must map to `RuntimeStatus::DegradedBindFailed`, `Some(ErrorKind::BindFailed)`, `Some("missing runtime state")`.

- [ ] **Step 4: Implement read/list methods**

Implement:

```rust
pub async fn get_allocation(&self, id: &str) -> Result<Option<Allocation>, ServiceError>;
pub async fn list_allocation_resources(&self) -> Result<Vec<AllocationResource>, ServiceError>;
pub async fn get_allocation_resource(&self, id: &str) -> Result<Option<AllocationResource>, ServiceError>;
pub async fn list_allocations(&self) -> Result<Vec<AllocationView>, ServiceError>;
pub async fn get_allocation_view(&self, id: &str) -> Result<Option<AllocationView>, ServiceError>;
pub async fn get_binding_view(&self, id: &str) -> Result<Option<BindingView>, ServiceError>;
pub async fn snapshot_listener_metrics(&self) -> Result<Vec<ListenerMetricsSnapshot>, ServiceError>;
```

- [ ] **Step 5: Add read/list tests**

Add `#[cfg(test)]` module with temporary repository helper and deterministic ID generator. Test:

- empty lists are empty;
- inserted allocation can be read as allocation resource;
- missing runtime snapshot degrades aggregate view exactly as specified.

Run: `CARGO_TARGET_DIR=/tmp/relayd-m1-task2-target cargo test --locked service::allocation_service`

## Task 3: Implement create allocation and conflict behavior

**Files:**
- Modify: `src/service/allocation_service.rs`

- [ ] **Step 1: Implement `create_allocation`**

Add:

```rust
pub async fn create_allocation(&self, protocol: Protocol, target_port: Option<u16>) -> Result<Allocation, ServiceError>
```

Behavior:

1. Lock `mutation_lock`.
2. Return `Timeout` when `failpoints.create_timeout` is true.
3. Loop every port in `port_range.start..=port_range.end`.
4. Use `repo.list_allocations().await?` and `conflicts()` to skip occupied ports.
5. Create allocation with generated UUID, protocol, port, optional target_port, no host, current `created_at_ms`/`updated_at_ms` from `SystemTime::now()` milliseconds.
6. Call `runtime.create(&allocation, apply_timeout_ms)`.
7. If runtime returns `RuntimeCreateFailed`, skip this port and continue.
8. Persist allocation and optional compatibility binding atomically from the caller perspective. Prefer a SeaORM/SQLx transaction helper if available; otherwise if binding insertion fails after allocation insertion, delete the inserted allocation and binding best-effort before returning the repository error.
9. If any persistence step fails after runtime create, call `runtime.delete(id, apply_timeout_ms)` best-effort before returning the error.
10. Return allocation or `NoAvailablePort`.

- [ ] **Step 2: Add conflict tests**

Add tests covering:

- `tcp` then `udp` can share the same numeric port.
- Existing `both` blocks later `tcp` and later `udp` on that port.
- Existing `tcp` blocks later `both` on that port.
- Existing `udp` blocks later `both` on that port.
- Runtime create failure on first port skips to next port.
- Binding persistence failure during compatibility create removes the inserted allocation and deletes runtime state best-effort.
- All ports unavailable returns `NoAvailablePort`.

Run: `CARGO_TARGET_DIR=/tmp/relayd-m1-task3-target cargo test --locked service::allocation_service`

## Task 4: Implement binding, update, delete, and restore behavior

**Files:**
- Modify: `src/service/allocation_service.rs`

- [ ] **Step 1: Implement binding operations**

Add:

```rust
pub async fn put_binding(&self, id: &str, host: &str, target_port: u16) -> Result<Binding, ServiceError>;
pub async fn delete_binding(&self, id: &str) -> Result<(), ServiceError>;
```

Behavior mirrors Zig:

- `put_binding` validates host with `parse_ip_literal(host)`; missing allocation returns `NotFound`; existing binding preserves original `created_at_ms`; persistence happens before runtime update; update fail returns runtime error.
- `delete_binding` requires existing allocation and binding; clears binding in repo; updates runtime allocation to no target/no host; update fail returns runtime error.

- [ ] **Step 2: Implement compatibility update operations**

Add:

```rust
pub async fn set_target(&self, id: &str, host: &str) -> Result<Allocation, ServiceError>;
pub async fn update_allocation(&self, id: &str, target_port: Option<u16>, host: Option<&str>) -> Result<Allocation, ServiceError>;
```

Behavior mirrors Zig compatibility routes:

- validate host when provided;
- missing allocation returns `NotFound`;
- if no existing binding and no target port can be resolved, return `NotFound`;
- persist binding with existing created timestamp or now;
- update runtime after persistence;
- return the updated allocation.

- [ ] **Step 3: Implement delete and restore**

Add:

```rust
pub async fn delete_allocation(&self, id: &str) -> Result<(), ServiceError>;
pub async fn restore_all(&self, timeout_ms: u32) -> Result<(), ServiceError>;
```

Behavior:

- delete locks mutation, requires existing allocation, returns `Timeout` failpoint if set, calls runtime delete before repository delete;
- if `delete_db_failure` failpoint is set, call runtime restore with the saved allocation and return `DeletePersistenceFailed`;
- restore_all lists repository allocations and calls runtime restore for each.

- [ ] **Step 4: Add binding/delete/restore tests**

Add tests for:

- put/get binding returns exact API-facing fields and active observed runtime state.
- invalid host returns `InvalidHost` and does not call runtime update.
- delete binding clears host/target, produces no-host aggregate state, and missing binding returns `NotFound`.
- update runtime failure surfaces and leaves persisted binding visible.
- `set_target` preserves existing target port and binding created timestamp while updating host and calling runtime update.
- `update_allocation` with target only preserves existing host and created timestamp while calling runtime update.
- `update_allocation` with no target and no existing binding returns `NotFound`.
- delete runtime failure leaves repository row intact.
- delete persistence failpoint restores runtime and returns `DeletePersistenceFailed`.
- restore_all calls runtime restore for all persisted rows.
- snapshot_listener_metrics returns exactly the runtime facade rows without mutation.

Run: `CARGO_TARGET_DIR=/tmp/relayd-m1-task4-target cargo test --locked service::allocation_service`

## Task 5: Documentation, full verification, and commit

**Files:**
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-m1-allocation-service.md` if implementation notes need clarification.

- [ ] **Step 1: Update M1 status**

Update milestone design M1 status to:

```markdown
## M1 implementation status

- Status: implemented in Rust service layer with in-memory runtime facade.
- Verification: `cargo test --locked`; `zig build test`.
- API note: HTTP wire-level compatibility remains M2, but M1 preserves allocation resource, binding view, and aggregate allocation view definitions.
```

- [ ] **Step 2: Run full verification**

Run:

```bash
cargo fmt -- --check
CARGO_TARGET_DIR=/tmp/relayd-m1-final-target cargo test --locked
zig build test
```

Expected: all pass.

- [ ] **Step 3: Independent spec-compliance review**

Dispatch independent reviewer with M1 spec and actual artifacts. Fix and re-review until `APPROVED`.

- [ ] **Step 4: Commit and push after approval**

Use Lore Commit Protocol. Intent line example:

```text
Port allocation orchestration before exposing Rust HTTP routes

Constraint: Keep network runtime and HTTP wire parity for later milestones while preserving API-facing model definitions.
Rejected: Opening real listeners in M1 | belongs in TCP/UDP runtime milestones.
Confidence: high
Scope-risk: moderate
Directive: Keep service model outputs compatible with existing HTTP API shapes when implementing M2.
Tested: cargo test --locked; zig build test
Not-tested: Live TCP/UDP forwarding and HTTP JSON wire parity, deferred to M2-M5.
```
