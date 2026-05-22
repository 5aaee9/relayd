# Tracing Lifecycle Logs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add structured `tracing` lifecycle logs for relay allocation creation, binding assignment/update, binding deletion, and allocation deletion.

**Architecture:** Initialize tracing once in the binary with an `EnvFilter` defaulting to `info`. Emit service-layer success events after persistence/runtime side effects complete so primary and compatibility HTTP endpoints share the same logging behavior. Capture log output in service tests with a custom tracing subscriber writer.

**Tech Stack:** Rust 2024, Tokio, Axum, `tracing`, `tracing-subscriber`, existing relayd service/runtime/repository tests.

---

## File Structure

- Modify `Cargo.toml`: add `tracing` and `tracing-subscriber` dependencies.
- Modify `Cargo.lock`: resolve and lock the new dependencies before running `--locked` verification.
- Modify `src/bin/relayd.rs`: initialize subscriber, replace stderr shutdown failure with tracing error, add startup/restore/serve info events.
- Modify `src/service/allocation_service.rs`: import `tracing::info` and add success lifecycle events to `create_allocation`, `put_binding`, `update_allocation`, `delete_binding`, and `delete_allocation`; add tests capturing logs.
- Modify `README.md`: document default stderr logging and `RUST_LOG` filtering.

## Task 1: Add tracing dependencies and binary initialization

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/bin/relayd.rs`

- [ ] **Step 1: Add dependencies**

In `Cargo.toml`, add these dependencies under `[dependencies]`:

```toml
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
```

Then resolve the lockfile before any `--locked` commands:

```bash
cargo generate-lockfile
```

Expected: `Cargo.lock` is updated to include the new tracing subscriber dependency graph.

- [ ] **Step 2: Initialize tracing in the binary**

In `src/bin/relayd.rs`, add imports:

```rust
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
```

Update `main` so tracing is initialized before `run()`:

```rust
#[tokio::main]
async fn main() {
    init_tracing();
    if let Err(error) = run().await {
        error!(%error, "relayd exited with error");
        eprintln!("relayd: {error}");
        std::process::exit(1);
    }
}
```

Add this helper near `main`:

```rust
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
```

- [ ] **Step 3: Add startup and shutdown support events**

In `run_with_listener`, before opening the repository, add:

```rust
info!(
    http_listen = %listener.local_addr()?,
    proxy_listen_host = %config.proxy_listen_host,
    port_range_start = config.port_range.start,
    port_range_end = config.port_range.end,
    sqlite_path = %config.db_path,
    "starting relayd"
);
```

After `service.restore_all(...).await?;`, add:

```rust
info!("restored persisted relay allocations");
```

In `shutdown_signal`, replace the `eprintln!` branch with:

```rust
if let Err(error) = tokio::signal::ctrl_c().await {
    error!(%error, "failed to listen for shutdown signal");
}
```

- [ ] **Step 4: Verify binary-focused tests**

Run:

```bash
cargo test --locked --bin relayd
```

Expected: command exits 0 and existing binary tests pass.

## Task 2: Add service lifecycle tracing events and tests

**Files:**
- Modify: `src/service/allocation_service.rs`

- [ ] **Step 1: Import tracing**

At the top of `src/service/allocation_service.rs`, add:

```rust
use tracing::info;
```

- [ ] **Step 2: Log allocation creation success**

In `create_allocation`, immediately after `self.persist_created_allocation(&allocation).await` succeeds and before `return Ok(allocation);`, add:

```rust
info!(
    allocation_id = %allocation.id,
    protocol = %allocation.protocol,
    relay_port = allocation.port,
    target_port = ?allocation.target_port,
    host = ?allocation.host,
    "relay_allocation_created"
);
```

- [ ] **Step 3: Log binding assignment from `put_binding`**

In `put_binding`, after runtime update succeeds and before `Ok(binding)`, add:

```rust
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
```

- [ ] **Step 4: Log binding assignment from compatibility update paths**

In `update_allocation`, after runtime update succeeds and before `Ok(updated_allocation)`, add:

```rust
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
```

- [ ] **Step 5: Log binding deletion success**

In `delete_binding`, keep the loaded binding in a local before deletion:

```rust
let Some(existing_binding) = self.repo.get_binding(id).await? else {
    return Err(ServiceError::NotFound);
};
```

After runtime update succeeds and before `Ok(())`, add:

```rust
info!(
    allocation_id = %id,
    protocol = %updated_allocation.protocol,
    relay_port = updated_allocation.port,
    previous_target_port = existing_binding.target_port,
    previous_host = ?existing_binding.host,
    "relay_binding_deleted"
);
```

- [ ] **Step 6: Log allocation deletion success**

In `delete_allocation`, after `self.repo.delete_allocation(id).await?` returns true and before `Ok(())`, add:

```rust
info!(
    allocation_id = %allocation.id,
    protocol = %allocation.protocol,
    relay_port = allocation.port,
    target_port = ?allocation.target_port,
    host = ?allocation.host,
    "relay_allocation_deleted"
);
```

- [ ] **Step 7: Add service log capture tests**

Inside the existing `#[cfg(test)] mod tests` in `src/service/allocation_service.rs`, add imports:

```rust
use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use tracing::dispatcher::{self, Dispatch};
use tracing_subscriber::fmt::MakeWriter;
```

If `StdMutex` is already imported, do not duplicate it.

Add helper types/functions:

```rust
#[derive(Clone, Default)]
struct SharedLogWriter {
    buffer: Arc<StdMutex<Vec<u8>>>,
}

impl SharedLogWriter {
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

async fn capture_service_logs<F, Fut>(operation: F) -> String
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let writer = SharedLogWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer.clone())
        .with_ansi(false)
        .with_target(false)
        .with_level(false)
        .finish();
    let dispatch = Dispatch::new(subscriber);
    let _guard = dispatcher::set_default(&dispatch);
    operation().await;
    drop(_guard);
    writer.contents()
}
```

Add tests:

```rust
#[tokio::test]
async fn create_allocation_emits_lifecycle_log() {
    let svc = service(temp_repo().await, InMemoryRuntime::default());

    let logs = capture_service_logs(|| async {
        let allocation = svc.create_allocation(Protocol::Tcp, Some(8080)).await.unwrap();
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

    assert_eq!(logs.matches("relay_binding_assigned").count(), 2);
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
    repo.insert_allocation(&allocation("alloc-1", Protocol::Udp, 10000, Some(5353), None))
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
```

- [ ] **Step 8: Verify service tests**

Run:

```bash
cargo test --locked service::allocation_service
```

Expected: command exits 0 and new log tests pass.

## Task 3: Document and run final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document logging**

In `README.md`, after the Env section's paragraph about `HTTP_LISTEN`, add:

```markdown
## Logging

relayd initializes a `tracing` formatter and writes logs to stderr. The default filter is `info`, which includes lifecycle events for relay allocation creation, binding assignment/update, binding deletion, allocation deletion, startup restore, and shutdown-signal errors.

Use `RUST_LOG` to adjust filtering, for example:

```bash
RUST_LOG=relayd=debug cargo run --locked --bin relayd -- --http-listen :8080 --proxy-listen-host 127.0.0.1 --auth-token mytoken
```
```

- [ ] **Step 2: Run final checks**

Run these commands:

```bash
cargo fmt --check
cargo test --locked
cargo clippy --locked --lib --tests -- -D warnings
```

Expected: all commands exit 0.

- [ ] **Step 3: Review final diff**

Run:

```bash
git status --short
git diff -- Cargo.toml Cargo.lock src/bin/relayd.rs src/service/allocation_service.rs README.md docs/superpowers/specs/2026-05-22-tracing-lifecycle-logs-design.md docs/superpowers/plans/2026-05-22-tracing-lifecycle-logs.md
```

Expected: only spec, plan, dependency, logging, tests, and README changes are present.
