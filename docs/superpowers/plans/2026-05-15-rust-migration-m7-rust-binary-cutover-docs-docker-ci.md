# Rust Migration M7 Rust Binary Cutover, Docs, Docker, and CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Rust implementation the default runnable `relayd` binary and update docs, Docker, and CI around Cargo while keeping Zig tests as parity verification.

**Architecture:** Add a real async Rust binary entrypoint that composes existing Rust config, repository, service, `RealRuntime`, restore, and Axum router. Keep all HTTP/resource types stable. Update packaging surfaces to build the Cargo binary and update docs to describe Rust-primary operation plus deferred optional Zig lanes.

**Tech Stack:** Rust 1.95, Tokio multi-thread runtime, Axum, SQLx/SeaORM SQLite repository, existing `RealRuntime`, Cargo CI, Docker multi-stage Rust build, Zig reference tests.

---

## File Structure

- Modify: `src/bin/relayd.rs` — replace scaffold with real async startup.
- Modify if needed: `src/http/control_plane.rs` — expose or reuse router/AppState only; avoid route semantics changes.
- Modify if needed: `src/config.rs` — only add small public helpers if binary cannot access existing config cleanly.
- Modify: `README.md` — Rust-primary build/run/test docs; Zig reference note.
- Modify: `docs/API.md` and/or `docs/api/http.md` — only clarify Rust parity and `both` Prometheus concrete rows if missing.
- Modify: `Dockerfile` — Cargo build and Rust runtime image.
- Modify: `.github/workflows/test.yml`, `.github/workflows/build.yaml`, `.github/workflows/docker.yaml` — Rust primary CI/build and Zig parity test.
- Modify: `scripts/ci/e2e_iperf3.sh` — make `RELAYD_BIN` overrideable so Rust CI can run the harness against `target/release/relayd`.
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md` — add M7 implementation status after completion.

## Acceptance checklist

- [ ] Independent plan reviewer returns `APPROVED` before implementation.
- [ ] `src/bin/relayd.rs` starts the real Rust service.
- [ ] Binary parses env config and rejects invalid/missing required values with non-zero exit.
- [ ] Binary opens SQLite, creates `RealRuntime`, runs `restore_all`, and serves HTTP.
- [ ] Authenticated smoke test verifies the real startup composition path parses env/config, opens temp SQLite, builds `RealRuntime`/`Service`, runs `restore_all`, starts the router, and responds on loopback.
- [ ] Dockerfile builds Rust binary, runtime image copies Cargo artifact, and installs runtime SQLite libraries if dynamically linked.
- [ ] CI runs Rust fmt/test/clippy/build and `zig build test` parity.
- [ ] README and API docs reflect Rust-primary operation and unchanged API semantics.
- [ ] `cargo fmt -- --check` passes.
- [ ] `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked` passes.
- [ ] `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings` passes.
- [ ] `cargo build --locked --bin relayd` passes.
- [ ] `zig build test` passes.
- [ ] Docker build is run if Docker is available; otherwise final report includes `Not-tested: docker build unavailable`.
- [ ] Final independent spec-compliance reviewer returns `APPROVED` before commit/push.

## Task 1: Implement real Rust binary startup

**Files:**
- Modify: `src/bin/relayd.rs`
- Possibly inspect: `src/config.rs`, `src/http/control_plane.rs`, `src/storage/sqlite.rs`, `src/runtime/real.rs`

- [ ] **Step 1: Write a failing startup smoke test**

Because binary integration tests can be expensive, first add a unit-testable startup helper in `src/bin/relayd.rs` and test its config validation indirectly. Replace the scaffold with a module structure that will compile after implementation. Add tests at the bottom of `src/bin/relayd.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn startup_config_requires_auth_token() {
        let env = HashMap::from([
            ("HTTP_LISTEN".to_owned(), ":0".to_owned()),
            ("PORT_RANGE".to_owned(), "20000-20010".to_owned()),
        ]);
        let error = Config::from_env_map(&env).unwrap_err();
        assert!(error.to_string().contains("AUTH_TOKEN"));
    }
}
```

`Config::from_env_map` is the current public parser and takes `&HashMap<String, String>`. The binary should add a small `config_from_env()` wrapper that collects `std::env::vars()` into that map.

- [ ] **Step 2: Run red compile/test**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked --bin relayd -- --nocapture
```

Expected: FAIL while binary is still scaffolded or missing imports.

- [ ] **Step 3: Implement async main**

Implement `src/bin/relayd.rs` with this shape, adapting exact config field names to `src/config.rs`:

```rust
use relayd::config::Config;
use relayd::http::control_plane::{AppState, router};
use relayd::metrics::Metrics;
use relayd::runtime::real::{RealRuntime, RealRuntimeConfig};
use relayd::service::allocation_service::Service;
use relayd::storage::sqlite::Repository;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("relayd: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = config_from_env()?;
    run_with_config(config).await
}

fn config_from_env() -> Result<Config, relayd::config::ConfigError> {
    let env: HashMap<String, String> = std::env::vars().collect();
    Config::from_env_map(&env)
}

async fn run_with_config(config: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let metrics = Arc::new(Metrics::default());
    let repo = Repository::open(&config.db_path).await?;
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let service = Arc::new(Service::new(
        repo,
        runtime,
        config.port_range,
        config.runtime_apply_timeout_ms,
    ));
    service.restore_all(config.restore_sweep_timeout_ms).await?;
    let state = AppState::new(service, metrics, config.auth_token);
    let addr: SocketAddr = format!("{}:{}", config.http_listen_host, config.http_listen_port).parse()?;
    serve(addr, state).await
}

async fn serve(
    addr: SocketAddr,
    state: AppState<RealRuntime>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;
    Ok(())
}
```


- [ ] **Step 4: Run binary tests/build**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked --bin relayd -- --nocapture
cargo build --locked --bin relayd
```

Expected: PASS.

## Task 2: Add real startup composition smoke test

**Files:**
- Modify: `src/bin/relayd.rs` or add integration test under `tests/` if easier.

- [ ] **Step 1: Add testable server helper**

If not already present, make `serve` accept a `TcpListener` or add a helper:

```rust
async fn serve_listener(
    listener: TcpListener,
    state: AppState<RealRuntime>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    axum::serve(listener, router(state)).await?;
    Ok(())
}
```

Then have `serve(addr, state)` bind and call `serve_listener`.

- [ ] **Step 2: Add authenticated real startup composition smoke test**

Add a test in `src/bin/relayd.rs` that exercises the real startup path. The test must not manually construct `Repository`, `RealRuntime`, `Service`, or `AppState` outside the production helper. It must build an env map, call `Config::from_env_map(&env)`, and then call a `run_with_listener(config, listener)` helper that performs: `Repository::open(config.db_path)` → `RealRuntime::new` → `Service::new` → `service.restore_all(config.restore_sweep_timeout_ms)` → `AppState::new` → `serve_listener`.

Production helpers should be structured like this:

```rust
async fn run_with_config(config: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", config.http_listen_host, config.http_listen_port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    run_with_listener(config, listener).await
}

async fn run_with_listener(
    config: Config,
    listener: TcpListener,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let metrics = Arc::new(Metrics::default());
    let repo = Repository::open(&config.db_path).await?;
    let runtime = RealRuntime::new(RealRuntimeConfig::loopback(metrics.clone()));
    let service = Arc::new(Service::new(
        repo,
        runtime,
        config.port_range,
        config.runtime_apply_timeout_ms,
    ));
    service.restore_all(config.restore_sweep_timeout_ms).await?;
    let state = AppState::new(service, metrics, config.auth_token);
    serve_listener(listener, state).await
}
```

Then add the smoke test:

```rust
#[tokio::test]
async fn startup_path_serves_authenticated_metrics_from_temp_sqlite() {
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let parent = std::env::current_dir().unwrap().join("target/relayd-test-dbs");
    std::fs::create_dir_all(&parent).unwrap();
    let dir = tempfile::tempdir_in(parent).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let env = HashMap::from([
        ("HTTP_LISTEN".to_owned(), format!("127.0.0.1:{}", addr.port())),
        ("PORT_RANGE".to_owned(), "24000-24010".to_owned()),
        ("AUTH_TOKEN".to_owned(), "secret-token".to_owned()),
        ("SQLITE_PATH".to_owned(), dir.path().join("relayd.sqlite").display().to_string()),
        ("RUNTIME_APPLY_TIMEOUT_MS".to_owned(), "500".to_owned()),
        ("RESTORE_SWEEP_TIMEOUT_MS".to_owned(), "500".to_owned()),
    ]);
    let config = Config::from_env_map(&env).unwrap();
    let server = tokio::spawn(async move { run_with_listener(config, listener).await.unwrap() });

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer secret-token\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut body = String::new();
    stream.read_to_string(&mut body).await.unwrap();
    assert!(body.starts_with("HTTP/1.1 200 OK"));
    assert!(body.contains("allocations_total"));

    server.abort();
}
```

- [ ] **Step 3: Run smoke test**

Run:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked --bin relayd startup_path_serves_authenticated_metrics_from_temp_sqlite -- --nocapture
```

Expected: PASS.

## Task 3: Update Dockerfile and CI to Rust-primary

**Files:**
- Modify: `Dockerfile`
- Modify: `.github/workflows/test.yml`
- Modify: `.github/workflows/build.yaml`
- Inspect/possibly modify: `.github/workflows/docker.yaml`

- [ ] **Step 1: Update Dockerfile**

Replace Zig builder with Rust builder:

```dockerfile
FROM rust:1.95-alpine3.20 AS builder

RUN apk add --no-cache ca-certificates musl-dev pkgconfig openssl-dev openssl-libs-static sqlite-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin relayd

FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates sqlite-libs
WORKDIR /app
COPY --from=builder /src/target/release/relayd /usr/local/bin/relayd

ENV HTTP_LISTEN=:8080 \
    PORT_RANGE=10000-30000 \
    SQLITE_PATH=/data/relayd.sqlite3

EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/relayd"]
```

If `rust:1.95-alpine3.20` is unavailable in CI/Docker, use `rust:1.95-alpine` and keep the runtime Alpine version stable.

- [ ] **Step 2: Update test workflow**

In `.github/workflows/test.yml`:

- Add `RUST_VERSION: 1.95` to env.
- Install Rust with `dtolnay/rust-toolchain@stable` and `toolchain: ${{ env.RUST_VERSION }}`.
- Add cache via `Swatinem/rust-cache@v2` if desired.
- Replace Zig-only format/test with:

```yaml
      - name: Rust format check
        run: cargo fmt -- --check

      - name: Rust tests
        run: cargo test --locked

      - name: Rust clippy
        run: cargo clippy --locked --lib --tests -- -D warnings

      - name: Rust release build
        run: cargo build --locked --release --bin relayd

      - name: Zig reference tests
        run: zig build test
```

Keep Zig setup because reference tests still run. The e2e bandwidth harness currently hardcodes `RELAYD_BIN="zig-out/bin/relayd"`; update `scripts/ci/e2e_iperf3.sh` to allow `RELAYD_BIN=${RELAYD_BIN:-zig-out/bin/relayd}`. Then the Rust-primary e2e lane can run:

```yaml
      - name: Build relayd
        run: cargo build --locked --release --bin relayd

      - name: Run e2e bandwidth harness
        run: RELAYD_BIN=target/release/relayd ./scripts/ci/e2e_iperf3.sh
```

If maintaining a Zig-reference e2e lane instead, keep `zig build` before the harness and label the job clearly as Zig reference. Do not call the harness after Rust cutover without setting `RELAYD_BIN` or building Zig.

- [ ] **Step 3: Update build workflow**

In `.github/workflows/build.yaml`:

- Rename typo `build bianry` to `build binary`.
- Make the main artifact build use Cargo release on Ubuntu.
- Upload `target/release/relayd` as a Rust Linux artifact, e.g. `relayd-x86_64-unknown-linux-gnu`.
- Keep Zig cross-arch artifacts only if explicitly labeled reference; otherwise remove the Zig matrix to avoid implying Zig is primary.

A minimal accepted workflow is one Linux artifact:

```yaml
name: build binary

env:
  RUST_VERSION: 1.95

# existing on/concurrency blocks

jobs:
  compile:
    name: compile-x86_64-unknown-linux-gnu
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: ${{ env.RUST_VERSION }}
      - uses: Swatinem/rust-cache@v2
      - run: cargo build --locked --release --bin relayd
      - uses: actions/upload-artifact@v4
        with:
          name: relayd-x86_64-unknown-linux-gnu
          path: target/release/relayd
```

- [ ] **Step 4: Docker workflow remains Buildx**

`.github/workflows/docker.yaml` can remain mostly unchanged because it delegates to Dockerfile. Ensure no Zig-specific build args are required.

## Task 4: Update README/API/operator docs

**Files:**
- Modify: `README.md`
- Modify: `docs/API.md` if needed
- Modify: `docs/api/http.md` and/or `docs/architecture/port-forwarder.md` if they still describe Zig-primary operation.
- Modify: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`

- [ ] **Step 1: Update README product/build/run language**

Change opening from Zig-primary to Rust-primary. Required README content:

```markdown
Linux-first Rust port-forwarder with:
- authenticated HTTP API
- SQLite persistence and startup restore
- TCP forwarding with default Tokio copy path
- UDP forwarding with default Tokio listener/session handling
- dual-protocol allocations (`both`) sharing one TCP+UDP port
- Prometheus listener metrics with per-scrape byte rates
```

Build/run sections:

```markdown
## Build
```bash
cargo build --locked --release --bin relayd
cargo test --locked
cargo clippy --locked --lib --tests -- -D warnings
zig build test # reference parity tests
```

## Run
```bash
HTTP_LISTEN=:8080 AUTH_TOKEN=mytoken target/release/relayd
```
```

Document that optional Zig feature-flag lanes remain deferred in Rust and that Zig source remains as reference.

- [ ] **Step 2: Update API docs only for implementation note**

If `docs/API.md` already has correct API semantics, add a short implementation note under `/metrics` that `protocol = both` allocations render separate concrete `tcp` and `udp` metric series with byte rates. Do not change endpoint shapes.

- [ ] **Step 3: Update architecture docs if Zig-primary wording exists**

Search:

```bash
rg -n "Zig|zig build|zig-out|Rust migration scaffold|splice fast-path|workerized" README.md docs .github Dockerfile src/bin/relayd.rs
```

Update only stale primary-operation language. Keep optional feature/deferred notes where relevant.

- [ ] **Step 4: Add M7 status to milestone ledger**

Append:

```markdown
## M7 implementation status

- Status: implemented Rust binary cutover with real startup, Rust-primary Docker/CI/docs, and Zig retained as parity/reference tests.
- Verification: `cargo fmt -- --check`; `cargo test --locked`; `cargo clippy --locked --lib --tests -- -D warnings`; `cargo build --locked --bin relayd`; `zig build test`.
- Scope note: Optional TCP/UDP feature-flag lanes remain deferred after main/default parity.
```

## Task 5: Final verification, review, commit, and push

**Files:**
- All M7 changed files

- [ ] **Step 1: Run full local verification**

Run:

```bash
cargo fmt -- --check
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings
cargo build --locked --bin relayd
zig build test
```

If Docker is available, run:

```bash
docker build -t relayd:rust-cutover .
```

- [ ] **Step 2: Request independent final reviews**

Dispatch:

1. Spec-compliance reviewer for M7 spec/plan and changed files.
2. Code-quality reviewer for startup, CI/Docker, docs, and test robustness.

Fix any `BLOCKED` items, rerun verification, and repeat reviews until both return `APPROVED`.

- [ ] **Step 3: Commit and push**

Commit with Lore protocol:

```bash
git add src/bin/relayd.rs src/http/control_plane.rs src/config.rs scripts/ci/e2e_iperf3.sh \
  README.md docs/API.md docs/api/http.md docs/architecture/port-forwarder.md \
  Dockerfile .github/workflows/test.yml .github/workflows/build.yaml .github/workflows/docker.yaml \
  docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md \
  docs/superpowers/specs/2026-05-15-rust-migration-m7-rust-binary-cutover-docs-docker-ci.md \
  docs/superpowers/plans/2026-05-15-rust-migration-m7-rust-binary-cutover-docs-docker-ci.md

git commit -m "Cut relayd over to the Rust runtime" \
  -m "Make the Rust binary start the real service and update Docker, CI, and operator docs so Cargo builds are the primary artifact while Zig remains a parity reference." \
  -m "Constraint: HTTP API/resource definitions and default runtime behavior must remain compatible with the Zig implementation." \
  -m "Rejected: Removing Zig source in M7 | Keeping Zig reference tests lowers cutover risk and preserves optional deferred lanes for later work." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Directive: Audit full migration parity before marking the overall migration goal complete." \
  -m "Tested: cargo fmt -- --check; TMPDIR=\$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked; TMPDIR=\$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings; cargo build --locked --bin relayd; zig build test" \
  -m "Not-tested: docker build if Docker is unavailable locally; optional TCP/UDP feature-flag lanes" \
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"

git push
```

- [ ] **Step 4: Start final migration audit gate**

After M7 is pushed, do not mark the goal complete yet. Create or run a final audit checklist covering M0-M7, API compatibility, verification commands, and deferred optional lanes. Only after audit approval should the overall migration be considered complete.
