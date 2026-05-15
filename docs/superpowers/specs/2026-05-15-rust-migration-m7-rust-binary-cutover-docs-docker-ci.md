# Rust Migration M7 Rust Binary Cutover, Docs, Docker, and CI Spec

## Goal

Make the Rust implementation the default `relayd` build/run artifact while preserving the Zig implementation as a parity/reference test target. The Rust binary must start the authenticated HTTP control plane, SQLite repository, startup restore, real TCP/UDP/both runtime, JSON metrics, and Prometheus metrics by default. README/API/operator docs, Dockerfile, and GitHub CI must use Rust tooling for the primary artifact while keeping Zig tests as a reference verification lane.

## Source requirements

- Complete the Zig-to-Rust migration with full main/default functionality parity.
- API interfaces and definitions must remain exactly aligned with the existing Zig HTTP API.
- Functionality hidden behind feature flags or optional fast-path environment gates may remain deferred and documented.
- This milestone must have a Superpowers-style spec and plan, plan review must return `APPROVED` before implementation, implementation must use Subagent Driven Development, final spec-compliance review must approve, docs must be updated, then commit and push.
- Do not call `update_goal` until all migration milestones are complete and audited.

## Source of truth

- Zig app startup/config/runtime orchestration: `src/main.zig`, `src/app.zig`.
- Rust config/model/repository/service/http/runtime modules: `src/config.rs`, `src/storage/sqlite.rs`, `src/service/allocation_service.rs`, `src/http/control_plane.rs`, `src/runtime/real.rs`.
- Cargo metadata: `Cargo.toml`, `Cargo.lock`.
- Existing operator docs: `README.md`, `docs/API.md`, `docs/api/http.md`, `docs/architecture/port-forwarder.md`.
- Current Docker and CI: `Dockerfile`, `.github/workflows/*.yml`, `.github/workflows/*.yaml`.
- Migration ledger: `docs/superpowers/specs/2026-05-15-rust-migration-milestones-design.md`.

## In scope

- Replace the Rust scaffold binary in `src/bin/relayd.rs` with a real Tokio main that:
  - Parses environment configuration using existing Rust config code (`Config::from_env_map(&HashMap<String, String>)`).
  - Opens SQLite repository using `SQLITE_PATH`.
  - Creates shared `Metrics`.
  - Creates `RealRuntime` with loopback/default runtime behavior.
  - Creates `Service<RealRuntime>` with configured `PORT_RANGE` and `RUNTIME_APPLY_TIMEOUT_MS`.
  - Runs startup restore with `RESTORE_SWEEP_TIMEOUT_MS`.
  - Starts the Axum HTTP router with bearer auth on configured `HTTP_LISTEN`.
  - Handles bind errors and startup errors with useful stderr messages and non-zero exit.
- Add any minimal library exports/helpers needed to start the server without changing API response shapes.
- Update Dockerfile to build the Rust binary via Cargo and copy it as `/usr/local/bin/relayd`.
- Update GitHub CI to make Rust fmt/test/clippy/build the primary checks and keep `zig build test` as a parity/reference check.
- Update build artifact workflow to upload Rust Linux binaries. Cross-compile may be limited to targets available through stable Rust/Cargo in CI; if musl cross builds are too large for M7, use native `x86_64-unknown-linux-gnu` and document any deferred cross-arch packaging.
- Update README build/run instructions from Zig-primary to Rust-primary, documenting Zig as reference and optional feature-flag lanes as deferred/non-default.
- Ensure API docs still describe unchanged HTTP interfaces and Prometheus behavior.
- Add smoke/integration tests for Rust app startup where feasible without binding external production resources.

## Out of scope

- Porting optional feature-flag lanes: TCP session-model/splice, UDP worker/io_uring/GRO/dataplane/fast-path.
- Removing Zig source entirely; Zig may remain as reference code and `zig build test` verification.
- Full release engineering for all prior Zig artifact names/targets if Cargo cross-compilation is not already configured.
- External production deployment or publishing images outside normal CI definitions.

## Required runtime behavior

### Rust binary startup

The Rust `relayd` binary must be runnable with:

```bash
HTTP_LISTEN=:8080 AUTH_TOKEN=mytoken cargo run --locked --bin relayd
```

It must bind the HTTP API to `127.0.0.1:8080` for `:8080`, use the configured SQLite path, restore persisted allocations, and serve all M2-M6 endpoints. Missing/empty `AUTH_TOKEN`, invalid `HTTP_LISTEN`, invalid `PORT_RANGE`, SQLite open errors, restore errors, or HTTP bind errors must fail startup with non-zero exit.

### HTTP serving

The server must use the existing `http::control_plane::router(AppState::new(...))` and preserve auth/content-type/error behavior. Shutdown behavior may be Ctrl-C driven; deterministic test shutdown can use Tokio task abort if no graceful shutdown helper exists.

### Packaging and CI

Docker and CI must invoke Cargo for the primary binary. CI must run:

- `cargo fmt -- --check`
- `cargo test --locked`
- `cargo clippy --locked --lib --tests -- -D warnings`
- `cargo build --locked --release --bin relayd`
- `zig build test`

Docker runtime image must execute the Rust binary by default with the same environment variables and exposed HTTP port. If dynamically linked against SQLite on Alpine, runtime image must install `sqlite-libs`.

## Acceptance criteria

- M7 plan is independently reviewed to `APPROVED` before implementation.
- Rust binary is no longer a scaffold; it starts the real service with Rust config, repository, service, real runtime, restore, and HTTP router.
- A Rust startup smoke test verifies the real startup composition path can parse env/config, open temporary SQLite, create `RealRuntime`, create `Service`, run `restore_all`, start the router on loopback, and serve an authenticated request.
- README build/run instructions use Cargo/Rust as primary and identify Zig as reference/parity testing.
- Dockerfile builds and packages Rust `relayd` and includes required runtime shared libraries such as `sqlite-libs` when dynamically linked.
- GitHub workflows run Rust fmt/test/clippy/build and retain `zig build test`; any e2e harness lane must either run the Rust artifact via a configurable `RELAYD_BIN` or remain explicitly Zig-reference with a Zig build.
- API docs remain unchanged in interface semantics and mention that `both` metrics emit concrete TCP/UDP series with rates.
- Fresh verification passes locally: `cargo fmt -- --check`; `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo test --locked`; `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target cargo clippy --locked --lib --tests -- -D warnings`; `cargo build --locked --bin relayd`; `zig build test`.
- If Docker build is changed, `docker build` should be run when Docker is available; if unavailable, document `Not-tested`.
- Final independent spec-compliance review returns `APPROVED` before commit/push.
- After M7 push, run or plan a final migration audit before marking the overall migration goal complete.
