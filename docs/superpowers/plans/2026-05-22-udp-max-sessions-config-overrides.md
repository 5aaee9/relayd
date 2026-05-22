# UDP Max Sessions Config Overrides Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Let operators override the UDP per-listener max active sessions with `UDP_MAX_SESSIONS` or `--udp-max-sessions` while preserving the `65_536` default.

**Architecture:** Keep `src/config.rs` as the validation source of truth, keep `src/bin/relayd.rs` as the CLI-over-env overlay, then thread `Config.udp_max_sessions` through `RealRuntimeConfig` into `UdpRuntimeConfig`. The UDP runtime enforcement stays unchanged and continues to use each `ListenerEntry.max_sessions`.

**Tech Stack:** Rust 1.95, clap derive, Tokio runtime tests, existing `cargo test --locked`, `cargo fmt`, `cargo clippy`.

---

## Files

- Modify: `src/config.rs` — add parsed `udp_max_sessions`, default/override tests, invalid/zero validation.
- Modify: `src/bin/relayd.rs` — add `--udp-max-sessions`, overlay it into `UDP_MAX_SESSIONS`, thread into runtime, update CLI tests.
- Modify: `src/runtime/udp.rs` — add `UdpRuntimeConfig::with_udp_max_sessions`, a crate-visible `UdpRuntime::max_sessions()` inspection helper, and test custom value.
- Modify: `src/runtime/real.rs` — add `RealRuntimeConfig.udp_max_sessions`, builder, propagation test.
- Modify: `README.md` — document env and CLI override.
- Modify: `docs/superpowers/plans/2026-05-22-udp-max-sessions-config-overrides.md` — check off tasks as they complete.

## Task 1: Add config parsing for `UDP_MAX_SESSIONS`

**Files:**
- Modify: `src/config.rs`

- [x] **Step 1: Write failing config tests**

Add these tests inside `#[cfg(test)] mod tests` in `src/config.rs`:

```rust
#[test]
fn config_from_env_map_defaults_udp_max_sessions_to_65536() {
    let cfg = Config::from_env_map(&env_with_token()).unwrap();
    assert_eq!(cfg.udp_max_sessions, 65_536);
}

#[test]
fn config_from_env_map_parses_udp_max_sessions_override() {
    let mut env = env_with_token();
    env.insert("UDP_MAX_SESSIONS".to_owned(), "12345".to_owned());
    let cfg = Config::from_env_map(&env).unwrap();
    assert_eq!(cfg.udp_max_sessions, 12_345);
}

#[test]
fn config_from_env_map_rejects_invalid_udp_max_sessions() {
    let mut env = env_with_token();
    env.insert("UDP_MAX_SESSIONS".to_owned(), "bad".to_owned());
    assert!(matches!(
        Config::from_env_map(&env),
        Err(ConfigError::InvalidInteger("UDP_MAX_SESSIONS"))
    ));

    let mut env = env_with_token();
    env.insert("UDP_MAX_SESSIONS".to_owned(), "0".to_owned());
    assert!(matches!(
        Config::from_env_map(&env),
        Err(ConfigError::InvalidInteger("UDP_MAX_SESSIONS"))
    ));
}
```

- [x] **Step 2: Run the new tests to confirm they fail before implementation**

Run:

```bash
cargo test --locked config::tests::config_from_env_map_defaults_udp_max_sessions_to_65536
cargo test --locked config::tests::config_from_env_map_parses_udp_max_sessions_override
cargo test --locked config::tests::config_from_env_map_rejects_invalid_udp_max_sessions
```

Expected: each new focused test FAILS before implementation because `Config` does not yet expose `udp_max_sessions`.

- [x] **Step 3: Implement config field and parser**

In `Config`, add:

```rust
pub udp_max_sessions: usize,
```

In `Config::from_env_map`, near the existing UDP fields, add:

```rust
udp_max_sessions: env_nonzero_usize(env, "UDP_MAX_SESSIONS", 65_536)?,
```

Add helper near `env_u32`:

```rust
fn env_nonzero_usize(
    env: &HashMap<String, String>,
    name: &'static str,
    default_value: usize,
) -> Result<usize, ConfigError> {
    match env.get(name) {
        Some(value) => value
            .parse::<usize>()
            .ok()
            .filter(|parsed| *parsed > 0)
            .ok_or(ConfigError::InvalidInteger(name)),
        None => Ok(default_value),
    }
}
```

Update the literal `Config { ... }` in `config_surface_does_not_expose_tcp_splice_activation_flags` to include:

```rust
udp_max_sessions: 65_536,
```

- [x] **Step 4: Run config tests**

Run:

```bash
cargo test --locked config::tests::config_from_env_map_defaults_udp_max_sessions_to_65536
cargo test --locked config::tests::config_from_env_map_parses_udp_max_sessions_override
cargo test --locked config::tests::config_from_env_map_rejects_invalid_udp_max_sessions
cargo test --locked config::tests::config_from_env_map_applies_defaults_and_requires_auth_token
```

Expected: PASS.

## Task 2: Add CLI override and startup wiring

**Files:**
- Modify: `src/bin/relayd.rs`

- [x] **Step 1: Write failing CLI and startup-wiring test updates**

In `cli_help_documents_runtime_options`, add assertions:

```rust
assert!(help.contains("--udp-max-sessions <COUNT>"));
assert!(help.contains("env: UDP_MAX_SESSIONS"));
```

In `cli_options_override_environment_config`, add CLI args before the closing `])`:

```rust
"--udp-max-sessions",
"777",
```

Add env baseline value:

```rust
("UDP_MAX_SESSIONS".to_owned(), "333".to_owned()),
```

Add final assertion:

```rust
assert_eq!(config.udp_max_sessions, 777);
```

Add this focused startup-wiring test in `src/bin/relayd.rs` tests before running it:

```rust
#[test]
fn runtime_config_from_config_carries_udp_max_sessions() {
    let mut env = HashMap::from([
        ("AUTH_TOKEN".to_owned(), "secret-token".to_owned()),
        ("UDP_MAX_SESSIONS".to_owned(), "444".to_owned()),
    ]);
    let config = Config::from_env_map(&env).unwrap();
    let runtime_config = real_runtime_config_from_config(&config, Arc::new(Metrics::default()));

    assert_eq!(runtime_config.udp_max_sessions(), 444);

    env.insert("UDP_MAX_SESSIONS".to_owned(), "555".to_owned());
    let config = Config::from_env_map(&env).unwrap();
    let runtime_config = real_runtime_config_from_config(&config, Arc::new(Metrics::default()));

    assert_eq!(runtime_config.udp_max_sessions(), 555);
}
```

- [x] **Step 2: Run CLI tests to confirm they fail before implementation**

Run:

```bash
cargo test --locked --bin relayd cli_help_documents_runtime_options
cargo test --locked --bin relayd cli_options_override_environment_config
cargo test --locked --bin relayd runtime_config_from_config_carries_udp_max_sessions
```

Expected: the help and override tests FAIL because the CLI/config option is missing; the startup-wiring test FAILS until the helper and runtime getter exist.

- [x] **Step 3: Implement CLI option and overlay**

In `Cli`, add near the other UDP options:

```rust
#[arg(
    long,
    value_name = "COUNT",
    help = "Maximum active UDP sessions per relay port (env: UDP_MAX_SESSIONS). Default: 65536."
)]
udp_max_sessions: Option<String>,
```

In `Cli::apply_to_env`, add:

```rust
insert_if_present(env, "UDP_MAX_SESSIONS", self.udp_max_sessions);
```

- [x] **Step 4: Add a testable startup runtime-config helper**

Add this helper near `run_with_listener`:

```rust
fn real_runtime_config_from_config(config: &Config, metrics: Arc<Metrics>) -> RealRuntimeConfig {
    RealRuntimeConfig::with_bind_host(config.proxy_listen_host.clone(), metrics)
        .with_udp_max_sessions(config.udp_max_sessions)
}
```

In `run_with_listener`, replace direct construction with:

```rust
let runtime = RealRuntime::new(real_runtime_config_from_config(&config, metrics.clone()));
```

The `runtime_config_from_config_carries_udp_max_sessions` test from Step 1 proves the startup path helper used by `run_with_listener` carries the parsed config value into `RealRuntimeConfig`; Task 3 adds the `udp_max_sessions()` getter.

- [x] **Step 5: Run CLI/startup tests after Task 3 runtime support**

Run:

```bash
cargo test --locked --bin relayd cli_help_documents_runtime_options
cargo test --locked --bin relayd cli_options_override_environment_config
cargo test --locked --bin relayd runtime_config_from_config_carries_udp_max_sessions
```

Expected: if run immediately after Task 2 Step 4 and before Task 3, these may still fail only because `RealRuntimeConfig::with_udp_max_sessions` or `udp_max_sessions()` is missing. After Task 3 runtime config support is implemented, rerun these commands and they must PASS.

## Task 3: Add runtime config propagation

**Files:**
- Modify: `src/runtime/udp.rs`
- Modify: `src/runtime/real.rs`

- [x] **Step 1: Write failing runtime tests**

In `src/runtime/udp.rs` tests, add:

```rust
#[test]
fn udp_runtime_config_accepts_custom_max_sessions() {
    let config = UdpRuntimeConfig::loopback(Arc::new(Metrics::default()))
        .with_udp_max_sessions(123);

    assert_eq!(config.max_sessions, 123);
}
```

In `src/runtime/real.rs` tests, add:

```rust
#[test]
fn real_runtime_config_propagates_udp_max_sessions() {
    let config = RealRuntimeConfig::loopback(Arc::new(Metrics::default()))
        .with_udp_max_sessions(321);
    let runtime = RealRuntime::new(config);

    assert_eq!(runtime.udp.max_sessions(), 321);
}
```

This test uses the crate-visible `UdpRuntime::max_sessions()` helper added in Step 3, avoiding access to private sibling-module fields.

- [x] **Step 2: Run runtime tests to confirm they fail before implementation**

Run:

```bash
cargo test --locked runtime::udp::tests::udp_runtime_config_accepts_custom_max_sessions
cargo test --locked runtime::real::tests::real_runtime_config_propagates_udp_max_sessions
```

Expected: each test FAILS because builder/field propagation is missing.

- [x] **Step 3: Implement `UdpRuntimeConfig` builder and runtime inspection helper**

In `impl UdpRuntimeConfig`, add:

```rust
pub fn with_udp_max_sessions(mut self, max_sessions: usize) -> Self {
    self.max_sessions = max_sessions;
    self
}
```

In `impl UdpRuntime`, add a crate-visible getter for tests and same-crate runtime wiring checks:

```rust
pub(crate) fn max_sessions(&self) -> usize {
    self.config.max_sessions
}
```

- [x] **Step 4: Implement `RealRuntimeConfig` propagation**

In `RealRuntimeConfig`, add field:

```rust
udp_max_sessions: usize,
```

Initialize it in `with_bind_host`:

```rust
udp_max_sessions: 65_536,
```

Add builder in `impl RealRuntimeConfig`:

```rust
pub fn with_udp_max_sessions(mut self, max_sessions: usize) -> Self {
    self.udp_max_sessions = max_sessions;
    self
}

pub fn udp_max_sessions(&self) -> usize {
    self.udp_max_sessions
}
```

In `RealRuntime::new`, update UDP config chain:

```rust
UdpRuntimeConfig::with_bind_host(config.bind_host, config.metrics.clone())
    .with_session_ttl(config.udp_session_ttl)
    .with_udp_max_sessions(config.udp_max_sessions),
```

- [x] **Step 5: Run runtime tests**

Run:

```bash
cargo test --locked runtime::udp::tests::udp_runtime_config_defaults_to_65536_max_sessions
cargo test --locked runtime::udp::tests::udp_runtime_config_accepts_custom_max_sessions
cargo test --locked runtime::real::tests::real_runtime_config_propagates_udp_max_sessions
cargo test --locked --bin relayd runtime_config_from_config_carries_udp_max_sessions
```

Expected: PASS.

## Task 4: Update README docs

**Files:**
- Modify: `README.md`

- [x] **Step 1: Document env variable**

In the Env list, add:

```markdown
- `UDP_MAX_SESSIONS` — maximum active UDP sessions per relay port, default `65536`
```

- [x] **Step 2: Document CLI usage**

In the Logging/Run area or near existing CLI example, ensure the text states that CLI options override matching env vars and include `--udp-max-sessions` in a sample command, for example:

```markdown
CLI options override matching environment variables; for example `--udp-max-sessions 131072` overrides `UDP_MAX_SESSIONS` for the current process.
```

- [x] **Step 3: Check docs diff**

Run:

```bash
git diff -- README.md
```

Expected: README mentions both `UDP_MAX_SESSIONS` and `--udp-max-sessions`.

## Task 5: Full verification and cleanup

**Files:**
- Inspect all changed files

- [x] **Step 1: Run targeted tests**

Run:

```bash
cargo test --locked config::tests::config_from_env_map_defaults_udp_max_sessions_to_65536
cargo test --locked config::tests::config_from_env_map_parses_udp_max_sessions_override
cargo test --locked config::tests::config_from_env_map_rejects_invalid_udp_max_sessions
cargo test --locked --bin relayd cli_help_documents_runtime_options
cargo test --locked --bin relayd cli_options_override_environment_config
cargo test --locked --bin relayd runtime_config_from_config_carries_udp_max_sessions
cargo test --locked runtime::udp::tests::udp_runtime_config_accepts_custom_max_sessions
cargo test --locked runtime::real::tests::real_runtime_config_propagates_udp_max_sessions
cargo test --locked runtime::udp
```

Expected: all pass.

- [x] **Step 2: Run standard checks**

Run:

```bash
cargo fmt -- --check
cargo clippy --locked --lib --tests -- -D warnings
git diff --check
```

Expected: all pass.

- [x] **Step 3: Inspect final diff**

Run:

```bash
git diff -- src/config.rs src/bin/relayd.rs src/runtime/udp.rs src/runtime/real.rs README.md docs/superpowers/specs/2026-05-22-udp-max-sessions-config-overrides-design.md docs/superpowers/plans/2026-05-22-udp-max-sessions-config-overrides.md
```

Expected: diff only contains spec/plan plus requested config/runtime/docs changes.
