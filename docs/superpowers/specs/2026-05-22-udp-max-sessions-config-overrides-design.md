# UDP Max Sessions Config Overrides Design

## Status

Draft for `$sdd-workflow` independent review on 2026-05-22.

## Goal

Allow operators to override the UDP per-listener maximum active session count from environment variables or command-line arguments while preserving the current default of `65_536` sessions per UDP relay port.

## Context

The previous UDP max-session change raised `UdpRuntimeConfig::with_bind_host`'s default `max_sessions` from `4_096` to `65_536` and added a regression test in `src/runtime/udp.rs`. That cap is copied into every `ListenerEntry`, and `UdpRuntime::session_for` rejects new client sessions when `sessions.len() >= entry.max_sessions`.

The Rust binary already has a configuration flow where CLI options override matching environment variables:

1. `src/bin/relayd.rs` parses `Cli` with clap.
2. `Cli::apply_to_env` overlays present CLI values into a mutable environment map.
3. `Config::from_env_map` in `src/config.rs` parses typed settings and defaults.
4. `run_with_listener` builds `RealRuntimeConfig::with_bind_host(...)`, and `RealRuntime::new` builds `UdpRuntimeConfig::with_bind_host(...)`.

## Scope

In scope:

- Add a `Config` field for the UDP per-listener max sessions setting.
- Add an environment variable named `UDP_MAX_SESSIONS` with default `65_536`.
- Add a CLI argument named `--udp-max-sessions <COUNT>` that overrides `UDP_MAX_SESSIONS` using the existing CLI-over-env overlay pattern.
- Thread the parsed value through `RealRuntimeConfig` into `UdpRuntimeConfig` so all newly created/restored UDP listeners use the configured cap.
- Preserve `65_536` as the default when neither env nor CLI is supplied.
- Reject invalid non-integer values through the existing `ConfigError::InvalidInteger` path.
- Reject `0` as an invalid session cap, because a zero cap would make UDP forwarding unable to create any client session.
- Update README usage documentation.
- Add focused regression tests for default, env override, CLI override, invalid integer, zero rejection, `RealRuntimeConfig` propagation, and direct `UdpRuntimeConfig` builder override.

Out of scope:

- Changing the cap-enforcement condition or session-map data structure.
- Changing TCP session limits or TCP compatibility fields.
- Adding API endpoints, metrics labels, runtime reconfiguration, or per-allocation overrides.
- Rewriting historical spec/plan documents that intentionally described the earlier default-only change.

## Design

### Configuration parsing

`Config` gains:

```rust
pub udp_max_sessions: usize,
```

`Config::from_env_map` parses it with a helper equivalent to the existing `env_u32` helper but returning `usize` and rejecting zero. The env variable name is `UDP_MAX_SESSIONS`; its default is `65_536`.

Invalid examples:

- `UDP_MAX_SESSIONS=bad` returns `ConfigError::InvalidInteger("UDP_MAX_SESSIONS")`.
- `UDP_MAX_SESSIONS=0` returns `ConfigError::InvalidInteger("UDP_MAX_SESSIONS")`.

### CLI override

`Cli` gains:

```rust
#[arg(
    long,
    value_name = "COUNT",
    help = "Maximum active UDP sessions per relay port (env: UDP_MAX_SESSIONS). Default: 65536."
)]
udp_max_sessions: Option<String>,
```

`Cli::apply_to_env` inserts `UDP_MAX_SESSIONS` when the CLI option is present. This preserves the current CLI-over-env behavior and lets clap handle only presence/string capture while `Config::from_env_map` remains the validation source of truth.

### Runtime propagation

`RealRuntimeConfig` gains an `udp_max_sessions: usize` field initialized to `65_536` by `with_bind_host`. It exposes a builder method:

```rust
pub fn with_udp_max_sessions(mut self, max_sessions: usize) -> Self
```

`UdpRuntimeConfig` gains the same builder method, assigning `self.max_sessions = max_sessions`.

`run_with_listener` constructs the runtime with:

```rust
RealRuntimeConfig::with_bind_host(config.proxy_listen_host.clone(), metrics.clone())
    .with_udp_max_sessions(config.udp_max_sessions)
```

`RealRuntime::new` applies the value when constructing `UdpRuntimeConfig`:

```rust
UdpRuntimeConfig::with_bind_host(config.bind_host, config.metrics.clone())
    .with_session_ttl(config.udp_session_ttl)
    .with_udp_max_sessions(config.udp_max_sessions)
```

Validation occurs before runtime construction, so the builder methods do not need to return `Result`.

## Acceptance Criteria

- Default behavior remains `65_536` UDP max sessions per listener when no override is supplied.
- `UDP_MAX_SESSIONS=<COUNT>` changes the parsed config value and the runtime config value used by new UDP listeners.
- `--udp-max-sessions <COUNT>` overrides `UDP_MAX_SESSIONS` using the same precedence as existing CLI options.
- `UDP_MAX_SESSIONS=bad` and `UDP_MAX_SESSIONS=0` fail config parsing with `ConfigError::InvalidInteger("UDP_MAX_SESSIONS")`.
- `RealRuntimeConfig` propagates the configured value into `UdpRuntimeConfig`.
- Existing UDP cap enforcement remains `sessions.len() >= entry.max_sessions`.
- README documents `UDP_MAX_SESSIONS` and the CLI flag.
- Fresh verification passes:
  - `cargo test --locked config::tests::config_from_env_map_defaults_udp_max_sessions_to_65536`
  - `cargo test --locked config::tests::config_from_env_map_parses_udp_max_sessions_override`
  - `cargo test --locked config::tests::config_from_env_map_rejects_invalid_udp_max_sessions`
  - `cargo test --locked --bin relayd cli_help_documents_runtime_options`
  - `cargo test --locked --bin relayd cli_options_override_environment_config`
  - `cargo test --locked runtime::udp::tests::udp_runtime_config_accepts_custom_max_sessions`
  - `cargo test --locked runtime::real::tests::real_runtime_config_propagates_udp_max_sessions`
  - `cargo test --locked runtime::udp`
  - `cargo fmt -- --check`
  - `cargo clippy --locked --lib --tests -- -D warnings`
  - `git diff --check`
