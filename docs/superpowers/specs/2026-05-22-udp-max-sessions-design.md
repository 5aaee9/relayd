# UDP Max Sessions Design

## Status

Approved by user on 2026-05-22 for `$sdd-workflow` execution.

## Goal

Raise the default maximum number of active UDP sessions for a single relay listener/port from 4,096 to 65,536.

## Scope

In scope:

- Change the default `UdpRuntimeConfig` per-listener `max_sessions` value in `src/runtime/udp.rs` from `4096` to `65_536`.
- Add a focused regression test that locks the default at `65_536` so future changes do not silently lower it.
- Run targeted UDP tests and standard formatting/lint verification.

Out of scope:

- Adding a new CLI flag or environment variable for UDP max sessions.
- Changing TCP session limits or compatibility-only TCP session config.
- Changing UDP TTL, cleanup behavior, session map structure, metrics names, or API responses.
- Rewriting historical planning documents that mention the previous default.

## Design

`UdpRuntimeConfig::with_bind_host` currently initializes the per-listener `max_sessions` field with `4096`. Each `ListenerEntry` copies that config value into its own `max_sessions`, and `session_for` rejects new clients once `sessions.len() >= entry.max_sessions`.

The implementation should update only the default to `65_536`:

```rust
max_sessions: 65_536,
```

A lightweight unit test in `src/runtime/udp.rs` should instantiate `UdpRuntimeConfig::loopback(Arc::new(Metrics::default()))` and assert `config.max_sessions == 65_536`. The test lives in the existing `#[cfg(test)] mod tests`, which can access private fields, so no production accessor is needed.

## Acceptance Criteria

- `src/runtime/udp.rs` default UDP per-listener session cap is `65_536`.
- The existing cap enforcement remains in place and still compares `sessions.len() >= entry.max_sessions`.
- A regression test fails if the default is lowered from `65_536`.
- No public configuration surface, docs, Cargo dependencies, or lockfile changes are introduced.
- Fresh verification passes:
  - `cargo test --locked runtime::udp::tests::udp_runtime_config_defaults_to_65536_max_sessions`
  - `cargo test --locked runtime::udp`
  - `cargo fmt -- --check`
  - `cargo clippy --locked --lib --tests -- -D warnings`
  - `git diff --check`
