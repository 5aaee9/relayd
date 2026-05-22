# UDP Forward Source Bind Host Design

## Status

Created for `$sdd-workflow` after the user requested modifying the behavior introduced by commit `a0a27b99be377e7215d858c3b3f062e4a7a19241`.

This workflow will create a new corrective commit instead of rewriting already-pushed history.

## Goal

Make UDP forwarding session sockets use the configured relay bind host (`PROXY_LISTEN_HOST`) instead of a hard-coded `0.0.0.0` source bind.

## Context

Commit `a0a27b99be377e7215d858c3b3f062e4a7a19241` changed UDP per-client upstream sockets from binding `127.0.0.1:0` to binding `0.0.0.0:0` so UDP relays could reach non-loopback upstream targets.

Current startup configuration already builds the real runtime with `config.proxy_listen_host`:

```rust
RealRuntime::new(RealRuntimeConfig::with_bind_host(
    config.proxy_listen_host.clone(),
    metrics.clone(),
))
```

`RealRuntime` passes that bind host to both `TcpRuntimeConfig::with_bind_host` and `UdpRuntimeConfig::with_bind_host`, so UDP relay listener sockets already use `PROXY_LISTEN_HOST`. The remaining hard-coded value from the target commit is the UDP forwarding/upstream session socket helper:

```rust
async fn bind_upstream_socket() -> std::io::Result<UdpSocket> {
    UdpSocket::bind(("0.0.0.0", 0)).await
}
```

## Scope

In scope:

- Change UDP per-client forwarding/upstream session socket binding to use `UdpRuntimeConfig.bind_host`, which is supplied from `PROXY_LISTEN_HOST` in the production `relayd` startup path.
- Preserve current default production behavior when `PROXY_LISTEN_HOST` is unset, because config defaults it to `0.0.0.0`.
- Update tests that currently assert hard-coded unspecified upstream binding so they assert configured bind-host behavior instead.
- Add an end-to-end UDP runtime regression test proving `UdpRuntimeConfig::with_bind_host("127.0.0.2", ...)` flows through the listener entry into per-client forwarding sessions by checking the upstream capture server observes source IP `127.0.0.2`. Use `127.0.0.2` because hard-coded `0.0.0.0` would still appear as `127.0.0.1` when targeting loopback and would not prove the configured bind host was used.
- Add or adjust operator-facing documentation/help to state that `PROXY_LISTEN_HOST` controls TCP/UDP relay listener binding and UDP forwarding session source binding anywhere README or CLI help explains relay listener binding.

Out of scope:

- Rewriting the historical commit `a0a27b99be377e7215d858c3b3f062e4a7a19241` or force-pushing history.
- Changing `PROXY_LISTEN_HOST` parsing/defaults.
- Changing TCP runtime behavior except operator-facing CLI help text shared by TCP/UDP relay bind host.
- Changing UDP target host validation, API shapes, metrics names, session TTL, max sessions, cleanup behavior, or dual-protocol orchestration.
- Adding a new CLI flag or environment variable for UDP upstream source binding.

## Design

`UdpRuntimeConfig` already stores a private `bind_host: String`. `bind_entry` uses it to bind the UDP relay listener. The forwarding session path should reuse the same configured host rather than using a static helper with hard-coded `0.0.0.0`.

Implementation options:

1. Add `bind_host: String` to `ListenerEntry`, copy `self.config.bind_host.clone()` into each entry during `bind_entry`, and have `create_session` bind upstream sockets through `entry.bind_host`.
2. Pass the configured bind host through `session_for` into `create_session` for every datagram.

Use option 1. It keeps session creation self-contained around the listener entry that owns the relay port and avoids passing another parameter through the receive path on every packet.

The upstream bind helper should accept a host string and parse it as an IP literal before binding an ephemeral UDP socket:

```rust
async fn bind_upstream_socket(bind_host: &str) -> std::io::Result<UdpSocket>
```

Because `Config::from_env_map` already validates `PROXY_LISTEN_HOST` as an IP literal, production should not pass invalid values. For direct `UdpRuntimeConfig::with_bind_host` unit-test construction with invalid values, binding should fail and the existing UDP send/drop error path should handle it.

## Acceptance Criteria

- UDP forwarding/upstream session sockets bind to the configured `UdpRuntimeConfig.bind_host` rather than hard-coded `0.0.0.0`.
- Production default remains equivalent when `PROXY_LISTEN_HOST` is unset (`0.0.0.0`).
- A helper-level regression test proves `bind_upstream_socket("127.0.0.2")` creates an upstream socket bound to `127.0.0.2`, and preserves `0.0.0.0` when configured.
- An end-to-end UDP runtime test proves `UdpRuntimeConfig::with_bind_host("127.0.0.2", ...)` is used by actual forwarded sessions: a UDP capture target receives a datagram from source IP `127.0.0.2`.
- The old test that expected hard-coded unspecified binding is removed or updated.
- Existing UDP forwarding tests continue passing.
- README and CLI help document the `PROXY_LISTEN_HOST` scope clearly enough for operators.
- No Cargo dependency or lockfile changes are introduced.
- Fresh verification passes:
  - targeted new/updated UDP upstream bind-host test
  - targeted CLI help test
  - `cargo test --locked runtime::udp`
  - `cargo test --locked runtime::real`
  - `cargo fmt -- --check`
  - `cargo clippy --locked --lib --tests -- -D warnings`
  - `git diff --check`

## Review Notes

The end-to-end source-bind regression must not use `127.0.0.1` as the configured bind host because a hard-coded `0.0.0.0` upstream socket can still appear as `127.0.0.1` when sending to a loopback target. Use `127.0.0.2` so the observed upstream peer IP distinguishes the configured bind host from unspecified binding.
