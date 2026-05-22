# UDP Forward Source Bind Host Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make UDP forwarding/upstream session sockets bind to the configured `PROXY_LISTEN_HOST` value instead of a hard-coded `0.0.0.0` source address.

**Architecture:** `UdpRuntimeConfig.bind_host` already controls UDP relay listener binding and is populated from `PROXY_LISTEN_HOST` in production via `RealRuntimeConfig::with_bind_host`. Copy that bind host into each `ListenerEntry` and use it when creating per-client UDP upstream sockets. Verify both the helper and the actual config→listener-entry→session path with tests.

**Tech Stack:** Rust 1.95, Tokio UDP sockets, existing `RuntimeFacade`, existing config/README docs, Cargo tests.

---

## File Structure

- Modify: `src/runtime/udp.rs` — carry configured bind host into listener entries, use it for upstream session socket binds, update the helper regression test, and add an end-to-end UDP forwarding source-bind test.
- Modify: `src/bin/relayd.rs` — update CLI help text and its help-output test for `--proxy-listen-host`.
- Modify: `README.md` — clarify every current `PROXY_LISTEN_HOST` explanation that discusses relay binding so it also covers UDP forwarding session source binding.
- Existing reviewed spec: `docs/superpowers/specs/2026-05-22-udp-forward-source-bind-host-design.md`.
- No changes to `Cargo.toml`, `Cargo.lock`, runtime semantics outside UDP upstream socket binding, config parsing/defaults, API docs, CI workflows, or historical commit rewriting.

## Task 1: Route UDP upstream source binds through the configured bind host

**Files:**
- Modify: `src/runtime/udp.rs`

- [ ] **Step 1: Add bind_host to ListenerEntry**

In `struct ListenerEntry`, add a field immediately after `port: u16`:

```rust
    bind_host: String,
```

- [ ] **Step 2: Store the runtime config bind host when creating entries**

In `UdpRuntime::bind_entry`, when constructing `ListenerEntry`, add:

```rust
            bind_host: self.config.bind_host.clone(),
```

immediately after `port: allocation.port,`.

- [ ] **Step 3: Change the upstream bind helper signature**

Replace:

```rust
    async fn bind_upstream_socket() -> std::io::Result<UdpSocket> {
        UdpSocket::bind(("0.0.0.0", 0)).await
    }
```

with:

```rust
    async fn bind_upstream_socket(bind_host: &str) -> std::io::Result<UdpSocket> {
        UdpSocket::bind((bind_host, 0)).await
    }
```

Do not add a new public config field or change `Config::from_env_map`; production already validates `PROXY_LISTEN_HOST` as an IP literal.

- [ ] **Step 4: Use the listener entry bind host when creating sessions**

In `create_session`, replace:

```rust
        let upstream = match Self::bind_upstream_socket().await {
```

with:

```rust
        let upstream = match Self::bind_upstream_socket(&entry.bind_host).await {
```

Keep existing send/drop error handling unchanged.

- [ ] **Step 5: Update the helper-level upstream-bind regression test**

Replace the current test `udp_upstream_socket_binds_unspecified_addr_for_non_loopback_targets` with:

```rust
    #[tokio::test]
    async fn udp_upstream_socket_uses_configured_bind_host() {
        let loopback = UdpRuntime::bind_upstream_socket("127.0.0.2")
            .await
            .unwrap();
        assert_eq!(
            loopback.local_addr().unwrap().ip(),
            std::net::Ipv4Addr::new(127, 0, 0, 2)
        );

        let unspecified = UdpRuntime::bind_upstream_socket("0.0.0.0")
            .await
            .unwrap();
        assert_eq!(
            unspecified.local_addr().unwrap().ip(),
            std::net::Ipv4Addr::UNSPECIFIED
        );
    }
```

- [ ] **Step 6: Add an end-to-end session source-bind regression test**

Add this test near the existing UDP forwarding tests:

```rust
    #[tokio::test]
    async fn udp_runtime_upstream_sessions_use_configured_bind_host() {
        let metrics = Arc::new(Metrics::default());
        let runtime = UdpRuntime::new(UdpRuntimeConfig::with_bind_host(
            "127.0.0.2",
            metrics.clone(),
        ));
        let relay_port = free_udp_port().await;
        let capture = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let target_port = capture.local_addr().unwrap().port();

        runtime
            .create(&allocation("alloc-source-bind", relay_port, None, None), 500)
            .await
            .unwrap();
        runtime
            .update(
                &allocation(
                    "alloc-source-bind",
                    relay_port,
                    Some(target_port),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();

        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        client
            .send_to(b"source-bind", ("127.0.0.2", relay_port))
            .await
            .unwrap();

        let mut buf = [0_u8; 64];
        let (n, peer) = timeout(Duration::from_millis(500), capture.recv_from(&mut buf))
            .await
            .expect("capture target did not receive forwarded datagram")
            .unwrap();

        assert_eq!(&buf[..n], b"source-bind");
        assert_eq!(peer.ip(), std::net::Ipv4Addr::new(127, 0, 0, 2));
        assert_eq!(metrics.udp_session_create_total.load(), 1);

        runtime.delete("alloc-source-bind", 500).await.unwrap();
    }
```

This proves `UdpRuntimeConfig::with_bind_host("127.0.0.2", ...)` flows through actual runtime entry/session creation, not just the helper. The client sends to the relay listener on `127.0.0.2`; the upstream capture target remains on `127.0.0.1`; and the distinct loopback alias avoids a false positive where hard-coded `0.0.0.0` appears as `127.0.0.1` when targeting loopback.

- [ ] **Step 7: Run targeted tests**

Run:

```bash
cargo test --locked runtime::udp::tests::udp_upstream_socket_uses_configured_bind_host
cargo test --locked runtime::udp::tests::udp_runtime_upstream_sessions_use_configured_bind_host
```

Expected after implementation: both pass. Before implementation, the new end-to-end test should fail to compile or fail because the production session path still calls a no-argument helper/hard-coded bind.

## Task 2: Clarify operator-facing PROXY_LISTEN_HOST binding text

**Files:**
- Modify: `README.md`
- Modify: `src/bin/relayd.rs`

- [ ] **Step 1: Update the Env bullet**

Change:

```markdown
- `PROXY_LISTEN_HOST` — TCP/UDP relay listen host, default `0.0.0.0`
```

to:

```markdown
- `PROXY_LISTEN_HOST` — TCP/UDP relay listen host and UDP forwarding session source bind host, default `0.0.0.0`
```

- [ ] **Step 2: Update the Env explanatory paragraph**

Change:

```markdown
If `HTTP_LISTEN` is `:PORT`, relayd binds the HTTP API to `127.0.0.1:PORT`. Relay listeners use `PROXY_LISTEN_HOST` independently, so allocations bind TCP/UDP ports on `0.0.0.0` by default.
```

to:

```markdown
If `HTTP_LISTEN` is `:PORT`, relayd binds the HTTP API to `127.0.0.1:PORT`. Relay listeners use `PROXY_LISTEN_HOST` independently, so allocations bind TCP/UDP ports on `0.0.0.0` by default; UDP forwarding session sockets use the same host as their source bind address.
```

- [ ] **Step 3: Update the Docker/local note**

In the Docker section paragraph that starts with `Docker uses HTTP_LISTEN=0.0.0.0:8080`, replace:

```markdown
Relay TCP/UDP ports bind `0.0.0.0` by default via `PROXY_LISTEN_HOST`.
```

with:

```markdown
Relay TCP/UDP listener ports and UDP forwarding session sockets bind `0.0.0.0` by default via `PROXY_LISTEN_HOST`.
```

Keep the rest of the paragraph unchanged.

## Task 3: Update CLI help text and test

**Files:**
- Modify: `src/bin/relayd.rs`

- [ ] **Step 1: Update proxy-listen-host help text**

In `Cli`, change the `proxy_listen_host` help from:

```rust
        help = "TCP/UDP relay listen host (env: PROXY_LISTEN_HOST). Default: 0.0.0.0."
```

to:

```rust
        help = "TCP/UDP relay listen host and UDP forwarding session source bind host (env: PROXY_LISTEN_HOST). Default: 0.0.0.0."
```

- [ ] **Step 2: Update the CLI help regression test**

In `cli_help_documents_runtime_options`, keep the existing `assert!(help.contains("env: PROXY_LISTEN_HOST"));` and add:

```rust
        assert!(help.contains("UDP forwarding session source bind host"));
```

- [ ] **Step 3: Run the targeted CLI help test**

Run:

```bash
cargo test --locked --bin relayd cli_help_documents_runtime_options
```

Expected: PASS.

## Task 4: Verify behavior, commit, and prepare push

**Files:**
- Read-only verification after Tasks 1 and 2, then normal corrective commit.

- [ ] **Step 1: Run targeted bind-host tests**

Run:

```bash
cargo test --locked runtime::udp::tests::udp_upstream_socket_uses_configured_bind_host
cargo test --locked runtime::udp::tests::udp_runtime_upstream_sessions_use_configured_bind_host
cargo test --locked --bin relayd cli_help_documents_runtime_options
```

Expected: all pass.

- [ ] **Step 2: Run UDP runtime tests**

Run:

```bash
cargo test --locked runtime::udp
```

Expected: all UDP runtime tests pass.

- [ ] **Step 3: Run real runtime tests**

Run:

```bash
cargo test --locked runtime::real
```

Expected: all real runtime tests pass.

- [ ] **Step 4: Run formatting check**

Run:

```bash
cargo fmt -- --check
```

Expected: command exits 0.

- [ ] **Step 5: Run clippy**

Run:

```bash
cargo clippy --locked --lib --tests -- -D warnings
```

Expected: command exits 0.

- [ ] **Step 6: Check diff hygiene**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` exits 0. `git status --short` shows only:

```text
 M README.md
 M src/bin/relayd.rs
 M src/runtime/udp.rs
?? docs/superpowers/specs/2026-05-22-udp-forward-source-bind-host-design.md
?? docs/superpowers/plans/2026-05-22-udp-forward-source-bind-host.md
```

- [ ] **Step 7: Create a normal corrective commit, no history rewrite**

After final SDD implementation review approval and fresh verification, commit the intended files with Lore format. Do not amend, rebase, force push, or rewrite commit `a0a27b99be377e7215d858c3b3f062e4a7a19241`.

Expected commit scope:

```text
README.md
src/bin/relayd.rs
src/runtime/udp.rs
docs/superpowers/specs/2026-05-22-udp-forward-source-bind-host-design.md
docs/superpowers/plans/2026-05-22-udp-forward-source-bind-host.md
```

## Self-Review

- Spec coverage: helper-level and end-to-end config→session source bind behavior, README and CLI operator docs, no history rewrite, and verification commands are covered.
- Placeholder scan: no `TBD`, `TODO`, or unspecified implementation steps remain.
- Type/command consistency: helper signature is consistently `bind_upstream_socket(bind_host: &str)`, and the configured host is consistently sourced from `entry.bind_host` copied from `UdpRuntimeConfig.bind_host`.
