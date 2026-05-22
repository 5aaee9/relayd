# UDP Max Sessions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raise the default active UDP session cap per relay listener from 4,096 to 65,536.

**Architecture:** This is a one-file default-value change in the UDP runtime config. The existing listener/session enforcement path stays unchanged; a private-field unit test locks the new default without adding public API.

**Tech Stack:** Rust 1.95, Tokio UDP runtime tests, Cargo nextest/cargo test-compatible unit tests.

---

## File Structure

- Modify: `src/runtime/udp.rs` — change the default `UdpRuntimeConfig.max_sessions` value and add one unit test.
- Existing approved spec: `docs/superpowers/specs/2026-05-22-udp-max-sessions-design.md`.
- No changes to `Cargo.toml`, `Cargo.lock`, README, API docs, CI workflows, or historical plan docs.

## Task 1: Lock and update the UDP default max sessions

**Files:**
- Modify: `src/runtime/udp.rs`

- [ ] **Step 1: Add a failing regression test for the desired default**

In the existing `#[cfg(test)] mod tests` in `src/runtime/udp.rs`, add this test after `reserve_two_udp_port_range` or before the first async runtime behavior test:

```rust
    #[test]
    fn udp_runtime_config_defaults_to_65536_max_sessions() {
        let config = UdpRuntimeConfig::loopback(Arc::new(Metrics::default()));

        assert_eq!(config.max_sessions, 65_536);
    }
```

- [ ] **Step 2: Run the targeted test and confirm it fails before the implementation change**

Run:

```bash
cargo test --locked runtime::udp::tests::udp_runtime_config_defaults_to_65536_max_sessions
```

Expected before implementation: FAIL with an assertion showing the old default `4096` does not equal `65536`.

- [ ] **Step 3: Update the default max session value**

In `UdpRuntimeConfig::with_bind_host`, change:

```rust
            max_sessions: 4096,
```

to:

```rust
            max_sessions: 65_536,
```

Do not change `ListenerEntry.max_sessions`, `session_for`, metrics, TTL behavior, or any public config surface.

- [ ] **Step 4: Run the targeted test and confirm it passes**

Run:

```bash
cargo test --locked runtime::udp::tests::udp_runtime_config_defaults_to_65536_max_sessions
```

Expected after implementation: PASS.

- [ ] **Step 5: Verify the UDP runtime test module**

Run:

```bash
cargo test --locked runtime::udp
```

Expected: all UDP runtime tests pass.

## Task 2: Final verification and diff hygiene

**Files:**
- Read-only verification after Task 1.

- [ ] **Step 1: Run formatting check**

Run:

```bash
cargo fmt -- --check
```

Expected: command exits 0 with no formatting diff.

- [ ] **Step 2: Run clippy**

Run:

```bash
cargo clippy --locked --lib --tests -- -D warnings
```

Expected: command exits 0 with no warnings promoted to errors.

- [ ] **Step 3: Check diff hygiene**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` exits 0. `git status --short` shows only:

```text
 M src/runtime/udp.rs
?? docs/superpowers/specs/2026-05-22-udp-max-sessions-design.md
?? docs/superpowers/plans/2026-05-22-udp-max-sessions.md
```

## Self-Review

- Spec coverage: the default cap change, regression test, unchanged enforcement path, no public config expansion, and verification commands are each covered.
- Placeholder scan: no `TBD`, `TODO`, or unspecified implementation steps remain.
- Type/command consistency: the new cap is consistently written as `65_536`; tests use the existing private test module and `Metrics::default()` import already present in `src/runtime/udp.rs` tests.
