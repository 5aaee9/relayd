# Cargo Nextest CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `cargo-nextest` the primary Rust test runner in CI and README instructions while preserving doctest coverage and existing non-test validation.

**Architecture:** This is a small CI/docs migration. The GitHub Actions test workflow installs a pinned nextest series, runs normal Rust tests with nextest, then runs doctests separately with Cargo. The README mirrors those commands for local development.

**Tech Stack:** Rust 1.95, Cargo, cargo-nextest 0.9 series, GitHub Actions, `taiki-e/install-action@v2`.

---

## File Structure

- Modify: `.github/workflows/test.yml` — install cargo-nextest and replace the Rust test command while preserving doctest coverage.
- Modify: `README.md` — document nextest local installation and the new build/test command sequence.
- Existing approved spec: `docs/superpowers/specs/2026-05-22-cargo-nextest-ci-design.md`.
- No changes to `Cargo.toml`, `Cargo.lock`, build workflows, Docker workflow, source code, or historical plan docs.

## Task 1: Update CI test workflow

**Files:**
- Modify: `.github/workflows/test.yml`

- [ ] **Step 1: Inspect the current workflow section**

Run:

```bash
sed -n '25,60p' .github/workflows/test.yml
```

Expected current relevant output includes:

```yaml
      - name: Rust format check
        run: cargo fmt -- --check

      - name: Rust tests
        run: cargo test --locked

      - name: Rust clippy
        run: cargo clippy --locked --lib --tests -- -D warnings
```

- [ ] **Step 2: Add nextest install and doctest preservation**

Replace the workflow block from `Rust format check` through `Rust clippy` with exactly:

```yaml
      - name: Rust format check
        run: cargo fmt -- --check

      - name: Install cargo-nextest
        uses: taiki-e/install-action@v2
        with:
          tool: nextest@0.9

      - name: Rust tests
        run: cargo nextest run --locked

      - name: Rust doc tests
        run: cargo test --locked --doc

      - name: Rust clippy
        run: cargo clippy --locked --lib --tests -- -D warnings
```

Keep all later Zig setup, cargo-zigbuild install, release build, and `e2e-bandwidth` job steps unchanged.

- [ ] **Step 3: Verify workflow text changed only as intended**

Run:

```bash
git diff -- .github/workflows/test.yml
```

Expected diff shape:

```diff
+      - name: Install cargo-nextest
+        uses: taiki-e/install-action@v2
+        with:
+          tool: nextest@0.9
+
       - name: Rust tests
-        run: cargo test --locked
+        run: cargo nextest run --locked
+
+      - name: Rust doc tests
+        run: cargo test --locked --doc
```

## Task 2: Update README build/test instructions

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Inspect the current Build and test section**

Run:

```bash
sed -n '/## Build and test/,/## Run/p' README.md
```

Expected current setup block includes:

```bash
cargo install cargo-zigbuild --locked
rustup target add x86_64-unknown-linux-musl
```

Expected current command block includes:

```bash
cargo zigbuild --locked --release --bin relayd --target x86_64-unknown-linux-musl
cargo test --locked
cargo clippy --locked --lib --tests -- -D warnings
```

- [ ] **Step 2: Add local cargo-nextest install command**

Change the one-time setup block to exactly:

```bash
cargo install cargo-zigbuild --locked
cargo install cargo-nextest --locked
rustup target add x86_64-unknown-linux-musl
```

- [ ] **Step 3: Replace the test command sequence with nextest plus doctests**

Change the build/test command block to exactly:

```bash
cargo zigbuild --locked --release --bin relayd --target x86_64-unknown-linux-musl
cargo nextest run --locked
cargo test --locked --doc
cargo clippy --locked --lib --tests -- -D warnings
```

- [ ] **Step 4: Add CI note after the command block**

After the command block, add this paragraph:

```markdown
CI installs nextest with `taiki-e/install-action@v2` and `tool: nextest@0.9`, runs normal Rust tests with `cargo nextest run --locked`, and keeps `cargo test --locked --doc` as a separate doctest coverage step because nextest does not run doctests.
```

- [ ] **Step 5: Verify README diff**

Run:

```bash
git diff -- README.md
```

Expected diff shape:

```diff
 cargo install cargo-zigbuild --locked
+cargo install cargo-nextest --locked
 rustup target add x86_64-unknown-linux-musl
...
 cargo zigbuild --locked --release --bin relayd --target x86_64-unknown-linux-musl
-cargo test --locked
+cargo nextest run --locked
+cargo test --locked --doc
 cargo clippy --locked --lib --tests -- -D warnings
```

## Task 3: Verify and prepare completion evidence

**Files:**
- Read-only verification across repository state.

- [ ] **Step 1: Run nextest**

Run:

```bash
cargo nextest run --locked
```

Expected: command exits 0 and reports all Rust tests passing.

- [ ] **Step 2: Run doctests**

Run:

```bash
cargo test --locked --doc
```

Expected: command exits 0. Current repository may report 0 doctests.

- [ ] **Step 3: Run formatting check**

Run:

```bash
cargo fmt -- --check
```

Expected: command exits 0 with no formatting diff.

- [ ] **Step 4: Run clippy**

Run:

```bash
cargo clippy --locked --lib --tests -- -D warnings
```

Expected: command exits 0 with no warnings promoted to errors.

- [ ] **Step 5: Check diff hygiene**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` exits 0. `git status --short` shows only these intended files before final SDD docs/commit handling:

```text
 M .github/workflows/test.yml
 M README.md
?? docs/superpowers/specs/2026-05-22-cargo-nextest-ci-design.md
?? docs/superpowers/plans/2026-05-22-cargo-nextest-ci.md
```

## Self-Review

- Spec coverage: CI nextest install, nextest test command, doctest preservation, unchanged non-test validation/build/e2e behavior, README sync, and verification commands are each covered by tasks.
- Placeholder scan: no `TBD`, `TODO`, or unspecified implementation steps remain.
- Type/command consistency: nextest is consistently `cargo nextest run --locked`; doctests are consistently `cargo test --locked --doc`; install action is consistently `taiki-e/install-action@v2` with `tool: nextest@0.9`.
