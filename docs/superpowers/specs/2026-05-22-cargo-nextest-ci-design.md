# Cargo Nextest CI Design

## Status

Approved by user on 2026-05-22 after independent spec review. A first independent review requested two revisions: preserve doctest coverage and pin the nextest installation policy. A second independent review returned `VERDICT: APPROVE` with no required changes.

## Goal

Use `cargo-nextest` as the primary Rust test runner in CI and documented local development commands while preserving the non-test validation, release build, e2e harness, and doctest coverage currently provided by `cargo test --locked`.

## Scope

In scope:

- Update `.github/workflows/test.yml` so the main Rust test step runs `cargo nextest run --locked`.
- Install nextest in CI with a pinned action major version and pinned nextest series:
  - `uses: taiki-e/install-action@v2`
  - `tool: nextest@0.9`
- Add a separate CI doctest step using `cargo test --locked --doc` because nextest does not run doctests.
- Update `README.md` Build and test instructions to document local nextest installation and the new test commands.
- Verify locally with nextest, doctests, formatting, clippy, and diff/status checks.

Out of scope:

- Changing Rust dependencies or `Cargo.lock`.
- Changing `cargo fmt`, `cargo clippy`, `cargo zigbuild`, Docker, build artifact, or e2e bandwidth behavior.
- Adding a nextest profile/config file; default nextest behavior is sufficient for this small migration.
- Rewriting historical planning docs that mention `cargo test`; those are archived implementation records and not current user-facing build instructions.

## Design

### CI workflow

`.github/workflows/test.yml` keeps the current job shape and validation order, with nextest installed before Rust tests. The Rust test command changes from `cargo test --locked` to `cargo nextest run --locked`. A new doctest step runs `cargo test --locked --doc` after nextest so future documentation tests remain covered.

Expected relevant CI sequence:

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

The action is pinned to major version `v2` and nextest is pinned to the compatible `0.9` series. This avoids the unpinned moving `@nextest` shorthand while still allowing compatible patch updates.

### Coverage preservation

`cargo nextest run` covers normal Rust test binaries but does not run doctests. The current repository has no doctests (`cargo test --locked --doc` reports 0 tests), but keeping a separate doctest step preserves the old `cargo test --locked` behavior for future doctests.

### Documentation

`README.md` Build and test instructions should:

- Add `cargo install cargo-nextest --locked` to one-time local tooling setup.
- Replace `cargo test --locked` with `cargo nextest run --locked`.
- Include `cargo test --locked --doc` alongside nextest to mirror CI coverage.
- Keep the existing clippy command unchanged.

## Acceptance Criteria

- `.github/workflows/test.yml` installs nextest with `taiki-e/install-action@v2` and `tool: nextest@0.9`.
- `.github/workflows/test.yml` runs `cargo nextest run --locked` for Rust tests.
- `.github/workflows/test.yml` runs `cargo test --locked --doc` for doctests.
- Existing formatting, clippy, musl build, and e2e harness commands remain unchanged.
- `README.md` documents local nextest installation and the nextest/doctest/clippy test sequence.
- No Cargo dependency or lockfile changes are introduced.
- Fresh verification passes:
  - `cargo nextest run --locked`
  - `cargo test --locked --doc`
  - `cargo fmt -- --check`
  - `cargo clippy --locked --lib --tests -- -D warnings`
  - `git diff --check`
