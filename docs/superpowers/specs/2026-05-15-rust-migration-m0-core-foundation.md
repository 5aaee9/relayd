# Rust Migration M0 Core Foundation Spec

## Goal

Create the Rust crate foundation for relayd by porting configuration parsing, core model types, UUIDv7 generation, and SQLite repository behavior from Zig while leaving the existing Zig implementation intact.

## In scope

- Cargo package, library, lockfile, and placeholder binary.
- `.gitignore` updates for Rust build artifacts.
- Config parsing for all current environment variables, including feature-flag variables such as `TCP_SESSION_MODEL_MAX_ACTIVE` as parsed config fields only.
- Domain model enums and structs needed by later API/service/runtime milestones.
- UUIDv7 string generation.
- SQLite schema creation, WAL/busy timeout setup, self-check, legacy binding migration, allocation CRUD/list, and binding CRUD behavior.
- Unit tests and repository tests run by `cargo test`.

## Out of scope

- HTTP routes.
- Allocation service orchestration.
- TCP/UDP runtime listeners and forwarding.
- Prometheus endpoint rendering.
- Docker/CI cutover.
- Implementing optional feature-gated runtime lanes.

## Acceptance criteria

- `cargo test` passes.
- The Rust model parser and config parser match Zig unit-test semantics for protocols, host configured helper, HTTP listen parsing, port parsing, port ranges, and current environment defaults including `TCP_SESSION_MODEL_MAX_ACTIVE = 256`.
- The Rust repository preserves Zig schema and migration behavior for `allocations` and `bindings`.
- The Rust repository enables WAL mode and a 5000 ms busy timeout at open.
- The Rust repository updates legacy binding columns when writing bindings and clears them when deleting bindings.
- The Rust repository deletes bindings when deleting allocations.
- The Rust repository hydrates binding data into allocations using the same left-join behavior as Zig.
- The Rust repository orders allocation list results by protocol then port.
- M0 does not remove or rewrite the existing Zig implementation.

## Dependency revision: SeaORM + SQLx

User revision on 2026-05-15: replace the handwritten Rust SQLite repository with SeaORM and SQLx. M0 therefore uses:

- SeaORM entities and ActiveModels for allocation and binding CRUD/list behavior.
- SQLx SQLite pool/connect options for SQLite connection setup, WAL mode, and busy timeout.
- SeaORM's `SqlxSqliteConnector` so the repository has one SQLx-backed pool and one SeaORM `DatabaseConnection` view over that pool.
- SeaQuery/SeaORM schema helpers for table/index creation where practical.

Version decision: use stable `sea-orm 1.1.x` and `sqlx 0.8.x` instead of the currently advertised `sea-orm 2.0.0-rc.*` and `sqlx 0.9.0-alpha.*`, because M0 is a foundation milestone and should avoid release-candidate/alpha APIs unless a later milestone explicitly opts in.


## Transaction helper note

M0 intentionally omits transaction-control helpers to avoid handwritten transaction SQL in the SeaORM/SQLx storage foundation. Allocation-service transaction semantics will be specified in M1 using SeaORM/SQLx transaction APIs.
