# Runtime Mode Netlink Design

## Goal

Add a configurable runtime mode so relayd can either keep its current user-space TCP/UDP forwarding runtime or manage nftables DNAT rules through libnftnl netlink bindings.

## Scope

- Add `RuntimeMode` with values `proxy` and `netlink`.
- Add environment and CLI configuration:
  - `RELAYD_RUNTIME_MODE` / `--runtime-mode`, default `proxy`.
  - `RELAYD_NFTABLES_TABLE` / `--nftables-table`, default `relayd`.
  - `RELAYD_NFTABLES_CHAIN` / `--nftables-chain`, default `mapping`.
- Preserve current default behavior. Without configuration changes, relayd starts TCP and UDP user-space listeners and forwards exactly as it does today.
- In `netlink` mode, relayd starts the HTTP control plane and allocation service, but forwarding is done by nftables DNAT rules instead of relayd-owned TCP/UDP listener sockets.

Out of scope:

- Shelling out to `nft`.
- Using the nftables JSON API crate for runtime mutation.
- Changing the HTTP API shape, SQLite schema, allocation conflict rules, auth, or metrics endpoint paths.

## Runtime Modes

`proxy` is the existing runtime. It binds TCP/UDP relay sockets on `PROXY_LISTEN_HOST`, accepts traffic, forwards to bindings, records listener/session metrics, and reports no-host allocations as `rejecting_no_host`.

`netlink` is a new runtime implementation behind the existing `RuntimeFacade`. It stores allocation state in memory for snapshots, and applies the active, bound allocation set to nftables through a small backend abstraction backed by libnftnl bindings. It does not bind relay sockets for allocation ports.

The startup path chooses the runtime from parsed `Config`:

- `proxy`: construct `RealRuntime` from `RealRuntimeConfig` as today.
- `netlink`: construct `NetlinkRuntime` with table and chain settings.

The binary keeps static dispatch instead of boxing the async trait. `serve_listener` and `serve_listener_until_shutdown` become generic over `R: RuntimeFacade + 'static`; `run_with_listener` matches on `config.runtime_mode` and calls a shared generic helper with either `RealRuntime` or `NetlinkRuntime`.

`RuntimeFacade` gains an async `initialize(&self) -> Result<(), RuntimeError>` method with no-op implementations for existing runtimes. `Service::restore_all` calls `runtime.initialize()` before reading persisted allocations. This is the startup lifecycle hook that lets `netlink` ensure and flush an empty owned chain even when SQLite contains zero allocations.

## nftables Topology

`netlink` mode manages one exclusive nftables table and chain:

- Family: `inet`.
- Table name: configured by `RELAYD_NFTABLES_TABLE` / `--nftables-table`, default `relayd`.
- Chain name: configured by `RELAYD_NFTABLES_CHAIN` / `--nftables-chain`, default `mapping`.
- Chain type: `nat`.
- Hook: `prerouting`.
- Priority: destination NAT priority.

On startup restore, `Service::restore_all` calls `RuntimeFacade::initialize` before iterating allocations. In `netlink` mode, `initialize` creates the table and chain if missing, flushes the configured chain, and applies an empty ruleset. The following per-allocation `restore` calls repopulate in-memory state and rewrite the chain from restored allocations. If SQLite has zero allocations, startup still leaves the configured chain present and empty. The configured chain is relayd-owned; relayd may flush it.

On create, update, delete, and restore, `NetlinkRuntime` updates its in-memory allocation map and rewrites the whole configured chain from that map. The implementation does not rely on nftables rule handles surviving process restarts.

Chain replacement is submitted as one nftnl batch transaction containing table/chain ensure operations, chain flush, and rule additions. If the kernel rejects the batch, relayd treats the entire managed chain as potentially stale and does not claim that any subset of kernel rules is current.

## Rule Generation

A rule is generated only when an allocation has both `host` and `target_port`.

Unbound allocations remain valid and reserve the relay port in SQLite/control-plane state, but no DNAT rule is installed. Their snapshot follows the current TCP/UDP runtime convention and reports:

- `runtime_status = rejecting_no_host`
- `effective_host = allocation.host`
- `effective_target_port = allocation.target_port`
- no error kind
- no last error

For `protocol = tcp` and `protocol = udp`, relayd generates one DNAT rule for the concrete protocol.

For `protocol = both`, relayd generates two DNAT rules with the same relay port and target binding:

- one TCP DNAT rule
- one UDP DNAT rule

Each rule matches:

- L4 protocol equal to TCP or UDP.
- Transport destination port equal to allocation relay port.
- FIB destination address type local, equivalent to the existing Go rule shape using `fib daddr type local`.

Each rule DNATs to:

- allocation binding host address
- allocation binding target port

The `inet` table supports both IPv4 and IPv6. Rule generation branches by parsed binding host address:

- IPv4 host: encode IPv4 destination address and use IPv4 NAT family.
- IPv6 host: encode IPv6 destination address and use IPv6 NAT family.

Existing service host validation already accepts IP literals only; `netlink` continues to rely on IP literal bindings.

## Backend Boundary

`NetlinkRuntime` owns runtime semantics and should not expose libnftnl details to service or HTTP code. It depends on this internal backend trait:

```rust
pub(crate) trait NftBackend: Send + Sync {
    fn replace_ruleset(
        &self,
        table: &str,
        chain: &str,
        rules: &[NftDnatRule],
    ) -> Result<(), NftBackendError>;
}
```

`replace_ruleset` is responsible for ensuring the inet table exists, ensuring the `nat`/`prerouting`/`dstnat` chain exists, flushing the chain, and adding the complete ordered DNAT rule list in one nftnl batch.

Production backend uses crates.io `nftnl = 0.9.2`, the Mullvad safe abstraction over system `libnftnl`, plus `mnl = 0.3.0` to send finalized batches to `NETLINK_NETFILTER` and process ACK/error responses. These dependencies are behind the `netlink` Cargo feature so the default proxy build does not require system libnftnl/libmnl. `nftnl-rs = 0.5.1` is not used because its README states it only covers table/set operations and is not actively extended.

The backend should prefer `nftnl` safe types such as `Batch`, `Table`, `Chain`, `Rule`, `expr::Meta`, `expr::Payload`, `expr::Cmp`, `expr::Immediate`, and `expr::Nat`. When the safe wrapper lacks a required operation, such as the `fib daddr type local` expression or chain flush message, the backend may use `nftnl::nftnl_sys` directly inside the production backend module. That low-level usage must stay behind `NftBackend` and must not leak into service, HTTP, config, or tests.

When built with `--features netlink`, the project must link against system `libnftnl` and `libmnl`; missing libraries, insufficient netlink privileges, unsupported kernel nftables features, or netlink ACK errors surface as `NftBackendError` and then map to the operation's `RuntimeError`. A binary built without the feature fails startup clearly if `runtime-mode=netlink` is selected.

Unit tests use an in-memory backend that records topology and rules without requiring root privileges, system libnftnl, or kernel nftables support. Production backend tests may be limited to compile-time construction and unit conversion tests unless the environment provides root/netlink privileges.

Netlink backend apply failures are not proxy bind failures and must not be surfaced as retryable port conflicts. `RuntimeError` gains a non-retryable `RuntimeApplyFailed` variant for whole-ruleset backend failures. `Service::create_allocation` continues to treat `RuntimeCreateFailed` as a retryable bind/port conflict for proxy runtimes, but treats `RuntimeApplyFailed` as fatal and returns a service runtime error without trying the next port.

Runtime methods return:

- proxy create bind failure: `RuntimeCreateFailed`
- netlink whole-ruleset apply failure on create/update/delete/restore/initialize: `RuntimeApplyFailed`
- proxy update/delete/restore failures keep the existing `RuntimeUpdateFailed`, `RuntimeDeleteFailed`, and `RuntimeRestoreFailed` behavior

`NetlinkRuntime` keeps two concepts separate:

- the in-memory allocation map used for snapshots and future desired rulesets
- the last successful kernel ruleset generation

State mutation rules:

- `initialize`: calls `replace_ruleset` with no rules and records `initialized = true` only on success. Failure returns `RuntimeApplyFailed` and marks the runtime stale.
- `create`: builds a candidate map containing the new allocation and applies it. On success, commits the candidate map. On failure, leaves the map unchanged, marks the runtime stale, and returns `RuntimeApplyFailed`; because SQLite has not persisted the allocation yet, the failed allocation must not appear in snapshots or future rulesets.
- `update`: builds a candidate map with the updated allocation and applies it. On success, commits the candidate map. On failure, commits the attempted allocation to the map, marks the runtime stale, and returns `RuntimeApplyFailed`; this matches the service order where the binding may already be persisted before runtime update.
- `delete`: builds a candidate map without the allocation and applies it. On success, commits the candidate map. On failure, leaves the allocation in the map, marks the runtime stale, and returns `RuntimeApplyFailed`; this matches the service order where SQLite still contains the allocation when runtime delete fails.
- `restore`: builds a candidate map with the restored allocation and applies it. On success, commits the candidate map. On failure, commits the attempted restored allocation to the map, marks the runtime stale, and returns `RuntimeApplyFailed` so startup restore fails visibly.

While stale, every bound allocation snapshot reports `degraded_apply_failed` with `error_kind = apply_failed` and the backend error as `last_error`, because the whole chain rewrite may have failed before or during kernel application. Unbound allocations still report `rejecting_no_host` because they do not depend on DNAT rules. The stale marker clears only after a later successful full `replace_ruleset`.

## Snapshots And Metrics

`NetlinkRuntime::snapshot` reports state from its in-memory map:

- bound allocations with successful apply: `active`
- unbound allocations: `rejecting_no_host`
- bound allocations while the latest ruleset generation is stale: `degraded_apply_failed`
- missing allocation id: `None`

`NetlinkRuntime::snapshot_listener_metrics` returns an empty vector. Kernel DNAT rules include counters internally, but exporting nftables counters as relayd listener metrics is out of scope for this change.

`netlink` mode does not increment proxy-runtime TCP/UDP listener counters because it does not own relay sockets or user-space packet sessions.

## Configuration Validation

`RuntimeMode` parsing is case-insensitive and accepts:

- `proxy`
- `netlink`

Invalid values return a config error.

The nftables table and chain names must be non-empty strings. They are parsed for both runtime modes so invalid startup configuration is caught early, although they only affect `netlink` mode.

CLI options override matching environment variables using the existing `Cli::apply_to_env` pattern.

## Documentation Impact

Update README and architecture docs to describe:

- default `proxy` runtime mode
- `netlink` runtime mode
- required nftables/kernel privileges for `netlink`
- table/chain defaults and override flags
- chain ownership and startup flush/rewrite behavior
- empty listener metrics in `netlink` mode
- configured chain ownership: relayd creates, owns, flushes, and rewrites it on startup and runtime changes

## Acceptance Criteria

- Default config produces `runtime_mode = proxy`.
- `RELAYD_RUNTIME_MODE=netlink` and `--runtime-mode netlink` select the netlink runtime.
- CLI override beats environment for runtime mode and nftables table/chain names.
- Invalid runtime mode, empty nftables table, and empty nftables chain fail config parsing.
- `serve_listener` and `serve_listener_until_shutdown` accept generic runtime state, and startup chooses `RealRuntime` or `NetlinkRuntime` without async trait objects.
- `Service::restore_all` calls `RuntimeFacade::initialize` before restoring allocations.
- `RuntimeError::RuntimeApplyFailed` exists, maps to HTTP 503, and is not treated as a retryable port conflict by `Service::create_allocation`.
- Netlink startup with an empty SQLite database still ensures table/chain existence and flushes the owned chain to an empty ruleset.
- `runtime-mode=netlink` does not bind allocation TCP/UDP relay sockets.
- In netlink runtime tests, unbound allocations install no rules and snapshot as `rejecting_no_host`.
- Bound TCP and UDP allocations install DNAT rules with configured table/chain, inet family, protocol, relay port, host IP, and target port.
- `both` installs exactly two rules, one TCP and one UDP.
- IPv4 and IPv6 host bindings are both represented with the correct destination address family.
- Deleting an allocation rewrites the managed chain without that allocation's rules.
- Deleting a binding rewrites the managed chain without that allocation's rules while keeping the allocation snapshot as `rejecting_no_host`.
- Restore flushes/rewrites the configured chain from persisted allocations.
- If a whole-chain replace fails, all bound netlink snapshots report `degraded_apply_failed` until a later successful replace.
- Failed netlink create does not persist or retain an unpersisted allocation and does not continue trying later ports.
- Failed netlink delete keeps the still-persisted allocation visible in runtime snapshots as `degraded_apply_failed`.
- The production backend is implemented with `nftnl = 0.9.2` plus `mnl = 0.3.0`, not nftables JSON API and not shell execution.
- Proxy mode listener metrics and existing forwarding behavior are preserved.
- CLI help and docs warn that the configured nftables chain is relayd-owned and flushed/replaced.
- Existing proxy runtime tests continue to pass.
