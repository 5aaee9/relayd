# relayd

Linux-first Rust port-forwarder with:
- authenticated HTTP API
- SQLite persistence and startup restore
- TCP forwarding with a default buffered Tokio path and optional Linux `splice(2)` fast path
- UDP forwarding with default Tokio listener/session handling
- dual-protocol allocations (`both`) sharing one TCP+UDP port
- Prometheus listener metrics with per-scrape byte rates
- optional nftables DNAT runtime through libnftnl when built with `--features netlink`

## Env
- `HTTP_LISTEN` — HTTP API listen address, e.g. `:8080` or `127.0.0.1:8080`
- `PROXY_LISTEN_HOST` — TCP/UDP relay listen host and UDP forwarding session source bind host, default `0.0.0.0`
- `PORT_RANGE` — default `10000-30000`
- `AUTH_TOKEN` — required bearer token
- `SQLITE_PATH` — optional, default `relayd.sqlite3`
- `RELAYD_RUNTIME_MODE` — runtime forwarding mode, `proxy` by default. `proxy` starts relayd-owned TCP/UDP forwarding sockets; `netlink` installs nftables DNAT rules instead.
- `RELAYD_NFTABLES_TABLE` — nftables `inet` table for `netlink` mode, default `relayd`
- `RELAYD_NFTABLES_CHAIN` — nftables NAT prerouting chain for `netlink` mode, default `mapping`
- `RUNTIME_APPLY_TIMEOUT_MS` — optional, default `2000`
- `RESTORE_SWEEP_TIMEOUT_MS` — optional, default `30000`
- `TCP_SPLICE_ENABLED` — enable the Linux `splice(2)` TCP forwarding path, default `false`
- `FORCE_TCP_COPY_FALLBACK` — force buffered TCP copying and disable splice for comparison or recovery, default `false`
- `UDP_MAX_SESSIONS` — maximum active UDP sessions per relay port, default `65536`
- `UDP_BATCH_ENABLED` — enable adaptive Linux `recvmmsg(2)`/`sendmmsg(2)` UDP batching, default `false`
- `UDP_SOCKET_RCVBUF_BYTES` / `UDP_SOCKET_SNDBUF_BYTES` — parsed for compatibility, default `8388608`

The splice path preserves the buffered path's bidirectional EOF behavior and falls back to buffered copying if its pipes cannot be created. Each active splice session owns two pipes (four additional file descriptors), so deployments with many concurrent TCP sessions should size `LimitNOFILE` accordingly. Non-Linux builds always use buffered copying.

UDP batching receives the first datagram without delay, drains up to 15 additional queued datagrams, and groups sends by upstream session. This preserves per-client datagram order while reducing syscall and session-lookup overhead under burst traffic. Non-Linux builds retain single-message forwarding.

If `HTTP_LISTEN` is `:PORT`, relayd binds the HTTP API to `127.0.0.1:PORT`. Relay listeners use `PROXY_LISTEN_HOST` independently, so allocations bind TCP/UDP ports on `0.0.0.0` by default. UDP forwarding session sockets use the same host as their source bind address.

## Runtime Modes

`proxy` is the default runtime mode and keeps the existing behavior: relayd binds TCP and UDP sockets for allocated relay ports and forwards traffic in user space.

`netlink` mode keeps the HTTP control plane and SQLite allocation model, but forwarding is performed by kernel nftables DNAT rules created through libnftnl/libmnl. The configured nftables table is an `inet` table, and the configured chain is a relayd-owned `nat` prerouting destination-NAT chain. On startup and each runtime change, relayd creates the table/chain when missing, flushes the configured chain, and rewrites rules from the current allocation state. Do not put unrelated rules in that chain.

Unbound allocations reserve the relay port inside relayd but install no nftables DNAT rule. `both` allocations install one TCP rule and one UDP rule. IPv4 and IPv6 targets are both supported. Listener metrics are empty in `netlink` mode because relayd does not own per-port data-plane sockets.

`netlink` mode requires building relayd with `--features netlink`, plus nftables/netlink privileges and system `libnftnl`, `libmnl`, and `pkg-config` development/runtime support. A binary built without that feature still accepts the configuration key, but startup fails clearly if `--runtime-mode netlink` is selected.

```bash
cargo run --locked --features netlink --bin relayd -- \
  --http-listen :8080 \
  --runtime-mode netlink \
  --nftables-table relayd \
  --nftables-chain mapping \
  --auth-token mytoken
```

## Logging

relayd initializes a `tracing` formatter and writes logs to stderr. The default filter is `info`, which includes lifecycle events for relay allocation creation, binding assignment/update, binding deletion, allocation deletion, startup restore, and shutdown-signal errors.

Use `RUST_LOG` to adjust filtering, for example:

```bash
RUST_LOG=relayd=debug cargo run --locked --bin relayd -- --http-listen :8080 --proxy-listen-host 127.0.0.1 --auth-token mytoken
```

## Build and test

Release builds use [`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild) and target `x86_64-unknown-linux-musl` so published artifacts are musl-linked Linux binaries. Install once with:

```bash
cargo install cargo-zigbuild --locked
cargo install cargo-nextest --locked
rustup target add x86_64-unknown-linux-musl
```

```bash
cargo zigbuild --locked --release --bin relayd --target x86_64-unknown-linux-musl
cargo nextest run --locked
cargo test --locked --doc
cargo clippy --locked --lib --tests -- -D warnings
```

The default build does not link `libnftnl`/`libmnl`. To compile and test the optional netlink runtime, install system `libnftnl`, `libmnl`, and `pkg-config`, then run:

```bash
cargo test --locked --features netlink
cargo clippy --locked --features netlink --lib --tests -- -D warnings
```

CI installs nextest with `taiki-e/install-action@v2` and `tool: nextest@0.9`, runs normal Rust tests with `cargo nextest run --locked`, and keeps `cargo test --locked --doc` as a separate doctest coverage step because nextest does not run doctests.

### NixOS VM end-to-end tests

Linux flake checks under [`nix/tests`](nix/tests) exercise relayd as a real systemd service in an isolated three-VM topology (`client -> relay -> backend`):

```bash
nix build -L .#checks.x86_64-linux.relayd-nixos-functional
nix build -L .#checks.x86_64-linux.relayd-nixos-netlink
nix build -L .#checks.x86_64-linux.relayd-nixos-performance-proxy
nix build -L .#checks.x86_64-linux.relayd-nixos-performance-netlink
```

The functional checks cover bearer authentication, primary and compatibility API lifecycles, TCP/UDP/`both` forwarding, rebinding and deletion, IPv6 targets, Prometheus output, port exhaustion, and SQLite restore after a service restart. The netlink check sends traffic across separate client/backend VLANs, so it verifies actual nftables DNAT behavior rather than only inspecting generated rules.

Performance checks run three alternating direct/relayed `iperf3` samples for TCP and UDP and compare receiver medians within the same VM topology. The proxy performance check enables `TCP_SPLICE_ENABLED` and `UDP_BATCH_ENABLED`, asserts that every attempted splice session succeeds without buffered fallback, and verifies that UDP traffic is actually coalesced into multi-message batches. Every sample must exceed 1 Mbit/s. The conservative relay/direct ratio floors are 25% for `proxy` and 50% for `netlink`; UDP loss may exceed the direct median by at most 25 and 15 percentage points respectively. These are regression gates, not host benchmark claims. CI runs the four checks in parallel with a matrix. KVM is recommended; QEMU software emulation is substantially slower.

## Run
```bash
target/x86_64-unknown-linux-musl/release/relayd \
  --http-listen :8080 \
  --proxy-listen-host 0.0.0.0 \
  --auth-token mytoken
```

Environment variables remain supported and can be mixed with CLI options; CLI options take precedence. Use `relayd --help` to see all options and their matching environment variables.

CLI options override matching environment variables; for example `--udp-max-sessions 131072` overrides `UDP_MAX_SESSIONS` for the current process.

For development:

```bash
cargo run --locked --bin relayd -- --http-listen :8080 --proxy-listen-host 127.0.0.1 --udp-max-sessions 131072 --auth-token mytoken
```

## Docker
Docker images package a prebuilt musl binary from `dist/relayd`; the Dockerfile does not download or run compiler toolchains. For local image builds:

```bash
cargo zigbuild --locked --release --bin relayd --target x86_64-unknown-linux-musl
mkdir -p dist
cp target/x86_64-unknown-linux-musl/release/relayd dist/relayd
docker build -t relayd:local .
docker run --rm \
  -e AUTH_TOKEN=mytoken \
  -e HTTP_LISTEN=0.0.0.0:8080 \
  -p 8080:8080 \
  -v relayd-data:/data \
  relayd:local
```

Docker uses `HTTP_LISTEN=0.0.0.0:8080` so the service is reachable through the published port. Relay TCP/UDP listener ports and UDP forwarding session sockets bind `0.0.0.0` by default via `PROXY_LISTEN_HOST`. For local non-container runs, `:8080` still maps the HTTP API to `127.0.0.1:8080`; set `PROXY_LISTEN_HOST=127.0.0.1` if relay ports should be loopback-only.

## End-to-end bandwidth harness
Local e2e runs use `scripts/ci/e2e_iperf3.sh` to drive real TCP and UDP `iperf3` traffic through `relayd`.

```bash
cargo zigbuild --locked --release --bin relayd --target x86_64-unknown-linux-musl
RELAYD_BIN=target/x86_64-unknown-linux-musl/release/relayd ./scripts/ci/e2e_iperf3.sh
```

By default the harness uses `AUTH_TOKEN=test-token`, `HTTP_LISTEN=127.0.0.1:18080`, and `PORT_RANGE=18100-18120`. Override them for local reruns:

```bash
HTTP_LISTEN=127.0.0.1:28080 PORT_RANGE=28100-28120 RELAYD_BIN=target/x86_64-unknown-linux-musl/release/relayd ./scripts/ci/e2e_iperf3.sh
```

The harness includes matrix modes for optional benchmark lanes, but the Rust default runtime currently targets main TCP/UDP/`both` forwarding parity rather than optional fast-path gates.

## API
Canonical API documentation lives in [`docs/API.md`](docs/API.md).

Quick example:

```bash
curl -sS \
  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  http://127.0.0.1:8080/v1/allocations
```

Primary control-plane resources:

- `POST /v1/allocations` — reserve a relay port without binding it yet
- `PUT /v1/allocations/{id}/binding` — attach or replace the upstream target
- `DELETE /v1/allocations/{id}/binding` — detach the upstream target while keeping the allocation
- `GET /v1/ports` — compatibility / aggregate read model
- `GET /metrics` — authenticated Prometheus listener metrics; `both` allocations emit concrete `tcp` and `udp` series for the same port

Legacy write endpoints under `/v1/ports*` remain available for compatibility, but new clients should prefer the allocation/binding API.
