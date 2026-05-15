# relayd

Linux-first Rust port-forwarder with:
- authenticated HTTP API
- SQLite persistence and startup restore
- TCP forwarding with default Tokio copy path
- UDP forwarding with default Tokio listener/session handling
- dual-protocol allocations (`both`) sharing one TCP+UDP port
- Prometheus listener metrics with per-scrape byte rates


## Env
- `HTTP_LISTEN` — HTTP API listen address, e.g. `:8080` or `127.0.0.1:8080`
- `PORT_RANGE` — default `10000-30000`
- `AUTH_TOKEN` — required bearer token
- `SQLITE_PATH` — optional, default `relayd.sqlite3`
- `RUNTIME_APPLY_TIMEOUT_MS` — optional, default `2000`
- `RESTORE_SWEEP_TIMEOUT_MS` — optional, default `30000`
- `UDP_SOCKET_RCVBUF_BYTES` / `UDP_SOCKET_SNDBUF_BYTES` — parsed for compatibility, default `8388608`

Optional compatibility feature gates are parsed but are not part of the Rust default runtime yet: `TCP_SESSION_MODEL_*`, `TCP_SPLICE_ENABLED`, `FORCE_TCP_COPY_FALLBACK`, `UDP_SESSION_WORKERS`, `UDP_IO_URING_ENABLED`, `UDP_GRO_ENABLED`, `UDP_DATAPLANE_REDESIGN_ENABLED`, and `UDP_FAST_PATH_*`.

If `HTTP_LISTEN` is `:PORT`, relayd binds `127.0.0.1:PORT`.

## Build and test

Release builds use [`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild) and target `x86_64-unknown-linux-musl` so published artifacts are musl-linked Linux binaries. Install once with:

```bash
cargo install cargo-zigbuild --locked
rustup target add x86_64-unknown-linux-musl
```

```bash
cargo zigbuild --locked --release --bin relayd --target x86_64-unknown-linux-musl
cargo test --locked
cargo clippy --locked --lib --tests -- -D warnings
```

## Run
```bash
HTTP_LISTEN=:8080 AUTH_TOKEN=mytoken target/x86_64-unknown-linux-musl/release/relayd
```

For development:

```bash
HTTP_LISTEN=:8080 AUTH_TOKEN=mytoken cargo run --locked --bin relayd
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

Docker uses `HTTP_LISTEN=0.0.0.0:8080` so the service is reachable through the published port. For local non-container runs, `:8080` still maps to `127.0.0.1:8080`.

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
