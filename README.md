# relayd

Linux-first Zig port-forwarder with:
- authenticated HTTP API
- SQLite persistence
- startup restore
- TCP forwarding with copy path + gated Linux splice fast-path
- UDP forwarding with async listener/session handling

## Env
- `HTTP_LISTEN` — HTTP API listen address, e.g. `:8080` or `127.0.0.1:8080`
- `PORT_RANGE` — default `10000-30000`
- `AUTH_TOKEN` — required bearer token
- `TCP_SESSION_MODEL_ENABLED` — optional `true/false`, default `false`
- `TCP_SESSION_MODEL_WORKERS` — optional worker count for workerized TCP session-model mode, default `0`
- `TCP_SESSION_MODEL_ACCEPT_BALANCED` — optional `true/false`, default `false`
- `TCP_SESSION_MODEL_SHARDED_ACCEPT` — optional `true/false`, default `false`
- `TCP_SPLICE_ENABLED` — optional `true/false`, default `false`
- `FORCE_TCP_COPY_FALLBACK` — optional `true/false`
- `UDP_SESSION_WORKERS` — optional worker count for UDP attribution / workerized mode, default `0`
- `UDP_IO_URING_ENABLED` — optional `true/false`, enables the bounded io_uring UDP path
- `UDP_GRO_ENABLED` — optional `true/false`, enables the UDP GRO receive-offload path
- `UDP_DATAPLANE_REDESIGN_ENABLED` — optional `true/false`, enables the broader worker-owned UDP dataplane redesign path
- `UDP_FAST_PATH_ENABLED` — optional `true/false`, default `false`
- `UDP_FAST_PATH_SEGMENT_SIZE` — optional UDP fast-path segment size, default `1472`
- `UDP_FAST_PATH_GSO_BURST` — optional UDP fast-path max batch size, default `16`
- `UDP_SOCKET_RCVBUF_BYTES` — optional UDP listener/upstream receive buffer size, default `8388608`
- `UDP_SOCKET_SNDBUF_BYTES` — optional UDP listener/upstream send buffer size, default `8388608`
- `RUNTIME_APPLY_TIMEOUT_MS` — optional, default `2000`
- `RESTORE_SWEEP_TIMEOUT_MS` — optional, default `30000`
- `SQLITE_PATH` — optional, default `relayd.sqlite3`

If `HTTP_LISTEN` is `:PORT`, relayd binds `127.0.0.1:PORT`.

## Build
```bash
zig build
zig build test
```

## Run
```bash
HTTP_LISTEN=:8080 AUTH_TOKEN=mytoken zig-out/bin/relayd
```

## End-to-end bandwidth harness
Local e2e runs use `scripts/ci/e2e_iperf3.sh` to drive real TCP and UDP `iperf3` traffic through `relayd`.

### Local prerequisites
- `zig`
- `python3`
- `iperf3`

### Local invocation
```bash
zig build
./scripts/ci/e2e_iperf3.sh
```

By default the harness runs relayd with `AUTH_TOKEN=test-token`, `HTTP_LISTEN=127.0.0.1:18080`, and `PORT_RANGE=18100-18120`. The listen address and relay port range are overrideable for local reruns, for example:

```bash
HTTP_LISTEN=127.0.0.1:28080 PORT_RANGE=28100-28120 ./scripts/ci/e2e_iperf3.sh
```

### Matrix mode
The harness also supports repeated UDP benchmark sweeps for relay-vs-direct comparison:

```bash
IPERF_MODE=matrix \
UDP_SWEEP_RATES=10G,25G \
UDP_PACKET_SIZES=1472 \
IPERF_REPETITIONS=3 \
UDP_MATRIX_DURATION=2 \
./scripts/ci/e2e_iperf3.sh
```

Useful knobs:
- `UDP_RATE` / `UDP_PACKET_SIZE` for one-shot mode
- `TCP_DIRECT_VS_RELAY`, `TCP_COMPARE_MODE`, `TCP_BENCH_DURATION`, `TCP_BENCH_REPETITIONS`, `TCP_STREAMS` for the one-shot TCP comparison suites
- `TCP_SESSION_MODEL_ENABLED` enables the runtime-owned TCP session-model path
- `TCP_SESSION_MODEL_WORKERS=2` enables the workerized TCP session-model lane used by `TCP_COMPARE_MODE=workerized-session`
- `TCP_SESSION_MODEL_WORKERS=4 TCP_SESSION_MODEL_SHARDED_ACCEPT=1` enables the sharded-worker lane used by `TCP_COMPARE_MODE=sharded-worker`
- `TCP_SESSION_MODEL_WORKERS=4 TCP_SESSION_MODEL_ACCEPT_BALANCED=1` enables the accept-balanced lane used by `TCP_COMPARE_MODE=accept-balanced`
- `TCP_SPLICE_ENABLED` enables the relay splice path; `FORCE_TCP_COPY_FALLBACK=1` overrides it and keeps relay TCP on copy
- `UDP_SWEEP_RATES`, `UDP_PACKET_SIZES`, `IPERF_REPETITIONS`, `UDP_MATRIX_DURATION` for matrix mode
- `UDP_SESSION_WORKERS=1` enables the UDP attribution probe lane
- `UDP_SESSION_WORKERS=2` (or higher) enables the UDP workerized lane
- `UDP_COMPARE_MODE=dataplane-redesign` runs the focused fast-path-baseline vs broader UDP redesign comparison and a redesign single-worker vs redesign workerized multi-flow comparison
- `UDP_COMPARE_MODE=fast-path` runs the focused single-flow fast-path comparison and the narrow multi-flow workerization comparison
- `UDP_COMPARE_MODE=gro` runs the focused single-flow fast-path-baseline vs GRO comparison and then preserves the narrow multi-flow workerization comparison
- `UDP_COMPARE_MODE=io-uring` runs the focused single-flow fast-path-baseline vs io_uring comparison and then preserves the narrow multi-flow workerization comparison
- `UDP_COMPARE_MODE=workerized` runs the focused UDP baseline/probe/workerized comparison at `10G/1472` and `25G/1472`
- `UDP_MULTI_FLOW_STREAMS` controls the narrow multi-flow workerization suite, default `4`
- `UDP_MULTI_FLOW_RATE` controls the per-suite offered UDP rate for the narrow multi-flow workerization comparison, default `10G`
- `UDP_MULTI_FLOW_PACKET_SIZE` controls the packet size for the narrow multi-flow suite, default `1472`
- `UDP_MULTI_FLOW_DURATION` controls the duration for the narrow multi-flow suite, default `UDP_MATRIX_DURATION`
- `UDP_SOCKET_RCVBUF_BYTES`, `UDP_SOCKET_SNDBUF_BYTES` for relay UDP socket buffer tuning
- artifacts are written under `.zig-cache/e2e/iperf3-latest`, including:
  - `udp-fast-path-summary.txt`
  - `udp-fast-path-focus-10g-1472.txt`
  - `udp-fast-path-focus-25g-1472.txt`
  - `udp-fast-path-multiflow-summary.txt`
  - `udp-fast-path-overall-summary.txt`
  - `udp-gro-summary.txt`
  - `udp-gro-focus-10g-1472.txt`
  - `udp-gro-focus-25g-1472.txt`
  - `udp-gro-overall-summary.txt`
  - `udp-dataplane-redesign-summary.txt`
  - `udp-dataplane-redesign-focus-10g-1472.txt`
  - `udp-dataplane-redesign-focus-25g-1472.txt`
  - `udp-dataplane-redesign-multiflow-summary.txt`
  - `udp-dataplane-redesign-overall-summary.txt`
  - `udp-io-uring-summary.txt`
  - `udp-io-uring-focus-10g-1472.txt`
  - `udp-io-uring-focus-25g-1472.txt`
  - `udp-io-uring-overall-summary.txt`
  - `udp-workerized-summary.txt`
  - `udp-workerized-focus-10g-1472.txt`
  - `udp-workerized-focus-25g-1472.txt`
  - `tcp-accept-balanced-streams-1-summary.txt`
  - `tcp-accept-balanced-streams-4-summary.txt`
  - `tcp-accept-balanced-overall-summary.txt`
  - `tcp-sharded-worker-streams-1-summary.txt`
  - `tcp-sharded-worker-streams-4-summary.txt`
  - `tcp-sharded-worker-overall-summary.txt`
  - `tcp-workerized-session-streams-1-summary.txt`
  - `tcp-workerized-session-streams-4-summary.txt`
  - `tcp-workerized-session-overall-summary.txt`
  - `tcp-session-model-streams-1-summary.txt`
  - `tcp-session-model-streams-4-summary.txt`
  - `tcp-session-model-overall-summary.txt`
  - `tcp-copy-vs-splice-streams-1-summary.txt`
  - `tcp-copy-vs-splice-streams-4-summary.txt`
  - `tcp-copy-vs-splice-overall-summary.txt`
  - representative direct/threaded/session-model/workerized/sharded-worker/accept-balanced TCP JSON/log outputs under `tcp-accept-balanced/streams-*`
  - representative direct/threaded/session-model/workerized/sharded-worker TCP JSON/log outputs under `tcp-sharded-worker/streams-*`
  - representative direct/threaded/session-model/workerized-session TCP JSON/log outputs under `tcp-workerized-session/streams-*`
  - representative direct/threaded/session-model TCP JSON/log outputs under `tcp-session-model/streams-*`
  - representative direct/copy/splice TCP JSON/log outputs under `tcp-copy-vs-splice/streams-*`

Example focused UDP workerization run:

```bash
IPERF_MODE=matrix \
UDP_COMPARE_MODE=workerized \
UDP_SESSION_WORKERS=2 \
IPERF_REPETITIONS=3 \
UDP_MATRIX_DURATION=2 \
./scripts/ci/e2e_iperf3.sh
```

Example focused UDP fast-path run:

```bash
IPERF_MODE=matrix \
UDP_COMPARE_MODE=fast-path \
UDP_SESSION_WORKERS=2 \
UDP_MULTI_FLOW_STREAMS=4 \
IPERF_REPETITIONS=3 \
UDP_MATRIX_DURATION=2 \
./scripts/ci/e2e_iperf3.sh
```

Example focused broader UDP dataplane redesign run:

```bash
IPERF_MODE=matrix \
UDP_COMPARE_MODE=dataplane-redesign \
UDP_DATAPLANE_REDESIGN_ENABLED=1 \
UDP_MULTI_FLOW_STREAMS=4 \
IPERF_REPETITIONS=3 \
UDP_MATRIX_DURATION=2 \
./scripts/ci/e2e_iperf3.sh
```

Example focused UDP GRO run:

```bash
IPERF_MODE=matrix \
UDP_COMPARE_MODE=gro \
UDP_GRO_ENABLED=1 \
UDP_MULTI_FLOW_STREAMS=4 \
IPERF_REPETITIONS=3 \
UDP_MATRIX_DURATION=2 \
./scripts/ci/e2e_iperf3.sh
```

Example focused UDP io_uring run:

```bash
IPERF_MODE=matrix \
UDP_COMPARE_MODE=io-uring \
UDP_IO_URING_ENABLED=1 \
UDP_MULTI_FLOW_STREAMS=4 \
IPERF_REPETITIONS=3 \
UDP_MATRIX_DURATION=2 \
./scripts/ci/e2e_iperf3.sh
```

## API
Canonical API documentation lives in [`docs/API.md`](docs/API.md).

Quick example:

```bash
curl -sS \
  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  http://127.0.0.1:8080/v1/ports
```

## Notes
- `host` currently accepts IP literals only.
- Control-plane TLS is intentionally out of scope; run behind a trusted network or TLS reverse proxy.
