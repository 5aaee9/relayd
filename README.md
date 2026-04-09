# relayd

Linux-first Zig port-forwarder with:
- authenticated HTTP API
- SQLite persistence
- startup restore
- TCP forwarding with copy path + Linux splice fast-path structure
- UDP forwarding with async listener/session handling

## Env
- `HTTP_LISTEN` — HTTP API listen address, e.g. `:8080` or `127.0.0.1:8080`
- `PORT_RANGE` — default `10000-30000`
- `AUTH_TOKEN` — required bearer token
- `FORCE_TCP_COPY_FALLBACK` — optional `true/false`
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
- `TCP_DIRECT_VS_RELAY`, `TCP_BENCH_DURATION`, `TCP_BENCH_REPETITIONS`, `TCP_STREAMS` for the one-shot TCP direct-vs-relay decision artifact
- `UDP_SWEEP_RATES`, `UDP_PACKET_SIZES`, `IPERF_REPETITIONS`, `UDP_MATRIX_DURATION` for matrix mode
- `UDP_SOCKET_RCVBUF_BYTES`, `UDP_SOCKET_SNDBUF_BYTES` for relay UDP socket buffer tuning
- artifacts are written under `.zig-cache/e2e/iperf3-latest`, including `tcp-direct-vs-relay-summary.txt` plus representative direct/relay TCP JSON/log outputs

## API
### Create
`POST /v1/ports`
```json
{"protocol":"tcp","target_port":80}
```

### Set target host
`POST /v1/ports/target`
```json
{"id":"<uuid-v7>","host":"127.0.0.1"}
```

### Update
`POST /v1/ports/{id}`
```json
{"target_port":8080,"host":"127.0.0.1"}
```

### Delete
`DELETE /v1/ports/{id}`

### List
`GET /v1/ports`

All requests must send:
```text
Authorization: Bearer <AUTH_TOKEN>
```

## Notes
- `host` currently accepts IP literals only.
- Control-plane TLS is intentionally out of scope; run behind a trusted network or TLS reverse proxy.
