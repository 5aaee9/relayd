# Prometheus HTTP Metrics Design

**Goal:** Add an authenticated `GET /metrics` HTTP route that exports Prometheus text exposition metrics for relayd per-listener connection count and rx/tx speed, labeled by concrete `port` and `protocol`.

## Context

relayd already has an authenticated HTTP control plane in `src/http/server.zig`; all current routes require `Authorization: Bearer <token>`. It also has internal atomic counters and gauges in `src/metrics.zig`, and runtime listener/session ownership in `src/runtime/manager.zig`. Existing `/v1/metrics` returns JSON counters and should remain compatible. The new route is `/metrics` without the `/v1` prefix.

The project is on Zig `0.16.0`. Prefer a third-party Prometheus library where practical. The selected dependency is `github.com/karlseguin/metrics.zig` because it supports Prometheus output, counters/gauges, labeled vector metrics, Zig `0.16.0`, and had recent maintenance activity in 2026. It will be used for Prometheus exposition formatting of the new route. The library's labeled gauges require an `std.Io` value, so relayd will pass `compat.io()` when constructing temporary `GaugeVec` values during scrape rendering and call `deinit()` before returning. Existing in-process atomics remain the source of truth for hot-path counters.

## Requirements

1. `GET /metrics` exists and uses the same Bearer Auth check as existing HTTP routes.
2. Missing or wrong bearer token returns `401 unauthorized` for `/metrics`.
3. `GET /metrics` returns Prometheus text exposition format, not JSON.
4. Response content type is Prometheus-compatible text, `text/plain; version=0.0.4; charset=utf-8`.
5. Export current active connection/session count labeled by concrete listener `port` and concrete `protocol`:
   - Metric name: `relayd_connections_current`
   - Labels: `port`, `protocol`
   - `protocol` values are `tcp` or `udp`; a `both` allocation emits separate `tcp` and `udp` series for the same port.
   - TCP counts active accepted TCP sessions for that listener across copy/splice thread mode, runtime session mode, workerized runtime mode, and accept-balanced mode.
   - UDP counts active UDP client sessions for that listener across manager-owned sessions, worker-sharded sessions, io_uring, and fast paths.
6. Export current receive speed labeled by `port` and `protocol`:
   - Metric name: `relayd_rx_bytes_per_second`
   - Labels: `port`, `protocol`
   - Receive direction means bytes accepted by relayd from external clients on the allocated listener port.
7. Export current transmit speed labeled by `port` and `protocol`:
   - Metric name: `relayd_tx_bytes_per_second`
   - Labels: `port`, `protocol`
   - Transmit direction means bytes sent by relayd back to external clients on the allocated listener port.
8. Speed gauges are derived from monotonically increasing per-listener byte totals and the elapsed time between snapshots. First observation for a label reports `0` bytes/sec because there is no prior sample. Later observations report `(current_total - previous_total) / elapsed_seconds`, clamped to `0` if time does not advance.
9. The hot datapath must not allocate for normal counter/gauge updates. Allocations are allowed while registering/removing listeners and while rendering `/metrics`.
10. The implementation must keep existing JSON `/v1/metrics` behavior and tests passing.
11. Documentation must describe `/metrics`, auth, format, metric names, labels, and speed semantics.

## Design

### Dependency and formatting

Add a `build.zig.zon` dependency on `github.com/karlseguin/metrics.zig` and import it into the executable and test modules as `metrics_prometheus`. Add a small `src/prometheus_exporter.zig` module that builds a temporary metrics struct for each render using `metrics_prometheus.GaugeVec`. The module sets the three required labeled gauges from a runtime snapshot and writes them through `metrics_prometheus.write`. This keeps the third-party library responsible for Prometheus formatting and label serialization.

### Runtime metric source of truth

Add a separately allocated `ListenerMetricsState` for each listener and store a pointer on `ListenerEntry`. Sessions hold `*ListenerMetricsState`, not `*ListenerEntry`, so metric updates remain safe if an allocation is deleted while an accepted connection is still closing. The state contains concrete per-protocol stats fields:

- `tcp_active_sessions: Gauge`
- `udp_active_sessions: Gauge`
- `tcp_rx_bytes_total: Counter`
- `tcp_tx_bytes_total: Counter`
- `udp_rx_bytes_total: Counter`
- `udp_tx_bytes_total: Counter`

Increment/decrement the active gauges when concrete sessions are created/removed. Increment byte counters on successful relay IO. The metrics state must be ref-counted or otherwise manager-owned so listener deletion removes the row from new snapshots but does not free the state until all active TCP/UDP session references release it:

- TCP copy/splice thread mode: session creation increments per-listener TCP active count immediately after upstream connection succeeds; the defer path decrements it exactly once when the thread exits. Client-to-upstream copied/spliced bytes increment TCP rx; upstream-to-client copied/spliced bytes increment TCP tx.
- TCP runtime session mode, workerized runtime mode, and accept-balanced pending sessions: session creation increments the owning listener TCP active count wherever global TCP active count increments; every removal/error/shutdown path that decrements global TCP active count decrements the same listener. Bytes read from client increment TCP rx; bytes written to client increment TCP tx.
- UDP normal, worker, io_uring, and fast paths: session creation/removal mirrors global UDP active-session accounting on the owning listener. Listener receive payload bytes increment UDP rx; successful send-to-client bytes increment UDP tx.

Existing aggregate metrics remain unchanged. `RuntimeManager.delete` removes the listener from the registry so future snapshots no longer emit the deleted label. Any live session keeps only the metrics-state reference needed to decrement active counts and finish byte accounting safely; when the last reference releases, the metrics state is destroyed.

### Snapshot API

Add a runtime snapshot method that returns an owned list of concrete listener metric rows:

```zig
pub const ListenerMetricsSnapshot = struct {
    port: u16,
    protocol: model.Protocol,
    connections_current: u64,
    rx_bytes_total: u64,
    tx_bytes_total: u64,
};

pub fn snapshotListenerMetrics(self: *RuntimeManager, allocator: std.mem.Allocator) !std.ArrayList(ListenerMetricsSnapshot)
```

The method locks the registry, then each entry mutex or metrics-state mutex, and appends rows only for entries still present in the registry. Deleted allocations are not emitted even if old sessions are draining. It emits `tcp`/`udp` rows for `both` entries.

### Speed calculation

Add a small stateful calculator in `src/prometheus_exporter.zig` owned by each `HttpServer`. It stores the last `(rx_total, tx_total, timestamp_ms)` per `(port, protocol)` and computes rates during `/metrics` rendering. It removes stale samples not present in the current snapshot to avoid unbounded growth when listeners are deleted. It has explicit `init`/`deinit`, and every `HttpServer` initializer in `src/app.zig` and tests must provide a calculator and deinitialize it in teardown.

### HTTP route

In `src/http/server.zig`, route `GET /metrics` after the existing authorization check and before the `/v1/...` routes. The handler obtains `server.service.runtime` or a service wrapper snapshot, renders Prometheus text, and responds with status `200`, `Connection: close`, and Prometheus content type. Existing `GET /v1/metrics` continues to return JSON.

### Testing

Use TDD. Add integration tests in `tests/integration/http_api_test.zig` that first fail against current code:

1. `/metrics` requires Bearer Auth: unauthenticated and wrong-token requests return `401`.
2. `/metrics` returns Prometheus text with Prometheus content type and contains the three metric names, `# TYPE ... gauge`, and `port`/`protocol` labels after creating a TCP allocation.
3. Active TCP connection count becomes `1` while a TCP client connection is open, then returns to `0` after close.
4. Rate calculator unit tests prove first scrape returns `0`, later byte deltas produce positive rates, non-advancing time clamps to `0`, and stale labels are removed. Integration speed assertions require strictly positive rx and tx after traffic, not merely non-negative values.
5. UDP integration tests prove a UDP allocation reports an active UDP session and strictly positive UDP rx/tx speeds after traffic; snapshot/export tests also prove UDP rows are emitted and a `both` allocation produces separate `{protocol="tcp"}` and `{protocol="udp"}` series for the same port.
6. Deleting an allocation while a TCP connection is open does not crash or use freed listener memory; the next scrape omits the deleted label, closing the client exactly once is safe, and stale rate samples are removed.
7. Existing `/v1/metrics` JSON endpoint still returns JSON counter names.

## Non-goals

- Do not remove or rename existing `/v1/metrics` JSON metrics.
- Do not introduce histograms or Prometheus counters beyond what is needed for requested gauges.
- Do not add unauthenticated metrics access.
- Do not expose allocation IDs as labels; requested labels are only `port` and `protocol`.

## Workflow gates

Implementation must use `superpowers:subagent-driven-development` with fresh bounded subagents for the plan tasks. After code implementation and tests pass, an independent implementation reviewer must compare the diff against this spec and return `APPROVE` before documentation is updated. Only after that approval may docs be updated, verified, committed, and pushed.


Test helpers that allocate response metadata must provide a `deinit` method and all tests must call it so content-type parsing does not introduce leaks. `src/prometheus_exporter.zig` unit tests must be imported by `test_root.zig` or otherwise wired into `zig build test`.
