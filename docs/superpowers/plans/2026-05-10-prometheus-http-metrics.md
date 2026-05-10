# Prometheus HTTP Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add authenticated `GET /metrics` Prometheus exposition for current per-port/protocol connection count and rx/tx speeds.

**Architecture:** Keep existing hot-path atomics as the source of truth, snapshot concrete listener rows from `RuntimeManager`, compute per-scrape byte rates in a small stateful calculator, and use the third-party `karlseguin/metrics.zig` library to render labeled Prometheus gauges with `compat.io()` and proper `GaugeVec.deinit()`. Preserve existing `/v1/metrics` JSON behavior.

**Tech Stack:** Zig 0.16.0, relayd HTTP server/runtime modules, `github.com/karlseguin/metrics.zig` Prometheus formatting library, `zig build test`.

---

## File Structure

- Create: `build.zig.zon` — declare the `metrics` package dependency.
- Modify: `build.zig` — import the dependency as `metrics_prometheus` into executable and test modules.
- Modify: `src/metrics.zig` — keep existing counters/gauges; add reusable rate snapshot calculator if not placed in exporter.
- Create: `src/prometheus_exporter.zig` — render required labeled gauges using the third-party library.
- Modify: `test_root.zig` — import `src/prometheus_exporter.zig` so exporter and rate unit tests run under `zig build test`.
- Modify: `src/runtime/manager.zig` — track per-listener concrete protocol active sessions and byte totals through a safe session-held metrics state; expose listener metrics snapshots.
- Modify: `src/service/allocation_service.zig` — expose a service-level snapshot wrapper if needed by HTTP without breaking service encapsulation.
- Modify: `src/http/server.zig` — add authenticated `GET /metrics` route and Prometheus response content type.
- Modify: `tests/integration/http_api_test.zig` — add auth, Prometheus format/content-type, active connection 1->0, speed, UDP/both label, delete-while-open, and `/v1/metrics` compatibility tests.
- Modify: `docs/API.md` and `docs/api/http.md` — document `/metrics` auth, Prometheus format, metrics, labels, and speed semantics.

---

### Task 1: Add failing HTTP tests for Prometheus `/metrics`

**Files:**
- Modify: `tests/integration/http_api_test.zig`

- [ ] **Step 1: Add unauthenticated HTTP helper**

First update `HttpResponse` so allocated response metadata is cleaned consistently:

```zig
const HttpResponse = struct {
    status: u16,
    content_type: ?[]u8 = null,
    body: []u8,

    fn deinit(self: *HttpResponse, allocator: std.mem.Allocator) void {
        if (self.content_type) |value| allocator.free(value);
        allocator.free(self.body);
        self.* = undefined;
    }
};
```

Then update all existing test call sites from `defer std.testing.allocator.free(response.body);` to `defer response.deinit(std.testing.allocator);`, including old `/v1/...` tests, so `content_type` never leaks.

Add a helper next to `doHttp` that can send requests with or without the Authorization header:

```zig
fn doHttpWithAuth(allocator: std.mem.Allocator, port: u16, method: []const u8, path: []const u8, body: []const u8, auth_header: ?[]const u8) !HttpResponse {
    const addr = try config.parseIpLiteral("127.0.0.1", port);
    const stream = try net.tcpConnectToAddress(addr);
    defer stream.close();

    const auth = auth_header orelse "";
    const request = try std.fmt.allocPrint(
        allocator,
        "{s} {s} HTTP/1.1\r\nHost: 127.0.0.1\r\n{s}Content-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
        .{ method, path, auth, body.len, body },
    );
    defer allocator.free(request);
    _ = try compat.write(stream.handle, request);
    try compat.shutdown(stream.handle, .send);
    return try readHttpResponse(allocator, stream.handle);
}
```

Extract the response parsing body of existing `doHttp` into:

```zig
fn readHttpResponse(allocator: std.mem.Allocator, fd: std.posix.fd_t) !HttpResponse {
    var header_buf = std.ArrayList(u8).empty;
    defer header_buf.deinit(allocator);
    var byte: [1]u8 = undefined;
    while (std.mem.indexOf(u8, header_buf.items, "\r\n\r\n") == null) {
        const amt = try std.posix.read(fd, &byte);
        if (amt == 0) return error.InvalidHttpResponse;
        try header_buf.append(allocator, byte[0]);
    }
    const headers = header_buf.items;
    const sep = std.mem.indexOf(u8, headers, "\r\n\r\n") orelse return error.InvalidHttpResponse;
    const status_line_end = std.mem.indexOf(u8, headers, "\r\n") orelse return error.InvalidHttpResponse;
    const status_line = headers[0..status_line_end];
    const parts_idx = std.mem.indexOfScalar(u8, status_line, ' ') orelse return error.InvalidHttpResponse;
    const status = try std.fmt.parseInt(u16, status_line[parts_idx + 1 .. parts_idx + 4], 10);

    var content_length: usize = 0;
    var content_type: ?[]u8 = null;
    errdefer if (content_type) |value| allocator.free(value);
    var line_it = std.mem.splitSequence(u8, headers[status_line_end + 2 .. sep], "\r\n");
    while (line_it.next()) |line| {
        if (std.ascii.startsWithIgnoreCase(line, "content-length:")) {
            const value = std.mem.trim(u8, line["content-length:".len..], " ");
            content_length = try std.fmt.parseInt(usize, value, 10);
        } else if (std.ascii.startsWithIgnoreCase(line, "content-type:")) {
            const value = std.mem.trim(u8, line["content-type:".len..], " ");
            content_type = try allocator.dupe(u8, value);
        }
    }

    const already_read = headers.len - (sep + 4);
    var body_copy = try allocator.alloc(u8, content_length);
    if (already_read > 0) @memcpy(body_copy[0..already_read], headers[sep + 4 ..]);
    var offset = already_read;
    while (offset < content_length) {
        const amt = try std.posix.read(fd, body_copy[offset..content_length]);
        if (amt == 0) break;
        offset += amt;
    }
    return .{ .status = status, .content_type = content_type, .body = body_copy };
}
```

Then simplify existing `doHttp` to call `doHttpWithAuth(..., "Authorization: Bearer secret-token\r\n")`.

- [ ] **Step 2: Add failing auth test**

```zig
test "prometheus metrics endpoint requires bearer auth" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    const http_port = try harness.http.assignedPort();
    var response = try doHttpWithAuth(std.testing.allocator, http_port, "GET", "/metrics", "", null);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 401), response.status);

    var wrong = try doHttpWithAuth(std.testing.allocator, http_port, "GET", "/metrics", "", "Authorization: Bearer wrong-token\r\n");
    defer wrong.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 401), wrong.status);
}
```

- [ ] **Step 3: Add failing format and label test**

```zig
test "prometheus metrics endpoint exposes labeled gauges" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    const http_port = try harness.http.assignedPort();
    var create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/allocations", "{\"protocol\":\"tcp\"}");
    defer create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), create_resp.status);
    const allocation_port = try extractJsonU16(create_resp.body, "port");

    var metrics_resp = try doHttp(std.testing.allocator, http_port, "GET", "/metrics", "");
    defer metrics_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), metrics_resp.status);
    try std.testing.expect(metrics_resp.content_type != null);
    try std.testing.expectEqualStrings("text/plain; version=0.0.4; charset=utf-8", metrics_resp.content_type.?);

    const expected_label = try std.fmt.allocPrint(std.testing.allocator, "{{port=\"{d}\",protocol=\"tcp\"}}", .{allocation_port});
    defer std.testing.allocator.free(expected_label);
    try std.testing.expect(std.mem.indexOf(u8, metrics_resp.body, "# TYPE relayd_connections_current gauge") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics_resp.body, "# TYPE relayd_rx_bytes_per_second gauge") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics_resp.body, "# TYPE relayd_tx_bytes_per_second gauge") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics_resp.body, expected_label) != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics_resp.body, "{\"") == null);
}
```

- [ ] **Step 4: Add failing active connection and speed test**

```zig
test "prometheus metrics endpoint reports active tcp connection and speeds" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    var echo = try startTcpEchoServer(std.testing.allocator);
    defer {
        echo.server.deinit();
        echo.thread.join();
    }

    const http_port = try harness.http.assignedPort();
    const create_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"protocol\":\"tcp\",\"target_port\":{d}}}", .{echo.port});
    defer std.testing.allocator.free(create_body);
    var create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/ports", create_body);
    defer create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), create_resp.status);
    const allocation_id = try extractJsonString(std.testing.allocator, create_resp.body, "id");
    defer std.testing.allocator.free(allocation_id);
    const allocation_port = try extractJsonU16(create_resp.body, "port");

    const target_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"id\":\"{s}\",\"host\":\"127.0.0.1\"}}", .{allocation_id});
    defer std.testing.allocator.free(target_body);
    var target_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/ports/target", target_body);
    defer target_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), target_resp.status);

    const forward_addr = try config.parseIpLiteral("127.0.0.1", allocation_port);
    const stream = try net.tcpConnectToAddress(forward_addr);
    var stream_open = true;
    defer if (stream_open) stream.close();

    var first_scrape = try doHttp(std.testing.allocator, http_port, "GET", "/metrics", "");
    defer first_scrape.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), first_scrape.status);

    _ = try compat.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    compat.sleep(20 * std.time.ns_per_ms);

    var metrics_resp = try doHttp(std.testing.allocator, http_port, "GET", "/metrics", "");
    defer metrics_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), metrics_resp.status);

    const active_line = try std.fmt.allocPrint(std.testing.allocator, "relayd_connections_current{{port=\"{d}\",protocol=\"tcp\"}} 1", .{allocation_port});
    defer std.testing.allocator.free(active_line);
    try std.testing.expect(std.mem.indexOf(u8, metrics_resp.body, active_line) != null);
    try expectMetricSampleAtLeast(metrics_resp.body, "relayd_rx_bytes_per_second", allocation_port, "tcp", 0.000001);
    try expectMetricSampleAtLeast(metrics_resp.body, "relayd_tx_bytes_per_second", allocation_port, "tcp", 0.000001);

If timing makes the second scrape race with byte accounting, wrap the second scrape in a bounded retry helper (for example 500 ms total, 20 ms sleep between attempts) and pass only when both rx and tx samples are strictly greater than zero. Do not accept `>= 0` because that would pass without implemented byte accounting.

    stream.close();
    stream_open = false;
    compat.sleep(20 * std.time.ns_per_ms);
    var closed_resp = try doHttp(std.testing.allocator, http_port, "GET", "/metrics", "");
    defer closed_resp.deinit(std.testing.allocator);
    const inactive_line = try std.fmt.allocPrint(std.testing.allocator, "relayd_connections_current{{port=\"{d}\",protocol=\"tcp\"}} 0", .{allocation_port});
    defer std.testing.allocator.free(inactive_line);
    try std.testing.expect(std.mem.indexOf(u8, closed_resp.body, inactive_line) != null);
}
```

Add helper:

```zig
fn expectMetricSampleAtLeast(body: []const u8, name: []const u8, port: u16, protocol: []const u8, minimum: f64) !void {
    var needle_buf: [128]u8 = undefined;
    const needle = try std.fmt.bufPrint(&needle_buf, "{s}{{port=\"{d}\",protocol=\"{s}\"}} ", .{ name, port, protocol });
    const start = std.mem.indexOf(u8, body, needle) orelse return error.InvalidHttpResponse;
    const value_start = start + needle.len;
    const rest = body[value_start..];
    const value_end = std.mem.indexOfScalar(u8, rest, '\n') orelse rest.len;
    const value = try std.fmt.parseFloat(f64, std.mem.trim(u8, rest[0..value_end], " \r"));
    try std.testing.expect(value >= minimum);
}
```


- [ ] **Step 4b: Add failing delete-while-open lifetime test**

Add a test that creates an active TCP allocation, opens a client connection, scrapes `/metrics` and observes the active line, deletes the allocation while the connection is still open, scrapes `/metrics` again and verifies the deleted `port`/`protocol` line is absent, then closes the client. This is required to prove sessions do not hold dangling `ListenerEntry` pointers and stale rate labels are removed.


- [ ] **Step 4c: Add failing UDP active-session and speed test**

Add a UDP echo helper and an integration test that creates a UDP allocation, binds it to the echo target, performs an initial `/metrics` scrape to seed the rate calculator, sends a UDP payload through the allocated port, receives the echoed payload, then scrapes `/metrics` with a bounded retry until these are true for the allocation port:

```text
relayd_connections_current{port="<port>",protocol="udp"} 1
relayd_rx_bytes_per_second{port="<port>",protocol="udp"} > 0
relayd_tx_bytes_per_second{port="<port>",protocol="udp"} > 0
```

This test is required because row-existence alone does not prove UDP active-session or byte-rate accounting.

- [ ] **Step 5: Verify tests fail for missing `/metrics`**

Run: `zig build test --summary all`

Expected now: failure because `/metrics` is not implemented or does not emit the required Prometheus series.

---

### Task 2: Add third-party Prometheus dependency, exporter, and rate tests

**Files:**
- Create: `build.zig.zon`
- Modify: `build.zig`
- Create: `src/prometheus_exporter.zig`

- [ ] **Step 1: Add dependency manifest**

Run:

```bash
zig fetch --save git+https://github.com/karlseguin/metrics.zig
```

This should create or update `build.zig.zon` with a `.metrics` dependency. Keep the generated hash from the command; do not hand-invent it.

- [ ] **Step 2: Import dependency in build graph**

In `build.zig`, after creating `exe_module`, add:

```zig
const prometheus_dep = b.dependency("metrics", .{ .target = target, .optimize = optimize });
const prometheus_module = prometheus_dep.module("metrics");
exe_module.addImport("metrics_prometheus", prometheus_module);
```

Also add the same import to `test_module` after it is created:

```zig
test_module.addImport("metrics_prometheus", prometheus_module);
```

- [ ] **Step 3: Add exporter module**

Create `src/prometheus_exporter.zig` with these public contracts:

```zig
const std = @import("std");
const model = @import("model/allocation.zig");
const prom = @import("metrics_prometheus");

pub const ListenerMetricsSnapshot = struct {
    port: u16,
    protocol: model.Protocol,
    connections_current: u64,
    rx_bytes_total: u64,
    tx_bytes_total: u64,
};

pub const ListenerMetricsRate = struct {
    port: u16,
    protocol: model.Protocol,
    connections_current: u64,
    rx_bytes_per_second: f64,
    tx_bytes_per_second: f64,
};

pub const RateCalculator = struct {
    // implementation in Task 4
};

pub fn render(allocator: std.mem.Allocator, rows: []const ListenerMetricsRate) ![]u8 {
    // implementation in this task
}
```

Implement `render` using `prom.GaugeVec` for:

- `relayd_connections_current`
- `relayd_rx_bytes_per_second`
- `relayd_tx_bytes_per_second`

Labels must be `struct { port: []const u8, protocol: []const u8 }`. Convert the `u16` port to a short stack buffer per row before setting labels. Initialize each vector with the Zig 0.16 signature `try GaugeVec.init(allocator, compat.io(), name, opts, .{})`, call `deinit()` for every vector before returning, and write to `std.Io.Writer.Allocating`. Import `compat.zig` so the exporter can supply `compat.io()` to the library.

- [ ] **Step 4: Add exporter and rate unit tests**

Add `const prometheus_exporter = @import("src/prometheus_exporter.zig");` to `test_root.zig` so these tests are included in `zig build test`. Add tests in `src/prometheus_exporter.zig` verifying `render` emits:

```text
# TYPE relayd_connections_current gauge
relayd_connections_current{port="1234",protocol="tcp"} 2
# TYPE relayd_rx_bytes_per_second gauge
relayd_rx_bytes_per_second{port="1234",protocol="tcp"} 10
# TYPE relayd_tx_bytes_per_second gauge
relayd_tx_bytes_per_second{port="1234",protocol="tcp"} 20
```

Also add deterministic `RateCalculator` unit tests:

```zig
test "rate calculator returns zero for first sample then positive deltas" { ... }
test "rate calculator clamps non advancing time to zero" { ... }
test "rate calculator removes stale labels" { ... }
```

The stale-label test must scrape one row, then scrape an empty row list, then scrape the same row again and verify it is treated as a first sample with `0` rates.

- [ ] **Step 5: Verify exporter tests**

Run: `zig build test --summary all`

Expected: compile may still fail until Task 3/4 route integration is complete; exporter-specific failures should be fixed before continuing.

---

### Task 3: Track per-listener concrete protocol connection and byte totals with safe session lifetime

**Files:**
- Modify: `src/runtime/manager.zig`
- Modify: `src/metrics.zig` if helper counter APIs are needed

- [ ] **Step 1: Add session-safe `ListenerMetricsState`**

Do not store long-lived session pointers to `ListenerEntry`. Add a separately allocated metrics state:

```zig
const ListenerMetricsState = struct {
    allocator: std.mem.Allocator,
    ref_count: std.atomic.Value(u32),
    port: u16,
    protocol: model.Protocol,
    tcp_active_sessions: Metrics.Gauge = .{},
    udp_active_sessions: Metrics.Gauge = .{},
    tcp_rx_bytes_total: Metrics.Counter = .{},
    tcp_tx_bytes_total: Metrics.Counter = .{},
    udp_rx_bytes_total: Metrics.Counter = .{},
    udp_tx_bytes_total: Metrics.Counter = .{},

    fn retain(self: *ListenerMetricsState) void { _ = self.ref_count.fetchAdd(1, .monotonic); }
    fn release(self: *ListenerMetricsState) void {
        if (self.ref_count.fetchSub(1, .acq_rel) == 1) self.allocator.destroy(self);
    }
};
```

Create it in `createEntry` with `ref_count = 1` for the registry/listener owner and store `metrics_state: *ListenerMetricsState` on `ListenerEntry`. In `destroyEntry`, release the listener owner reference after unregistering/removing the entry. Sessions must `retain()` before storing the pointer and `release()` in their cleanup path.

- [ ] **Step 2: Count TCP active sessions per listener**

Add a `metrics_state: *ListenerMetricsState` pointer to `TcpRuntimeSession`, `PendingAcceptedTcpSession`, and `TcpSessionCtx` where sessions are created from a listener. Retain the state before it can outlive the listener lock/entry. Do not assume global `tcp_active_sessions` is already correct for all paths: explicitly increment `metrics_state.tcp_active_sessions` and the global gauge for default copy/splice `TcpSessionCtx` after upstream connection succeeds, and decrement both exactly once in the thread defer after that increment. For runtime-session, workerized runtime-session, pending-accept, sharded-accept, and accept-balanced paths, increment the owning metrics state wherever global `tcp_active_sessions` increments; decrement the same state in every removal, failed registration cleanup, shutdown, worker removal, and error branch where global `tcp_active_sessions` decrements. Release the retained metrics state in every session cleanup branch after the final decrement/byte update.

- [ ] **Step 3: Count UDP active sessions per listener**

Increment `entry.metrics_state.udp_active_sessions` or `shard.entry.metrics_state.udp_active_sessions` in `getOrCreateUdpSession` and `getOrCreateUdpSessionOnWorker` when global `udp_active_sessions` increments. UDP sessions are removed during listener deletion today; if any UDP session can outlive `ListenerEntry`, retain/release `ListenerMetricsState` for it as well. Decrement the same metrics-state gauge in `removeUdpSessionLocked`, `removeUdpSessionFromWorkerShardLocked`, and error cleanup branches where global `udp_active_sessions` decrements.

- [ ] **Step 4: Count TCP bytes per listener**

Add metric parameters to TCP IO helpers:

- `runTcpCopy(ctx)` or `runTcpCopy(client_fd, upstream_fd, metrics_state)` so copy pumps know direction.
- `copyPumpThread` increments rx for client-to-upstream reads and tx for upstream-to-client successful writes.
- `fillTcpRuntimeBuffer` increments rx when reading from `session.client_fd`.
- `flushTcpRuntimeBuffer` increments tx when writing to `session.client_fd`.
- `splicePump` returns moved byte totals or accepts a counter pointer so client-to-upstream splice increments rx and upstream-to-client splice increments tx.

- [ ] **Step 5: Count UDP bytes per listener**

Where existing code increments aggregate `udp_bytes_in_total`, also increment `entry.metrics_state.udp_rx_bytes_total` or `shard.entry.metrics_state.udp_rx_bytes_total`. Where existing code increments aggregate `udp_bytes_out_total` after successful send-to-client, also increment `entry.metrics_state.udp_tx_bytes_total` or `shard.entry.metrics_state.udp_tx_bytes_total`.

- [ ] **Step 6: Add snapshot method**

Add to `RuntimeManager`:

```zig
pub const ListenerMetricsSnapshot = @import("../prometheus_exporter.zig").ListenerMetricsSnapshot;

pub fn snapshotListenerMetrics(self: *RuntimeManager, allocator: std.mem.Allocator) !std.ArrayList(ListenerMetricsSnapshot) {
    var rows = std.ArrayList(ListenerMetricsSnapshot).empty;
    errdefer rows.deinit(allocator);
    self.registry_mutex.lock();
    defer self.registry_mutex.unlock();
    var it = self.listeners.iterator();
    while (it.next()) |kv| {
        const entry = kv.value_ptr.*;
        entry.mutex.lock();
        const port = entry.port;
        const protocol = entry.protocol;
        const state = entry.metrics_state;
        const tcp_row = ListenerMetricsSnapshot{
            .port = port,
            .protocol = .tcp,
            .connections_current = state.tcp_active_sessions.load(),
            .rx_bytes_total = state.tcp_rx_bytes_total.load(),
            .tx_bytes_total = state.tcp_tx_bytes_total.load(),
        };
        const udp_row = ListenerMetricsSnapshot{
            .port = port,
            .protocol = .udp,
            .connections_current = state.udp_active_sessions.load(),
            .rx_bytes_total = state.udp_rx_bytes_total.load(),
            .tx_bytes_total = state.udp_tx_bytes_total.load(),
        };
        entry.mutex.unlock();
        if (protocol == .tcp or protocol == .both) try rows.append(allocator, tcp_row);
        if (protocol == .udp or protocol == .both) try rows.append(allocator, udp_row);
    }
    return rows;
}
```

Use explicit lock/unlock as shown above; do not use `defer entry.mutex.unlock()` inside the loop. Snapshot rows are built only from current registry entries; deleted listener states retained by draining sessions are intentionally not emitted.

- [ ] **Step 7: Verify runtime tests**

Run: `zig build test --summary all`

Expected: active connection and byte tests may still fail until route/rate integration is done, but no compile errors from runtime changes.

---

### Task 4: Compute rates and add authenticated `/metrics` route

**Files:**
- Modify: `src/prometheus_exporter.zig`
- Modify: `src/http/server.zig`
- Modify: `src/service/allocation_service.zig` if needed

- [ ] **Step 1: Implement `RateCalculator`**

In `src/prometheus_exporter.zig`, implement a stateful calculator keyed by `(port, protocol)`:

```zig
pub const RateCalculator = struct {
    allocator: std.mem.Allocator,
    samples: std.AutoHashMap(Key, Sample),
    mutex: std.Thread.Mutex = .{},

    pub fn init(allocator: std.mem.Allocator) RateCalculator { ... }
    pub fn deinit(self: *RateCalculator) void { ... }
    pub fn calculate(self: *RateCalculator, snapshots: []const ListenerMetricsSnapshot, now_ms: i64) !std.ArrayList(ListenerMetricsRate) { ... }
};
```

`calculate` returns `0` rates for first samples and computes bytes/sec for repeated samples. Remove stale keys that are absent from the current snapshot.

- [ ] **Step 2: Store calculator in `HttpServer`**

Add to `HttpServer`:

```zig
metrics_rate_calculator: @import("../prometheus_exporter.zig").RateCalculator,
```

Initialize it in every `HttpServer` literal, including `src/app.zig` and all test harness/direct initializers in `tests/integration/http_api_test.zig`, with `prometheus_exporter.RateCalculator.init(allocator)`. Add an `HttpServer.deinit()` or explicit teardown call that deinitializes the calculator after `stop()`, and update `App.deinit`, `Harness.deinit`, and direct test defers to call it exactly once.

- [ ] **Step 3: Add service snapshot wrapper if needed**

If `HttpServer` should not access runtime internals directly, add to `Service`:

```zig
pub fn snapshotListenerMetrics(self: *Service, allocator: std.mem.Allocator) !std.ArrayList(prometheus_exporter.ListenerMetricsSnapshot) {
    return self.runtime.snapshotListenerMetrics(allocator);
}
```

- [ ] **Step 4: Add route and handler**

In `handleRequest`, after auth and before `/v1` routes:

```zig
if (request.head.method == .GET and std.mem.eql(u8, target, "/metrics")) return handlePrometheusMetrics(server, request);
```

Implement:

```zig
fn handlePrometheusMetrics(server: *HttpServer, request: *http.Server.Request) !void {
    var snapshots = try server.service.snapshotListenerMetrics(server.allocator);
    defer snapshots.deinit(server.allocator);
    var rates = try server.metrics_rate_calculator.calculate(snapshots.items, compat.milliTimestamp());
    defer rates.deinit(server.allocator);
    const payload = try prometheus_exporter.render(server.allocator, rates.items);
    defer server.allocator.free(payload);
    try request.respond(payload, .{
        .status = .ok,
        .keep_alive = false,
        .extra_headers = &.{.{ .name = "content-type", .value = "text/plain; version=0.0.4; charset=utf-8" }},
    });
}
```

- [ ] **Step 5: Preserve `/v1/metrics`**

Keep existing `handleMetrics` and `encodeMetrics` JSON route unchanged for `GET /v1/metrics`.

- [ ] **Step 6: Verify HTTP tests pass**

Run: `zig build test --summary all`

Expected: all tests pass.

---

### Task 5: Independent implementation review, documentation, final verification, commit, and push

**Files:**
- Modify: `docs/API.md`
- Modify: `docs/api/http.md`

- [ ] **Step 1: Independent spec implementation review before docs**

Dispatch an independent review agent with the spec file, plan file, test output, and diff. It must answer `APPROVE` only if implementation satisfies every spec requirement. If it does not approve, fix issues and repeat review until `APPROVE`. Do not update docs until this gate is approved.

- [ ] **Step 2: Document `/metrics` in API docs after APPROVE**

Add a section describing:

- `GET /metrics`
- Required header: `Authorization: Bearer <token>`
- Response: Prometheus text exposition
- Content-Type: `text/plain; version=0.0.4; charset=utf-8`
- Metrics:
  - `relayd_connections_current{port="<port>",protocol="tcp|udp"}`
  - `relayd_rx_bytes_per_second{port="<port>",protocol="tcp|udp"}`
  - `relayd_tx_bytes_per_second{port="<port>",protocol="tcp|udp"}`
- Speed rates are calculated from byte-total deltas between scrapes; first scrape reports `0` rates for new labels.
- Existing `GET /v1/metrics` remains JSON.

- [ ] **Step 3: Run full verification**

Run:

```bash
zig build test --summary all
```

Expected: all tests pass.

- [ ] **Step 4: Commit with Lore protocol**

After independent `APPROVE`, commit all intended files:

```bash
git add build.zig build.zig.zon src/metrics.zig src/prometheus_exporter.zig src/runtime/manager.zig src/service/allocation_service.zig src/http/server.zig tests/integration/http_api_test.zig docs/API.md docs/api/http.md docs/superpowers/specs/2026-05-10-prometheus-http-metrics-design.md docs/superpowers/plans/2026-05-10-prometheus-http-metrics.md
git commit -m "Expose authenticated Prometheus listener metrics

Constraint: /metrics must keep HTTP Bearer Auth and export Prometheus text for connection count plus rx/tx speed by port and protocol.
Rejected: Replacing /v1/metrics JSON | Existing compatibility should remain intact.
Confidence: high
Scope-risk: moderate
Directive: Keep hot-path metric updates allocation-free; use scrape-time allocation only for snapshots and Prometheus rendering.
Tested: zig build test --summary all
Not-tested: External Prometheus server scrape integration"
```

- [ ] **Step 5: Push**

Run:

```bash
git push
```

Expected: push succeeds to the current branch remote.

---

## Subagent-Driven Execution Ownership

Implementation must use `superpowers:subagent-driven-development` with fresh subagents and these bounded ownership slices:

1. **Test subagent:** owns `tests/integration/http_api_test.zig` and test-only helpers. It writes failing tests first and records the failing `zig build test --summary all` evidence.
2. **Exporter subagent:** owns `build.zig.zon`, `build.zig`, and `src/prometheus_exporter.zig`. It integrates `karlseguin/metrics.zig`, uses `compat.io()` for `GaugeVec`, calls `deinit()`, and owns rate calculator unit tests.
3. **Runtime subagent:** owns `src/runtime/manager.zig` and any minimal `src/metrics.zig` helper changes. It tracks per-listener TCP/UDP active sessions and rx/tx byte totals across every concrete path.
4. **HTTP/service subagent:** owns `src/http/server.zig`, `src/service/allocation_service.zig`, and `src/app.zig`. It adds the authenticated `/metrics` route and lifecycle wiring.
5. **Documentation subagent:** runs only after independent implementation `APPROVE`; owns `docs/API.md` and `docs/api/http.md`.

Each implementation subagent must be followed by spec-compliance review and code-quality review before the next dependent slice is treated as complete. If reviewers request changes, re-dispatch fixes and re-review until approved.

## Additional Required Tests

Add these tests before implementation is considered complete:

- `prometheus metrics endpoint rejects wrong bearer token` or include wrong-token assertion in the auth test.
- `prometheus metrics endpoint uses prometheus content type` by parsing `Content-Type` from the response.
- `prometheus metrics endpoint reports active tcp connection returning to zero` by scraping while a TCP client is open and again after close, with explicit stream ownership to avoid double-close.
- `rate calculator returns zero for first scrape and positive rx tx rates after deltas` in `src/prometheus_exporter.zig`; integration speed assertions must require strictly positive rx and tx after traffic.
- `rate calculator clamps zero elapsed time to zero` in `src/prometheus_exporter.zig`.
- `rate calculator removes stale labels` in `src/prometheus_exporter.zig`.
- `listener metrics snapshot emits tcp and udp rows for both allocations` via a deterministic runtime/service test or HTTP scrape test that creates a `both` allocation and checks both Prometheus label series.
- `udp prometheus metrics report active session and positive rx tx rates` by sending a UDP datagram through an active UDP allocation to a UDP echo target, scraping once to seed rates, sending traffic, scraping with bounded retry until `relayd_connections_current{protocol="udp"}` is `1` and both UDP rx/tx speeds are strictly positive, then allowing/removing the session and verifying no leaks.
- `listener metrics snapshot emits udp row` via deterministic snapshot/export coverage; this is in addition to the UDP traffic/rate integration test, not a substitute.
- `deleting allocation while tcp connection is open removes prometheus label safely` by opening a TCP connection, scraping active `1`, deleting the allocation, scraping again to ensure the port/protocol line is absent, then closing the TCP client without crash/leak/use-after-free under `zig build test`.
