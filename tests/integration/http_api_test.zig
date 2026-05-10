const std = @import("std");
const compat = @import("../../src/compat.zig");
const net = @import("../../src/net_compat.zig");
const config = @import("../../src/config.zig");
const metrics_mod = @import("../../src/metrics.zig");
const sqlite = @import("../../src/storage/sqlite.zig");
const runtime = @import("../../src/runtime/manager.zig");
const service_mod = @import("../../src/service/allocation_service.zig");
const http_mod = @import("../../src/http/server.zig");
const prometheus_exporter = @import("../../src/prometheus_exporter.zig");

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

fn tempDbPath(allocator: std.mem.Allocator) ![]u8 {
    try compat.makePath(".zig-cache/integration-tests");
    return std.fmt.allocPrint(allocator, ".zig-cache/integration-tests/{d}.sqlite", .{compat.nanoTimestamp()});
}

const Harness = struct {
    allocator: std.mem.Allocator,
    repo: sqlite.Repository,
    metrics: metrics_mod.Metrics,
    runtime_manager: runtime.RuntimeManager,
    service: service_mod.Service,
    http: http_mod.HttpServer,
    db_path: []u8,

    fn init(allocator: std.mem.Allocator) !*Harness {
        const self = try allocator.create(Harness);
        const db_path = try tempDbPath(allocator);
        self.* = .{
            .allocator = allocator,
            .repo = try sqlite.Repository.open(allocator, db_path),
            .metrics = .{},
            .runtime_manager = undefined,
            .service = undefined,
            .http = undefined,
            .db_path = db_path,
        };
        self.runtime_manager = try runtime.RuntimeManager.init(allocator, &self.metrics, .{});
        try self.runtime_manager.start();
        const base: u16 = @intCast(56000 + @as(u16, @intCast(@mod(compat.nanoTimestamp(), 500))));
        self.service = service_mod.Service.init(allocator, &self.repo, &self.runtime_manager, .{ .start = base, .end = base + 50 }, 2000);
        self.http = .{
            .allocator = allocator,
            .service = &self.service,
            .metrics = &self.metrics,
            .host = try allocator.dupe(u8, "127.0.0.1"),
            .port = 0,
            .auth_token = try allocator.dupe(u8, "secret-token"),
            .rate_calculator = prometheus_exporter.RateCalculator.init(allocator),
        };
        try self.http.start();
        return self;
    }

    fn deinit(self: *Harness) void {
        self.http.deinit();
        self.allocator.free(self.http.host);
        self.allocator.free(self.http.auth_token);
        self.runtime_manager.deinit();
        self.repo.close();
        compat.deleteFile(self.db_path);
        self.allocator.free(self.db_path);
        self.allocator.destroy(self);
    }
};

fn doHttp(allocator: std.mem.Allocator, port: u16, method: []const u8, path: []const u8, body: []const u8) !HttpResponse {
    return doHttpWithAuth(allocator, port, method, path, body, "Authorization: Bearer secret-token\r\n");
}

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
    const body_copy = try allocator.alloc(u8, content_length);
    errdefer allocator.free(body_copy);
    if (already_read > 0) {
        const copy_len = @min(already_read, content_length);
        @memcpy(body_copy[0..copy_len], headers[sep + 4 ..][0..copy_len]);
    }
    var offset = @min(already_read, content_length);
    while (offset < content_length) {
        const amt = try std.posix.read(fd, body_copy[offset..content_length]);
        if (amt == 0) break;
        offset += amt;
    }
    return .{ .status = status, .content_type = content_type, .body = body_copy };
}

fn startTcpEchoServer(_: std.mem.Allocator) !struct { server: net.Server, port: u16, thread: std.Thread } {
    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    const server = try addr.listen(.{ .reuse_address = true });
    var bound: net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(net.Address);
    try compat.getsockname(server.stream.handle, &bound.any, &len);
    const port = bound.getPort();
    const thread = try std.Thread.spawn(.{}, tcpEchoThread, .{server.stream.handle});
    return .{ .server = server, .port = port, .thread = thread };
}

fn tcpEchoThread(server_fd: std.posix.fd_t) void {
    var server = net.Server{ .listen_address = undefined, .stream = .{ .handle = server_fd } };
    const conn = server.accept() catch return;
    defer conn.stream.close();
    var buf: [1024]u8 = undefined;
    while (true) {
        const amt = std.posix.read(conn.stream.handle, &buf) catch break;
        if (amt == 0) break;
        _ = compat.write(conn.stream.handle, buf[0..amt]) catch break;
    }
}
fn startUdpEchoServer() !struct { fd: std.posix.fd_t, port: u16, thread: std.Thread } {
    const fd = try compat.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC, std.posix.IPPROTO.UDP);
    errdefer closeIgnore(fd);
    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    try compat.bind(fd, &addr.any, addr.getOsSockLen());
    var bound: net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(net.Address);
    try compat.getsockname(fd, &bound.any, &len);
    const thread = try std.Thread.spawn(.{}, udpEchoThread, .{fd});
    return .{ .fd = fd, .port = bound.getPort(), .thread = thread };
}

fn udpEchoThread(fd: std.posix.fd_t) void {
    var buf: [1024]u8 = undefined;
    var addr: net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(net.Address);
    const amt = compat.recvfrom(fd, &buf, 0, &addr.any, &len) catch return;
    _ = compat.sendto(fd, buf[0..amt], 0, &addr.any, addr.getOsSockLen()) catch {};
}

fn createUdpClient() !std.posix.fd_t {
    return compat.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK, std.posix.IPPROTO.UDP);
}

fn wakeTcpEchoPort(port: u16) void {
    const addr = config.parseIpLiteral("127.0.0.1", port) catch return;
    const stream = net.tcpConnectToAddress(addr) catch return;
    defer stream.close();
    _ = compat.write(stream.handle, "x") catch {};
}

fn wakeUdpEchoPort(port: u16) void {
    const client = createUdpClient() catch return;
    defer closeIgnore(client);
    const addr = config.parseIpLiteral("127.0.0.1", port) catch return;
    _ = compat.sendto(client, "x", 0, &addr.any, addr.getOsSockLen()) catch {};
}

fn closeIgnore(fd: std.posix.fd_t) void {
    switch (std.posix.errno(std.posix.system.close(fd))) {
        .SUCCESS, .BADF => {},
        else => {},
    }
}

fn sendUdpAndExpect(client_fd: std.posix.fd_t, target: net.Address, payload: []const u8, expected: []const u8, timeout_ms: u32) !void {
    _ = try compat.sendto(client_fd, payload, 0, &target.any, target.getOsSockLen());
    var buf: [128]u8 = undefined;
    const amt = (try recvUdpWithTimeout(client_fd, &buf, timeout_ms)) orelse return error.Timeout;
    try std.testing.expectEqualStrings(expected, buf[0..amt]);
}

fn recvUdpWithTimeout(fd: std.posix.fd_t, buf: []u8, timeout_ms: u32) !?usize {
    const deadline_ms = compat.milliTimestamp() + @as(i64, @intCast(timeout_ms));
    while (compat.milliTimestamp() < deadline_ms) {
        const amt = compat.recv(fd, buf, 0) catch |err| switch (err) {
            error.WouldBlock => {
                compat.sleep(5 * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
        return amt;
    }
    return null;
}

fn metricNeedle(buf: []u8, name: []const u8, port: u16, protocol: []const u8) ![]u8 {
    return try std.fmt.bufPrint(buf, "{s}{{port=\"{d}\",protocol=\"{s}\"}} ", .{ name, port, protocol });
}

fn expectMetricSampleAtLeast(body: []const u8, name: []const u8, port: u16, protocol: []const u8, minimum: f64) !void {
    var needle_buf: [128]u8 = undefined;
    const needle = try metricNeedle(&needle_buf, name, port, protocol);
    const start = std.mem.indexOf(u8, body, needle) orelse return error.InvalidHttpResponse;
    const value_start = start + needle.len;
    const rest = body[value_start..];
    const value_end = std.mem.indexOfScalar(u8, rest, '\n') orelse rest.len;
    const value = try std.fmt.parseFloat(f64, std.mem.trim(u8, rest[0..value_end], " \r"));
    try std.testing.expect(value >= minimum);
}

fn bodyHasMetricSampleAtLeast(body: []const u8, name: []const u8, port: u16, protocol: []const u8, minimum: f64) bool {
    expectMetricSampleAtLeast(body, name, port, protocol, minimum) catch return false;
    return true;
}

fn expectMetricLine(body: []const u8, name: []const u8, port: u16, protocol: []const u8, value: []const u8) !void {
    var line_buf: [160]u8 = undefined;
    const line = try std.fmt.bufPrint(&line_buf, "{s}{{port=\"{d}\",protocol=\"{s}\"}} {s}", .{ name, port, protocol, value });
    try std.testing.expect(std.mem.indexOf(u8, body, line) != null);
}

fn scrapeUntilTcpSpeedsPositive(allocator: std.mem.Allocator, http_port: u16, allocation_port: u16) !HttpResponse {
    const deadline_ms = compat.milliTimestamp() + 500;
    var last = try doHttp(allocator, http_port, "GET", "/metrics", "");
    while (true) {
        if (bodyHasMetricSampleAtLeast(last.body, "relayd_rx_bytes_per_second", allocation_port, "tcp", 0.000001) and
            bodyHasMetricSampleAtLeast(last.body, "relayd_tx_bytes_per_second", allocation_port, "tcp", 0.000001))
        {
            return last;
        }
        if (compat.milliTimestamp() >= deadline_ms) return last;
        last.deinit(allocator);
        compat.sleep(20 * std.time.ns_per_ms);
        last = try doHttp(allocator, http_port, "GET", "/metrics", "");
    }
}

fn scrapeUntilUdpActiveAndSpeedsPositive(allocator: std.mem.Allocator, http_port: u16, allocation_port: u16) !HttpResponse {
    const deadline_ms = compat.milliTimestamp() + 500;
    var last = try doHttp(allocator, http_port, "GET", "/metrics", "");
    while (true) {
        const active = blk: {
            var line_buf: [160]u8 = undefined;
            const line = try std.fmt.bufPrint(&line_buf, "relayd_connections_current{{port=\"{d}\",protocol=\"udp\"}} 1", .{allocation_port});
            break :blk std.mem.indexOf(u8, last.body, line) != null;
        };
        if (active and
            bodyHasMetricSampleAtLeast(last.body, "relayd_rx_bytes_per_second", allocation_port, "udp", 0.000001) and
            bodyHasMetricSampleAtLeast(last.body, "relayd_tx_bytes_per_second", allocation_port, "udp", 0.000001))
        {
            return last;
        }
        if (compat.milliTimestamp() >= deadline_ms) return last;
        last.deinit(allocator);
        compat.sleep(20 * std.time.ns_per_ms);
        last = try doHttp(allocator, http_port, "GET", "/metrics", "");
    }
}

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

test "prometheus metrics endpoint emits separate labels for dual protocol allocations" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    const http_port = try harness.http.assignedPort();
    var create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/allocations", "{\"protocol\":\"both\"}");
    defer create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), create_resp.status);
    const allocation_port = try extractJsonU16(create_resp.body, "port");

    var metrics_resp = try doHttp(std.testing.allocator, http_port, "GET", "/metrics", "");
    defer metrics_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), metrics_resp.status);
    try expectMetricLine(metrics_resp.body, "relayd_connections_current", allocation_port, "tcp", "0");
    try expectMetricLine(metrics_resp.body, "relayd_connections_current", allocation_port, "udp", "0");
}

test "prometheus metrics endpoint reports active tcp connection and speeds" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    var echo = try startTcpEchoServer(std.testing.allocator);
    defer {
        wakeTcpEchoPort(echo.port);
        echo.thread.join();
        echo.server.deinit();
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

    var metrics_resp = try scrapeUntilTcpSpeedsPositive(std.testing.allocator, http_port, allocation_port);
    defer metrics_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), metrics_resp.status);
    try expectMetricLine(metrics_resp.body, "relayd_connections_current", allocation_port, "tcp", "1");
    try expectMetricSampleAtLeast(metrics_resp.body, "relayd_rx_bytes_per_second", allocation_port, "tcp", 0.000001);
    try expectMetricSampleAtLeast(metrics_resp.body, "relayd_tx_bytes_per_second", allocation_port, "tcp", 0.000001);

    stream.close();
    stream_open = false;
    compat.sleep(20 * std.time.ns_per_ms);
    var closed_resp = try doHttp(std.testing.allocator, http_port, "GET", "/metrics", "");
    defer closed_resp.deinit(std.testing.allocator);
    try expectMetricLine(closed_resp.body, "relayd_connections_current", allocation_port, "tcp", "0");
}

test "prometheus metrics endpoint omits deleted tcp allocation while connection is open" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    var echo = try startTcpEchoServer(std.testing.allocator);
    defer {
        wakeTcpEchoPort(echo.port);
        echo.thread.join();
        echo.server.deinit();
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

    var active_scrape = try doHttp(std.testing.allocator, http_port, "GET", "/metrics", "");
    defer active_scrape.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), active_scrape.status);
    try expectMetricLine(active_scrape.body, "relayd_connections_current", allocation_port, "tcp", "1");

    const delete_path = try std.fmt.allocPrint(std.testing.allocator, "/v1/ports/{s}", .{allocation_id});
    defer std.testing.allocator.free(delete_path);
    var delete_resp = try doHttp(std.testing.allocator, http_port, "DELETE", delete_path, "");
    defer delete_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 204), delete_resp.status);

    var after_delete = try doHttp(std.testing.allocator, http_port, "GET", "/metrics", "");
    defer after_delete.deinit(std.testing.allocator);
    var label_buf: [96]u8 = undefined;
    const deleted_label = try std.fmt.bufPrint(&label_buf, "{{port=\"{d}\",protocol=\"tcp\"}}", .{allocation_port});
    try std.testing.expect(std.mem.indexOf(u8, after_delete.body, deleted_label) == null);

    stream.close();
    stream_open = false;
}

test "prometheus metrics endpoint reports active udp session and speeds" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    var echo = try startUdpEchoServer();
    defer {
        wakeUdpEchoPort(echo.port);
        echo.thread.join();
        closeIgnore(echo.fd);
    }

    const http_port = try harness.http.assignedPort();
    const create_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"protocol\":\"udp\",\"target_port\":{d}}}", .{echo.port});
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

    var first_scrape = try doHttp(std.testing.allocator, http_port, "GET", "/metrics", "");
    defer first_scrape.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), first_scrape.status);

    const client = try createUdpClient();
    defer closeIgnore(client);
    const target = try config.parseIpLiteral("127.0.0.1", allocation_port);
    try sendUdpAndExpect(client, target, "pong", "pong", 250);
    compat.sleep(20 * std.time.ns_per_ms);

    var metrics_resp = try scrapeUntilUdpActiveAndSpeedsPositive(std.testing.allocator, http_port, allocation_port);
    defer metrics_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), metrics_resp.status);
    try expectMetricLine(metrics_resp.body, "relayd_connections_current", allocation_port, "udp", "1");
    try expectMetricSampleAtLeast(metrics_resp.body, "relayd_rx_bytes_per_second", allocation_port, "udp", 0.000001);
    try expectMetricSampleAtLeast(metrics_resp.body, "relayd_tx_bytes_per_second", allocation_port, "udp", 0.000001);
}

test "http create target forward delete tcp" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    var echo = try startTcpEchoServer(std.testing.allocator);
    defer {
        wakeTcpEchoPort(echo.port);
        echo.thread.join();
        echo.server.deinit();
    }

    const http_port = try harness.http.assignedPort();
    const create_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"protocol\":\"tcp\",\"target_port\":{d}}}", .{echo.port});
    defer std.testing.allocator.free(create_body);
    var create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/ports", create_body);
    defer create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), create_resp.status);
    const created_id = try extractJsonString(std.testing.allocator, create_resp.body, "id");
    defer std.testing.allocator.free(created_id);
    const created_runtime_status = try extractJsonString(std.testing.allocator, create_resp.body, "runtime_status");
    defer std.testing.allocator.free(created_runtime_status);
    const created_port = try extractJsonU16(create_resp.body, "port");
    try std.testing.expectEqualStrings("rejecting_no_host", created_runtime_status);

    const target_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"id\":\"{s}\",\"host\":\"127.0.0.1\"}}", .{created_id});
    defer std.testing.allocator.free(target_body);
    var target_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/ports/target", target_body);
    defer target_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), target_resp.status);

    const forward_addr = try config.parseIpLiteral("127.0.0.1", created_port);
    const stream = try net.tcpConnectToAddress(forward_addr);
    defer stream.close();
    _ = try compat.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);

    const delete_path = try std.fmt.allocPrint(std.testing.allocator, "/v1/ports/{s}", .{created_id});
    defer std.testing.allocator.free(delete_path);
    var delete_resp = try doHttp(std.testing.allocator, http_port, "DELETE", delete_path, "");
    defer delete_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 204), delete_resp.status);
}

test "http metrics endpoint exposes tcp splice counters" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    const http_port = try harness.http.assignedPort();
    var response = try doHttp(std.testing.allocator, http_port, "GET", "/v1/metrics", "");
    defer response.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_splice_attempt_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_splice_success_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_splice_hard_failure_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_session_create_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_session_close_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_session_worker_dispatch_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_session_worker0_dispatch_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_session_worker1_dispatch_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_accept_handoff_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_accept_handoff_worker0_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_accept_handoff_worker1_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_accept_handoff_worker2_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_accept_handoff_worker3_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_listener_accept_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_listener_accept_worker0_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_listener_accept_worker1_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_listener_accept_worker2_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_listener_accept_worker3_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_upstream_connect_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_upstream_connect_fail_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_fast_path_packets_in_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_fast_path_packets_out_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_fast_path_gso_send_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_fast_path_gro_recv_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_io_uring_submit_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_io_uring_cqe_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_io_uring_fallback_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_io_uring_multishot_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_io_uring_buf_release_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_dataplane_redesign_packets_in_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_dataplane_redesign_packets_out_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_fast_path_fallback_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_fast_path_drop_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_worker_packets_in_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_worker_packets_out_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_worker0_packets_in_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_worker1_packets_in_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_worker2_packets_in_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"udp_worker3_packets_in_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_active_sessions\":") != null);
}

test "http start failure does not deadlock cleanup" {
    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    var occupied = try addr.listen(.{});
    defer occupied.deinit();

    var bound: net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(net.Address);
    try compat.getsockname(occupied.stream.handle, &bound.any, &len);

    const db_path = try tempDbPath(std.testing.allocator);
    defer {
        compat.deleteFile(db_path);
        std.testing.allocator.free(db_path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, db_path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var runtime_manager = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer runtime_manager.deinit();
    try runtime_manager.start();
    var service = service_mod.Service.init(std.testing.allocator, &repo, &runtime_manager, .{ .start = 57000, .end = 57100 }, 2000);
    var http = http_mod.HttpServer{
        .allocator = std.testing.allocator,
        .service = &service,
        .metrics = &metrics,
        .host = try std.testing.allocator.dupe(u8, "127.0.0.1"),
        .port = bound.getPort(),
        .auth_token = try std.testing.allocator.dupe(u8, "secret-token"),
        .rate_calculator = prometheus_exporter.RateCalculator.init(std.testing.allocator),
    };
    defer {
        http.deinit();
        std.testing.allocator.free(http.host);
        std.testing.allocator.free(http.auth_token);
    }

    try std.testing.expectError(error.AddressInUse, http.start());
}

test "http allocation and binding lifecycle endpoints" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    var echo = try startTcpEchoServer(std.testing.allocator);
    defer {
        wakeTcpEchoPort(echo.port);
        echo.thread.join();
        echo.server.deinit();
    }

    const http_port = try harness.http.assignedPort();

    const create_body = "{\"protocol\":\"tcp\"}";
    var create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/allocations", create_body);
    defer create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), create_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, create_resp.body, "\"target_port\":") == null);
    const allocation_id = try extractJsonString(std.testing.allocator, create_resp.body, "id");
    defer std.testing.allocator.free(allocation_id);
    const allocation_port = try extractJsonU16(create_resp.body, "port");

    const get_alloc_path = try std.fmt.allocPrint(std.testing.allocator, "/v1/allocations/{s}", .{allocation_id});
    defer std.testing.allocator.free(get_alloc_path);
    var get_alloc_resp = try doHttp(std.testing.allocator, http_port, "GET", get_alloc_path, "");
    defer get_alloc_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), get_alloc_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, get_alloc_resp.body, "\"target_port\":") == null);

    var list_alloc_resp = try doHttp(std.testing.allocator, http_port, "GET", "/v1/allocations", "");
    defer list_alloc_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), list_alloc_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, list_alloc_resp.body, allocation_id) != null);

    const get_binding_path = try std.fmt.allocPrint(std.testing.allocator, "/v1/allocations/{s}/binding", .{allocation_id});
    defer std.testing.allocator.free(get_binding_path);
    var missing_binding_resp = try doHttp(std.testing.allocator, http_port, "GET", get_binding_path, "");
    defer missing_binding_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), missing_binding_resp.status);

    const binding_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"host\":\"127.0.0.1\",\"target_port\":{d}}}", .{echo.port});
    defer std.testing.allocator.free(binding_body);
    var put_binding_resp = try doHttp(std.testing.allocator, http_port, "PUT", get_binding_path, binding_body);
    defer put_binding_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), put_binding_resp.status);
    const binding_allocation_id = try extractJsonString(std.testing.allocator, put_binding_resp.body, "allocation_id");
    defer std.testing.allocator.free(binding_allocation_id);
    try std.testing.expectEqualStrings(allocation_id, binding_allocation_id);
    try std.testing.expect(std.mem.indexOf(u8, put_binding_resp.body, "\"runtime_status\":\"active\"") != null);

    const forward_addr = try config.parseIpLiteral("127.0.0.1", allocation_port);
    const stream = try net.tcpConnectToAddress(forward_addr);
    defer stream.close();
    _ = try compat.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);

    var get_binding_resp = try doHttp(std.testing.allocator, http_port, "GET", get_binding_path, "");
    defer get_binding_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), get_binding_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, get_binding_resp.body, "\"effective_target_port\":") != null);

    var delete_binding_resp = try doHttp(std.testing.allocator, http_port, "DELETE", get_binding_path, "");
    defer delete_binding_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 204), delete_binding_resp.status);

    var get_binding_after_delete = try doHttp(std.testing.allocator, http_port, "GET", get_binding_path, "");
    defer get_binding_after_delete.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), get_binding_after_delete.status);

    var allocation_still_exists = try doHttp(std.testing.allocator, http_port, "GET", get_alloc_path, "");
    defer allocation_still_exists.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), allocation_still_exists.status);

    var aggregate_after_detach = try doHttp(std.testing.allocator, http_port, "GET", "/v1/ports", "");
    defer aggregate_after_detach.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), aggregate_after_detach.status);
    const detached_fragment = try std.fmt.allocPrint(std.testing.allocator, "\"id\":\"{s}\",\"protocol\":\"tcp\",\"port\":{d},\"target_port\":null", .{ allocation_id, allocation_port });
    defer std.testing.allocator.free(detached_fragment);
    try std.testing.expect(std.mem.indexOf(u8, aggregate_after_detach.body, detached_fragment) != null);

    var delete_alloc_resp = try doHttp(std.testing.allocator, http_port, "DELETE", get_alloc_path, "");
    defer delete_alloc_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 204), delete_alloc_resp.status);

    var get_binding_after_allocation_delete = try doHttp(std.testing.allocator, http_port, "GET", get_binding_path, "");
    defer get_binding_after_allocation_delete.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), get_binding_after_allocation_delete.status);
}

test "http dual protocol allocation endpoints return one aggregate row" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    const http_port = try harness.http.assignedPort();

    var create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/allocations", "{\"protocol\":\"both\"}");
    defer create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), create_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, create_resp.body, "\"protocol\":\"both\"") != null);
    const allocation_id = try extractJsonString(std.testing.allocator, create_resp.body, "id");
    defer std.testing.allocator.free(allocation_id);
    const allocation_port = try extractJsonU16(create_resp.body, "port");

    const get_alloc_path = try std.fmt.allocPrint(std.testing.allocator, "/v1/allocations/{s}", .{allocation_id});
    defer std.testing.allocator.free(get_alloc_path);
    var get_alloc_resp = try doHttp(std.testing.allocator, http_port, "GET", get_alloc_path, "");
    defer get_alloc_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), get_alloc_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, get_alloc_resp.body, "\"protocol\":\"both\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, get_alloc_resp.body, allocation_id) != null);
    try std.testing.expectEqual(@as(usize, 1), countOccurrences(get_alloc_resp.body, allocation_id));

    var list_alloc_resp = try doHttp(std.testing.allocator, http_port, "GET", "/v1/allocations", "");
    defer list_alloc_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), list_alloc_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, list_alloc_resp.body, "\"protocol\":\"both\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, list_alloc_resp.body, allocation_id) != null);
    try std.testing.expectEqual(@as(usize, 1), countOccurrences(list_alloc_resp.body, allocation_id));

    var list_ports_resp = try doHttp(std.testing.allocator, http_port, "GET", "/v1/ports", "");
    defer list_ports_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), list_ports_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, list_ports_resp.body, "\"protocol\":\"both\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, list_ports_resp.body, allocation_id) != null);
    try std.testing.expectEqual(@as(usize, 1), countOccurrences(list_ports_resp.body, allocation_id));

    const compat_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"protocol\":\"both\",\"target_port\":{d}}}", .{allocation_port});
    defer std.testing.allocator.free(compat_body);
    var compat_create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/ports", compat_body);
    defer compat_create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), compat_create_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, compat_create_resp.body, "\"protocol\":\"both\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, compat_create_resp.body, "\"target_port\":") != null);
    try std.testing.expectEqual(allocation_port, try extractJsonU16(compat_create_resp.body, "target_port"));
    const compat_id = try extractJsonString(std.testing.allocator, compat_create_resp.body, "id");
    defer std.testing.allocator.free(compat_id);

    var compat_list_resp = try doHttp(std.testing.allocator, http_port, "GET", "/v1/ports", "");
    defer compat_list_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), compat_list_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, compat_list_resp.body, compat_id) != null);
    try std.testing.expectEqual(@as(usize, 1), countOccurrences(compat_list_resp.body, compat_id));
    try std.testing.expect(countOccurrences(compat_list_resp.body, "\"protocol\":\"both\"") >= 2);
}

fn countOccurrences(haystack: []const u8, needle: []const u8) usize {
    var count: usize = 0;
    var rest = haystack;
    while (std.mem.indexOf(u8, rest, needle)) |idx| {
        count += 1;
        rest = rest[idx + needle.len ..];
    }
    return count;
}

fn extractJsonString(allocator: std.mem.Allocator, body: []const u8, key: []const u8) ![]u8 {
    const needle = try std.fmt.allocPrint(allocator, "\"{s}\":\"", .{key});
    defer allocator.free(needle);
    const start = std.mem.indexOf(u8, body, needle) orelse return error.InvalidHttpResponse;
    const value_start = start + needle.len;
    const rest = body[value_start..];
    const value_end = std.mem.indexOfScalar(u8, rest, '"') orelse return error.InvalidHttpResponse;
    return allocator.dupe(u8, rest[0..value_end]);
}

fn extractJsonU16(body: []const u8, key: []const u8) !u16 {
    var buf: [64]u8 = undefined;
    const needle = try std.fmt.bufPrint(&buf, "\"{s}\":", .{key});
    const start = std.mem.indexOf(u8, body, needle) orelse return error.InvalidHttpResponse;
    const rest = body[start + needle.len ..];
    var end: usize = 0;
    while (end < rest.len and rest[end] >= '0' and rest[end] <= '9') : (end += 1) {}
    if (end == 0) return error.InvalidHttpResponse;
    return std.fmt.parseInt(u16, rest[0..end], 10);
}
