const std = @import("std");
const config = @import("../../src/config.zig");
const metrics_mod = @import("../../src/metrics.zig");
const sqlite = @import("../../src/storage/sqlite.zig");
const runtime = @import("../../src/runtime/manager.zig");
const service_mod = @import("../../src/service/allocation_service.zig");
const http_mod = @import("../../src/http/server.zig");

const HttpResponse = struct {
    status: u16,
    body: []u8,
};

fn tempDbPath(allocator: std.mem.Allocator) ![]u8 {
    try std.fs.cwd().makePath(".zig-cache/integration-tests");
    return std.fmt.allocPrint(allocator, ".zig-cache/integration-tests/{d}.sqlite", .{std.time.nanoTimestamp()});
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
        const base: u16 = @intCast(56000 + @as(u16, @intCast(@mod(std.time.nanoTimestamp(), 500))));
        self.service = service_mod.Service.init(allocator, &self.repo, &self.runtime_manager, .{ .start = base, .end = base + 50 }, 2000);
        self.http = .{
            .allocator = allocator,
            .service = &self.service,
            .metrics = &self.metrics,
            .host = try allocator.dupe(u8, "127.0.0.1"),
            .port = 0,
            .auth_token = try allocator.dupe(u8, "secret-token"),
        };
        try self.http.start();
        return self;
    }

    fn deinit(self: *Harness) void {
        self.http.stop();
        self.allocator.free(self.http.host);
        self.allocator.free(self.http.auth_token);
        self.runtime_manager.deinit();
        self.repo.close();
        std.fs.cwd().deleteFile(self.db_path) catch {};
        self.allocator.free(self.db_path);
        self.allocator.destroy(self);
    }
};

fn doHttp(allocator: std.mem.Allocator, port: u16, method: []const u8, path: []const u8, body: []const u8) !HttpResponse {
    const addr = try config.parseIpLiteral("127.0.0.1", port);
    const stream = try std.net.tcpConnectToAddress(addr);
    defer stream.close();

    const request = try std.fmt.allocPrint(
        allocator,
        "{s} {s} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer secret-token\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
        .{ method, path, body.len, body },
    );
    defer allocator.free(request);
    _ = try std.posix.write(stream.handle, request);
    try std.posix.shutdown(stream.handle, .send);

    var header_buf = std.ArrayList(u8){};
    defer header_buf.deinit(allocator);
    var byte: [1]u8 = undefined;
    while (std.mem.indexOf(u8, header_buf.items, "\r\n\r\n") == null) {
        const amt = try std.posix.read(stream.handle, &byte);
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
    var line_it = std.mem.splitSequence(u8, headers[status_line_end + 2 .. sep], "\r\n");
    while (line_it.next()) |line| {
        if (std.ascii.startsWithIgnoreCase(line, "content-length:")) {
            const value = std.mem.trim(u8, line["content-length:".len..], " ");
            content_length = try std.fmt.parseInt(usize, value, 10);
        }
    }

    const already_read = headers.len - (sep + 4);
    var body_copy = try allocator.alloc(u8, content_length);
    if (already_read > 0) {
        @memcpy(body_copy[0..already_read], headers[sep + 4 ..]);
    }
    var offset = already_read;
    while (offset < content_length) {
        const amt = try std.posix.read(stream.handle, body_copy[offset..content_length]);
        if (amt == 0) break;
        offset += amt;
    }
    return .{ .status = status, .body = body_copy };
}

fn startTcpEchoServer(_: std.mem.Allocator) !struct { server: std.net.Server, port: u16, thread: std.Thread } {
    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    const server = try addr.listen(.{ .reuse_address = true });
    var bound: std.net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.net.Address);
    try std.posix.getsockname(server.stream.handle, &bound.any, &len);
    const port = bound.getPort();
    const thread = try std.Thread.spawn(.{}, tcpEchoThread, .{server.stream.handle});
    return .{ .server = server, .port = port, .thread = thread };
}

fn tcpEchoThread(server_fd: std.posix.fd_t) void {
    var server = std.net.Server{ .listen_address = undefined, .stream = .{ .handle = server_fd } };
    const conn = server.accept() catch return;
    defer conn.stream.close();
    var buf: [1024]u8 = undefined;
    while (true) {
        const amt = std.posix.read(conn.stream.handle, &buf) catch break;
        if (amt == 0) break;
        _ = std.posix.write(conn.stream.handle, buf[0..amt]) catch break;
    }
}

test "http create target forward delete tcp" {
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
    const create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/ports", create_body);
    defer std.testing.allocator.free(create_resp.body);
    try std.testing.expectEqual(@as(u16, 201), create_resp.status);
    const created_id = try extractJsonString(std.testing.allocator, create_resp.body, "id");
    defer std.testing.allocator.free(created_id);
    const created_runtime_status = try extractJsonString(std.testing.allocator, create_resp.body, "runtime_status");
    defer std.testing.allocator.free(created_runtime_status);
    const created_port = try extractJsonU16(create_resp.body, "port");
    try std.testing.expectEqualStrings("rejecting_no_host", created_runtime_status);

    const target_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"id\":\"{s}\",\"host\":\"127.0.0.1\"}}", .{created_id});
    defer std.testing.allocator.free(target_body);
    const target_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/ports/target", target_body);
    defer std.testing.allocator.free(target_resp.body);
    try std.testing.expectEqual(@as(u16, 200), target_resp.status);

    const forward_addr = try config.parseIpLiteral("127.0.0.1", created_port);
    const stream = try std.net.tcpConnectToAddress(forward_addr);
    defer stream.close();
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);

    const delete_path = try std.fmt.allocPrint(std.testing.allocator, "/v1/ports/{s}", .{created_id});
    defer std.testing.allocator.free(delete_path);
    const delete_resp = try doHttp(std.testing.allocator, http_port, "DELETE", delete_path, "");
    defer std.testing.allocator.free(delete_resp.body);
    try std.testing.expectEqual(@as(u16, 204), delete_resp.status);
}

test "http metrics endpoint exposes tcp splice counters" {
    const harness = try Harness.init(std.testing.allocator);
    defer harness.deinit();

    const http_port = try harness.http.assignedPort();
    const response = try doHttp(std.testing.allocator, http_port, "GET", "/v1/metrics", "");
    defer std.testing.allocator.free(response.body);

    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_splice_attempt_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_splice_success_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_splice_hard_failure_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_session_create_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_session_close_total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"tcp_active_sessions\":") != null);
}

test "http start failure does not deadlock cleanup" {
    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    var occupied = try addr.listen(.{});
    defer occupied.deinit();

    var bound: std.net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.net.Address);
    try std.posix.getsockname(occupied.stream.handle, &bound.any, &len);

    const db_path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(db_path) catch {};
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
    };
    defer {
        http.stop();
        std.testing.allocator.free(http.host);
        std.testing.allocator.free(http.auth_token);
    }

    try std.testing.expectError(error.AddressInUse, http.start());
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
