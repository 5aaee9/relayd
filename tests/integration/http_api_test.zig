const std = @import("std");
const compat = @import("../../src/compat.zig");
const net = @import("../../src/net_compat.zig");
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
        compat.deleteFile(self.db_path);
        self.allocator.free(self.db_path);
        self.allocator.destroy(self);
    }
};

fn doHttp(allocator: std.mem.Allocator, port: u16, method: []const u8, path: []const u8, body: []const u8) !HttpResponse {
    const addr = try config.parseIpLiteral("127.0.0.1", port);
    const stream = try net.tcpConnectToAddress(addr);
    defer stream.close();

    const request = try std.fmt.allocPrint(
        allocator,
        "{s} {s} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer secret-token\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
        .{ method, path, body.len, body },
    );
    defer allocator.free(request);
    _ = try compat.write(stream.handle, request);
    try compat.shutdown(stream.handle, .send);

    var header_buf = std.ArrayList(u8).empty;
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
    const stream = try net.tcpConnectToAddress(forward_addr);
    defer stream.close();
    _ = try compat.write(stream.handle, "ping");
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
    };
    defer {
        http.stop();
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
        echo.server.deinit();
        echo.thread.join();
    }

    const http_port = try harness.http.assignedPort();

    const create_body = "{\"protocol\":\"tcp\"}";
    const create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/allocations", create_body);
    defer std.testing.allocator.free(create_resp.body);
    try std.testing.expectEqual(@as(u16, 201), create_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, create_resp.body, "\"target_port\":") == null);
    const allocation_id = try extractJsonString(std.testing.allocator, create_resp.body, "id");
    defer std.testing.allocator.free(allocation_id);
    const allocation_port = try extractJsonU16(create_resp.body, "port");

    const get_alloc_path = try std.fmt.allocPrint(std.testing.allocator, "/v1/allocations/{s}", .{allocation_id});
    defer std.testing.allocator.free(get_alloc_path);
    const get_alloc_resp = try doHttp(std.testing.allocator, http_port, "GET", get_alloc_path, "");
    defer std.testing.allocator.free(get_alloc_resp.body);
    try std.testing.expectEqual(@as(u16, 200), get_alloc_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, get_alloc_resp.body, "\"target_port\":") == null);

    const list_alloc_resp = try doHttp(std.testing.allocator, http_port, "GET", "/v1/allocations", "");
    defer std.testing.allocator.free(list_alloc_resp.body);
    try std.testing.expectEqual(@as(u16, 200), list_alloc_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, list_alloc_resp.body, allocation_id) != null);

    const get_binding_path = try std.fmt.allocPrint(std.testing.allocator, "/v1/allocations/{s}/binding", .{allocation_id});
    defer std.testing.allocator.free(get_binding_path);
    const missing_binding_resp = try doHttp(std.testing.allocator, http_port, "GET", get_binding_path, "");
    defer std.testing.allocator.free(missing_binding_resp.body);
    try std.testing.expectEqual(@as(u16, 404), missing_binding_resp.status);

    const binding_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"host\":\"127.0.0.1\",\"target_port\":{d}}}", .{echo.port});
    defer std.testing.allocator.free(binding_body);
    const put_binding_resp = try doHttp(std.testing.allocator, http_port, "PUT", get_binding_path, binding_body);
    defer std.testing.allocator.free(put_binding_resp.body);
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

    const get_binding_resp = try doHttp(std.testing.allocator, http_port, "GET", get_binding_path, "");
    defer std.testing.allocator.free(get_binding_resp.body);
    try std.testing.expectEqual(@as(u16, 200), get_binding_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, get_binding_resp.body, "\"effective_target_port\":") != null);

    const delete_binding_resp = try doHttp(std.testing.allocator, http_port, "DELETE", get_binding_path, "");
    defer std.testing.allocator.free(delete_binding_resp.body);
    try std.testing.expectEqual(@as(u16, 204), delete_binding_resp.status);

    const get_binding_after_delete = try doHttp(std.testing.allocator, http_port, "GET", get_binding_path, "");
    defer std.testing.allocator.free(get_binding_after_delete.body);
    try std.testing.expectEqual(@as(u16, 404), get_binding_after_delete.status);

    const allocation_still_exists = try doHttp(std.testing.allocator, http_port, "GET", get_alloc_path, "");
    defer std.testing.allocator.free(allocation_still_exists.body);
    try std.testing.expectEqual(@as(u16, 200), allocation_still_exists.status);

    const aggregate_after_detach = try doHttp(std.testing.allocator, http_port, "GET", "/v1/ports", "");
    defer std.testing.allocator.free(aggregate_after_detach.body);
    try std.testing.expectEqual(@as(u16, 200), aggregate_after_detach.status);
    const detached_fragment = try std.fmt.allocPrint(std.testing.allocator, "\"id\":\"{s}\",\"protocol\":\"tcp\",\"port\":{d},\"target_port\":null", .{ allocation_id, allocation_port });
    defer std.testing.allocator.free(detached_fragment);
    try std.testing.expect(std.mem.indexOf(u8, aggregate_after_detach.body, detached_fragment) != null);

    const delete_alloc_resp = try doHttp(std.testing.allocator, http_port, "DELETE", get_alloc_path, "");
    defer std.testing.allocator.free(delete_alloc_resp.body);
    try std.testing.expectEqual(@as(u16, 204), delete_alloc_resp.status);

    const get_binding_after_allocation_delete = try doHttp(std.testing.allocator, http_port, "GET", get_binding_path, "");
    defer std.testing.allocator.free(get_binding_after_allocation_delete.body);
    try std.testing.expectEqual(@as(u16, 404), get_binding_after_allocation_delete.status);
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
