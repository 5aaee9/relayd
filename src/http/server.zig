const std = @import("std");
const net = std.net;
const http = std.http;
const json = std.json;
const config_mod = @import("../config.zig");
const model = @import("../model/allocation.zig");
const service_mod = @import("../service/allocation_service.zig");
const Metrics = @import("../metrics.zig").Metrics;
const posix = std.posix;

pub const HttpServer = struct {
    allocator: std.mem.Allocator,
    service: *service_mod.Service,
    metrics: *Metrics,
    host: []const u8,
    port: u16,
    auth_token: []const u8,
    thread: ?std.Thread = null,
    server: ?net.Server = null,
    server_mutex: std.Thread.Mutex = .{},
    active_mutex: std.Thread.Mutex = .{},
    active_cond: std.Thread.Condition = .{},
    active_count: usize = 0,

    pub fn start(self: *HttpServer) !void {
        const addr = try config_mod.parseIpLiteral(self.host, self.port);
        if (!std.mem.eql(u8, self.host, "127.0.0.1") and !std.mem.eql(u8, self.host, "::1")) {
            self.metrics.http_non_loopback_bind_total.inc();
            std.log.warn("HTTP control plane bound to non-loopback address {s}:{d}", .{ self.host, self.port });
        }
        self.server_mutex.lock();
        errdefer self.server_mutex.unlock();
        self.server = try addr.listen(.{ .reuse_address = true, .force_nonblocking = true });
        self.server_mutex.unlock();
        self.thread = std.Thread.spawn(.{}, acceptLoop, .{self}) catch |err| {
            self.server_mutex.lock();
            if (self.server) |*server| {
                server.deinit();
                self.server = null;
            }
            self.server_mutex.unlock();
            return err;
        };
    }

    pub fn stop(self: *HttpServer) void {
        self.server_mutex.lock();
        var local_server = self.server;
        self.server = null;
        self.server_mutex.unlock();
        if (local_server) |*server| {
            server.deinit();
        }
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
        self.active_mutex.lock();
        defer self.active_mutex.unlock();
        while (self.active_count != 0) self.active_cond.wait(&self.active_mutex);
    }

    pub fn assignedPort(self: *HttpServer) !u16 {
        self.server_mutex.lock();
        defer self.server_mutex.unlock();
        const server = self.server orelse return error.NotListening;
        var addr: net.Address = undefined;
        var len: std.posix.socklen_t = @sizeOf(net.Address);
        try std.posix.getsockname(server.stream.handle, &addr.any, &len);
        return addr.getPort();
    }

    fn acceptLoop(self: *HttpServer) void {
        while (true) {
            self.server_mutex.lock();
            const server = self.server;
            self.server_mutex.unlock();
            const listener_fd = if (server) |srv| srv.stream.handle else break;

            var addr: net.Address = undefined;
            var len: posix.socklen_t = @sizeOf(net.Address);
            const rc = posix.system.accept4(listener_fd, &addr.any, &len, posix.SOCK.CLOEXEC);
            switch (posix.errno(rc)) {
                .SUCCESS => {},
                .AGAIN => {
                    std.Thread.sleep(10 * std.time.ns_per_ms);
                    continue;
                },
                .BADF => break,
                else => break,
            }
            const conn = net.Server.Connection{
                .stream = .{ .handle = @intCast(rc) },
                .address = addr,
            };
            var timeout = std.posix.timeval{ .sec = 1, .usec = 0 };
            std.posix.setsockopt(conn.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&timeout)) catch {};
            self.active_mutex.lock();
            self.active_count += 1;
            self.active_mutex.unlock();
            const ctx = self.allocator.create(ConnectionCtx) catch {
                conn.stream.close();
                self.active_mutex.lock();
                self.active_count -= 1;
                self.active_cond.broadcast();
                self.active_mutex.unlock();
                continue;
            };
            ctx.* = .{ .server = self, .connection = conn };
            const thread = std.Thread.spawn(.{}, handleConnection, .{ctx}) catch {
                conn.stream.close();
                self.allocator.destroy(ctx);
                self.active_mutex.lock();
                self.active_count -= 1;
                self.active_cond.broadcast();
                self.active_mutex.unlock();
                continue;
            };
            thread.detach();
        }
    }
};

const ConnectionCtx = struct {
    server: *HttpServer,
    connection: net.Server.Connection,
};

const CreateRequest = struct { protocol: []const u8, target_port: u16 };
const AllocationCreateRequest = struct { protocol: []const u8 };
const BindingPutRequest = struct { target_port: u16, host: []const u8 };
const TargetRequest = struct { id: []const u8, host: []const u8 };
const UpdateRequest = struct { target_port: ?u16 = null, host: ?[]const u8 = null };

fn handleConnection(ctx: *ConnectionCtx) void {
    defer {
        ctx.connection.stream.close();
        ctx.server.active_mutex.lock();
        ctx.server.active_count -= 1;
        ctx.server.active_cond.broadcast();
        ctx.server.active_mutex.unlock();
        ctx.server.allocator.destroy(ctx);
    }

    var recv_buffer: [8192]u8 = undefined;
    var send_buffer: [8192]u8 = undefined;
    var reader = ctx.connection.stream.reader(&recv_buffer);
    var writer = ctx.connection.stream.writer(&send_buffer);
    var server = http.Server.init(reader.interface(), &writer.interface);

    while (true) {
        var request = server.receiveHead() catch break;
        handleRequest(ctx.server, &request) catch |err| {
            std.log.err("http request failed: {s}", .{@errorName(err)});
            _ = request.respond("internal server error", .{ .status = .internal_server_error, .keep_alive = false }) catch {};
            break;
        };
    }
}

fn handleRequest(server: *HttpServer, request: *http.Server.Request) !void {
    if (!authorized(server.auth_token, request)) {
        try request.respond("unauthorized", .{ .status = .unauthorized, .keep_alive = false });
        return;
    }

    const target = request.head.target;
    if (request.head.method == .GET and std.mem.eql(u8, target, "/v1/allocations")) return handleListAllocations(server, request);
    if (request.head.method == .POST and std.mem.eql(u8, target, "/v1/allocations")) return handleCreateAllocation(server, request);
    if (request.head.method == .GET and std.mem.eql(u8, target, "/v1/ports")) return handleList(server, request);
    if (request.head.method == .GET and std.mem.eql(u8, target, "/v1/metrics")) return handleMetrics(server, request);
    if (request.head.method == .POST and std.mem.eql(u8, target, "/v1/ports")) return handleCreate(server, request);
    if (request.head.method == .POST and std.mem.eql(u8, target, "/v1/ports/target")) return handleSetTarget(server, request);
    if (std.mem.startsWith(u8, target, "/v1/allocations/")) {
        const rest = target["/v1/allocations/".len..];
        if (std.mem.endsWith(u8, rest, "/binding")) {
            const id = rest[0 .. rest.len - "/binding".len];
            if (request.head.method == .PUT) return handlePutBinding(server, request, id);
            if (request.head.method == .GET) return handleGetBinding(server, request, id);
            if (request.head.method == .DELETE) return handleDeleteBinding(server, request, id);
        } else {
            if (request.head.method == .GET) return handleGetAllocation(server, request, rest);
            if (request.head.method == .DELETE) return handleDelete(server, request, rest);
        }
    }
    if (std.mem.startsWith(u8, target, "/v1/ports/")) {
        const id = target[10..];
        if (request.head.method == .POST) return handleUpdate(server, request, id);
        if (request.head.method == .DELETE) return handleDelete(server, request, id);
    }
    try request.respond("not found", .{ .status = .not_found, .keep_alive = false });
}

fn handleCreate(server: *HttpServer, request: *http.Server.Request) !void {
    var body_buf: [4096]u8 = undefined;
    const body = try readBody(server.allocator, request, &body_buf);
    defer server.allocator.free(body);
    var parsed = try json.parseFromSlice(CreateRequest, server.allocator, body, .{});
    defer parsed.deinit();
    const protocol = model.Protocol.fromString(parsed.value.protocol) orelse {
        try request.respond("invalid protocol", .{ .status = .bad_request, .keep_alive = false });
        return;
    };
    var allocation = server.service.createAllocation(protocol, parsed.value.target_port) catch |err| {
        try respondServiceError(request, err);
        return;
    };
    defer allocation.deinit(server.allocator);
    var view = (try server.service.getAllocationView(server.allocator, allocation.id)).?;
    defer service_mod.deinitView(server.allocator, &view);
    const payload = try encodeView(server.allocator, view);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .created, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

fn handleCreateAllocation(server: *HttpServer, request: *http.Server.Request) !void {
    var body_buf: [4096]u8 = undefined;
    const body = try readBody(server.allocator, request, &body_buf);
    defer server.allocator.free(body);
    var parsed = try json.parseFromSlice(AllocationCreateRequest, server.allocator, body, .{});
    defer parsed.deinit();
    const protocol = model.Protocol.fromString(parsed.value.protocol) orelse {
        try request.respond("invalid protocol", .{ .status = .bad_request, .keep_alive = false });
        return;
    };
    var allocation = server.service.createAllocation(protocol, null) catch |err| {
        try respondServiceError(request, err);
        return;
    };
    defer allocation.deinit(server.allocator);
    var resource = (try server.service.getAllocationResource(server.allocator, allocation.id)).?;
    defer service_mod.deinitAllocationResource(server.allocator, &resource);
    const payload = try encodeAllocationResource(server.allocator, resource);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .created, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

fn handleSetTarget(server: *HttpServer, request: *http.Server.Request) !void {
    var body_buf: [4096]u8 = undefined;
    const body = try readBody(server.allocator, request, &body_buf);
    defer server.allocator.free(body);
    var parsed = try json.parseFromSlice(TargetRequest, server.allocator, body, .{});
    defer parsed.deinit();
    var allocation = server.service.setTarget(parsed.value.id, parsed.value.host) catch |err| {
        try respondServiceError(request, err);
        return;
    };
    defer allocation.deinit(server.allocator);
    var view = (try server.service.getAllocationView(server.allocator, allocation.id)).?;
    defer service_mod.deinitView(server.allocator, &view);
    const payload = try encodeView(server.allocator, view);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .ok, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

fn handleUpdate(server: *HttpServer, request: *http.Server.Request, id: []const u8) !void {
    var body_buf: [4096]u8 = undefined;
    const body = try readBody(server.allocator, request, &body_buf);
    defer server.allocator.free(body);
    var parsed = try json.parseFromSlice(UpdateRequest, server.allocator, body, .{});
    defer parsed.deinit();
    if (parsed.value.target_port == null and parsed.value.host == null) {
        try request.respond("empty update", .{ .status = .bad_request, .keep_alive = false });
        return;
    }
    var allocation = server.service.updateAllocation(id, parsed.value.target_port, parsed.value.host) catch |err| {
        try respondServiceError(request, err);
        return;
    };
    defer allocation.deinit(server.allocator);
    var view = (try server.service.getAllocationView(server.allocator, id)).?;
    defer service_mod.deinitView(server.allocator, &view);
    const payload = try encodeView(server.allocator, view);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .ok, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

fn handleDelete(server: *HttpServer, request: *http.Server.Request, id: []const u8) !void {
    server.service.deleteAllocation(id) catch |err| {
        try respondServiceError(request, err);
        return;
    };
    try request.respond("", .{ .status = .no_content, .keep_alive = false });
}

fn handleListAllocations(server: *HttpServer, request: *http.Server.Request) !void {
    var resources = try server.service.listAllocationResources(server.allocator);
    defer {
        for (resources.items) |*resource| service_mod.deinitAllocationResource(server.allocator, resource);
        resources.deinit(server.allocator);
    }
    const payload = try encodeAllocationResources(server.allocator, resources.items);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .ok, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

fn handleGetAllocation(server: *HttpServer, request: *http.Server.Request, id: []const u8) !void {
    var resource = (try server.service.getAllocationResource(server.allocator, id)) orelse {
        try request.respond("NotFound", .{ .status = .not_found, .keep_alive = false });
        return;
    };
    defer service_mod.deinitAllocationResource(server.allocator, &resource);
    const payload = try encodeAllocationResource(server.allocator, resource);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .ok, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

fn handlePutBinding(server: *HttpServer, request: *http.Server.Request, id: []const u8) !void {
    var body_buf: [4096]u8 = undefined;
    const body = try readBody(server.allocator, request, &body_buf);
    defer server.allocator.free(body);
    var parsed = try json.parseFromSlice(BindingPutRequest, server.allocator, body, .{});
    defer parsed.deinit();
    var binding = server.service.putBinding(id, parsed.value.host, parsed.value.target_port) catch |err| {
        try respondServiceError(request, err);
        return;
    };
    defer binding.deinit(server.allocator);
    var view = (try server.service.getBindingView(server.allocator, id)).?;
    defer service_mod.deinitBindingView(server.allocator, &view);
    const payload = try encodeBindingView(server.allocator, view);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .ok, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

fn handleGetBinding(server: *HttpServer, request: *http.Server.Request, id: []const u8) !void {
    var view = (try server.service.getBindingView(server.allocator, id)) orelse {
        try request.respond("NotFound", .{ .status = .not_found, .keep_alive = false });
        return;
    };
    defer service_mod.deinitBindingView(server.allocator, &view);
    const payload = try encodeBindingView(server.allocator, view);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .ok, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

fn handleDeleteBinding(server: *HttpServer, request: *http.Server.Request, id: []const u8) !void {
    server.service.deleteBinding(id) catch |err| {
        try respondServiceError(request, err);
        return;
    };
    try request.respond("", .{ .status = .no_content, .keep_alive = false });
}

fn handleList(server: *HttpServer, request: *http.Server.Request) !void {
    var views = try server.service.listAllocations(server.allocator);
    defer {
        for (views.items) |*view| service_mod.deinitView(server.allocator, view);
        views.deinit(server.allocator);
    }
    const payload = try encodeViews(server.allocator, views.items);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .ok, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

const JsonMetrics = struct {
    allocations_total: u64,
    runtime_apply_total: u64,
    restore_failures_total: u64,
    rejected_no_host_total: u64,
    bind_fail_total: u64,
    tcp_splice_fast_path_total: u64,
    tcp_copy_fallback_total: u64,
    tcp_splice_attempt_total: u64,
    tcp_splice_success_total: u64,
    tcp_splice_fallback_total: u64,
    tcp_splice_hard_failure_total: u64,
    tcp_splice_fallback_forced_total: u64,
    tcp_splice_fallback_unsupported_total: u64,
    tcp_splice_fallback_runtime_error_total: u64,
    tcp_session_create_total: u64,
    tcp_session_close_total: u64,
    tcp_session_event_total: u64,
    tcp_session_worker_dispatch_total: u64,
    tcp_session_worker0_dispatch_total: u64,
    tcp_session_worker1_dispatch_total: u64,
    tcp_accept_handoff_total: u64,
    tcp_accept_handoff_worker0_total: u64,
    tcp_accept_handoff_worker1_total: u64,
    tcp_accept_handoff_worker2_total: u64,
    tcp_accept_handoff_worker3_total: u64,
    tcp_listener_accept_total: u64,
    tcp_listener_accept_worker0_total: u64,
    tcp_listener_accept_worker1_total: u64,
    tcp_listener_accept_worker2_total: u64,
    tcp_listener_accept_worker3_total: u64,
    tcp_upstream_connect_total: u64,
    tcp_upstream_connect_fail_total: u64,
    tcp_active_sessions: u64,
    udp_packets_in_total: u64,
    udp_packets_out_total: u64,
    udp_bytes_in_total: u64,
    udp_bytes_out_total: u64,
    udp_recv_errors_total: u64,
    udp_send_errors_total: u64,
    udp_session_create_total: u64,
    udp_session_expire_total: u64,
    udp_batch_calls_total: u64,
    udp_batch_messages_total: u64,
    udp_drop_total: u64,
    udp_reply_primary_total: u64,
    udp_reply_drop_total: u64,
    udp_reply_stale_total: u64,
    udp_worker_packets_in_total: u64,
    udp_worker_packets_out_total: u64,
    udp_worker0_packets_in_total: u64,
    udp_worker1_packets_in_total: u64,
    udp_worker2_packets_in_total: u64,
    udp_worker3_packets_in_total: u64,
    udp_worker0_packets_out_total: u64,
    udp_worker1_packets_out_total: u64,
    udp_worker2_packets_out_total: u64,
    udp_worker3_packets_out_total: u64,
    udp_dataplane_redesign_packets_in_total: u64,
    udp_dataplane_redesign_packets_out_total: u64,
    udp_io_uring_submit_total: u64,
    udp_io_uring_cqe_total: u64,
    udp_io_uring_multishot_total: u64,
    udp_io_uring_buf_release_total: u64,
    udp_io_uring_fallback_total: u64,
    udp_fast_path_packets_in_total: u64,
    udp_fast_path_packets_out_total: u64,
    udp_fast_path_gso_send_total: u64,
    udp_fast_path_gro_recv_total: u64,
    udp_fast_path_fallback_total: u64,
    udp_fast_path_drop_total: u64,
    udp_active_sessions: u64,
    restore_timeout_total: u64,
    http_non_loopback_bind_total: u64,
};

fn handleMetrics(server: *HttpServer, request: *http.Server.Request) !void {
    const payload = try encodeMetrics(server.allocator, server.metrics);
    defer server.allocator.free(payload);
    try request.respond(payload, .{ .status = .ok, .keep_alive = false, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}

fn authorized(token: []const u8, request: *const http.Server.Request) bool {
    var it = request.iterateHeaders();
    while (it.next()) |header| {
        if (!std.ascii.eqlIgnoreCase(header.name, "authorization")) continue;
        if (!std.mem.startsWith(u8, header.value, "Bearer ")) return false;
        const provided = header.value[7..];
        if (provided.len != token.len) return false;
        var diff: u8 = 0;
        for (provided, token) |a, b| diff |= a ^ b;
        return diff == 0;
    }
    return false;
}

fn respondServiceError(request: *http.Server.Request, err: anyerror) !void {
    const status: http.Status = switch (err) {
        error.NotFound => .not_found,
        error.Timeout, error.RuntimeUpdateFailed, error.RuntimeDeleteFailed => .service_unavailable,
        error.NoAvailablePort => .conflict,
        error.InvalidHost, error.InvalidPort => .bad_request,
        else => .internal_server_error,
    };
    try request.respond(@errorName(err), .{ .status = status, .keep_alive = false });
}

fn readBody(allocator: std.mem.Allocator, request: *http.Server.Request, buffer: []u8) ![]u8 {
    request.head.expect = null;
    const reader = request.readerExpectNone(buffer);
    const length = request.head.content_length orelse 0;
    return try reader.readAlloc(allocator, length);
}

const JsonView = struct {
    id: []const u8,
    protocol: []const u8,
    port: u16,
    target_port: ?u16,
    host: ?[]const u8,
    effective_target_port: ?u16,
    effective_host: ?[]const u8,
    host_configured: bool,
    runtime_status: []const u8,
    error_kind: ?[]const u8,
    last_error: ?[]const u8,
    created_at_ms: i64,
    updated_at_ms: i64,
};

const JsonAllocationResource = struct {
    id: []const u8,
    protocol: []const u8,
    port: u16,
    created_at_ms: i64,
    updated_at_ms: i64,
};

const JsonBindingView = struct {
    allocation_id: []const u8,
    host: ?[]const u8,
    target_port: u16,
    effective_target_port: ?u16,
    effective_host: ?[]const u8,
    runtime_status: []const u8,
    error_kind: ?[]const u8,
    last_error: ?[]const u8,
    created_at_ms: i64,
    updated_at_ms: i64,
};

fn toJsonView(view: model.AllocationView) JsonView {
    return .{
        .id = view.id,
        .protocol = view.protocol.asString(),
        .port = view.port,
        .target_port = view.target_port,
        .host = view.host,
        .effective_target_port = view.effective_target_port,
        .effective_host = view.effective_host,
        .host_configured = view.host_configured,
        .runtime_status = view.runtime_status.asString(),
        .error_kind = if (view.error_kind) |kind| kind.asString() else null,
        .last_error = view.last_error,
        .created_at_ms = view.created_at_ms,
        .updated_at_ms = view.updated_at_ms,
    };
}

fn toJsonAllocationResource(resource: model.AllocationResource) JsonAllocationResource {
    return .{
        .id = resource.id,
        .protocol = resource.protocol.asString(),
        .port = resource.port,
        .created_at_ms = resource.created_at_ms,
        .updated_at_ms = resource.updated_at_ms,
    };
}

fn toJsonBindingView(view: model.BindingView) JsonBindingView {
    return .{
        .allocation_id = view.allocation_id,
        .host = view.host,
        .target_port = view.target_port,
        .effective_target_port = view.effective_target_port,
        .effective_host = view.effective_host,
        .runtime_status = view.runtime_status.asString(),
        .error_kind = if (view.error_kind) |kind| kind.asString() else null,
        .last_error = view.last_error,
        .created_at_ms = view.created_at_ms,
        .updated_at_ms = view.updated_at_ms,
    };
}

fn toJsonMetrics(metrics: *const Metrics) JsonMetrics {
    return .{
        .allocations_total = metrics.allocations_total.load(),
        .runtime_apply_total = metrics.runtime_apply_total.load(),
        .restore_failures_total = metrics.restore_failures_total.load(),
        .rejected_no_host_total = metrics.rejected_no_host_total.load(),
        .bind_fail_total = metrics.bind_fail_total.load(),
        .tcp_splice_fast_path_total = metrics.tcp_splice_fast_path_total.load(),
        .tcp_copy_fallback_total = metrics.tcp_copy_fallback_total.load(),
        .tcp_splice_attempt_total = metrics.tcp_splice_attempt_total.load(),
        .tcp_splice_success_total = metrics.tcp_splice_success_total.load(),
        .tcp_splice_fallback_total = metrics.tcp_splice_fallback_total.load(),
        .tcp_splice_hard_failure_total = metrics.tcp_splice_hard_failure_total.load(),
        .tcp_splice_fallback_forced_total = metrics.tcp_splice_fallback_forced_total.load(),
        .tcp_splice_fallback_unsupported_total = metrics.tcp_splice_fallback_unsupported_total.load(),
        .tcp_splice_fallback_runtime_error_total = metrics.tcp_splice_fallback_runtime_error_total.load(),
        .tcp_session_create_total = metrics.tcp_session_create_total.load(),
        .tcp_session_close_total = metrics.tcp_session_close_total.load(),
        .tcp_session_event_total = metrics.tcp_session_event_total.load(),
        .tcp_session_worker_dispatch_total = metrics.tcp_session_worker_dispatch_total.load(),
        .tcp_session_worker0_dispatch_total = metrics.tcp_session_worker0_dispatch_total.load(),
        .tcp_session_worker1_dispatch_total = metrics.tcp_session_worker1_dispatch_total.load(),
        .tcp_accept_handoff_total = metrics.tcp_accept_handoff_total.load(),
        .tcp_accept_handoff_worker0_total = metrics.tcp_accept_handoff_worker0_total.load(),
        .tcp_accept_handoff_worker1_total = metrics.tcp_accept_handoff_worker1_total.load(),
        .tcp_accept_handoff_worker2_total = metrics.tcp_accept_handoff_worker2_total.load(),
        .tcp_accept_handoff_worker3_total = metrics.tcp_accept_handoff_worker3_total.load(),
        .tcp_listener_accept_total = metrics.tcp_listener_accept_total.load(),
        .tcp_listener_accept_worker0_total = metrics.tcp_listener_accept_worker0_total.load(),
        .tcp_listener_accept_worker1_total = metrics.tcp_listener_accept_worker1_total.load(),
        .tcp_listener_accept_worker2_total = metrics.tcp_listener_accept_worker2_total.load(),
        .tcp_listener_accept_worker3_total = metrics.tcp_listener_accept_worker3_total.load(),
        .tcp_upstream_connect_total = metrics.tcp_upstream_connect_total.load(),
        .tcp_upstream_connect_fail_total = metrics.tcp_upstream_connect_fail_total.load(),
        .tcp_active_sessions = metrics.tcp_active_sessions.load(),
        .udp_packets_in_total = metrics.udp_packets_in_total.load(),
        .udp_packets_out_total = metrics.udp_packets_out_total.load(),
        .udp_bytes_in_total = metrics.udp_bytes_in_total.load(),
        .udp_bytes_out_total = metrics.udp_bytes_out_total.load(),
        .udp_recv_errors_total = metrics.udp_recv_errors_total.load(),
        .udp_send_errors_total = metrics.udp_send_errors_total.load(),
        .udp_session_create_total = metrics.udp_session_create_total.load(),
        .udp_session_expire_total = metrics.udp_session_expire_total.load(),
        .udp_batch_calls_total = metrics.udp_batch_calls_total.load(),
        .udp_batch_messages_total = metrics.udp_batch_messages_total.load(),
        .udp_drop_total = metrics.udp_drop_total.load(),
        .udp_reply_primary_total = metrics.udp_reply_primary_total.load(),
        .udp_reply_drop_total = metrics.udp_reply_drop_total.load(),
        .udp_reply_stale_total = metrics.udp_reply_stale_total.load(),
        .udp_worker_packets_in_total = metrics.udp_worker_packets_in_total.load(),
        .udp_worker_packets_out_total = metrics.udp_worker_packets_out_total.load(),
        .udp_worker0_packets_in_total = metrics.udp_worker0_packets_in_total.load(),
        .udp_worker1_packets_in_total = metrics.udp_worker1_packets_in_total.load(),
        .udp_worker2_packets_in_total = metrics.udp_worker2_packets_in_total.load(),
        .udp_worker3_packets_in_total = metrics.udp_worker3_packets_in_total.load(),
        .udp_worker0_packets_out_total = metrics.udp_worker0_packets_out_total.load(),
        .udp_worker1_packets_out_total = metrics.udp_worker1_packets_out_total.load(),
        .udp_worker2_packets_out_total = metrics.udp_worker2_packets_out_total.load(),
        .udp_worker3_packets_out_total = metrics.udp_worker3_packets_out_total.load(),
        .udp_dataplane_redesign_packets_in_total = metrics.udp_dataplane_redesign_packets_in_total.load(),
        .udp_dataplane_redesign_packets_out_total = metrics.udp_dataplane_redesign_packets_out_total.load(),
        .udp_io_uring_submit_total = metrics.udp_io_uring_submit_total.load(),
        .udp_io_uring_cqe_total = metrics.udp_io_uring_cqe_total.load(),
        .udp_io_uring_multishot_total = metrics.udp_io_uring_multishot_total.load(),
        .udp_io_uring_buf_release_total = metrics.udp_io_uring_buf_release_total.load(),
        .udp_io_uring_fallback_total = metrics.udp_io_uring_fallback_total.load(),
        .udp_fast_path_packets_in_total = metrics.udp_fast_path_packets_in_total.load(),
        .udp_fast_path_packets_out_total = metrics.udp_fast_path_packets_out_total.load(),
        .udp_fast_path_gso_send_total = metrics.udp_fast_path_gso_send_total.load(),
        .udp_fast_path_gro_recv_total = metrics.udp_fast_path_gro_recv_total.load(),
        .udp_fast_path_fallback_total = metrics.udp_fast_path_fallback_total.load(),
        .udp_fast_path_drop_total = metrics.udp_fast_path_drop_total.load(),
        .udp_active_sessions = metrics.udp_active_sessions.load(),
        .restore_timeout_total = metrics.restore_timeout_total.load(),
        .http_non_loopback_bind_total = metrics.http_non_loopback_bind_total.load(),
    };
}

fn encodeView(allocator: std.mem.Allocator, view: model.AllocationView) ![]u8 {
    return std.fmt.allocPrint(allocator, "{f}", .{json.fmt(toJsonView(view), .{})});
}

fn encodeViews(allocator: std.mem.Allocator, views: []const model.AllocationView) ![]u8 {
    const payloads = try allocator.alloc(JsonView, views.len);
    defer allocator.free(payloads);
    for (views, 0..) |view, i| payloads[i] = toJsonView(view);
    return std.fmt.allocPrint(allocator, "{f}", .{json.fmt(payloads, .{})});
}

fn encodeAllocationResource(allocator: std.mem.Allocator, resource: model.AllocationResource) ![]u8 {
    return std.fmt.allocPrint(allocator, "{f}", .{json.fmt(toJsonAllocationResource(resource), .{})});
}

fn encodeAllocationResources(allocator: std.mem.Allocator, resources: []const model.AllocationResource) ![]u8 {
    const payloads = try allocator.alloc(JsonAllocationResource, resources.len);
    defer allocator.free(payloads);
    for (resources, 0..) |resource, i| payloads[i] = toJsonAllocationResource(resource);
    return std.fmt.allocPrint(allocator, "{f}", .{json.fmt(payloads, .{})});
}

fn encodeBindingView(allocator: std.mem.Allocator, view: model.BindingView) ![]u8 {
    return std.fmt.allocPrint(allocator, "{f}", .{json.fmt(toJsonBindingView(view), .{})});
}

fn encodeMetrics(allocator: std.mem.Allocator, metrics: *const Metrics) ![]u8 {
    return std.fmt.allocPrint(allocator, "{f}", .{json.fmt(toJsonMetrics(metrics), .{})});
}
