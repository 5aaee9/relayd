const std = @import("std");
const config = @import("../../src/config.zig");
const sqlite = @import("../../src/storage/sqlite.zig");
const runtime = @import("../../src/runtime/manager.zig");
const service_mod = @import("../../src/service/allocation_service.zig");
const metrics_mod = @import("../../src/metrics.zig");
const model = @import("../../src/model/allocation.zig");

const UdpResponseMode = enum {
    echo,
    fixed,
};

const UdpPacketRecord = struct {
    remote_port: u16 = 0,
    payload_len: usize = 0,
    payload: [64]u8 = [_]u8{0} ** 64,
};

const UdpCaptureState = struct {
    fd: std.posix.fd_t,
    expected_packets: usize,
    response_mode: UdpResponseMode,
    fixed_response: []const u8,
    records: []UdpPacketRecord,
    actual_packets: usize = 0,
};

const UdpCaptureServer = struct {
    fd: std.posix.fd_t,
    port: u16,
    thread: std.Thread,
    state: *UdpCaptureState,
    joined: bool = false,

    fn deinit(self: *UdpCaptureServer, allocator: std.mem.Allocator) void {
        if (!self.joined) self.thread.join();
        closeIgnore(self.fd);
        allocator.free(self.state.records);
        allocator.destroy(self.state);
    }
};

const DelayedUdpReplyState = struct {
    fd: std.posix.fd_t,
    fixed_response: []const u8,
    record: UdpPacketRecord = .{},
    actual_packets: usize = 0,
    packet_received: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    release_reply: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    reply_sent: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

const DelayedUdpReplyServer = struct {
    fd: std.posix.fd_t,
    port: u16,
    thread: std.Thread,
    state: *DelayedUdpReplyState,
    joined: bool = false,

    fn release(self: *DelayedUdpReplyServer) void {
        self.state.release_reply.store(true, .monotonic);
    }

    fn deinit(self: *DelayedUdpReplyServer, allocator: std.mem.Allocator) void {
        self.release();
        if (!self.joined) self.thread.join();
        closeIgnore(self.fd);
        allocator.destroy(self.state);
    }
};

fn tempDbPath(allocator: std.mem.Allocator) ![]u8 {
    try std.fs.cwd().makePath(".zig-cache/integration-tests");
    return std.fmt.allocPrint(allocator, ".zig-cache/integration-tests/{d}.sqlite", .{std.time.nanoTimestamp()});
}

const TcpEchoServer = struct {
    server: std.net.Server,
    port: u16,
    thread: std.Thread,

    fn deinit(self: *TcpEchoServer) void {
        self.server.deinit();
        self.thread.join();
    }
};

const TcpMultiEchoServer = struct {
    server: std.net.Server,
    port: u16,
    thread: std.Thread,

    fn deinit(self: *TcpMultiEchoServer) void {
        self.server.deinit();
        self.thread.join();
    }
};

const TcpHalfCloseServer = struct {
    server: std.net.Server,
    port: u16,
    thread: std.Thread,

    fn deinit(self: *TcpHalfCloseServer) void {
        self.server.deinit();
        self.thread.join();
    }
};

const TcpForwardHarness = struct {
    path: []u8,
    repo: sqlite.Repository,
    metrics: metrics_mod.Metrics,
    rt: runtime.RuntimeManager,
    svc: service_mod.Service,
    echo: TcpEchoServer,
    alloc: model.Allocation,
    updated: model.Allocation,

    fn deinit(self: *TcpForwardHarness, allocator: std.mem.Allocator) void {
        self.updated.deinit(allocator);
        self.alloc.deinit(allocator);
        self.echo.deinit();
        self.rt.deinit();
        self.repo.close();
        std.fs.cwd().deleteFile(self.path) catch {};
        allocator.free(self.path);
        allocator.destroy(self);
    }
};

fn startTcpEchoServer() !TcpEchoServer {
    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    const server = try addr.listen(.{ .reuse_address = true });
    var bound: std.net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.net.Address);
    try std.posix.getsockname(server.stream.handle, &bound.any, &len);
    const port = bound.getPort();
    const thread = try std.Thread.spawn(.{}, tcpEchoThread, .{server.stream.handle});
    return .{ .server = server, .port = port, .thread = thread };
}

fn tcpEchoThread(fd: std.posix.fd_t) void {
    var server = std.net.Server{ .listen_address = undefined, .stream = .{ .handle = fd } };
    const conn = server.accept() catch return;
    defer conn.stream.close();
    var buf: [4]u8 = undefined;
    const amt = std.posix.read(conn.stream.handle, &buf) catch return;
    if (amt > 0) _ = std.posix.write(conn.stream.handle, buf[0..amt]) catch {};
}

fn startTcpMultiEchoServer(expected_connections: usize) !TcpMultiEchoServer {
    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    const server = try addr.listen(.{ .reuse_address = true });
    var bound: std.net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.net.Address);
    try std.posix.getsockname(server.stream.handle, &bound.any, &len);
    const port = bound.getPort();
    const thread = try std.Thread.spawn(.{}, tcpMultiEchoThread, .{ server.stream.handle, expected_connections });
    return .{ .server = server, .port = port, .thread = thread };
}

fn tcpMultiEchoThread(fd: std.posix.fd_t, expected_connections: usize) void {
    var server = std.net.Server{ .listen_address = undefined, .stream = .{ .handle = fd } };
    var handled: usize = 0;
    while (handled < expected_connections) : (handled += 1) {
        const conn = server.accept() catch return;
        defer conn.stream.close();
        var buf: [4]u8 = undefined;
        const amt = std.posix.read(conn.stream.handle, &buf) catch return;
        if (amt > 0) _ = std.posix.write(conn.stream.handle, buf[0..amt]) catch {};
    }
}

fn startTcpHalfCloseServer() !TcpHalfCloseServer {
    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    const server = try addr.listen(.{ .reuse_address = true });
    var bound: std.net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.net.Address);
    try std.posix.getsockname(server.stream.handle, &bound.any, &len);
    const port = bound.getPort();
    const thread = try std.Thread.spawn(.{}, tcpHalfCloseThread, .{server.stream.handle});
    return .{ .server = server, .port = port, .thread = thread };
}

fn tcpHalfCloseThread(fd: std.posix.fd_t) void {
    var server = std.net.Server{ .listen_address = undefined, .stream = .{ .handle = fd } };
    const conn = server.accept() catch return;
    defer conn.stream.close();
    var buf: [256]u8 = undefined;
    while (true) {
        const amt = std.posix.read(conn.stream.handle, &buf) catch return;
        if (amt == 0) break;
    }
    _ = std.posix.write(conn.stream.handle, "ack") catch {};
}

fn startTcpForwardHarness(options: runtime.Options) !*TcpForwardHarness {
    const allocator = std.testing.allocator;
    const harness = try allocator.create(TcpForwardHarness);
    errdefer allocator.destroy(harness);
    const path = try tempDbPath(allocator);
    errdefer {
        std.fs.cwd().deleteFile(path) catch {};
        allocator.free(path);
    }

    harness.* = .{
        .path = path,
        .repo = try sqlite.Repository.open(allocator, path),
        .metrics = .{},
        .rt = undefined,
        .svc = undefined,
        .echo = undefined,
        .alloc = undefined,
        .updated = undefined,
    };
    errdefer harness.repo.close();

    harness.rt = try runtime.RuntimeManager.init(allocator, &harness.metrics, options);
    errdefer harness.rt.deinit();
    try harness.rt.start();

    harness.svc = service_mod.Service.init(allocator, &harness.repo, &harness.rt, .{ .start = 55000, .end = 55100 }, 2000);
    harness.echo = try startTcpEchoServer();
    errdefer harness.echo.deinit();

    harness.alloc = try harness.svc.createAllocation(model.Protocol.tcp, harness.echo.port);
    errdefer harness.alloc.deinit(allocator);
    harness.updated = try harness.svc.setTarget(harness.alloc.id, "127.0.0.1");
    errdefer harness.updated.deinit(allocator);

    return harness;
}

fn connectRelayStream(port: u16) !std.net.Stream {
    const addr = try config.parseIpLiteral("127.0.0.1", port);
    return try std.net.tcpConnectToAddress(addr);
}

fn expectTcpReadClosed(stream: *const std.net.Stream, timeout_ms: u32) !void {
    const deadline_ms = std.time.milliTimestamp() + @as(i64, @intCast(timeout_ms));
    var pollfds = [_]std.posix.pollfd{
        .{ .fd = stream.handle, .events = std.posix.POLL.IN | std.posix.POLL.HUP | std.posix.POLL.ERR, .revents = 0 },
    };
    while (std.time.milliTimestamp() < deadline_ms) {
        const ready = try std.posix.poll(&pollfds, 50);
        if (ready == 0) continue;
        var buf: [16]u8 = undefined;
        const amt = std.posix.read(stream.handle, &buf) catch |err| switch (err) {
            error.ConnectionResetByPeer => return,
            else => return err,
        };
        try std.testing.expectEqual(@as(usize, 0), amt);
        return;
    }
    return error.Timeout;
}

fn startUdpEchoServer() !struct { fd: std.posix.fd_t, port: u16, thread: std.Thread } {
    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC, std.posix.IPPROTO.UDP);
    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    try std.posix.bind(fd, &addr.any, addr.getOsSockLen());
    var bound: std.net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.net.Address);
    try std.posix.getsockname(fd, &bound.any, &len);
    const thread = try std.Thread.spawn(.{}, udpEchoThread, .{fd});
    return .{ .fd = fd, .port = bound.getPort(), .thread = thread };
}

fn udpEchoThread(fd: std.posix.fd_t) void {
    defer closeIgnore(fd);
    var buf: [1024]u8 = undefined;
    var addr: std.net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.net.Address);
    const amt = std.posix.recvfrom(fd, &buf, 0, &addr.any, &len) catch return;
    _ = std.posix.sendto(fd, buf[0..amt], 0, &addr.any, addr.getOsSockLen()) catch {};
}

fn startUdpCaptureServer(allocator: std.mem.Allocator, expected_packets: usize, response_mode: UdpResponseMode, fixed_response: []const u8) !UdpCaptureServer {
    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK, std.posix.IPPROTO.UDP);
    errdefer closeIgnore(fd);

    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    try std.posix.bind(fd, &addr.any, addr.getOsSockLen());

    var bound: std.net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.net.Address);
    try std.posix.getsockname(fd, &bound.any, &len);

    const state = try allocator.create(UdpCaptureState);
    errdefer allocator.destroy(state);
    const records = try allocator.alloc(UdpPacketRecord, expected_packets);
    errdefer allocator.free(records);
    for (records) |*record| record.* = .{};

    state.* = .{
        .fd = fd,
        .expected_packets = expected_packets,
        .response_mode = response_mode,
        .fixed_response = fixed_response,
        .records = records,
    };

    const thread = try std.Thread.spawn(.{}, udpCaptureThread, .{state});
    return .{ .fd = fd, .port = bound.getPort(), .thread = thread, .state = state };
}

fn udpCaptureThread(state: *UdpCaptureState) void {
    const deadline_ms = std.time.milliTimestamp() + 2_000;
    while (state.actual_packets < state.expected_packets and std.time.milliTimestamp() < deadline_ms) {
        var buf: [1024]u8 = undefined;
        var addr: std.net.Address = undefined;
        var len: std.posix.socklen_t = @sizeOf(std.net.Address);
        const amt = std.posix.recvfrom(state.fd, &buf, 0, &addr.any, &len) catch |err| switch (err) {
            error.WouldBlock => {
                std.Thread.sleep(10 * std.time.ns_per_ms);
                continue;
            },
            else => break,
        };

        const idx = state.actual_packets;
        const copy_len = @min(amt, state.records[idx].payload.len);
        state.records[idx].remote_port = addr.getPort();
        state.records[idx].payload_len = copy_len;
        @memcpy(state.records[idx].payload[0..copy_len], buf[0..copy_len]);
        state.actual_packets += 1;

        const response = switch (state.response_mode) {
            .echo => buf[0..amt],
            .fixed => state.fixed_response,
        };
        _ = std.posix.sendto(state.fd, response, 0, &addr.any, addr.getOsSockLen()) catch {};
    }
}

fn startDelayedUdpReplyServer(allocator: std.mem.Allocator, fixed_response: []const u8) !DelayedUdpReplyServer {
    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK, std.posix.IPPROTO.UDP);
    errdefer closeIgnore(fd);

    const addr = try config.parseIpLiteral("127.0.0.1", 0);
    try std.posix.bind(fd, &addr.any, addr.getOsSockLen());

    var bound: std.net.Address = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.net.Address);
    try std.posix.getsockname(fd, &bound.any, &len);

    const state = try allocator.create(DelayedUdpReplyState);
    errdefer allocator.destroy(state);
    state.* = .{
        .fd = fd,
        .fixed_response = fixed_response,
    };

    const thread = try std.Thread.spawn(.{}, delayedUdpReplyThread, .{state});
    return .{ .fd = fd, .port = bound.getPort(), .thread = thread, .state = state };
}

fn delayedUdpReplyThread(state: *DelayedUdpReplyState) void {
    const recv_deadline_ms = std.time.milliTimestamp() + 2_000;
    var peer: std.net.Address = undefined;
    var peer_len: std.posix.socklen_t = @sizeOf(std.net.Address);
    while (std.time.milliTimestamp() < recv_deadline_ms) {
        var buf: [1024]u8 = undefined;
        peer_len = @sizeOf(std.net.Address);
        const amt = std.posix.recvfrom(state.fd, &buf, 0, &peer.any, &peer_len) catch |err| switch (err) {
            error.WouldBlock => {
                std.Thread.sleep(10 * std.time.ns_per_ms);
                continue;
            },
            else => return,
        };

        const copy_len = @min(amt, state.record.payload.len);
        state.record.remote_port = peer.getPort();
        state.record.payload_len = copy_len;
        @memcpy(state.record.payload[0..copy_len], buf[0..copy_len]);
        state.actual_packets = 1;
        state.packet_received.store(true, .monotonic);
        break;
    }

    const reply_deadline_ms = std.time.milliTimestamp() + 2_000;
    while (!state.release_reply.load(.monotonic) and std.time.milliTimestamp() < reply_deadline_ms) {
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
    if (!state.release_reply.load(.monotonic)) return;

    _ = std.posix.sendto(state.fd, state.fixed_response, 0, &peer.any, peer.getOsSockLen()) catch {};
    state.reply_sent.store(true, .monotonic);
}

fn waitForAtomicTrue(flag: *const std.atomic.Value(bool), timeout_ms: u32) !void {
    const deadline_ms = std.time.milliTimestamp() + @as(i64, @intCast(timeout_ms));
    while (std.time.milliTimestamp() < deadline_ms) {
        if (flag.load(.monotonic)) return;
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    return error.Timeout;
}

fn waitForCounterValue(counter: *const metrics_mod.Counter, expected: u64, timeout_ms: u32) !void {
    const deadline_ms = std.time.milliTimestamp() + @as(i64, @intCast(timeout_ms));
    while (std.time.milliTimestamp() < deadline_ms) {
        if (counter.load() == expected) return;
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    return error.Timeout;
}

fn expectUdpReplyCountersIfActive(metrics: *const metrics_mod.Metrics, expected_primary: ?u64, min_dropped_or_stale: u64) !void {
    const primary = metrics.udp_reply_primary_total.load();
    const dropped_or_stale = metrics.udp_reply_drop_total.load() + metrics.udp_reply_stale_total.load();
    if (expected_primary) |value| {
        try std.testing.expectEqual(value, primary);
    }
    try std.testing.expect(dropped_or_stale >= min_dropped_or_stale);
}

fn createUdpClient() !std.posix.fd_t {
    return std.posix.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK, std.posix.IPPROTO.UDP);
}

const UdpBurstSenderCtx = struct {
    target: std.net.Address,
    payload: []const u8,
    count: usize,
    sleep_ms: u64,
};

fn udpBurstSender(ctx: UdpBurstSenderCtx) void {
    const client = createUdpClient() catch return;
    defer closeIgnore(client);
    var idx: usize = 0;
    while (idx < ctx.count) : (idx += 1) {
        _ = std.posix.sendto(client, ctx.payload, 0, &ctx.target.any, ctx.target.getOsSockLen()) catch {};
        if (ctx.sleep_ms > 0) std.Thread.sleep(ctx.sleep_ms * std.time.ns_per_ms);
    }
}

fn sendUdpAndExpect(client_fd: std.posix.fd_t, target: std.net.Address, payload: []const u8, expected: []const u8, timeout_ms: u32) !void {
    _ = try std.posix.sendto(client_fd, payload, 0, &target.any, target.getOsSockLen());
    var buf: [128]u8 = undefined;
    const amt = (try recvUdpWithTimeout(client_fd, &buf, timeout_ms)) orelse return error.Timeout;
    try std.testing.expectEqualStrings(expected, buf[0..amt]);
}

fn expectNoUdpResponse(client_fd: std.posix.fd_t, timeout_ms: u32) !void {
    var buf: [128]u8 = undefined;
    try std.testing.expect((try recvUdpWithTimeout(client_fd, &buf, timeout_ms)) == null);
}

fn recvUdpWithTimeout(fd: std.posix.fd_t, buf: []u8, timeout_ms: u32) !?usize {
    const deadline_ms = std.time.milliTimestamp() + @as(i64, @intCast(timeout_ms));
    while (std.time.milliTimestamp() < deadline_ms) {
        const amt = std.posix.recv(fd, buf, 0) catch |err| switch (err) {
            error.WouldBlock => {
                std.Thread.sleep(5 * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
        return amt;
    }
    return null;
}

fn currentUdpSessionCount(rt: *runtime.RuntimeManager, id: []const u8) ?usize {
    rt.registry_mutex.lock();
    defer rt.registry_mutex.unlock();
    const entry = rt.listeners.get(id) orelse return null;
    entry.mutex.lock();
    defer entry.mutex.unlock();
    return entry.udp_sessions.count();
}

fn waitForUdpSessionCount(rt: *runtime.RuntimeManager, id: []const u8, expected: usize, timeout_ms: u32) !void {
    const deadline_ms = std.time.milliTimestamp() + @as(i64, @intCast(timeout_ms));
    while (std.time.milliTimestamp() < deadline_ms) {
        if (currentUdpSessionCount(rt, id)) |count| {
            if (count == expected) return;
        }
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
    return error.Timeout;
}

fn waitForAllocationRemoval(rt: *runtime.RuntimeManager, id: []const u8, timeout_ms: u32) !void {
    const deadline_ms = std.time.milliTimestamp() + @as(i64, @intCast(timeout_ms));
    while (std.time.milliTimestamp() < deadline_ms) {
        if (currentUdpSessionCount(rt, id) == null) return;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
    return error.Timeout;
}

fn setUdpSessionTtlMs(rt: *runtime.RuntimeManager, id: []const u8, ttl_ms: i64) !void {
    rt.registry_mutex.lock();
    defer rt.registry_mutex.unlock();
    const entry = rt.listeners.get(id) orelse return error.NotFound;
    entry.mutex.lock();
    defer entry.mutex.unlock();
    entry.udp_ttl_ms = ttl_ms;
}

fn closeIgnore(fd: std.posix.fd_t) void {
    switch (std.posix.errno(std.posix.system.close(fd))) {
        .SUCCESS, .BADF => {},
        else => {},
    }
}

test "service forwards tcp traffic after target is configured" {
    var harness = try startTcpForwardHarness(.{});
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    defer closeIgnore(stream.handle);
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_attempt_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_success_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_fallback_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_copy_fallback_total.load());
}

test "tcp session-model forwards traffic and cleans up counters" {
    var harness = try startTcpForwardHarness(.{
        .tcp_session_model_enabled = true,
    });
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    stream.close();

    try waitForCounterValue(&harness.metrics.tcp_session_create_total, 1, 500);
    try waitForCounterValue(&harness.metrics.tcp_session_close_total, 1, 500);
    try std.testing.expect(harness.metrics.tcp_session_event_total.load() > 0);
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_active_sessions.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_attempt_total.load());
}

test "tcp session-model propagates client half-close" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{
        .tcp_session_model_enabled = true,
    });
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55120, .end = 55220 }, 2000);

    var server = try startTcpHalfCloseServer();
    defer server.deinit();

    var alloc = try svc.createAllocation(model.Protocol.tcp, server.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const stream = try connectRelayStream(alloc.port);
    defer closeIgnore(stream.handle);
    _ = try std.posix.write(stream.handle, "ping");
    try std.posix.shutdown(stream.handle, .send);
    var buf: [8]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqualStrings("ack", buf[0..amt]);

    try waitForCounterValue(&metrics.tcp_session_create_total, 1, 500);
    try waitForCounterValue(&metrics.tcp_session_close_total, 1, 500);
    try std.testing.expectEqual(@as(u64, 0), metrics.tcp_active_sessions.load());
}

test "tcp workerized session-model forwards traffic and cleans up counters" {
    var harness = try startTcpForwardHarness(.{
        .tcp_session_model_enabled = true,
        .tcp_session_model_workers = 2,
    });
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    stream.close();

    try waitForCounterValue(&harness.metrics.tcp_session_create_total, 1, 500);
    try waitForCounterValue(&harness.metrics.tcp_session_close_total, 1, 500);
    try std.testing.expect(harness.metrics.tcp_session_worker_dispatch_total.load() >= 1);
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_active_sessions.load());
}

test "tcp workerized session-model distributes sessions across workers" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{
        .tcp_session_model_enabled = true,
        .tcp_session_model_workers = 2,
    });
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55000, .end = 55100 }, 2000);

    var echo = try startTcpMultiEchoServer(2);
    defer echo.deinit();

    var alloc = try svc.createAllocation(model.Protocol.tcp, echo.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    var stream_a = try connectRelayStream(alloc.port);
    var stream_b = try connectRelayStream(alloc.port);
    _ = try std.posix.write(stream_a.handle, "ping");
    _ = try std.posix.write(stream_b.handle, "ping");
    var buf: [4]u8 = undefined;
    _ = try std.posix.read(stream_a.handle, &buf);
    _ = try std.posix.read(stream_b.handle, &buf);
    stream_a.close();
    stream_b.close();

    try waitForCounterValue(&metrics.tcp_session_close_total, 2, 500);
    try std.testing.expect(metrics.tcp_session_worker0_dispatch_total.load() > 0);
    try std.testing.expect(metrics.tcp_session_worker1_dispatch_total.load() > 0);
}

test "tcp sharded-worker forwards traffic and cleans up counters" {
    var harness = try startTcpForwardHarness(.{
        .tcp_session_model_enabled = true,
        .tcp_session_model_workers = 4,
        .tcp_session_model_sharded_accept = true,
    });
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    stream.close();

    try waitForCounterValue(&harness.metrics.tcp_session_create_total, 1, 500);
    try waitForCounterValue(&harness.metrics.tcp_session_close_total, 1, 500);
    try std.testing.expect(harness.metrics.tcp_listener_accept_total.load() >= 1);
    try std.testing.expect(harness.metrics.tcp_upstream_connect_total.load() >= 1);
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_active_sessions.load());
}

test "tcp sharded-worker accept counters cover all workers" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{
        .tcp_session_model_enabled = true,
        .tcp_session_model_workers = 4,
        .tcp_session_model_sharded_accept = true,
    });
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55000, .end = 55100 }, 2000);

    var echo = try startTcpMultiEchoServer(16);
    defer echo.deinit();

    var alloc = try svc.createAllocation(model.Protocol.tcp, echo.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    var idx: usize = 0;
    while (idx < 16) : (idx += 1) {
        var stream = try connectRelayStream(alloc.port);
        _ = try std.posix.write(stream.handle, "ping");
        var buf: [4]u8 = undefined;
        _ = try std.posix.read(stream.handle, &buf);
        stream.close();
    }

    try waitForCounterValue(&metrics.tcp_session_close_total, 16, 1_000);
    const accept_hits = [_]u64{
        metrics.tcp_listener_accept_worker0_total.load(),
        metrics.tcp_listener_accept_worker1_total.load(),
        metrics.tcp_listener_accept_worker2_total.load(),
        metrics.tcp_listener_accept_worker3_total.load(),
    };
    var nonzero_workers: usize = 0;
    for (accept_hits) |value| {
        if (value > 0) nonzero_workers += 1;
    }
    try std.testing.expect(metrics.tcp_listener_accept_total.load() >= 16);
    try std.testing.expect(nonzero_workers >= 2);
}

test "tcp accept-balanced forwards traffic and cleans up counters" {
    var harness = try startTcpForwardHarness(.{
        .tcp_session_model_enabled = true,
        .tcp_session_model_workers = 4,
        .tcp_session_model_accept_balanced = true,
    });
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    stream.close();

    try waitForCounterValue(&harness.metrics.tcp_session_create_total, 1, 500);
    try waitForCounterValue(&harness.metrics.tcp_session_close_total, 1, 500);
    try std.testing.expect(harness.metrics.tcp_accept_handoff_total.load() >= 1);
    try std.testing.expect(harness.metrics.tcp_upstream_connect_total.load() >= 1);
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_active_sessions.load());
}

test "tcp accept-balanced handoff counters cover all workers" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{
        .tcp_session_model_enabled = true,
        .tcp_session_model_workers = 4,
        .tcp_session_model_accept_balanced = true,
    });
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55000, .end = 55100 }, 2000);

    var echo = try startTcpMultiEchoServer(16);
    defer echo.deinit();

    var alloc = try svc.createAllocation(model.Protocol.tcp, echo.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    var idx: usize = 0;
    while (idx < 16) : (idx += 1) {
        var stream = try connectRelayStream(alloc.port);
        _ = try std.posix.write(stream.handle, "ping");
        var buf: [4]u8 = undefined;
        _ = try std.posix.read(stream.handle, &buf);
        stream.close();
    }

    try waitForCounterValue(&metrics.tcp_session_close_total, 16, 1_000);
    try std.testing.expect(metrics.tcp_accept_handoff_worker0_total.load() > 0);
    try std.testing.expect(metrics.tcp_accept_handoff_worker1_total.load() > 0);
    try std.testing.expect(metrics.tcp_accept_handoff_worker2_total.load() > 0);
    try std.testing.expect(metrics.tcp_accept_handoff_worker3_total.load() > 0);
}

test "tcp forced-copy override wins over splice-enabled mode" {
    var harness = try startTcpForwardHarness(.{
        .tcp_splice_enabled = true,
        .force_tcp_copy_fallback = true,
    });
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    defer closeIgnore(stream.handle);
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_attempt_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_success_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_fallback_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_fallback_forced_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_copy_fallback_total.load());
}

test "tcp splice-enabled mode records splice success" {
    var harness = try startTcpForwardHarness(.{
        .tcp_splice_enabled = true,
    });
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    stream.close();
    try waitForCounterValue(&harness.metrics.tcp_splice_success_total, 1, 500);
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_attempt_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_success_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_fast_path_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_fallback_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_hard_failure_total.load());
}

test "tcp splice recoverable setup failure falls back to copy" {
    var harness = try startTcpForwardHarness(.{
        .tcp_splice_enabled = true,
        .tcp_splice_test_mode = .recoverable_setup_failure,
    });
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    defer closeIgnore(stream.handle);
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_attempt_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_success_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_fallback_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_fallback_unsupported_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_hard_failure_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_copy_fallback_total.load());
}

test "tcp splice recoverable runtime failure increments runtime fallback bucket" {
    var harness = try startTcpForwardHarness(.{
        .tcp_splice_enabled = true,
        .tcp_splice_test_mode = .recoverable_runtime_failure,
    });
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    defer closeIgnore(stream.handle);
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_attempt_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_success_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_fallback_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_fallback_runtime_error_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_hard_failure_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_copy_fallback_total.load());
}

test "tcp splice hard failure aborts session predictably" {
    var harness = try startTcpForwardHarness(.{
        .tcp_splice_enabled = true,
        .tcp_splice_test_mode = .hard_failure,
    });
    defer harness.deinit(std.testing.allocator);

    const stream = try connectRelayStream(harness.alloc.port);
    defer closeIgnore(stream.handle);
    _ = std.posix.write(stream.handle, "ping") catch {};
    try expectTcpReadClosed(&stream, 500);
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_attempt_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_success_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_splice_fallback_total.load());
    try std.testing.expectEqual(@as(u64, 1), harness.metrics.tcp_splice_hard_failure_total.load());
    try std.testing.expectEqual(@as(u64, 0), harness.metrics.tcp_copy_fallback_total.load());
}

test "runtime deinit does not hang with active tcp session" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55200, .end = 55300 }, 2000);

    try rt.start();
    var echo = try startTcpEchoServer();
    defer {
        echo.server.deinit();
        echo.thread.join();
    }

    var alloc = try svc.createAllocation(model.Protocol.tcp, echo.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const addr = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const stream = try std.net.tcpConnectToAddress(addr);
    defer closeIgnore(stream.handle);

    rt.deinit();
}

test "runtime deinit does not hang with active tcp session in session-model mode" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{
        .tcp_session_model_enabled = true,
    });
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55320, .end = 55420 }, 2000);

    try rt.start();
    var echo = try startTcpEchoServer();
    defer echo.deinit();

    var alloc = try svc.createAllocation(model.Protocol.tcp, echo.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const stream = try connectRelayStream(alloc.port);
    defer closeIgnore(stream.handle);
    try waitForCounterValue(&metrics.tcp_session_create_total, 1, 500);

    rt.deinit();
    try std.testing.expectEqual(@as(u64, 0), metrics.tcp_active_sessions.load());
    try std.testing.expect(metrics.tcp_session_close_total.load() >= 1);
}

test "runtime deinit does not hang with active tcp session in workerized session-model mode" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{
        .tcp_session_model_enabled = true,
        .tcp_session_model_workers = 2,
    });
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55440, .end = 55540 }, 2000);

    try rt.start();
    var echo = try startTcpEchoServer();
    defer echo.deinit();

    var alloc = try svc.createAllocation(model.Protocol.tcp, echo.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const stream = try connectRelayStream(alloc.port);
    defer closeIgnore(stream.handle);
    try waitForCounterValue(&metrics.tcp_session_create_total, 1, 500);

    rt.deinit();
    try std.testing.expectEqual(@as(u64, 0), metrics.tcp_active_sessions.load());
    try std.testing.expect(metrics.tcp_session_close_total.load() >= 1);
}

test "runtime deinit does not hang with active tcp session in sharded-worker mode" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{
        .tcp_session_model_enabled = true,
        .tcp_session_model_workers = 4,
        .tcp_session_model_sharded_accept = true,
    });
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55560, .end = 55660 }, 2000);

    try rt.start();
    var echo = try startTcpEchoServer();
    defer echo.deinit();

    var alloc = try svc.createAllocation(model.Protocol.tcp, echo.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const stream = try connectRelayStream(alloc.port);
    defer closeIgnore(stream.handle);
    try waitForCounterValue(&metrics.tcp_session_create_total, 1, 500);

    rt.deinit();
    try std.testing.expectEqual(@as(u64, 0), metrics.tcp_active_sessions.load());
    try std.testing.expect(metrics.tcp_session_close_total.load() >= 1);
}

test "runtime deinit does not hang with active tcp session in accept-balanced mode" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{
        .tcp_session_model_enabled = true,
        .tcp_session_model_workers = 4,
        .tcp_session_model_accept_balanced = true,
    });
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55680, .end = 55780 }, 2000);

    try rt.start();
    var echo = try startTcpEchoServer();
    defer echo.deinit();

    var alloc = try svc.createAllocation(model.Protocol.tcp, echo.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const stream = try connectRelayStream(alloc.port);
    defer closeIgnore(stream.handle);
    try waitForCounterValue(&metrics.tcp_session_create_total, 1, 500);

    rt.deinit();
    try std.testing.expectEqual(@as(u64, 0), metrics.tcp_active_sessions.load());
    try std.testing.expect(metrics.tcp_session_close_total.load() >= 1);
}

test "service forwards udp traffic after target is configured" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55400, .end = 55500 }, 2000);

    var echo = try startUdpEchoServer();
    defer {
        closeIgnore(echo.fd);
        echo.thread.join();
    }

    var alloc = try svc.createAllocation(model.Protocol.udp, echo.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const client = try createUdpClient();
    defer closeIgnore(client);
    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    try sendUdpAndExpect(client, target, "pong", "pong", 250);
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_packets_in_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_packets_out_total.load());
    try std.testing.expect(metrics.udp_bytes_in_total.load() >= 4);
    try std.testing.expect(metrics.udp_bytes_out_total.load() >= 4);
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_session_create_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_active_sessions.load());
}

test "udp runtime reuses a session for repeat packets from one client and creates a second session for another client" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55600, .end = 55700 }, 2000);

    var upstream = try startUdpCaptureServer(std.testing.allocator, 3, .echo, "");
    defer upstream.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const client_one = try createUdpClient();
    defer closeIgnore(client_one);
    const client_two = try createUdpClient();
    defer closeIgnore(client_two);

    try sendUdpAndExpect(client_one, target, "one-a", "one-a", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    try sendUdpAndExpect(client_one, target, "one-b", "one-b", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    try sendUdpAndExpect(client_two, target, "two-a", "two-a", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 2, 500);

    upstream.thread.join();
    try std.testing.expectEqual(@as(usize, 3), upstream.state.actual_packets);
    try std.testing.expectEqual(upstream.state.records[0].remote_port, upstream.state.records[1].remote_port);
    try std.testing.expect(upstream.state.records[0].remote_port != upstream.state.records[2].remote_port);
    try std.testing.expectEqual(@as(u64, 3), metrics.udp_packets_in_total.load());
    try std.testing.expectEqual(@as(u64, 3), metrics.udp_packets_out_total.load());
    try std.testing.expect(metrics.udp_bytes_in_total.load() >= 15);
    try std.testing.expect(metrics.udp_bytes_out_total.load() >= 15);
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_session_create_total.load());
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_active_sessions.load());
    upstream.joined = true;
}

test "udp session cleanup expires idle sessions after ttl" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55800, .end = 55900 }, 2000);

    var upstream = try startUdpCaptureServer(std.testing.allocator, 2, .echo, "");
    defer upstream.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);
    try setUdpSessionTtlMs(&rt, alloc.id, 25);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const client = try createUdpClient();
    defer closeIgnore(client);

    try sendUdpAndExpect(client, target, "ttl-1", "ttl-1", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);
    try waitForUdpSessionCount(&rt, alloc.id, 0, 1_000);

    try sendUdpAndExpect(client, target, "ttl-2", "ttl-2", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    upstream.thread.join();
    try std.testing.expectEqual(@as(usize, 2), upstream.state.actual_packets);
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_packets_in_total.load());
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_packets_out_total.load());
    try std.testing.expect(metrics.udp_bytes_in_total.load() >= 10);
    try std.testing.expect(metrics.udp_bytes_out_total.load() >= 10);
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_session_create_total.load());
    try std.testing.expect(metrics.udp_session_expire_total.load() >= 1);
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_active_sessions.load());
    upstream.joined = true;
}

test "udp update replaces the active session and forwards to the new target" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 56000, .end = 56100 }, 2000);

    var upstream_one = try startUdpCaptureServer(std.testing.allocator, 1, .fixed, "one");
    defer upstream_one.deinit(std.testing.allocator);
    var upstream_two = try startUdpCaptureServer(std.testing.allocator, 1, .fixed, "two");
    defer upstream_two.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream_one.port);
    defer alloc.deinit(std.testing.allocator);
    var configured = try svc.setTarget(alloc.id, "127.0.0.1");
    defer configured.deinit(std.testing.allocator);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const client = try createUdpClient();
    defer closeIgnore(client);

    try sendUdpAndExpect(client, target, "first", "one", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    var updated = try svc.updateAllocation(alloc.id, upstream_two.port, null);
    defer updated.deinit(std.testing.allocator);
    try waitForUdpSessionCount(&rt, alloc.id, 0, 500);

    try sendUdpAndExpect(client, target, "second", "two", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    upstream_one.thread.join();
    upstream_two.thread.join();
    try std.testing.expectEqual(@as(usize, 1), upstream_one.state.actual_packets);
    try std.testing.expectEqual(@as(usize, 1), upstream_two.state.actual_packets);
    try std.testing.expectEqual(@as(u64, 2), metrics.runtime_apply_total.load());
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_packets_in_total.load());
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_packets_out_total.load());
    try std.testing.expect(metrics.udp_bytes_in_total.load() >= 11);
    try std.testing.expect(metrics.udp_bytes_out_total.load() >= 6);
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_session_create_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_active_sessions.load());
    upstream_one.joined = true;
    upstream_two.joined = true;
}

test "udp delayed reply from replaced target is dropped and the new target still replies" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 56800, .end = 56900 }, 2000);

    var upstream_old = try startDelayedUdpReplyServer(std.testing.allocator, "old");
    defer upstream_old.deinit(std.testing.allocator);
    var upstream_new = try startUdpCaptureServer(std.testing.allocator, 1, .fixed, "new");
    defer upstream_new.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream_old.port);
    defer alloc.deinit(std.testing.allocator);
    var configured = try svc.setTarget(alloc.id, "127.0.0.1");
    defer configured.deinit(std.testing.allocator);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const client = try createUdpClient();
    defer closeIgnore(client);

    _ = try std.posix.sendto(client, "before-update", 0, &target.any, target.getOsSockLen());
    try waitForAtomicTrue(&upstream_old.state.packet_received, 500);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    var updated = try svc.updateAllocation(alloc.id, upstream_new.port, null);
    defer updated.deinit(std.testing.allocator);
    try waitForUdpSessionCount(&rt, alloc.id, 0, 500);

    upstream_old.release();
    try waitForAtomicTrue(&upstream_old.state.reply_sent, 500);
    try expectNoUdpResponse(client, 150);

    try sendUdpAndExpect(client, target, "after-update", "new", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    upstream_old.thread.join();
    upstream_old.joined = true;
    upstream_new.thread.join();
    upstream_new.joined = true;

    try std.testing.expectEqual(@as(usize, 1), upstream_old.state.actual_packets);
    try std.testing.expectEqual(@as(usize, 1), upstream_new.state.actual_packets);
    try std.testing.expectEqual(@as(u64, 2), metrics.runtime_apply_total.load());
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_packets_in_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_packets_out_total.load());
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_session_create_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_active_sessions.load());
    try expectUdpReplyCountersIfActive(&metrics, 1, 1);
}

test "udp delayed reply from deleted target is dropped without leaking the session" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 57000, .end = 57100 }, 2000);

    var upstream = try startDelayedUdpReplyServer(std.testing.allocator, "gone");
    defer upstream.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const client = try createUdpClient();
    defer closeIgnore(client);

    _ = try std.posix.sendto(client, "before-delete", 0, &target.any, target.getOsSockLen());
    try waitForAtomicTrue(&upstream.state.packet_received, 500);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    try svc.deleteAllocation(alloc.id);
    try waitForAllocationRemoval(&rt, alloc.id, 500);

    upstream.release();
    try waitForAtomicTrue(&upstream.state.reply_sent, 500);
    try expectNoUdpResponse(client, 150);

    upstream.thread.join();
    upstream.joined = true;

    try std.testing.expectEqual(@as(usize, 1), upstream.state.actual_packets);
    try std.testing.expectEqual(@as(u64, 2), metrics.runtime_apply_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_packets_in_total.load());
    try std.testing.expectEqual(@as(u64, 0), metrics.udp_packets_out_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_session_create_total.load());
    try std.testing.expectEqual(@as(u64, 0), metrics.udp_active_sessions.load());
    try expectUdpReplyCountersIfActive(&metrics, 0, 1);
}

const RuntimeDeinitCtx = struct {
    rt: *runtime.RuntimeManager,
    done: *std.atomic.Value(bool),
};

fn runtimeDeinitThread(ctx: RuntimeDeinitCtx) void {
    ctx.rt.deinit();
    ctx.done.store(true, .monotonic);
}

test "udp delayed reply during runtime deinit is dropped and shutdown completes" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 57200, .end = 57300 }, 2000);

    var upstream = try startDelayedUdpReplyServer(std.testing.allocator, "late");
    defer upstream.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const client = try createUdpClient();
    defer closeIgnore(client);

    _ = try std.posix.sendto(client, "before-stop", 0, &target.any, target.getOsSockLen());
    try waitForAtomicTrue(&upstream.state.packet_received, 500);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    var deinit_done = std.atomic.Value(bool).init(false);
    const deinit_thread = try std.Thread.spawn(.{}, runtimeDeinitThread, .{RuntimeDeinitCtx{ .rt = &rt, .done = &deinit_done }});
    defer deinit_thread.join();

    try waitForAtomicTrue(&deinit_done, 1_000);
    upstream.release();
    try waitForAtomicTrue(&upstream.state.reply_sent, 500);
    try expectNoUdpResponse(client, 150);

    upstream.thread.join();
    upstream.joined = true;

    try std.testing.expectEqual(@as(usize, 1), upstream.state.actual_packets);
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_packets_in_total.load());
    try std.testing.expectEqual(@as(u64, 0), metrics.udp_packets_out_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_session_create_total.load());
    try std.testing.expectEqual(@as(u64, 0), metrics.udp_active_sessions.load());
    try expectUdpReplyCountersIfActive(&metrics, 0, 1);
}

test "udp delete closes the active session and stops forwarding" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 56200, .end = 56300 }, 2000);

    var upstream = try startUdpCaptureServer(std.testing.allocator, 1, .echo, "");
    defer upstream.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const client = try createUdpClient();
    defer closeIgnore(client);

    try sendUdpAndExpect(client, target, "before-delete", "before-delete", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    try svc.deleteAllocation(alloc.id);
    try waitForAllocationRemoval(&rt, alloc.id, 500);

    _ = try std.posix.sendto(client, "after-delete", 0, &target.any, target.getOsSockLen());
    try expectNoUdpResponse(client, 150);

    upstream.thread.join();
    try std.testing.expectEqual(@as(usize, 1), upstream.state.actual_packets);
    try std.testing.expectEqual(@as(u64, 2), metrics.runtime_apply_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_session_create_total.load());
    try std.testing.expectEqual(@as(u64, 0), metrics.udp_active_sessions.load());
    upstream.joined = true;
}

test "udp delete clears two active sessions and resets the active-session gauge" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 56320, .end = 56420 }, 2000);

    var upstream = try startUdpCaptureServer(std.testing.allocator, 2, .echo, "");
    defer upstream.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const client_one = try createUdpClient();
    defer closeIgnore(client_one);
    const client_two = try createUdpClient();
    defer closeIgnore(client_two);

    try sendUdpAndExpect(client_one, target, "alpha", "alpha", 250);
    try sendUdpAndExpect(client_two, target, "beta", "beta", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 2, 500);
    try std.testing.expectEqual(@as(u64, 2), metrics.udp_active_sessions.load());

    try svc.deleteAllocation(alloc.id);
    try waitForAllocationRemoval(&rt, alloc.id, 500);
    try std.testing.expectEqual(@as(u64, 0), metrics.udp_active_sessions.load());

    upstream.thread.join();
    upstream.joined = true;
}

test "runtime deinit does not hang with an active udp session" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 56400, .end = 56500 }, 2000);

    var upstream = try startUdpCaptureServer(std.testing.allocator, 1, .echo, "");
    defer upstream.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream.port);
    defer alloc.deinit(std.testing.allocator);
    var updated = try svc.setTarget(alloc.id, "127.0.0.1");
    defer updated.deinit(std.testing.allocator);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const client = try createUdpClient();
    defer closeIgnore(client);

    try sendUdpAndExpect(client, target, "live", "live", 250);
    try waitForUdpSessionCount(&rt, alloc.id, 1, 500);

    rt.deinit();

    _ = try std.posix.sendto(client, "after-stop", 0, &target.any, target.getOsSockLen());
    try expectNoUdpResponse(client, 150);
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_packets_in_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_packets_out_total.load());
    try std.testing.expectEqual(@as(u64, 1), metrics.udp_session_create_total.load());
    try std.testing.expectEqual(@as(u64, 0), metrics.udp_active_sessions.load());
}

test "udp update under concurrent ingress keeps forwarding and does not hang" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, .{});
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 56600, .end = 56700 }, 2000);

    var upstream_one = try startUdpCaptureServer(std.testing.allocator, 32, .fixed, "one");
    defer upstream_one.deinit(std.testing.allocator);
    var upstream_two = try startUdpCaptureServer(std.testing.allocator, 1, .fixed, "two");
    defer upstream_two.deinit(std.testing.allocator);

    var alloc = try svc.createAllocation(model.Protocol.udp, upstream_one.port);
    defer alloc.deinit(std.testing.allocator);
    var configured = try svc.setTarget(alloc.id, "127.0.0.1");
    defer configured.deinit(std.testing.allocator);

    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    const burst = try std.Thread.spawn(.{}, udpBurstSender, .{UdpBurstSenderCtx{
        .target = target,
        .payload = "burst",
        .count = 32,
        .sleep_ms = 1,
    }});

    std.Thread.sleep(25 * std.time.ns_per_ms);
    var updated = try svc.updateAllocation(alloc.id, upstream_two.port, null);
    defer updated.deinit(std.testing.allocator);
    try waitForUdpSessionCount(&rt, alloc.id, 0, 1_000);
    burst.join();
    try std.testing.expect(metrics.udp_session_create_total.load() >= 1);
    try std.testing.expect(metrics.runtime_apply_total.load() >= 2);
    try std.testing.expect(currentUdpSessionCount(&rt, alloc.id) != null);
}
