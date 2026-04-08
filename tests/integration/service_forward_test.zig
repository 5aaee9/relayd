const std = @import("std");
const config = @import("../../src/config.zig");
const sqlite = @import("../../src/storage/sqlite.zig");
const runtime = @import("../../src/runtime/manager.zig");
const service_mod = @import("../../src/service/allocation_service.zig");
const metrics_mod = @import("../../src/metrics.zig");
const model = @import("../../src/model/allocation.zig");

fn startTcpEchoServer() !struct { server: std.net.Server, port: u16, thread: std.Thread } {
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

fn closeIgnore(fd: std.posix.fd_t) void {
    switch (std.posix.errno(std.posix.system.close(fd))) {
        .SUCCESS, .BADF => {},
        else => {},
    }
}

test "service forwards tcp traffic after target is configured" {
    try std.fs.cwd().makePath(".zig-cache/integration-tests");
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/integration-tests/{d}.sqlite", .{std.time.nanoTimestamp()});
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, false);
    defer rt.deinit();
    try rt.start();
    var svc = service_mod.Service.init(std.testing.allocator, &repo, &rt, .{ .start = 55000, .end = 55100 }, 2000);

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
    defer stream.close();
    _ = try std.posix.write(stream.handle, "ping");
    var buf: [4]u8 = undefined;
    const amt = try std.posix.read(stream.handle, &buf);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("ping", &buf);
}

test "runtime deinit does not hang with active tcp session" {
    try std.fs.cwd().makePath(".zig-cache/integration-tests");
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/integration-tests/{d}.sqlite", .{std.time.nanoTimestamp()});
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, false);
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
    defer stream.close();

    rt.deinit();
}

test "service forwards udp traffic after target is configured" {
    try std.fs.cwd().makePath(".zig-cache/integration-tests");
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/integration-tests/{d}.sqlite", .{std.time.nanoTimestamp()});
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.testing.allocator.free(path);
    }

    var repo = try sqlite.Repository.open(std.testing.allocator, path);
    defer repo.close();
    var metrics = metrics_mod.Metrics{};
    var rt = try runtime.RuntimeManager.init(std.testing.allocator, &metrics, false);
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

    const client = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC, std.posix.IPPROTO.UDP);
    defer closeIgnore(client);
    const target = try config.parseIpLiteral("127.0.0.1", alloc.port);
    _ = try std.posix.sendto(client, "pong", 0, &target.any, target.getOsSockLen());
    var buf: [4]u8 = undefined;
    const amt = try std.posix.recv(client, &buf, 0);
    try std.testing.expectEqual(@as(usize, 4), amt);
    try std.testing.expectEqualStrings("pong", &buf);
}
