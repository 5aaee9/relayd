const std = @import("std");
const posix = std.posix;
const net = std.net;
const model = @import("../model/allocation.zig");
const config_mod = @import("../config.zig");
const Metrics = @import("../metrics.zig").Metrics;

pub const ObservedState = struct {
    runtime_status: model.RuntimeStatus,
    effective_host: ?[]u8,
    effective_target_port: ?u16,
    error_kind: ?model.ErrorKind,
    last_error: ?[]u8,

    pub fn deinit(self: *ObservedState, allocator: std.mem.Allocator) void {
        if (self.effective_host) |host| allocator.free(host);
        if (self.last_error) |msg| allocator.free(msg);
        self.* = undefined;
    }
};

pub const Options = struct {
    tcp_session_model_enabled: bool = false,
    tcp_splice_enabled: bool = false,
    force_tcp_copy_fallback: bool = false,
    tcp_splice_test_mode: TcpSpliceTestMode = .off,
    udp_socket_recv_buffer_bytes: u32 = 0,
    udp_socket_send_buffer_bytes: u32 = 0,
};

pub const TcpSpliceTestMode = enum {
    off,
    recoverable_setup_failure,
    recoverable_runtime_failure,
    hard_failure,
};

pub const RuntimeManager = struct {
    allocator: std.mem.Allocator,
    metrics: *Metrics,
    tcp_session_model_enabled: bool,
    tcp_splice_enabled: bool,
    force_tcp_copy_fallback: bool,
    tcp_splice_test_mode: TcpSpliceTestMode,
    udp_socket_recv_buffer_bytes: u32,
    udp_socket_send_buffer_bytes: u32,
    epoll_fd: posix.fd_t,
    thread: ?std.Thread,
    stop_flag: bool,
    stop_mutex: std.Thread.Mutex,
    registry_mutex: std.Thread.Mutex,
    listeners: std.StringHashMap(*ListenerEntry),
    listener_fds: std.AutoHashMap(posix.fd_t, *ListenerEntry),
    udp_reply_fds: std.AutoHashMap(posix.fd_t, UdpReplyDispatch),
    tcp_sessions_mutex: std.Thread.Mutex,
    tcp_sessions: std.ArrayList(*TcpSessionCtx),
    tcp_runtime_sessions: std.ArrayList(*TcpRuntimeSession),
    tcp_runtime_session_fds: std.AutoHashMap(posix.fd_t, *TcpRuntimeSession),

    pub fn init(allocator: std.mem.Allocator, metrics: *Metrics, options: Options) !RuntimeManager {
        const epoll_fd = try posix.epoll_create1(0);
        return .{
            .allocator = allocator,
            .metrics = metrics,
            .tcp_session_model_enabled = options.tcp_session_model_enabled,
            .tcp_splice_enabled = options.tcp_splice_enabled,
            .force_tcp_copy_fallback = options.force_tcp_copy_fallback,
            .tcp_splice_test_mode = options.tcp_splice_test_mode,
            .udp_socket_recv_buffer_bytes = options.udp_socket_recv_buffer_bytes,
            .udp_socket_send_buffer_bytes = options.udp_socket_send_buffer_bytes,
            .epoll_fd = epoll_fd,
            .thread = null,
            .stop_flag = false,
            .stop_mutex = .{},
            .registry_mutex = .{},
            .listeners = std.StringHashMap(*ListenerEntry).init(allocator),
            .listener_fds = std.AutoHashMap(posix.fd_t, *ListenerEntry).init(allocator),
            .udp_reply_fds = std.AutoHashMap(posix.fd_t, UdpReplyDispatch).init(allocator),
            .tcp_sessions_mutex = .{},
            .tcp_sessions = .{},
            .tcp_runtime_sessions = .{},
            .tcp_runtime_session_fds = std.AutoHashMap(posix.fd_t, *TcpRuntimeSession).init(allocator),
        };
    }

    pub fn start(self: *RuntimeManager) !void {
        self.thread = try std.Thread.spawn(.{}, threadMain, .{self});
    }

    pub fn stop(self: *RuntimeManager) void {
        self.setStop();
        if (self.thread) |thread| thread.join();
        self.thread = null;
    }

    pub fn deinit(self: *RuntimeManager) void {
        self.stop();
        self.registry_mutex.lock();
        var it = self.listeners.iterator();
        while (it.next()) |entry| {
            self.closeEntryFd(entry.value_ptr.*);
            destroyEntry(self.allocator, entry.value_ptr.*);
        }
        self.listeners.deinit();
        self.listener_fds.deinit();
        self.udp_reply_fds.deinit();
        self.registry_mutex.unlock();
        self.shutdownTcpSessions();
        self.tcp_sessions.deinit(self.allocator);
        self.shutdownTcpRuntimeSessions();
        self.tcp_runtime_sessions.deinit(self.allocator);
        self.tcp_runtime_session_fds.deinit();
        posix.close(self.epoll_fd);
    }

    pub fn create(self: *RuntimeManager, allocation: model.Allocation, timeout_ms: u32) !void {
        _ = timeout_ms;
        const entry = try self.createEntry(allocation);
        if (self.bindEntry(entry)) |fd| {
            entry.fd = fd;
            try self.addToRegistry(entry);
            self.metrics.allocations_total.inc();
            return;
        } else |_| {
            destroyEntry(self.allocator, entry);
            self.metrics.bind_fail_total.inc();
            return error.RuntimeCreateFailed;
        }
    }

    pub fn restore(self: *RuntimeManager, allocation: model.Allocation, timeout_ms: u32) !void {
        _ = timeout_ms;
        const entry = try self.createEntry(allocation);
        if (self.bindEntry(entry)) |fd| {
            entry.fd = fd;
            try self.addToRegistry(entry);
            self.metrics.allocations_total.inc();
            return;
        } else |err| {
            entry.status = .degraded_bind_failed;
            entry.error_kind = .bind_failed;
            entry.last_error = dupErr(self.allocator, @errorName(err)) catch null;
            try self.addToRegistry(entry);
            self.metrics.bind_fail_total.inc();
            self.metrics.restore_failures_total.inc();
        }
    }

    pub fn update(self: *RuntimeManager, allocation: model.Allocation, timeout_ms: u32) !void {
        _ = timeout_ms;
        self.registry_mutex.lock();
        defer self.registry_mutex.unlock();
        const entry = self.listeners.get(allocation.id) orelse return error.RuntimeUpdateFailed;
        entry.mutex.lock();
        defer entry.mutex.unlock();

        replaceOptionalString(self.allocator, &entry.desired_host, allocation.host) catch {};
        entry.desired_target_port = allocation.target_port;
        entry.updated_at_ms = allocation.updated_at_ms;

        if (!model.isHostConfigured(entry.desired_host)) {
            replaceOptionalString(self.allocator, &entry.effective_host, null) catch {};
            entry.effective_target_port = null;
            entry.status = .rejecting_no_host;
            entry.error_kind = null;
            clearLastError(self.allocator, entry);
            self.metrics.runtime_apply_total.inc();
            return;
        }

        if (entry.fd == null) {
            entry.status = .degraded_apply_failed;
            entry.error_kind = .apply_failed;
            setLastError(self.allocator, entry, "listener unavailable") catch {};
            self.metrics.runtime_apply_total.inc();
            return error.RuntimeUpdateFailed;
        }

        replaceOptionalString(self.allocator, &entry.effective_host, entry.desired_host) catch {};
        entry.effective_target_port = entry.desired_target_port;
        entry.status = .active;
        entry.error_kind = null;
        clearLastError(self.allocator, entry);
        closeUdpSessions(self.allocator, entry);
        self.metrics.runtime_apply_total.inc();
    }

    pub fn delete(self: *RuntimeManager, id: []const u8, timeout_ms: u32) !void {
        _ = timeout_ms;
        self.registry_mutex.lock();
        defer self.registry_mutex.unlock();
        const entry = self.listeners.fetchRemove(id) orelse return error.RuntimeDeleteFailed;
        entry.value.mutex.lock();
        closeUdpSessions(self.allocator, entry.value);
        self.closeEntryFd(entry.value);
        entry.value.mutex.unlock();
        destroyEntry(self.allocator, entry.value);
        self.metrics.runtime_apply_total.inc();
    }

    pub fn snapshot(self: *RuntimeManager, allocator: std.mem.Allocator, id: []const u8) !?ObservedState {
        self.registry_mutex.lock();
        defer self.registry_mutex.unlock();
        const entry = self.listeners.get(id) orelse return null;
        entry.mutex.lock();
        defer entry.mutex.unlock();
        return try entry.observed(allocator);
    }

    fn threadMain(self: *RuntimeManager) void {
        var events: [64]std.os.linux.epoll_event = undefined;
        while (true) {
            if (self.shouldStop()) break;
            self.cleanupUdpSessions();
            self.reapTcpSessions();
            const count = posix.epoll_wait(self.epoll_fd, events[0..], 100);
            for (events[0..count]) |event| {
                self.handleFd(@intCast(event.data.fd));
            }
        }
    }

    fn shouldStop(self: *RuntimeManager) bool {
        self.stop_mutex.lock();
        defer self.stop_mutex.unlock();
        return self.stop_flag;
    }

    fn setStop(self: *RuntimeManager) void {
        self.stop_mutex.lock();
        defer self.stop_mutex.unlock();
        self.stop_flag = true;
    }

    fn handleFd(self: *RuntimeManager, fd: posix.fd_t) void {
        self.registry_mutex.lock();
        if (self.listener_fds.get(fd)) |entry| {
            entry.mutex.lock();
            switch (entry.protocol) {
                .tcp => {
                    self.registry_mutex.unlock();
                    defer entry.mutex.unlock();
                    handleTcpAccept(self, entry);
                },
                .udp => {
                    defer self.registry_mutex.unlock();
                    defer entry.mutex.unlock();
                    handleUdpReadable(self.allocator, self.metrics, entry);
                },
            }
            return;
        }
        if (self.udp_reply_fds.get(fd)) |dispatch| {
            const entry = dispatch.entry;
            entry.mutex.lock();
            defer self.registry_mutex.unlock();
            defer entry.mutex.unlock();
            handleUdpReplyReadable(self.metrics, entry, dispatch.key);
            return;
        }
        self.registry_mutex.unlock();
        if (self.tcp_runtime_session_fds.get(fd)) |session| {
            self.handleTcpRuntimeSessionFd(session, fd);
            return;
        }
        self.metrics.udp_reply_stale_total.inc();
    }

    fn cleanupUdpSessions(self: *RuntimeManager) void {
        self.registry_mutex.lock();
        defer self.registry_mutex.unlock();
        var it = self.listeners.iterator();
        const now_ms = std.time.milliTimestamp();
        while (it.next()) |kv| {
            const entry = kv.value_ptr.*;
            if (entry.protocol != .udp) continue;
            entry.mutex.lock();
            var removed_any = true;
            while (removed_any) {
                removed_any = false;
                var session_it = entry.udp_sessions.iterator();
                while (session_it.next()) |session_kv| {
                    const session = session_kv.value_ptr.*;
                    if (now_ms - session.last_seen_ms.load(.monotonic) <= entry.udp_ttl_ms) continue;
                    const owned_key = session.key;
                    const removed = entry.udp_sessions.fetchRemove(owned_key) orelse continue;
                    _ = removed;
                    if (entry.udp_cached_key) |cached_key| {
                        if (std.meta.eql(cached_key, owned_key)) {
                            entry.udp_cached_key = null;
                            entry.udp_cached_session = null;
                        }
                    }
                    self.metrics.udp_session_expire_total.inc();
                    removeUdpSessionLocked(entry, owned_key, session, false);
                    removed_any = true;
                    break;
                }
            }
            entry.mutex.unlock();
        }
    }

    fn reapTcpSessions(self: *RuntimeManager) void {
        self.tcp_sessions_mutex.lock();
        defer self.tcp_sessions_mutex.unlock();
        var idx: usize = 0;
        while (idx < self.tcp_sessions.items.len) {
            const ctx = self.tcp_sessions.items[idx];
            ctx.mutex.lock();
            const finished = ctx.finished;
            ctx.mutex.unlock();
            if (!finished) {
                idx += 1;
                continue;
            }
            if (ctx.thread) |thread| thread.join();
            self.allocator.free(ctx.host);
            self.allocator.destroy(ctx);
            _ = self.tcp_sessions.swapRemove(idx);
        }
    }

    fn shutdownTcpSessions(self: *RuntimeManager) void {
        while (true) {
            self.tcp_sessions_mutex.lock();
            if (self.tcp_sessions.items.len == 0) {
                self.tcp_sessions_mutex.unlock();
                break;
            }
            const ctx = self.tcp_sessions.swapRemove(self.tcp_sessions.items.len - 1);
            self.tcp_sessions_mutex.unlock();

            shutdownIgnoreBadFd(ctx.client_fd);
            closeIgnoreBadFd(ctx.client_fd);
            if (ctx.upstream_fd) |fd| {
                shutdownIgnoreBadFd(fd);
                closeIgnoreBadFd(fd);
            }
            if (ctx.thread) |thread| thread.join();
            self.allocator.free(ctx.host);
            self.allocator.destroy(ctx);
        }
    }

    fn createEntry(self: *RuntimeManager, allocation: model.Allocation) !*ListenerEntry {
        const entry = try self.allocator.create(ListenerEntry);
        entry.* = .{
            .allocator = self.allocator,
            .manager = self,
            .id = try self.allocator.dupe(u8, allocation.id),
            .protocol = allocation.protocol,
            .port = allocation.port,
            .desired_target_port = allocation.target_port,
            .desired_host = if (allocation.host) |host| try self.allocator.dupe(u8, host) else null,
            .effective_target_port = if (model.isHostConfigured(allocation.host)) allocation.target_port else null,
            .effective_host = if (allocation.host) |host| try self.allocator.dupe(u8, host) else null,
            .status = if (model.isHostConfigured(allocation.host)) .active else .rejecting_no_host,
            .error_kind = null,
            .last_error = null,
            .fd = null,
            .mutex = .{},
            .udp_sessions = std.AutoHashMap(ClientKey, *UdpSession).init(self.allocator),
            .udp_cached_key = null,
            .udp_cached_session = null,
            .udp_ttl_ms = 60_000,
            .udp_max_sessions = 4096,
            .updated_at_ms = allocation.updated_at_ms,
        };
        return entry;
    }

    fn addToRegistry(self: *RuntimeManager, entry: *ListenerEntry) !void {
        self.registry_mutex.lock();
        defer self.registry_mutex.unlock();
        try self.listeners.put(entry.id, entry);
        if (entry.fd) |fd| try self.listener_fds.put(fd, entry);
    }

    fn bindEntry(self: *RuntimeManager, entry: *ListenerEntry) !posix.fd_t {
        return switch (entry.protocol) {
            .tcp => try bindTcpListener(entry.port, self.epoll_fd),
            .udp => try bindUdpSocket(entry.port, self.epoll_fd, self.udp_socket_recv_buffer_bytes, self.udp_socket_send_buffer_bytes),
        };
    }

    fn closeEntryFd(self: *RuntimeManager, entry: *ListenerEntry) void {
        if (entry.fd) |fd| {
            _ = self.listener_fds.remove(fd);
            closeIgnoreBadFd(fd);
            entry.fd = null;
        }
    }

    fn handleTcpRuntimeSessionFd(self: *RuntimeManager, session: *TcpRuntimeSession, fd: posix.fd_t) void {
        self.metrics.tcp_session_event_total.inc();
        if (fd == session.upstream_fd and !session.upstream_connected) {
            finishTcpRuntimeConnect(session) catch {
                self.removeTcpRuntimeSession(session);
                return;
            };
        }
        driveTcpRuntimeSession(session);
        if (shouldCloseTcpRuntimeSession(session)) {
            self.removeTcpRuntimeSession(session);
            return;
        }
        self.updateTcpRuntimeSessionInterest(session);
    }

    fn updateTcpRuntimeSessionInterest(self: *RuntimeManager, session: *TcpRuntimeSession) void {
        var client_events: u32 = std.os.linux.EPOLL.ERR | std.os.linux.EPOLL.RDHUP | std.os.linux.EPOLL.HUP;
        if (!session.client_read_closed and session.client_to_upstream.writableLen() > 0) {
            client_events |= std.os.linux.EPOLL.IN;
        }
        if (session.upstream_to_client.readableLen() > 0) {
            client_events |= std.os.linux.EPOLL.OUT;
        }

        var upstream_events: u32 = std.os.linux.EPOLL.ERR | std.os.linux.EPOLL.RDHUP | std.os.linux.EPOLL.HUP;
        if (!session.upstream_connected) {
            upstream_events |= std.os.linux.EPOLL.OUT;
        } else {
            if (!session.upstream_read_closed and session.upstream_to_client.writableLen() > 0) {
                upstream_events |= std.os.linux.EPOLL.IN;
            }
            if (session.client_to_upstream.readableLen() > 0) {
                upstream_events |= std.os.linux.EPOLL.OUT;
            }
        }

        var client_event = std.os.linux.epoll_event{ .events = client_events, .data = .{ .fd = session.client_fd } };
        posix.epoll_ctl(self.epoll_fd, std.os.linux.EPOLL.CTL_MOD, session.client_fd, &client_event) catch {};
        var upstream_event = std.os.linux.epoll_event{ .events = upstream_events, .data = .{ .fd = session.upstream_fd } };
        posix.epoll_ctl(self.epoll_fd, std.os.linux.EPOLL.CTL_MOD, session.upstream_fd, &upstream_event) catch {};
    }

    fn removeTcpRuntimeSession(self: *RuntimeManager, session: *TcpRuntimeSession) void {
        _ = self.tcp_runtime_session_fds.remove(session.client_fd);
        _ = self.tcp_runtime_session_fds.remove(session.upstream_fd);
        posix.epoll_ctl(self.epoll_fd, std.os.linux.EPOLL.CTL_DEL, session.client_fd, null) catch {};
        posix.epoll_ctl(self.epoll_fd, std.os.linux.EPOLL.CTL_DEL, session.upstream_fd, null) catch {};
        closeIgnoreBadFd(session.client_fd);
        closeIgnoreBadFd(session.upstream_fd);

        var idx: usize = 0;
        while (idx < self.tcp_runtime_sessions.items.len) : (idx += 1) {
            if (self.tcp_runtime_sessions.items[idx] == session) {
                _ = self.tcp_runtime_sessions.swapRemove(idx);
                break;
            }
        }

        self.metrics.tcp_active_sessions.dec();
        self.metrics.tcp_session_close_total.inc();
        self.allocator.destroy(session);
    }

    fn shutdownTcpRuntimeSessions(self: *RuntimeManager) void {
        while (self.tcp_runtime_sessions.items.len > 0) {
            const session = self.tcp_runtime_sessions.pop() orelse break;
            _ = self.tcp_runtime_session_fds.remove(session.client_fd);
            _ = self.tcp_runtime_session_fds.remove(session.upstream_fd);
            closeIgnoreBadFd(session.client_fd);
            closeIgnoreBadFd(session.upstream_fd);
            self.metrics.tcp_active_sessions.dec();
            self.metrics.tcp_session_close_total.inc();
            self.allocator.destroy(session);
        }
    }
};

const ListenerEntry = struct {
    allocator: std.mem.Allocator,
    manager: *RuntimeManager,
    id: []u8,
    protocol: model.Protocol,
    port: u16,
    desired_target_port: u16,
    desired_host: ?[]u8,
    effective_target_port: ?u16,
    effective_host: ?[]u8,
    status: model.RuntimeStatus,
    error_kind: ?model.ErrorKind,
    last_error: ?[]u8,
    fd: ?posix.fd_t,
    mutex: std.Thread.Mutex,
    udp_sessions: std.AutoHashMap(ClientKey, *UdpSession),
    udp_cached_key: ?ClientKey,
    udp_cached_session: ?*UdpSession,
    udp_ttl_ms: i64,
    udp_max_sessions: usize,
    updated_at_ms: i64,

    fn observed(self: *const ListenerEntry, allocator: std.mem.Allocator) !ObservedState {
        return .{
            .runtime_status = self.status,
            .effective_host = if (self.effective_host) |host| try allocator.dupe(u8, host) else null,
            .effective_target_port = self.effective_target_port,
            .error_kind = self.error_kind,
            .last_error = if (self.last_error) |msg| try allocator.dupe(u8, msg) else null,
        };
    }
};

const ClientKey = struct {
    family: u16,
    port: u16,
    addr: [16]u8,
};

const UdpReplyDispatch = struct {
    entry: *ListenerEntry,
    key: ClientKey,
};

const UdpSession = struct {
    key: ClientKey,
    client_addr: net.Address,
    client_addr_len: posix.socklen_t,
    upstream_fd: posix.fd_t,
    last_seen_ms: std.atomic.Value(i64),
};

fn destroyEntry(allocator: std.mem.Allocator, entry: *ListenerEntry) void {
    allocator.free(entry.id);
    if (entry.desired_host) |host| allocator.free(host);
    if (entry.effective_host) |host| allocator.free(host);
    if (entry.last_error) |msg| allocator.free(msg);
    closeUdpSessions(allocator, entry);
    entry.udp_sessions.deinit();
    allocator.destroy(entry);
}

fn closeUdpSessions(allocator: std.mem.Allocator, entry: *ListenerEntry) void {
    _ = allocator;
    while (entry.udp_sessions.count() > 0) {
        var it = entry.udp_sessions.iterator();
        const kv = it.next() orelse break;
        const session = kv.value_ptr.*;
        removeUdpSessionLocked(entry, session.key, session, true);
    }
    entry.udp_sessions.clearRetainingCapacity();
    entry.udp_cached_key = null;
    entry.udp_cached_session = null;
}

fn dupErr(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    return allocator.dupe(u8, text);
}

fn setLastError(allocator: std.mem.Allocator, entry: *ListenerEntry, message: []const u8) !void {
    if (entry.last_error) |old| allocator.free(old);
    entry.last_error = try allocator.dupe(u8, message);
}

fn clearLastError(allocator: std.mem.Allocator, entry: *ListenerEntry) void {
    if (entry.last_error) |old| allocator.free(old);
    entry.last_error = null;
}

fn replaceOptionalString(allocator: std.mem.Allocator, target: *?[]u8, value: ?[]const u8) !void {
    if (target.*) |old| allocator.free(old);
    target.* = if (value) |text| try allocator.dupe(u8, text) else null;
}

fn bindTcpListener(port: u16, epoll_fd: posix.fd_t) !posix.fd_t {
    const fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    errdefer posix.close(fd);
    var one: c_int = 1;
    try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&one));
    const addr = try net.Address.parseIp4("0.0.0.0", port);
    try posix.bind(fd, &addr.any, addr.getOsSockLen());
    try posix.listen(fd, 128);
    var event = std.os.linux.epoll_event{ .events = std.os.linux.EPOLL.IN, .data = .{ .fd = fd } };
    try posix.epoll_ctl(epoll_fd, std.os.linux.EPOLL.CTL_ADD, fd, &event);
    return fd;
}

fn bindUdpSocket(port: u16, epoll_fd: posix.fd_t, recv_buf_bytes: u32, send_buf_bytes: u32) !posix.fd_t {
    const fd = try posix.socket(posix.AF.INET, posix.SOCK.DGRAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, posix.IPPROTO.UDP);
    errdefer posix.close(fd);
    var one: c_int = 1;
    try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&one));
    if (recv_buf_bytes > 0) {
        var recv_value: c_int = @intCast(recv_buf_bytes);
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, std.mem.asBytes(&recv_value));
    }
    if (send_buf_bytes > 0) {
        var send_value: c_int = @intCast(send_buf_bytes);
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, std.mem.asBytes(&send_value));
    }
    const addr = try net.Address.parseIp4("0.0.0.0", port);
    try posix.bind(fd, &addr.any, addr.getOsSockLen());
    var event = std.os.linux.epoll_event{ .events = std.os.linux.EPOLL.IN, .data = .{ .fd = fd } };
    try posix.epoll_ctl(epoll_fd, std.os.linux.EPOLL.CTL_ADD, fd, &event);
    return fd;
}

const tcp_runtime_buffer_size = 1024 * 1024;

const TcpRuntimeBuffer = struct {
    data: [tcp_runtime_buffer_size]u8 = undefined,
    start: usize = 0,
    end: usize = 0,

    fn readableLen(self: *const TcpRuntimeBuffer) usize {
        return self.end - self.start;
    }

    fn writableLen(self: *TcpRuntimeBuffer) usize {
        if (self.end == self.data.len and self.start > 0) self.compact();
        return self.data.len - self.end;
    }

    fn writableSlice(self: *TcpRuntimeBuffer) []u8 {
        _ = self.writableLen();
        return self.data[self.end..];
    }

    fn readableSlice(self: *const TcpRuntimeBuffer) []const u8 {
        return self.data[self.start..self.end];
    }

    fn appendWritten(self: *TcpRuntimeBuffer, amount: usize) void {
        self.end += amount;
    }

    fn consume(self: *TcpRuntimeBuffer, amount: usize) void {
        self.start += amount;
        if (self.start >= self.end) {
            self.start = 0;
            self.end = 0;
        }
    }

    fn clear(self: *TcpRuntimeBuffer) void {
        self.start = 0;
        self.end = 0;
    }

    fn compact(self: *TcpRuntimeBuffer) void {
        if (self.start == 0) return;
        const len = self.readableLen();
        if (len > 0) {
            std.mem.copyForwards(u8, self.data[0..len], self.data[self.start..self.end]);
        }
        self.start = 0;
        self.end = len;
    }
};

const TcpRuntimeSession = struct {
    client_fd: posix.fd_t,
    upstream_fd: posix.fd_t,
    upstream_connected: bool,
    client_read_closed: bool,
    upstream_read_closed: bool,
    client_shutdown_sent: bool,
    upstream_shutdown_sent: bool,
    client_to_upstream: TcpRuntimeBuffer = .{},
    upstream_to_client: TcpRuntimeBuffer = .{},
};

const TcpSpliceFallbackReason = enum {
    forced,
    unsupported,
    runtime_error,
};

const TcpSessionMode = enum {
    copy,
    splice,
};

const TcpSpliceOutcome = union(enum) {
    success,
    fallback: TcpSpliceFallbackReason,
    hard_failure,
};

const SplicePumpResult = enum {
    success,
    unsupported,
    runtime_error,
};

fn handleTcpAccept(manager: *RuntimeManager, entry: *ListenerEntry) void {
    const listen_fd = entry.fd orelse return;
    const use_runtime_session_model = manager.tcp_session_model_enabled and !manager.tcp_splice_enabled;
    while (true) {
        var addr: net.Address = undefined;
        var len: posix.socklen_t = @sizeOf(net.Address);
        const accept_flags: u32 = if (use_runtime_session_model)
            posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK
        else
            posix.SOCK.CLOEXEC;
        const conn_fd = posix.accept(listen_fd, &addr.any, &len, accept_flags) catch |err| switch (err) {
            error.WouldBlock => break,
            else => break,
        };
        if (entry.status != .active or entry.effective_host == null or entry.effective_target_port == null) {
            manager.metrics.rejected_no_host_total.inc();
            posix.close(conn_fd);
            continue;
        }

        if (use_runtime_session_model) {
            startTcpRuntimeSession(manager, entry, conn_fd) catch {
                closeIgnoreBadFd(conn_fd);
            };
            continue;
        }

        const allocator = manager.allocator;
        const ctx = allocator.create(TcpSessionCtx) catch {
            posix.close(conn_fd);
            continue;
        };
        ctx.* = .{
            .allocator = allocator,
            .manager = null,
            .client_fd = conn_fd,
            .host = allocator.dupe(u8, entry.effective_host.?) catch {
                posix.close(conn_fd);
                allocator.destroy(ctx);
                continue;
            },
            .port = entry.effective_target_port.?,
            .session_mode = if (manager.tcp_splice_enabled and !manager.force_tcp_copy_fallback) .splice else .copy,
            .force_copy = manager.force_tcp_copy_fallback,
            .splice_test_mode = manager.tcp_splice_test_mode,
            .metrics = manager.metrics,
            .upstream_fd = null,
            .thread = null,
            .mutex = .{},
            .finished = false,
        };
        manager.tcp_sessions_mutex.lock();
        manager.tcp_sessions.append(manager.allocator, ctx) catch {
            manager.tcp_sessions_mutex.unlock();
            allocator.free(ctx.host);
            allocator.destroy(ctx);
            posix.close(conn_fd);
            continue;
        };
        manager.tcp_sessions_mutex.unlock();
        const thread = std.Thread.spawn(.{}, tcpSessionThread, .{ctx}) catch {
            manager.tcp_sessions_mutex.lock();
            var idx: usize = 0;
            while (idx < manager.tcp_sessions.items.len) : (idx += 1) {
                if (manager.tcp_sessions.items[idx] == ctx) {
                    _ = manager.tcp_sessions.swapRemove(idx);
                    break;
                }
            }
            manager.tcp_sessions_mutex.unlock();
            allocator.free(ctx.host);
            allocator.destroy(ctx);
            posix.close(conn_fd);
            continue;
        };
        ctx.thread = thread;
        ctx.manager = manager;
    }
}

fn startTcpRuntimeSession(manager: *RuntimeManager, entry: *ListenerEntry, client_fd: posix.fd_t) !void {
    const host = entry.effective_host.?;
    const port = entry.effective_target_port.?;
    const addr = try config_mod.parseIpLiteral(host, port);
    const family: u32 = switch (addr.any.family) {
        std.posix.AF.INET => std.posix.AF.INET,
        std.posix.AF.INET6 => std.posix.AF.INET6,
        else => return error.AddressFamilyNotSupported,
    };
    const upstream_fd = try posix.socket(family, posix.SOCK.STREAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    errdefer closeIgnoreBadFd(upstream_fd);

    const upstream_connected = blk: {
        posix.connect(upstream_fd, &addr.any, addr.getOsSockLen()) catch |err| switch (err) {
            error.WouldBlock, error.ConnectionPending => break :blk false,
            else => return err,
        };
        break :blk true;
    };

    const session = try manager.allocator.create(TcpRuntimeSession);
    errdefer manager.allocator.destroy(session);
    session.* = .{
        .client_fd = client_fd,
        .upstream_fd = upstream_fd,
        .upstream_connected = upstream_connected,
        .client_read_closed = false,
        .upstream_read_closed = false,
        .client_shutdown_sent = false,
        .upstream_shutdown_sent = false,
    };

    try manager.tcp_runtime_sessions.append(manager.allocator, session);
    errdefer _ = manager.tcp_runtime_sessions.pop();
    try manager.tcp_runtime_session_fds.put(client_fd, session);
    errdefer _ = manager.tcp_runtime_session_fds.remove(client_fd);
    try manager.tcp_runtime_session_fds.put(upstream_fd, session);
    errdefer _ = manager.tcp_runtime_session_fds.remove(upstream_fd);

    var client_event = std.os.linux.epoll_event{ .events = 0, .data = .{ .fd = client_fd } };
    try posix.epoll_ctl(manager.epoll_fd, std.os.linux.EPOLL.CTL_ADD, client_fd, &client_event);
    errdefer posix.epoll_ctl(manager.epoll_fd, std.os.linux.EPOLL.CTL_DEL, client_fd, null) catch {};
    var upstream_event = std.os.linux.epoll_event{ .events = 0, .data = .{ .fd = upstream_fd } };
    try posix.epoll_ctl(manager.epoll_fd, std.os.linux.EPOLL.CTL_ADD, upstream_fd, &upstream_event);

    manager.metrics.tcp_session_create_total.inc();
    manager.metrics.tcp_active_sessions.inc();
    manager.updateTcpRuntimeSessionInterest(session);
}

fn finishTcpRuntimeConnect(session: *TcpRuntimeSession) !void {
    try posix.getsockoptError(session.upstream_fd);
    session.upstream_connected = true;
}

fn driveTcpRuntimeSession(session: *TcpRuntimeSession) void {
    if (!session.client_read_closed and session.client_to_upstream.writableLen() > 0) {
        fillTcpRuntimeBuffer(session.client_fd, &session.client_to_upstream, &session.client_read_closed);
    }
    if (!session.upstream_connected) return;
    if (!session.upstream_read_closed and session.upstream_to_client.writableLen() > 0) {
        fillTcpRuntimeBuffer(session.upstream_fd, &session.upstream_to_client, &session.upstream_read_closed);
    }

    flushTcpRuntimeBuffer(session.upstream_fd, &session.client_to_upstream, &session.upstream_read_closed);
    flushTcpRuntimeBuffer(session.client_fd, &session.upstream_to_client, &session.client_read_closed);

    if (session.client_read_closed and session.client_to_upstream.readableLen() == 0 and !session.upstream_shutdown_sent) {
        shutdownIgnore(session.upstream_fd, .send);
        session.upstream_shutdown_sent = true;
    }
    if (session.upstream_read_closed and session.upstream_to_client.readableLen() == 0 and !session.client_shutdown_sent) {
        shutdownIgnore(session.client_fd, .send);
        session.client_shutdown_sent = true;
    }
}

fn shouldCloseTcpRuntimeSession(session: *const TcpRuntimeSession) bool {
    return session.client_read_closed and session.upstream_read_closed and
        session.client_to_upstream.readableLen() == 0 and
        session.upstream_to_client.readableLen() == 0;
}

fn fillTcpRuntimeBuffer(fd: posix.fd_t, buffer: *TcpRuntimeBuffer, read_closed: *bool) void {
    while (!read_closed.* and buffer.writableLen() > 0) {
        const amt = posix.read(fd, buffer.writableSlice()) catch |err| switch (err) {
            error.WouldBlock => return,
            error.ConnectionResetByPeer, error.BrokenPipe, error.NotOpenForReading => {
                read_closed.* = true;
                return;
            },
            else => {
                read_closed.* = true;
                return;
            },
        };
        if (amt == 0) {
            read_closed.* = true;
            return;
        }
        buffer.appendWritten(amt);
        if (amt == 0) return;
    }
}

fn flushTcpRuntimeBuffer(fd: posix.fd_t, buffer: *TcpRuntimeBuffer, peer_read_closed: *bool) void {
    while (buffer.readableLen() > 0) {
        const written = posix.write(fd, buffer.readableSlice()) catch |err| switch (err) {
            error.WouldBlock => return,
            error.ConnectionResetByPeer, error.BrokenPipe, error.NotOpenForWriting => {
                peer_read_closed.* = true;
                buffer.clear();
                return;
            },
            else => {
                peer_read_closed.* = true;
                buffer.clear();
                return;
            },
        };
        if (written == 0) return;
        buffer.consume(written);
    }
}

const TcpSessionCtx = struct {
    allocator: std.mem.Allocator,
    manager: ?*RuntimeManager,
    client_fd: posix.fd_t,
    host: []u8,
    port: u16,
    session_mode: TcpSessionMode,
    force_copy: bool,
    splice_test_mode: TcpSpliceTestMode,
    metrics: *Metrics,
    upstream_fd: ?posix.fd_t,
    thread: ?std.Thread,
    mutex: std.Thread.Mutex,
    finished: bool,
};

fn tcpSessionThread(ctx: *TcpSessionCtx) void {
    defer {
        closeIgnoreBadFd(ctx.client_fd);
        if (ctx.upstream_fd) |fd| closeIgnoreBadFd(fd);
        ctx.mutex.lock();
        ctx.finished = true;
        ctx.mutex.unlock();
    }

    const addr = config_mod.parseIpLiteral(ctx.host, ctx.port) catch return;
    const upstream = net.tcpConnectToAddress(addr) catch return;
    const upstream_fd = upstream.handle;
    ctx.upstream_fd = upstream_fd;
    if (ctx.force_copy) {
        recordTcpFallback(ctx.metrics, .forced);
        runTcpCopy(ctx.client_fd, upstream_fd);
        return;
    }
    if (ctx.session_mode == .copy) {
        ctx.metrics.tcp_copy_fallback_total.inc();
        runTcpCopy(ctx.client_fd, upstream_fd);
        return;
    }

    ctx.metrics.tcp_splice_attempt_total.inc();
    switch (runTcpSplice(ctx.client_fd, upstream_fd, ctx.splice_test_mode)) {
        .success => {
            ctx.metrics.tcp_splice_success_total.inc();
            ctx.metrics.tcp_splice_fast_path_total.inc();
        },
        .fallback => |reason| {
            recordTcpFallback(ctx.metrics, reason);
            runTcpCopy(ctx.client_fd, upstream_fd);
        },
        .hard_failure => {
            ctx.metrics.tcp_splice_hard_failure_total.inc();
        },
    }
}

fn recordTcpFallback(metrics: *Metrics, reason: TcpSpliceFallbackReason) void {
    metrics.tcp_copy_fallback_total.inc();
    metrics.tcp_splice_fallback_total.inc();
    switch (reason) {
        .forced => metrics.tcp_splice_fallback_forced_total.inc(),
        .unsupported => metrics.tcp_splice_fallback_unsupported_total.inc(),
        .runtime_error => metrics.tcp_splice_fallback_runtime_error_total.inc(),
    }
}

fn runTcpCopy(client_fd: posix.fd_t, upstream_fd: posix.fd_t) void {
    const CopyCtx = struct {
        src: posix.fd_t,
        dst: posix.fd_t,
        how: posix.ShutdownHow,
    };
    var reverse = CopyCtx{ .src = upstream_fd, .dst = client_fd, .how = .send };
    const thread = std.Thread.spawn(.{}, copyPumpThread, .{&reverse}) catch return;
    var forward = CopyCtx{ .src = client_fd, .dst = upstream_fd, .how = .send };
    copyPumpThread(&forward);
    thread.join();
}

fn copyPumpThread(ctx: anytype) void {
    var buffer: [16 * 1024]u8 = undefined;
    while (true) {
        const amt = posix.read(ctx.src, &buffer) catch break;
        if (amt == 0) break;
        var offset: usize = 0;
        while (offset < amt) {
            const written = posix.write(ctx.dst, buffer[offset..amt]) catch break;
            offset += written;
        }
    }
    shutdownIgnore(ctx.dst, ctx.how);
}

extern "c" fn splice(fd_in: c_int, off_in: ?*i64, fd_out: c_int, off_out: ?*i64, len: usize, flags: c_uint) isize;

fn runTcpSplice(client_fd: posix.fd_t, upstream_fd: posix.fd_t, test_mode: TcpSpliceTestMode) TcpSpliceOutcome {
    if (test_mode == .recoverable_setup_failure) return .{ .fallback = .unsupported };
    if (test_mode == .hard_failure) return .hard_failure;

    const forward_pipe = posix.pipe2(.{ .CLOEXEC = true }) catch return .{ .fallback = .unsupported };
    defer {
        closeIgnoreBadFd(forward_pipe[0]);
        closeIgnoreBadFd(forward_pipe[1]);
    }
    const reverse_pipe = posix.pipe2(.{ .CLOEXEC = true }) catch return .{ .fallback = .unsupported };
    defer {
        closeIgnoreBadFd(reverse_pipe[0]);
        closeIgnoreBadFd(reverse_pipe[1]);
    }

    if (test_mode == .recoverable_runtime_failure) return .{ .fallback = .runtime_error };

    const SpliceCtx = struct {
        src: posix.fd_t,
        dst: posix.fd_t,
        pipe_r: posix.fd_t,
        pipe_w: posix.fd_t,
        result: SplicePumpResult = .success,
    };
    var reverse = SpliceCtx{ .src = upstream_fd, .dst = client_fd, .pipe_r = reverse_pipe[0], .pipe_w = reverse_pipe[1] };
    const thread = std.Thread.spawn(.{}, splicePumpThread, .{&reverse}) catch return .{ .fallback = .unsupported };
    var forward = SpliceCtx{ .src = client_fd, .dst = upstream_fd, .pipe_r = forward_pipe[0], .pipe_w = forward_pipe[1] };
    splicePumpThread(&forward);
    thread.join();

    return mergeSplicePumpResults(forward.result, reverse.result);
}

fn splicePumpThread(ctx: anytype) void {
    ctx.result = splicePump(ctx.src, ctx.dst, ctx.pipe_r, ctx.pipe_w);
}

fn splicePump(src: posix.fd_t, dst: posix.fd_t, pipe_r: posix.fd_t, pipe_w: posix.fd_t) SplicePumpResult {
    while (true) {
        const moved = splice(src, null, pipe_w, null, 64 * 1024, 0);
        if (moved == 0) break;
        if (moved < 0) return classifySpliceErrno();
        var remaining: usize = @intCast(moved);
        while (remaining > 0) {
            const sent = splice(pipe_r, null, dst, null, remaining, 0);
            if (sent == 0) return .runtime_error;
            if (sent < 0) return classifySpliceErrno();
            remaining -= @intCast(sent);
        }
        if (remaining > 0) return .runtime_error;
    }
    shutdownIgnore(dst, .send);
    return .success;
}

fn mergeSplicePumpResults(a: SplicePumpResult, b: SplicePumpResult) TcpSpliceOutcome {
    if (a == .runtime_error or b == .runtime_error) return .{ .fallback = .runtime_error };
    if (a == .unsupported or b == .unsupported) return .{ .fallback = .unsupported };
    return .success;
}

fn classifySpliceErrno() SplicePumpResult {
    const err: posix.E = @enumFromInt(std.c._errno().*);
    return switch (err) {
        .INVAL, .NOSYS, .OPNOTSUPP, .XDEV => .unsupported,
        .PIPE, .CONNRESET, .NOTCONN => .success,
        else => .runtime_error,
    };
}

fn handleUdpReadable(allocator: std.mem.Allocator, metrics: *Metrics, entry: *ListenerEntry) void {
    const fd = entry.fd orelse return;
    const batch_len = 16;
    var buffers: [batch_len][64 * 1024]u8 = undefined;
    var addrs: [batch_len]net.Address = undefined;
    var lens: [batch_len]posix.socklen_t = undefined;
    var iovecs: [batch_len]posix.iovec = undefined;
    var msgvec: [batch_len]std.os.linux.mmsghdr = undefined;
    var lengths: [batch_len]usize = [_]usize{0} ** batch_len;
    var sessions: [batch_len]?*UdpSession = [_]?*UdpSession{null} ** batch_len;
    while (true) {
        for (&iovecs, &msgvec, &lens, 0..) |*iov, *msg, *len, idx| {
            len.* = @sizeOf(net.Address);
            iov.* = .{ .base = &buffers[idx], .len = buffers[idx].len };
            msg.* = .{
                .hdr = .{
                    .name = &addrs[idx].any,
                    .namelen = @sizeOf(net.Address),
                    .iov = @ptrCast(iov),
                    .iovlen = 1,
                    .control = null,
                    .controllen = 0,
                    .flags = 0,
                },
                .len = 0,
            };
        }

        const recv_raw = std.os.linux.recvmmsg(fd, &msgvec, batch_len, 0, null);
        const recv_signed: isize = @bitCast(recv_raw);
        if (recv_signed < 0) switch (posix.errno(recv_raw)) {
            .AGAIN => break,
            else => {
                metrics.udp_recv_errors_total.inc();
                break;
            },
        };
        if (recv_signed == 0) break;
        const batch_count: usize = @intCast(@min(recv_signed, batch_len));
        metrics.udp_batch_calls_total.inc();
        metrics.udp_batch_messages_total.add(batch_count);

        for (0..batch_count) |idx| {
            const amt = msgvec[idx].len;
            metrics.udp_packets_in_total.inc();
            metrics.udp_bytes_in_total.add(amt);
            lengths[idx] = amt;

            if (entry.status != .active or entry.effective_host == null or entry.effective_target_port == null) {
                metrics.rejected_no_host_total.inc();
                sessions[idx] = null;
                continue;
            }

            const key = makeClientKey(addrs[idx], msgvec[idx].hdr.namelen);
            const session = getOrCreateUdpSession(allocator, metrics, entry, key, addrs[idx], msgvec[idx].hdr.namelen) orelse {
                metrics.udp_drop_total.inc();
                sessions[idx] = null;
                continue;
            };
            session.last_seen_ms.store(std.time.milliTimestamp(), .monotonic);
            sessions[idx] = session;
        }

        if (batch_count == 0) break;

        var batched_session: ?*UdpSession = null;
        var batched_count: usize = 0;
        var batched_lengths: [batch_len]usize = [_]usize{0} ** batch_len;
        for (0..batch_count) |idx| {
            const session = sessions[idx] orelse continue;
            if (batched_session != null and batched_session.? != session) {
                flushUdpConnectedBatch(metrics, batched_session.?, &buffers, &batched_lengths, batched_count);
                batched_session = null;
                batched_count = 0;
            }
            batched_session = session;
            batched_lengths[batched_count] = lengths[idx];
            if (batched_count != idx) {
                @memcpy(buffers[batched_count][0..lengths[idx]], buffers[idx][0..lengths[idx]]);
            }
            batched_count += 1;
        }
        if (batched_session) |session| {
            flushUdpConnectedBatch(metrics, session, &buffers, &batched_lengths, batched_count);
        }
    }
}

fn getOrCreateUdpSession(allocator: std.mem.Allocator, metrics: *Metrics, entry: *ListenerEntry, key: ClientKey, addr: net.Address, addr_len: posix.socklen_t) ?*UdpSession {
    _ = addr_len;
    if (entry.udp_cached_key) |cached_key| {
        if (std.meta.eql(cached_key, key)) {
            if (entry.udp_cached_session) |cached_session| {
                return cached_session;
            }
        }
    }
    if (entry.udp_sessions.getPtr(key)) |existing| {
        entry.udp_cached_key = key;
        entry.udp_cached_session = existing.*;
        return existing.*;
    }
    if (entry.udp_sessions.count() >= entry.udp_max_sessions) return null;

    const upstream_addr = config_mod.parseIpLiteral(entry.effective_host.?, entry.effective_target_port.?) catch {
        metrics.udp_send_errors_total.inc();
        return null;
    };
    const upstream_fd = posix.socket(posix.AF.INET, posix.SOCK.DGRAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, posix.IPPROTO.UDP) catch {
        metrics.udp_send_errors_total.inc();
        return null;
    };
    if (entry.manager.udp_socket_recv_buffer_bytes > 0) {
        var recv_value: c_int = @intCast(entry.manager.udp_socket_recv_buffer_bytes);
        posix.setsockopt(upstream_fd, posix.SOL.SOCKET, posix.SO.RCVBUF, std.mem.asBytes(&recv_value)) catch {};
    }
    if (entry.manager.udp_socket_send_buffer_bytes > 0) {
        var send_value: c_int = @intCast(entry.manager.udp_socket_send_buffer_bytes);
        posix.setsockopt(upstream_fd, posix.SOL.SOCKET, posix.SO.SNDBUF, std.mem.asBytes(&send_value)) catch {};
    }
    posix.connect(upstream_fd, &upstream_addr.any, upstream_addr.getOsSockLen()) catch {
        posix.close(upstream_fd);
        metrics.udp_send_errors_total.inc();
        return null;
    };

    const session = allocator.create(UdpSession) catch {
        posix.close(upstream_fd);
        metrics.udp_drop_total.inc();
        return null;
    };
    errdefer allocator.destroy(session);
    session.* = .{
        .key = key,
        .client_addr = addr,
        .client_addr_len = addr.getOsSockLen(),
        .upstream_fd = upstream_fd,
        .last_seen_ms = std.atomic.Value(i64).init(std.time.milliTimestamp()),
    };
    entry.udp_sessions.put(key, session) catch {
        posix.close(upstream_fd);
        allocator.destroy(session);
        metrics.udp_drop_total.inc();
        return null;
    };
    metrics.udp_session_create_total.inc();
    metrics.udp_active_sessions.inc();
    var event = std.os.linux.epoll_event{ .events = std.os.linux.EPOLL.IN, .data = .{ .fd = upstream_fd } };
    posix.epoll_ctl(entry.manager.epoll_fd, std.os.linux.EPOLL.CTL_ADD, upstream_fd, &event) catch {
        _ = entry.udp_sessions.remove(key);
        posix.close(upstream_fd);
        allocator.destroy(session);
        metrics.udp_active_sessions.dec();
        return null;
    };
    entry.manager.udp_reply_fds.put(upstream_fd, .{ .entry = entry, .key = key }) catch {
        posix.epoll_ctl(entry.manager.epoll_fd, std.os.linux.EPOLL.CTL_DEL, upstream_fd, &event) catch {};
        _ = entry.udp_sessions.remove(key);
        posix.close(upstream_fd);
        allocator.destroy(session);
        metrics.udp_active_sessions.dec();
        return null;
    };
    entry.udp_cached_key = key;
    entry.udp_cached_session = session;
    return session;
}

fn flushUdpConnectedBatch(metrics: *Metrics, session: *UdpSession, buffers: *const [16][64 * 1024]u8, lengths: *const [16]usize, count: usize) void {
    if (count == 0) return;
    if (count == 1) {
        if (!sendConnectedUdp(session.upstream_fd, buffers[0][0..lengths[0]])) {
            metrics.udp_send_errors_total.inc();
            metrics.udp_drop_total.inc();
        }
        return;
    }

    var iovecs: [16]posix.iovec_const = undefined;
    var msgvec: [16]std.os.linux.mmsghdr_const = undefined;
    for (0..count) |idx| {
        iovecs[idx] = .{ .base = &buffers[idx], .len = lengths[idx] };
        msgvec[idx] = .{
            .hdr = .{
                .name = null,
                .namelen = 0,
                .iov = @ptrCast(&iovecs[idx]),
                .iovlen = 1,
                .control = null,
                .controllen = 0,
                .flags = 0,
            },
            .len = 0,
        };
    }

    const send_raw = std.os.linux.sendmmsg(session.upstream_fd, &msgvec, @intCast(count), 0);
    const send_signed: isize = @bitCast(send_raw);
    if (send_signed >= 0) {
        const sent_count: usize = @intCast(send_signed);
        metrics.udp_batch_calls_total.inc();
        metrics.udp_batch_messages_total.add(sent_count);
        if (sent_count >= count) return;

        var idx = sent_count;
        while (idx < count) : (idx += 1) {
            if (!sendConnectedUdp(session.upstream_fd, buffers[idx][0..lengths[idx]])) {
                metrics.udp_send_errors_total.inc();
                metrics.udp_drop_total.inc();
            }
        }
        return;
    }

    var idx: usize = 0;
    while (idx < count) : (idx += 1) {
        if (!sendConnectedUdp(session.upstream_fd, buffers[idx][0..lengths[idx]])) {
            metrics.udp_send_errors_total.inc();
            metrics.udp_drop_total.inc();
        }
    }
}

fn handleUdpReplyReadable(metrics: *Metrics, entry: *ListenerEntry, key: ClientKey) void {
    const session = entry.udp_sessions.get(key) orelse {
        metrics.udp_reply_stale_total.inc();
        return;
    };

    var buffer: [64 * 1024]u8 = undefined;
    while (true) {
        const amt = posix.recv(session.upstream_fd, &buffer, 0) catch |err| switch (err) {
            error.WouldBlock => break,
            else => {
                metrics.udp_recv_errors_total.inc();
                metrics.udp_reply_drop_total.inc();
                removeUdpSessionLocked(entry, key, session, false);
                break;
            },
        };
        if (amt == 0) {
            removeUdpSessionLocked(entry, key, session, false);
            break;
        }
        _ = posix.sendto(entry.fd orelse break, buffer[0..amt], 0, &session.client_addr.any, session.client_addr_len) catch {
            metrics.udp_send_errors_total.inc();
            metrics.udp_reply_drop_total.inc();
            removeUdpSessionLocked(entry, key, session, false);
            break;
        };
        metrics.udp_reply_primary_total.inc();
        metrics.udp_packets_out_total.inc();
        metrics.udp_bytes_out_total.add(@intCast(amt));
        session.last_seen_ms.store(std.time.milliTimestamp(), .monotonic);
    }
}

fn removeUdpSessionLocked(entry: *ListenerEntry, key: ClientKey, session: *UdpSession, count_as_reply_drop: bool) void {
    var event = std.os.linux.epoll_event{ .events = 0, .data = .{ .fd = session.upstream_fd } };
    posix.epoll_ctl(entry.manager.epoll_fd, std.os.linux.EPOLL.CTL_DEL, session.upstream_fd, &event) catch {};
    _ = entry.manager.udp_reply_fds.remove(session.upstream_fd);
    _ = entry.udp_sessions.remove(key);
    if (entry.udp_cached_key) |cached_key| {
        if (std.meta.eql(cached_key, key)) {
            entry.udp_cached_key = null;
            entry.udp_cached_session = null;
        }
    }
    shutdownIgnoreBadFd(session.upstream_fd);
    closeIgnoreBadFd(session.upstream_fd);
    if (count_as_reply_drop) entry.manager.metrics.udp_reply_drop_total.inc();
    entry.manager.metrics.udp_active_sessions.dec();
    entry.allocator.destroy(session);
}

fn makeClientKey(addr: net.Address, len: posix.socklen_t) ClientKey {
    _ = len;
    var key = ClientKey{
        .family = addr.any.family,
        .port = addr.getPort(),
        .addr = [_]u8{0} ** 16,
    };
    switch (addr.any.family) {
        posix.AF.INET => {
            const raw = std.mem.asBytes(&addr.in.sa.addr);
            @memcpy(key.addr[0..4], raw[0..4]);
        },
        posix.AF.INET6 => {
            @memcpy(key.addr[0..16], addr.in6.sa.addr[0..16]);
        },
        else => {
            const raw = std.mem.asBytes(&addr);
            const copy_len = @min(raw.len, key.addr.len);
            @memcpy(key.addr[0..copy_len], raw[0..copy_len]);
        },
    }
    return key;
}

fn sendConnectedUdp(fd: posix.fd_t, payload: []const u8) bool {
    const rc = std.os.linux.sendto(fd, payload.ptr, payload.len, 0, null, 0);
    return switch (posix.errno(rc)) {
        .SUCCESS => true,
        else => |err| blk: {
            std.debug.print("udp send failed fd={d} err={s}\n", .{ fd, @tagName(err) });
            break :blk false;
        },
    };
}

fn closeIgnoreBadFd(fd: posix.fd_t) void {
    switch (posix.errno(posix.system.close(fd))) {
        .SUCCESS, .BADF => {},
        else => {},
    }
}

fn shutdownIgnoreBadFd(fd: posix.fd_t) void {
    switch (posix.errno(posix.system.shutdown(fd, @intFromEnum(posix.ShutdownHow.both)))) {
        .SUCCESS, .BADF, .NOTCONN => {},
        else => {},
    }
}

fn shutdownIgnore(fd: posix.fd_t, how: posix.ShutdownHow) void {
    switch (posix.errno(posix.system.shutdown(fd, @intFromEnum(how)))) {
        .SUCCESS, .BADF, .NOTCONN => {},
        else => {},
    }
}
