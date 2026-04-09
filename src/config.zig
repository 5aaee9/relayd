const std = @import("std");

pub const HttpListen = struct {
    host: []u8,
    port: u16,
};

pub const PortRange = struct {
    start: u16,
    end: u16,

    pub fn contains(self: PortRange, port: u16) bool {
        return port >= self.start and port <= self.end;
    }
};

pub const Config = struct {
    http_listen_host: []u8,
    http_listen_port: u16,
    port_range: PortRange,
    auth_token: []u8,
    tcp_session_model_enabled: bool,
    tcp_splice_enabled: bool,
    force_tcp_copy_fallback: bool,
    udp_socket_recv_buffer_bytes: u32,
    udp_socket_send_buffer_bytes: u32,
    runtime_apply_timeout_ms: u32,
    restore_sweep_timeout_ms: u32,
    db_path: []u8,

    pub fn parseEnv(allocator: std.mem.Allocator) !Config {
        const raw_listen = try envOwned(allocator, "HTTP_LISTEN", ":8080");
        defer allocator.free(raw_listen);
        const listen = try parseHttpListen(allocator, raw_listen);
        const raw_range = try envOwned(allocator, "PORT_RANGE", "10000-30000");
        defer allocator.free(raw_range);
        const token = try std.process.getEnvVarOwned(allocator, "AUTH_TOKEN");
        if (token.len == 0) return error.EmptyAuthToken;
        return .{
            .http_listen_host = listen.host,
            .http_listen_port = listen.port,
            .port_range = try parsePortRange(raw_range),
            .auth_token = token,
            .tcp_session_model_enabled = envBool("TCP_SESSION_MODEL_ENABLED"),
            .tcp_splice_enabled = envBool("TCP_SPLICE_ENABLED"),
            .force_tcp_copy_fallback = envBool("FORCE_TCP_COPY_FALLBACK"),
            .udp_socket_recv_buffer_bytes = try envU32("UDP_SOCKET_RCVBUF_BYTES", 8 * 1024 * 1024),
            .udp_socket_send_buffer_bytes = try envU32("UDP_SOCKET_SNDBUF_BYTES", 8 * 1024 * 1024),
            .runtime_apply_timeout_ms = try envU32("RUNTIME_APPLY_TIMEOUT_MS", 2000),
            .restore_sweep_timeout_ms = try envU32("RESTORE_SWEEP_TIMEOUT_MS", 30000),
            .db_path = try envOwned(allocator, "SQLITE_PATH", "relayd.sqlite3"),
        };
    }

    pub fn deinit(self: *Config, allocator: std.mem.Allocator) void {
        allocator.free(self.http_listen_host);
        allocator.free(self.auth_token);
        allocator.free(self.db_path);
        self.* = undefined;
    }
};

fn envOwned(allocator: std.mem.Allocator, name: []const u8, default_value: []const u8) ![]u8 {
    return std.process.getEnvVarOwned(allocator, name) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => try allocator.dupe(u8, default_value),
        else => err,
    };
}

fn envBool(name: []const u8) bool {
    const value = std.process.getEnvVarOwned(std.heap.page_allocator, name) catch return false;
    defer std.heap.page_allocator.free(value);
    return std.ascii.eqlIgnoreCase(value, "1") or std.ascii.eqlIgnoreCase(value, "true") or std.ascii.eqlIgnoreCase(value, "yes") or std.ascii.eqlIgnoreCase(value, "on");
}

fn envU32(name: []const u8, default_value: u32) !u32 {
    const value = std.process.getEnvVarOwned(std.heap.page_allocator, name) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => return default_value,
        else => return err,
    };
    defer std.heap.page_allocator.free(value);
    return try std.fmt.parseInt(u32, value, 10);
}

pub fn parseHttpListen(allocator: std.mem.Allocator, raw: []const u8) !HttpListen {
    if (raw.len == 0) return error.InvalidHttpListen;
    if (raw[0] == ':') {
        return .{ .host = try allocator.dupe(u8, "127.0.0.1"), .port = try parsePort(raw[1..]) };
    }
    if (raw[0] == '[') {
        const end = std.mem.indexOfScalar(u8, raw, ']') orelse return error.InvalidHttpListen;
        if (end + 2 > raw.len or raw[end + 1] != ':') return error.InvalidHttpListen;
        const host = raw[1..end];
        _ = try parseIpLiteral(host, 0);
        return .{ .host = try allocator.dupe(u8, host), .port = try parsePort(raw[end + 2 ..]) };
    }
    const idx = std.mem.lastIndexOfScalar(u8, raw, ':') orelse return error.InvalidHttpListen;
    const host = raw[0..idx];
    if (host.len == 0) return error.InvalidHttpListen;
    _ = try parseIpLiteral(host, 0);
    return .{ .host = try allocator.dupe(u8, host), .port = try parsePort(raw[idx + 1 ..]) };
}

pub fn parsePortRange(raw: []const u8) !PortRange {
    const idx = std.mem.indexOfScalar(u8, raw, '-') orelse return error.InvalidPortRange;
    const start = try parsePort(raw[0..idx]);
    const end = try parsePort(raw[idx + 1 ..]);
    if (start > end) return error.InvalidPortRange;
    return .{ .start = start, .end = end };
}

pub fn parsePort(raw: []const u8) !u16 {
    if (raw.len == 0) return error.InvalidPort;
    const parsed = try std.fmt.parseInt(u32, raw, 10);
    if (parsed == 0 or parsed > 65535) return error.InvalidPort;
    return @intCast(parsed);
}

pub fn parseIpLiteral(host: []const u8, port: u16) !std.net.Address {
    if (std.net.Address.parseIp4(host, port)) |addr| return addr else |_| {}
    if (std.net.Address.parseIp6(host, port)) |addr| return addr else |_| {}
    return error.InvalidHost;
}
