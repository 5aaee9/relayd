const std = @import("std");
const net = @import("net_compat.zig");

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
    tcp_session_model_workers: u32,
    tcp_session_model_accept_balanced: bool,
    tcp_session_model_sharded_accept: bool,
    tcp_session_model_max_active: u32,
    tcp_splice_enabled: bool,
    force_tcp_copy_fallback: bool,
    udp_session_workers: u32,
    udp_io_uring_enabled: bool,
    udp_gro_enabled: bool,
    udp_dataplane_redesign_enabled: bool,
    udp_fast_path_enabled: bool,
    udp_fast_path_segment_size: u32,
    udp_fast_path_gso_burst: u32,
    udp_socket_recv_buffer_bytes: u32,
    udp_socket_send_buffer_bytes: u32,
    runtime_apply_timeout_ms: u32,
    restore_sweep_timeout_ms: u32,
    db_path: []u8,

    pub fn parseEnv(allocator: std.mem.Allocator) !Config {
        const raw_listen = try envOwned(allocator, "HTTP_LISTEN", ":8080");
        defer allocator.free(raw_listen);
        const listen = try parseHttpListen(allocator, raw_listen);
        errdefer allocator.free(listen.host);

        const raw_range = try envOwned(allocator, "PORT_RANGE", "10000-30000");
        defer allocator.free(raw_range);
        const port_range = try parsePortRange(raw_range);

        const token = try getRequiredEnvVarOwned(allocator, "AUTH_TOKEN");
        errdefer allocator.free(token);
        if (token.len == 0) {
            std.log.err("required environment variable {s} must not be empty", .{"AUTH_TOKEN"});
            return error.EmptyAuthToken;
        }

        const tcp_session_model_workers = try envU32("TCP_SESSION_MODEL_WORKERS", 0);
        const tcp_session_model_max_active = try envU32("TCP_SESSION_MODEL_MAX_ACTIVE", 256);
        const udp_session_workers = try envU32("UDP_SESSION_WORKERS", 0);
        const udp_fast_path_segment_size = try envU32("UDP_FAST_PATH_SEGMENT_SIZE", 1472);
        const udp_fast_path_gso_burst = try envU32("UDP_FAST_PATH_GSO_BURST", 16);
        const udp_socket_recv_buffer_bytes = try envU32("UDP_SOCKET_RCVBUF_BYTES", 8 * 1024 * 1024);
        const udp_socket_send_buffer_bytes = try envU32("UDP_SOCKET_SNDBUF_BYTES", 8 * 1024 * 1024);
        const runtime_apply_timeout_ms = try envU32("RUNTIME_APPLY_TIMEOUT_MS", 2000);
        const restore_sweep_timeout_ms = try envU32("RESTORE_SWEEP_TIMEOUT_MS", 30000);

        const db_path = try envOwned(allocator, "SQLITE_PATH", "relayd.sqlite3");
        errdefer allocator.free(db_path);

        return .{
            .http_listen_host = listen.host,
            .http_listen_port = listen.port,
            .port_range = port_range,
            .auth_token = token,
            .tcp_session_model_enabled = envBool("TCP_SESSION_MODEL_ENABLED"),
            .tcp_session_model_workers = tcp_session_model_workers,
            .tcp_session_model_accept_balanced = envBool("TCP_SESSION_MODEL_ACCEPT_BALANCED"),
            .tcp_session_model_sharded_accept = envBool("TCP_SESSION_MODEL_SHARDED_ACCEPT"),
            .tcp_session_model_max_active = tcp_session_model_max_active,
            .tcp_splice_enabled = envBool("TCP_SPLICE_ENABLED"),
            .force_tcp_copy_fallback = envBool("FORCE_TCP_COPY_FALLBACK"),
            .udp_session_workers = udp_session_workers,
            .udp_io_uring_enabled = envBool("UDP_IO_URING_ENABLED"),
            .udp_gro_enabled = envBool("UDP_GRO_ENABLED"),
            .udp_dataplane_redesign_enabled = envBool("UDP_DATAPLANE_REDESIGN_ENABLED"),
            .udp_fast_path_enabled = envBool("UDP_FAST_PATH_ENABLED"),
            .udp_fast_path_segment_size = udp_fast_path_segment_size,
            .udp_fast_path_gso_burst = udp_fast_path_gso_burst,
            .udp_socket_recv_buffer_bytes = udp_socket_recv_buffer_bytes,
            .udp_socket_send_buffer_bytes = udp_socket_send_buffer_bytes,
            .runtime_apply_timeout_ms = runtime_apply_timeout_ms,
            .restore_sweep_timeout_ms = restore_sweep_timeout_ms,
            .db_path = db_path,
        };
    }

    pub fn deinit(self: *Config, allocator: std.mem.Allocator) void {
        allocator.free(self.http_listen_host);
        allocator.free(self.auth_token);
        allocator.free(self.db_path);
        self.* = undefined;
    }
};

fn getEnvVarOwned(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const name_z = try allocator.dupeZ(u8, name);
    defer allocator.free(name_z);
    const raw = std.c.getenv(name_z.ptr) orelse return error.EnvironmentVariableNotFound;
    return try allocator.dupe(u8, std.mem.span(raw));
}

fn getRequiredEnvVarOwned(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    return getEnvVarOwned(allocator, name) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => {
            std.log.err("missing required environment variable: {s}", .{name});
            return err;
        },
        else => err,
    };
}

fn envOwned(allocator: std.mem.Allocator, name: []const u8, default_value: []const u8) ![]u8 {
    return getEnvVarOwned(allocator, name) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => try allocator.dupe(u8, default_value),
        else => err,
    };
}

fn envBool(name: []const u8) bool {
    const value = getEnvVarOwned(std.heap.page_allocator, name) catch return false;
    defer std.heap.page_allocator.free(value);
    return std.ascii.eqlIgnoreCase(value, "1") or std.ascii.eqlIgnoreCase(value, "true") or std.ascii.eqlIgnoreCase(value, "yes") or std.ascii.eqlIgnoreCase(value, "on");
}

fn envU32(name: []const u8, default_value: u32) !u32 {
    const value = getEnvVarOwned(std.heap.page_allocator, name) catch |err| switch (err) {
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

pub fn parseIpLiteral(host: []const u8, port: u16) !net.Address {
    if (net.Address.parseIp4(host, port)) |addr| return addr else |_| {}
    if (net.Address.parseIp6(host, port)) |addr| return addr else |_| {}
    return error.InvalidHost;
}
