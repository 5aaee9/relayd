const std = @import("std");

pub const Protocol = enum {
    tcp,
    udp,

    pub fn fromString(text: []const u8) ?Protocol {
        if (std.ascii.eqlIgnoreCase(text, "tcp")) return .tcp;
        if (std.ascii.eqlIgnoreCase(text, "udp")) return .udp;
        return null;
    }

    pub fn asString(self: Protocol) []const u8 {
        return @tagName(self);
    }
};

pub const RuntimeStatus = enum {
    active,
    rejecting_no_host,
    degraded_bind_failed,
    degraded_apply_failed,
    degraded_close_failed,

    pub fn asString(self: RuntimeStatus) []const u8 {
        return @tagName(self);
    }
};

pub const ErrorKind = enum {
    bind_failed,
    apply_failed,
    close_failed,
    restore_failed,
    invalid_input,

    pub fn asString(self: ErrorKind) []const u8 {
        return @tagName(self);
    }
};

pub const Allocation = struct {
    id: []u8,
    protocol: Protocol,
    port: u16,
    target_port: u16,
    host: ?[]u8,
    created_at_ms: i64,
    updated_at_ms: i64,

    pub fn clone(self: Allocation, allocator: std.mem.Allocator) !Allocation {
        return .{
            .id = try allocator.dupe(u8, self.id),
            .protocol = self.protocol,
            .port = self.port,
            .target_port = self.target_port,
            .host = if (self.host) |host| try allocator.dupe(u8, host) else null,
            .created_at_ms = self.created_at_ms,
            .updated_at_ms = self.updated_at_ms,
        };
    }

    pub fn deinit(self: *Allocation, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        if (self.host) |host| allocator.free(host);
        self.* = undefined;
    }
};

pub const AllocationView = struct {
    id: []u8,
    protocol: Protocol,
    port: u16,
    target_port: u16,
    host: ?[]u8,
    effective_target_port: ?u16,
    effective_host: ?[]u8,
    host_configured: bool,
    runtime_status: RuntimeStatus,
    error_kind: ?ErrorKind,
    last_error: ?[]u8,
    created_at_ms: i64,
    updated_at_ms: i64,
};

pub fn isHostConfigured(host: ?[]const u8) bool {
    return if (host) |value| value.len != 0 else false;
}
