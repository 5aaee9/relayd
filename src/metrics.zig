const std = @import("std");

pub const Counter = struct {
    value: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn inc(self: *Counter) void {
        _ = self.value.fetchAdd(1, .monotonic);
    }

    pub fn add(self: *Counter, amount: u64) void {
        _ = self.value.fetchAdd(amount, .monotonic);
    }

    pub fn load(self: *const Counter) u64 {
        return self.value.load(.monotonic);
    }
};

pub const Metrics = struct {
    allocations_total: Counter = .{},
    runtime_apply_total: Counter = .{},
    restore_failures_total: Counter = .{},
    rejected_no_host_total: Counter = .{},
    bind_fail_total: Counter = .{},
    tcp_splice_fast_path_total: Counter = .{},
    tcp_copy_fallback_total: Counter = .{},
    udp_packets_in_total: Counter = .{},
    udp_packets_out_total: Counter = .{},
    restore_timeout_total: Counter = .{},
    http_non_loopback_bind_total: Counter = .{},
};
