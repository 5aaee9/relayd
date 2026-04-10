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

pub const Gauge = struct {
    value: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn inc(self: *Gauge) void {
        _ = self.value.fetchAdd(1, .monotonic);
    }

    pub fn dec(self: *Gauge) void {
        _ = self.value.fetchSub(1, .monotonic);
    }

    pub fn load(self: *const Gauge) u64 {
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
    tcp_splice_attempt_total: Counter = .{},
    tcp_splice_success_total: Counter = .{},
    tcp_splice_fallback_total: Counter = .{},
    tcp_splice_hard_failure_total: Counter = .{},
    tcp_splice_fallback_forced_total: Counter = .{},
    tcp_splice_fallback_unsupported_total: Counter = .{},
    tcp_splice_fallback_runtime_error_total: Counter = .{},
    tcp_session_create_total: Counter = .{},
    tcp_session_close_total: Counter = .{},
    tcp_session_event_total: Counter = .{},
    tcp_session_worker_dispatch_total: Counter = .{},
    tcp_session_worker0_dispatch_total: Counter = .{},
    tcp_session_worker1_dispatch_total: Counter = .{},
    tcp_accept_handoff_total: Counter = .{},
    tcp_accept_handoff_worker0_total: Counter = .{},
    tcp_accept_handoff_worker1_total: Counter = .{},
    tcp_accept_handoff_worker2_total: Counter = .{},
    tcp_accept_handoff_worker3_total: Counter = .{},
    tcp_listener_accept_total: Counter = .{},
    tcp_listener_accept_worker0_total: Counter = .{},
    tcp_listener_accept_worker1_total: Counter = .{},
    tcp_listener_accept_worker2_total: Counter = .{},
    tcp_listener_accept_worker3_total: Counter = .{},
    tcp_upstream_connect_total: Counter = .{},
    tcp_upstream_connect_fail_total: Counter = .{},
    tcp_active_sessions: Gauge = .{},
    udp_packets_in_total: Counter = .{},
    udp_packets_out_total: Counter = .{},
    udp_bytes_in_total: Counter = .{},
    udp_bytes_out_total: Counter = .{},
    udp_recv_errors_total: Counter = .{},
    udp_send_errors_total: Counter = .{},
    udp_session_create_total: Counter = .{},
    udp_session_expire_total: Counter = .{},
    udp_batch_calls_total: Counter = .{},
    udp_batch_messages_total: Counter = .{},
    udp_drop_total: Counter = .{},
    udp_reply_primary_total: Counter = .{},
    udp_reply_drop_total: Counter = .{},
    udp_reply_stale_total: Counter = .{},
    udp_active_sessions: Gauge = .{},
    restore_timeout_total: Counter = .{},
    http_non_loopback_bind_total: Counter = .{},
};
