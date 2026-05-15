use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn add(&self, amount: u64) {
        self.0.fetch_add(amount, Ordering::Relaxed);
    }

    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub struct Metrics {
    pub allocations_total: Counter,
    pub runtime_apply_total: Counter,
    pub restore_failures_total: Counter,
    pub rejected_no_host_total: Counter,
    pub bind_fail_total: Counter,
    pub tcp_splice_fast_path_total: Counter,
    pub tcp_copy_fallback_total: Counter,
    pub tcp_splice_attempt_total: Counter,
    pub tcp_splice_success_total: Counter,
    pub tcp_splice_fallback_total: Counter,
    pub tcp_splice_hard_failure_total: Counter,
    pub tcp_splice_fallback_forced_total: Counter,
    pub tcp_splice_fallback_unsupported_total: Counter,
    pub tcp_splice_fallback_runtime_error_total: Counter,
    pub tcp_session_create_total: Counter,
    pub tcp_session_close_total: Counter,
    pub tcp_session_event_total: Counter,
    pub tcp_session_worker_dispatch_total: Counter,
    pub tcp_session_worker0_dispatch_total: Counter,
    pub tcp_session_worker1_dispatch_total: Counter,
    pub tcp_accept_handoff_total: Counter,
    pub tcp_accept_handoff_worker0_total: Counter,
    pub tcp_accept_handoff_worker1_total: Counter,
    pub tcp_accept_handoff_worker2_total: Counter,
    pub tcp_accept_handoff_worker3_total: Counter,
    pub tcp_listener_accept_total: Counter,
    pub tcp_listener_accept_worker0_total: Counter,
    pub tcp_listener_accept_worker1_total: Counter,
    pub tcp_listener_accept_worker2_total: Counter,
    pub tcp_listener_accept_worker3_total: Counter,
    pub tcp_upstream_connect_total: Counter,
    pub tcp_upstream_connect_fail_total: Counter,
    pub tcp_active_sessions: Gauge,
    pub udp_packets_in_total: Counter,
    pub udp_packets_out_total: Counter,
    pub udp_bytes_in_total: Counter,
    pub udp_bytes_out_total: Counter,
    pub udp_recv_errors_total: Counter,
    pub udp_send_errors_total: Counter,
    pub udp_session_create_total: Counter,
    pub udp_session_expire_total: Counter,
    pub udp_batch_calls_total: Counter,
    pub udp_batch_messages_total: Counter,
    pub udp_drop_total: Counter,
    pub udp_reply_primary_total: Counter,
    pub udp_reply_drop_total: Counter,
    pub udp_reply_stale_total: Counter,
    pub udp_worker_packets_in_total: Counter,
    pub udp_worker_packets_out_total: Counter,
    pub udp_worker0_packets_in_total: Counter,
    pub udp_worker1_packets_in_total: Counter,
    pub udp_worker2_packets_in_total: Counter,
    pub udp_worker3_packets_in_total: Counter,
    pub udp_worker0_packets_out_total: Counter,
    pub udp_worker1_packets_out_total: Counter,
    pub udp_worker2_packets_out_total: Counter,
    pub udp_worker3_packets_out_total: Counter,
    pub udp_dataplane_redesign_packets_in_total: Counter,
    pub udp_dataplane_redesign_packets_out_total: Counter,
    pub udp_io_uring_submit_total: Counter,
    pub udp_io_uring_cqe_total: Counter,
    pub udp_io_uring_multishot_total: Counter,
    pub udp_io_uring_buf_release_total: Counter,
    pub udp_io_uring_fallback_total: Counter,
    pub udp_fast_path_packets_in_total: Counter,
    pub udp_fast_path_packets_out_total: Counter,
    pub udp_fast_path_gso_send_total: Counter,
    pub udp_fast_path_gro_recv_total: Counter,
    pub udp_fast_path_fallback_total: Counter,
    pub udp_fast_path_drop_total: Counter,
    pub udp_active_sessions: Gauge,
    pub restore_timeout_total: Counter,
    pub http_non_loopback_bind_total: Counter,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct JsonMetrics {
    pub allocations_total: u64,
    pub runtime_apply_total: u64,
    pub restore_failures_total: u64,
    pub rejected_no_host_total: u64,
    pub bind_fail_total: u64,
    pub tcp_splice_fast_path_total: u64,
    pub tcp_copy_fallback_total: u64,
    pub tcp_splice_attempt_total: u64,
    pub tcp_splice_success_total: u64,
    pub tcp_splice_fallback_total: u64,
    pub tcp_splice_hard_failure_total: u64,
    pub tcp_splice_fallback_forced_total: u64,
    pub tcp_splice_fallback_unsupported_total: u64,
    pub tcp_splice_fallback_runtime_error_total: u64,
    pub tcp_session_create_total: u64,
    pub tcp_session_close_total: u64,
    pub tcp_session_event_total: u64,
    pub tcp_session_worker_dispatch_total: u64,
    pub tcp_session_worker0_dispatch_total: u64,
    pub tcp_session_worker1_dispatch_total: u64,
    pub tcp_accept_handoff_total: u64,
    pub tcp_accept_handoff_worker0_total: u64,
    pub tcp_accept_handoff_worker1_total: u64,
    pub tcp_accept_handoff_worker2_total: u64,
    pub tcp_accept_handoff_worker3_total: u64,
    pub tcp_listener_accept_total: u64,
    pub tcp_listener_accept_worker0_total: u64,
    pub tcp_listener_accept_worker1_total: u64,
    pub tcp_listener_accept_worker2_total: u64,
    pub tcp_listener_accept_worker3_total: u64,
    pub tcp_upstream_connect_total: u64,
    pub tcp_upstream_connect_fail_total: u64,
    pub tcp_active_sessions: u64,
    pub udp_packets_in_total: u64,
    pub udp_packets_out_total: u64,
    pub udp_bytes_in_total: u64,
    pub udp_bytes_out_total: u64,
    pub udp_recv_errors_total: u64,
    pub udp_send_errors_total: u64,
    pub udp_session_create_total: u64,
    pub udp_session_expire_total: u64,
    pub udp_batch_calls_total: u64,
    pub udp_batch_messages_total: u64,
    pub udp_drop_total: u64,
    pub udp_reply_primary_total: u64,
    pub udp_reply_drop_total: u64,
    pub udp_reply_stale_total: u64,
    pub udp_worker_packets_in_total: u64,
    pub udp_worker_packets_out_total: u64,
    pub udp_worker0_packets_in_total: u64,
    pub udp_worker1_packets_in_total: u64,
    pub udp_worker2_packets_in_total: u64,
    pub udp_worker3_packets_in_total: u64,
    pub udp_worker0_packets_out_total: u64,
    pub udp_worker1_packets_out_total: u64,
    pub udp_worker2_packets_out_total: u64,
    pub udp_worker3_packets_out_total: u64,
    pub udp_dataplane_redesign_packets_in_total: u64,
    pub udp_dataplane_redesign_packets_out_total: u64,
    pub udp_io_uring_submit_total: u64,
    pub udp_io_uring_cqe_total: u64,
    pub udp_io_uring_multishot_total: u64,
    pub udp_io_uring_buf_release_total: u64,
    pub udp_io_uring_fallback_total: u64,
    pub udp_fast_path_packets_in_total: u64,
    pub udp_fast_path_packets_out_total: u64,
    pub udp_fast_path_gso_send_total: u64,
    pub udp_fast_path_gro_recv_total: u64,
    pub udp_fast_path_fallback_total: u64,
    pub udp_fast_path_drop_total: u64,
    pub udp_active_sessions: u64,
    pub restore_timeout_total: u64,
    pub http_non_loopback_bind_total: u64,
}

impl Metrics {
    pub fn snapshot(&self) -> JsonMetrics {
        JsonMetrics {
            allocations_total: self.allocations_total.load(),
            runtime_apply_total: self.runtime_apply_total.load(),
            restore_failures_total: self.restore_failures_total.load(),
            rejected_no_host_total: self.rejected_no_host_total.load(),
            bind_fail_total: self.bind_fail_total.load(),
            tcp_splice_fast_path_total: self.tcp_splice_fast_path_total.load(),
            tcp_copy_fallback_total: self.tcp_copy_fallback_total.load(),
            tcp_splice_attempt_total: self.tcp_splice_attempt_total.load(),
            tcp_splice_success_total: self.tcp_splice_success_total.load(),
            tcp_splice_fallback_total: self.tcp_splice_fallback_total.load(),
            tcp_splice_hard_failure_total: self.tcp_splice_hard_failure_total.load(),
            tcp_splice_fallback_forced_total: self.tcp_splice_fallback_forced_total.load(),
            tcp_splice_fallback_unsupported_total: self
                .tcp_splice_fallback_unsupported_total
                .load(),
            tcp_splice_fallback_runtime_error_total: self
                .tcp_splice_fallback_runtime_error_total
                .load(),
            tcp_session_create_total: self.tcp_session_create_total.load(),
            tcp_session_close_total: self.tcp_session_close_total.load(),
            tcp_session_event_total: self.tcp_session_event_total.load(),
            tcp_session_worker_dispatch_total: self.tcp_session_worker_dispatch_total.load(),
            tcp_session_worker0_dispatch_total: self.tcp_session_worker0_dispatch_total.load(),
            tcp_session_worker1_dispatch_total: self.tcp_session_worker1_dispatch_total.load(),
            tcp_accept_handoff_total: self.tcp_accept_handoff_total.load(),
            tcp_accept_handoff_worker0_total: self.tcp_accept_handoff_worker0_total.load(),
            tcp_accept_handoff_worker1_total: self.tcp_accept_handoff_worker1_total.load(),
            tcp_accept_handoff_worker2_total: self.tcp_accept_handoff_worker2_total.load(),
            tcp_accept_handoff_worker3_total: self.tcp_accept_handoff_worker3_total.load(),
            tcp_listener_accept_total: self.tcp_listener_accept_total.load(),
            tcp_listener_accept_worker0_total: self.tcp_listener_accept_worker0_total.load(),
            tcp_listener_accept_worker1_total: self.tcp_listener_accept_worker1_total.load(),
            tcp_listener_accept_worker2_total: self.tcp_listener_accept_worker2_total.load(),
            tcp_listener_accept_worker3_total: self.tcp_listener_accept_worker3_total.load(),
            tcp_upstream_connect_total: self.tcp_upstream_connect_total.load(),
            tcp_upstream_connect_fail_total: self.tcp_upstream_connect_fail_total.load(),
            tcp_active_sessions: self.tcp_active_sessions.load(),
            udp_packets_in_total: self.udp_packets_in_total.load(),
            udp_packets_out_total: self.udp_packets_out_total.load(),
            udp_bytes_in_total: self.udp_bytes_in_total.load(),
            udp_bytes_out_total: self.udp_bytes_out_total.load(),
            udp_recv_errors_total: self.udp_recv_errors_total.load(),
            udp_send_errors_total: self.udp_send_errors_total.load(),
            udp_session_create_total: self.udp_session_create_total.load(),
            udp_session_expire_total: self.udp_session_expire_total.load(),
            udp_batch_calls_total: self.udp_batch_calls_total.load(),
            udp_batch_messages_total: self.udp_batch_messages_total.load(),
            udp_drop_total: self.udp_drop_total.load(),
            udp_reply_primary_total: self.udp_reply_primary_total.load(),
            udp_reply_drop_total: self.udp_reply_drop_total.load(),
            udp_reply_stale_total: self.udp_reply_stale_total.load(),
            udp_worker_packets_in_total: self.udp_worker_packets_in_total.load(),
            udp_worker_packets_out_total: self.udp_worker_packets_out_total.load(),
            udp_worker0_packets_in_total: self.udp_worker0_packets_in_total.load(),
            udp_worker1_packets_in_total: self.udp_worker1_packets_in_total.load(),
            udp_worker2_packets_in_total: self.udp_worker2_packets_in_total.load(),
            udp_worker3_packets_in_total: self.udp_worker3_packets_in_total.load(),
            udp_worker0_packets_out_total: self.udp_worker0_packets_out_total.load(),
            udp_worker1_packets_out_total: self.udp_worker1_packets_out_total.load(),
            udp_worker2_packets_out_total: self.udp_worker2_packets_out_total.load(),
            udp_worker3_packets_out_total: self.udp_worker3_packets_out_total.load(),
            udp_dataplane_redesign_packets_in_total: self
                .udp_dataplane_redesign_packets_in_total
                .load(),
            udp_dataplane_redesign_packets_out_total: self
                .udp_dataplane_redesign_packets_out_total
                .load(),
            udp_io_uring_submit_total: self.udp_io_uring_submit_total.load(),
            udp_io_uring_cqe_total: self.udp_io_uring_cqe_total.load(),
            udp_io_uring_multishot_total: self.udp_io_uring_multishot_total.load(),
            udp_io_uring_buf_release_total: self.udp_io_uring_buf_release_total.load(),
            udp_io_uring_fallback_total: self.udp_io_uring_fallback_total.load(),
            udp_fast_path_packets_in_total: self.udp_fast_path_packets_in_total.load(),
            udp_fast_path_packets_out_total: self.udp_fast_path_packets_out_total.load(),
            udp_fast_path_gso_send_total: self.udp_fast_path_gso_send_total.load(),
            udp_fast_path_gro_recv_total: self.udp_fast_path_gro_recv_total.load(),
            udp_fast_path_fallback_total: self.udp_fast_path_fallback_total.load(),
            udp_fast_path_drop_total: self.udp_fast_path_drop_total.load(),
            udp_active_sessions: self.udp_active_sessions.load(),
            restore_timeout_total: self.restore_timeout_total.load(),
            http_non_loopback_bind_total: self.http_non_loopback_bind_total.load(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_metrics_serializes_exact_documented_field_names() {
        let metrics = Metrics::default();
        metrics.allocations_total.inc();
        metrics.runtime_apply_total.add(2);
        metrics.tcp_active_sessions.inc();
        metrics.udp_active_sessions.inc();
        metrics.udp_active_sessions.dec();

        let value = serde_json::to_value(metrics.snapshot()).unwrap();
        let object = value.as_object().unwrap();
        let fields = [
            "allocations_total",
            "runtime_apply_total",
            "restore_failures_total",
            "restore_timeout_total",
            "rejected_no_host_total",
            "bind_fail_total",
            "http_non_loopback_bind_total",
            "tcp_splice_fast_path_total",
            "tcp_copy_fallback_total",
            "tcp_splice_attempt_total",
            "tcp_splice_success_total",
            "tcp_splice_fallback_total",
            "tcp_splice_hard_failure_total",
            "tcp_splice_fallback_forced_total",
            "tcp_splice_fallback_unsupported_total",
            "tcp_splice_fallback_runtime_error_total",
            "tcp_session_create_total",
            "tcp_session_close_total",
            "tcp_session_event_total",
            "tcp_session_worker_dispatch_total",
            "tcp_session_worker0_dispatch_total",
            "tcp_session_worker1_dispatch_total",
            "tcp_accept_handoff_total",
            "tcp_accept_handoff_worker0_total",
            "tcp_accept_handoff_worker1_total",
            "tcp_accept_handoff_worker2_total",
            "tcp_accept_handoff_worker3_total",
            "tcp_listener_accept_total",
            "tcp_listener_accept_worker0_total",
            "tcp_listener_accept_worker1_total",
            "tcp_listener_accept_worker2_total",
            "tcp_listener_accept_worker3_total",
            "tcp_upstream_connect_total",
            "tcp_upstream_connect_fail_total",
            "tcp_active_sessions",
            "udp_packets_in_total",
            "udp_packets_out_total",
            "udp_bytes_in_total",
            "udp_bytes_out_total",
            "udp_recv_errors_total",
            "udp_send_errors_total",
            "udp_session_create_total",
            "udp_session_expire_total",
            "udp_batch_calls_total",
            "udp_batch_messages_total",
            "udp_drop_total",
            "udp_reply_primary_total",
            "udp_reply_drop_total",
            "udp_reply_stale_total",
            "udp_active_sessions",
            "udp_worker_packets_in_total",
            "udp_worker_packets_out_total",
            "udp_worker0_packets_in_total",
            "udp_worker1_packets_in_total",
            "udp_worker2_packets_in_total",
            "udp_worker3_packets_in_total",
            "udp_worker0_packets_out_total",
            "udp_worker1_packets_out_total",
            "udp_worker2_packets_out_total",
            "udp_worker3_packets_out_total",
            "udp_dataplane_redesign_packets_in_total",
            "udp_dataplane_redesign_packets_out_total",
            "udp_io_uring_submit_total",
            "udp_io_uring_cqe_total",
            "udp_io_uring_multishot_total",
            "udp_io_uring_buf_release_total",
            "udp_io_uring_fallback_total",
            "udp_fast_path_packets_in_total",
            "udp_fast_path_packets_out_total",
            "udp_fast_path_gso_send_total",
            "udp_fast_path_gro_recv_total",
            "udp_fast_path_fallback_total",
            "udp_fast_path_drop_total",
        ];

        for field in fields {
            assert!(object.contains_key(field), "missing field {field}");
        }
        assert_eq!(object.len(), fields.len());
        assert_eq!(object["allocations_total"], 1);
        assert_eq!(object["runtime_apply_total"], 2);
        assert_eq!(object["tcp_active_sessions"], 1);
        assert_eq!(object["udp_active_sessions"], 0);
    }
}
