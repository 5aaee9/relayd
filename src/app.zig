const std = @import("std");
const config_mod = @import("config.zig");
const Metrics = @import("metrics.zig").Metrics;
const storage = @import("storage/sqlite.zig");
const runtime = @import("runtime/manager.zig");
const service_mod = @import("service/allocation_service.zig");
const http_server = @import("http/server.zig");

pub const App = struct {
    allocator: std.mem.Allocator,
    config: config_mod.Config,
    metrics: Metrics,
    repo: storage.Repository,
    runtime_manager: runtime.RuntimeManager,
    service: service_mod.Service,
    http: http_server.HttpServer,

    pub fn init(allocator: std.mem.Allocator) !*App {
        const app = try allocator.create(App);
        errdefer allocator.destroy(app);

        var config = try config_mod.Config.parseEnv(allocator);
        errdefer config.deinit(allocator);

        var repo = try storage.Repository.open(allocator, config.db_path);
        errdefer repo.close();
        try repo.selfCheck();

        app.* = .{
            .allocator = allocator,
            .config = config,
            .metrics = .{},
            .repo = repo,
            .runtime_manager = undefined,
            .service = undefined,
            .http = undefined,
        };

        app.runtime_manager = try runtime.RuntimeManager.init(allocator, &app.metrics, .{
            .tcp_session_model_enabled = config.tcp_session_model_enabled,
            .tcp_session_model_workers = config.tcp_session_model_workers,
            .tcp_session_model_accept_balanced = config.tcp_session_model_accept_balanced,
            .tcp_session_model_sharded_accept = config.tcp_session_model_sharded_accept,
            .tcp_splice_enabled = config.tcp_splice_enabled,
            .force_tcp_copy_fallback = config.force_tcp_copy_fallback,
            .udp_session_workers = config.udp_session_workers,
            .udp_io_uring_enabled = config.udp_io_uring_enabled,
            .udp_gro_enabled = config.udp_gro_enabled,
            .udp_dataplane_redesign_enabled = config.udp_dataplane_redesign_enabled,
            .udp_fast_path_enabled = config.udp_fast_path_enabled,
            .udp_fast_path_segment_size = config.udp_fast_path_segment_size,
            .udp_fast_path_gso_burst = config.udp_fast_path_gso_burst,
            .udp_socket_recv_buffer_bytes = config.udp_socket_recv_buffer_bytes,
            .udp_socket_send_buffer_bytes = config.udp_socket_send_buffer_bytes,
        });
        errdefer app.runtime_manager.deinit();
        try app.runtime_manager.start();

        app.service = service_mod.Service.init(allocator, &app.repo, &app.runtime_manager, config.port_range, config.runtime_apply_timeout_ms);
        try app.service.restoreAll(config.restore_sweep_timeout_ms);

        app.http = .{
            .allocator = allocator,
            .service = &app.service,
            .metrics = &app.metrics,
            .host = config.http_listen_host,
            .port = config.http_listen_port,
            .auth_token = config.auth_token,
        };

        return app;
    }

    pub fn start(self: *App) !void {
        try self.http.start();
    }

    pub fn stop(self: *App) void {
        self.http.stop();
        self.runtime_manager.stop();
    }

    pub fn deinit(self: *App) void {
        self.http.stop();
        self.runtime_manager.deinit();
        self.repo.close();
        self.config.deinit(self.allocator);
        self.allocator.destroy(self);
    }
};
