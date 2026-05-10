const std = @import("std");
const compat = @import("compat.zig");
const metrics_prometheus = @import("metrics_prometheus");
const model = @import("model/allocation.zig");

pub const ListenerMetricsSnapshot = struct {
    port: u16,
    protocol: model.Protocol,
    connections_current: u64,
    rx_bytes_total: u64,
    tx_bytes_total: u64,
};

pub const ListenerMetricsRate = struct {
    port: u16,
    protocol: model.Protocol,
    connections_current: u64,
    rx_bytes_per_second: f64,
    tx_bytes_per_second: f64,
};

pub const RateCalculator = struct {
    allocator: std.mem.Allocator,
    samples: std.AutoHashMap(Key, Sample),

    const Key = struct {
        port: u16,
        protocol: model.Protocol,
    };

    const Sample = struct {
        rx_bytes_total: u64,
        tx_bytes_total: u64,
        timestamp_ms: i64,
    };

    pub fn init(allocator: std.mem.Allocator) RateCalculator {
        return .{
            .allocator = allocator,
            .samples = std.AutoHashMap(Key, Sample).init(allocator),
        };
    }

    pub fn deinit(self: *RateCalculator) void {
        self.samples.deinit();
        self.* = undefined;
    }

    pub fn calculate(
        self: *RateCalculator,
        allocator: std.mem.Allocator,
        snapshots: []const ListenerMetricsSnapshot,
        timestamp_ms: i64,
    ) ![]ListenerMetricsRate {
        var rates = try allocator.alloc(ListenerMetricsRate, snapshots.len);
        errdefer allocator.free(rates);

        for (snapshots, 0..) |snapshot, i| {
            const key: Key = .{ .port = snapshot.port, .protocol = snapshot.protocol };
            const previous = self.samples.get(key);
            const rate = calculateRate(snapshot, previous, timestamp_ms);
            rates[i] = rate;
            try self.samples.put(key, .{
                .rx_bytes_total = snapshot.rx_bytes_total,
                .tx_bytes_total = snapshot.tx_bytes_total,
                .timestamp_ms = timestamp_ms,
            });
        }

        try self.removeStaleSamples(snapshots);
        return rates;
    }

    pub fn sampleCount(self: *const RateCalculator) usize {
        return self.samples.count();
    }

    fn calculateRate(snapshot: ListenerMetricsSnapshot, previous: ?Sample, timestamp_ms: i64) ListenerMetricsRate {
        var rx_rate: f64 = 0;
        var tx_rate: f64 = 0;

        if (previous) |sample| {
            const elapsed_ms = timestamp_ms - sample.timestamp_ms;
            if (elapsed_ms > 0) {
                const elapsed_seconds = @as(f64, @floatFromInt(elapsed_ms)) / 1000.0;
                rx_rate = bytesPerSecond(snapshot.rx_bytes_total, sample.rx_bytes_total, elapsed_seconds);
                tx_rate = bytesPerSecond(snapshot.tx_bytes_total, sample.tx_bytes_total, elapsed_seconds);
            }
        }

        return .{
            .port = snapshot.port,
            .protocol = snapshot.protocol,
            .connections_current = snapshot.connections_current,
            .rx_bytes_per_second = rx_rate,
            .tx_bytes_per_second = tx_rate,
        };
    }

    fn bytesPerSecond(current: u64, previous: u64, elapsed_seconds: f64) f64 {
        if (current <= previous) return 0;
        return @as(f64, @floatFromInt(current - previous)) / elapsed_seconds;
    }

    fn removeStaleSamples(self: *RateCalculator, snapshots: []const ListenerMetricsSnapshot) !void {
        var stale = std.ArrayList(Key).empty;
        defer stale.deinit(self.allocator);

        var it = self.samples.keyIterator();
        while (it.next()) |key| {
            if (!hasSnapshot(snapshots, key.*)) {
                try stale.append(self.allocator, key.*);
            }
        }

        for (stale.items) |key| {
            _ = self.samples.remove(key);
        }
    }

    fn hasSnapshot(snapshots: []const ListenerMetricsSnapshot, key: Key) bool {
        for (snapshots) |snapshot| {
            if (snapshot.port == key.port and snapshot.protocol == key.protocol) return true;
        }
        return false;
    }
};

const MetricLabels = struct {
    port: u16,
    protocol: []const u8,
};

const RenderMetrics = struct {
    connections_current: metrics_prometheus.GaugeVec(u64, MetricLabels),
    rx_bytes_per_second: metrics_prometheus.GaugeVec(f64, MetricLabels),
    tx_bytes_per_second: metrics_prometheus.GaugeVec(f64, MetricLabels),
};

pub fn render(
    allocator: std.mem.Allocator,
    snapshots: []const ListenerMetricsSnapshot,
    calculator: *RateCalculator,
    timestamp_ms: i64,
) ![]u8 {
    const rates = try calculator.calculate(allocator, snapshots, timestamp_ms);
    defer allocator.free(rates);

    var metrics = RenderMetrics{
        .connections_current = try metrics_prometheus.GaugeVec(u64, MetricLabels).init(
            allocator,
            compat.io(),
            "relayd_connections_current",
            .{},
            .{},
        ),
        .rx_bytes_per_second = try metrics_prometheus.GaugeVec(f64, MetricLabels).init(
            allocator,
            compat.io(),
            "relayd_rx_bytes_per_second",
            .{},
            .{},
        ),
        .tx_bytes_per_second = try metrics_prometheus.GaugeVec(f64, MetricLabels).init(
            allocator,
            compat.io(),
            "relayd_tx_bytes_per_second",
            .{},
            .{},
        ),
    };
    defer metrics.tx_bytes_per_second.deinit();
    defer metrics.rx_bytes_per_second.deinit();
    defer metrics.connections_current.deinit();

    for (rates) |rate| {
        const labels: MetricLabels = .{
            .port = rate.port,
            .protocol = rate.protocol.asString(),
        };
        try metrics.connections_current.set(labels, rate.connections_current);
        try metrics.rx_bytes_per_second.set(labels, rate.rx_bytes_per_second);
        try metrics.tx_bytes_per_second.set(labels, rate.tx_bytes_per_second);
    }

    var writer: std.Io.Writer.Allocating = .init(allocator);
    errdefer writer.deinit();
    try metrics_prometheus.write(&metrics, &writer.writer);
    return try writer.toOwnedSlice();
}

test "rate calculator reports zero for first sample" {
    var calculator = RateCalculator.init(std.testing.allocator);
    defer calculator.deinit();

    const snapshots = [_]ListenerMetricsSnapshot{.{
        .port = 7000,
        .protocol = .tcp,
        .connections_current = 2,
        .rx_bytes_total = 100,
        .tx_bytes_total = 50,
    }};

    const rates = try calculator.calculate(std.testing.allocator, &snapshots, 1_000);
    defer std.testing.allocator.free(rates);

    try std.testing.expectEqual(@as(usize, 1), rates.len);
    try std.testing.expectEqual(@as(u64, 2), rates[0].connections_current);
    try std.testing.expectEqual(@as(f64, 0), rates[0].rx_bytes_per_second);
    try std.testing.expectEqual(@as(f64, 0), rates[0].tx_bytes_per_second);
}

test "rate calculator reports positive delta rates" {
    var calculator = RateCalculator.init(std.testing.allocator);
    defer calculator.deinit();

    const first = [_]ListenerMetricsSnapshot{.{
        .port = 7000,
        .protocol = .tcp,
        .connections_current = 1,
        .rx_bytes_total = 100,
        .tx_bytes_total = 50,
    }};
    const first_rates = try calculator.calculate(std.testing.allocator, &first, 1_000);
    defer std.testing.allocator.free(first_rates);

    const second = [_]ListenerMetricsSnapshot{.{
        .port = 7000,
        .protocol = .tcp,
        .connections_current = 1,
        .rx_bytes_total = 300,
        .tx_bytes_total = 150,
    }};
    const second_rates = try calculator.calculate(std.testing.allocator, &second, 2_000);
    defer std.testing.allocator.free(second_rates);

    try std.testing.expectEqual(@as(f64, 200), second_rates[0].rx_bytes_per_second);
    try std.testing.expectEqual(@as(f64, 100), second_rates[0].tx_bytes_per_second);
}

test "rate calculator clamps zero elapsed time to zero" {
    var calculator = RateCalculator.init(std.testing.allocator);
    defer calculator.deinit();

    const first = [_]ListenerMetricsSnapshot{.{
        .port = 7000,
        .protocol = .udp,
        .connections_current = 1,
        .rx_bytes_total = 100,
        .tx_bytes_total = 50,
    }};
    const first_rates = try calculator.calculate(std.testing.allocator, &first, 1_000);
    defer std.testing.allocator.free(first_rates);

    const second = [_]ListenerMetricsSnapshot{.{
        .port = 7000,
        .protocol = .udp,
        .connections_current = 1,
        .rx_bytes_total = 300,
        .tx_bytes_total = 150,
    }};
    const second_rates = try calculator.calculate(std.testing.allocator, &second, 1_000);
    defer std.testing.allocator.free(second_rates);

    try std.testing.expectEqual(@as(f64, 0), second_rates[0].rx_bytes_per_second);
    try std.testing.expectEqual(@as(f64, 0), second_rates[0].tx_bytes_per_second);
}

test "rate calculator removes stale labels" {
    var calculator = RateCalculator.init(std.testing.allocator);
    defer calculator.deinit();

    const first = [_]ListenerMetricsSnapshot{
        .{ .port = 7000, .protocol = .tcp, .connections_current = 1, .rx_bytes_total = 100, .tx_bytes_total = 50 },
        .{ .port = 7001, .protocol = .udp, .connections_current = 1, .rx_bytes_total = 200, .tx_bytes_total = 75 },
    };
    const first_rates = try calculator.calculate(std.testing.allocator, &first, 1_000);
    defer std.testing.allocator.free(first_rates);
    try std.testing.expectEqual(@as(usize, 2), calculator.sampleCount());

    const second = [_]ListenerMetricsSnapshot{.{
        .port = 7001,
        .protocol = .udp,
        .connections_current = 1,
        .rx_bytes_total = 250,
        .tx_bytes_total = 100,
    }};
    const second_rates = try calculator.calculate(std.testing.allocator, &second, 2_000);
    defer std.testing.allocator.free(second_rates);

    try std.testing.expectEqual(@as(usize, 1), calculator.sampleCount());
    try std.testing.expectEqual(@as(u16, 7001), second_rates[0].port);
    try std.testing.expectEqual(model.Protocol.udp, second_rates[0].protocol);
}

test "render emits prometheus labeled gauges" {
    var calculator = RateCalculator.init(std.testing.allocator);
    defer calculator.deinit();

    const snapshots = [_]ListenerMetricsSnapshot{.{
        .port = 7000,
        .protocol = .tcp,
        .connections_current = 3,
        .rx_bytes_total = 100,
        .tx_bytes_total = 50,
    }};

    const body = try render(std.testing.allocator, &snapshots, &calculator, 1_000);
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "# TYPE relayd_connections_current gauge") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "relayd_connections_current{port=\"7000\",protocol=\"tcp\"} 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "# TYPE relayd_rx_bytes_per_second gauge") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "relayd_rx_bytes_per_second{port=\"7000\",protocol=\"tcp\"} 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "# TYPE relayd_tx_bytes_per_second gauge") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "relayd_tx_bytes_per_second{port=\"7000\",protocol=\"tcp\"} 0") != null);
}
