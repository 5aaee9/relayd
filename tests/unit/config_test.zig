const std = @import("std");
const config = @import("../../src/config.zig");

test "parse http listen defaults to loopback" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const parsed = try config.parseHttpListen(arena.allocator(), ":8080");
    try std.testing.expectEqualStrings("127.0.0.1", parsed.host);
    try std.testing.expectEqual(@as(u16, 8080), parsed.port);
}

test "parse port range" {
    const range = try config.parsePortRange("100-200");
    try std.testing.expect(range.contains(150));
}

test "parse ip literal rejects hostname" {
    _ = try config.parseIpLiteral("127.0.0.1", 80);
    try std.testing.expectError(error.InvalidHost, config.parseIpLiteral("example.com", 80));
}
