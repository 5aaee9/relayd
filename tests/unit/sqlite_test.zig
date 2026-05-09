const std = @import("std");
const compat = @import("../../src/compat.zig");
const sqlite = @import("../../src/storage/sqlite.zig");
const model = @import("../../src/model/allocation.zig");

fn tempDbPath(allocator: std.mem.Allocator) ![]u8 {
    try compat.makePath(".zig-cache/sqlite-tests");
    return std.fmt.allocPrint(allocator, ".zig-cache/sqlite-tests/{d}.sqlite", .{compat.nanoTimestamp()});
}

test "sqlite persists and reloads allocations" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        compat.deleteFile(path);
        std.testing.allocator.free(path);
    }
    var db = try sqlite.Repository.open(std.testing.allocator, path);
    defer db.close();
    try db.selfCheck();
    var allocation = model.Allocation{
        .id = try std.testing.allocator.dupe(u8, "abc"),
        .protocol = .udp,
        .port = 12001,
        .target_port = 5353,
        .host = try std.testing.allocator.dupe(u8, "127.0.0.1"),
        .created_at_ms = 100,
        .updated_at_ms = 200,
    };
    defer allocation.deinit(std.testing.allocator);
    try db.insertAllocation(allocation);
    var rows = try db.listAllocations(std.testing.allocator);
    defer {
        for (rows.items) |*row| row.deinit(std.testing.allocator);
        rows.deinit(std.testing.allocator);
    }
    try std.testing.expectEqual(@as(usize, 1), rows.items.len);
    try std.testing.expectEqualStrings("abc", rows.items[0].id);
}

test "sqlite migrates legacy allocation binding into bindings table" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        compat.deleteFile(path);
        std.testing.allocator.free(path);
    }

    {
        var db = try sqlite.Repository.open(std.testing.allocator, path);
        defer db.close();
        var allocation = model.Allocation{
            .id = try std.testing.allocator.dupe(u8, "legacy"),
            .protocol = .tcp,
            .port = 12002,
            .target_port = 8080,
            .host = try std.testing.allocator.dupe(u8, "127.0.0.1"),
            .created_at_ms = 300,
            .updated_at_ms = 400,
        };
        defer allocation.deinit(std.testing.allocator);
        try db.insertAllocation(allocation);
    }

    var reopened = try sqlite.Repository.open(std.testing.allocator, path);
    defer reopened.close();

    var binding = (try reopened.getBinding(std.testing.allocator, "legacy")).?;
    defer binding.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("legacy", binding.allocation_id);
    try std.testing.expectEqual(@as(u16, 8080), binding.target_port);
    try std.testing.expectEqualStrings("127.0.0.1", binding.host.?);
}
