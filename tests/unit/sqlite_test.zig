const std = @import("std");
const sqlite = @import("../../src/storage/sqlite.zig");
const model = @import("../../src/model/allocation.zig");

fn tempDbPath(allocator: std.mem.Allocator) ![]u8 {
    try std.fs.cwd().makePath(".zig-cache/sqlite-tests");
    return std.fmt.allocPrint(allocator, ".zig-cache/sqlite-tests/{d}.sqlite", .{std.time.nanoTimestamp()});
}

test "sqlite persists and reloads allocations" {
    const path = try tempDbPath(std.testing.allocator);
    defer {
        std.fs.cwd().deleteFile(path) catch {};
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
