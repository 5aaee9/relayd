const std = @import("std");
const compat = @import("compat.zig");
const App = @import("app.zig").App;

pub fn main() !void {
    compat.ignoreSigpipe();

    var gpa = std.heap.DebugAllocator(.{}){};
    defer {
        const status = gpa.deinit();
        if (status == .leak) std.log.err("memory leaks detected", .{});
    }
    const allocator = gpa.allocator();
    const app = try App.init(allocator);
    defer app.deinit();
    try app.start();
    while (true) compat.sleep(std.time.ns_per_s);
}
