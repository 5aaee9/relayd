const std = @import("std");
const App = @import("app.zig").App;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const status = gpa.deinit();
        if (status == .leak) std.log.err("memory leaks detected", .{});
    }
    const allocator = gpa.allocator();
    const app = try App.init(allocator);
    defer app.deinit();
    try app.start();
    while (true) std.Thread.sleep(std.time.ns_per_s);
}
