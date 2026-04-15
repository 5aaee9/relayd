const std = @import("std");
const builtin = @import("builtin");
const runtime = @import("../../src/runtime/manager.zig");

test "udp segment control message encodes header and payload" {
    var control: [32]u8 align(@alignOf(usize)) = undefined;
    const used = runtime.encodeUdpSegmentControlMessage(&control, 1472) orelse return error.UnexpectedNull;
    try std.testing.expect(used >= 18);
    const endian = builtin.target.cpu.arch.endian();
    const len = std.mem.readInt(usize, control[0..@sizeOf(usize)], endian);
    try std.testing.expectEqual(@as(usize, 18), len);
    const level_offset = @sizeOf(usize);
    const type_offset = level_offset + @sizeOf(c_int);
    const level = std.mem.readInt(c_int, control[level_offset .. level_offset + @sizeOf(c_int)], endian);
    const msg_type = std.mem.readInt(c_int, control[type_offset .. type_offset + @sizeOf(c_int)], endian);
    try std.testing.expectEqual(@as(c_int, 17), level);
    try std.testing.expectEqual(@as(c_int, 103), msg_type);

    const header_size = @sizeOf(usize) + (2 * @sizeOf(c_int));
    const align_to: usize = @sizeOf(usize);
    const mask: usize = align_to - 1;
    const payload_offset = (header_size + mask) & ~mask;
    const value = std.mem.readInt(u16, control[payload_offset .. payload_offset + 2], endian);
    try std.testing.expectEqual(@as(u16, 1472), value);
}
