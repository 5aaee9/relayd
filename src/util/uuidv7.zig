const std = @import("std");

pub const UuidString = [36]u8;

pub fn generateUuidV7(random: std.Random, now_ms: u64) UuidString {
    var bytes: [16]u8 = undefined;
    const ts: u64 = now_ms & 0xFFFFFFFFFFFF;
    bytes[0] = @intCast((ts >> 40) & 0xff);
    bytes[1] = @intCast((ts >> 32) & 0xff);
    bytes[2] = @intCast((ts >> 24) & 0xff);
    bytes[3] = @intCast((ts >> 16) & 0xff);
    bytes[4] = @intCast((ts >> 8) & 0xff);
    bytes[5] = @intCast(ts & 0xff);
    random.bytes(bytes[6..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    return format(bytes);
}

fn format(bytes: [16]u8) UuidString {
    var out: UuidString = undefined;
    _ = std.fmt.bufPrint(&out, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
        bytes[0],  bytes[1],  bytes[2],  bytes[3],
        bytes[4],  bytes[5],  bytes[6],  bytes[7],
        bytes[8],  bytes[9],  bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15],
    }) catch unreachable;
    return out;
}

test "uuid v7 format" {
    var prng = std.Random.DefaultPrng.init(42);
    const uuid = generateUuidV7(prng.random(), 1_700_000_000_000);
    try std.testing.expectEqual(@as(usize, 36), uuid.len);
    try std.testing.expectEqual(@as(u8, '7'), uuid[14]);
}
