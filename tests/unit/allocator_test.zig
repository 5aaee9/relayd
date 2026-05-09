const std = @import("std");
const model = @import("../../src/model/allocation.zig");

test "protocol parser" {
    try std.testing.expectEqual(model.Protocol.tcp, model.Protocol.fromString("tcp").?);
    try std.testing.expectEqual(model.Protocol.both, model.Protocol.fromString("both").?);
    try std.testing.expectEqual(model.Protocol.both, model.Protocol.fromString("BOTH").?);
    try std.testing.expect(model.Protocol.fromString("bogus") == null);
}

test "host configured helper" {
    try std.testing.expect(model.isHostConfigured("127.0.0.1"));
    try std.testing.expect(!model.isHostConfigured(null));
}
