test {
    _ = @import("src/util/uuidv7.zig");
    _ = @import("tests/unit/config_test.zig");
    _ = @import("tests/unit/sqlite_test.zig");
    _ = @import("tests/unit/allocator_test.zig");
    _ = @import("tests/unit/runtime_udp_fast_path_test.zig");
    _ = @import("tests/integration/service_forward_test.zig");
    _ = @import("tests/integration/http_api_test.zig");
}
