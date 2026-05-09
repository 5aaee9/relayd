const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    if (dirExists(b, ".local/lib")) exe_module.addLibraryPath(b.path(".local/lib"));
    exe_module.linkSystemLibrary("sqlite3", .{});

    const exe = b.addExecutable(.{
        .name = "relayd",
        .root_module = exe_module,
    });
    b.installArtifact(exe);

    const test_module = b.createModule(.{
        .root_source_file = b.path("test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    if (dirExists(b, ".local/lib")) test_module.addLibraryPath(b.path(".local/lib"));
    test_module.linkSystemLibrary("sqlite3", .{});

    const tests = b.addTest(.{
        .root_module = test_module,
    });

    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run relayd tests");
    test_step.dependOn(&run_tests.step);
}

fn dirExists(b: *std.Build, path: []const u8) bool {
    if (@hasDecl(std.Io, "Dir")) {
        std.Io.Dir.cwd().access(b.graph.io, path, .{}) catch return false;
    } else {
        std.fs.cwd().access(path, .{}) catch return false;
    }
    return true;
}
