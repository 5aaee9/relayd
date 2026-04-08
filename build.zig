const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "relayd",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.linkLibC();
    if (dirExists(".local/lib")) exe.addLibraryPath(b.path(".local/lib"));
    exe.linkSystemLibrary("sqlite3");
    b.installArtifact(exe);

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("test_root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    tests.linkLibC();
    if (dirExists(".local/lib")) tests.addLibraryPath(b.path(".local/lib"));
    tests.linkSystemLibrary("sqlite3");

    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run relayd tests");
    test_step.dependOn(&run_tests.step);
}

fn dirExists(path: []const u8) bool {
    std.fs.cwd().access(path, .{}) catch return false;
    return true;
}
