const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const sqlite_flags = &[_][]const u8{
        "-DSQLITE_DQS=0",
        "-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1",
        "-DSQLITE_USE_ALLOCA=1",
        "-DSQLITE_THREADSAFE=1",
        "-DSQLITE_TEMP_STORE=3",
        "-DSQLITE_ENABLE_API_ARMOR=1",
        "-DSQLITE_ENABLE_UNLOCK_NOTIFY",
        "-DSQLITE_DEFAULT_FILE_PERMISSIONS=0600",
        "-DSQLITE_OMIT_DECLTYPE=1",
        "-DSQLITE_OMIT_DEPRECATED=1",
        "-DSQLITE_OMIT_LOAD_EXTENSION=1",
        "-DSQLITE_OMIT_PROGRESS_CALLBACK=1",
        "-DSQLITE_OMIT_SHARED_CACHE",
        "-DSQLITE_OMIT_TRACE=1",
        "-DSQLITE_OMIT_UTF16=1",
        "-DHAVE_USLEEP=0",
    };
    const has_bundled_sqlite = fileExists(b, "lib/sqlite3.c");
    const metrics_prometheus = b.dependency("metrics", .{
        .target = target,
        .optimize = optimize,
    }).module("metrics");

    const exe_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .strip = optimize != .Debug,
    });
    exe_module.addImport("metrics_prometheus", metrics_prometheus);
    linkSqlite(b, exe_module, has_bundled_sqlite, sqlite_flags);

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
    test_module.addImport("metrics_prometheus", metrics_prometheus);
    linkSqlite(b, test_module, has_bundled_sqlite, sqlite_flags);

    const tests = b.addTest(.{
        .root_module = test_module,
    });

    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run relayd tests");
    test_step.dependOn(&run_tests.step);
}

fn linkSqlite(
    b: *std.Build,
    module: *std.Build.Module,
    has_bundled_sqlite: bool,
    sqlite_flags: []const []const u8,
) void {
    if (has_bundled_sqlite) {
        module.addCSourceFile(.{
            .file = b.path("lib/sqlite3.c"),
            .flags = sqlite_flags,
        });
    } else {
        if (dirExists(b, ".local/lib")) module.addLibraryPath(b.path(".local/lib"));
        module.linkSystemLibrary("sqlite3", .{});
    }
}

fn fileExists(b: *std.Build, path: []const u8) bool {
    if (@hasDecl(std.Io, "Dir")) {
        std.Io.Dir.cwd().access(b.graph.io, path, .{}) catch return false;
    } else {
        std.fs.cwd().access(path, .{}) catch return false;
    }
    return true;
}

fn dirExists(b: *std.Build, path: []const u8) bool {
    return fileExists(b, path);
}
