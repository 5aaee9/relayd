const std = @import("std");

pub const sqlite3 = opaque {};
pub const sqlite3_stmt = opaque {};

pub const SQLITE_OK: c_int = 0;
pub const SQLITE_ROW: c_int = 100;
pub const SQLITE_DONE: c_int = 101;
pub const SQLITE_OPEN_READWRITE: c_int = 0x00000002;
pub const SQLITE_OPEN_CREATE: c_int = 0x00000004;
pub const SQLITE_OPEN_FULLMUTEX: c_int = 0x00010000;
pub const SQLITE_TRANSIENT = @as(?*const anyopaque, @ptrFromInt(@as(usize, @bitCast(@as(isize, -1)))));

pub extern "c" fn sqlite3_open_v2(filename: [*:0]const u8, ppDb: *?*sqlite3, flags: c_int, zVfs: ?[*:0]const u8) c_int;
pub extern "c" fn sqlite3_close_v2(db: *sqlite3) c_int;
pub extern "c" fn sqlite3_errmsg(db: *sqlite3) [*:0]const u8;
pub extern "c" fn sqlite3_exec(db: *sqlite3, sql: [*:0]const u8, callback: ?*const anyopaque, arg: ?*anyopaque, errmsg: ?*?[*:0]u8) c_int;
pub extern "c" fn sqlite3_free(ptr: ?*anyopaque) void;
pub extern "c" fn sqlite3_prepare_v2(db: *sqlite3, sql: [*:0]const u8, nByte: c_int, ppStmt: *?*sqlite3_stmt, pzTail: ?*?[*:0]const u8) c_int;
pub extern "c" fn sqlite3_step(stmt: *sqlite3_stmt) c_int;
pub extern "c" fn sqlite3_finalize(stmt: *sqlite3_stmt) c_int;
pub extern "c" fn sqlite3_bind_text(stmt: *sqlite3_stmt, idx: c_int, value: [*]const u8, n: c_int, destructor: ?*const anyopaque) c_int;
pub extern "c" fn sqlite3_bind_int(stmt: *sqlite3_stmt, idx: c_int, value: c_int) c_int;
pub extern "c" fn sqlite3_bind_int64(stmt: *sqlite3_stmt, idx: c_int, value: i64) c_int;
pub extern "c" fn sqlite3_bind_null(stmt: *sqlite3_stmt, idx: c_int) c_int;
pub extern "c" fn sqlite3_column_text(stmt: *sqlite3_stmt, idx: c_int) ?[*:0]const u8;
pub extern "c" fn sqlite3_column_int(stmt: *sqlite3_stmt, idx: c_int) c_int;
pub extern "c" fn sqlite3_column_int64(stmt: *sqlite3_stmt, idx: c_int) i64;
pub extern "c" fn sqlite3_busy_timeout(db: *sqlite3, ms: c_int) c_int;
pub extern "c" fn sqlite3_libversion() [*:0]const u8;

pub fn check(code: c_int, db: ?*sqlite3) !void {
    if (code == SQLITE_OK or code == SQLITE_ROW or code == SQLITE_DONE) return;
    if (db) |conn| std.log.err("sqlite error {d}: {s}", .{ code, std.mem.span(sqlite3_errmsg(conn)) });
    return error.SqliteFailure;
}
