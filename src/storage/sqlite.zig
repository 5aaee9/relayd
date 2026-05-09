const std = @import("std");
const abi = @import("sqlite_abi.zig");
const model = @import("../model/allocation.zig");

pub const Repository = struct {
    db: *abi.sqlite3,

    pub fn open(_: std.mem.Allocator, path: []const u8) !Repository {
        var db_opt: ?*abi.sqlite3 = null;
        const zpath = try std.heap.c_allocator.dupeZ(u8, path);
        defer std.heap.c_allocator.free(zpath);
        try abi.check(abi.sqlite3_open_v2(zpath.ptr, &db_opt, abi.SQLITE_OPEN_READWRITE | abi.SQLITE_OPEN_CREATE | abi.SQLITE_OPEN_FULLMUTEX, null), null);
        const db = db_opt orelse return error.SqliteFailure;
        errdefer _ = abi.sqlite3_close_v2(db);
        try abi.check(abi.sqlite3_busy_timeout(db, 5000), db);
        try exec(db, "PRAGMA journal_mode=WAL;");
        try exec(db, "CREATE TABLE IF NOT EXISTS allocations (id TEXT PRIMARY KEY, protocol TEXT NOT NULL, port INTEGER NOT NULL, target_port INTEGER NOT NULL, host TEXT, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL, UNIQUE(protocol, port));");
        try exec(db, "CREATE TABLE IF NOT EXISTS bindings (allocation_id TEXT PRIMARY KEY, target_port INTEGER NOT NULL, host TEXT, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL);");
        try exec(
            db,
            "INSERT INTO bindings(allocation_id, target_port, host, created_at_ms, updated_at_ms) " ++
                "SELECT id, target_port, host, created_at_ms, updated_at_ms FROM allocations " ++
                "WHERE target_port > 0 AND NOT EXISTS (SELECT 1 FROM bindings WHERE bindings.allocation_id = allocations.id);",
        );
        return .{ .db = db };
    }

    pub fn close(self: *Repository) void {
        _ = abi.sqlite3_close_v2(self.db);
    }

    pub fn selfCheck(self: *Repository) !void {
        var stmt = try prepare(self.db, "SELECT sqlite_version();");
        defer stmt.deinit();
        try abi.check(abi.sqlite3_step(stmt.stmt), self.db);
        _ = abi.sqlite3_column_text(stmt.stmt, 0) orelse return error.SqliteFailure;
    }

    pub fn begin(self: *Repository) !void {
        try exec(self.db, "BEGIN IMMEDIATE TRANSACTION;");
    }

    pub fn commit(self: *Repository) !void {
        try exec(self.db, "COMMIT;");
    }

    pub fn rollback(self: *Repository) void {
        exec(self.db, "ROLLBACK;") catch {};
    }

    pub fn insertAllocation(self: *Repository, allocation: model.Allocation) !void {
        var stmt = try prepare(self.db, "INSERT INTO allocations(id, protocol, port, target_port, host, created_at_ms, updated_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?);");
        defer stmt.deinit();
        try stmt.bindText(1, allocation.id);
        try stmt.bindText(2, allocation.protocol.asString());
        try stmt.bindInt(3, allocation.port);
        try stmt.bindInt(4, allocation.target_port orelse 0);
        try stmt.bindOptionalText(5, allocation.host);
        try stmt.bindInt64(6, allocation.created_at_ms);
        try stmt.bindInt64(7, allocation.updated_at_ms);
        try stmt.done(self.db);
    }

    pub fn putBinding(self: *Repository, binding: model.Binding) !void {
        var stmt = try prepare(
            self.db,
            "INSERT INTO bindings(allocation_id, target_port, host, created_at_ms, updated_at_ms) VALUES(?, ?, ?, ?, ?) " ++
                "ON CONFLICT(allocation_id) DO UPDATE SET target_port = excluded.target_port, host = excluded.host, updated_at_ms = excluded.updated_at_ms;",
        );
        defer stmt.deinit();
        try stmt.bindText(1, binding.allocation_id);
        try stmt.bindInt(2, binding.target_port);
        try stmt.bindOptionalText(3, binding.host);
        try stmt.bindInt64(4, binding.created_at_ms);
        try stmt.bindInt64(5, binding.updated_at_ms);
        try stmt.done(self.db);

        try self.updateLegacyBindingColumns(binding.allocation_id, binding.target_port, binding.host, binding.updated_at_ms);
    }

    pub fn deleteBinding(self: *Repository, allocation_id: []const u8, updated_at_ms: i64) !bool {
        var stmt = try prepare(self.db, "DELETE FROM bindings WHERE allocation_id = ?;");
        defer stmt.deinit();
        try stmt.bindText(1, allocation_id);
        try stmt.done(self.db);
        const changed = abi.sqlite3_changes(self.db) > 0;
        if (changed) {
            try self.clearLegacyBindingColumns(allocation_id, updated_at_ms);
        }
        return changed;
    }

    pub fn deleteAllocation(self: *Repository, id: []const u8) !bool {
        var delete_binding = try prepare(self.db, "DELETE FROM bindings WHERE allocation_id = ?;");
        defer delete_binding.deinit();
        try delete_binding.bindText(1, id);
        try delete_binding.done(self.db);

        var stmt = try prepare(self.db, "DELETE FROM allocations WHERE id = ?;");
        defer stmt.deinit();
        try stmt.bindText(1, id);
        try stmt.done(self.db);
        return abi.sqlite3_changes(self.db) > 0;
    }

    pub fn getBinding(self: *Repository, allocator: std.mem.Allocator, allocation_id: []const u8) !?model.Binding {
        var stmt = try prepare(self.db, "SELECT allocation_id, target_port, host, created_at_ms, updated_at_ms FROM bindings WHERE allocation_id = ?;");
        defer stmt.deinit();
        try stmt.bindText(1, allocation_id);
        const rc = abi.sqlite3_step(stmt.stmt);
        if (rc == abi.SQLITE_DONE) return null;
        try abi.check(rc, self.db);
        return try rowToBinding(allocator, stmt.stmt);
    }

    pub fn getAllocation(self: *Repository, allocator: std.mem.Allocator, id: []const u8) !?model.Allocation {
        var stmt = try prepare(
            self.db,
            "SELECT a.id, a.protocol, a.port, COALESCE(b.target_port, NULLIF(a.target_port, 0)), COALESCE(b.host, a.host), a.created_at_ms, a.updated_at_ms " ++
                "FROM allocations a LEFT JOIN bindings b ON b.allocation_id = a.id WHERE a.id = ?;",
        );
        defer stmt.deinit();
        try stmt.bindText(1, id);
        const rc = abi.sqlite3_step(stmt.stmt);
        if (rc == abi.SQLITE_DONE) return null;
        try abi.check(rc, self.db);
        return try rowToAllocation(allocator, stmt.stmt);
    }

    pub fn listAllocations(self: *Repository, allocator: std.mem.Allocator) !std.ArrayList(model.Allocation) {
        var rows = std.ArrayList(model.Allocation).empty;
        errdefer {
            for (rows.items) |*item| item.deinit(allocator);
            rows.deinit(allocator);
        }
        var stmt = try prepare(
            self.db,
            "SELECT a.id, a.protocol, a.port, COALESCE(b.target_port, NULLIF(a.target_port, 0)), COALESCE(b.host, a.host), a.created_at_ms, a.updated_at_ms " ++
                "FROM allocations a LEFT JOIN bindings b ON b.allocation_id = a.id ORDER BY a.protocol, a.port;",
        );
        defer stmt.deinit();
        while (true) {
            const rc = abi.sqlite3_step(stmt.stmt);
            if (rc == abi.SQLITE_DONE) break;
            try abi.check(rc, self.db);
            try rows.append(allocator, try rowToAllocation(allocator, stmt.stmt));
        }
        return rows;
    }

    fn updateLegacyBindingColumns(self: *Repository, allocation_id: []const u8, target_port: u16, host: ?[]const u8, updated_at_ms: i64) !void {
        var stmt = try prepare(self.db, "UPDATE allocations SET target_port = ?, host = ?, updated_at_ms = ? WHERE id = ?;");
        defer stmt.deinit();
        try stmt.bindInt(1, target_port);
        try stmt.bindOptionalText(2, host);
        try stmt.bindInt64(3, updated_at_ms);
        try stmt.bindText(4, allocation_id);
        try stmt.done(self.db);
    }

    fn clearLegacyBindingColumns(self: *Repository, allocation_id: []const u8, updated_at_ms: i64) !void {
        var stmt = try prepare(self.db, "UPDATE allocations SET target_port = 0, host = NULL, updated_at_ms = ? WHERE id = ?;");
        defer stmt.deinit();
        try stmt.bindInt64(1, updated_at_ms);
        try stmt.bindText(2, allocation_id);
        try stmt.done(self.db);
    }
};

const Statement = struct {
    stmt: *abi.sqlite3_stmt,

    fn deinit(self: *Statement) void {
        _ = abi.sqlite3_finalize(self.stmt);
    }

    fn bindText(self: *Statement, idx: c_int, text: []const u8) !void {
        try abi.check(abi.sqlite3_bind_text(self.stmt, idx, text.ptr, @intCast(text.len), abi.SQLITE_TRANSIENT), null);
    }

    fn bindOptionalText(self: *Statement, idx: c_int, text: ?[]const u8) !void {
        if (text) |v| try self.bindText(idx, v) else try self.bindNull(idx);
    }

    fn bindInt(self: *Statement, idx: c_int, value: anytype) !void {
        try abi.check(abi.sqlite3_bind_int(self.stmt, idx, @intCast(value)), null);
    }

    fn bindInt64(self: *Statement, idx: c_int, value: i64) !void {
        try abi.check(abi.sqlite3_bind_int64(self.stmt, idx, value), null);
    }

    fn bindNull(self: *Statement, idx: c_int) !void {
        try abi.check(abi.sqlite3_bind_null(self.stmt, idx), null);
    }

    fn done(self: *Statement, db: *abi.sqlite3) !void {
        try abi.check(abi.sqlite3_step(self.stmt), db);
    }
};

fn exec(db: *abi.sqlite3, sql: []const u8) !void {
    const zsql = try std.heap.c_allocator.dupeZ(u8, sql);
    defer std.heap.c_allocator.free(zsql);
    try abi.check(abi.sqlite3_exec(db, zsql.ptr, null, null, null), db);
}

fn prepare(db: *abi.sqlite3, sql: []const u8) !Statement {
    const zsql = try std.heap.c_allocator.dupeZ(u8, sql);
    defer std.heap.c_allocator.free(zsql);
    var stmt_opt: ?*abi.sqlite3_stmt = null;
    try abi.check(abi.sqlite3_prepare_v2(db, zsql.ptr, -1, &stmt_opt, null), db);
    return .{ .stmt = stmt_opt orelse return error.SqliteFailure };
}

fn rowToAllocation(allocator: std.mem.Allocator, stmt: *abi.sqlite3_stmt) !model.Allocation {
    const id = try allocator.dupe(u8, std.mem.span(abi.sqlite3_column_text(stmt, 0).?));
    errdefer allocator.free(id);
    const host_ptr = abi.sqlite3_column_text(stmt, 4);
    return .{
        .id = id,
        .protocol = model.Protocol.fromString(std.mem.span(abi.sqlite3_column_text(stmt, 1).?)) orelse return error.InvalidData,
        .port = @intCast(abi.sqlite3_column_int(stmt, 2)),
        .target_port = if (abi.sqlite3_column_type(stmt, 3) == abi.SQLITE_NULL) null else blk: {
            const value = abi.sqlite3_column_int(stmt, 3);
            if (value <= 0) break :blk null;
            break :blk @as(u16, @intCast(value));
        },
        .host = if (host_ptr) |ptr| try allocator.dupe(u8, std.mem.span(ptr)) else null,
        .created_at_ms = abi.sqlite3_column_int64(stmt, 5),
        .updated_at_ms = abi.sqlite3_column_int64(stmt, 6),
    };
}

fn rowToBinding(allocator: std.mem.Allocator, stmt: *abi.sqlite3_stmt) !model.Binding {
    const allocation_id = try allocator.dupe(u8, std.mem.span(abi.sqlite3_column_text(stmt, 0).?));
    errdefer allocator.free(allocation_id);
    const host_ptr = abi.sqlite3_column_text(stmt, 2);
    return .{
        .allocation_id = allocation_id,
        .target_port = @intCast(abi.sqlite3_column_int(stmt, 1)),
        .host = if (host_ptr) |ptr| try allocator.dupe(u8, std.mem.span(ptr)) else null,
        .created_at_ms = abi.sqlite3_column_int64(stmt, 3),
        .updated_at_ms = abi.sqlite3_column_int64(stmt, 4),
    };
}
