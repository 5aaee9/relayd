const std = @import("std");
const compat = @import("../compat.zig");
const config_mod = @import("../config.zig");
const model = @import("../model/allocation.zig");
const uuidv7 = @import("../util/uuidv7.zig");
const storage = @import("../storage/sqlite.zig");
const runtime = @import("../runtime/manager.zig");

pub const Failpoints = struct {
    create_timeout: bool = false,
    update_timeout: bool = false,
    delete_timeout: bool = false,
    delete_db_failure: bool = false,
};

pub const Service = struct {
    allocator: std.mem.Allocator,
    repo: *storage.Repository,
    runtime_manager: *runtime.RuntimeManager,
    port_range: config_mod.PortRange,
    apply_timeout_ms: u32,
    failpoints: Failpoints = .{},
    mutex: compat.Mutex = .{},
    prng: std.Random.DefaultPrng,

    pub fn init(allocator: std.mem.Allocator, repo: *storage.Repository, runtime_manager: *runtime.RuntimeManager, port_range: config_mod.PortRange, apply_timeout_ms: u32) Service {
        return .{
            .allocator = allocator,
            .repo = repo,
            .runtime_manager = runtime_manager,
            .port_range = port_range,
            .apply_timeout_ms = apply_timeout_ms,
            .prng = std.Random.DefaultPrng.init(@intCast(compat.nanoTimestamp())),
        };
    }

    pub fn createAllocation(self: *Service, protocol: model.Protocol, target_port: ?u16) !model.Allocation {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.failpoints.create_timeout) return error.Timeout;

        var port: u16 = self.port_range.start;
        while (port <= self.port_range.end) : (port += 1) {
            if (try self.exists(protocol, port)) continue;
            const now = compat.milliTimestamp();
            const id_arr = uuidv7.generateUuidV7(self.prng.random(), @intCast(now));
            var allocation = model.Allocation{
                .id = try self.allocator.dupe(u8, id_arr[0..]),
                .protocol = protocol,
                .port = port,
                .target_port = target_port,
                .host = null,
                .created_at_ms = now,
                .updated_at_ms = now,
            };
            errdefer allocation.deinit(self.allocator);

            self.runtime_manager.create(allocation, self.apply_timeout_ms) catch |err| switch (err) {
                error.RuntimeCreateFailed => {
                    allocation.deinit(self.allocator);
                    continue;
                },
                else => return err,
            };

            self.repo.begin() catch {};
            errdefer self.repo.rollback();
            self.repo.insertAllocation(allocation) catch |err| {
                self.runtime_manager.delete(allocation.id, self.apply_timeout_ms) catch {};
                return err;
            };
            if (target_port) |bound_port| {
                try self.repo.putBinding(.{
                    .allocation_id = allocation.id,
                    .target_port = bound_port,
                    .host = null,
                    .created_at_ms = now,
                    .updated_at_ms = now,
                });
            }
            try self.repo.commit();
            return allocation;
        }
        return error.NoAvailablePort;
    }

    pub fn getAllocation(self: *Service, allocator: std.mem.Allocator, id: []const u8) !?model.Allocation {
        return self.repo.getAllocation(allocator, id);
    }

    pub fn listAllocationResources(self: *Service, allocator: std.mem.Allocator) !std.ArrayList(model.AllocationResource) {
        var allocations = try self.repo.listAllocations(allocator);
        defer {
            for (allocations.items) |*item| item.deinit(allocator);
            allocations.deinit(allocator);
        }

        var resources = std.ArrayList(model.AllocationResource).empty;
        errdefer {
            for (resources.items) |*resource| deinitAllocationResource(allocator, resource);
            resources.deinit(allocator);
        }
        for (allocations.items) |allocation| {
            try resources.append(allocator, try cloneAllocationResource(allocator, allocation));
        }
        return resources;
    }

    pub fn getAllocationResource(self: *Service, allocator: std.mem.Allocator, id: []const u8) !?model.AllocationResource {
        var allocation = (try self.repo.getAllocation(allocator, id)) orelse return null;
        defer allocation.deinit(allocator);
        return try cloneAllocationResource(allocator, allocation);
    }

    pub fn getBindingView(self: *Service, allocator: std.mem.Allocator, id: []const u8) !?model.BindingView {
        var binding = (try self.repo.getBinding(allocator, id)) orelse return null;
        defer binding.deinit(allocator);
        var maybe_observed = try self.runtime_manager.snapshot(allocator, id);
        defer if (maybe_observed) |*obs| obs.deinit(allocator);
        return .{
            .allocation_id = try allocator.dupe(u8, binding.allocation_id),
            .host = if (binding.host) |host| try allocator.dupe(u8, host) else null,
            .target_port = binding.target_port,
            .effective_target_port = if (maybe_observed) |obs| obs.effective_target_port else null,
            .effective_host = if (maybe_observed) |obs| if (obs.effective_host) |host| try allocator.dupe(u8, host) else null else null,
            .runtime_status = if (maybe_observed) |obs| obs.runtime_status else .degraded_bind_failed,
            .error_kind = if (maybe_observed) |obs| obs.error_kind else .bind_failed,
            .last_error = if (maybe_observed) |obs| if (obs.last_error) |msg| try allocator.dupe(u8, msg) else null else try allocator.dupe(u8, "missing runtime state"),
            .created_at_ms = binding.created_at_ms,
            .updated_at_ms = binding.updated_at_ms,
        };
    }

    pub fn putBinding(self: *Service, id: []const u8, host: []const u8, target_port: u16) !model.Binding {
        self.mutex.lock();
        defer self.mutex.unlock();

        _ = try config_mod.parseIpLiteral(host, 0);
        var allocation = (try self.repo.getAllocation(self.allocator, id)) orelse return error.NotFound;
        defer allocation.deinit(self.allocator);

        const now = compat.milliTimestamp();
        const created_at_ms = if (try self.repo.getBinding(self.allocator, id)) |binding_value| blk: {
            var existing = binding_value;
            defer existing.deinit(self.allocator);
            break :blk existing.created_at_ms;
        } else now;

        var binding = model.Binding{
            .allocation_id = try self.allocator.dupe(u8, id),
            .target_port = target_port,
            .host = try self.allocator.dupe(u8, host),
            .created_at_ms = created_at_ms,
            .updated_at_ms = now,
        };
        errdefer binding.deinit(self.allocator);

        allocation.target_port = target_port;
        if (allocation.host) |old| self.allocator.free(old);
        allocation.host = try self.allocator.dupe(u8, host);
        allocation.updated_at_ms = now;

        self.repo.begin() catch {};
        errdefer self.repo.rollback();
        try self.repo.putBinding(binding);
        try self.repo.commit();

        if (self.failpoints.update_timeout) return error.Timeout;
        try self.runtime_manager.update(allocation, self.apply_timeout_ms);
        return binding;
    }

    pub fn deleteBinding(self: *Service, id: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var allocation = (try self.repo.getAllocation(self.allocator, id)) orelse return error.NotFound;
        defer allocation.deinit(self.allocator);

        const now = compat.milliTimestamp();
        self.repo.begin() catch {};
        errdefer self.repo.rollback();
        const changed = try self.repo.deleteBinding(id, now);
        if (!changed) return error.NotFound;
        try self.repo.commit();

        allocation.target_port = null;
        if (allocation.host) |old| self.allocator.free(old);
        allocation.host = null;
        allocation.updated_at_ms = now;

        if (self.failpoints.update_timeout) return error.Timeout;
        try self.runtime_manager.update(allocation, self.apply_timeout_ms);
    }

    pub fn setTarget(self: *Service, id: []const u8, host: []const u8) !model.Allocation {
        return self.updateAllocation(id, null, host);
    }

    pub fn updateAllocation(self: *Service, id: []const u8, target_port: ?u16, host_value: ?[]const u8) !model.Allocation {
        self.mutex.lock();
        defer self.mutex.unlock();

        var allocation = (try self.repo.getAllocation(self.allocator, id)) orelse return error.NotFound;
        errdefer allocation.deinit(self.allocator);

        if (host_value) |host| _ = try config_mod.parseIpLiteral(host, 0);

        const existing_binding = try self.repo.getBinding(self.allocator, id);
        defer if (existing_binding) |binding_value| {
            var binding = binding_value;
            binding.deinit(self.allocator);
        };

        const next_target_port = if (target_port) |port| port else if (existing_binding) |binding| binding.target_port else return error.NotFound;
        const next_host = if (host_value) |host| host else if (existing_binding) |binding| binding.host else null;
        const now = compat.milliTimestamp();
        const created_at_ms = if (existing_binding) |binding| binding.created_at_ms else now;

        var binding = model.Binding{
            .allocation_id = try self.allocator.dupe(u8, id),
            .target_port = next_target_port,
            .host = if (next_host) |host| try self.allocator.dupe(u8, host) else null,
            .created_at_ms = created_at_ms,
            .updated_at_ms = now,
        };
        defer binding.deinit(self.allocator);

        allocation.target_port = next_target_port;
        if (allocation.host) |old| self.allocator.free(old);
        allocation.host = if (next_host) |host| try self.allocator.dupe(u8, host) else null;
        allocation.updated_at_ms = now;

        self.repo.begin() catch {};
        errdefer self.repo.rollback();
        try self.repo.putBinding(binding);
        try self.repo.commit();

        if (self.failpoints.update_timeout) return error.Timeout;
        try self.runtime_manager.update(allocation, self.apply_timeout_ms);
        return allocation;
    }

    pub fn deleteAllocation(self: *Service, id: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var snapshot = (try self.repo.getAllocation(self.allocator, id)) orelse return error.NotFound;
        defer snapshot.deinit(self.allocator);

        if (self.failpoints.delete_timeout) return error.Timeout;
        try self.runtime_manager.delete(id, self.apply_timeout_ms);
        if (self.failpoints.delete_db_failure) {
            try self.runtime_manager.restore(snapshot, self.apply_timeout_ms);
            return error.DeletePersistenceFailed;
        }
        _ = try self.repo.deleteAllocation(id);
    }

    pub fn restoreAll(self: *Service, timeout_ms: u32) !void {
        var allocations = try self.repo.listAllocations(self.allocator);
        defer {
            for (allocations.items) |*item| item.deinit(self.allocator);
            allocations.deinit(self.allocator);
        }
        for (allocations.items) |allocation| {
            try self.runtime_manager.restore(allocation, timeout_ms);
        }
    }

    pub fn listAllocations(self: *Service, allocator: std.mem.Allocator) !std.ArrayList(model.AllocationView) {
        var allocations = try self.repo.listAllocations(allocator);
        defer {
            for (allocations.items) |*item| item.deinit(allocator);
            allocations.deinit(allocator);
        }

        var views = std.ArrayList(model.AllocationView).empty;
        errdefer {
            for (views.items) |*view| deinitView(allocator, view);
            views.deinit(allocator);
        }

        for (allocations.items) |allocation| {
            var maybe_observed = try self.runtime_manager.snapshot(allocator, allocation.id);
            defer if (maybe_observed) |*obs| obs.deinit(allocator);
            try views.append(allocator, .{
                .id = try allocator.dupe(u8, allocation.id),
                .protocol = allocation.protocol,
                .port = allocation.port,
                .target_port = allocation.target_port,
                .host = if (allocation.host) |host| try allocator.dupe(u8, host) else null,
                .effective_target_port = if (maybe_observed) |obs| obs.effective_target_port else null,
                .effective_host = if (maybe_observed) |obs| if (obs.effective_host) |host| try allocator.dupe(u8, host) else null else null,
                .host_configured = model.isHostConfigured(allocation.host),
                .runtime_status = if (maybe_observed) |obs| obs.runtime_status else .degraded_bind_failed,
                .error_kind = if (maybe_observed) |obs| obs.error_kind else .bind_failed,
                .last_error = if (maybe_observed) |obs| if (obs.last_error) |msg| try allocator.dupe(u8, msg) else null else try allocator.dupe(u8, "missing runtime state"),
                .created_at_ms = allocation.created_at_ms,
                .updated_at_ms = allocation.updated_at_ms,
            });
        }

        return views;
    }

    pub fn getAllocationView(self: *Service, allocator: std.mem.Allocator, id: []const u8) !?model.AllocationView {
        var views = try self.listAllocations(allocator);
        defer views.deinit(allocator);
        for (views.items) |*view| {
            if (!std.mem.eql(u8, view.id, id)) continue;
            const result = view.*;
            view.id = &.{};
            view.host = null;
            view.effective_host = null;
            view.last_error = null;
            return result;
        }
        return null;
    }

    fn exists(self: *Service, protocol: model.Protocol, port: u16) !bool {
        var allocations = try self.repo.listAllocations(self.allocator);
        defer {
            for (allocations.items) |*item| item.deinit(self.allocator);
            allocations.deinit(self.allocator);
        }
        for (allocations.items) |allocation| {
            if (allocation.protocol == protocol and allocation.port == port) return true;
        }
        return false;
    }
};

pub fn deinitAllocationResource(allocator: std.mem.Allocator, resource: *model.AllocationResource) void {
    allocator.free(resource.id);
    resource.* = undefined;
}

fn cloneAllocationResource(allocator: std.mem.Allocator, allocation: model.Allocation) !model.AllocationResource {
    return .{
        .id = try allocator.dupe(u8, allocation.id),
        .protocol = allocation.protocol,
        .port = allocation.port,
        .created_at_ms = allocation.created_at_ms,
        .updated_at_ms = allocation.updated_at_ms,
    };
}

pub fn deinitBindingView(allocator: std.mem.Allocator, view: *model.BindingView) void {
    allocator.free(view.allocation_id);
    if (view.host) |host| allocator.free(host);
    if (view.effective_host) |host| allocator.free(host);
    if (view.last_error) |msg| allocator.free(msg);
    view.* = undefined;
}

pub fn deinitView(allocator: std.mem.Allocator, view: *model.AllocationView) void {
    allocator.free(view.id);
    if (view.host) |host| allocator.free(host);
    if (view.effective_host) |host| allocator.free(host);
    if (view.last_error) |msg| allocator.free(msg);
    view.* = undefined;
}
