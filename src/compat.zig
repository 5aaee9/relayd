const std = @import("std");

pub fn io() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

pub const Mutex = struct {
    inner: std.atomic.Mutex = .unlocked,

    pub fn lock(self: *Mutex) void {
        while (!self.inner.tryLock()) {
            std.Thread.yield() catch {};
        }
    }

    pub fn unlock(self: *Mutex) void {
        self.inner.unlock();
    }
};

pub const Condition = struct {
    pub fn wait(_: *Condition, mutex: *Mutex) void {
        mutex.unlock();
        sleep(1 * std.time.ns_per_ms);
        mutex.lock();
    }

    pub fn broadcast(_: *Condition) void {}
    pub fn signal(_: *Condition) void {}
};

pub const EpollCreateError = error{
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
} || std.posix.UnexpectedError;

pub fn epoll_create1(flags: u32) EpollCreateError!std.posix.fd_t {
    const rc = std.os.linux.epoll_create1(flags);
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .INVAL => unreachable,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM => return error.SystemResources,
        else => |err| return std.posix.unexpectedErrno(err),
    }
}

pub const EpollCtlError = error{
    FileDescriptorAlreadyPresentInSet,
    OperationCausesCircularLoop,
    FileDescriptorNotRegistered,
    SystemResources,
    UserResourceLimitReached,
    FileDescriptorIncompatibleWithEpoll,
} || std.posix.UnexpectedError;

pub fn epoll_ctl(epfd: std.posix.fd_t, op: u32, fd: std.posix.fd_t, event: ?*std.os.linux.epoll_event) EpollCtlError!void {
    const rc = std.os.linux.epoll_ctl(epfd, op, fd, event);
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => return,
        .BADF => unreachable,
        .EXIST => return error.FileDescriptorAlreadyPresentInSet,
        .INVAL => unreachable,
        .LOOP => return error.OperationCausesCircularLoop,
        .NOENT => return error.FileDescriptorNotRegistered,
        .NOMEM => return error.SystemResources,
        .NOSPC => return error.UserResourceLimitReached,
        .PERM => return error.FileDescriptorIncompatibleWithEpoll,
        else => |err| return std.posix.unexpectedErrno(err),
    }
}

pub fn epoll_wait(epfd: std.posix.fd_t, events: []std.os.linux.epoll_event, timeout: i32) usize {
    while (true) {
        const rc = std.os.linux.epoll_wait(epfd, events.ptr, @intCast(events.len), timeout);
        switch (std.os.linux.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .BADF => unreachable,
            .FAULT => unreachable,
            .INVAL => unreachable,
            else => unreachable,
        }
    }
}

pub const PipeError = error{
    SystemFdQuotaExceeded,
    ProcessFdQuotaExceeded,
} || std.posix.UnexpectedError;

pub fn pipe2(flags: std.posix.O) PipeError![2]std.posix.fd_t {
    var fds: [2]std.posix.fd_t = undefined;
    const rc = std.os.linux.pipe2(&fds, flags);
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => return fds,
        .INVAL => unreachable,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        else => |err| return std.posix.unexpectedErrno(err),
    }
}

pub fn close(fd: std.posix.fd_t) void {
    switch (std.os.linux.errno(std.os.linux.close(fd))) {
        .SUCCESS => {},
        .BADF => unreachable,
        .INTR => {},
        else => {},
    }
}

pub fn milliTimestamp() i64 {
    var ts: std.os.linux.timespec = undefined;
    switch (std.os.linux.errno(std.os.linux.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => return @as(i64, ts.sec) * 1000 + @divTrunc(ts.nsec, std.time.ns_per_ms),
        else => return 0,
    }
}

pub fn nanoTimestamp() i128 {
    var ts: std.os.linux.timespec = undefined;
    switch (std.os.linux.errno(std.os.linux.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec,
        else => return 0,
    }
}

pub const PosixIoError = error{
    WouldBlock,
    ConnectionResetByPeer,
    BrokenPipe,
    NotOpenForWriting,
    NotOpenForReading,
    NetworkUnreachable,
    ConnectionRefused,
    ConnectionPending,
    ConnectionTimedOut,
    AddressInUse,
    AddressNotAvailable,
    AddressFamilyNotSupported,
    AccessDenied,
    PermissionDenied,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    ProtocolNotSupported,
    SocketTypeNotSupported,
} || std.posix.UnexpectedError;

fn mapErr(err: std.os.linux.E) PosixIoError {
    return switch (err) {
        .AGAIN => error.WouldBlock,
        .CONNRESET => error.ConnectionResetByPeer,
        .PIPE => error.BrokenPipe,
        .NETUNREACH, .HOSTUNREACH => error.NetworkUnreachable,
        .CONNREFUSED => error.ConnectionRefused,
        .INPROGRESS, .ALREADY => error.ConnectionPending,
        .TIMEDOUT => error.ConnectionTimedOut,
        .ADDRINUSE => error.AddressInUse,
        .ADDRNOTAVAIL => error.AddressNotAvailable,
        .AFNOSUPPORT => error.AddressFamilyNotSupported,
        .ACCES => error.AccessDenied,
        .PERM => error.PermissionDenied,
        .MFILE => error.ProcessFdQuotaExceeded,
        .NFILE => error.SystemFdQuotaExceeded,
        .NOMEM, .NOBUFS => error.SystemResources,
        .PROTONOSUPPORT => error.ProtocolNotSupported,
        .SOCKTNOSUPPORT => error.SocketTypeNotSupported,
        else => std.posix.unexpectedErrno(err),
    };
}

pub fn read(fd: std.posix.fd_t, buf: []u8) PosixIoError!usize {
    const rc = std.os.linux.read(fd, buf.ptr, buf.len);
    return switch (std.os.linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .BADF => error.NotOpenForReading,
        else => |err| mapErr(err),
    };
}

pub fn write(fd: std.posix.fd_t, bytes: []const u8) PosixIoError!usize {
    const rc = std.os.linux.write(fd, bytes.ptr, bytes.len);
    return switch (std.os.linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .BADF => error.NotOpenForWriting,
        else => |err| mapErr(err),
    };
}

pub fn socket(domain: u32, socket_type: u32, protocol: u32) PosixIoError!std.posix.fd_t {
    const rc = std.os.linux.socket(domain, socket_type, protocol);
    return switch (std.os.linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .INVAL => unreachable,
        else => |err| mapErr(err),
    };
}

pub fn bind(fd: std.posix.fd_t, addr: *const std.posix.sockaddr, len: std.posix.socklen_t) PosixIoError!void {
    const rc = std.os.linux.bind(fd, addr, len);
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => return,
        .BADF, .INVAL, .NOTSOCK => unreachable,
        else => |err| return mapErr(err),
    }
}

pub fn listen(fd: std.posix.fd_t, backlog: u31) PosixIoError!void {
    const rc = std.os.linux.listen(fd, backlog);
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => return,
        .BADF, .DESTADDRREQ, .INVAL, .NOTSOCK, .OPNOTSUPP => unreachable,
        else => |err| return mapErr(err),
    }
}

pub fn connect(fd: std.posix.fd_t, addr: *const std.posix.sockaddr, len: std.posix.socklen_t) PosixIoError!void {
    const rc = std.os.linux.connect(fd, addr, len);
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => return,
        .BADF, .FAULT, .NOTSOCK => unreachable,
        else => |err| return mapErr(err),
    }
}

pub fn accept(fd: std.posix.fd_t, addr: *std.posix.sockaddr, len: *std.posix.socklen_t, flags: u32) PosixIoError!std.posix.fd_t {
    const rc = std.os.linux.accept4(fd, addr, len, flags);
    return switch (std.os.linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .BADF, .FAULT, .INVAL, .NOTSOCK, .OPNOTSUPP => unreachable,
        else => |err| mapErr(err),
    };
}

pub fn recv(fd: std.posix.fd_t, buf: []u8, flags: u32) PosixIoError!usize {
    var addr: std.posix.sockaddr = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    const rc = std.os.linux.recvfrom(fd, buf.ptr, buf.len, flags, &addr, &len);
    return switch (std.os.linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .BADF, .FAULT, .INVAL, .NOTSOCK => unreachable,
        else => |err| mapErr(err),
    };
}

pub fn sendto(fd: std.posix.fd_t, buf: []const u8, flags: u32, addr: *const std.posix.sockaddr, len: std.posix.socklen_t) PosixIoError!usize {
    const rc = std.os.linux.sendto(fd, buf.ptr, buf.len, flags, addr, len);
    return switch (std.os.linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .BADF, .FAULT, .INVAL, .NOTSOCK => unreachable,
        else => |err| mapErr(err),
    };
}

pub const ShutdownHow = enum(u2) { recv = 0, send = 1, both = 2 };

pub fn shutdown(fd: std.posix.fd_t, how: ShutdownHow) PosixIoError!void {
    const rc = std.os.linux.shutdown(fd, @intFromEnum(how));
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => return,
        .BADF, .INVAL, .NOTSOCK => unreachable,
        else => |err| return mapErr(err),
    }
}

pub fn getsockoptError(fd: std.posix.fd_t) PosixIoError!void {
    var err_code: i32 = 0;
    var size: std.posix.socklen_t = @sizeOf(i32);
    const rc = std.os.linux.getsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.ERROR, @ptrCast(&err_code), &size);
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => switch (@as(std.os.linux.E, @enumFromInt(err_code))) {
            .SUCCESS => return,
            else => |err| return mapErr(err),
        },
        .BADF, .FAULT, .INVAL, .NOPROTOOPT, .NOTSOCK => unreachable,
        else => |err| return mapErr(err),
    }
}

pub fn sleep(nanoseconds: u64) void {
    var req = std.os.linux.timespec{
        .sec = @intCast(nanoseconds / std.time.ns_per_s),
        .nsec = @intCast(nanoseconds % std.time.ns_per_s),
    };
    while (std.os.linux.errno(std.os.linux.nanosleep(&req, &req)) == .INTR) {}
}

pub fn getsockname(fd: std.posix.fd_t, addr: *std.posix.sockaddr, len: *std.posix.socklen_t) PosixIoError!void {
    const rc = std.os.linux.getsockname(fd, addr, len);
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => return,
        .BADF, .FAULT, .INVAL, .NOTSOCK => unreachable,
        else => |err| return mapErr(err),
    }
}

pub fn sendmsg(fd: std.posix.fd_t, msg: *const std.posix.msghdr_const, flags: u32) PosixIoError!usize {
    const rc = std.os.linux.sendmsg(fd, msg, flags);
    return switch (std.os.linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .BADF, .FAULT, .INVAL, .NOTSOCK => unreachable,
        else => |err| mapErr(err),
    };
}

pub fn makePath(path: []const u8) !void {
    return std.Io.Dir.cwd().createDirPath(io(), path);
}

pub fn deleteFile(path: []const u8) void {
    std.Io.Dir.cwd().deleteFile(io(), path) catch {};
}

pub fn recvfrom(fd: std.posix.fd_t, buf: []u8, flags: u32, addr: *std.posix.sockaddr, len: *std.posix.socklen_t) PosixIoError!usize {
    const rc = std.os.linux.recvfrom(fd, buf.ptr, buf.len, flags, addr, len);
    return switch (std.os.linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .BADF, .FAULT, .INVAL, .NOTSOCK => unreachable,
        else => |err| mapErr(err),
    };
}
