const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;

// Simple spinlock - works from any thread without needing std.Io.
const SpinLock = struct {
    locked: std.atomic.Value(bool) = .init(false),

    fn lock(self: *SpinLock) void {
        while (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {}
    }

    fn unlock(self: *SpinLock) void {
        self.locked.store(false, .release);
    }
};

pub const AppLogger = struct {
    spin: SpinLock,
    io: Io,
    file: ?File,
    path: [512]u8,
    path_len: usize,
    max_size: u64,
    max_files: u32,
    write_offset: u64,
    buf: [65536]u8,
    buf_pos: usize,

    pub fn setup(self: *AppLogger, io: Io, path: []const u8, max_size_mb: u32, max_files: u32) void {
        self.spin = .{};
        self.io = io;
        self.file = null;
        self.buf_pos = 0;
        self.write_offset = 0;
        self.max_size = @as(u64, max_size_mb) * 1024 * 1024;
        self.max_files = max_files;

        const n = @min(path.len, self.path.len);
        @memcpy(self.path[0..n], path[0..n]);
        self.path_len = n;

        const f = Dir.openFile(.cwd(), io, path, .{ .mode = .read_write }) catch
            Dir.createFile(.cwd(), io, path, .{ .truncate = false }) catch return;
        self.file = f;

        const st = f.stat(io) catch return;
        self.write_offset = st.size;
    }

    pub fn deinit(self: *AppLogger) void {
        self.spin.lock();
        defer self.spin.unlock();
        self.flushLocked();
        if (self.file) |f| {
            f.close(self.io);
            self.file = null;
        }
    }

    fn flushLocked(self: *AppLogger) void {
        if (self.buf_pos == 0) return;
        if (self.file) |f| {
            f.writePositionalAll(self.io, self.buf[0..self.buf_pos], self.write_offset) catch {};
            self.write_offset += self.buf_pos;
        }
        self.buf_pos = 0;
        if (self.write_offset >= self.max_size) self.rotateLocked();
    }

    fn rotateLocked(self: *AppLogger) void {
        if (self.file) |f| {
            f.close(self.io);
            self.file = null;
        }

        const base = self.path[0..self.path_len];
        var buf1: [520]u8 = undefined;
        var buf2: [520]u8 = undefined;

        if (std.fmt.bufPrint(&buf1, "{s}.{d}", .{ base, self.max_files })) |oldest| {
            Dir.deleteFile(.cwd(), self.io, oldest) catch {};
        } else |_| {}

        var i: u32 = self.max_files;
        while (i > 1) {
            i -= 1;
            const from = std.fmt.bufPrint(&buf1, "{s}.{d}", .{ base, i }) catch continue;
            const to = std.fmt.bufPrint(&buf2, "{s}.{d}", .{ base, i + 1 }) catch continue;
            Dir.rename(.cwd(), from, .cwd(), to, self.io) catch {};
        }

        if (std.fmt.bufPrint(&buf1, "{s}.1", .{base})) |to1| {
            Dir.rename(.cwd(), base, .cwd(), to1, self.io) catch {};
        } else |_| {}

        const new_file = Dir.createFile(.cwd(), self.io, base, .{ .truncate = true }) catch return;
        self.file = new_file;
        self.write_offset = 0;
    }

    pub fn write(self: *AppLogger, comptime _: std.log.Level, data: []const u8) void {
        self.spin.lock();
        defer self.spin.unlock();
        if (self.file == null) return;

        if (self.buf_pos + data.len > self.buf.len) self.flushLocked();

        if (data.len <= self.buf.len) {
            @memcpy(self.buf[self.buf_pos..][0..data.len], data);
            self.buf_pos += data.len;
        } else {
            if (self.file) |f| {
                f.writePositionalAll(self.io, data, self.write_offset) catch {};
                self.write_offset += data.len;
            }
        }

        self.flushLocked();
    }
};

pub fn parseLevel(s: []const u8) std.log.Level {
    if (std.mem.eql(u8, s, "err") or std.mem.eql(u8, s, "error")) return .err;
    if (std.mem.eql(u8, s, "warn") or std.mem.eql(u8, s, "warning")) return .warn;
    if (std.mem.eql(u8, s, "debug")) return .debug;
    return .info;
}
