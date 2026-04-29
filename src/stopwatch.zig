const std = @import("std");
const Io = std.Io;

/// High-resolution monotonic stopwatch using std.Io.Clock.
/// Pass `io` to start() and stop() - consistent with the Zig 0.16 Io-passing style.
///
/// Usage:
///   var sw = StopWatch{};
///   sw.start(io);
///   // ... work ...
///   sw.stop(io);
///   std.debug.print("elapsed: {d:.3} ms\n", .{sw.elapsedMs()});
///   sw.reset();
pub const StopWatch = struct {
    start_ns: ?i128 = null,
    elapsed_ns: i128 = 0,

    pub fn start(self: *StopWatch, io: Io) void {
        self.start_ns = Io.Clock.now(.awake, io).toNanoseconds();
    }

    pub fn stop(self: *StopWatch, io: Io) void {
        if (self.start_ns) |begin| {
            const now: i128 = Io.Clock.now(.awake, io).toNanoseconds();
            self.elapsed_ns += now - begin;
            self.start_ns = null;
        }
    }

    pub fn reset(self: *StopWatch) void {
        self.start_ns = null;
        self.elapsed_ns = 0;
    }

    /// Total elapsed nanoseconds across all start/stop pairs since last reset.
    pub fn elapsedNs(self: *const StopWatch) u64 {
        return @intCast(self.elapsed_ns);
    }

    /// Total elapsed microseconds (f64 for sub-µs precision).
    pub fn elapsedUs(self: *const StopWatch) f64 {
        return @as(f64, @floatFromInt(self.elapsed_ns)) / 1_000.0;
    }

    /// Total elapsed milliseconds.
    pub fn elapsedMs(self: *const StopWatch) f64 {
        return @as(f64, @floatFromInt(self.elapsed_ns)) / 1_000_000.0;
    }

    /// Total elapsed seconds.
    pub fn elapsedS(self: *const StopWatch) f64 {
        return @as(f64, @floatFromInt(self.elapsed_ns)) / 1_000_000_000.0;
    }
};
