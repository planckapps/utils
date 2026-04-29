const std = @import("std");
const Io = std.Io;

pub const Now = struct {
    io: Io,

    /// Current wall-clock time in milliseconds.
    pub fn toMilliSeconds(self: Now) i64 {
        return Io.Clock.now(.real, self.io).toMilliseconds();
    }

    /// Current wall-clock time in seconds.
    pub fn toSeconds(self: Now) i64 {
        return Io.Clock.now(.real, self.io).toSeconds();
    }

    /// Current wall-clock time in nanoseconds.
    pub fn toNanoSeconds(self: Now) i96 {
        return Io.Clock.now(.real, self.io).toNanoseconds();
    }
};
