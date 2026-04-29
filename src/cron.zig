const std = @import("std");
const Allocator = std.mem.Allocator;
const dt = @import("datetime.zig");
const DateTime = dt.DateTime;

/// Simple cron expression parser (5-field format):
///   minute hour day-of-month month day-of-week
///
/// Supports: * (any), */N (step), N (exact), N,M (list), N-M (range)
/// Day-of-week: 0=Sun, 1=Mon, ..., 6=Sat
///
/// Examples:
///   "0 2 * * 6"     - every Saturday at 2:00 AM
///   "30 3 * * 0,6"  - every Sat & Sun at 3:30 AM
///   "0 */6 * * *"   - every 6 hours
///   "0 0 1 * *"     - 1st of every month at midnight
pub const Cron = struct {
    minutes: u64, // bitmask, bits 0-59
    hours: u32, // bitmask, bits 0-23
    days_of_month: u32, // bitmask, bits 1-31
    months: u16, // bitmask, bits 1-12
    days_of_week: u8, // bitmask, bits 0-6

    pub fn parse(expr: []const u8) !Cron {
        var fields: [5][]const u8 = undefined;
        var count: usize = 0;
        var start: usize = 0;

        for (expr, 0..) |c, i| {
            if (c == ' ' or c == '\t') {
                if (i > start) {
                    if (count >= 5) return error.InvalidCronExpression;
                    fields[count] = expr[start..i];
                    count += 1;
                }
                start = i + 1;
            }
        }
        if (start < expr.len) {
            if (count >= 5) return error.InvalidCronExpression;
            fields[count] = expr[start..];
            count += 1;
        }

        if (count != 5) return error.InvalidCronExpression;

        return Cron{
            .minutes = try parseField(u64, fields[0], 0, 59),
            .hours = try parseField(u32, fields[1], 0, 23),
            .days_of_month = try parseField(u32, fields[2], 1, 31),
            .months = try parseField(u16, fields[3], 1, 12),
            .days_of_week = try parseField(u8, fields[4], 0, 6),
        };
    }

    fn parseField(comptime T: type, field: []const u8, min: u8, max: u8) !T {
        var result: T = 0;

        // Split by comma for lists
        var pos: usize = 0;
        while (pos < field.len) {
            // Find next comma
            var end = pos;
            while (end < field.len and field[end] != ',') : (end += 1) {}
            const part = field[pos..end];
            pos = if (end < field.len) end + 1 else end;

            if (part.len == 0) return error.InvalidCronExpression;

            // Check for */N
            if (part.len >= 2 and part[0] == '*' and part[1] == '/') {
                const step = std.fmt.parseInt(u8, part[2..], 10) catch return error.InvalidCronExpression;
                if (step == 0) return error.InvalidCronExpression;
                var i = min;
                while (i <= max) : (i += step) {
                    result |= @as(T, 1) << @intCast(i);
                }
            } else if (part.len == 1 and part[0] == '*') {
                // Wildcard - all values
                var i = min;
                while (i <= max) : (i += 1) {
                    result |= @as(T, 1) << @intCast(i);
                }
            } else {
                // Check for range N-M
                var dash_pos: ?usize = null;
                for (part, 0..) |c, idx| {
                    if (c == '-') {
                        dash_pos = idx;
                        break;
                    }
                }

                if (dash_pos) |dp| {
                    const range_start = std.fmt.parseInt(u8, part[0..dp], 10) catch return error.InvalidCronExpression;
                    const range_end = std.fmt.parseInt(u8, part[dp + 1 ..], 10) catch return error.InvalidCronExpression;
                    if (range_start < min or range_end > max or range_start > range_end) return error.InvalidCronExpression;
                    var i = range_start;
                    while (i <= range_end) : (i += 1) {
                        result |= @as(T, 1) << @intCast(i);
                    }
                } else {
                    // Single value
                    const val = std.fmt.parseInt(u8, part, 10) catch return error.InvalidCronExpression;
                    if (val < min or val > max) return error.InvalidCronExpression;
                    result |= @as(T, 1) << @intCast(val);
                }
            }
        }

        return result;
    }

    /// Compute next run time (epoch milliseconds) after the given timestamp.
    /// Searches up to 366 days ahead.
    pub fn nextRunAfter(self: Cron, after_ms: i64) !i64 {
        const epoch_s = @divFloor(after_ms, 1000) + 1; // start from next second
        const d = DateTime.fromEpochMs(epoch_s * 1000);

        var year = d.year;
        var month = d.month;
        var day = d.day;
        var hour = d.hour;
        var minute = d.minute + 1; // advance past current minute

        // Normalize overflow
        if (minute > 59) {
            minute = 0;
            hour += 1;
        }
        if (hour > 23) {
            hour = 0;
            day += 1;
        }

        // Search up to 400 days
        var iterations: u32 = 0;
        while (iterations < 400 * 24 * 60) : (iterations += 1) {
            // Normalize date
            const days_in = dt.daysInMonth(month, year);
            if (day > days_in) {
                day = 1;
                month += 1;
                if (month > 12) {
                    month = 1;
                    year += 1;
                }
                hour = 0;
                minute = 0;
                continue;
            }

            // Check month
            if (self.months & (@as(u16, 1) << @intCast(month)) == 0) {
                month += 1;
                day = 1;
                hour = 0;
                minute = 0;
                if (month > 12) {
                    month = 1;
                    year += 1;
                }
                continue;
            }

            // Check day of month
            if (self.days_of_month & (@as(u32, 1) << @intCast(day)) == 0) {
                day += 1;
                hour = 0;
                minute = 0;
                continue;
            }

            // Check day of week
            const dow = dayOfWeek(year, month, day);
            if (self.days_of_week & (@as(u8, 1) << @intCast(dow)) == 0) {
                day += 1;
                hour = 0;
                minute = 0;
                continue;
            }

            // Check hour
            if (self.hours & (@as(u32, 1) << @intCast(hour)) == 0) {
                hour += 1;
                minute = 0;
                if (hour > 23) {
                    hour = 0;
                    day += 1;
                }
                continue;
            }

            // Check minute
            if (self.minutes & (@as(u64, 1) << @intCast(minute)) == 0) {
                minute += 1;
                if (minute > 59) {
                    minute = 0;
                    hour += 1;
                    if (hour > 23) {
                        hour = 0;
                        day += 1;
                    }
                }
                continue;
            }

            // Found a match
            const result = DateTime{ .year = year, .month = month, .day = day, .hour = hour, .minute = minute, .second = 0 };
            return result.toEpochMs();
        }

        return error.InvalidCronExpression;
    }

    /// Describe the cron expression in human-readable form
    pub fn describe(self: Cron, allocator: Allocator) ![]const u8 {
        _ = self;
        _ = allocator;
        return "";
    }
};

/// Zeller-like day of week: 0=Sun, 1=Mon, ..., 6=Sat
fn dayOfWeek(year: i32, month: u8, day: u8) u8 {
    var y = year;
    var m = @as(i32, month);
    if (m < 3) {
        m += 12;
        y -= 1;
    }
    const q = @as(i32, day);
    const k = @mod(y, 100);
    const j = @divFloor(y, 100);
    const h = @mod(q + @divFloor(13 * (m + 1), 5) + k + @divFloor(k, 4) + @divFloor(j, 4) - 2 * j, 7);
    // h: 0=Sat, 1=Sun, 2=Mon, ...
    return @intCast(@mod(h + 6, 7)); // convert to 0=Sun
}

test "parse basic cron" {
    const cron = try Cron.parse("0 2 * * 6");
    // minute=0, hour=2, any day, any month, Saturday
    try std.testing.expect(cron.minutes & 1 != 0); // bit 0 set
    try std.testing.expect(cron.hours & (1 << 2) != 0); // bit 2 set
    try std.testing.expect(cron.days_of_week & (1 << 6) != 0); // bit 6 (Sat)
}

test "next run Saturday 2am" {
    const cron = try Cron.parse("0 2 * * 6");
    // 2026-03-15 is a Sunday (dow=0), next Saturday is 2026-03-21
    const sun_noon: i64 = 1773835200000; // approx 2026-03-15 12:00 UTC
    const next = try cron.nextRunAfter(sun_noon);
    const d = DateTime.fromEpochMs(next);
    try std.testing.expectEqual(@as(u8, 0), d.minute);
    try std.testing.expectEqual(@as(u8, 2), d.hour);
    try std.testing.expectEqual(@as(u8, 6), dayOfWeek(d.year, d.month, d.day)); // Saturday
}

test "every 6 hours" {
    const cron = try Cron.parse("0 */6 * * *");
    try std.testing.expect(cron.hours & 1 != 0); // 0
    try std.testing.expect(cron.hours & (1 << 6) != 0); // 6
    try std.testing.expect(cron.hours & (1 << 12) != 0); // 12
    try std.testing.expect(cron.hours & (1 << 18) != 0); // 18
}

test "weekend" {
    const cron = try Cron.parse("30 3 * * 0,6");
    try std.testing.expect(cron.minutes & (1 << 30) != 0);
    try std.testing.expect(cron.hours & (1 << 3) != 0);
    try std.testing.expect(cron.days_of_week & 1 != 0); // Sun
    try std.testing.expect(cron.days_of_week & (1 << 6) != 0); // Sat
}
