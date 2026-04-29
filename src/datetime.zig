const std = @import("std");

/// UTC date-time with second precision.
pub const DateTime = struct {
    year: i32,
    month: u8, // 1–12
    day: u8, // 1–31
    hour: u8 = 0, // 0–23
    minute: u8 = 0, // 0–59
    second: u8 = 0, // 0–59

    /// Convert to milliseconds since Unix epoch (1970-01-01T00:00:00Z).
    pub fn toEpochMs(self: DateTime) i64 {
        return dateTimeToEpochS(self.year, self.month, self.day, self.hour, self.minute, self.second) * 1000;
    }

    /// Create from milliseconds since Unix epoch.
    pub fn fromEpochMs(ms: i64) DateTime {
        return epochSToDateTime(@divFloor(ms, 1000));
    }

    /// Format as ISO 8601 UTC string: "2024-01-15T10:30:00Z"
    pub fn formatIso(self: DateTime, allocator: std.mem.Allocator) ![]const u8 {
        return std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
            self.year, self.month, self.day, self.hour, self.minute, self.second,
        });
    }
};

/// Parse an ISO 8601 date or datetime string to epoch milliseconds.
///
/// Supported formats:
///   "2024-01-15"              → midnight UTC
///   "2024-01-15T10:30:00Z"   → full datetime UTC
///   "2024-01-15T10:30:00"    → treated as UTC (Z optional)
///   "2024-01-15 10:30:00"    → space separator also accepted
///
/// Returns epoch milliseconds (i64), suitable for BSON datetime.
pub fn parseIso(s: []const u8) !i64 {
    if (s.len < 10) return error.InvalidDateFormat;

    // Parse date part: YYYY-MM-DD
    const year = std.fmt.parseInt(i32, s[0..4], 10) catch return error.InvalidDateFormat;
    if (s[4] != '-') return error.InvalidDateFormat;
    const month = std.fmt.parseInt(u8, s[5..7], 10) catch return error.InvalidDateFormat;
    if (s[7] != '-') return error.InvalidDateFormat;
    const day = std.fmt.parseInt(u8, s[8..10], 10) catch return error.InvalidDateFormat;

    if (month < 1 or month > 12) return error.InvalidDateFormat;
    if (day < 1 or day > daysInMonth(month, year)) return error.InvalidDateFormat;

    var hour: u8 = 0;
    var minute: u8 = 0;
    var second: u8 = 0;

    // Parse optional time part: Thh:mm:ss or ' hh:mm:ss'
    if (s.len >= 19 and (s[10] == 'T' or s[10] == ' ')) {
        hour = std.fmt.parseInt(u8, s[11..13], 10) catch return error.InvalidDateFormat;
        if (s[13] != ':') return error.InvalidDateFormat;
        minute = std.fmt.parseInt(u8, s[14..16], 10) catch return error.InvalidDateFormat;
        if (s[16] != ':') return error.InvalidDateFormat;
        second = std.fmt.parseInt(u8, s[17..19], 10) catch return error.InvalidDateFormat;
    } else if (s.len != 10) {
        // Not pure date and not full datetime
        return error.InvalidDateFormat;
    }

    return dateTimeToEpochS(year, month, day, hour, minute, second) * 1000;
}

// ========== Internal calendar math ==========

pub fn isLeapYear(year: i32) bool {
    if (@mod(year, 400) == 0) return true;
    if (@mod(year, 100) == 0) return false;
    return @mod(year, 4) == 0;
}

pub fn daysInMonth(month: u8, year: i32) u8 {
    const days = [_]u8{ 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (month == 2 and isLeapYear(year)) return 29;
    return days[month];
}

fn dateTimeToEpochS(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8) i64 {
    var total_days: i64 = 0;

    var y: i32 = 1970;
    while (y < year) : (y += 1) {
        total_days += if (isLeapYear(y)) @as(i64, 366) else @as(i64, 365);
    }

    var m: u8 = 1;
    while (m < month) : (m += 1) {
        total_days += @as(i64, daysInMonth(m, year));
    }

    total_days += @as(i64, day) - 1;

    return total_days * 86400 + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
}

fn epochSToDateTime(epoch_s: i64) DateTime {
    var days = @divFloor(epoch_s, 86400);
    var remaining = @mod(epoch_s, 86400);
    if (remaining < 0) {
        days -= 1;
        remaining += 86400;
    }

    const hour: u8 = @intCast(@divFloor(remaining, 3600));
    remaining = @mod(remaining, 3600);
    const minute: u8 = @intCast(@divFloor(remaining, 60));
    const second: u8 = @intCast(@mod(remaining, 60));

    var year: i32 = 1970;
    while (true) {
        const diy: i64 = if (isLeapYear(year)) 366 else 365;
        if (days < diy) break;
        days -= diy;
        year += 1;
    }

    var month: u8 = 1;
    while (month <= 12) {
        const dim = @as(i64, daysInMonth(month, year));
        if (days < dim) break;
        days -= dim;
        month += 1;
    }

    return .{
        .year = year,
        .month = month,
        .day = @intCast(days + 1),
        .hour = hour,
        .minute = minute,
        .second = second,
    };
}

// ========== Tests ==========

const testing = std.testing;

test "parseIso - date only" {
    const ms = try parseIso("2024-01-15");
    const dt = DateTime.fromEpochMs(ms);
    try testing.expectEqual(@as(i32, 2024), dt.year);
    try testing.expectEqual(@as(u8, 1), dt.month);
    try testing.expectEqual(@as(u8, 15), dt.day);
    try testing.expectEqual(@as(u8, 0), dt.hour);
    try testing.expectEqual(@as(u8, 0), dt.minute);
    try testing.expectEqual(@as(u8, 0), dt.second);
}

test "parseIso - full datetime with Z" {
    const ms = try parseIso("2024-01-15T10:30:45Z");
    const dt = DateTime.fromEpochMs(ms);
    try testing.expectEqual(@as(i32, 2024), dt.year);
    try testing.expectEqual(@as(u8, 1), dt.month);
    try testing.expectEqual(@as(u8, 15), dt.day);
    try testing.expectEqual(@as(u8, 10), dt.hour);
    try testing.expectEqual(@as(u8, 30), dt.minute);
    try testing.expectEqual(@as(u8, 45), dt.second);
}

test "parseIso - datetime without Z" {
    const ms = try parseIso("2024-01-15T10:30:45");
    const dt = DateTime.fromEpochMs(ms);
    try testing.expectEqual(@as(u8, 10), dt.hour);
    try testing.expectEqual(@as(u8, 45), dt.second);
}

test "parseIso - space separator" {
    const ms = try parseIso("2024-01-15 10:30:45");
    const dt = DateTime.fromEpochMs(ms);
    try testing.expectEqual(@as(u8, 10), dt.hour);
}

test "parseIso - epoch roundtrip" {
    // Known: 2024-01-15T00:00:00Z = 1705276800000 ms
    const ms = try parseIso("2024-01-15");
    try testing.expectEqual(@as(i64, 1705276800000), ms);
}

test "parseIso - rejects invalid" {
    try testing.expectError(error.InvalidDateFormat, parseIso("not-a-date"));
    try testing.expectError(error.InvalidDateFormat, parseIso("2024-13-01")); // month 13
    try testing.expectError(error.InvalidDateFormat, parseIso("2024-02-30")); // Feb 30
}

test "DateTime.toEpochMs and fromEpochMs roundtrip" {
    const dt = DateTime{ .year = 2024, .month = 6, .day = 15, .hour = 14, .minute = 30, .second = 59 };
    const ms = dt.toEpochMs();
    const back = DateTime.fromEpochMs(ms);
    try testing.expectEqual(dt.year, back.year);
    try testing.expectEqual(dt.month, back.month);
    try testing.expectEqual(dt.day, back.day);
    try testing.expectEqual(dt.hour, back.hour);
    try testing.expectEqual(dt.minute, back.minute);
    try testing.expectEqual(dt.second, back.second);
}

test "DateTime.formatIso" {
    const dt = DateTime{ .year = 2024, .month = 1, .day = 5, .hour = 9, .minute = 3, .second = 7 };
    const s = try dt.formatIso(testing.allocator);
    defer testing.allocator.free(s);
    try testing.expectEqualStrings("2024-01-05T09:03:07Z", s);
}

test "leap year Feb 29" {
    const ms = try parseIso("2024-02-29");
    const dt = DateTime.fromEpochMs(ms);
    try testing.expectEqual(@as(u8, 29), dt.day);
    try testing.expectEqual(@as(u8, 2), dt.month);
}

test "non-leap year Feb 29 rejected" {
    try testing.expectError(error.InvalidDateFormat, parseIso("2023-02-29"));
}
