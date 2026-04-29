const std = @import("std");
const Io = std.Io;

pub const StopWatch = @import("utils").StopWatch;

pub const DbOperation = enum { Read, Write, Delete, Update, Flush };

pub const DbMetrics = struct {
    total_reads: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_writes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_deletes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_updates: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_flushes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    read_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    write_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    update_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    flush_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn start(self: *DbMetrics, io: Io, op: DbOperation) StopWatch {
        _ = self;
        _ = op;
        var sw = StopWatch{};
        sw.start(io);
        return sw;
    }

    pub fn stop(self: *DbMetrics, io: Io, sw: *StopWatch, op: DbOperation) void {
        sw.stop(io);
        const latency_us = @as(u64, @intFromFloat(sw.elapsedUs()));

        switch (op) {
            .Read => {
                _ = self.total_reads.fetchAdd(1, .monotonic);
                _ = self.read_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Write => {
                _ = self.total_writes.fetchAdd(1, .monotonic);
                _ = self.write_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Delete => {
                _ = self.total_deletes.fetchAdd(1, .monotonic);
            },
            .Update => {
                _ = self.total_updates.fetchAdd(1, .monotonic);
                _ = self.update_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Flush => {
                _ = self.total_flushes.fetchAdd(1, .monotonic);
                _ = self.flush_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
        }
    }
    pub fn getAvgReadLatency(self: *const DbMetrics) f64 {
        const count = self.total_reads.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.read_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }

    pub fn getAvgWriteLatency(self: *const DbMetrics) f64 {
        const count = self.total_writes.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.write_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }

    pub fn getAvgUpdateLatency(self: *const DbMetrics) f64 {
        const count = self.total_updates.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.update_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }

    pub fn getAvgFlushLatency(self: *const DbMetrics) f64 {
        const count = self.total_flushes.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.flush_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }
};

pub const WalOperation = enum {
    Append,
    Flush,
    Truncate,
    Fsync,
    Replay,
};

pub const WalMetrics = struct {
    total_appends: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_bytes_written: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_fsyncs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_replays: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_flushes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_truncates: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    append_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    fsync_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    flush_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    truncate_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn start(self: *WalMetrics, io: Io, op: WalOperation) StopWatch {
        _ = self;
        _ = op;
        var sw = StopWatch{};
        sw.start(io);
        return sw;
    }

    pub fn stop(self: *WalMetrics, io: Io, sw: *StopWatch, op: WalOperation) void {
        sw.stop(io);
        const latency_us = @as(u64, @intFromFloat(sw.elapsedUs()));

        switch (op) {
            .Append => {
                _ = self.total_appends.fetchAdd(1, .monotonic);
                _ = self.append_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Fsync => {
                _ = self.total_fsyncs.fetchAdd(1, .monotonic);
                _ = self.fsync_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Replay => {
                _ = self.total_replays.fetchAdd(1, .monotonic);
            },
            .Flush => {
                _ = self.total_flushes.fetchAdd(1, .monotonic);
                _ = self.flush_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Truncate => {
                _ = self.total_truncates.fetchAdd(1, .monotonic);
                _ = self.truncate_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
        }
    }

    pub fn recordBytes(self: *WalMetrics, bytes: u64) void {
        _ = self.total_bytes_written.fetchAdd(bytes, .monotonic);
    }

    pub fn getAvgAppendLatency(self: *const WalMetrics) f64 {
        const count = self.total_appends.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.append_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }

    pub fn getAvgFsyncLatency(self: *const WalMetrics) f64 {
        const count = self.total_fsyncs.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.fsync_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }

    pub fn getAvgFlushLatency(self: *const WalMetrics) f64 {
        const count = self.total_flushes.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.flush_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }

    pub fn getAvgTruncateLatency(self: *const WalMetrics) f64 {
        const count = self.total_truncates.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.truncate_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }
};

pub const VlogOperation = enum {
    Write,
    Read,
    Flush,
    Gc,
};

pub const VlogMetrics = struct {
    total_writes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_reads: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_bytes_written: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_gc_runs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    bytes_reclaimed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_flushes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    write_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    read_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    flush_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    pub fn start(self: *VlogMetrics, io: Io, op: VlogOperation) StopWatch {
        _ = self;
        _ = op;
        var sw = StopWatch{};
        sw.start(io);
        return sw;
    }

    pub fn stop(self: *VlogMetrics, io: Io, sw: *StopWatch, op: VlogOperation) void {
        sw.stop(io);
        const latency_us = @as(u64, @intFromFloat(sw.elapsedUs()));

        switch (op) {
            .Write => {
                _ = self.total_writes.fetchAdd(1, .monotonic);
                _ = self.write_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Read => {
                _ = self.total_reads.fetchAdd(1, .monotonic);
                _ = self.read_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Gc => {
                _ = self.total_gc_runs.fetchAdd(1, .monotonic);
            },
            .Flush => {
                _ = self.total_flushes.fetchAdd(1, .monotonic);
                _ = self.flush_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
        }
    }

    pub fn recordBytes(self: *VlogMetrics, bytes: u64) void {
        _ = self.total_bytes_written.fetchAdd(bytes, .monotonic);
    }

    pub fn recordGcReclaimed(self: *VlogMetrics, bytes: u64) void {
        _ = self.bytes_reclaimed.fetchAdd(bytes, .monotonic);
    }

    pub fn getAvgWriteLatency(self: *const VlogMetrics) f64 {
        const count = self.total_writes.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.write_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }

    pub fn getAvgReadLatency(self: *const VlogMetrics) f64 {
        const count = self.total_reads.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.read_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }
    pub fn getAvgFlushLatency(self: *const VlogMetrics) f64 {
        const count = self.total_flushes.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.flush_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }
};

pub const IndexOperation = enum {
    Insert,
    Update,
    Search,
    Delete,
    RangeScan,
    Flush,
};

pub const IndexMetrics = struct {
    total_inserts: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_searches: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_deletes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_scans: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_flushes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_updates: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    update_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    insert_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    search_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    flush_latency_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn start(self: *IndexMetrics, io: Io, op: IndexOperation) StopWatch {
        _ = self;
        _ = op;
        var sw = StopWatch{};
        sw.start(io);
        return sw;
    }

    pub fn stop(self: *IndexMetrics, io: Io, sw: *StopWatch, op: IndexOperation) void {
        sw.stop(io);
        const latency_us = @as(u64, @intFromFloat(sw.elapsedUs()));

        switch (op) {
            .Insert => {
                _ = self.total_inserts.fetchAdd(1, .monotonic);
                _ = self.insert_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Update => {
                _ = self.total_updates.fetchAdd(1, .monotonic);
                _ = self.update_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Search => {
                _ = self.total_searches.fetchAdd(1, .monotonic);
                _ = self.search_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
            .Delete => {
                _ = self.total_deletes.fetchAdd(1, .monotonic);
            },
            .RangeScan => {
                _ = self.total_scans.fetchAdd(1, .monotonic);
            },
            .Flush => {
                _ = self.total_flushes.fetchAdd(1, .monotonic);
                _ = self.flush_latency_sum_us.fetchAdd(latency_us, .monotonic);
            },
        }
    }

    pub fn getAvgInsertLatency(self: *const IndexMetrics) f64 {
        const count = self.total_inserts.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.insert_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }

    pub fn getAvgSearchLatency(self: *const IndexMetrics) f64 {
        const count = self.total_searches.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.search_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }

    pub fn getAvgFlushLatency(self: *const IndexMetrics) f64 {
        const count = self.total_flushes.load(.monotonic);
        if (count == 0) return 0.0;
        const sum = self.flush_latency_sum_us.load(.monotonic);
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
    }
};

pub const GcMetrics = struct {
    total_runs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_bytes_reclaimed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    last_run_duration_ms: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn recordRun(self: *GcMetrics, bytes_reclaimed: u64, duration_ms: u64) void {
        _ = self.total_runs.fetchAdd(1, .monotonic);
        _ = self.total_bytes_reclaimed.fetchAdd(bytes_reclaimed, .monotonic);
        self.last_run_duration_ms.store(duration_ms, .monotonic);
    }
};

pub const EngineMetrics = struct {
    db: DbMetrics = .{},
    wal: WalMetrics = .{},
    vlog: VlogMetrics = .{},
    index: IndexMetrics = .{},
    gc: GcMetrics = .{},

    pub fn init(allocator: std.mem.Allocator) !*EngineMetrics {
        const em = try allocator.create(EngineMetrics);
        em.* = .{};
        return em;
    }

    pub fn deinit(self: *EngineMetrics, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }
};

// ========== Tests ==========

const testing = std.testing;

test "DbMetrics - counters increment" {
    var m = DbMetrics{};
    try testing.expectEqual(@as(u64, 0), m.total_reads.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), m.total_writes.load(.monotonic));
    _ = m.total_reads.fetchAdd(1, .monotonic);
    _ = m.total_reads.fetchAdd(1, .monotonic);
    _ = m.total_writes.fetchAdd(1, .monotonic);
    try testing.expectEqual(@as(u64, 2), m.total_reads.load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.total_writes.load(.monotonic));
}

test "DbMetrics - avg latency zero when no ops" {
    const m = DbMetrics{};
    try testing.expectEqual(@as(f64, 0.0), m.getAvgReadLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgWriteLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgUpdateLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgFlushLatency());
}

test "DbMetrics - avg latency calculation" {
    var m = DbMetrics{};
    _ = m.total_reads.fetchAdd(2, .monotonic);
    _ = m.read_latency_sum_us.fetchAdd(100, .monotonic);
    try testing.expectEqual(@as(f64, 50.0), m.getAvgReadLatency());
}

test "WalMetrics - counters and bytes" {
    var m = WalMetrics{};
    _ = m.total_appends.fetchAdd(5, .monotonic);
    m.recordBytes(1024);
    m.recordBytes(2048);
    try testing.expectEqual(@as(u64, 5), m.total_appends.load(.monotonic));
    try testing.expectEqual(@as(u64, 3072), m.total_bytes_written.load(.monotonic));
}

test "WalMetrics - avg latency zero when no ops" {
    const m = WalMetrics{};
    try testing.expectEqual(@as(f64, 0.0), m.getAvgAppendLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgFsyncLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgFlushLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgTruncateLatency());
}

test "VlogMetrics - counters and bytes" {
    var m = VlogMetrics{};
    _ = m.total_writes.fetchAdd(3, .monotonic);
    _ = m.total_reads.fetchAdd(7, .monotonic);
    m.recordBytes(4096);
    m.recordGcReclaimed(512);
    try testing.expectEqual(@as(u64, 3), m.total_writes.load(.monotonic));
    try testing.expectEqual(@as(u64, 7), m.total_reads.load(.monotonic));
    try testing.expectEqual(@as(u64, 4096), m.total_bytes_written.load(.monotonic));
    try testing.expectEqual(@as(u64, 512), m.bytes_reclaimed.load(.monotonic));
}

test "VlogMetrics - avg latency zero when no ops" {
    const m = VlogMetrics{};
    try testing.expectEqual(@as(f64, 0.0), m.getAvgWriteLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgReadLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgFlushLatency());
}

test "IndexMetrics - counters" {
    var m = IndexMetrics{};
    _ = m.total_inserts.fetchAdd(10, .monotonic);
    _ = m.total_searches.fetchAdd(20, .monotonic);
    _ = m.total_deletes.fetchAdd(3, .monotonic);
    _ = m.total_scans.fetchAdd(5, .monotonic);
    try testing.expectEqual(@as(u64, 10), m.total_inserts.load(.monotonic));
    try testing.expectEqual(@as(u64, 20), m.total_searches.load(.monotonic));
    try testing.expectEqual(@as(u64, 3), m.total_deletes.load(.monotonic));
    try testing.expectEqual(@as(u64, 5), m.total_scans.load(.monotonic));
}

test "IndexMetrics - avg latency zero when no ops" {
    const m = IndexMetrics{};
    try testing.expectEqual(@as(f64, 0.0), m.getAvgInsertLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgSearchLatency());
    try testing.expectEqual(@as(f64, 0.0), m.getAvgFlushLatency());
}

test "GcMetrics - recordRun" {
    var m = GcMetrics{};
    m.recordRun(1024, 50);
    m.recordRun(2048, 75);
    try testing.expectEqual(@as(u64, 2), m.total_runs.load(.monotonic));
    try testing.expectEqual(@as(u64, 3072), m.total_bytes_reclaimed.load(.monotonic));
    try testing.expectEqual(@as(u64, 75), m.last_run_duration_ms.load(.monotonic));
}

test "EngineMetrics - init and deinit" {
    const allocator = testing.allocator;
    const em = try EngineMetrics.init(allocator);
    defer em.deinit(allocator);
    try testing.expectEqual(@as(u64, 0), em.db.total_reads.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), em.wal.total_appends.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), em.vlog.total_writes.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), em.index.total_inserts.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), em.gc.total_runs.load(.monotonic));
}
