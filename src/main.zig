const std = @import("std");

pub fn main(init: std.process.Init) !void {
    _ = init;
    std.debug.print("utils library - import as a dependency, no standalone executable.\n", .{});
}
