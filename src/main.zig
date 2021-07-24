const std = @import("std");
const ring = @import("FileRing.zig");

pub fn main() anyerror!void {
    var r = try ring.FileRing.init("/tmp/file-ring-test", std.heap.page_allocator);
    defer r.deinit();

    var args: TestReadArgs = .{ .done = .{}, .buffer = undefined };

    try r.read(0, 12, test_read_callback, @ptrToInt(&args));

    args.done.wait();
    std.log.debug("got: {s}", .{args.buffer});
}

const TestReadArgs = struct {
    done: std.Thread.StaticResetEvent,
    buffer: []u8,
};

fn test_read_callback(res: anyerror![]u8, user_data: u64) void {
    var args = @intToPtr(*TestReadArgs, user_data);
    args.buffer = res catch unreachable;
    args.done.set();
}
