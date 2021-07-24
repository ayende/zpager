const std = @import("std");
const files = @import("files/Pager.zig");

pub fn main() anyerror!void {
    var pager = try files.FilePager.init("/tmp/file-ring-test", 1024 * 1024, std.heap.page_allocator);
    var i: usize = 0;
    while (i < 3) {
        i += 1;
        var page = try pager.get_page(0, 1);
        std.log.debug("{s}", .{page});
    }

    // var r = try files.FileRing.init("/tmp/file-ring-test", std.heap.page_allocator);
    // defer r.deinit();

    // var args: TestReadArgs = .{ .done = .{}, .buffer = undefined };

    // try r.read(0, 12, test_read_callback, @ptrToInt(&args));

    // args.done.wait();
    // std.log.debug("got: {s}", .{args.buffer});
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
