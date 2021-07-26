const std = @import("std");
const files = @import("files/Pager.zig");

pub fn main() anyerror!void {
    // var limits = files.MemoryLimits {.global_hard = 1024 * 1024, .global_soft = 512 * 1024, .self_hard = 768 * 1024, .self_soft = 256*1024};
    // var pager = try files.FilePager.init("/tmp/file-ring-test", &limits, std.heap.page_allocator);
    // var i: usize = 0;
    // while (i < 3) {
    //     i += 1;
    //     var page = try pager.get_page(0, 1);
    //     std.log.debug("{s}", .{page});
    // }

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
