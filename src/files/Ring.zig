const std = @import("std");
const IO_Uring = std.os.linux.IO_Uring;

pub const FileRing = struct {
    const FileIndex = 0; // we only use a single registered file per rings
    const IoRingQueueSize = 32;
    const OsPageSize = 4096;

    const ReadReqStack = std.atomic.Stack(*ReadPageReq);
    const CallbackFn = fn (anyerror![]u8, u64) void;

    const ReadPageReq = struct {
        position: u64,
        size: u32,
        buffer: []u8,
        iovec: [1]std.os.iovec,
        user_data: u64,
        callback: CallbackFn,
    };

    ring: IO_Uring,
    file: std.fs.File,
    allocator: *std.mem.Allocator,
    event_fd: i32,
    worker: std.Thread,
    background_error: ?anyerror,
    pending: ReadReqStack,
    running: bool,

    pub fn init(path: []const u8, allocator: *std.mem.Allocator) !*FileRing {
        var self = try allocator.create(FileRing);
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.pending = ReadReqStack.init();

        self.file = try std.fs.createFileAbsolute(path, .{ .truncate = false, .read = true });
        errdefer self.file.close();

        self.ring = try IO_Uring.init(IoRingQueueSize, 0);
        errdefer self.ring.deinit();

        const fds = [_]std.os.fd_t{self.file.handle};
        try self.ring.register_files(&fds);

        self.event_fd = try std.os.eventfd(0, 0);

        try self.ring.register_eventfd(self.event_fd);
        self.running = true;
        self.worker = try std.Thread.spawn(.{}, background_worker_wrapper, .{self});

        return self;
    }

    pub fn deinit(self: *FileRing) void {
        self.running = false;
        @fence(.Acquire);
        self.wake_worker() catch {};
        self.worker.join();
        self.ring.deinit();
        std.os.close(self.event_fd);
        self.file.close();
    }

    pub fn read(self: *FileRing, position: u64, size: u32, callback: CallbackFn, user_data: u64) !void {
        {
            var req = try self.allocator.create(ReadPageReq);
            errdefer self.allocator.destroy(req);
            req.position = position;
            req.size = size;
            req.callback = callback;
            req.user_data = user_data;

            var node = try self.allocator.create(ReadReqStack.Node);
            errdefer self.allocator.destroy(node);

            node.data = req;
            self.pending.push(node);
        }
        try self.wake_worker();
    }

    fn background_worker_wrapper(self: *FileRing) void {
        self.background_worker() catch |err| {
            self.background_error = err;
        };
    }

    fn wake_worker(self: *FileRing) !void {
        var buf = [_]u8{0} ** 8;
        buf[7] = 1;
        _ = try std.os.write(self.event_fd, buf[0..]);
    }

    fn wait_for_work(self: *FileRing) !void {
        var buf = [_]u8{0} ** 8;
        _ = try std.os.read(self.event_fd, buf[0..]);
    }

    fn submit_read(self: *FileRing, req: *ReadPageReq) !bool {
        const sqe = self.ring.readv(@ptrToInt(req), FileIndex, req.iovec[0..], 0) catch |err| switch (err) {
            error.SubmissionQueueFull => {
                var new_node = try self.allocator.create(std.atomic.Stack(*ReadPageReq).Node);
                new_node.data = req;
                self.pending.push(new_node);
                try self.wake_worker();
                return true; // sumbit immediately whatever we have...
            },
            else => return err,
        };
        sqe.flags |= std.os.linux.IOSQE_FIXED_FILE;
        return false;
    }

    fn handle_read_completion(self: *FileRing, cqe: *const std.os.linux.io_uring_cqe) !void {
        var req = @intToPtr(*ReadPageReq, cqe.user_data);
        if (cqe.res > 0 and cqe.res < req.iovec[0].iov_len) {
            // partial read, need to resubmit
            req.iovec[0].iov_base += @intCast(usize, cqe.res);
            req.iovec[0].iov_len -= @intCast(usize, cqe.res);
            _ = try self.submit_read(req);
            return;
        }
        defer self.allocator.destroy(req); // won't need the req after the call complete...

        if (cqe.res < 0) {
            self.allocator.free(req.buffer);
            req.buffer = undefined;
            var err = switch (-cqe.res) {
                9 => error.InvalidFileDescriptor,
                14 => error.ParamsOutsideAccessibleAddressSpace,
                else => {
                    std.log.debug("Unexpected ioring error {}", .{-cqe.res});
                    return error.unexpectedError;
                },
            };
            req.callback(err, req.user_data);
            return;
        }
        if (cqe.res == 0) { // unexpected end of file?
            req.callback(error.EndOfFile, req.user_data);
            return;
        }
        // only in tests will that be false
        if (std.mem.isAligned(@ptrToInt(req.buffer.ptr), OsPageSize) and std.mem.isAligned(req.buffer.len, OsPageSize)) {
            // we want to ensure that the only way to mutate this memory is through the ring API, not manual memory
            try std.os.mprotect(@alignCast(OsPageSize, req.buffer), std.os.PROT_READ);
        }
        // report done reading...
        req.callback(req.buffer, req.user_data);
    }

    fn background_worker(self: *FileRing) !void {
        var cqes: []std.os.linux.io_uring_cqe = try self.allocator.alloc(std.os.linux.io_uring_cqe, IoRingQueueSize);
        defer self.allocator.free(cqes);

        while (self.running) {
            try self.wait_for_work();

            while (self.pending.pop()) |node| {
                defer self.allocator.destroy(node);
                var req = node.data;
                req.buffer = try self.allocator.alloc(u8, req.size);
                errdefer self.allocator.free(req.buffer);
                req.iovec[0] = .{ .iov_base = req.buffer.ptr, .iov_len = req.buffer.len };
                if (try self.submit_read(req)) {
                    break; // need to submit immediately
                }
            }

            _ = self.ring.submit() catch |err| switch (err) {
                error.CompletionQueueOvercommitted => try self.wake_worker(),
                error.SignalInterrupt => try self.wake_worker(),
                else => return err,
            };

            // now let's process the completed values
            const n = try self.ring.copy_cqes(cqes[0..], 0);
            for (cqes[0..n]) |cqe| {
                try self.handle_read_completion(&cqe);
            }
        }
    }
};

test "can read file" {
    {
        var file = try std.fs.createFileAbsolute("/tmp/file-ring-test", .{ .truncate = true });
        defer file.close();
        try file.writeAll("hello world\n");
    }

    var ring = try FileRing.init("/tmp/file-ring-test", std.heap.page_allocator);
    defer ring.deinit();

    var args: TestReadArgs = .{ .done = .{}, .buffer = undefined };

    try ring.read(0, 12, test_read_callback, @ptrToInt(&args));

    try std.testing.expectEqual(std.Thread.ResetEvent.TimedWaitResult.event_set, args.done.timedWait(1_000));
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
