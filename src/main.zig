const std = @import("std");
const IO_Uring = std.os.linux.IO_Uring;
const testing = std.testing;

const Pager = struct {
    const FileIndex = 0; // we only use a single registered file per rings
    const PageSize = 8192; // 8 KB
    const BlockSize = 1024 * 1024 * 2; // 2 MB, huge page size
    const IoRingQueueSize = 32;

    const ReadPageReq = struct {
        page_num: u64,
        number_of_pages: u32,
        node: *std.atomic.Queue(*ReadPageReq).Node,
        frame: anyframe,
        err: ?anyerror,
        buffer: []u8,
    };

    const BlockDetails = packed union { data: packed struct {
        ptr: [*]u8,
        references: u32,
        version: u16,
        dirty: bool,
    }, raw: u128 };

    ring: IO_Uring,
    size_used: u64,
    size_max: u64,
    file: std.fs.File,
    map: []BlockDetails,
    allocator: *std.mem.Allocator,
    submitter: std.Thread,
    completer: std.Thread,
    executer: std.Thread,
    work_submitted: std.Thread.ResetEvent,
    work_completed: std.Thread.ResetEvent,
    work_ready: std.Thread.ResetEvent,
    submittions: std.atomic.Queue(*ReadPageReq),
    executions: std.atomic.Queue(*ReadPageReq),
    running: bool,
    got_error: anyerror,

    pub fn init(path: []const u8, max_size: u64, allocator: *std.mem.Allocator) !*Pager {
        const self = try allocator.create(Pager);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.size_used = 0;
        self.size_max = max_size;

        try self.work_submitted.init();
        errdefer self.work_submitted.deinit();
        try self.work_completed.init();
        errdefer self.work_completed.deinit();
        try self.work_ready.init();
        errdefer self.work_ready.deinit();

        self.executions = std.atomic.Queue(*ReadPageReq).init();
        self.submittions = std.atomic.Queue(*ReadPageReq).init();

        self.file = try std.fs.createFileAbsolute(path, .{ .truncate = false, .read = true });
        errdefer self.file.close();

        self.ring = try IO_Uring.init(IoRingQueueSize, 0);
        errdefer self.ring.deinit();

        const fds = [_]std.os.fd_t{self.file.handle};
        try self.ring.register_files(&fds);

        var stats = try self.file.stat();
        if (stats.size < BlockSize) {
            try self.file.setEndPos(BlockSize);
            stats = try self.file.stat();
        }
        var pages = (stats.size / BlockSize);
        if (stats.size % BlockSize != 0) {
            pages += 1;
        }
        self.map = try allocator.alloc(BlockDetails, pages);
        std.mem.set(u128, @bitCast([]u128, self.map), 0);
        self.running = true;
        self.submitter = try std.Thread.spawn(.{}, struct {
            pub fn callback(me: *Pager) void {
                if (submit_work(me)) {} else |err| {
                    me.got_error = err;
                }
            }
        }.callback, .{self});
        self.completer = try std.Thread.spawn(.{}, struct {
            pub fn callback(me: *Pager) void {
                if (complete_work(me)) {} else |err| {
                    me.got_error = err;
                }
            }
        }.callback, .{self});
        self.executer = try std.Thread.spawn(.{}, struct {
            pub fn callback(me: *Pager) void {
                _ = execute_work(me) catch undefined;
            }
        }.callback, .{self});
        return self;
    }

    pub fn deinit(self: *Pager) void {
        @atomicStore(bool, &self.running, false, .Release);
        self.work_submitted.set();
        self.work_completed.set();
        self.work_ready.set();
        self.submitter.join();
        self.completer.join();
        self.ring.deinit();
        self.file.close();
        self.work_submitted.deinit();
        self.work_completed.deinit();
        self.work_ready.deinit();
        self.allocator.free(self.map);
    }

    pub fn page(self: *Pager, page_num: u64, number_of_pages: u32) ![]const u8 {
        std.debug.assert(number_of_pages == 1); // for now
        const block_num = page_num / BlockSize;
        const page_in_block = page_num % BlockSize;
        while (true) {
            const block = self.map[block_num];
            if (block.data.references != 0) {
                var updated = block;
                updated.data.references += 1;
                updated.data.version +%= 1; // just need to be different
                // try increment the ref count safely, update the
                var result = @cmpxchgWeak(u128, &self.map[block_num].raw, block.raw, updated.raw, .Monotonic, .Monotonic);
                if (result == null) { // successfully replaced the value, can return the page back
                    return block.data.ptr[page_in_block * PageSize .. PageSize * number_of_pages];
                }
                continue; // raced with someone else?
            }
            try self.load_page(block_num, number_of_pages);
        }
    }

    fn load_page(self: *Pager, block_num: u64, number_of_pages: u32) !void {
        var req = try self.allocator.create(ReadPageReq);
        errdefer self.allocator.destroy(req);
        req.number_of_pages = number_of_pages;
        req.page_num = block_num * PageSize;
        req.err = null;
        req.buffer = undefined;

        var node = try self.allocator.create(std.atomic.Queue(*ReadPageReq).Node);
        defer self.allocator.destroy(node);
        node.prev = undefined;
        node.next = undefined;
        node.data = req;
        req.node = node;

        suspend {
            req.frame = @frame();
            self.submittions.put(node);
            self.work_submitted.set();
        }

        if (req.err) |err| {
            return err;
        }
        while (true) {
            const block = self.map[block_num];
            if (block.data.references != 0) {
                self.allocator.free(req.buffer); // free the buffer
                return; // someone else already loaded this?
            }
            var updated = block;
            updated.data.ptr = req.buffer.ptr;
            updated.data.references = 1;
            updated.data.version +%= 1;

            var result = @cmpxchgWeak(u128, &self.map[block_num].raw, block.raw, updated.raw, .Monotonic, .Monotonic);
            if (result == null) { // successfully replaced the value, can return the page back
                return;
            }
            // failed to update? someone else did it first, retry...
        }
    }

    fn complete_work(self: *Pager) !void {
        var cqes: []std.os.linux.io_uring_cqe = try self.allocator.alloc(std.os.linux.io_uring_cqe, IoRingQueueSize);
        defer self.allocator.free(cqes);

        var wait_nr: u32 = 1;
        while (true) {
            const read = try self.ring.copy_cqes(cqes[0..], wait_nr);
            if (read == 0) // if we read 0, means we are done...
                return;
            self.work_completed.set();
            for (cqes[0..read]) |cqe| {
                if (cqe.user_data == 0) {
                    wait_nr = 0;
                    continue; // probably closing nop
                }

                var req = @intToPtr(*ReadPageReq, cqe.user_data);
                if (cqe.res < 0) {
                    self.allocator.free(req.buffer);
                    req.buffer = undefined;
                    req.err = switch (-cqe.res) {
                        0 => undefined,
                        9 => error.InvalidFileDescriptor,
                        else => error.unexpectedError,
                    };
                }
                self.executions.put(req.node);
            }
            self.work_ready.set();
        }
    }

    fn execute_work(self: *Pager) !void {
        while (true) {
            if (@atomicLoad(bool, &self.running, .Acquire)) {
                self.work_ready.wait();
                self.work_ready.reset();
            } else {
                return;
            }
            while (self.executions.get()) |work| {
                resume work.data.frame;
            }
        }
    }

    fn get_number_of_blocks(number_of_pages: u64) u64 {
        const size_in_bytes = number_of_pages * PageSize;
        var size_in_blocks: usize = (size_in_bytes / BlockSize);
        if (size_in_bytes % BlockSize != 0) {
            size_in_blocks += 1;
        }
        return size_in_blocks;
    }

    fn submit_work(self: *Pager) !void {
        defer {
            // release anything still waiting
            while (self.submittions.get()) |work| {
                var req: *ReadPageReq = work.data;
                req.err = error.RingShuttingDown;
                resume req.frame;
                self.allocator.destroy(work);
            }
            _ = self.ring.nop(0) catch undefined; // free the completer thread
            _ = self.ring.submit() catch undefined; // ignore the errors
        }

        while (@atomicLoad(bool, &self.running, .Acquire)) {
            self.work_submitted.wait();
            self.work_submitted.reset();

            while (self.submittions.get()) |work| { // read work from queue and submit to ring
                var req: *ReadPageReq = work.data;
                const len: usize = BlockSize * get_number_of_blocks(req.number_of_pages);
                req.buffer = try self.allocator.alloc(u8, len);
                errdefer self.allocator.free(req.buffer);
                const iovecs = [_]std.os.iovec{std.os.iovec{ .iov_base = req.buffer.ptr, .iov_len = req.buffer.len }};
                const sqe = self.ring.readv(@ptrToInt(req), FileIndex, iovecs[0..], 0) catch |err| switch (err) {
                    error.SubmissionQueueFull => {
                        self.submittions.unget(work);
                        self.work_submitted.set();
                        break; // sumbit immediately, we are full
                    },
                };
                sqe.flags |= std.os.linux.IOSQE_FIXED_FILE;
            }
            _ = self.ring.submit() catch |err| switch (err) {
                error.CompletionQueueOvercommitted => {
                    self.work_completed.wait();
                    self.work_completed.reset();
                    self.work_submitted.set(); // so we'll retry
                },
                error.SignalInterrupt => self.work_submitted.set(), // need to retry
                else => return err,
            };
        }
    }
};

fn async_main(done: *std.Thread.ResetEvent) anyerror!void {
    defer done.set();
    var pager = try Pager.init("/tmp/hello", 16 * 1024 * 1024, std.heap.page_allocator);
    defer pager.deinit();
    var pageFrame = async pager.page(0, 1);
    var page = try await pageFrame;
    std.log.debug("page {s}", .{page});
}

pub fn main() anyerror!void {
    var done: std.Thread.ResetEvent = undefined;
    try std.Thread.ResetEvent.init(&done);
    defer done.deinit();
    _ = async async_main(&done);
    done.wait();
}
