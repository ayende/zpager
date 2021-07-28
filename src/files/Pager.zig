const std = @import("std");
const lazy = @import("../Lazy.zig");
const files = @import("Ring.zig");
const utils = @import("../utils.zig");

const testing = std.testing;

const Megabytes = 1024 * 1024;

pub const MemoryLimits = struct {
    global_hard: u64,
    global_soft: u64,
    self_hard: u64,
    self_soft: u64,

    pub fn simple(limit: u64) MemoryLimits {
        return .{ .global_hard = limit, .global_soft = limit, .self_hard = limit, .self_soft = limit };
    }

    pub fn hard(self: *MemoryLimits) u64 {
        return std.math.max(self.global_hard, self.self_hard);
    }

    pub fn soft(self: *MemoryLimits) u64 {
        return std.math.max(self.global_soft, self.self_soft);
    }
};

pub const FilePager = struct {
    pub const PageSize = 8192; // 8 KB
    const BlockSize = 1024 * 1024 * 2; // 2 MB, huge page size
    const IoRingQueueSize = 32;
    const MaxFileSize = 4 * 1024 * 1024 * 1024; // 4GB
    const NumberOfBlocks = (MaxFileSize / BlockSize);
    const BlockMapLengthInBytes = NumberOfBlocks * @sizeOf(lazy.Lazy([*]u8)); // 32 KB for 2048 entries for 2MB each
    const MaxNumberOfPages = MaxFileSize / PageSize;
    const NumberOfAccessGenerations = 4;

    size_used: std.atomic.Atomic(u64),
    limits: *MemoryLimits,
    map: []lazy.Lazy([*]u8),
    allocator: *std.mem.Allocator,
    file: *files.FileRing,
    accessed: [NumberOfAccessGenerations]utils.atomic_bitmap,
    disjoint_rwl: utils.read_write_lock,
    disjoint_map: std.AutoHashMap(u64, *lazy.Lazy([*]u8)),
    current_access_idx: usize,
    bitmapBuffer: []u64,

    pub fn init(path: []const u8, limits: *MemoryLimits, allocator: *std.mem.Allocator) !*FilePager {
        const self = try allocator.create(FilePager);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.size_used = std.atomic.Atomic(u64).init(0);
        const single_bitmap_size = (NumberOfBlocks / @bitSizeOf(u64));
        self.bitmapBuffer = try allocator.alloc(u64, single_bitmap_size * NumberOfAccessGenerations);
        errdefer allocator.free(self.bitmapBuffer);
        std.mem.set(u64, self.bitmapBuffer, 0);

        self.current_access_idx = 0;
        for (self.accessed) |*access, i| {
            access.data = self.bitmapBuffer[i..(i * single_bitmap_size + single_bitmap_size)];
        }

        self.disjoint_map = std.AutoHashMap(u64, *lazy.Lazy([*]u8)).init(self.allocator);
        self.disjoint_rwl.init();

        self.map = try allocator.alloc(lazy.Lazy([*]u8), NumberOfBlocks);
        errdefer allocator.free(self.map);
        @memset(@ptrCast([*]u8, self.map.ptr), 0, BlockMapLengthInBytes);

        self.file = try files.FileRing.init(path, allocator);
        self.limits = limits;
        return self;
    }

    pub fn deinit(self: *FilePager) void {
        defer self.allocator.destroy(self);
        defer self.allocator.free(self.bitmapBuffer);
        defer self.allocator.free(self.map);
        defer self.file.deinit();
    }

    pub fn let_go(self: *FilePager, page_num: u64, number_of_pages: u32) void {
        _ = number_of_pages;
        const block_num = page_num / BlockSize;
        self.map[block_num].release();
    }

    pub fn try_page(self: *FilePager, page_num: u64, number_of_pages: u32) !?[]const u8 {
        const block_num = page_num / BlockSize;
        const page_in_block = page_num % BlockSize;

        const end_block_num = (page_num + number_of_pages) / BlockSize;
        if (end_block_num != block_num) { // this is a disjoint read
            // if (try try_page(block_num * BlockSize, 1) == null)
            //     return null;// we *require* that the parent
            var held = self.disjoint_rwl.reader();
            defer held.release();

            if (self.disjoint_map.get(page_num)) |p| {
                if (p.has_value() == false)
                    return null;
                var block = try p.get();
                return block[page_in_block * PageSize .. PageSize * number_of_pages];
            }
        }

        if (self.map[block_num].has_value() == false) {
            if (self.map[block_num].should_init()) {
                try self.read_from_disk(block_num); // start loading it in the background
            }
            return null;
        }
        var block = try self.map[block_num].get();
        return block[page_in_block * PageSize .. PageSize * number_of_pages];
    }

    fn complete_read(res: anyerror![]u8, user_data: u64) void {
        var state = @intToPtr(*ReadData, user_data);
        defer state.self.allocator.destroy(state);
        var buf = res catch |err| {
            state.block.opps(err);
            return;
        };
        std.debug.assert(buf.len == BlockSize);
        state.block.init(buf.ptr);
    }

    const ReadData = struct {
        self: *FilePager,
        block: *lazy.Lazy([*]u8),
    };

    pub fn evict(self: *FilePager) !usize {
        var free = try std.ArrayList(usize).initCapacity(self.allocator, NumberOfBlocks);
        defer free.deinit();

        if (self.size_used.loadUnchecked() < self.limits.self_soft)
            return 0; // nothing to do, we are below the limits

        for (self.map) |l, i| {
            var cur = l.data;
            if (cur.data.references == 1 and cur.data.val != null) {
                try free.append(i);
            }
        }
        std.sort.sort(usize, free.items, self, sort_blocks_by_references);

        var freed: usize = 0;
        var idx: usize = 0;
        while (idx < free.items.len) : (idx += 1) {
            var cur = self.map[free.items[idx]];
            if (self.map[free.items[idx]].reset() == false)
                continue;
            if (cur.data.data.val) |val| {
                self.allocator.free(val[0..BlockSize]);
                freed += BlockSize;
                var current_used = @atomicRmw(u64, &self.size_used.value, .Sub, BlockSize, .SeqCst) - BlockSize;
                if (current_used < self.limits.self_soft)
                    return freed; // returned enough
            }
        }
        return freed;
    }

    const ranking_by_usage: [16]u8 = .{ 0, 1, 2, 3, 4, 6, 7, 9, 5, 8, 10, 12, 11, 14, 15, 16 };

    fn usage_count(self: *FilePager, block_num: usize) usize {
        var val: usize = 0;
        var idx: u6 = 0;
        while (idx < NumberOfAccessGenerations) : (idx += 1) {
            var in_use = self.accessed[(idx + self.current_access_idx) % NumberOfAccessGenerations].get(block_num);
            if (in_use) {
                val |= (@as(usize, 1) << (NumberOfAccessGenerations - idx));
            }
        }
        return ranking_by_usage[val];
    }

    fn sort_blocks_by_references(self: *FilePager, left: usize, right: usize) bool {
        return self.usage_count(left) < self.usage_count(right);
    }

    fn read_from_disk(self: *FilePager, block_num: u64) !void {
        var current_used = @atomicRmw(u64, &self.size_used.value, .Add, BlockSize, .SeqCst) + BlockSize;
        if (current_used > self.limits.soft()) {
            _ = try self.evict();
            if (self.size_used.load(.SeqCst) > self.limits.hard()) {
                _ = @atomicRmw(u64, &self.size_used.value, .Sub, BlockSize, .SeqCst); // remove the allocation record
                return error.OutOfMemory;
            }
        }

        errdefer |err| {
            self.map[block_num].opps(err);
        }
        var state = try self.allocator.create(ReadData);
        errdefer self.allocator.destroy(state);
        state.self = self;
        state.block = &self.map[block_num];
        try self.file.read(block_num * BlockSize, BlockSize, complete_read, @ptrToInt(state));
    }

    fn get_disjointed(self: *FilePager, page_num: u64, number_of_pages: u32) ![]const u8 {
        { // read lock scope
            var rheld = self.disjoint_rwl.reader();
            defer rheld.release();
            if (self.disjoint_map.get(page_num)) |p| {
                if (p.has_value()) {
                    var buf = try p.get();
                    return buf[0 .. number_of_pages * PageSize];
                }
            }
        }

        var should_init: bool = undefined;

        var lazy_val: *lazy.Lazy([*]u8) = undefined;
        { // write lock scope
            var wheld = self.disjoint_rwl.writer();
            defer wheld.release();
            if (self.disjoint_map.get(page_num)) |p| { // already here, someone else intiailizing...
                should_init = false;
                lazy_val = p;
            } else {
                lazy_val = try self.allocator.create(lazy.Lazy([*]u8));
                errdefer self.allocator.destroy(lazy_val);
                lazy_val.data.data = .{ .version = 1, .references = 0, .val = null };
                try self.disjoint_map.put(page_num, lazy_val);
                should_init = true;
            }
        }
        if (should_init) {
            try self.file.read(page_num * PageSize, number_of_pages * PageSize, complete_read, @ptrToInt(lazy_val));
        }
        var buf = try lazy_val.get();
        return buf[0 .. number_of_pages * PageSize];
    }

    pub fn get_page(self: *FilePager, page_num: u64, number_of_pages: u32) ![]const u8 {
        std.debug.assert(number_of_pages == 1); // for now
        const block_num = page_num / BlockSize;
        const page_in_block = page_num % BlockSize;
        const end_block_num = (page_num + number_of_pages) / BlockSize;
        if (end_block_num != block_num) { // this is a disjoint read
            return self.get_disjointed(page_num, number_of_pages);
        }

        if (self.map[block_num].should_init()) {
            try self.read_from_disk(block_num);
        }

        var block = try self.map[block_num].get();
        self.accessed[self.current_access_idx % NumberOfAccessGenerations].set(page_num);
        return block[page_in_block * PageSize .. PageSize * number_of_pages];
    }
};

pub fn test_setup_env() !void {
    var file = try std.fs.createFileAbsolute("/tmp/file-pager-test", .{ .truncate = true });
    defer file.close();
    try file.setEndPos(8 * Megabytes);
    try file.writeAll("hello world\n");
    try file.seekTo(2 * Megabytes);
    try file.writeAll("hello world\n");
}

pub fn test_can_read_values_from_page() !void {
    var limits = MemoryLimits.simple(2 * Megabytes);
    var pager = try FilePager.init("/tmp/file-pager-test", &limits, std.heap.page_allocator);
    defer pager.deinit();

    try testing.expect((try pager.try_page(0, 1)) == null);
    var page = try pager.get_page(0, 1);
    try testing.expect(page.len == FilePager.PageSize);
    try testing.expect(std.mem.eql(u8, "hello world\n", page[0..12 :0]));
}

pub fn test_will_refuse_to_use_too_much_mem() !void {
    var limits = MemoryLimits.simple(2 * Megabytes);
    var pager = try FilePager.init("/tmp/file-pager-test", &limits, std.heap.page_allocator);
    defer pager.deinit();

    var page = try pager.get_page(0, 1);
    try testing.expect(pager.size_used.loadUnchecked() == 2 * Megabytes);
    page = try pager.get_page(1, 1);
    try testing.expect(pager.size_used.loadUnchecked() == 2 * Megabytes); // same block, no extra mem

    try testing.expectError(error.OutOfMemory, pager.get_page(257, 1) catch |e| e);
}

test "will refuse to use too much mem" {
    try test_setup_env();
    try test_will_refuse_to_use_too_much_mem();
}

test "can read value by page" {
    try test_setup_env();
    try test_can_read_values_from_page();
}
