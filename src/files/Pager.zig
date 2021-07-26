const std = @import("std");
const lazy = @import("../Lazy.zig");
const files = @import("Ring.zig");
const utils = @import("../utils/AtomicBitmap.zig");

const testing = std.testing;

pub const MemoryLimits = struct {
    global_hard: u64,
    global_soft: u64,
    self_hard: u64,
    self_soft: u64,

    pub fn hard(self: *MemoryLimits) u64 {
        return std.math.max(self.global_hard, self.self_hard);
    }

    pub fn soft(self: *MemoryLimits) u64 {
        return std.math.max(self.global_soft, self.self_soft);
    }
};

pub const FilePager = struct {
    const PageSize = 8192; // 8 KB
    const BlockSize = 1024 * 1024 * 2; // 2 MB, huge page size
    const IoRingQueueSize = 32;
    const MaxFileSize = 4 * 1024 * 1024 * 1024; // 4GB
    const NumberOfBlocks = (MaxFileSize / BlockSize);
    const BlockMapLengthInBytes = NumberOfBlocks * @sizeOf(lazy.Lazy([*]u8)); // 32 KB for 2048 entries for 2MB each
    const MaxNumberOfPages = MaxFileSize / PageSize;
    const NumberOfAccessGenerations = 4;

    size_used: u64,
    limits: *MemoryLimits,
    map: []lazy.Lazy([*]u8),
    allocator: *std.mem.Allocator,
    file: *files.FileRing,
    accessed: [NumberOfAccessGenerations]utils.AtomicBitmap,
    current_access_idx: usize,

    pub fn init(path: []const u8, limits: *MemoryLimits, allocator: *std.mem.Allocator) !*FilePager {
        const self = try allocator.create(FilePager);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        var bitmapBuffer = try allocator.alloc(u64, (NumberOfBlocks / @bitSizeOf(u64)) * NumberOfAccessGenerations);
        errdefer allocator.free(bitmapBuffer);
        std.mem.set(u64, bitmapBuffer, 0);

        self.current_access_idx = 0;
        for (self.accessed) |*access, i| {
            access.data = bitmapBuffer[i..(i * NumberOfBlocks)];
        }

        self.map = try allocator.alloc(lazy.Lazy([*]u8), NumberOfBlocks);
        errdefer allocator.free(self.map);
        @memset(@ptrCast([*]u8, self.map.ptr), 0, BlockMapLengthInBytes);

        self.file = try files.FileRing.init(path, allocator);
        self.limits = limits;

        return self;
    }

    pub fn deinit(self: *FilePager) void {
        defer self.allocator.destroy(self);
        defer self.allocator.free(self.accessed.data);
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
        if (self.map[block_num].has_value() == false) {
            try read_page_from_disk(); // start loading it in the background
            return null;
        }
        var block = self.map[block_num].get();
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

    pub fn evict(self: *FilePager) !void {
        var free = try std.ArrayList(usize).initCapacity(self.allocator, NumberOfBlocks);
        defer free.deinit();

        for (self.map) |l, i| {
            if (l.data.data.references == 1) {
                try free.append(i);
            }
        }
        std.sort.sort(usize, free.items, self, sort_blocks_by_references);
    }

    fn usage_count(self: *FilePager, block_num: usize) usize {
        var usage: usize = 0;
        var gens: usize = 0;
        var idx: usize = 0;
        while (idx < NumberOfAccessGenerations) : (idx += 1) {
            var in_use = self.accessed[(idx + self.current_access_idx) % NumberOfAccessGenerations].get(block_num);
            if (in_use) {
                usage += 1;
                gens += 1;
            }
        }
        return usage + gens;
    }

    fn sort_blocks_by_references(self: *FilePager, left: usize, right: usize) bool {
        return self.usage_count(left) < self.usage_count(right);
    }

    fn read_page_from_disk(self: *FilePager, block_num: u64) !void {
        if (self.map[block_num].should_init() == false)
            return;

        if (self.size_used + BlockSize > self.limits.soft()) {
            try self.evict();
            if (self.size_used + BlockSize > self.limits.hard()) {
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

    pub fn get_page(self: *FilePager, page_num: u64, number_of_pages: u32) ![]const u8 {
        std.debug.assert(number_of_pages == 1); // for now
        const block_num = page_num / BlockSize;
        const page_in_block = page_num % BlockSize;
        try read_page_from_disk(self, block_num);
        var block = try self.map[block_num].get();
        self.accessed[self.current_access_idx % NumberOfAccessGenerations].set(page_num);
        return block[page_in_block * PageSize .. PageSize * number_of_pages];
    }
};
