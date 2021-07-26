const std = @import("std");
const lazy = @import("../Lazy.zig");
const files = @import("Ring.zig");
const utils = @import("../utils.zig");

const testing = std.testing;

pub const FilePager = struct {
    const PageSize = 8192; // 8 KB
    const BlockSize = 1024 * 1024 * 2; // 2 MB, huge page size
    const IoRingQueueSize = 32;
    const MaxFileSize = 4 * 1024 * 1024 * 1024; // 4GB
    const BlockMapLength = (MaxFileSize / BlockSize);
    const BlockMapLengthInBytes = BlockMapLength * @sizeOf(lazy.Lazy([*]u8)); // 32 KB for 2048 entries for 2MB each
    const MaxNumberOfPages = MaxFileSize / PageSize;

    size_used: u64,
    size_max: u64,
    map: []lazy.Lazy([*]u8),
    allocator: *std.mem.Allocator,
    file: *files.FileRing,
    accessed: utils.atomic_bitmap,
    disjoint_rwl: utils.read_write_lock,
    disjoint_map: std.AutoHashMap(u64, *lazy.Lazy([*]u8)),

    pub fn init(path: []const u8, max_size: u64, allocator: *std.mem.Allocator) !*FilePager {
        const self = try allocator.create(FilePager);
        errdefer allocator.destroy(self);
        self.allocator = allocator;

        self.disjoint_map = std.AutoHashMap(u64, *lazy.Lazy([*]u8)).init(self.allocator);
        self.disjoint_rwl.init();
        self.accessed = .{ .data = try allocator.alloc(u64, MaxNumberOfPages) };
        errdefer allocator.free(self.accessed.data);

        self.map = try allocator.alloc(lazy.Lazy([*]u8), BlockMapLength);
        errdefer allocator.free(self.map);
        @memset(@ptrCast([*]u8, self.map.ptr), 0, BlockMapLengthInBytes);

        self.file = try files.FileRing.init(path, allocator);
        self.size_max = max_size;

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

        const end_block_num = (page_num + number_of_pages) / BlockSize;
        if (end_block_num != block_num) { // this is a disjoint read
            // if (try try_page(block_num * BlockSize, 1) == null)
            //     return null;// we *require* that the parent
            var held = self.disjoint_rwl.reader();
            defer held.release();

            if (self.disjoint_map.get(page_num)) |p| {
                if (p.has_value() == false)
                    return null;
                return p.get();
            }
        }

        if (self.map[block_num].has_value() == false)
            return null;
        var block = self.map[block_num].get();
        return block[page_in_block * PageSize .. PageSize * number_of_pages];
    }

    fn complete_read(res: anyerror![]u8, user_data: u64) void {
        var lazy_val = @intToPtr(*lazy.Lazy([*]u8), user_data);
        var buf = res catch {
            lazy_val.reset();
            unreachable; //TODO: error handling here
        };
        std.debug.assert(buf.len == BlockSize);
        lazy_val.init(buf.ptr);
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
            errdefer {
                self.map[block_num].reset();
            }
            try self.file.read(block_num * BlockSize, BlockSize, complete_read, @ptrToInt(&self.map[block_num]));
        }
        var block = try self.map[block_num].get();
        self.accessed.set(page_num);
        return block[page_in_block * PageSize .. PageSize * number_of_pages];
    }
};
