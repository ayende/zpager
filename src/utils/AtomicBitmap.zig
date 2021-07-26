const std = @import("std");

pub const AtomicBitmap = @This();

data: []u64,

pub fn get(self: *AtomicBitmap, idx: u64) bool {
    var item_index = idx / 64;
    var bit_idx = @intCast(u6, idx % 64);

    return self.data[item_index] & (@as(u64, 1) << bit_idx) != 0;
}

pub fn set(self: *AtomicBitmap, idx: u64) void {
    var item_index = idx / 64;
    var bit_idx = @intCast(u6, idx % 64);
    var mask = @as(u64, 1) << bit_idx;
    var cur = self.data[item_index];
    if ((mask & cur) != 0)
        return; // already there

    _ = @atomicRmw(u64, &self.data[item_index], .Or, mask, .SeqCst);
}
