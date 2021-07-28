const std = @import("std");
const files = @import("files/Pager.zig");
const testing = std.testing;

pub fn main() anyerror!void {
    try files.test_setup_env();
    try files.test_will_refuse_to_use_too_much_mem();
}
