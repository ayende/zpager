const std = @import("std");

pub fn Lazy(comptime T: type) type {
    return struct {
        const Reference = extern struct { val: ?T, references: u32, version: u32 };
        const AtomicRef = extern union { data: Reference, raw: u128 };

        const SelfLazy = @This();

        data: AtomicRef,

        comptime {
            if (@sizeOf(T) != @sizeOf(usize)) {
                @compileError("Lazy can only accept pointer size types");
            }

            if (@sizeOf(Reference) != @sizeOf(u128)) {
                @compileError("Lazy ref *must* be 128bits excatly");
            }
        }

        pub fn should_init(self: *SelfLazy) bool {
            var cur = self.data;
            if (cur.data.val != null) {
                return false; //initialized already
            }
            if (cur.data.version != 0)
                return false; // someone else is initalizing
            var update = AtomicRef{ .raw = 0 };
            update.data.version = 1;
            var result = @cmpxchgWeak(u128, &self.data.raw, cur.raw, update.raw, .Monotonic, .Monotonic);
            return result == null; // only if won the update, need to do the actual initialization
        }

        pub fn has_value(self: *SelfLazy) bool {
            return self.data.data.val != null;
        }

        pub fn release(self: *SelfLazy) void {
            std.debug.assert(self.data.val != null);
            while (true) {
                var cur: SelfLazy = self.*;
                var update = cur;
                update.data.version +%= 1;
                update.data.references -= 1;
                var result = @cmpxchgWeak(u128, self, cur.raw, update.raw, .Monotonic, .Monotonic);
                if (result == null)
                    break; // successfully updated
            }
        }

        pub fn reset(self: *SelfLazy) void {
            while (true) {
                var cur = self.data;
                var update = std.mem.zeroes(AtomicRef);
                var result = @cmpxchgWeak(u128, &self.data.raw, cur.raw, update.raw, .Monotonic, .Monotonic);
                if (result == null)
                    break;
            }
        }

        pub fn get(self: *SelfLazy) !T {
            while (true) {
                var cur = self.data;
                if (cur.data.val) |val| {
                    var update = cur;
                    update.data.version +%= 1;
                    update.data.references += 1;
                    var result = @cmpxchgWeak(u128, &self.data.raw, cur.raw, update.raw, .Monotonic, .Monotonic);
                    if (result != null)
                        continue;
                    return val; // we have new reference...
                }
                try std.Thread.Futex.wait(@ptrCast(*std.atomic.Atomic(u32), &self.data.data.references), 0, null);
            }
        }

        pub fn init(self: *SelfLazy, val: T) void {
            while (true) {
                var cur: SelfLazy = self.*;
                std.debug.assert(cur.data.data.val == null); // only single set is allowed
                var atomicRef: AtomicRef = .{ .data = .{ .references = 1, .version = 1, .val = val } };
                var result = @cmpxchgWeak(u128, &self.data.raw, cur.data.raw, atomicRef.raw, .Monotonic, .Monotonic);
                if (result != null)
                    continue; // fail to initialize, retry
                std.Thread.Futex.wake(@ptrCast(*std.atomic.Atomic(u32), &self.data.data.references), std.math.maxInt(u32));
                break;
            }
        }
    };
}
