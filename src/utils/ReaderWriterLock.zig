const std = @import("std");

const ReaderWriterLock = @This();

state: State,

const State = extern union { raw: u64, parts: extern struct { readers: u32, writers: u32 } };

comptime {
    if (@sizeOf(State) != @sizeOf(u64)) {
        @compileError("Expected State to be exactly 64 bits");
    }
}

pub fn init(self: *ReaderWriterLock) void {
    self.state.raw = 0;
}

pub fn writer(self: *ReaderWriterLock) WriteHeld {
    while (true) {
        var cur = self.state;
        if (cur.parts.readers != 0) {
            std.Thread.Futex.wait(@ptrCast(*const std.atomic.Atomic(u32), &self.state.parts.readers), cur.parts.writers, null) catch {};
            continue;
        }
        if (cur.parts.writers != 0) {
            std.Thread.Futex.wait(@ptrCast(*const std.atomic.Atomic(u32), &self.state.parts.writers), cur.parts.writers, null) catch {};
            continue;
        }
        var updated = cur;
        updated.parts.writers += 1;
        if (@cmpxchgWeak(u64, &self.state.raw, cur.raw, updated.raw, .SeqCst, .SeqCst) == null)
            return WriteHeld{ .parent = self };
    }
}

pub const WriteHeld = struct {
    parent: *ReaderWriterLock,

    pub fn release(self: *WriteHeld) void {
        while (true) {
            var cur = self.parent.state;
            var updated = cur;
            updated.parts.writers -= 1;
            if (@cmpxchgWeak(u64, &self.parent.state.raw, cur.raw, updated.raw, .SeqCst, .SeqCst) != null)
                continue;
            if (updated.parts.writers == 0) {
                std.Thread.Futex.wake(@ptrCast(*const std.atomic.Atomic(u32), &self.parent.state.parts.writers), std.math.maxInt(u32));
            }
            return;
        }
    }
};

pub fn reader(self: *ReaderWriterLock) ReadHeld {
    while (true) {
        var cur = self.state;
        if (cur.parts.writers != 0) {
            std.Thread.Futex.wait(@ptrCast(*const std.atomic.Atomic(u32), &self.state.parts.writers), cur.parts.writers, null) catch {};
            continue;
        }
        var updated = cur;
        updated.parts.readers += 1;
        if (@cmpxchgWeak(u64, &self.state.raw, cur.raw, updated.raw, .SeqCst, .SeqCst) == null)
            return ReadHeld{ .parent = self };
    }
}

pub const ReadHeld = struct {
    parent: *ReaderWriterLock,

    pub fn release(self: *ReadHeld) void {
        while (true) {
            var cur = self.parent.state;
            var updated = cur;
            updated.parts.readers -= 1;
            if (@cmpxchgWeak(u64, &self.parent.state.raw, cur.raw, updated.raw, .SeqCst, .SeqCst) != null)
                continue;
            if (updated.parts.readers == 0) {
                std.Thread.Futex.wake(@ptrCast(*const std.atomic.Atomic(u32), &self.parent.state.parts.readers), std.math.maxInt(u32));
            }
            return;
        }
    }
};
