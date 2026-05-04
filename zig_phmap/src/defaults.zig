//! Default hashing and equality policies used by Auto* containers.

const std = @import("std");

pub fn defaultHash(comptime Key: type) fn (void, Key) u64 {
    return struct {
        fn hash(_: void, key: Key) u64 {
            if (Key == []const u8) return std.hash.Wyhash.hash(0, key);
            if (Key == u64) return key;
            if (Key == usize) return @intCast(key);
            if (Key == u32 or Key == u16 or Key == u8) return @intCast(key);
            if (Key == i64) return @bitCast(key);
            if (Key == isize) return @bitCast(@as(isize, key));
            if (Key == i32 or Key == i16 or Key == i8) return @bitCast(@as(i64, key));
            return std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
        }
    }.hash;
}

pub fn defaultEql(comptime Key: type) fn (void, Key, Key) bool {
    return struct {
        fn eql(_: void, a: Key, b: Key) bool {
            if (Key == []const u8) return std.mem.eql(u8, a, b);
            if (Key == u64 or Key == usize or Key == u32 or Key == u16 or Key == u8) return a == b;
            if (Key == i64 or Key == isize or Key == i32 or Key == i16 or Key == i8) return a == b;
            return std.meta.eql(a, b);
        }
    }.eql;
}
