const std = @import("std");
const btree = @import("btree");

const Map = btree.AutoBTreeMapWithConfig(u64, u64, .{ .target_node_size = 256 });

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const n = 1_000_000;

    const keys = try allocator.alloc(u64, n);
    defer allocator.free(keys);

    var prng = std.Random.DefaultPrng.init(0x5eed_b7ee);
    for (keys) |*key| key.* = prng.random().int(u64);

    var map = Map.init(allocator);
    defer map.deinit();

    var last = nowNs();
    for (keys, 0..) |key, i| {
        _ = try map.put(key, @intCast(i));
    }
    const insert_ns = elapsedSince(&last);

    var checksum: u64 = 0;
    for (keys) |key| {
        if (map.get(key)) |value| checksum +%= value.*;
    }
    const lookup_ns = elapsedSince(&last);

    var iter_count: usize = 0;
    var it = map.iterator();
    while (it.next()) |entry| {
        checksum +%= entry.key ^ entry.value;
        iter_count += 1;
    }
    const iterate_ns = elapsedSince(&last);

    for (keys) |key| {
        _ = map.remove(key);
    }
    const remove_ns = elapsedSince(&last);

    std.debug.print(
        \\items inserted: {}
        \\unique items: {}
        \\insert:  {d:.3} ns/op
        \\lookup:  {d:.3} ns/op
        \\iterate: {d:.3} ns/item
        \\remove:  {d:.3} ns/op
        \\checksum: {}
        \\
    , .{
        n,
        iter_count,
        nsPerOp(insert_ns, n),
        nsPerOp(lookup_ns, n),
        nsPerOp(iterate_ns, @max(iter_count, 1)),
        nsPerOp(remove_ns, n),
        checksum,
    });
}

fn nsPerOp(ns: u64, ops: usize) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(ops));
}

fn elapsedSince(last: *u64) u64 {
    const current = nowNs();
    defer last.* = current;
    return current - last.*;
}

fn nowNs() u64 {
    var ts: std.os.linux.timespec = undefined;
    const rc = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    std.debug.assert(rc == 0);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}
