const std = @import("std");
const phmap = @import("phmap");

const Map = phmap.AutoFlatHashMap(u64, u64);
const StringMap = phmap.AutoFlatHashMap([]const u8, u64);

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    const n: usize = 1_000_000;

    const keys = try allocator.alloc(u64, n);
    defer allocator.free(keys);
    fillKeys(keys);

    var map = Map.init(allocator);
    defer map.deinit();

    var last = nowNs();
    try map.ensureTotalCapacity(n);
    for (keys, 0..) |key, i| {
        _ = try map.put(key, @intCast(i));
    }
    const insert_ns = elapsedSince(&last);

    var checksum: u64 = 0;
    for (keys) |key| {
        if (map.getConst(key)) |value| checksum +%= value.*;
    }
    const lookup_ns = elapsedSince(&last);

    for (keys) |key| {
        if (map.getConst(key ^ 0xaaaa_aaaa_aaaa_aaaa)) |value| checksum +%= value.*;
    }
    const miss_ns = elapsedSince(&last);

    var iter_count: usize = 0;
    var it = map.constIterator();
    while (it.next()) |entry| {
        checksum +%= entry.key ^ entry.value;
        iter_count += 1;
    }
    const iterate_ns = elapsedSince(&last);

    const mixed_ns = blk: {
        var mixed = Map.init(allocator);
        defer mixed.deinit();
        try mixed.ensureTotalCapacity(n);
        var i: usize = 0;
        const start = nowNs();
        while (i < n) : (i += 1) {
            _ = try mixed.put(keys[i], @intCast(i));
            if (i >= 2) _ = mixed.getConst(keys[i - 2]);
            if (i % 3 == 0) _ = mixed.remove(keys[i / 3]);
        }
        break :blk nowNs() - start;
    };
    last = nowNs();

    for (keys) |key| {
        _ = map.remove(key);
    }
    const remove_ns = elapsedSince(&last);

    const string_n: usize = 100_000;
    const string_storage = try allocator.alloc([16]u8, string_n);
    defer allocator.free(string_storage);
    const string_keys = try allocator.alloc([]const u8, string_n);
    defer allocator.free(string_keys);
    fillStringKeys(string_storage, string_keys);

    var string_map = StringMap.init(allocator);
    defer string_map.deinit();
    last = nowNs();
    try string_map.ensureTotalCapacity(string_n);
    for (string_keys, 0..) |key, i| {
        _ = try string_map.put(key, @intCast(i));
    }
    const string_insert_ns = elapsedSince(&last);
    for (string_keys) |key| {
        if (string_map.getConst(key)) |value| checksum +%= value.*;
    }
    const string_lookup_ns = elapsedSince(&last);

    const high_n: usize = 1_800_000;
    const high_keys = try allocator.alloc(u64, high_n);
    defer allocator.free(high_keys);
    fillKeys(high_keys);
    var high_map = Map.init(allocator);
    defer high_map.deinit();
    try high_map.ensureTotalCapacity(high_n);
    for (high_keys, 0..) |key, i| {
        _ = try high_map.put(key, @intCast(i));
    }
    last = nowNs();
    for (high_keys) |key| {
        if (high_map.getConst(key ^ 0x5555_5555_5555_5555)) |value| checksum +%= value.*;
    }
    const high_load_miss_ns = elapsedSince(&last);

    const churn_n: usize = 500_000;
    var churn_map = Map.init(allocator);
    defer churn_map.deinit();
    try churn_map.ensureTotalCapacity(churn_n);
    last = nowNs();
    for (keys[0..churn_n], 0..) |key, i| {
        _ = try churn_map.put(key, @intCast(i));
    }
    for (keys[0..churn_n]) |key| {
        _ = churn_map.remove(key);
    }
    for (keys[0..churn_n], 0..) |key, i| {
        _ = try churn_map.put(key ^ 0x3333_3333_3333_3333, @intCast(i));
    }
    const tombstone_churn_ns = elapsedSince(&last);

    std.debug.print(
        \\items inserted: {}
        \\unique items: {}
        \\insert_reserved: {d:.3} ns/op
        \\lookup_hit:      {d:.3} ns/op
        \\lookup_miss:     {d:.3} ns/op
        \\iterate:         {d:.3} ns/item
        \\mixed:           {d:.3} ns/op
        \\remove:          {d:.3} ns/op
        \\string_insert:   {d:.3} ns/op
        \\string_lookup:   {d:.3} ns/op
        \\high_load_miss:  {d:.3} ns/op
        \\tombstone_churn: {d:.3} ns/op
        \\checksum: {}
        \\
    , .{
        n,
        iter_count,
        nsPerOp(insert_ns, n),
        nsPerOp(lookup_ns, n),
        nsPerOp(miss_ns, n),
        nsPerOp(iterate_ns, @max(iter_count, 1)),
        nsPerOp(mixed_ns, n),
        nsPerOp(remove_ns, n),
        nsPerOp(string_insert_ns, string_n),
        nsPerOp(string_lookup_ns, string_n),
        nsPerOp(high_load_miss_ns, high_n),
        nsPerOp(tombstone_churn_ns, churn_n * 3),
        checksum,
    });
}

fn fillKeys(keys: []u64) void {
    var state: u64 = 0x5eed_b7ee;
    for (keys) |*key| key.* = splitmix64(&state);
}

fn fillStringKeys(storage: [][16]u8, keys: [][]const u8) void {
    var state: u64 = 0x5171_9eed;
    for (storage, keys) |*bytes, *key| {
        const a = splitmix64(&state);
        const b = splitmix64(&state);
        @memcpy(bytes[0..8], std.mem.asBytes(&a));
        @memcpy(bytes[8..16], std.mem.asBytes(&b));
        key.* = bytes[0..];
    }
}

fn splitmix64(state: *u64) u64 {
    state.* +%= 0x9e37_79b9_7f4a_7c15;
    var z = state.*;
    z = (z ^ (z >> 30)) *% 0xbf58_476d_1ce4_e5b9;
    z = (z ^ (z >> 27)) *% 0x94d0_49bb_1331_11eb;
    return z ^ (z >> 31);
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
