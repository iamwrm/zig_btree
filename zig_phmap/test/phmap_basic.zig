const std = @import("std");
const phmap = @import("phmap");

test "flat map basic operations" {
    var map = phmap.AutoFlatHashMap(u64, u64).init(std.testing.allocator);
    defer map.deinit();

    try std.testing.expect(map.isEmpty());
    try std.testing.expect((try map.put(1, 10)).inserted);
    try std.testing.expect(!(try map.put(1, 11)).inserted);
    try std.testing.expectEqual(@as(u64, 11), map.get(1).?.*);
    try std.testing.expect(map.contains(1));
    try std.testing.expect(!map.contains(2));
    try std.testing.expect(map.remove(1));
    try std.testing.expect(!map.remove(1));
    try std.testing.expectEqual(@as(usize, 0), map.len());
    map.validate();
}

test "flat set basic operations" {
    var set = phmap.AutoFlatHashSet(u64).init(std.testing.allocator);
    defer set.deinit();

    try std.testing.expect(try set.insert(10));
    try std.testing.expect(!try set.insert(10));
    try std.testing.expect(set.contains(10));
    try std.testing.expect(set.remove(10));
    try std.testing.expect(!set.contains(10));
    set.validate();
}

test "byte slice keys use content equality" {
    var map = phmap.AutoFlatHashMap([]const u8, u64).init(std.testing.allocator);
    defer map.deinit();

    try std.testing.expect((try map.put("alpha", 1)).inserted);
    try std.testing.expect(!(try map.put("alpha", 2)).inserted);
    try std.testing.expectEqual(@as(u64, 2), map.get("alpha").?.*);
}

test "group clones remain valid across boundary mutations" {
    var map = phmap.AutoFlatHashMap(u64, u64).init(std.testing.allocator);
    defer map.deinit();

    try map.ensureTotalCapacity(32);
    var i: u64 = 0;
    while (i < 64) : (i += 1) {
        _ = try map.put(i, i * 10);
    }
    map.validate();

    i = 0;
    while (i < 64) : (i += 3) {
        _ = map.remove(i);
    }
    map.validate();

    map.clearRetainingCapacity();
    map.validate();
    try std.testing.expectEqual(@as(usize, 0), map.len());
    try map.ensureTotalCapacity(128);
    map.validate();
}

test "high load misses and sparse/dense iteration" {
    var map = phmap.AutoFlatHashMap(u64, u64).init(std.testing.allocator);
    defer map.deinit();

    try map.ensureTotalCapacity(1800);
    var i: u64 = 0;
    while (i < 1800) : (i += 1) {
        _ = try map.put(i * 17, i);
    }
    map.validate();

    i = 0;
    while (i < 1800) : (i += 1) {
        try std.testing.expect(map.getConst((i * 17) ^ 0xaaaa_aaaa_aaaa_aaaa) == null);
    }

    var seen_dense: usize = 0;
    var it = map.constIterator();
    while (it.next()) |_| seen_dense += 1;
    try std.testing.expectEqual(map.len(), seen_dense);

    i = 0;
    while (i < 1800) : (i += 2) {
        _ = map.remove(i * 17);
    }
    map.validate();

    var seen_sparse: usize = 0;
    it = map.constIterator();
    while (it.next()) |_| seen_sparse += 1;
    try std.testing.expectEqual(map.len(), seen_sparse);
}

test "custom hash and equality context" {
    const Context = struct {
        salt: u64,
    };
    const Map = phmap.FlatHashMap(u64, u64, Context, struct {
        fn hash(ctx: Context, key: u64) u64 {
            return mix64((key / 10) ^ ctx.salt);
        }
    }.hash, struct {
        fn eql(_: Context, a: u64, b: u64) bool {
            return a / 10 == b / 10;
        }
    }.eql, .{});

    var map = Map.initContext(std.testing.allocator, .{ .salt = 0xabc });
    defer map.deinit();

    try std.testing.expect((try map.put(21, 1)).inserted);
    try std.testing.expect(!(try map.put(29, 2)).inserted);
    try std.testing.expectEqual(@as(u64, 2), map.get(20).?.*);
    try std.testing.expect(map.contains(28));
    try std.testing.expect(!map.contains(31));
    map.validate();
}

test "node map keeps value pointers stable across growth" {
    var map = phmap.AutoNodeHashMap(u64, u64).init(std.testing.allocator);
    defer map.deinit();

    _ = try map.put(1, 10);
    const ptr = map.get(1).?;
    try map.ensureTotalCapacity(1024);
    var i: u64 = 2;
    while (i < 1024) : (i += 1) {
        _ = try map.put(i, i);
    }

    try std.testing.expect(ptr == map.get(1).?);
    try std.testing.expectEqual(@as(u64, 10), ptr.*);
    try std.testing.expect(map.remove(1));
    map.validate();
}

test "node set basic operations" {
    var set = phmap.AutoNodeHashSet(u64).init(std.testing.allocator);
    defer set.deinit();

    try std.testing.expect(try set.insert(1));
    try std.testing.expect(!try set.insert(1));
    try std.testing.expect(set.contains(1));
    try std.testing.expect(set.remove(1));
    set.validate();
}

fn mix64(value: u64) u64 {
    return value *% 0x9e37_79b9_7f4a_7c15;
}
