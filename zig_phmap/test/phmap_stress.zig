const std = @import("std");
const phmap = @import("phmap");

test "randomized model test against std.AutoHashMap" {
    const Map = phmap.AutoFlatHashMap(u64, u64);
    var map = Map.init(std.testing.allocator);
    defer map.deinit();

    var model = std.AutoHashMap(u64, u64).init(std.testing.allocator);
    defer model.deinit();

    var prng = std.Random.DefaultPrng.init(0x1234_5678);
    const random = prng.random();

    var i: usize = 0;
    while (i < 50_000) : (i += 1) {
        const key = random.int(u16);
        switch (random.intRangeLessThan(u8, 0, 4)) {
            0 => {
                const value = random.int(u64);
                _ = try map.put(key, value);
                try model.put(key, value);
            },
            1 => {
                const a = map.remove(key);
                const b = model.remove(key);
                try std.testing.expectEqual(b, a);
            },
            else => {
                const a = map.getConst(key);
                const b = model.get(key);
                if (b) |value| {
                    try std.testing.expect(a != null);
                    try std.testing.expectEqual(value, a.?.*);
                } else {
                    try std.testing.expect(a == null);
                }
            },
        }
        if (i % 997 == 0) map.validate();
    }

    try std.testing.expectEqual(model.count(), map.len());
    var it = model.iterator();
    while (it.next()) |entry| {
        try std.testing.expectEqual(entry.value_ptr.*, map.get(entry.key_ptr.*).?.*);
    }
    map.validate();
}

test "reserve shrink and tombstone churn" {
    var map = phmap.AutoFlatHashMap(u64, u64).init(std.testing.allocator);
    defer map.deinit();

    try map.ensureTotalCapacity(4096);
    try std.testing.expect(map.capacity() >= 4096);

    var round: usize = 0;
    while (round < 8) : (round += 1) {
        var i: usize = 0;
        while (i < 4096) : (i += 1) {
            _ = try map.put(@intCast(i), @intCast(i + round));
        }
        i = 0;
        while (i < 4096) : (i += 2) {
            try std.testing.expect(map.remove(@intCast(i)));
        }
        i = 0;
        while (i < 4096) : (i += 2) {
            _ = try map.put(@intCast(i), @intCast(i + round + 100));
        }
        map.validate();
    }

    try std.testing.expectEqual(@as(usize, 4096), map.len());
    try map.shrinkAndFree();
    try std.testing.expect(map.capacity() >= map.len());
    map.validate();
}

test "allocation failure coverage" {
    const Map = phmap.AutoFlatHashMap(u64, u64);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(allocator: std.mem.Allocator) !void {
            var map = Map.init(allocator);
            defer map.deinit();

            var i: usize = 0;
            while (i < 512) : (i += 1) {
                _ = try map.put(@intCast(i), @intCast(i * 3));
            }
            map.validate();
        }
    }.run, .{});
}
