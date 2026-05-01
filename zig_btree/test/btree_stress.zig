const std = @import("std");
const btree = @import("btree");

const Item = struct { key: u32, value: u32 };

test "btree randomized model against sorted ArrayList" {
    const testing = std.testing;
    const Map = btree.AutoBTreeMapWithConfig(u32, u32, .{ .target_node_size = 128 });
    var map = Map.init(testing.allocator);
    defer map.deinit();

    var model: std.ArrayList(Item) = .empty;
    defer model.deinit(testing.allocator);

    var rng = std.Random.DefaultPrng.init(0xBADC0FFEE);
    var step: usize = 0;
    while (step < 20_000) : (step += 1) {
        const k = rng.random().int(u32) % 4096;
        const op = rng.random().int(u32) % 3;
        switch (op) {
            0, 1 => {
                const v = rng.random().int(u32);
                _ = try map.put(k, v);
                const idx = lowerBoundModel(model.items, k);
                if (idx < model.items.len and model.items[idx].key == k) {
                    model.items[idx].value = v;
                } else {
                    try model.insert(testing.allocator, idx, .{ .key = k, .value = v });
                }
            },
            else => {
                const removed = map.remove(k);
                const idx = lowerBoundModel(model.items, k);
                const expected = idx < model.items.len and model.items[idx].key == k;
                try testing.expectEqual(expected, removed);
                if (expected) _ = model.orderedRemove(idx);
            },
        }
        if (step % 257 == 0) {
            map.validate();
            try expectMatchesModel(&map, model.items);
            try expectReverseMatchesModel(&map, model.items);
            try expectBoundsMatchModel(&map, model.items, step);
        }
    }
    map.validate();
    try expectMatchesModel(&map, model.items);
    try expectReverseMatchesModel(&map, model.items);
    try expectBoundsMatchModel(&map, model.items, step);
}

fn lowerBoundModel(items: []const Item, key: u32) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = (lo + hi) / 2;
        if (items[mid].key < key) lo = mid + 1 else hi = mid;
    }
    return lo;
}

fn upperBoundModel(items: []const Item, key: u32) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = (lo + hi) / 2;
        if (items[mid].key <= key) lo = mid + 1 else hi = mid;
    }
    return lo;
}

fn expectMatchesModel(map: anytype, model: []const Item) !void {
    const testing = std.testing;
    try testing.expectEqual(model.len, map.len());
    var it = map.iterator();
    var idx: usize = 0;
    while (it.next()) |e| : (idx += 1) {
        try testing.expect(idx < model.len);
        try testing.expectEqual(model[idx].key, e.key);
        try testing.expectEqual(model[idx].value, e.value);
        try testing.expect(map.contains(e.key));
    }
    try testing.expectEqual(model.len, idx);
}

fn expectReverseMatchesModel(map: anytype, model: []const Item) !void {
    const testing = std.testing;
    var it = map.reverseIterator();
    var remaining = model.len;
    while (it.next()) |e| {
        try testing.expect(remaining > 0);
        remaining -= 1;
        try testing.expectEqual(model[remaining].key, e.key);
        try testing.expectEqual(model[remaining].value, e.value);
    }
    try testing.expectEqual(@as(usize, 0), remaining);
}

fn expectBoundsMatchModel(map: anytype, model: []const Item, seed: usize) !void {
    const testing = std.testing;
    var i: usize = 0;
    while (i < 64) : (i += 1) {
        const key: u32 = @intCast((seed *% 131 +% i *% 977) % 5000);

        const lb_model = lowerBoundModel(model, key);
        const lb = map.lowerBound(key).entry();
        if (lb_model == model.len) {
            try testing.expect(lb == null);
        } else {
            try testing.expect(lb != null);
            try testing.expectEqual(model[lb_model].key, lb.?.key);
            try testing.expectEqual(model[lb_model].value, lb.?.value);
        }

        const ub_model = upperBoundModel(model, key);
        const ub = map.upperBound(key).entry();
        if (ub_model == model.len) {
            try testing.expect(ub == null);
        } else {
            try testing.expect(ub != null);
            try testing.expectEqual(model[ub_model].key, ub.?.key);
            try testing.expectEqual(model[ub_model].value, ub.?.value);
        }
    }
}
