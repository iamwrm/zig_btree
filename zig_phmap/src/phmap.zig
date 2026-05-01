//! Portable Swiss-table-inspired hash maps and hash sets.
//!
//! The flat containers keep control metadata separate from entry storage.  A
//! full control byte stores the low 7 hash bits, while empty/deleted sentinels
//! drive probing and tombstone cleanup.

const std = @import("std");

pub const Config = struct {
    max_load_percent: u8 = 87,
    min_capacity: usize = 8,
};

const empty: u8 = 0x80;
const deleted: u8 = 0xfe;

pub fn AutoFlatHashMap(comptime Key: type, comptime Value: type) type {
    return FlatHashMap(Key, Value, void, defaultHash(Key), defaultEql(Key), .{});
}

pub fn AutoFlatHashSet(comptime Key: type) type {
    return FlatHashSet(Key, void, defaultHash(Key), defaultEql(Key), .{});
}

pub fn AutoNodeHashMap(comptime Key: type, comptime Value: type) type {
    return NodeHashMap(Key, Value, void, defaultHash(Key), defaultEql(Key), .{});
}

pub fn AutoNodeHashSet(comptime Key: type) type {
    return NodeHashSet(Key, void, defaultHash(Key), defaultEql(Key), .{});
}

pub fn FlatHashMap(
    comptime Key: type,
    comptime Value: type,
    comptime Context: type,
    comptime hashFn: fn (Context, Key) u64,
    comptime eqlFn: fn (Context, Key, Key) bool,
    comptime config: Config,
) type {
    const Entry = struct {
        key: Key,
        value: Value,
    };

    return struct {
        const Self = @This();
        const Allocator = std.mem.Allocator;

        pub const key_type = Key;
        pub const value_type = Value;
        pub const context_type = Context;
        pub const entry_type = Entry;

        allocator: Allocator,
        context: Context,
        ctrl: []u8,
        entries: []Entry,
        count: usize,
        deleted_count: usize,

        pub const InsertResult = struct {
            entry: *Entry,
            inserted: bool,
        };

        pub const PutResult = struct {
            entry: *Entry,
            inserted: bool,
            old_value: ?Value,
        };

        pub const FetchRemoveResult = struct {
            key: Key,
            value: Value,
        };

        pub fn init(allocator: Allocator) Self {
            return initContext(allocator, undefinedContext(Context));
        }

        pub fn initContext(allocator: Allocator, context: Context) Self {
            return .{
                .allocator = allocator,
                .context = context,
                .ctrl = &.{},
                .entries = &.{},
                .count = 0,
                .deleted_count = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.ctrl);
            self.allocator.free(self.entries);
            self.* = initContext(self.allocator, self.context);
        }

        pub fn clear(self: *Self) void {
            self.allocator.free(self.ctrl);
            self.allocator.free(self.entries);
            self.ctrl = &.{};
            self.entries = &.{};
            self.count = 0;
            self.deleted_count = 0;
        }

        pub fn clearRetainingCapacity(self: *Self) void {
            @memset(self.ctrl, empty);
            self.count = 0;
            self.deleted_count = 0;
        }

        pub fn len(self: *const Self) usize {
            return self.count;
        }

        pub fn capacity(self: *const Self) usize {
            return self.entries.len;
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.count == 0;
        }

        pub fn contains(self: *const Self, key: Key) bool {
            return self.findIndex(key) != null;
        }

        pub fn get(self: *Self, key: Key) ?*Value {
            const index = self.findIndex(key) orelse return null;
            return &self.entries[index].value;
        }

        pub fn getConst(self: *const Self, key: Key) ?*const Value {
            const index = self.findIndex(key) orelse return null;
            return &self.entries[index].value;
        }

        pub fn getEntry(self: *Self, key: Key) ?*Entry {
            const index = self.findIndex(key) orelse return null;
            return &self.entries[index];
        }

        pub fn getEntryConst(self: *const Self, key: Key) ?*const Entry {
            const index = self.findIndex(key) orelse return null;
            return &self.entries[index];
        }

        pub fn insert(self: *Self, key: Key, value: Value) !InsertResult {
            const result = try self.getOrPut(key);
            if (result.inserted) {
                result.entry.value = value;
            }
            return result;
        }

        pub fn put(self: *Self, key: Key, value: Value) !PutResult {
            const result = try self.getOrPut(key);
            if (result.inserted) {
                result.entry.value = value;
                return .{ .entry = result.entry, .inserted = true, .old_value = null };
            }
            const old = result.entry.value;
            result.entry.value = value;
            return .{ .entry = result.entry, .inserted = false, .old_value = old };
        }

        pub fn getOrPutValue(self: *Self, key: Key, value: Value) !InsertResult {
            const result = try self.getOrPut(key);
            if (result.inserted) result.entry.value = value;
            return result;
        }

        pub fn getOrPut(self: *Self, key: Key) !InsertResult {
            try self.ensureAdditionalCapacity(1);
            const h = hashFn(self.context, key);
            const fp = fingerprint(h);
            var first_deleted: ?usize = null;
            var index = startIndex(self.entries.len, h);
            while (true) : (index = nextIndex(self.entries.len, index)) {
                const c = self.ctrl[index];
                if (c == empty) {
                    const target = first_deleted orelse index;
                    if (first_deleted != null) self.deleted_count -= 1;
                    self.ctrl[target] = fp;
                    self.entries[target].key = key;
                    self.count += 1;
                    return .{ .entry = &self.entries[target], .inserted = true };
                }
                if (c == deleted) {
                    if (first_deleted == null) first_deleted = index;
                    continue;
                }
                if (c == fp and eqlFn(self.context, self.entries[index].key, key)) {
                    return .{ .entry = &self.entries[index], .inserted = false };
                }
            }
        }

        pub fn remove(self: *Self, key: Key) bool {
            const index = self.findIndex(key) orelse return false;
            self.eraseIndex(index);
            return true;
        }

        pub fn fetchRemove(self: *Self, key: Key) ?FetchRemoveResult {
            const index = self.findIndex(key) orelse return null;
            const out = FetchRemoveResult{
                .key = self.entries[index].key,
                .value = self.entries[index].value,
            };
            self.eraseIndex(index);
            return out;
        }

        pub fn reserve(self: *Self, additional: usize) !void {
            try self.ensureAdditionalCapacity(additional);
        }

        pub fn ensureTotalCapacity(self: *Self, requested: usize) !void {
            if (requested <= maxLoad(self.entries.len) and self.deleted_count == 0) return;
            try self.rehash(capacityFor(requested));
        }

        pub fn shrinkAndFree(self: *Self) !void {
            if (self.count == 0) {
                self.clear();
                return;
            }
            try self.rehash(capacityFor(self.count));
        }

        pub fn iterator(self: *Self) Iterator {
            return .{ .map = self, .index = 0 };
        }

        pub fn constIterator(self: *const Self) ConstIterator {
            return .{ .map = self, .index = 0 };
        }

        pub const Iterator = struct {
            map: *Self,
            index: usize,

            pub fn next(it: *Iterator) ?*Entry {
                while (it.index < it.map.entries.len) : (it.index += 1) {
                    const current = it.index;
                    if (isFull(it.map.ctrl[current])) {
                        it.index += 1;
                        return &it.map.entries[current];
                    }
                }
                return null;
            }
        };

        pub const ConstIterator = struct {
            map: *const Self,
            index: usize,

            pub fn next(it: *ConstIterator) ?*const Entry {
                while (it.index < it.map.entries.len) : (it.index += 1) {
                    const current = it.index;
                    if (isFull(it.map.ctrl[current])) {
                        it.index += 1;
                        return &it.map.entries[current];
                    }
                }
                return null;
            }
        };

        pub fn validate(self: *const Self) void {
            std.debug.assert(self.ctrl.len == self.entries.len);
            var full_seen: usize = 0;
            var deleted_seen: usize = 0;
            for (self.ctrl, 0..) |c, i| {
                if (isFull(c)) {
                    full_seen += 1;
                    std.debug.assert(self.findIndex(self.entries[i].key) == i);
                } else if (c == deleted) {
                    deleted_seen += 1;
                } else {
                    std.debug.assert(c == empty);
                }
            }
            std.debug.assert(full_seen == self.count);
            std.debug.assert(deleted_seen == self.deleted_count);
            std.debug.assert(self.count <= maxLoad(self.entries.len));
        }

        fn ensureAdditionalCapacity(self: *Self, additional: usize) !void {
            const needed = try std.math.add(usize, self.count, additional);
            if (needed <= maxLoad(self.entries.len) and self.deleted_count * 2 < self.entries.len) return;
            const target = if (needed <= maxLoad(self.entries.len)) self.entries.len else capacityFor(needed);
            try self.rehash(target);
        }

        fn maxLoad(table_capacity: usize) usize {
            if (table_capacity == 0) return 0;
            return (table_capacity * @as(usize, config.max_load_percent)) / 100;
        }

        fn capacityFor(items: usize) usize {
            var table_capacity: usize = @max(@as(usize, 1), config.min_capacity);
            table_capacity = std.math.ceilPowerOfTwoAssert(usize, table_capacity);
            while (maxLoad(table_capacity) < items) table_capacity *= 2;
            return table_capacity;
        }

        fn normalizeCapacity(requested: usize) usize {
            var table_capacity: usize = @max(@as(usize, 1), config.min_capacity);
            table_capacity = std.math.ceilPowerOfTwoAssert(usize, table_capacity);
            while (table_capacity < requested) table_capacity *= 2;
            return table_capacity;
        }

        fn rehash(self: *Self, new_capacity: usize) !void {
            const new_cap = normalizeCapacity(new_capacity);
            const new_ctrl = try self.allocator.alloc(u8, new_cap);
            errdefer self.allocator.free(new_ctrl);
            const new_entries = try self.allocator.alloc(Entry, new_cap);
            errdefer self.allocator.free(new_entries);
            @memset(new_ctrl, empty);

            const old_ctrl = self.ctrl;
            const old_entries = self.entries;
            const old_count = self.count;

            self.ctrl = new_ctrl;
            self.entries = new_entries;
            self.count = 0;
            self.deleted_count = 0;

            for (old_ctrl, 0..) |c, i| {
                if (!isFull(c)) continue;
                const entry = old_entries[i];
                const result = self.insertRehashed(entry.key);
                self.entries[result].value = entry.value;
            }
            std.debug.assert(self.count == old_count);

            self.allocator.free(old_ctrl);
            self.allocator.free(old_entries);
        }

        fn insertRehashed(self: *Self, key: Key) usize {
            const h = hashFn(self.context, key);
            const fp = fingerprint(h);
            var index = startIndex(self.entries.len, h);
            while (true) : (index = nextIndex(self.entries.len, index)) {
                if (self.ctrl[index] == empty) {
                    self.ctrl[index] = fp;
                    self.entries[index].key = key;
                    self.count += 1;
                    return index;
                }
            }
        }

        fn findIndex(self: *const Self, key: Key) ?usize {
            if (self.entries.len == 0) return null;
            const h = hashFn(self.context, key);
            const fp = fingerprint(h);
            var index = startIndex(self.entries.len, h);
            while (true) : (index = nextIndex(self.entries.len, index)) {
                const c = self.ctrl[index];
                if (c == empty) return null;
                if (c == fp and eqlFn(self.context, self.entries[index].key, key)) return index;
            }
        }

        fn eraseIndex(self: *Self, index: usize) void {
            self.ctrl[index] = deleted;
            self.count -= 1;
            self.deleted_count += 1;
        }
    };
}

pub fn FlatHashSet(
    comptime Key: type,
    comptime Context: type,
    comptime hashFn: fn (Context, Key) u64,
    comptime eqlFn: fn (Context, Key, Key) bool,
    comptime config: Config,
) type {
    const Map = FlatHashMap(Key, void, Context, hashFn, eqlFn, config);

    return struct {
        const Self = @This();

        map: Map,

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .map = Map.init(allocator) };
        }

        pub fn initContext(allocator: std.mem.Allocator, context: Context) Self {
            return .{ .map = Map.initContext(allocator, context) };
        }

        pub fn deinit(self: *Self) void {
            self.map.deinit();
        }

        pub fn clear(self: *Self) void {
            self.map.clear();
        }

        pub fn clearRetainingCapacity(self: *Self) void {
            self.map.clearRetainingCapacity();
        }

        pub fn len(self: *const Self) usize {
            return self.map.len();
        }

        pub fn capacity(self: *const Self) usize {
            return self.map.capacity();
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.map.isEmpty();
        }

        pub fn contains(self: *const Self, key: Key) bool {
            return self.map.contains(key);
        }

        pub fn insert(self: *Self, key: Key) !bool {
            return (try self.map.getOrPutValue(key, {})).inserted;
        }

        pub fn remove(self: *Self, key: Key) bool {
            return self.map.remove(key);
        }

        pub fn reserve(self: *Self, additional: usize) !void {
            try self.map.reserve(additional);
        }

        pub fn ensureTotalCapacity(self: *Self, requested: usize) !void {
            try self.map.ensureTotalCapacity(requested);
        }

        pub fn shrinkAndFree(self: *Self) !void {
            try self.map.shrinkAndFree();
        }

        pub fn iterator(self: *Self) Iterator {
            return .{ .inner = self.map.iterator() };
        }

        pub fn validate(self: *const Self) void {
            self.map.validate();
        }

        pub const Iterator = struct {
            inner: Map.Iterator,

            pub fn next(it: *Iterator) ?*Key {
                const entry = it.inner.next() orelse return null;
                return &entry.key;
            }
        };
    };
}

pub fn NodeHashMap(
    comptime Key: type,
    comptime Value: type,
    comptime Context: type,
    comptime hashFn: fn (Context, Key) u64,
    comptime eqlFn: fn (Context, Key, Key) bool,
    comptime config: Config,
) type {
    const Node = struct {
        key: Key,
        value: Value,
    };
    const Index = FlatHashMap(Key, *Node, Context, hashFn, eqlFn, config);

    return struct {
        const Self = @This();

        pub const key_type = Key;
        pub const value_type = Value;
        pub const entry_type = Node;
        pub const context_type = Context;

        allocator: std.mem.Allocator,
        index: Index,

        pub const InsertResult = struct {
            entry: *Node,
            inserted: bool,
        };

        pub const PutResult = struct {
            entry: *Node,
            inserted: bool,
            old_value: ?Value,
        };

        pub const FetchRemoveResult = struct {
            key: Key,
            value: Value,
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .allocator = allocator, .index = Index.init(allocator) };
        }

        pub fn initContext(allocator: std.mem.Allocator, context: Context) Self {
            return .{ .allocator = allocator, .index = Index.initContext(allocator, context) };
        }

        pub fn deinit(self: *Self) void {
            self.destroyNodes();
            self.index.deinit();
        }

        pub fn clear(self: *Self) void {
            self.destroyNodes();
            self.index.clear();
        }

        pub fn clearRetainingCapacity(self: *Self) void {
            self.destroyNodes();
            self.index.clearRetainingCapacity();
        }

        pub fn len(self: *const Self) usize {
            return self.index.len();
        }

        pub fn capacity(self: *const Self) usize {
            return self.index.capacity();
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.index.isEmpty();
        }

        pub fn contains(self: *const Self, key: Key) bool {
            return self.index.contains(key);
        }

        pub fn get(self: *Self, key: Key) ?*Value {
            const node = self.getEntry(key) orelse return null;
            return &node.value;
        }

        pub fn getConst(self: *const Self, key: Key) ?*const Value {
            const node = self.getEntryConst(key) orelse return null;
            return &node.value;
        }

        pub fn getEntry(self: *Self, key: Key) ?*Node {
            const ptr = self.index.get(key) orelse return null;
            return ptr.*;
        }

        pub fn getEntryConst(self: *const Self, key: Key) ?*const Node {
            const ptr = self.index.getConst(key) orelse return null;
            return ptr.*;
        }

        pub fn insert(self: *Self, key: Key, value: Value) !InsertResult {
            const result = try self.getOrPut(key);
            if (result.inserted) result.entry.value = value;
            return result;
        }

        pub fn put(self: *Self, key: Key, value: Value) !PutResult {
            const result = try self.getOrPut(key);
            if (result.inserted) {
                result.entry.value = value;
                return .{ .entry = result.entry, .inserted = true, .old_value = null };
            }
            const old = result.entry.value;
            result.entry.value = value;
            return .{ .entry = result.entry, .inserted = false, .old_value = old };
        }

        pub fn getOrPutValue(self: *Self, key: Key, value: Value) !InsertResult {
            const result = try self.getOrPut(key);
            if (result.inserted) result.entry.value = value;
            return result;
        }

        pub fn getOrPut(self: *Self, key: Key) !InsertResult {
            const result = try self.index.getOrPut(key);
            if (!result.inserted) return .{ .entry = result.entry.value, .inserted = false };

            const node = self.allocator.create(Node) catch |err| {
                _ = self.index.remove(key);
                return err;
            };
            node.* = .{ .key = key, .value = undefined };
            result.entry.value = node;
            return .{ .entry = node, .inserted = true };
        }

        pub fn remove(self: *Self, key: Key) bool {
            const removed = self.index.fetchRemove(key) orelse return false;
            self.allocator.destroy(removed.value);
            return true;
        }

        pub fn fetchRemove(self: *Self, key: Key) ?FetchRemoveResult {
            const removed = self.index.fetchRemove(key) orelse return null;
            const node = removed.value;
            const out = FetchRemoveResult{ .key = node.key, .value = node.value };
            self.allocator.destroy(node);
            return out;
        }

        pub fn reserve(self: *Self, additional: usize) !void {
            try self.index.reserve(additional);
        }

        pub fn ensureTotalCapacity(self: *Self, requested: usize) !void {
            try self.index.ensureTotalCapacity(requested);
        }

        pub fn shrinkAndFree(self: *Self) !void {
            try self.index.shrinkAndFree();
        }

        pub fn iterator(self: *Self) Iterator {
            return .{ .inner = self.index.iterator() };
        }

        pub fn constIterator(self: *const Self) ConstIterator {
            return .{ .inner = self.index.constIterator() };
        }

        pub fn validate(self: *const Self) void {
            self.index.validate();
            var it = self.index.constIterator();
            while (it.next()) |entry| {
                std.debug.assert(eqlFn(self.index.context, entry.key, entry.value.key));
            }
        }

        pub const Iterator = struct {
            inner: Index.Iterator,

            pub fn next(it: *Iterator) ?*Node {
                const entry = it.inner.next() orelse return null;
                return entry.value;
            }
        };

        pub const ConstIterator = struct {
            inner: Index.ConstIterator,

            pub fn next(it: *ConstIterator) ?*const Node {
                const entry = it.inner.next() orelse return null;
                return entry.value;
            }
        };

        fn destroyNodes(self: *Self) void {
            var it = self.index.iterator();
            while (it.next()) |entry| self.allocator.destroy(entry.value);
        }
    };
}

pub fn NodeHashSet(
    comptime Key: type,
    comptime Context: type,
    comptime hashFn: fn (Context, Key) u64,
    comptime eqlFn: fn (Context, Key, Key) bool,
    comptime config: Config,
) type {
    const Map = NodeHashMap(Key, void, Context, hashFn, eqlFn, config);

    return struct {
        const Self = @This();

        map: Map,

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .map = Map.init(allocator) };
        }

        pub fn initContext(allocator: std.mem.Allocator, context: Context) Self {
            return .{ .map = Map.initContext(allocator, context) };
        }

        pub fn deinit(self: *Self) void {
            self.map.deinit();
        }

        pub fn clear(self: *Self) void {
            self.map.clear();
        }

        pub fn clearRetainingCapacity(self: *Self) void {
            self.map.clearRetainingCapacity();
        }

        pub fn len(self: *const Self) usize {
            return self.map.len();
        }

        pub fn capacity(self: *const Self) usize {
            return self.map.capacity();
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.map.isEmpty();
        }

        pub fn contains(self: *const Self, key: Key) bool {
            return self.map.contains(key);
        }

        pub fn insert(self: *Self, key: Key) !bool {
            return (try self.map.getOrPutValue(key, {})).inserted;
        }

        pub fn remove(self: *Self, key: Key) bool {
            return self.map.remove(key);
        }

        pub fn reserve(self: *Self, additional: usize) !void {
            try self.map.reserve(additional);
        }

        pub fn ensureTotalCapacity(self: *Self, requested: usize) !void {
            try self.map.ensureTotalCapacity(requested);
        }

        pub fn shrinkAndFree(self: *Self) !void {
            try self.map.shrinkAndFree();
        }

        pub fn iterator(self: *Self) Iterator {
            return .{ .inner = self.map.iterator() };
        }

        pub fn validate(self: *const Self) void {
            self.map.validate();
        }

        pub const Iterator = struct {
            inner: Map.Iterator,

            pub fn next(it: *Iterator) ?*Key {
                const node = it.inner.next() orelse return null;
                return &node.key;
            }
        };
    };
}

fn undefinedContext(comptime Context: type) Context {
    if (Context == void) return {};
    return undefined;
}

fn fingerprint(hash: u64) u8 {
    return @as(u8, @intCast(hash & 0x7f));
}

fn startIndex(capacity: usize, hash: u64) usize {
    return @as(usize, @intCast(hash & @as(u64, @intCast(capacity - 1))));
}

fn nextIndex(capacity: usize, index: usize) usize {
    return (index + 1) & (capacity - 1);
}

fn isFull(c: u8) bool {
    return c < 0x80;
}

pub fn defaultHash(comptime Key: type) fn (void, Key) u64 {
    return struct {
        fn hash(_: void, key: Key) u64 {
            if (Key == []const u8) return std.hash.Wyhash.hash(0, key);
            return std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
        }
    }.hash;
}

pub fn defaultEql(comptime Key: type) fn (void, Key, Key) bool {
    return struct {
        fn eql(_: void, a: Key, b: Key) bool {
            if (Key == []const u8) return std.mem.eql(u8, a, b);
            return std.meta.eql(a, b);
        }
    }.eql;
}

test "flat map basic operations" {
    var map = AutoFlatHashMap(u64, u64).init(std.testing.allocator);
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
    var set = AutoFlatHashSet(u64).init(std.testing.allocator);
    defer set.deinit();

    try std.testing.expect(try set.insert(10));
    try std.testing.expect(!try set.insert(10));
    try std.testing.expect(set.contains(10));
    try std.testing.expect(set.remove(10));
    try std.testing.expect(!set.contains(10));
    set.validate();
}

test "byte slice keys use content equality" {
    var map = AutoFlatHashMap([]const u8, u64).init(std.testing.allocator);
    defer map.deinit();

    try std.testing.expect((try map.put("alpha", 1)).inserted);
    try std.testing.expect(!(try map.put("alpha", 2)).inserted);
    try std.testing.expectEqual(@as(u64, 2), map.get("alpha").?.*);
}

test "node map keeps value pointers stable across growth" {
    var map = AutoNodeHashMap(u64, u64).init(std.testing.allocator);
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
    var set = AutoNodeHashSet(u64).init(std.testing.allocator);
    defer set.deinit();

    try std.testing.expect(try set.insert(1));
    try std.testing.expect(!try set.insert(1));
    try std.testing.expect(set.contains(1));
    try std.testing.expect(set.remove(1));
    set.validate();
}
