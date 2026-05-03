//! Portable Swiss-table-inspired hash maps and hash sets.
//!
//! The flat containers keep control metadata separate from entry storage.  A
//! full control byte stores the low 7 hash bits, while empty/deleted sentinels
//! drive probing and tombstone cleanup.

const std = @import("std");
const builtin = @import("builtin");

pub const Config = struct {
    max_load_percent: u8 = 87,
    min_capacity: usize = 8,
};

const empty: u8 = 0x80;
const deleted: u8 = 0xfe;
const sentinel: u8 = 0xff;
const vector_group = builtin.cpu.arch == .x86_64;
const group_width: usize = if (vector_group) 16 else 8;
const GroupMask = std.meta.Int(.unsigned, group_width * 8);

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
        growth_left: usize,

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

        const IndexResult = struct {
            index: usize,
            inserted: bool,
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
                .growth_left = 0,
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
            self.growth_left = 0;
        }

        pub fn clearRetainingCapacity(self: *Self) void {
            if (self.entries.len != 0) {
                @memset(self.ctrl[0..self.entries.len], empty);
                self.refreshClones();
            }
            self.count = 0;
            self.deleted_count = 0;
            self.growth_left = maxLoad(self.entries.len);
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

        pub inline fn insert(self: *Self, key: Key, value: Value) !InsertResult {
            const result = try self.getOrPut(key);
            if (result.inserted) {
                result.entry.value = value;
            }
            return result;
        }

        pub inline fn put(self: *Self, key: Key, value: Value) !PutResult {
            const result = if (self.hasInsertionCapacity())
                self.findOrInsertIndexAssumeCapacity(key)
            else
                try self.findOrInsertIndex(key);
            const entry = &self.entries[result.index];
            if (result.inserted) {
                entry.value = value;
                return .{ .entry = entry, .inserted = true, .old_value = null };
            }
            const old = entry.value;
            entry.value = value;
            return .{ .entry = entry, .inserted = false, .old_value = old };
        }

        pub inline fn getOrPutValue(self: *Self, key: Key, value: Value) !InsertResult {
            const result = try self.getOrPut(key);
            if (result.inserted) result.entry.value = value;
            return result;
        }

        pub inline fn getOrPut(self: *Self, key: Key) !InsertResult {
            const result = if (self.hasInsertionCapacity())
                self.findOrInsertIndexAssumeCapacity(key)
            else
                try self.findOrInsertIndex(key);
            return .{ .entry = &self.entries[result.index], .inserted = result.inserted };
        }

        inline fn findOrInsertIndex(self: *Self, key: Key) !IndexResult {
            try self.ensureAdditionalCapacity(1);
            return self.findOrInsertIndexAssumeCapacity(key);
        }

        inline fn hasInsertionCapacity(self: *const Self) bool {
            return self.growth_left != 0 and self.deleted_count * 2 < self.entries.len;
        }

        inline fn findOrInsertIndexAssumeCapacity(self: *Self, key: Key) IndexResult {
            const h = hashFn(self.context, key);
            const fp = fingerprint(h);
            var first_deleted: ?usize = null;
            var probe = ProbeSeq.init(self.entries.len, h);
            while (true) : (probe.next()) {
                const index = probe.offset;
                const first_ctrl = self.ctrl[index];
                if (first_ctrl == empty) {
                    const target = if (first_deleted) |deleted_slot| target: {
                        self.deleted_count -= 1;
                        break :target deleted_slot;
                    } else target: {
                        self.growth_left -= 1;
                        break :target index;
                    };
                    self.setCtrl(target, fp);
                    self.entries[target].key = key;
                    self.count += 1;
                    return .{ .index = target, .inserted = true };
                }
                if (first_ctrl == deleted) {
                    if (first_deleted == null) first_deleted = index;
                } else if (first_ctrl == fp and eqlFn(self.context, self.entries[index].key, key)) {
                    return .{ .index = index, .inserted = false };
                }

                const group = Group.load(self.ctrl, index);
                var matches = group.match(fp) & ~byteMask(0);
                while (matches != 0) {
                    const bit = takeLowestByte(&matches);
                    const candidate = slotAt(self.entries.len, index, bit);
                    if (eqlFn(self.context, self.entries[candidate].key, key)) {
                        return .{ .index = candidate, .inserted = false };
                    }
                }

                const deleted_mask = group.matchByte(deleted) & ~byteMask(0);
                if (first_deleted == null and deleted_mask != 0) {
                    first_deleted = slotAt(self.entries.len, index, lowestByte(deleted_mask));
                }

                const empty_mask = group.matchEmpty() & ~byteMask(0);
                if (empty_mask != 0) {
                    const target = if (first_deleted) |deleted_slot| target: {
                        self.deleted_count -= 1;
                        break :target deleted_slot;
                    } else target: {
                        self.growth_left -= 1;
                        break :target slotAt(self.entries.len, index, lowestByte(empty_mask));
                    };
                    self.setCtrl(target, fp);
                    self.entries[target].key = key;
                    self.count += 1;
                    return .{ .index = target, .inserted = true };
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
            base: usize = 0,
            full_mask: GroupMask = 0,

            pub fn next(it: *Iterator) ?*Entry {
                while (true) {
                    if (it.full_mask != 0) {
                        const bit = takeLowestByte(&it.full_mask);
                        return &it.map.entries[it.base + bit];
                    }
                    if (it.index >= it.map.entries.len) return null;
                    it.base = it.index;
                    it.full_mask = Group.load(it.map.ctrl, it.index).matchFull();
                    it.index += group_width;
                }
            }
        };

        pub const ConstIterator = struct {
            map: *const Self,
            index: usize,
            base: usize = 0,
            full_mask: GroupMask = 0,

            pub fn next(it: *ConstIterator) ?*const Entry {
                while (true) {
                    if (it.full_mask != 0) {
                        const bit = takeLowestByte(&it.full_mask);
                        return &it.map.entries[it.base + bit];
                    }
                    if (it.index >= it.map.entries.len) return null;
                    it.base = it.index;
                    it.full_mask = Group.load(it.map.ctrl, it.index).matchFull();
                    it.index += group_width;
                }
            }
        };

        pub fn validate(self: *const Self) void {
            if (self.entries.len == 0) {
                std.debug.assert(self.ctrl.len == 0);
                return;
            }
            std.debug.assert(self.ctrl.len == self.entries.len + group_width + 1);
            std.debug.assert(self.ctrl[self.entries.len + group_width] == sentinel);
            var clone_index: usize = 0;
            while (clone_index < group_width) : (clone_index += 1) {
                std.debug.assert(self.ctrl[self.entries.len + clone_index] == self.ctrl[clone_index]);
            }
            var full_seen: usize = 0;
            var deleted_seen: usize = 0;
            for (self.ctrl[0..self.entries.len], 0..) |c, i| {
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
            std.debug.assert(self.count + self.deleted_count <= maxLoad(self.entries.len));
            std.debug.assert(self.growth_left == maxLoad(self.entries.len) - self.count - self.deleted_count);
        }

        inline fn ensureAdditionalCapacity(self: *Self, additional: usize) !void {
            if (additional <= self.growth_left and self.deleted_count * 2 < self.entries.len) return;
            const needed = try std.math.add(usize, self.count, additional);
            const target = if (needed <= maxLoad(self.entries.len)) self.entries.len else capacityFor(needed);
            try self.rehash(target);
        }

        fn maxLoad(table_capacity: usize) usize {
            if (table_capacity == 0) return 0;
            return (table_capacity * @as(usize, config.max_load_percent)) / 100;
        }

        fn capacityFor(items: usize) usize {
            var table_capacity: usize = @max(@as(usize, group_width), config.min_capacity);
            table_capacity = std.math.ceilPowerOfTwoAssert(usize, table_capacity);
            while (maxLoad(table_capacity) < items) table_capacity *= 2;
            return table_capacity;
        }

        fn normalizeCapacity(requested: usize) usize {
            var table_capacity: usize = @max(@as(usize, group_width), config.min_capacity);
            table_capacity = std.math.ceilPowerOfTwoAssert(usize, table_capacity);
            while (table_capacity < requested) table_capacity *= 2;
            return table_capacity;
        }

        fn rehash(self: *Self, new_capacity: usize) !void {
            const new_cap = normalizeCapacity(new_capacity);
            const new_ctrl = try self.allocator.alloc(u8, new_cap + group_width + 1);
            errdefer self.allocator.free(new_ctrl);
            const new_entries = try self.allocator.alloc(Entry, new_cap);
            errdefer self.allocator.free(new_entries);
            @memset(new_ctrl[0..new_cap], empty);
            @memset(new_ctrl[new_cap .. new_cap + group_width], empty);
            new_ctrl[new_cap + group_width] = sentinel;

            const old_ctrl = self.ctrl;
            const old_entries = self.entries;
            const old_count = self.count;

            self.ctrl = new_ctrl;
            self.entries = new_entries;
            self.count = 0;
            self.deleted_count = 0;
            self.growth_left = maxLoad(new_cap);

            for (old_ctrl[0..old_entries.len], 0..) |c, i| {
                if (!isFull(c)) continue;
                const entry = old_entries[i];
                const result = self.insertRehashed(entry.key);
                self.entries[result].value = entry.value;
            }
            std.debug.assert(self.count == old_count);
            self.growth_left = maxLoad(self.entries.len) - self.count;

            self.allocator.free(old_ctrl);
            self.allocator.free(old_entries);
        }

        fn insertRehashed(self: *Self, key: Key) usize {
            const h = hashFn(self.context, key);
            const fp = fingerprint(h);
            var probe = ProbeSeq.init(self.entries.len, h);
            while (true) : (probe.next()) {
                const index = probe.offset;
                if (self.ctrl[index] == empty) {
                    self.setCtrl(index, fp);
                    self.entries[index].key = key;
                    self.count += 1;
                    self.growth_left -= 1;
                    return index;
                }

                const group = Group.load(self.ctrl, index);
                var mask = group.matchEmptyOrDeleted() & ~byteMask(0);
                if (mask != 0) {
                    const target = slotAt(self.entries.len, index, takeLowestByte(&mask));
                    if (self.ctrl[target] == empty) self.growth_left -= 1;
                    self.setCtrl(target, fp);
                    self.entries[target].key = key;
                    self.count += 1;
                    return target;
                }
            }
        }

        inline fn findIndex(self: *const Self, key: Key) ?usize {
            if (self.entries.len == 0) return null;
            const h = hashFn(self.context, key);
            const fp = fingerprint(h);
            var probe = ProbeSeq.init(self.entries.len, h);
            while (true) : (probe.next()) {
                const index = probe.offset;
                const group = Group.load(self.ctrl, index);
                if (group.firstByte() == fp) {
                    if (eqlFn(self.context, self.entries[index].key, key)) return index;
                }
                var matches = group.match(fp) & ~byteMask(0);
                while (matches != 0) {
                    const bit = takeLowestByte(&matches);
                    const candidate = slotAt(self.entries.len, index, bit);
                    if (eqlFn(self.context, self.entries[candidate].key, key)) return candidate;
                }
                if (group.matchEmpty() != 0) return null;
            }
        }

        fn eraseIndex(self: *Self, index: usize) void {
            self.setCtrl(index, deleted);
            self.count -= 1;
            self.deleted_count += 1;
        }

        fn setCtrl(self: *Self, index: usize, value: u8) void {
            self.ctrl[index] = value;
            if (index < group_width) {
                self.ctrl[self.entries.len + index] = value;
            }
        }

        fn refreshClones(self: *Self) void {
            @memcpy(self.ctrl[self.entries.len .. self.entries.len + group_width], self.ctrl[0..group_width]);
            self.ctrl[self.entries.len + group_width] = sentinel;
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

inline fn fingerprint(hash: u64) u8 {
    return @as(u8, @intCast(hash & 0x7f));
}

inline fn startIndex(capacity: usize, hash: u64) usize {
    return @as(usize, @intCast((hash >> 7) & @as(u64, @intCast(capacity - 1))));
}

const ProbeSeq = struct {
    mask: usize,
    offset: usize,
    index: usize = 0,

    inline fn init(capacity: usize, hash: u64) ProbeSeq {
        return .{
            .mask = capacity - 1,
            .offset = startIndex(capacity, hash),
        };
    }

    inline fn next(seq: *ProbeSeq) void {
        seq.index +%= group_width;
        seq.offset = (seq.offset +% seq.index) & seq.mask;
    }
};

inline fn slotAt(capacity: usize, base: usize, bit: usize) usize {
    return (base + bit) & (capacity - 1);
}

fn isFull(c: u8) bool {
    return c < 0x80;
}

const Group = WordGroup;

const WordGroup = struct {
    word: GroupMask,

    inline fn load(ctrl: []const u8, index: usize) WordGroup {
        const ptr: *align(1) const GroupMask = @ptrCast(ctrl[index..].ptr);
        return .{ .word = ptr.* };
    }

    inline fn match(g: WordGroup, value: u8) GroupMask {
        return matchByteWord(g.word, value);
    }

    inline fn matchByte(g: WordGroup, value: u8) GroupMask {
        return g.match(value);
    }

    inline fn matchEmpty(g: WordGroup) GroupMask {
        return (g.word & (~g.word << 6)) & msbs;
    }

    inline fn matchEmptyOrDeleted(g: WordGroup) GroupMask {
        return g.word & msbs;
    }

    inline fn matchFull(g: WordGroup) GroupMask {
        return (~g.matchEmptyOrDeleted()) & msbs;
    }

    inline fn firstByte(g: WordGroup) u8 {
        return @truncate(g.word);
    }
};

const lsbs: GroupMask = repeatedByte(0x01);
const msbs: GroupMask = repeatedByte(0x80);

inline fn matchByteWord(word: GroupMask, value: u8) GroupMask {
    const repeated = lsbs * @as(GroupMask, value);
    const x = word ^ repeated;
    return (x -% lsbs) & ~x & msbs;
}

inline fn byteMask(index: usize) GroupMask {
    return @as(GroupMask, 0x80) << @intCast(index * 8);
}

inline fn lowestByte(mask: GroupMask) usize {
    return @ctz(mask) >> 3;
}

inline fn takeLowestByte(mask: *GroupMask) usize {
    const bit = lowestByte(mask.*);
    mask.* &= mask.* - 1;
    return bit;
}

fn repeatedByte(comptime byte: u8) GroupMask {
    var out: GroupMask = 0;
    var i: usize = 0;
    while (i < group_width) : (i += 1) {
        out |= @as(GroupMask, byte) << @intCast(i * 8);
    }
    return out;
}

test "probe sequence uses upstream group stepping" {
    const capacity: usize = 128;
    const hash: u64 = 0x1234_5678_9abc_def0;
    const mask = capacity - 1;
    var expected = startIndex(capacity, hash);
    var delta: usize = 0;
    var seq = ProbeSeq.init(capacity, hash);

    var i: usize = 0;
    while (i < 12) : (i += 1) {
        try std.testing.expectEqual(expected, seq.offset);
        delta +%= group_width;
        expected = (expected +% delta) & mask;
        seq.next();
    }
}

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

fn mix64(value: u64) u64 {
    return value *% 0x9e37_79b9_7f4a_7c15;
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

test "group clones remain valid across boundary mutations" {
    var map = AutoFlatHashMap(u64, u64).init(std.testing.allocator);
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
    var map = AutoFlatHashMap(u64, u64).init(std.testing.allocator);
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
    const Map = FlatHashMap(u64, u64, Context, struct {
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
