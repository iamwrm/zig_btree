//! A production-oriented B-tree map/set implementation for Zig.
//!
//! Design notes:
//! - Stores values in both internal and leaf nodes, like Abseil's btree.
//! - Uses high fan-out cache-sized nodes with parent pointers and child indices.
//! - Implements full split, borrow, merge, root shrink, lower/upper-bound and
//!   bidirectional cursor/iterator traversal.
//! - Mutating operations invalidate cursors/iterators through a generation
//!   counter checked in Debug/ReleaseSafe builds.
//!
//! This file is intentionally self-contained.  It does not call destructors for
//! keys/values; as with std containers, callers that store owning values should
//! iterate and release those resources before deinit/clear if needed.

const std = @import("std");

pub const Config = struct {
    /// Target node size used to derive fan-out.  The implementation chooses an
    /// odd max key count so the classic B-tree split invariant is exact.
    target_node_size: usize = 256,
    /// Lower bound on max keys per node.  Five gives min degree three, avoiding
    /// the poor occupancy of tiny B-trees.
    min_max_slots: usize = 5,
    /// Upper bound on max keys per node.  Kept within u16 position fields.
    max_max_slots: usize = 255,
    /// Use linear search inside small nodes; binary search above this size.
    linear_search_threshold: usize = 16,
    /// Debug/ReleaseSafe iterator invalidation checks.
    check_iterator_generation: bool = true,
};

pub fn AutoBTreeMap(comptime Key: type, comptime Value: type) type {
    return BTreeMap(Key, Value, void, defaultCompare(Key), .{});
}

pub fn AutoBTreeMapWithConfig(comptime Key: type, comptime Value: type, comptime config: Config) type {
    return BTreeMap(Key, Value, void, defaultCompare(Key), config);
}

pub fn AutoBTreeSet(comptime Key: type) type {
    return BTreeSet(Key, void, defaultCompare(Key), .{});
}

pub fn AutoBTreeSetWithConfig(comptime Key: type, comptime config: Config) type {
    return BTreeSet(Key, void, defaultCompare(Key), config);
}

pub fn BTreeMap(
    comptime Key: type,
    comptime Value: type,
    comptime Context: type,
    comptime compare: fn (*const Context, *const Key, *const Key) std.math.Order,
    comptime config: Config,
) type {
    const Entry = struct {
        key: Key,
        value: Value,
    };

    const max_slots = deriveMaxSlots(Entry, config);
    const min_slots = max_slots / 2;

    return struct {
        const Self = @This();
        const Allocator = std.mem.Allocator;

        pub const key_type = Key;
        pub const value_type = Value;
        pub const entry_type = Entry;
        pub const context_type = Context;
        pub const max_node_slots: usize = max_slots;
        pub const min_node_slots: usize = min_slots;

        const Node = struct {
            parent: ?*Node,
            position: u16,
            len: u16,
            leaf: bool,
            entries: [max_slots]Entry,
            children: [max_slots + 1]?*Node,

            fn init(leaf: bool, parent: ?*Node, position: usize) Node {
                return .{
                    .parent = parent,
                    .position = narrowPos(position),
                    .len = 0,
                    .leaf = leaf,
                    .entries = undefined,
                    .children = [_]?*Node{null} ** (max_slots + 1),
                };
            }

            fn count(n: *const Node) usize {
                return @as(usize, n.len);
            }
        };

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

        pub const Stats = struct {
            len: usize,
            height: usize,
            nodes: usize,
            leaf_nodes: usize,
            internal_nodes: usize,
            max_node_slots: usize,
            min_node_slots: usize,
            fullness: f64,
            bytes_used: usize,
        };

        pub const Cursor = CursorImpl(*Self, *Node, *Entry, *Key, *Value);
        pub const ConstCursor = CursorImpl(*const Self, *const Node, *const Entry, *const Key, *const Value);

        fn CursorImpl(
            comptime TreePtr: type,
            comptime NodePtr: type,
            comptime EntryPtr: type,
            comptime KeyPtr: type,
            comptime ValuePtr: type,
        ) type {
            return struct {
                const C = @This();

                tree: TreePtr,
                node: ?NodePtr,
                index: usize,
                generation: usize,

                pub fn isEnd(c: C) bool {
                    return c.node == null;
                }

                pub fn entry(c: C) ?EntryPtr {
                    c.assertValid();
                    const n = c.node orelse return null;
                    return &n.entries[c.index];
                }

                pub fn key(c: C) ?KeyPtr {
                    const e = c.entry() orelse return null;
                    return &e.key;
                }

                pub fn value(c: C) ?ValuePtr {
                    const e = c.entry() orelse return null;
                    return &e.value;
                }

                pub fn next(c: *C) ?EntryPtr {
                    c.assertValid();
                    const n = c.node orelse return null;
                    const out = &n.entries[c.index];
                    c.advance();
                    return out;
                }

                pub fn prev(c: *C) ?EntryPtr {
                    c.assertValid();
                    if (c.node == null) c.retreat();
                    const out_node = c.node orelse return null;
                    const out_index = c.index;
                    c.retreat();
                    return &out_node.entries[out_index];
                }

                pub fn advance(c: *C) void {
                    c.assertValid();
                    const start = c.node orelse return;
                    if (!start.leaf) {
                        var n = childAt(start, c.index + 1);
                        while (!n.leaf) n = childAt(n, 0);
                        c.node = n;
                        c.index = 0;
                        return;
                    }
                    if (c.index + 1 < start.count()) {
                        c.index += 1;
                        return;
                    }
                    var n = start;
                    while (n.parent) |p| {
                        const pos = @as(usize, n.position);
                        if (pos < p.count()) {
                            c.node = p;
                            c.index = pos;
                            return;
                        }
                        n = p;
                    }
                    c.node = null;
                    c.index = 0;
                }

                pub fn retreat(c: *C) void {
                    c.assertValid();
                    const start = c.node orelse {
                        c.node = c.tree.rightmostNode();
                        if (c.node) |n| c.index = n.count() - 1;
                        return;
                    };
                    if (!start.leaf) {
                        var n = childAt(start, c.index);
                        while (!n.leaf) n = childAt(n, n.count());
                        c.node = n;
                        c.index = n.count() - 1;
                        return;
                    }
                    if (c.index > 0) {
                        c.index -= 1;
                        return;
                    }
                    var n = start;
                    while (n.parent) |p| {
                        const pos = @as(usize, n.position);
                        if (pos > 0) {
                            c.node = p;
                            c.index = pos - 1;
                            return;
                        }
                        n = p;
                    }
                    c.node = null;
                    c.index = 0;
                }

                fn assertValid(c: C) void {
                    if (config.check_iterator_generation and safetyChecks()) {
                        std.debug.assert(c.generation == c.tree.generation);
                    }
                }
            };
        }

        pub const Iterator = struct {
            cursor: Cursor,

            pub fn next(it: *Iterator) ?*Entry {
                return it.cursor.next();
            }
        };

        pub const ConstIterator = struct {
            cursor: ConstCursor,

            pub fn next(it: *ConstIterator) ?*const Entry {
                return it.cursor.next();
            }
        };

        pub const ReverseIterator = struct {
            cursor: Cursor,

            pub fn next(it: *ReverseIterator) ?*Entry {
                it.cursor.assertValid();
                const n = it.cursor.node orelse return null;
                const out_index = it.cursor.index;
                it.cursor.retreat();
                return &n.entries[out_index];
            }
        };

        pub const ConstReverseIterator = struct {
            cursor: ConstCursor,

            pub fn next(it: *ConstReverseIterator) ?*const Entry {
                it.cursor.assertValid();
                const n = it.cursor.node orelse return null;
                const out_index = it.cursor.index;
                it.cursor.retreat();
                return &n.entries[out_index];
            }
        };

        allocator: Allocator,
        context: Context,
        root: ?*Node = null,
        len_: usize = 0,
        generation: usize = 0,

        pub fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
                .context = defaultContext(Context),
            };
        }

        pub fn initContext(allocator: Allocator, context: Context) Self {
            return .{
                .allocator = allocator,
                .context = context,
            };
        }

        pub fn deinit(self: *Self) void {
            self.clear();
        }

        pub fn clear(self: *Self) void {
            if (self.root) |r| self.destroySubtree(r);
            self.root = null;
            self.len_ = 0;
            self.bumpGeneration();
        }

        pub fn len(self: *const Self) usize {
            return self.len_;
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.len_ == 0;
        }

        pub fn contains(self: *const Self, key_: Key) bool {
            return self.getEntryConst(key_) != null;
        }

        pub fn get(self: *Self, key_: Key) ?*Value {
            const e = self.getEntry(key_) orelse return null;
            return &e.value;
        }

        pub fn getConst(self: *const Self, key_: Key) ?*const Value {
            const c = self.findCursor(key_);
            const e = c.entry() orelse return null;
            return &e.value;
        }

        pub fn getEntry(self: *Self, key_: Key) ?*Entry {
            const c = self.findCursorMut(key_);
            return c.entry();
        }

        pub fn getEntryConst(self: *const Self, key_: Key) ?*const Entry {
            const c = self.findCursor(key_);
            return c.entry();
        }

        /// Insert only when key is absent.  Does not overwrite an existing value.
        pub fn insert(self: *Self, key_: Key, value_: Value) Allocator.Error!InsertResult {
            return self.insertEntry(.{ .key = key_, .value = value_ });
        }

        pub fn insertEntry(self: *Self, entry_: Entry) Allocator.Error!InsertResult {
            if (self.root == null) {
                const r = try self.newNode(true, null, 0);
                r.entries[0] = entry_;
                r.len = 1;
                self.root = r;
                self.len_ = 1;
                self.bumpGeneration();
                return .{ .entry = &r.entries[0], .inserted = true };
            }

            if (self.getEntry(entry_.key)) |existing| {
                return .{ .entry = existing, .inserted = false };
            }

            if (self.root.?.count() == max_slots) {
                try self.growRoot();
            }

            var n = self.root.?;
            while (!n.leaf) {
                var i = self.lowerBoundInNode(n, &entry_.key);
                var child = childAt(n, i);
                if (child.count() == max_slots) {
                    try self.splitChild(n, i);
                    if (self.less(&n.entries[i].key, &entry_.key)) {
                        i += 1;
                    }
                    child = childAt(n, i);
                }
                n = child;
            }

            const pos = self.lowerBoundInNode(n, &entry_.key);
            insertEntryAt(n, pos, entry_);
            self.len_ += 1;
            self.bumpGeneration();
            return .{ .entry = &n.entries[pos], .inserted = true };
        }

        /// Insert or replace.  Returns the old value when a replacement occurs.
        pub fn put(self: *Self, key_: Key, value_: Value) Allocator.Error!PutResult {
            const res = try self.insert(key_, value_);
            if (res.inserted) {
                return .{ .entry = res.entry, .inserted = true, .old_value = null };
            }
            const old = res.entry.value;
            res.entry.value = value_;
            self.bumpGeneration();
            return .{ .entry = res.entry, .inserted = false, .old_value = old };
        }

        /// Return a pointer to the value for key, inserting default_value if absent.
        pub fn getOrPutValue(self: *Self, key_: Key, default_value: Value) Allocator.Error!InsertResult {
            return self.insert(key_, default_value);
        }

        pub fn remove(self: *Self, key_: Key) bool {
            if (self.root == null) return false;
            if (self.getEntry(key_) == null) return false;
            const removed = self.deleteFromNode(self.root.?, &key_);
            std.debug.assert(removed);
            self.len_ -= 1;
            self.fixRootAfterDelete();
            self.bumpGeneration();
            return removed;
        }

        /// Remove a key and return the erased key/value pair when present.
        pub fn fetchRemove(self: *Self, key_: Key) ?FetchRemoveResult {
            const entry_ = self.getEntry(key_) orelse return null;
            const out: FetchRemoveResult = .{ .key = entry_.key, .value = entry_.value };
            const removed = self.remove(key_);
            std.debug.assert(removed);
            return out;
        }

        /// Lower bound: first entry whose key is not less than key.
        pub fn lowerBound(self: *const Self, key_: Key) ConstCursor {
            var last_internal: ?struct { node: *const Node, index: usize } = null;
            var n_opt = self.root;
            while (n_opt) |n| {
                const i = self.lowerBoundInNode(n, &key_);
                if (i < n.count()) last_internal = .{ .node = n, .index = i };
                if (n.leaf) {
                    if (i < n.count()) return self.cursorAtConst(n, i);
                    if (last_internal) |li| return self.cursorAtConst(li.node, li.index);
                    return self.endCursor();
                }
                n_opt = childAt(n, i);
            }
            return self.endCursor();
        }

        /// Mutable lower bound variant for callers that need to edit values in place.
        pub fn lowerBoundMut(self: *Self, key_: Key) Cursor {
            var last_internal: ?struct { node: *Node, index: usize } = null;
            var n_opt = self.root;
            while (n_opt) |n| {
                const i = self.lowerBoundInNode(n, &key_);
                if (i < n.count()) last_internal = .{ .node = n, .index = i };
                if (n.leaf) {
                    if (i < n.count()) return self.cursorAt(n, i);
                    if (last_internal) |li| return self.cursorAt(li.node, li.index);
                    return self.endCursorMut();
                }
                n_opt = childAt(n, i);
            }
            return self.endCursorMut();
        }

        /// Upper bound: first entry whose key is greater than key.
        pub fn upperBound(self: *const Self, key_: Key) ConstCursor {
            var last_internal: ?struct { node: *const Node, index: usize } = null;
            var n_opt = self.root;
            while (n_opt) |n| {
                const i = self.upperBoundInNode(n, &key_);
                if (i < n.count()) last_internal = .{ .node = n, .index = i };
                if (n.leaf) {
                    if (i < n.count()) return self.cursorAtConst(n, i);
                    if (last_internal) |li| return self.cursorAtConst(li.node, li.index);
                    return self.endCursor();
                }
                n_opt = childAt(n, i);
            }
            return self.endCursor();
        }

        /// Mutable upper bound variant for callers that need to edit values in place.
        pub fn upperBoundMut(self: *Self, key_: Key) Cursor {
            var last_internal: ?struct { node: *Node, index: usize } = null;
            var n_opt = self.root;
            while (n_opt) |n| {
                const i = self.upperBoundInNode(n, &key_);
                if (i < n.count()) last_internal = .{ .node = n, .index = i };
                if (n.leaf) {
                    if (i < n.count()) return self.cursorAt(n, i);
                    if (last_internal) |li| return self.cursorAt(li.node, li.index);
                    return self.endCursorMut();
                }
                n_opt = childAt(n, i);
            }
            return self.endCursorMut();
        }

        pub fn findCursor(self: *const Self, key_: Key) ConstCursor {
            var n_opt = self.root;
            while (n_opt) |n| {
                const i = self.lowerBoundInNode(n, &key_);
                if (i < n.count() and self.keysEqual(&key_, &n.entries[i].key)) {
                    return self.cursorAtConst(n, i);
                }
                if (n.leaf) break;
                n_opt = childAt(n, i);
            }
            return self.endCursor();
        }

        pub fn findCursorMut(self: *Self, key_: Key) Cursor {
            var n_opt = self.root;
            while (n_opt) |n| {
                const i = self.lowerBoundInNode(n, &key_);
                if (i < n.count() and self.keysEqual(&key_, &n.entries[i].key)) {
                    return self.cursorAt(n, i);
                }
                if (n.leaf) break;
                n_opt = childAt(n, i);
            }
            return self.endCursorMut();
        }

        pub fn iterator(self: *Self) Iterator {
            return .{ .cursor = self.beginCursor() };
        }

        pub fn constIterator(self: *const Self) ConstIterator {
            return .{ .cursor = self.beginConstCursor() };
        }

        pub fn reverseIterator(self: *Self) ReverseIterator {
            const n = self.rightmostNode() orelse return .{ .cursor = self.endCursorMut() };
            return .{ .cursor = self.cursorAt(n, n.count() - 1) };
        }

        pub fn constReverseIterator(self: *const Self) ConstReverseIterator {
            const n = self.rightmostNode() orelse return .{ .cursor = self.endCursor() };
            return .{ .cursor = self.cursorAtConst(n, n.count() - 1) };
        }

        pub fn beginCursor(self: *Self) Cursor {
            const n = self.leftmostNode() orelse return self.endCursorMut();
            return self.cursorAt(n, 0);
        }

        pub fn beginConstCursor(self: *const Self) ConstCursor {
            const n = self.leftmostNode() orelse return self.endCursor();
            return self.cursorAtConst(n, 0);
        }

        pub fn endCursor(self: *const Self) ConstCursor {
            return .{ .tree = self, .node = null, .index = 0, .generation = self.generation };
        }

        pub fn endCursorMut(self: *Self) Cursor {
            return .{ .tree = self, .node = null, .index = 0, .generation = self.generation };
        }

        pub fn height(self: *const Self) usize {
            var h: usize = 0;
            var n_opt = self.root;
            while (n_opt) |n| {
                h += 1;
                if (n.leaf) break;
                n_opt = childAt(n, 0);
            }
            return h;
        }

        pub fn stats(self: *const Self) Stats {
            var leaf_nodes: usize = 0;
            var internal_nodes: usize = 0;
            if (self.root) |r| countNodes(r, &leaf_nodes, &internal_nodes);
            const node_count = leaf_nodes + internal_nodes;
            const cap = node_count * max_slots;
            return .{
                .len = self.len_,
                .height = self.height(),
                .nodes = node_count,
                .leaf_nodes = leaf_nodes,
                .internal_nodes = internal_nodes,
                .max_node_slots = max_slots,
                .min_node_slots = min_slots,
                .fullness = if (cap == 0) 0 else @as(f64, @floatFromInt(self.len_)) / @as(f64, @floatFromInt(cap)),
                .bytes_used = @sizeOf(Self) + node_count * @sizeOf(Node),
            };
        }

        /// Expensive invariant verifier intended for tests and debug builds.
        pub fn validate(self: *const Self) void {
            if (self.root == null) {
                std.debug.assert(self.len_ == 0);
                return;
            }
            const r = self.root.?;
            std.debug.assert(r.parent == null);
            std.debug.assert(r.count() > 0);
            std.debug.assert(r.count() <= max_slots);
            var counted: usize = 0;
            const leaf_depth = validateNode(self, r, null, null, 1, &counted);
            _ = leaf_depth;
            std.debug.assert(counted == self.len_);
        }

        fn cursorAt(self: *Self, n: *Node, index: usize) Cursor {
            return .{ .tree = self, .node = n, .index = index, .generation = self.generation };
        }

        fn cursorAtConst(self: *const Self, n: *const Node, index: usize) ConstCursor {
            return .{ .tree = self, .node = n, .index = index, .generation = self.generation };
        }

        fn newNode(self: *Self, leaf: bool, parent: ?*Node, position: usize) Allocator.Error!*Node {
            const n = try self.allocator.create(Node);
            n.* = Node.init(leaf, parent, position);
            return n;
        }

        fn destroySubtree(self: *Self, n: *Node) void {
            if (!n.leaf) {
                var i: usize = 0;
                while (i <= n.count()) : (i += 1) {
                    if (n.children[i]) |c| self.destroySubtree(c);
                }
            }
            self.allocator.destroy(n);
        }

        fn growRoot(self: *Self) Allocator.Error!void {
            const old = self.root.?;
            const new_root = try self.newNode(false, null, 0);
            old.parent = new_root;
            old.position = 0;
            new_root.children[0] = old;
            self.root = new_root;
            self.splitChild(new_root, 0) catch |err| {
                old.parent = null;
                old.position = 0;
                new_root.children[0] = null;
                self.root = old;
                self.allocator.destroy(new_root);
                return err;
            };
        }

        fn splitChild(self: *Self, parent: *Node, child_index: usize) Allocator.Error!void {
            std.debug.assert(parent.count() < max_slots);
            const left = childAt(parent, child_index);
            std.debug.assert(left.count() == max_slots);

            const right = try self.newNode(left.leaf, parent, child_index + 1);
            const median_index = min_slots;
            const right_len = max_slots - median_index - 1;

            var j: usize = 0;
            while (j < right_len) : (j += 1) {
                right.entries[j] = left.entries[median_index + 1 + j];
            }
            if (!left.leaf) {
                j = 0;
                while (j <= right_len) : (j += 1) {
                    const c = left.children[median_index + 1 + j].?;
                    right.children[j] = c;
                    c.parent = right;
                    c.position = narrowPos(j);
                    left.children[median_index + 1 + j] = null;
                }
            }
            right.len = narrowLen(right_len);
            const median = left.entries[median_index];
            left.len = narrowLen(median_index);

            shiftChildrenRight(parent, child_index + 1);
            parent.children[child_index + 1] = right;
            shiftEntriesRight(parent, child_index);
            parent.entries[child_index] = median;
            parent.len += 1;
            fixChildPositions(parent, child_index + 1);
        }

        fn deleteFromNode(self: *Self, n: *Node, key_: *const Key) bool {
            const idx = self.lowerBoundInNode(n, key_);
            if (idx < n.count() and self.keysEqual(key_, &n.entries[idx].key)) {
                if (n.leaf) {
                    removeEntryAt(n, idx);
                    return true;
                }
                return self.deleteFromInternal(n, idx);
            }
            if (n.leaf) return false;

            const child_index = idx;
            var child = childAt(n, child_index);
            if (child.count() == min_slots) {
                child = self.fillChild(n, child_index);
            }
            return self.deleteFromNode(child, key_);
        }

        fn deleteFromInternal(self: *Self, n: *Node, idx: usize) bool {
            const left = childAt(n, idx);
            const right = childAt(n, idx + 1);
            if (left.count() > min_slots) {
                n.entries[idx] = self.removeMax(left);
                return true;
            }
            if (right.count() > min_slots) {
                n.entries[idx] = self.removeMin(right);
                return true;
            }
            const merged = self.mergeChildren(n, idx);
            _ = removeEntryAtKnownPresent(self, merged, min_slots);
            return true;
        }

        fn removeMin(self: *Self, start: *Node) Entry {
            var n = start;
            while (!n.leaf) {
                var child = childAt(n, 0);
                if (child.count() == min_slots) {
                    child = self.fillChild(n, 0);
                }
                n = child;
            }
            const out = n.entries[0];
            removeEntryAt(n, 0);
            return out;
        }

        fn removeMax(self: *Self, start: *Node) Entry {
            var n = start;
            while (!n.leaf) {
                const idx = n.count();
                var child = childAt(n, idx);
                if (child.count() == min_slots) {
                    child = self.fillChild(n, idx);
                }
                n = child;
            }
            const last = n.count() - 1;
            const out = n.entries[last];
            n.len -= 1;
            return out;
        }

        fn removeEntryAtKnownPresent(self: *Self, n: *Node, idx: usize) bool {
            if (n.leaf) {
                removeEntryAt(n, idx);
                return true;
            }
            return self.deleteFromInternal(n, idx);
        }

        /// Ensure child at idx has more than min_slots keys before descent.
        /// Returns the node that should be descended into after possible merge.
        fn fillChild(self: *Self, parent: *Node, idx: usize) *Node {
            std.debug.assert(idx <= parent.count());
            const child = childAt(parent, idx);
            std.debug.assert(child.count() == min_slots);

            if (idx > 0) {
                const left = childAt(parent, idx - 1);
                if (left.count() > min_slots) {
                    borrowFromLeft(parent, idx);
                    return childAt(parent, idx);
                }
            }
            if (idx < parent.count()) {
                const right = childAt(parent, idx + 1);
                if (right.count() > min_slots) {
                    borrowFromRight(parent, idx);
                    return childAt(parent, idx);
                }
            }
            if (idx < parent.count()) {
                return self.mergeChildren(parent, idx);
            }
            return self.mergeChildren(parent, idx - 1);
        }

        fn mergeChildren(self: *Self, parent: *Node, left_index: usize) *Node {
            const left = childAt(parent, left_index);
            const right = childAt(parent, left_index + 1);
            std.debug.assert(left.leaf == right.leaf);
            std.debug.assert(left.count() + 1 + right.count() <= max_slots);

            const left_len = left.count();
            left.entries[left_len] = parent.entries[left_index];
            var j: usize = 0;
            while (j < right.count()) : (j += 1) {
                left.entries[left_len + 1 + j] = right.entries[j];
            }
            if (!left.leaf) {
                j = 0;
                while (j <= right.count()) : (j += 1) {
                    const c = right.children[j].?;
                    left.children[left_len + 1 + j] = c;
                    c.parent = left;
                    c.position = narrowPos(left_len + 1 + j);
                }
            }
            left.len = narrowLen(left_len + 1 + right.count());

            removeEntryAt(parent, left_index);
            removeChildAt(parent, left_index + 1);
            self.allocator.destroy(right);
            fixChildPositions(parent, left_index);
            return left;
        }

        fn fixRootAfterDelete(self: *Self) void {
            const r = self.root orelse return;
            if (r.count() != 0) return;
            if (r.leaf) {
                self.allocator.destroy(r);
                self.root = null;
                return;
            }
            const child = childAt(r, 0);
            child.parent = null;
            child.position = 0;
            self.root = child;
            self.allocator.destroy(r);
        }

        fn lowerBoundInNode(self: *const Self, n: *const Node, key_: *const Key) usize {
            if (max_slots <= config.linear_search_threshold) {
                var i: usize = 0;
                while (i < n.count() and self.less(&n.entries[i].key, key_)) : (i += 1) {}
                return i;
            }
            var lo: usize = 0;
            var hi: usize = n.count();
            while (lo < hi) {
                const mid = (lo + hi) / 2;
                if (self.less(&n.entries[mid].key, key_)) lo = mid + 1 else hi = mid;
            }
            return lo;
        }

        fn upperBoundInNode(self: *const Self, n: *const Node, key_: *const Key) usize {
            if (max_slots <= config.linear_search_threshold) {
                var i: usize = 0;
                while (i < n.count() and !self.less(key_, &n.entries[i].key)) : (i += 1) {}
                return i;
            }
            var lo: usize = 0;
            var hi: usize = n.count();
            while (lo < hi) {
                const mid = (lo + hi) / 2;
                if (!self.less(key_, &n.entries[mid].key)) lo = mid + 1 else hi = mid;
            }
            return lo;
        }

        fn leftmostNode(self: *const Self) ?*Node {
            var n = self.root orelse return null;
            while (!n.leaf) n = childAt(n, 0);
            return n;
        }

        fn rightmostNode(self: *const Self) ?*Node {
            var n = self.root orelse return null;
            while (!n.leaf) n = childAt(n, n.count());
            return n;
        }

        fn less(self: *const Self, a: *const Key, b: *const Key) bool {
            return compare(&self.context, a, b) == .lt;
        }

        fn keysEqual(self: *const Self, a: *const Key, b: *const Key) bool {
            return compare(&self.context, a, b) == .eq;
        }

        fn bumpGeneration(self: *Self) void {
            self.generation +%= 1;
        }
    };
}

pub fn BTreeSet(
    comptime Key: type,
    comptime Context: type,
    comptime compare: fn (*const Context, *const Key, *const Key) std.math.Order,
    comptime config: Config,
) type {
    const Map = BTreeMap(Key, void, Context, compare, config);
    return struct {
        const Self = @This();
        pub const key_type = Key;
        pub const context_type = Context;
        pub const max_node_slots = Map.max_node_slots;
        pub const min_node_slots = Map.min_node_slots;

        pub const InsertResult = struct {
            key: *const Key,
            inserted: bool,
        };

        pub const Iterator = struct {
            inner: Map.ConstIterator,

            pub fn next(it: *Iterator) ?*const Key {
                const e = it.inner.next() orelse return null;
                return &e.key;
            }
        };

        pub const ReverseIterator = struct {
            inner: Map.ConstReverseIterator,

            pub fn next(it: *ReverseIterator) ?*const Key {
                const e = it.inner.next() orelse return null;
                return &e.key;
            }
        };

        pub const Cursor = struct {
            inner: Map.ConstCursor,

            pub fn isEnd(c: Cursor) bool {
                return c.inner.isEnd();
            }

            pub fn key(c: Cursor) ?*const Key {
                return c.inner.key();
            }

            pub fn next(c: *Cursor) ?*const Key {
                const e = c.inner.next() orelse return null;
                return &e.key;
            }

            pub fn prev(c: *Cursor) ?*const Key {
                const e = c.inner.prev() orelse return null;
                return &e.key;
            }

            pub fn advance(c: *Cursor) void {
                c.inner.advance();
            }

            pub fn retreat(c: *Cursor) void {
                c.inner.retreat();
            }
        };

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

        pub fn len(self: *const Self) usize {
            return self.map.len();
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.map.isEmpty();
        }

        pub fn contains(self: *const Self, key_: Key) bool {
            return self.map.contains(key_);
        }

        pub fn insert(self: *Self, key_: Key) std.mem.Allocator.Error!InsertResult {
            const res = try self.map.insert(key_, {});
            return .{ .key = &res.entry.key, .inserted = res.inserted };
        }

        pub fn remove(self: *Self, key_: Key) bool {
            return self.map.remove(key_);
        }

        pub fn fetchRemove(self: *Self, key_: Key) ?Key {
            const removed = self.map.fetchRemove(key_) orelse return null;
            return removed.key;
        }

        pub fn lowerBound(self: *const Self, key_: Key) Cursor {
            return .{ .inner = self.map.lowerBound(key_) };
        }

        pub fn upperBound(self: *const Self, key_: Key) Cursor {
            return .{ .inner = self.map.upperBound(key_) };
        }

        pub fn findCursor(self: *const Self, key_: Key) Cursor {
            return .{ .inner = self.map.findCursor(key_) };
        }

        pub fn iterator(self: *const Self) Iterator {
            return .{ .inner = self.map.constIterator() };
        }

        pub fn reverseIterator(self: *const Self) ReverseIterator {
            return .{ .inner = self.map.constReverseIterator() };
        }

        pub fn beginCursor(self: *const Self) Cursor {
            return .{ .inner = self.map.beginConstCursor() };
        }

        pub fn endCursor(self: *const Self) Cursor {
            return .{ .inner = self.map.endCursor() };
        }

        pub fn height(self: *const Self) usize {
            return self.map.height();
        }

        pub fn stats(self: *const Self) Map.Stats {
            return self.map.stats();
        }

        pub fn validate(self: *const Self) void {
            self.map.validate();
        }
    };
}

fn defaultCompare(comptime T: type) fn (*const void, *const T, *const T) std.math.Order {
    return struct {
        fn cmp(_: *const void, a: *const T, b: *const T) std.math.Order {
            if (comptime isByteSlice(T)) {
                return std.mem.order(u8, a.*, b.*);
            }
            if (a.* < b.*) return .lt;
            if (a.* > b.*) return .gt;
            return .eq;
        }
    }.cmp;
}

fn isByteSlice(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .pointer => |p| p.size == .slice and p.child == u8,
        else => false,
    };
}

fn defaultContext(comptime Context: type) Context {
    return switch (@typeInfo(Context)) {
        .void => {},
        else => @compileError("Use initContext() for a non-void BTree comparator context."),
    };
}

fn deriveMaxSlots(comptime Entry: type, comptime config: Config) usize {
    const ptr_size = @sizeOf(?*anyopaque);
    const entry_size = @max(@sizeOf(Entry), 1);
    const header_estimate = ptr_size + @sizeOf(u16) * 2 + @sizeOf(bool) + ptr_size;
    const per_slot_estimate = entry_size + ptr_size;
    var raw: usize = if (config.target_node_size > header_estimate)
        (config.target_node_size - header_estimate) / per_slot_estimate
    else
        0;
    raw = @max(raw, config.min_max_slots);
    raw = @min(raw, config.max_max_slots);
    if (raw < 3) raw = 3;
    if (raw % 2 == 0) raw -= 1;
    if (raw < 3) raw = 3;
    if (raw > std.math.maxInt(u16) - 1) raw = std.math.maxInt(u16) - 1;
    return raw;
}

fn safetyChecks() bool {
    return switch (@import("builtin").mode) {
        .Debug, .ReleaseSafe => true,
        else => false,
    };
}

fn narrowLen(x: usize) u16 {
    std.debug.assert(x <= std.math.maxInt(u16));
    return @as(u16, @intCast(x));
}

fn narrowPos(x: usize) u16 {
    std.debug.assert(x <= std.math.maxInt(u16));
    return @as(u16, @intCast(x));
}

fn childAt(n: anytype, idx: usize) *@TypeOf(n.*) {
    std.debug.assert(idx <= @as(usize, n.len));
    return n.children[idx].?;
}

fn shiftEntriesRight(n: anytype, start: usize) void {
    var i = @as(usize, n.len);
    while (i > start) : (i -= 1) {
        n.entries[i] = n.entries[i - 1];
    }
}

fn shiftChildrenRight(n: anytype, start: usize) void {
    var i = @as(usize, n.len) + 1;
    while (i > start) : (i -= 1) {
        n.children[i] = n.children[i - 1];
    }
}

fn insertEntryAt(n: anytype, idx: usize, entry: @TypeOf(n.entries[0])) void {
    std.debug.assert(@as(usize, n.len) < n.entries.len);
    shiftEntriesRight(n, idx);
    n.entries[idx] = entry;
    n.len += 1;
}

fn removeEntryAt(n: anytype, idx: usize) void {
    std.debug.assert(idx < @as(usize, n.len));
    var i = idx;
    while (i + 1 < @as(usize, n.len)) : (i += 1) {
        n.entries[i] = n.entries[i + 1];
    }
    n.len -= 1;
}

fn removeChildAt(n: anytype, idx: usize) void {
    std.debug.assert(idx <= @as(usize, n.len) + 1);
    var i = idx;
    while (i + 1 <= @as(usize, n.len) + 1) : (i += 1) {
        n.children[i] = n.children[i + 1];
    }
    n.children[@as(usize, n.len) + 1] = null;
}

fn fixChildPositions(parent: anytype, start: usize) void {
    if (parent.leaf) return;
    var i = start;
    while (i <= @as(usize, parent.len)) : (i += 1) {
        if (parent.children[i]) |c| {
            c.parent = parent;
            c.position = narrowPos(i);
        }
    }
}

fn borrowFromLeft(parent: anytype, idx: usize) void {
    const child = childAt(parent, idx);
    const left = childAt(parent, idx - 1);
    std.debug.assert(left.count() > parentContextMinSlots(parent));
    std.debug.assert(child.count() < child.entries.len);

    shiftEntriesRight(child, 0);
    if (!child.leaf) {
        shiftChildrenRight(child, 0);
        const moved_child = left.children[left.count()].?;
        child.children[0] = moved_child;
        moved_child.parent = child;
        moved_child.position = 0;
        left.children[left.count()] = null;
    }
    child.entries[0] = parent.entries[idx - 1];
    child.len += 1;
    parent.entries[idx - 1] = left.entries[left.count() - 1];
    left.len -= 1;
    fixChildPositions(child, 0);
}

fn borrowFromRight(parent: anytype, idx: usize) void {
    const child = childAt(parent, idx);
    const right = childAt(parent, idx + 1);
    std.debug.assert(right.count() > parentContextMinSlots(parent));
    std.debug.assert(child.count() < child.entries.len);

    const child_len = child.count();
    child.entries[child_len] = parent.entries[idx];
    if (!child.leaf) {
        const moved_child = right.children[0].?;
        child.children[child_len + 1] = moved_child;
        moved_child.parent = child;
        moved_child.position = narrowPos(child_len + 1);
    }
    child.len += 1;
    parent.entries[idx] = right.entries[0];
    removeEntryAt(right, 0);
    if (!right.leaf) {
        var j: usize = 0;
        while (j <= right.count()) : (j += 1) {
            right.children[j] = right.children[j + 1];
        }
        right.children[right.count() + 1] = null;
        fixChildPositions(right, 0);
    }
}

fn parentContextMinSlots(parent: anytype) usize {
    return parent.entries.len / 2;
}

fn countNodes(n: anytype, leaf_nodes: *usize, internal_nodes: *usize) void {
    if (n.leaf) {
        leaf_nodes.* += 1;
        return;
    }
    internal_nodes.* += 1;
    var i: usize = 0;
    while (i <= n.count()) : (i += 1) countNodes(childAt(n, i), leaf_nodes, internal_nodes);
}

fn validateNode(
    tree: anytype,
    n: anytype,
    lo: ?*const @TypeOf(n.entries[0].key),
    hi: ?*const @TypeOf(n.entries[0].key),
    depth: usize,
    counted: *usize,
) usize {
    std.debug.assert(n.count() > 0);
    std.debug.assert(n.count() <= n.entries.len);
    if (n.parent != null) std.debug.assert(n.count() >= n.entries.len / 2);
    if (lo) |l| std.debug.assert(tree.less(l, &n.entries[0].key));
    if (hi) |h| std.debug.assert(tree.less(&n.entries[n.count() - 1].key, h));
    var i: usize = 1;
    while (i < n.count()) : (i += 1) {
        std.debug.assert(tree.less(&n.entries[i - 1].key, &n.entries[i].key));
    }
    counted.* += n.count();
    if (n.leaf) return depth;

    var leaf_depth: ?usize = null;
    i = 0;
    while (i <= n.count()) : (i += 1) {
        const c = childAt(n, i);
        std.debug.assert(c.parent == n);
        std.debug.assert(@as(usize, c.position) == i);
        const child_lo = if (i == 0) lo else &n.entries[i - 1].key;
        const child_hi = if (i == n.count()) hi else &n.entries[i].key;
        const d = validateNode(tree, c, child_lo, child_hi, depth + 1, counted);
        if (leaf_depth) |expected| std.debug.assert(expected == d) else leaf_depth = d;
    }
    return leaf_depth.?;
}

// Focused unit tests.  More stress tests are in test/btree_stress.zig.
test "BTreeMap ordered insert, lookup, iteration, lower bound" {
    const testing = std.testing;
    var map = AutoBTreeMapWithConfig(u32, u32, .{ .target_node_size = 128 }).init(testing.allocator);
    defer map.deinit();

    var i: u32 = 100;
    while (i > 0) {
        i -= 1;
        const res = try map.insert(i, i * 10);
        try testing.expect(res.inserted);
    }
    try testing.expectEqual(@as(usize, 100), map.len());
    map.validate();

    i = 0;
    var it = map.iterator();
    while (it.next()) |e| : (i += 1) {
        try testing.expectEqual(i, e.key);
        try testing.expectEqual(i * 10, e.value);
    }
    try testing.expectEqual(@as(u32, 100), i);

    var c = map.lowerBound(42);
    try testing.expect(c.entry() != null);
    try testing.expectEqual(@as(u32, 42), c.entry().?.key);
    c = map.upperBound(42);
    try testing.expectEqual(@as(u32, 43), c.entry().?.key);
}

test "BTreeMap put replaces and remove rebalances" {
    const testing = std.testing;
    var map = AutoBTreeMapWithConfig(u32, u64, .{ .target_node_size = 128 }).init(testing.allocator);
    defer map.deinit();

    var i: u32 = 0;
    while (i < 1000) : (i += 1) _ = try map.put(i, i);
    map.validate();

    const p = try map.put(10, 9999);
    try testing.expect(!p.inserted);
    try testing.expectEqual(@as(u64, 10), p.old_value.?);
    try testing.expectEqual(@as(u64, 9999), map.get(10).?.*);

    i = 0;
    while (i < 1000) : (i += 2) {
        try testing.expect(map.remove(i));
        if (i % 64 == 0) map.validate();
    }
    map.validate();
    try testing.expectEqual(@as(usize, 500), map.len());
    i = 0;
    while (i < 1000) : (i += 1) {
        const present = map.contains(i);
        try testing.expectEqual(i % 2 == 1, present);
    }
}

test "BTreeMap duplicate insert does not split full root" {
    const testing = std.testing;
    const Map = AutoBTreeMapWithConfig(u32, u32, .{ .target_node_size = 128 });
    var map = Map.init(testing.allocator);
    defer map.deinit();

    var i: u32 = 0;
    while (i < Map.max_node_slots) : (i += 1) {
        try testing.expect((try map.insert(i, i)).inserted);
    }
    map.validate();
    try testing.expectEqual(@as(usize, 1), map.stats().nodes);

    const duplicate = try map.insert(0, 999);
    try testing.expect(!duplicate.inserted);
    try testing.expectEqual(@as(u32, 0), duplicate.entry.value);
    try testing.expectEqual(@as(usize, 1), map.stats().nodes);
    map.validate();
}

test "BTreeMap absent remove does not rebalance" {
    const testing = std.testing;
    const Map = AutoBTreeMapWithConfig(u32, u32, .{ .target_node_size = 128 });
    var map = Map.init(testing.allocator);
    defer map.deinit();

    var i: u32 = 0;
    while (i < 2000) : (i += 1) {
        try testing.expect((try map.insert(i * 2, i)).inserted);
    }
    map.validate();
    const before = map.stats();

    try testing.expect(!map.remove(3333));
    try testing.expectEqual(before.len, map.stats().len);
    try testing.expectEqual(before.nodes, map.stats().nodes);
    map.validate();
}

test "BTreeMap fetchRemove returns erased entry" {
    const testing = std.testing;
    var map = AutoBTreeMapWithConfig(u32, u64, .{ .target_node_size = 128 }).init(testing.allocator);
    defer map.deinit();

    _ = try map.put(10, 100);
    _ = try map.put(20, 200);
    const removed = map.fetchRemove(10).?;
    try testing.expectEqual(@as(u32, 10), removed.key);
    try testing.expectEqual(@as(u64, 100), removed.value);
    try testing.expect(!map.contains(10));
    try testing.expect(map.fetchRemove(10) == null);
    map.validate();
}

fn allocationFailureInsertScenario(allocator: std.mem.Allocator) !void {
    const Map = AutoBTreeMapWithConfig(u32, u32, .{ .target_node_size = 128 });
    var map = Map.init(allocator);
    defer map.deinit();

    var i: u32 = 0;
    while (i < 250) : (i += 1) {
        _ = try map.insert(i, i * 3);
        map.validate();
    }
}

test "BTreeMap handles allocation failures without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        allocationFailureInsertScenario,
        .{},
    );
}

const ReverseContext = struct { reverse: bool };

fn compareWithDirection(ctx: *const ReverseContext, a: *const i32, b: *const i32) std.math.Order {
    if (a.* == b.*) return .eq;
    const natural: std.math.Order = if (a.* < b.*) .lt else .gt;
    if (!ctx.reverse) return natural;
    return if (natural == .lt) .gt else .lt;
}

test "BTreeMap supports comparator context" {
    const testing = std.testing;
    const Map = BTreeMap(i32, i32, ReverseContext, compareWithDirection, .{ .target_node_size = 128 });
    var map = Map.initContext(testing.allocator, .{ .reverse = true });
    defer map.deinit();

    for ([_]i32{ 1, 4, 2, 3 }) |key_| {
        try testing.expect((try map.insert(key_, key_ * 10)).inserted);
    }
    map.validate();

    var it = map.iterator();
    for ([_]i32{ 4, 3, 2, 1 }) |expected| {
        const entry = it.next().?;
        try testing.expectEqual(expected, entry.key);
        try testing.expectEqual(expected * 10, entry.value);
    }
    try testing.expect(it.next() == null);
    try testing.expectEqual(@as(i32, 3), map.lowerBound(3).entry().?.key);
    try testing.expectEqual(@as(i32, 2), map.upperBound(3).entry().?.key);
}

test "BTreeMap supports byte slice keys" {
    const testing = std.testing;
    var map = AutoBTreeMap([]const u8, u32).init(testing.allocator);
    defer map.deinit();

    _ = try map.put("bravo", 2);
    _ = try map.put("alpha", 1);
    _ = try map.put("charlie", 3);
    map.validate();

    try testing.expectEqual(@as(u32, 1), map.get("alpha").?.*);
    try testing.expectEqualStrings("bravo", map.lowerBound("blue").entry().?.key);
    try testing.expectEqualStrings("charlie", map.upperBound("bravo").entry().?.key);
}

test "BTreeMap const and mutable cursor variants" {
    const testing = std.testing;
    var map = AutoBTreeMapWithConfig(u32, u32, .{ .target_node_size = 128 }).init(testing.allocator);
    defer map.deinit();

    _ = try map.put(1, 10);
    _ = try map.put(2, 20);

    const const_map = &map;
    var cit = const_map.constIterator();
    const first = cit.next().?;
    try testing.expectEqual(@as(u32, 1), first.key);
    try testing.expectEqual(@as(u32, 10), first.value);

    const c = const_map.lowerBound(2);
    try testing.expectEqual(@as(u32, 20), c.value().?.*);

    const m = map.lowerBoundMut(2);
    m.value().?.* = 200;
    try testing.expectEqual(@as(u32, 200), map.get(2).?.*);
}

test "BTreeSet smoke" {
    const testing = std.testing;
    var set = AutoBTreeSetWithConfig(i32, .{ .target_node_size = 128 }).init(testing.allocator);
    defer set.deinit();
    try testing.expect((try set.insert(5)).inserted);
    try testing.expect(!(try set.insert(5)).inserted);
    try testing.expect(set.contains(5));
    try testing.expect(set.remove(5));
    try testing.expect(!set.contains(5));
    try testing.expect((try set.insert(7)).inserted);
    try testing.expectEqual(@as(i32, 7), set.fetchRemove(7).?);
    try testing.expect(set.fetchRemove(7) == null);
    set.validate();
}

test "BTreeSet bounds and reverse iteration" {
    const testing = std.testing;
    var set = AutoBTreeSetWithConfig(i32, .{ .target_node_size = 128 }).init(testing.allocator);
    defer set.deinit();

    for ([_]i32{ 10, 30, 20, 40 }) |key_| {
        try testing.expect((try set.insert(key_)).inserted);
    }
    set.validate();

    var lb = set.lowerBound(25);
    try testing.expectEqual(@as(i32, 30), lb.key().?.*);
    var ub = set.upperBound(30);
    try testing.expectEqual(@as(i32, 40), ub.key().?.*);

    var rit = set.reverseIterator();
    for ([_]i32{ 40, 30, 20, 10 }) |expected| {
        try testing.expectEqual(expected, rit.next().?.*);
    }
    try testing.expect(rit.next() == null);

    try testing.expectEqual(@as(usize, 4), set.stats().len);
    try testing.expect(set.height() > 0);
}
