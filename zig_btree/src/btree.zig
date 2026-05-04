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
    /// Upper bound on max keys per node.  Kept within u8 position fields.
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

        const internal_node_max_count: u8 = 0;

        const NodeHeader = extern struct {
            parent: ?*Node,
            position: u8,
            start: u8,
            finish: u8,
            max_count: u8,
        };

        const Node = struct {
            header: NodeHeader,
            entries: [max_slots]Entry,

            fn init(leaf: bool, parent: ?*Node, position: usize) Node {
                return .{
                    .header = .{
                        .parent = parent,
                        .position = narrowPos(position),
                        .start = 0,
                        .finish = 0,
                        .max_count = if (leaf) narrowLen(max_slots) else internal_node_max_count,
                    },
                    .entries = undefined,
                };
            }

            fn isLeaf(n: *const Node) bool {
                return n.header.max_count != internal_node_max_count;
            }

            fn count(n: *const Node) usize {
                std.debug.assert(n.header.finish >= n.header.start);
                return @as(usize, n.header.finish - n.header.start);
            }

            fn capacity(n: *const Node) usize {
                return if (n.isLeaf()) @as(usize, n.header.max_count) else max_slots;
            }

            fn childArray(n: *Node) *[max_slots + 1]?*Node {
                std.debug.assert(!n.isLeaf());
                const internal: *InternalNode = @fieldParentPtr("node", n);
                return &internal.children;
            }

            fn childArrayConst(n: *const Node) *const [max_slots + 1]?*Node {
                std.debug.assert(!n.isLeaf());
                const internal: *const InternalNode = @fieldParentPtr("node", n);
                return &internal.children;
            }
        };

        const InternalNode = struct {
            node: Node,
            children: [max_slots + 1]?*Node,
        };

        pub const node_entry_offset: usize = @offsetOf(Node, "entries");
        pub const leaf_node_size: usize = @sizeOf(Node);
        pub const internal_node_size: usize = @sizeOf(InternalNode);
        pub const child_array_offset: usize = @offsetOf(InternalNode, "children");

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

                pub inline fn isEnd(c: C) bool {
                    return c.node == null;
                }

                pub inline fn entry(c: C) ?EntryPtr {
                    c.assertValid();
                    const n = c.node orelse return null;
                    return &n.entries[c.index];
                }

                pub inline fn key(c: C) ?KeyPtr {
                    const e = c.entry() orelse return null;
                    return &e.key;
                }

                pub inline fn value(c: C) ?ValuePtr {
                    const e = c.entry() orelse return null;
                    return &e.value;
                }

                pub inline fn next(c: *C) ?EntryPtr {
                    c.assertValid();
                    const n = c.node orelse return null;
                    const out = &n.entries[c.index];
                    c.advance();
                    return out;
                }

                pub inline fn prev(c: *C) ?EntryPtr {
                    c.assertValid();
                    if (c.node == null) c.retreat();
                    const out_node = c.node orelse return null;
                    const out_index = c.index;
                    c.retreat();
                    return &out_node.entries[out_index];
                }

                pub inline fn advance(c: *C) void {
                    c.assertValid();
                    const start = c.node orelse return;
                    if (!start.isLeaf()) {
                        var n = childAt(start, c.index + 1);
                        while (!n.isLeaf()) n = childAt(n, 0);
                        c.node = n;
                        c.index = 0;
                        return;
                    }
                    if (c.index + 1 < start.count()) {
                        c.index += 1;
                        return;
                    }
                    var n = start;
                    while (n.header.parent) |p| {
                        const pos = @as(usize, n.header.position);
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

                pub inline fn retreat(c: *C) void {
                    c.assertValid();
                    const start = c.node orelse {
                        c.node = c.tree.rightmostNode();
                        if (c.node) |n| c.index = n.count() - 1;
                        return;
                    };
                    if (!start.isLeaf()) {
                        var n = childAt(start, c.index);
                        while (!n.isLeaf()) n = childAt(n, n.count());
                        c.node = n;
                        c.index = n.count() - 1;
                        return;
                    }
                    if (c.index > 0) {
                        c.index -= 1;
                        return;
                    }
                    var n = start;
                    while (n.header.parent) |p| {
                        const pos = @as(usize, n.header.position);
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

                inline fn assertValid(c: C) void {
                    if (config.check_iterator_generation and safetyChecks()) {
                        std.debug.assert(c.generation == c.tree.generation);
                    }
                }
            };
        }

        pub const Iterator = IteratorImpl(*Self, *Node, *Entry, .forward);
        pub const ConstIterator = IteratorImpl(*const Self, *const Node, *const Entry, .forward);
        pub const ReverseIterator = IteratorImpl(*Self, *Node, *Entry, .reverse);
        pub const ConstReverseIterator = IteratorImpl(*const Self, *const Node, *const Entry, .reverse);

        const IteratorDirection = enum { forward, reverse };

        fn IteratorImpl(
            comptime TreePtr: type,
            comptime NodePtr: type,
            comptime EntryPtr: type,
            comptime direction: IteratorDirection,
        ) type {
            const check_generation = config.check_iterator_generation and safetyChecks();
            const StoredTreePtr = if (check_generation) TreePtr else void;
            const StoredGeneration = if (check_generation) usize else void;

            return struct {
                const I = @This();

                tree: StoredTreePtr,
                node: ?NodePtr,
                index: u8,
                end: u8,
                generation: StoredGeneration,

                fn init(tree: TreePtr, node: ?NodePtr, index: usize) I {
                    return .{
                        .tree = if (check_generation) tree else {},
                        .node = node,
                        .index = narrowPos(index),
                        .end = if (node) |n| n.header.finish else 0,
                        .generation = if (check_generation) tree.generation else {},
                    };
                }

                pub inline fn next(it: *I) ?EntryPtr {
                    it.assertValid();
                    const n = it.node orelse {
                        @branchHint(.unlikely);
                        return null;
                    };
                    const idx = @as(usize, it.index);
                    const out = &n.entries[idx];
                    switch (direction) {
                        .forward => it.advanceFrom(n, idx),
                        .reverse => it.retreatFrom(n, idx),
                    }
                    return out;
                }

                inline fn advanceFrom(it: *I, start: NodePtr, idx: usize) void {
                    if (start.isLeaf()) {
                        if (idx + 1 < @as(usize, it.end)) {
                            @branchHint(.likely);
                            it.index = narrowPos(idx + 1);
                            return;
                        }
                    } else {
                        @branchHint(.unlikely);
                        var n = childAt(start, idx + 1);
                        while (!n.isLeaf()) n = childAt(n, 0);
                        it.node = n;
                        it.index = 0;
                        it.end = n.header.finish;
                        return;
                    }
                    var n = start;
                    while (n.header.parent) |p| {
                        const pos = @as(usize, n.header.position);
                        if (pos < p.count()) {
                            it.node = p;
                            it.index = narrowPos(pos);
                            it.end = p.header.finish;
                            return;
                        }
                        n = p;
                    }
                    it.node = null;
                    it.index = 0;
                    it.end = 0;
                }

                inline fn retreatFrom(it: *I, start: NodePtr, idx: usize) void {
                    if (start.isLeaf()) {
                        if (idx > 0) {
                            @branchHint(.likely);
                            it.index = narrowPos(idx - 1);
                            return;
                        }
                    } else {
                        @branchHint(.unlikely);
                        var n = childAt(start, idx);
                        while (!n.isLeaf()) n = childAt(n, n.count());
                        it.node = n;
                        it.index = narrowPos(n.count() - 1);
                        it.end = n.header.finish;
                        return;
                    }
                    var n = start;
                    while (n.header.parent) |p| {
                        const pos = @as(usize, n.header.position);
                        if (pos > 0) {
                            it.node = p;
                            it.index = narrowPos(pos - 1);
                            it.end = p.header.finish;
                            return;
                        }
                        n = p;
                    }
                    it.node = null;
                    it.index = 0;
                    it.end = 0;
                }

                inline fn assertValid(it: I) void {
                    if (check_generation) {
                        std.debug.assert(it.generation == it.tree.generation);
                    }
                }
            };
        }

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

        pub inline fn contains(self: *const Self, key_: Key) bool {
            return self.getEntryConst(key_) != null;
        }

        pub inline fn get(self: *Self, key_: Key) ?*Value {
            const e = self.getEntry(key_) orelse return null;
            return &e.value;
        }

        pub inline fn getConst(self: *const Self, key_: Key) ?*const Value {
            const c = self.findCursor(key_);
            const e = c.entry() orelse return null;
            return &e.value;
        }

        pub inline fn getEntry(self: *Self, key_: Key) ?*Entry {
            const c = self.findCursorMut(key_);
            return c.entry();
        }

        pub inline fn getEntryConst(self: *const Self, key_: Key) ?*const Entry {
            const c = self.findCursor(key_);
            return c.entry();
        }

        /// Insert only when key is absent.  Does not overwrite an existing value.
        pub inline fn insert(self: *Self, key_: Key, value_: Value) Allocator.Error!InsertResult {
            return self.insertEntry(.{ .key = key_, .value = value_ });
        }

        pub fn insertEntry(self: *Self, entry_: Entry) Allocator.Error!InsertResult {
            if (self.root == null) {
                @branchHint(.unlikely);
                const r = try self.newNode(true, null, 0);
                r.entries[0] = entry_;
                r.header.finish = 1;
                self.root = r;
                self.len_ = 1;
                self.bumpGeneration();
                return .{ .entry = &r.entries[0], .inserted = true };
            }

            if (self.root.?.count() == max_slots) {
                @branchHint(.unlikely);
                if (self.getEntry(entry_.key)) |existing| {
                    return .{ .entry = existing, .inserted = false };
                }
                try self.growRoot();
            }

            var n = self.root.?;
            while (!n.isLeaf()) {
                var i = self.lowerBoundInNode(n, &entry_.key);
                if (i < n.count() and self.keysEqual(&entry_.key, &n.entries[i].key)) {
                    @branchHint(.unlikely);
                    return .{ .entry = &n.entries[i], .inserted = false };
                }
                var child = childAt(n, i);
                if (child.count() == max_slots) {
                    @branchHint(.unlikely);
                    if (self.findEntryInSubtree(child, &entry_.key)) |existing| {
                        return .{ .entry = existing, .inserted = false };
                    }
                    if (self.rebalanceFullChildBeforeInsert(n, i, &entry_.key)) |rebalanced_child| {
                        _ = rebalanced_child;
                        i = self.lowerBoundInNode(n, &entry_.key);
                        if (i < n.count() and self.keysEqual(&entry_.key, &n.entries[i].key)) {
                            @branchHint(.unlikely);
                            return .{ .entry = &n.entries[i], .inserted = false };
                        }
                        child = childAt(n, i);
                    } else {
                        try self.splitChild(n, i);
                        if (self.keysEqual(&n.entries[i].key, &entry_.key)) {
                            return .{ .entry = &n.entries[i], .inserted = false };
                        }
                        if (self.less(&n.entries[i].key, &entry_.key)) {
                            i += 1;
                        }
                        child = childAt(n, i);
                    }
                }
                n = child;
            }

            const pos = self.lowerBoundInNode(n, &entry_.key);
            if (pos < n.count() and self.keysEqual(&entry_.key, &n.entries[pos].key)) {
                @branchHint(.unlikely);
                return .{ .entry = &n.entries[pos], .inserted = false };
            }
            insertEntryAt(n, pos, entry_);
            self.len_ += 1;
            self.bumpGeneration();
            return .{ .entry = &n.entries[pos], .inserted = true };
        }

        /// Insert or replace.  Returns the old value when a replacement occurs.
        pub inline fn put(self: *Self, key_: Key, value_: Value) Allocator.Error!PutResult {
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
        pub inline fn getOrPutValue(self: *Self, key_: Key, default_value: Value) Allocator.Error!InsertResult {
            return self.insert(key_, default_value);
        }

        pub inline fn remove(self: *Self, key_: Key) bool {
            if (self.root == null) return false;
            var mutated = false;
            const removed = self.deleteFromNode(self.root.?, &key_, &mutated);
            if (removed) {
                self.len_ -= 1;
            }
            if (removed or mutated) {
                self.bumpGeneration();
            }
            self.fixRootAfterDelete();
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

        const BoundKind = enum { lower, upper };

        /// Lower bound: first entry whose key is not less than key.
        pub fn lowerBound(self: *const Self, key_: Key) ConstCursor {
            return self.boundCursor(key_, .lower);
        }

        /// Mutable lower bound variant for callers that need to edit values in place.
        pub fn lowerBoundMut(self: *Self, key_: Key) Cursor {
            return self.boundCursor(key_, .lower);
        }

        /// Upper bound: first entry whose key is greater than key.
        pub fn upperBound(self: *const Self, key_: Key) ConstCursor {
            return self.boundCursor(key_, .upper);
        }

        /// Mutable upper bound variant for callers that need to edit values in place.
        pub fn upperBoundMut(self: *Self, key_: Key) Cursor {
            return self.boundCursor(key_, .upper);
        }

        pub fn findCursor(self: *const Self, key_: Key) ConstCursor {
            return self.findCursorImpl(key_);
        }

        pub fn findCursorMut(self: *Self, key_: Key) Cursor {
            return self.findCursorImpl(key_);
        }

        fn findCursorImpl(self: anytype, key_: Key) SearchCursor(@TypeOf(self)) {
            var n_opt = self.root;
            while (n_opt) |n| {
                const i = self.lowerBoundInNode(n, &key_);
                if (i < n.count() and self.keysEqual(&key_, &n.entries[i].key)) {
                    return self.cursorAtSearchResult(n, i);
                }
                if (n.isLeaf()) break;
                n_opt = childAt(n, i);
            }
            return self.endSearchCursor();
        }

        pub fn iterator(self: *Self) Iterator {
            return Iterator.init(self, self.leftmostNode(), 0);
        }

        pub fn constIterator(self: *const Self) ConstIterator {
            return ConstIterator.init(self, self.leftmostNode(), 0);
        }

        pub fn reverseIterator(self: *Self) ReverseIterator {
            const n = self.rightmostNode() orelse return ReverseIterator.init(self, null, 0);
            return ReverseIterator.init(self, n, n.count() - 1);
        }

        pub fn constReverseIterator(self: *const Self) ConstReverseIterator {
            const n = self.rightmostNode() orelse return ConstReverseIterator.init(self, null, 0);
            return ConstReverseIterator.init(self, n, n.count() - 1);
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
                if (n.isLeaf()) break;
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
                .bytes_used = @sizeOf(Self) + leaf_nodes * @sizeOf(Node) + internal_nodes * @sizeOf(InternalNode),
            };
        }

        /// Expensive invariant verifier intended for tests and debug builds.
        pub fn validate(self: *const Self) void {
            if (self.root == null) {
                std.debug.assert(self.len_ == 0);
                return;
            }
            const r = self.root.?;
            std.debug.assert(r.header.parent == null);
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

        fn boundCursor(self: anytype, key_: Key, comptime kind: BoundKind) SearchCursor(@TypeOf(self)) {
            const is_const = @typeInfo(@TypeOf(self)).pointer.is_const;
            const NodePtr = if (is_const) *const Node else *Node;
            var last_internal: ?struct { node: NodePtr, index: usize } = null;
            var n_opt = self.root;
            while (n_opt) |n| {
                const i = switch (kind) {
                    .lower => self.lowerBoundInNode(n, &key_),
                    .upper => self.upperBoundInNode(n, &key_),
                };
                if (i < n.count()) last_internal = .{ .node = n, .index = i };
                if (n.isLeaf()) {
                    if (i < n.count()) return self.cursorAtSearchResult(n, i);
                    if (last_internal) |li| return self.cursorAtSearchResult(li.node, li.index);
                    return self.endSearchCursor();
                }
                n_opt = childAt(n, i);
            }
            return self.endSearchCursor();
        }

        fn SearchCursor(comptime SelfPtr: type) type {
            return if (@typeInfo(SelfPtr).pointer.is_const) ConstCursor else Cursor;
        }

        fn cursorAtSearchResult(self: anytype, n: anytype, index: usize) SearchCursor(@TypeOf(self)) {
            if (@typeInfo(@TypeOf(self)).pointer.is_const) {
                return self.cursorAtConst(n, index);
            }
            return self.cursorAt(n, index);
        }

        fn endSearchCursor(self: anytype) SearchCursor(@TypeOf(self)) {
            if (@typeInfo(@TypeOf(self)).pointer.is_const) {
                return self.endCursor();
            }
            return self.endCursorMut();
        }

        fn newNode(self: *Self, leaf: bool, parent: ?*Node, position: usize) Allocator.Error!*Node {
            if (leaf) {
                const n = try self.allocator.create(Node);
                n.* = Node.init(true, parent, position);
                return n;
            }
            const internal = try self.allocator.create(InternalNode);
            internal.* = .{
                .node = Node.init(false, parent, position),
                .children = [_]?*Node{null} ** (max_slots + 1),
            };
            return &internal.node;
        }

        fn destroySubtree(self: *Self, n: *Node) void {
            if (!n.isLeaf()) {
                var i: usize = 0;
                const children = n.childArray();
                while (i <= n.count()) : (i += 1) {
                    if (children[i]) |c| self.destroySubtree(c);
                }
            }
            self.destroyNode(n);
        }

        fn destroyNode(self: *Self, n: *Node) void {
            if (n.isLeaf()) {
                self.allocator.destroy(n);
                return;
            }
            const internal: *InternalNode = @fieldParentPtr("node", n);
            self.allocator.destroy(internal);
        }

        fn growRoot(self: *Self) Allocator.Error!void {
            const old = self.root.?;
            const new_root = try self.newNode(false, null, 0);
            old.header.parent = new_root;
            old.header.position = 0;
            new_root.childArray()[0] = old;
            self.root = new_root;
            self.splitChild(new_root, 0) catch |err| {
                old.header.parent = null;
                old.header.position = 0;
                new_root.childArray()[0] = null;
                self.root = old;
                self.destroyNode(new_root);
                return err;
            };
        }

        fn splitChild(self: *Self, parent: *Node, child_index: usize) Allocator.Error!void {
            std.debug.assert(parent.count() < max_slots);
            const left = childAt(parent, child_index);
            std.debug.assert(left.count() == max_slots);

            const right = try self.newNode(left.isLeaf(), parent, child_index + 1);
            const median_index = min_slots;
            const right_len = max_slots - median_index - 1;

            var j: usize = 0;
            while (j < right_len) : (j += 1) {
                right.entries[j] = left.entries[median_index + 1 + j];
            }
            if (!left.isLeaf()) {
                j = 0;
                const left_children = left.childArray();
                const right_children = right.childArray();
                while (j <= right_len) : (j += 1) {
                    const c = left_children[median_index + 1 + j].?;
                    right_children[j] = c;
                    c.header.parent = right;
                    c.header.position = narrowPos(j);
                    left_children[median_index + 1 + j] = null;
                }
            }
            right.header.finish = narrowLen(right_len);
            const median = left.entries[median_index];
            left.header.finish = narrowLen(median_index);

            shiftChildrenRight(parent, child_index + 1);
            parent.childArray()[child_index + 1] = right;
            shiftEntriesRight(parent, child_index);
            parent.entries[child_index] = median;
            parent.header.finish += 1;
            fixChildPositions(parent, child_index + 1);
        }

        fn rebalanceFullChildBeforeInsert(self: *const Self, parent: *Node, child_index: usize, key_: *const Key) ?*Node {
            const child = childAt(parent, child_index);
            const insert_pos = self.lowerBoundInNode(child, key_);
            if (insert_pos > 0 and child_index > 0 and childAt(parent, child_index - 1).count() < max_slots) {
                const left = childAt(parent, child_index - 1);
                const movable = @min(max_slots - left.count(), insert_pos);
                const to_move = @max(@as(usize, 1), movable / 2);
                var moved: usize = 0;
                while (moved < to_move) : (moved += 1) {
                    borrowFromRight(parent, child_index - 1);
                }
                return childAt(parent, child_index);
            }
            if (insert_pos < child.count() and child_index < parent.count() and childAt(parent, child_index + 1).count() < max_slots) {
                const right = childAt(parent, child_index + 1);
                const movable = @min(max_slots - right.count(), child.count() - insert_pos);
                const to_move = @max(@as(usize, 1), movable / 2);
                var moved: usize = 0;
                while (moved < to_move) : (moved += 1) {
                    borrowFromLeft(parent, child_index + 1);
                }
                return childAt(parent, child_index);
            }
            return null;
        }

        fn deleteFromNode(self: *Self, n: *Node, key_: *const Key, mutated: *bool) bool {
            const idx = self.lowerBoundInNode(n, key_);
            if (idx < n.count() and self.keysEqual(key_, &n.entries[idx].key)) {
                if (n.isLeaf()) {
                    removeEntryAt(n, idx);
                    return true;
                }
                return self.deleteFromInternal(n, idx);
            }
            if (n.isLeaf()) return false;

            const child_index = idx;
            var child = childAt(n, child_index);
            if (child.count() == min_slots) {
                mutated.* = true;
                child = self.fillChild(n, child_index);
            }
            return self.deleteFromNode(child, key_, mutated);
        }

        fn findEntryInSubtree(self: *const Self, start: anytype, key_: *const Key) ?@TypeOf(&start.entries[0]) {
            var n = start;
            while (true) {
                const i = self.lowerBoundInNode(n, key_);
                if (i < n.count() and self.keysEqual(key_, &n.entries[i].key)) return &n.entries[i];
                if (n.isLeaf()) return null;
                n = childAt(n, i);
            }
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
            while (!n.isLeaf()) {
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
            while (!n.isLeaf()) {
                const idx = n.count();
                var child = childAt(n, idx);
                if (child.count() == min_slots) {
                    child = self.fillChild(n, idx);
                }
                n = child;
            }
            const last = n.count() - 1;
            const out = n.entries[last];
            n.header.finish -= 1;
            return out;
        }

        fn removeEntryAtKnownPresent(self: *Self, n: *Node, idx: usize) bool {
            if (n.isLeaf()) {
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
            std.debug.assert(left.isLeaf() == right.isLeaf());
            std.debug.assert(left.count() + 1 + right.count() <= max_slots);

            const left_len = left.count();
            left.entries[left_len] = parent.entries[left_index];
            var j: usize = 0;
            while (j < right.count()) : (j += 1) {
                left.entries[left_len + 1 + j] = right.entries[j];
            }
            if (!left.isLeaf()) {
                j = 0;
                const right_children = right.childArray();
                const left_children = left.childArray();
                while (j <= right.count()) : (j += 1) {
                    const c = right_children[j].?;
                    left_children[left_len + 1 + j] = c;
                    c.header.parent = left;
                    c.header.position = narrowPos(left_len + 1 + j);
                }
            }
            left.header.finish = narrowLen(left_len + 1 + right.count());

            removeEntryAt(parent, left_index);
            removeChildAt(parent, left_index + 1);
            self.destroyNode(right);
            fixChildPositions(parent, left_index);
            return left;
        }

        fn fixRootAfterDelete(self: *Self) void {
            const r = self.root orelse return;
            if (r.count() != 0) return;
            if (r.isLeaf()) {
                self.destroyNode(r);
                self.root = null;
                return;
            }
            const child = childAt(r, 0);
            child.header.parent = null;
            child.header.position = 0;
            self.root = child;
            self.destroyNode(r);
        }

        inline fn lowerBoundInNode(self: *const Self, n: *const Node, key_: *const Key) usize {
            const node_len = n.count();
            if (max_slots <= config.linear_search_threshold) {
                var i: usize = 0;
                while (i < node_len and self.less(&n.entries[i].key, key_)) : (i += 1) {}
                return i;
            }
            var lo: usize = 0;
            var hi: usize = node_len;
            while (lo < hi) {
                const mid = (lo + hi) / 2;
                if (self.less(&n.entries[mid].key, key_)) lo = mid + 1 else hi = mid;
            }
            return lo;
        }

        inline fn upperBoundInNode(self: *const Self, n: *const Node, key_: *const Key) usize {
            const node_len = n.count();
            if (max_slots <= config.linear_search_threshold) {
                var i: usize = 0;
                while (i < node_len and !self.less(key_, &n.entries[i].key)) : (i += 1) {}
                return i;
            }
            var lo: usize = 0;
            var hi: usize = node_len;
            while (lo < hi) {
                const mid = (lo + hi) / 2;
                if (!self.less(key_, &n.entries[mid].key)) lo = mid + 1 else hi = mid;
            }
            return lo;
        }

        fn leftmostNode(self: *const Self) ?*Node {
            var n = self.root orelse return null;
            while (!n.isLeaf()) n = childAt(n, 0);
            return n;
        }

        fn rightmostNode(self: *const Self) ?*Node {
            var n = self.root orelse return null;
            while (!n.isLeaf()) n = childAt(n, n.count());
            return n;
        }

        inline fn less(self: *const Self, a: *const Key, b: *const Key) bool {
            return compare(&self.context, a, b) == .lt;
        }

        inline fn keysEqual(self: *const Self, a: *const Key, b: *const Key) bool {
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
    const unaligned_header = ptr_size + @sizeOf(u8) * 4;
    const entry_align = @alignOf(Entry);
    const header_estimate = ((unaligned_header + entry_align - 1) / entry_align) * entry_align;
    // Abseil derives target slots from leaf-node payload: leaf nodes dominate
    // node count and do not carry child pointers.  Zig now follows that split
    // layout, with child pointers present only in internal-node allocations.
    const per_slot_estimate = entry_size;
    var raw: usize = if (config.target_node_size > header_estimate)
        (config.target_node_size - header_estimate) / per_slot_estimate
    else
        0;
    raw = @max(raw, config.min_max_slots);
    raw = @min(raw, config.max_max_slots);
    if (raw < 3) raw = 3;
    if (raw % 2 == 0) raw -= 1;
    if (raw < 3) raw = 3;
    if (raw > std.math.maxInt(u8)) raw = std.math.maxInt(u8);
    return raw;
}

fn safetyChecks() bool {
    return switch (@import("builtin").mode) {
        .Debug, .ReleaseSafe => true,
        else => false,
    };
}

fn narrowLen(x: usize) u8 {
    std.debug.assert(x <= std.math.maxInt(u8));
    return @as(u8, @intCast(x));
}

fn narrowPos(x: usize) u8 {
    std.debug.assert(x <= std.math.maxInt(u8));
    return @as(u8, @intCast(x));
}

inline fn childAt(n: anytype, idx: usize) *@TypeOf(n.*) {
    std.debug.assert(idx <= n.count());
    if (@typeInfo(@TypeOf(n)).pointer.is_const) {
        return n.childArrayConst()[idx].?;
    }
    return n.childArray()[idx].?;
}

fn shiftEntriesRight(n: anytype, start: usize) void {
    var i = n.count();
    while (i > start) : (i -= 1) {
        n.entries[i] = n.entries[i - 1];
    }
}

fn shiftChildrenRight(n: anytype, start: usize) void {
    var i = n.count() + 1;
    const children = n.childArray();
    while (i > start) : (i -= 1) {
        children[i] = children[i - 1];
    }
}

inline fn insertEntryAt(n: anytype, idx: usize, entry: @TypeOf(n.entries[0])) void {
    std.debug.assert(n.count() < n.capacity());
    shiftEntriesRight(n, idx);
    n.entries[idx] = entry;
    n.header.finish += 1;
}

inline fn removeEntryAt(n: anytype, idx: usize) void {
    std.debug.assert(idx < n.count());
    var i = idx;
    while (i + 1 < n.count()) : (i += 1) {
        n.entries[i] = n.entries[i + 1];
    }
    n.header.finish -= 1;
}

fn removeChildAt(n: anytype, idx: usize) void {
    std.debug.assert(idx <= n.count() + 1);
    var i = idx;
    const children = n.childArray();
    while (i + 1 <= n.count() + 1) : (i += 1) {
        children[i] = children[i + 1];
    }
    children[n.count() + 1] = null;
}

fn fixChildPositions(parent: anytype, start: usize) void {
    if (parent.isLeaf()) return;
    var i = start;
    const children = parent.childArray();
    while (i <= parent.count()) : (i += 1) {
        if (children[i]) |c| {
            c.header.parent = parent;
            c.header.position = narrowPos(i);
        }
    }
}

fn borrowFromLeft(parent: anytype, idx: usize) void {
    const child = childAt(parent, idx);
    const left = childAt(parent, idx - 1);
    std.debug.assert(left.count() > parentContextMinSlots(parent));
    std.debug.assert(child.count() < child.capacity());

    shiftEntriesRight(child, 0);
    if (!child.isLeaf()) {
        shiftChildrenRight(child, 0);
        const left_children = left.childArray();
        const child_children = child.childArray();
        const moved_child = left_children[left.count()].?;
        child_children[0] = moved_child;
        moved_child.header.parent = child;
        moved_child.header.position = 0;
        left_children[left.count()] = null;
    }
    child.entries[0] = parent.entries[idx - 1];
    child.header.finish += 1;
    parent.entries[idx - 1] = left.entries[left.count() - 1];
    left.header.finish -= 1;
    fixChildPositions(child, 0);
}

fn borrowFromRight(parent: anytype, idx: usize) void {
    const child = childAt(parent, idx);
    const right = childAt(parent, idx + 1);
    std.debug.assert(right.count() > parentContextMinSlots(parent));
    std.debug.assert(child.count() < child.capacity());

    const child_len = child.count();
    child.entries[child_len] = parent.entries[idx];
    if (!child.isLeaf()) {
        const right_children = right.childArray();
        const child_children = child.childArray();
        const moved_child = right_children[0].?;
        child_children[child_len + 1] = moved_child;
        moved_child.header.parent = child;
        moved_child.header.position = narrowPos(child_len + 1);
    }
    child.header.finish += 1;
    parent.entries[idx] = right.entries[0];
    removeEntryAt(right, 0);
    if (!right.isLeaf()) {
        var j: usize = 0;
        const right_children = right.childArray();
        while (j <= right.count()) : (j += 1) {
            right_children[j] = right_children[j + 1];
        }
        right_children[right.count() + 1] = null;
        fixChildPositions(right, 0);
    }
}

fn parentContextMinSlots(parent: anytype) usize {
    return parent.entries.len / 2;
}

fn countNodes(n: anytype, leaf_nodes: *usize, internal_nodes: *usize) void {
    if (n.isLeaf()) {
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
    std.debug.assert(n.count() <= n.capacity());
    std.debug.assert(n.header.start == 0);
    if (n.header.parent != null) std.debug.assert(n.count() >= n.entries.len / 2);
    if (lo) |l| std.debug.assert(tree.less(l, &n.entries[0].key));
    if (hi) |h| std.debug.assert(tree.less(&n.entries[n.count() - 1].key, h));
    var i: usize = 1;
    while (i < n.count()) : (i += 1) {
        std.debug.assert(tree.less(&n.entries[i - 1].key, &n.entries[i].key));
    }
    counted.* += n.count();
    if (n.isLeaf()) return depth;

    var leaf_depth: ?usize = null;
    i = 0;
    while (i <= n.count()) : (i += 1) {
        const c = childAt(n, i);
        std.debug.assert(c.header.parent == n);
        std.debug.assert(@as(usize, c.header.position) == i);
        const child_lo = if (i == 0) lo else &n.entries[i - 1].key;
        const child_hi = if (i == n.count()) hi else &n.entries[i].key;
        const d = validateNode(tree, c, child_lo, child_hi, depth + 1, counted);
        if (leaf_depth) |expected| std.debug.assert(expected == d) else leaf_depth = d;
    }
    return leaf_depth.?;
}

// Focused unit tests.  More stress tests are in test/btree_stress.zig.
test "BTreeMap u64 node layout matches Abseil full-node layout" {
    const testing = std.testing;
    const Map = AutoBTreeMapWithConfig(u64, u64, .{ .target_node_size = 256 });

    try testing.expectEqual(@as(usize, 15), Map.max_node_slots);
    try testing.expectEqual(@as(usize, 16), Map.node_entry_offset);
    try testing.expectEqual(@as(usize, 256), Map.leaf_node_size);
    try testing.expectEqual(@as(usize, 256), Map.child_array_offset);
    try testing.expectEqual(@as(usize, 384), Map.internal_node_size);
}

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

test "BTreeMap duplicate insert does not rebalance full child" {
    const testing = std.testing;
    const Map = AutoBTreeMapWithConfig(u32, u32, .{ .target_node_size = 64 });
    var map = Map.init(testing.allocator);
    defer map.deinit();

    var i: u32 = 0;
    while (i < 4096) : (i += 1) {
        const key_ = (i * 37) % 4096;
        try testing.expect((try map.insert(key_, key_)).inserted);
    }
    map.validate();

    const duplicate = struct {
        fn firstKey(n: anytype) u32 {
            var cur = n;
            while (!cur.isLeaf()) cur = childAt(cur, 0);
            return cur.entries[0].key;
        }

        fn inFullChild(n: anytype, max_slots: usize) ?u32 {
            if (n.isLeaf()) return null;
            var child_index: usize = 0;
            while (child_index <= n.count()) : (child_index += 1) {
                const child = childAt(n, child_index);
                if (child.count() == max_slots) return firstKey(child);
                if (inFullChild(child, max_slots)) |key_| return key_;
            }
            return null;
        }
    }.inFullChild(map.root.?, Map.max_node_slots) orelse return error.SkipZigTest;

    const before = map.stats();
    const before_generation = map.generation;
    const result = try map.insert(duplicate, 9999);
    try testing.expect(!result.inserted);
    try testing.expectEqual(duplicate, result.entry.key);
    try testing.expectEqual(before.len, map.stats().len);
    try testing.expectEqual(before.nodes, map.stats().nodes);
    try testing.expectEqual(before_generation, map.generation);
    map.validate();
}

test "BTreeMap insert rebalancing preserves iteration order" {
    const testing = std.testing;
    const Map = AutoBTreeMapWithConfig(u32, u32, .{ .target_node_size = 64 });
    var map = Map.init(testing.allocator);
    defer map.deinit();

    var i: u32 = 0;
    while (i < 512) : (i += 1) {
        const key_ = (i * 37) % 512;
        try testing.expect((try map.insert(key_, key_ + 1)).inserted);
        if (i % 31 == 0) map.validate();
    }
    map.validate();

    var it = map.iterator();
    i = 0;
    while (i < 512) : (i += 1) {
        const entry = it.next().?;
        try testing.expectEqual(i, entry.key);
        try testing.expectEqual(i + 1, entry.value);
    }
    try testing.expect(it.next() == null);

    var rit = map.reverseIterator();
    i = 512;
    while (i > 0) {
        i -= 1;
        const entry = rit.next().?;
        try testing.expectEqual(i, entry.key);
        try testing.expectEqual(i + 1, entry.value);
    }
    try testing.expect(rit.next() == null);
}

test "BTreeMap absent remove preserves contents" {
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
    const before_generation = map.generation;

    try testing.expect(!map.remove(3333));
    try testing.expectEqual(before.len, map.stats().len);
    if (map.stats().nodes != before.nodes) {
        try testing.expect(map.generation != before_generation);
    }
    map.validate();

    var it = map.iterator();
    i = 0;
    while (i < 2000) : (i += 1) {
        const entry = it.next().?;
        try testing.expectEqual(i * 2, entry.key);
        try testing.expectEqual(i, entry.value);
    }
    try testing.expect(it.next() == null);
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

    const found = map.findCursorMut(1);
    found.value().?.* = 100;
    try testing.expectEqual(@as(u32, 100), map.get(1).?.*);
    try testing.expect(map.findCursorMut(99).isEnd());

    const upper = map.upperBoundMut(1);
    upper.value().?.* = 220;
    try testing.expectEqual(@as(u32, 220), map.get(2).?.*);
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
