# zig-btree

A high-fanout B-tree map/set implementation for Zig, based on the core mechanics used by Abseil's `absl::btree_*` containers: compact cache-sized nodes, values in internal and leaf nodes, parent/position bookkeeping, split/borrow/merge deletion, lower/upper-bound search, and bidirectional traversal.

Implemented surfaces include:

- `AutoBTreeMap`, `BTreeMap`, `AutoBTreeSet`, and `BTreeSet`
- insert-only, insert-or-replace, lookup, contains, remove, and fetch-remove
- lower/upper-bound cursors, forward/reverse iterators, and const iterators
- custom comparator context and byte-slice default ordering
- invariant validation, basic stats, and an allocation-failure test harness

## Use

```zig
const std = @import("std");
const btree = @import("btree");

const Map = btree.AutoBTreeMap(u64, []const u8);

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var map = Map.init(gpa.allocator());
    defer map.deinit();

    _ = try map.put(42, "answer");
    if (map.get(42)) |v| std.debug.print("{s}\n", .{v.*});

    var it = map.iterator();
    while (it.next()) |entry| {
        std.debug.print("{} -> {s}\n", .{ entry.key, entry.value });
    }
}
```

For custom ordering, use `BTreeMap(Key, Value, Context, compare, config)` where `compare` has this signature:

```zig
fn compare(ctx: *const Context, a: *const Key, b: *const Key) std.math.Order
```

The container does not own resources inside keys or values.  If stored values need cleanup, iterate and clean them before `clear()` or `deinit()`.

## Tests

With the uploaded Zig toolchain extracted, run:

```sh
zig build test
```

or directly:

```sh
zig test src/btree.zig
```

The stress tests import the package module, so `zig build test` is the
recommended way to run the complete suite.

## Benchmark

```sh
zig build -Doptimize=ReleaseFast bench
```

The benchmark reports insert, lookup, ordered iteration, and remove throughput
for randomized `u64` keys.
