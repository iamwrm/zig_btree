# Goal 0006: Migrate Zig B-tree Toward Abseil Structure

Migrate the Zig `zig_btree` implementation toward the same core node and iterator architecture used by C++ Abseil `absl::btree_map`, then measure whether local aarch64 performance aligns.

## Background

Goal 0005 narrowed the remaining B-tree iteration gap to Abseil-backed structural causes rather than a single small hot-path tweak.

Final Goal 0005 local aarch64 7-sample medians:

- Zig:
  - insert: 138.481 ns/op
  - lookup: 145.089 ns/op
  - iterate: 5.592 ns/item
  - remove: 162.688 ns/op
- C++ Abseil:
  - insert: 114.045 ns/op
  - lookup: 132.659 ns/op
  - iterate: 3.935 ns/item
  - remove: 130.870 ns/op
- Gap:
  - insert: Zig is 21.4% slower
  - lookup: Zig is 9.4% slower
  - iterate: Zig is 42.1% slower
  - remove: Zig is 24.3% slower
  - 20% iterate target: `3.935 * 1.20 = 4.722 ns/item`

Goal 0005 evidence showed:

- Abseil uses 15-slot 256-byte leaves for `uint64_t -> uint64_t`.
- Abseil internal nodes store child pointers inline in the internal-node allocation.
- Abseil uses byte-sized node metadata for position/start/finish/max-count.
- Abseil's iterator is essentially node pointer plus position and is 16 bytes in the local probe.
- Abseil stores/caches leftmost and rightmost nodes for cheap begin/end.
- Zig now has compact leaf storage and byte position/length metadata, but internal child pointers are still separately allocated and cursor/iterator state remains 32 bytes.
- Zig has more benchmark-tree nodes than Abseil: 94583 versus 80628 in the Goal 0005 probes.

This goal should test the hypothesis that matching Abseil's node and iterator architecture closes most of the remaining performance gap.

## Objective

Rework `zig_btree` so its internal architecture is structurally close to Abseil's B-tree implementation, then compare local aarch64 performance against Abseil.

Primary targets:

- `iterate`: Zig must be no slower than 20% over C++ Abseil median, or faster, on repeated local aarch64 samples.
- The final audit must explain whether Abseil-style structure aligns performance across all benchmark workloads.

Secondary targets:

- `insert`, `lookup`, and `remove` should each be within 20% of C++ Abseil, or the final checkpoint must explain the remaining implementation difference.
- Do not regress any Zig workload by more than 10% from the Goal 0005 final Zig medians unless the regression is documented and explicitly justified.
- Preserve public map/set APIs.
- Preserve ordered map/set semantics.
- Preserve cursor and iterator behavior.
- Preserve invariant validation.
- Preserve allocation-failure correctness.
- Keep the implementation portable. Do not add aarch64-only or x86-only code unless it is isolated, measured, documented, and has a correct portable fallback.

## Scope

Use the local development host, currently aarch64, as the performance decision point.

GitHub Actions can be used as repository health signal only. Hosted CI performance must not be used to accept, reject, or complete this goal.

Do not copy Abseil code. Use Abseil as the architectural reference and implement Zig-native code with the same observable semantics and memory-safety requirements.

## Required Design Work

Before implementation, write a design checkpoint in `zig_btree/checkpoints.md` that maps the Abseil structure to the Zig structure.

Required Abseil features to map:

- leaf nodes allocate metadata plus entries only
- internal nodes allocate metadata plus entries plus inline child pointers
- byte-sized position/count metadata where possible
- root/empty representation
- leftmost and rightmost tracking
- iterator state based on node pointer plus slot position
- leaf-local increment fast path
- slow increment path from leaf end to parent separator
- slow increment path from internal separator to leftmost leaf in the right child
- insertion sibling rebalancing before split
- split bias based on insertion position
- root leaf growth behavior if applicable
- erase/rebalance structure and whether it must change for this migration

Required Zig design decisions:

- whether to keep one `Node` type with manual allocation-size variants, or introduce explicit `LeafNode` and `InternalNode` representations
- how to store and access inline internal child pointers without a separate child-array allocation
- how to represent leaf versus internal state without adding hot-path branches beyond Abseil's structure
- how to preserve existing public cursors while using a compact internal iterator state
- how to maintain leftmost/rightmost through insert, split, merge, borrow, root shrink, clear, and deinit
- how to keep allocation-failure behavior valid when node allocation and split/rebalance change
- how to keep invariant validation comprehensive after node representation changes

## Implementation Direction

Prefer a staged migration with correctness gates between stages.

Likely stages:

1. Introduce an internal node-allocation abstraction that can allocate leaf-sized and internal-sized nodes.
2. Move internal child pointers into the internal-node allocation, eliminating the separate child-array allocation.
3. Preserve current external `BTreeMap`, `BTreeSet`, cursor, iterator, and stats APIs during the migration.
4. Add leftmost/rightmost cached node pointers if they naturally fit the new root representation.
5. Rework iterator/cursor internals to use compact node pointer plus byte position for the hot path.
6. Revisit split/rebalance bias so Zig matches Abseil's occupancy behavior more closely.
7. Revisit remove/erase only after insert/lookup/iterate are correct and measured.

Do not:

- specialize behavior only for `u64 -> u64`
- weaken tests or invariant checks
- remove allocation-failure testing
- replace ordered B-tree semantics with B+ tree semantics in this goal unless the Abseil-style migration is completed or clearly rejected first
- tune only for one CPU backend

## Correctness Requirements

All existing correctness behavior must remain intact.

Run these gates from the `zig_btree/` package after every retained architecture stage:

```sh
cd /home/wr/gh/zig_tree/zig_btree
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.16.0/zig build test
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseSafe test
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseFast test
```

Add or update tests for:

- leaf and internal node allocation/deallocation
- split of leaf nodes and internal nodes
- sibling rebalance during insert
- merge and borrow after remove
- root leaf growth
- root split and root shrink
- clear and deinit with mixed leaf/internal trees
- leftmost/rightmost maintenance
- forward and reverse iteration after split, merge, borrow, root shrink, and clear
- lower/upper-bound edge cases across leaf/internal boundaries
- duplicate insert behavior
- absent-key remove behavior
- allocation failure during growth, split, and rebalancing

## Benchmark Requirements

Benchmark both Zig and C++ after every meaningful retained architecture stage.

Required local commands:

```sh
cd /home/wr/gh/zig_tree/zig_btree
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseFast bench

cd /home/wr/gh/zig_tree
.deps/abseil_btree_bench
```

Required workloads:

- `insert`
- `lookup`
- `iterate`
- `remove`

Use at least seven local samples for the starting baseline and final audit. Five samples are acceptable for quick experiment rejection if the result is clearly worse.

Required diagnostic measurements:

- Zig and Abseil tree shape after insert:
  - slots per node
  - height
  - total nodes
  - leaf nodes
  - internal nodes
  - fullness
  - bytes used
  - bytes per stored value
- allocation count or allocation-size comparison if practical
- focused iteration-only timing after each retained architecture stage
- optimized assembly or `objdump` comparison for the final Zig iterator hot path versus Abseil

## Checkpoint Requirements

Update `zig_btree/checkpoints.md` after every meaningful step.

Each checkpoint must include:

- timestamp
- short description
- files changed
- design notes when relevant
- Abseil source references when relevant
- Zig benchmark results
- C++ Abseil benchmark results
- percentage gaps for all measured workloads
- tree-shape diagnostics when relevant
- correctness commands and pass/fail status
- allocation-failure status
- notes on regressions
- next hypothesis

Required checkpoints:

- Goal 0006 starting audit from the current code
- Abseil-to-Zig structure mapping before code migration
- first retained leaf/internal allocation abstraction
- first retained inline-internal-child-pointer implementation
- first correctness milestone after representation migration
- first benchmark after representation migration
- first iterator/cursor compaction benchmark
- first benchmark showing `iterate` within 20% of Abseil, if achieved
- final audit comparing Goal 0006 starting and ending results

## Completion Criteria

Produce a final `zig_btree/checkpoints.md` entry summarizing:

- starting local aarch64 Zig performance
- ending local aarch64 Zig performance
- C++ Abseil comparison and percentage gap for each workload
- whether Abseil-style structure aligned performance
- final tree-shape comparison against Abseil
- final iterator hot-path/codegen comparison against Abseil
- explicit `iterate` target calculation and pass/fail status
- correctness verification
- allocation-failure verification
- remaining known bottlenecks
- whether any architecture-specific work was added and how portability is preserved

Stop only when either:

- Zig `iterate` is within 20% of C++ Abseil on local aarch64 medians, secondary workload gaps are documented, and correctness gates are green, or
- the Abseil-style structure has been implemented far enough to test the hypothesis, and `zig_btree/checkpoints.md` clearly explains why performance still does not align and what non-Abseil architecture would be required next.
