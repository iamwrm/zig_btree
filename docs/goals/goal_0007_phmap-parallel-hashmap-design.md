# Goal 0007: Align `zig_phmap` With Upstream Parallel Hashmap Design

Migrate `zig_phmap` closer to the production design used by upstream C++ `parallel-hashmap` v2.0.0, then verify whether the remaining local aarch64 performance gaps disappear and whether the existing Zig wins are preserved.

## Background

Goals 0003 and 0004 made `zig_phmap` competitive with C++ `parallel-hashmap` on the local aarch64 host. The latest Goal 0004 final audit reported these seven-sample medians:

- Zig:
  - `insert_reserved`: 16.193 ns/op
  - `lookup_hit`: 7.755 ns/op
  - `lookup_miss`: 3.418 ns/op
  - `iterate`: 1.906 ns/item
  - `mixed`: 19.605 ns/op
  - `remove`: 7.305 ns/op
  - `string_insert`: 13.256 ns/op
  - `string_lookup`: 10.307 ns/op
  - `high_load_miss`: 17.799 ns/op
  - `tombstone_churn`: 8.577 ns/op
- C++ `parallel-hashmap`:
  - `insert_reserved`: 31.271 ns/op
  - `lookup_hit`: 12.492 ns/op
  - `lookup_miss`: 4.019 ns/op
  - `iterate`: 4.203 ns/item
  - `mixed`: 34.906 ns/op
  - `remove`: 23.169 ns/op
  - `string_insert`: 41.975 ns/op
  - `string_lookup`: 20.952 ns/op
  - `high_load_miss`: 17.108 ns/op
  - `tombstone_churn`: 21.691 ns/op

Zig was faster on every recorded workload except `high_load_miss`, where it was 4.0% slower. The current implementation already has a Swiss-table-style control-byte layout, group probing, cloned control bytes, tombstones, growth-left accounting, and node-map pointer stability. Goal 0007 should test whether matching the upstream design more exactly improves the remaining high-load miss path and keeps or improves the existing wins.

## Objective

Rework `zig_phmap` toward the same production architecture used in `.deps/parallel-hashmap-2.0.0/parallel_hashmap/phmap.h`, then compare repeated local aarch64 medians against C++.

Primary target:

- `high_load_miss`: Zig must be no slower than C++ `parallel-hashmap` on repeated local aarch64 medians.

Secondary targets:

- Preserve or improve the existing Zig advantage on `insert_reserved`, `lookup_hit`, `lookup_miss`, `iterate`, `mixed`, `remove`, `string_insert`, `string_lookup`, and `tombstone_churn`.
- If any secondary workload regresses by more than 10% from the Goal 0004 final Zig medians, document the cause and either fix it or justify why it is required for a larger production-design win.
- Preserve public `FlatHashMap`, `FlatHashSet`, `NodeHashMap`, and `NodeHashSet` APIs.
- Preserve allocation-failure correctness.
- Preserve node-container pointer stability.
- Keep the implementation portable. Target-specific group implementations are allowed only behind a small abstraction with a correct portable fallback and benchmark evidence.

## Scope

Use the local development host, currently aarch64, as the performance decision point.

GitHub Actions can be used as repository health signal only. Hosted CI performance must not be used to accept, reject, or complete this goal.

Use upstream C++ only as an architectural reference. Do not copy upstream code. Implement Zig-native code with equivalent observable semantics, memory-safety behavior, allocator behavior, and portability.

## Required Source Inspection

Before changing Zig code, inspect the current Zig implementation and the upstream C++ design.

Inspect Zig:

- `zig_phmap/src/phmap.zig`
- `zig_phmap/bench/phmap_bench.zig`
- `zig_phmap/test/phmap_stress.zig`

Inspect upstream:

- `.deps/parallel-hashmap-2.0.0/parallel_hashmap/phmap.h`

Record concrete source locations in `zig_phmap/checkpoints.md` for:

- `ctrl_t`, special control values, H1/H2 hash splitting
- `probe_seq`
- `GroupSse2Impl`, `GroupPortableImpl`, and selected `Group`
- `NumClonedBytes`
- `raw_hash_set`
- iterator implementation
- `find`
- `find_first_non_full`
- `prepare_insert`
- `drop_deletes_without_resize`
- `ConvertDeletedToEmptyAndFullToDeleted`
- `set_ctrl`
- `reset_ctrl`
- `reset_growth_left`
- `growth_left`
- `Layout<ctrl_t, slot_type>` allocation shape
- `raw_hash_map`
- `flat_hash_map`
- `node_hash_map`
- `parallel_flat_hash_map` and `parallel_node_hash_map`

## Required Design Mapping

Write a design checkpoint in `zig_phmap/checkpoints.md` before implementation that maps upstream C++ structure to Zig structure.

Required upstream features to map:

- one allocation for control bytes plus slots where practical, using a layout equivalent to upstream `Layout<ctrl_t, slot_type>`
- capacity as a power-of-two mask compatible with upstream probe arithmetic
- one sentinel control byte plus `Group::kWidth - 1` cloned control bytes
- special control byte states: empty, deleted, sentinel
- 7-bit H2 fingerprints for full slots
- H1 seed behavior and whether Zig should match upstream `HashSeed(ctrl)` behavior
- upstream `probe_seq` quadratic/group stepping
- upstream `Group` width and portable/SIMD split
- upstream `find` candidate mask path
- upstream `find_first_non_full` path
- upstream `prepare_insert` growth-left and tombstone policy
- upstream `drop_deletes_without_resize` and in-place tombstone cleanup
- upstream `set_ctrl` cloned-control update semantics
- upstream iterator skip strategy using group masks
- upstream erased-slot handling during remove
- upstream flat versus node map storage ownership
- upstream sharded `parallel_*` containers and whether they are in scope for this goal

Required Zig design decisions:

- whether to replace separate `ctrl` and `entries` allocations with one manually laid out allocation
- whether to keep the current `WordGroup` as the default group or add a closer upstream-compatible group abstraction
- whether to add an aarch64 vector group implementation or keep a portable word group after measuring
- how to represent capacity, mask, sentinel, and clones without per-probe boundary checks
- how to preserve `FlatHashMap` and `NodeHashMap` public APIs while changing internals
- how to preserve node-map pointer stability through flat-index rehashes
- how to preserve allocation-failure correctness when allocation shape changes
- how to keep invariant validation comprehensive after layout changes
- whether to implement upstream sharded `parallel_flat_hash_map` semantics as a separate container or document it out of scope

## Implementation Direction

Prefer a staged migration with correctness gates between retained stages.

Likely stages:

1. Add layout diagnostics and tests comparing Zig control/slot layout against upstream.
2. Add an internal table-allocation abstraction that can allocate control bytes and slots in one block, with correct alignment and cleanup.
3. Migrate `FlatHashMap` to the unified allocation while preserving API and allocator behavior.
4. Rework `setCtrl`, cloned-control updates, sentinel placement, and validation to match upstream exactly.
5. Revisit `probe_seq` and `find_first_non_full` to match upstream high-load miss behavior.
6. Revisit `dropDeletesWithoutResize` to match upstream in-place tombstone cleanup more closely.
7. Revisit iterator group skipping and erased-slot handling after layout/probe changes.
8. Re-run `NodeHashMap` pointer-stability tests after each flat-index storage change.
9. Consider a target-specific group backend only after portable layout/probe changes are correct and measured.

Do not:

- specialize behavior only for `u64 -> u64`
- weaken tests or invariant checks
- remove allocation-failure testing
- break byte-slice key support
- break custom hash/equality contexts
- break node-container pointer stability
- add target-only code without a portable fallback

## Correctness Requirements

All existing correctness behavior must remain intact.

Run these gates after every retained architecture stage:

```sh
cd /home/wr/gh/zig_tree
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.16.0/zig build test
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseSafe test
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseFast test
```

Use this exact Zig toolchain unless a newer goal explicitly changes it.

Add or update tests for:

- one-block control/slot allocation and deallocation
- control-byte sentinel and cloned-byte placement
- group probing near control-array boundaries
- high-load miss lookup
- insertion through long probe chains
- tombstone reuse
- `dropDeletesWithoutResize` or equivalent in-place cleanup
- reserve followed by dense insertion
- clear, shrink, and deinit after mixed full/deleted/empty states
- duplicate insert behavior
- custom hash/equality contexts
- byte-slice keys
- iterator coverage with sparse tables, dense tables, and tombstones
- node map pointer stability across repeated flat-index growth/rehash
- allocation failure during reserve, growth, rehash, and tombstone cleanup

## Benchmark Requirements

Benchmark both Zig and C++ after every meaningful retained architecture stage.

Required local commands:

```sh
cd /home/wr/gh/zig_tree
g++ -O3 -DNDEBUG -std=c++17 -I .deps/parallel-hashmap-2.0.0 zig_phmap/bench/parallel_hashmap_bench.cc -o .deps/parallel_hashmap_bench

/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseFast bench
.deps/parallel_hashmap_bench
```

Required workloads:

- `insert_reserved`
- `lookup_hit`
- `lookup_miss`
- `iterate`
- `mixed`
- `remove`
- `string_insert`
- `string_lookup`
- `high_load_miss`
- `tombstone_churn`

Use at least seven local samples for the starting baseline and final audit. Five samples are acceptable for quick experiment rejection if the result is clearly worse.

Required diagnostic measurements:

- Zig and C++ table shape after each representative benchmark setup:
  - capacity
  - size
  - growth-left
  - deleted/tombstone count
  - load factor
  - bytes used if practical
  - probe length or group count for high-load misses if practical
- allocation count or allocation-size comparison if practical
- optimized assembly or `objdump` comparison for the final high-load miss path
- focused high-load miss timing independent of the full benchmark phase ordering

## Checkpoint Requirements

Update `zig_phmap/checkpoints.md` after every meaningful step.

Each checkpoint must include:

- timestamp
- short description
- files changed
- design notes when relevant
- upstream source references when relevant
- Zig benchmark results
- C++ benchmark results
- percentage gaps for all measured workloads
- table-shape diagnostics when relevant
- correctness commands and pass/fail status
- allocation-failure status
- node pointer stability status when relevant
- notes on regressions
- next hypothesis

Required checkpoints:

- Goal 0007 starting audit from the current code
- upstream-to-Zig structure mapping before code migration
- first retained table-allocation/layout abstraction
- first retained unified control/slot allocation, if adopted
- first correctness milestone after representation migration
- first benchmark after representation migration
- first high-load miss/probe-sequence benchmark
- first benchmark showing `high_load_miss` on-par or faster than C++, if achieved
- final audit comparing Goal 0007 starting and ending results

## Completion Criteria

Produce a final `zig_phmap/checkpoints.md` entry summarizing:

- starting local aarch64 Zig performance
- ending local aarch64 Zig performance
- C++ comparison and percentage gap for each workload
- whether upstream-style structure improved `high_load_miss`
- whether the existing Zig wins were preserved
- final table-shape comparison against C++
- final high-load miss/codegen comparison against C++
- correctness verification
- allocation-failure verification
- node pointer stability verification
- remaining known bottlenecks
- whether any architecture-specific work was added and how portability is preserved

Stop only when either:

- Zig `high_load_miss` is on-par with or faster than C++ on local aarch64 medians, secondary workload gaps are documented, existing correctness gates are green, allocation-failure coverage is preserved, and node pointer stability is verified, or
- the upstream-style structure has been implemented far enough to test the hypothesis, and `zig_phmap/checkpoints.md` clearly explains why performance still does not align and what different design would be required next.
