# Goal 0005: Improve Zig B-tree Performance on aarch64

Optimize the Zig `zig_btree` implementation against the C++ Abseil `absl::btree_map` benchmark on the local aarch64 development host.

## Background

The earlier B-tree performance pass improved the Zig implementation but still left substantial local gaps versus Abseil, especially ordered iteration and remove. The current repository also has a root `bench` step for `zig_phmap`, so B-tree performance work must run benchmarks from the `zig_btree/` package directly.

Historical local aarch64 results from the previous B-tree final audit were noisy but showed the remaining problem:

- Representative Zig sample:
  - insert: 172.959 ns/op
  - lookup: 208.325 ns/op
  - iterate: 12.213 ns/item
  - remove: 238.366 ns/op
- Final Abseil sample:
  - insert: 252.556 ns/op
  - lookup: 181.305 ns/op
  - iterate: 6.043 ns/item
  - remove: 154.198 ns/op
- Stable Abseil target samples in that run were often lower:
  - insert: 116-180 ns/op
  - lookup: 134-191 ns/op
  - iterate: 4.0-5.1 ns/item
  - remove: 132-178 ns/op

These numbers are stale and noisy. The first step in this goal is to establish a fresh repeated local aarch64 median baseline.

## Objective

Improve `zig_btree` so its local aarch64 median performance is competitive with C++ Abseil `absl::btree_map` on the same workload.

Primary targets:

- `insert`: Zig should be no slower than 20% over C++ median, or faster.
- `lookup`: Zig should be no slower than 20% over C++ median, or faster.
- `remove`: Zig should be no slower than 20% over C++ median, or faster.
- `iterate`: reduce the gap as much as practical; the target is no slower than 50% over C++ median unless a final checkpoint documents why this requires a larger layout redesign.

Secondary targets:

- Preserve all public APIs.
- Preserve ordered map/set semantics.
- Preserve cursor and iterator behavior.
- Preserve invariant validation.
- Preserve allocation-failure correctness.
- Keep production code general-purpose. Do not specialize for the benchmark key type or remove safety-oriented behavior just to improve benchmark output.

## Scope

Performance decisions for this goal are made from the local development host, currently aarch64.

GitHub Actions results may be used as repository health signals, but hosted CI performance must not be used to accept, reject, or complete this goal.

Use multiple local samples and compare medians. Do not claim progress from a single noisy outlier.

## Required Investigation

Before changing code, inspect the current Zig B-tree implementation and the Abseil comparison point.

Inspect Zig:

- `zig_btree/src/btree.zig`
- `zig_btree/bench/btree_bench.zig`
- `zig_btree/test/btree_stress.zig`

Focus on:

- node layout and `deriveMaxSlots`
- leaf versus internal node storage
- child pointer storage in leaf nodes
- in-node search policy and thresholds
- insertion split path
- lookup path and comparator calls
- iterator/cursor advancement
- remove path, merge, borrow, and root shrink
- parent pointer and child-position maintenance
- allocation behavior and node initialization
- invariant validation cost in ReleaseFast

Inspect C++:

- `.deps/abseil_btree_bench.cc`
- `.deps/abseil/absl/container/btree_map.h`
- `.deps/abseil/absl/container/internal/btree.h`

Focus on:

- Abseil leaf/internal layout distinction
- target node sizing
- slot storage and child storage
- in-node search strategy
- iterator representation
- erase/rebalance strategy
- allocation count and cache locality

Record source-inspection findings in `checkpoints.md` before making performance changes.

## Optimization Direction

Prefer production architecture improvements over benchmark-only shortcuts.

Likely areas to evaluate:

- Split leaf and internal node representations so leaf nodes do not carry unused child pointer arrays.
- Consider a manually allocated variable-size node layout if it materially improves cache footprint while keeping ownership clear.
- Revisit `target_node_size`, `deriveMaxSlots`, and actual node byte size on aarch64.
- Tune in-node search for local aarch64 using measured thresholds, while keeping correctness and general key support.
- Reduce iterator overhead by fast-pathing within-node advancement and minimizing parent traversal.
- Reduce duplicate searches in insert/remove where semantics allow it.
- Make remove rebalance less pointer-heavy while preserving non-mutating absent-key behavior.
- Reduce node initialization and child-position maintenance work where safe.
- Add targeted inline hints only when the generated code and measurements show a real improvement.

Do not:

- remove invariant checks or tests
- weaken allocation-failure handling
- specialize behavior only for `u64 -> u64`
- change public API semantics
- tune only for x86_64

Architecture-specific aarch64 improvements are acceptable only when they are isolated, measured, documented, and have a correct portable fallback.

## Correctness Requirements

All existing correctness behavior must remain intact.

Run these gates from the `zig_btree/` package:

```sh
cd /home/wr/gh/zig_tree/zig_btree
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build test
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build -Doptimize=ReleaseSafe test
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build -Doptimize=ReleaseFast test
```

Use this exact Zig toolchain unless a newer goal explicitly changes it.

Add or update tests if changes affect:

- split or merge behavior
- borrow from left or right sibling
- root shrink
- duplicate insert behavior
- absent-key remove behavior
- cursor/iterator invalidation
- lower/upper-bound edge cases
- leaf/internal node boundaries
- allocation failure during growth or rebalancing

## Benchmark Requirements

Benchmark both Zig and C++ after every meaningful optimization.

Required local commands:

```sh
cd /home/wr/gh/zig_tree/zig_btree
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build -Doptimize=ReleaseFast bench

cd /home/wr/gh/zig_tree
.deps/abseil_btree_bench
```

If `.deps/abseil_btree_bench.cc` or the Abseil dependency changes, rebuild the C++ benchmark and record the exact build command in `checkpoints.md`.

Required workloads:

- `insert`
- `lookup`
- `iterate`
- `remove`

Use at least seven local samples for the pre-change baseline and final audit. Five samples are acceptable for quick experiment rejection if the result is clearly worse.

## Checkpoint Requirements

Update `checkpoints.md` after every meaningful step.

Each checkpoint must include:

- timestamp
- short description
- files changed
- source-inspection notes when relevant
- benchmark commands
- Zig benchmark results
- C++ Abseil benchmark results
- percentage gaps for all measured operations
- correctness commands and pass/fail status
- notes on regressions
- next optimization hypothesis

Required checkpoints:

- source inspection of Zig and Abseil B-tree paths
- fresh pre-change local aarch64 median baseline
- first node-layout or hot-path optimization
- first correctness milestone after optimization
- first benchmark showing a material median improvement
- final audit comparing starting and ending local aarch64 results

## Completion Criteria

Produce a final `checkpoints.md` entry summarizing:

- starting local aarch64 Zig performance
- ending local aarch64 Zig performance
- C++ Abseil comparison and percentage gap for each workload
- correctness verification
- allocation-failure verification
- remaining known bottlenecks
- whether any architecture-specific work was added and how portability is preserved

Stop only when either:

- Zig is within the primary target thresholds on local aarch64 medians with correctness gates green, or
- further improvement requires a larger architecture change, and `checkpoints.md` clearly explains the blocker and proposed next design.
