# Goal 0005: Improve Zig B-tree Iteration on aarch64

Optimize Zig `zig_btree` ordered iteration until it is within 20% of the C++ Abseil `absl::btree_map` benchmark on the local aarch64 development host.

## Background

The earlier B-tree performance pass improved the Zig implementation but still left substantial local gaps versus Abseil, especially ordered iteration and remove. The current repository also has a root `bench` step for `zig_phmap`, so B-tree performance work must run benchmarks from the `zig_btree/` package directly.

The current compact-leaf-storage pass materially improved all B-tree workloads, but iteration still misses the parity target:

- Local aarch64 Zig median after compact leaf storage:
  - insert: 136.686 ns/op
  - lookup: 175.685 ns/op
  - iterate: 7.350 ns/item
  - remove: 200.519 ns/op
- Local aarch64 Abseil median from the same audit:
  - insert: 116.479 ns/op
  - lookup: 138.838 ns/op
  - iterate: 3.821 ns/item
  - remove: 129.703 ns/op
- Current iteration gap:
  - Zig is 92.4% slower than Abseil.
  - The 20% target requires Zig `iterate` <= 4.585 ns/item against that Abseil median.

These numbers are still noisy. The first step in this goal is to establish a fresh repeated local aarch64 median baseline from the current code before changing iteration internals.

## Objective

Improve `zig_btree` ordered iteration so its local aarch64 median performance is competitive with C++ Abseil `absl::btree_map` on the same workload.

Primary target:

- `iterate`: Zig must be no slower than 20% over C++ Abseil median, or faster, on repeated local aarch64 samples.

Secondary targets:

- Do not regress `insert`, `lookup`, or `remove` by more than 10% from the compact-leaf-storage local median unless the regression is documented and explicitly justified.
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
- whether a leaf-local forward iterator can avoid the generic cursor path
- whether leftmost/rightmost or leaf successor links can make forward iteration leaf-linear
- whether internal separator entries force unavoidable parent traversal under the current value-in-all-nodes design

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
- begin/end and increment implementation
- leftmost/rightmost tracking
- erase/rebalance strategy
- allocation count and cache locality

Record source-inspection findings in `checkpoints.md` before making performance changes.

## Optimization Direction

Prefer production architecture improvements over benchmark-only shortcuts.

Likely areas to evaluate:

- Add a forward-iteration fast path that stays within a leaf while possible and minimizes parent traversal when a leaf is exhausted.
- Add leaf predecessor/successor links if they materially reduce iteration overhead and can be maintained safely through split, merge, borrow, root shrink, and clear.
- Evaluate whether a B+ tree/value-in-leaf design is required to meet the 20% iteration target, and document the migration plan if so.
- Revisit iterator representation so the common forward iterator does not carry unnecessary cursor functionality.
- Use cached leftmost/rightmost leaf pointers if they remove repeated descent or simplify begin/end.
- Reduce iterator overhead by fast-pathing within-node advancement and minimizing parent traversal.
- Keep insert, remove, split, merge, and borrow maintenance costs bounded when adding iteration metadata.
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
- forward and reverse iteration after split, merge, borrow, root shrink, and clear
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

Primary pass/fail is based on `iterate`. The other workloads are regression guards.

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
- first iteration hot-path optimization
- first correctness milestone after optimization
- first benchmark showing a material `iterate` median improvement
- first benchmark showing `iterate` within 20% of C++ Abseil, if achieved
- final audit comparing starting and ending local aarch64 results

## Completion Criteria

Produce a final `checkpoints.md` entry summarizing:

- starting local aarch64 Zig performance
- ending local aarch64 Zig performance
- C++ Abseil comparison and percentage gap for each workload
- explicit `iterate` target calculation and pass/fail status
- correctness verification
- allocation-failure verification
- remaining known bottlenecks
- whether any architecture-specific work was added and how portability is preserved

Stop only when either:

- Zig `iterate` is within 20% of C++ Abseil on local aarch64 medians, `insert`/`lookup`/`remove` regressions are within the documented guardrails, and correctness gates are green, or
- reaching the 20% `iterate` target requires a larger architecture change, and `checkpoints.md` clearly explains the blocker and proposed next design.
