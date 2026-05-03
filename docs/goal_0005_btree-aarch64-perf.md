# Goal 0005: Explain and Improve Zig B-tree Iteration on aarch64

Find the concrete implementation reasons Zig `zig_btree` ordered iteration is still slower than C++ Abseil `absl::btree_map`, then optimize Zig until iteration is within 20% of Abseil on the local aarch64 development host or document the architecture change required to get there.

## Background

The earlier B-tree performance pass improved the Zig implementation but still left substantial local gaps versus Abseil, especially ordered iteration and remove. The current repository also has a root `bench` step for `zig_phmap`, so B-tree performance work must run benchmarks from the `zig_btree/` package directly.

The compact-leaf-storage pass materially improved all B-tree workloads, but iteration still missed the parity target:

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

A later portable insertion-rebalancing pass improved occupancy and iteration but still did not close the gap:

- Local aarch64 Zig median after insertion rebalancing:
  - insert: 136.101 ns/op
  - lookup: 145.680 ns/op
  - iterate: 5.916 ns/item
  - remove: 167.492 ns/op
- Local aarch64 Abseil median from the same audit:
  - insert: 124.053 ns/op
  - lookup: 150.311 ns/op
  - iterate: 3.992 ns/item
  - remove: 131.939 ns/op
- Current iteration gap:
  - Zig is 48.2% slower than Abseil.
  - The 20% target requires Zig `iterate` <= 4.790 ns/item against that Abseil median.

The next pass must not assume the remaining gap is from generic "iterator overhead." It must look closely at Abseil's C++ implementation and attribute the gap to specific design or code-generation differences before making larger Zig changes.

## Objective

Identify the concrete reasons for the remaining local aarch64 iteration performance gap versus C++ Abseil, then improve `zig_btree` ordered iteration based on that evidence.

Primary target:

- `iterate`: Zig must be no slower than 20% over C++ Abseil median, or faster, on repeated local aarch64 samples.

Primary diagnostic deliverable:

- A checkpointed Abseil-versus-Zig gap attribution that ties the measured iteration gap to specific implementation differences, source locations, and local evidence. Vague explanations such as "C++ is more optimized" or "iterator overhead" are not sufficient.

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

Before changing Zig code, inspect the current Zig B-tree implementation and the Abseil comparison point deeply enough to explain the remaining measured gap.

Inspect Zig:

- `zig_btree/src/btree.zig`
- `zig_btree/bench/btree_bench.zig`
- `zig_btree/test/btree_stress.zig`

Focus on:

- node layout and `deriveMaxSlots`
- final benchmark node height, leaf count, internal-node count, fullness, and bytes used
- leaf versus internal node storage
- child pointer storage in leaf nodes
- in-node search policy and thresholds
- insertion split path
- insertion sibling-rebalancing path and final tree occupancy
- lookup path and comparator calls
- iterator/cursor advancement
- remove path, merge, borrow, and root shrink
- parent pointer and child-position maintenance
- allocation behavior and node initialization
- invariant validation cost in ReleaseFast
- generated ReleaseFast code shape for `Iterator.next()`, `Cursor.advance()`, `childAt()`, and the benchmark iteration loop
- whether a leaf-local forward iterator can avoid the generic cursor path
- whether leftmost/rightmost or leaf successor links can make forward iteration leaf-linear
- whether internal separator entries force unavoidable parent traversal under the current value-in-all-nodes design
- how many iterator steps are leaf-local, leaf-to-parent, internal-to-leaf, and end transitions on the benchmark tree

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
- exact `btree_node` metadata fields and field widths
- `LeafSize()`, `InternalSize()`, `NodeTargetSlots()`, and actual slot count for `uint64_t -> uint64_t`
- whether Abseil generations are enabled in the local build and what iterator validation code remains
- `btree_iterator::increment()`, `increment_slow()`, dereference path, and range-for codegen
- root parent-as-leftmost and `rightmost_` end representation
- `rebalance_or_split()` insertion behavior and how it affects final occupancy
- whether Abseil avoids optional/null child loads, tagged unions, or extra branches in the iterator hot path
- whether Abseil's lower node count, node size, metadata packing, inlining, or generated machine code explains the measured per-item difference

Required Abseil evidence:

- Record the relevant Abseil source line ranges in `checkpoints.md`.
- Record Abseil's computed node slot count and leaf/internal allocation sizes for the benchmark key/value type.
- Compare Abseil and Zig benchmark tree shape after insert: height, leaf nodes, internal nodes, fullness or equivalent occupancy, and bytes per stored item where available.
- Inspect or generate local optimized assembly for the Abseil range-for iteration loop and the Zig benchmark iteration loop. Record only the specific differences that plausibly affect the measured gap.
- If using profiling tools such as `perf stat`, `perf record`, `objdump`, or compiler emitted assembly, record the exact commands and enough output to support the conclusion.
- Build a gap-attribution table that ranks likely causes by evidence strength and estimated impact:
  - tree shape and occupancy
  - leaf/internal allocation size
  - iterator hot-path branches and loads
  - parent traversal frequency
  - internal separator-entry traversal
  - generated code quality and inlining
  - benchmark harness differences

Record source-inspection findings in `checkpoints.md` before making performance changes.

## Optimization Direction

Prefer production architecture improvements over benchmark-only shortcuts.

Likely areas to evaluate:

- Match Abseil's proven behavior where it directly explains the measured gap.
- Add a forward-iteration fast path that stays within a leaf while possible and minimizes parent traversal when a leaf is exhausted.
- Add leaf predecessor/successor links if they materially reduce iteration overhead and can be maintained safely through split, merge, borrow, root shrink, and clear.
- Evaluate whether a B+ tree/value-in-leaf design is required to meet the 20% iteration target, and document the migration plan if so.
- Revisit iterator representation so the common forward iterator does not carry unnecessary cursor functionality.
- Use cached leftmost/rightmost leaf pointers if they remove repeated descent or simplify begin/end.
- Reduce iterator overhead by fast-pathing within-node advancement and minimizing parent traversal.
- Reduce node metadata footprint or optional child-pointer overhead if Abseil source/codegen evidence shows it contributes materially.
- Change insertion rebalancing, split policy, or node sizing only when tree-shape evidence shows it is a major remaining contributor.
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

Required diagnostic measurements before major architecture changes:

- Zig and Abseil tree-shape/occupancy measurements after building the benchmark tree.
- A focused iteration-only timing that excludes insert, lookup, and remove setup noise where practical.
- At least one codegen or profiling artifact comparing Zig and Abseil iteration hot paths.
- A written explanation of which measured difference is expected to close the gap and why.

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
- Abseil deep-dive gap attribution with source locations and local evidence
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
- the diagnosed root cause or ranked root causes of the remaining gap
- Abseil source locations and local evidence supporting that diagnosis
- explicit `iterate` target calculation and pass/fail status
- correctness verification
- allocation-failure verification
- remaining known bottlenecks
- whether any architecture-specific work was added and how portability is preserved

Stop only when either:

- Zig `iterate` is within 20% of C++ Abseil on local aarch64 medians, `insert`/`lookup`/`remove` regressions are within the documented guardrails, and correctness gates are green, or
- reaching the 20% `iterate` target requires a larger architecture change, and `checkpoints.md` clearly explains the blocker, the Abseil-backed reason for the blocker, and the proposed next design.
