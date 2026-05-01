# Goal 0003: Make Zig `parallel-hashmap` Port Performance On-Par With C++

Optimize the Zig `zig_phmap` implementation from Goal 0002 until lookup and iteration performance are at least on-par with upstream C++ `parallel-hashmap` v2.0.0 on the same machine and benchmark workload.

## Background

Goal 0002 produced a correct first-pass Zig package with:

- `FlatHashMap`
- `FlatHashSet`
- `NodeHashMap`
- `NodeHashSet`
- correctness tests
- allocation-failure tests
- Zig and C++ benchmarks

The first benchmark showed several strong results, but three critical operations were too slow versus C++:

- `lookup_hit`: Zig was 28.6% slower.
- `lookup_miss`: Zig was 356.3% slower.
- `iterate`: Zig was 29.6% slower.

This is not acceptable for the final port. The next goal is to close those gaps without sacrificing correctness, safety, allocator behavior, or API clarity.

## Objective

Redesign and optimize the flat hash table hot paths so the Zig implementation is at least on-par with C++ `parallel-hashmap` on the same benchmark workload.

Primary targets:

- `lookup_hit`: Zig must be no slower than C++.
- `lookup_miss`: Zig must be no slower than C++.
- `iterate`: Zig must be no slower than C++.

Secondary targets:

- Do not regress `insert_reserved`, `mixed`, `remove`, `string_insert`, or `string_lookup` by more than 10% from the Goal 0002 Zig baseline unless the regression is documented and justified by a larger win.
- Preserve all public APIs implemented in Goal 0002.
- Preserve allocation-failure correctness.
- Preserve node-container pointer stability.

## Baseline To Beat

Use the Goal 0002 benchmark commands and benchmark implementation unless this goal deliberately improves benchmark fairness. If benchmark code changes, record both old and new numbers.

Goal 0002 Zig baseline:

- `insert_reserved`: 27.888 ns/op
- `lookup_hit`: 18.292 ns/op
- `lookup_miss`: 17.536 ns/op
- `iterate`: 4.859 ns/item
- `mixed`: 35.774 ns/op
- `remove`: 16.072 ns/op
- `string_insert`: 15.738 ns/op
- `string_lookup`: 13.935 ns/op

Goal 0002 C++ `parallel-hashmap` baseline:

- `insert_reserved`: 36.034 ns/op
- `lookup_hit`: 14.229 ns/op
- `lookup_miss`: 3.843 ns/op
- `iterate`: 3.748 ns/item
- `mixed`: 41.503 ns/op
- `remove`: 24.584 ns/op
- `string_insert`: 41.154 ns/op
- `string_lookup`: 21.835 ns/op

Required final threshold:

- Zig `lookup_hit` <= C++ `lookup_hit`
- Zig `lookup_miss` <= C++ `lookup_miss`
- Zig `iterate` <= C++ `iterate`

Use multiple benchmark runs and compare medians or a clearly documented representative stable sample. Do not claim parity from a single noisy outlier.

## Required Source Inspection

Before optimizing, inspect the upstream implementation locally from:

- `.deps/parallel-hashmap-v2.0.0.tar.gz`
- extracted tree `.deps/parallel-hashmap-2.0.0` if present

Focus on:

- `parallel_hashmap/phmap.h`
- control-byte layout
- group probing
- cloned/sentinel control bytes
- `Group` abstractions
- `find` / `find_first_non_full`
- `drop_deletes_without_resize`
- iterator implementation
- resize and tombstone cleanup policy

Record the findings in `checkpoints.md` before changing code.

## Required Design Direction

The current scalar linear-probe implementation is not sufficient for upstream-level miss lookup or iteration performance. Implement a Swiss-table-style metadata group architecture.

Required design properties:

- Store control bytes in a layout compatible with group probing.
- Maintain cloned trailing control bytes or an equivalent correct strategy so probe groups can load without per-byte boundary checks.
- Use 7-bit H2 fingerprints for full slots.
- Use distinct empty, deleted, and sentinel states.
- Probe in groups rather than one slot at a time.
- Generate masks for:
  - matching H2 fingerprints
  - empty slots
  - deleted slots
  - non-full slots
- Use mask iteration to test only candidate entries for equality.
- Stop unsuccessful lookup as soon as a group contains an empty slot.
- Use a faster iterator that skips groups with no full slots.
- Keep tombstone cleanup deterministic and benchmarked.
- Keep a growth-left or equivalent capacity counter so tombstones do not silently degrade long-running workloads.

## SIMD and Portability

Prefer a portable abstraction with target-specific fast paths.

Minimum acceptable implementation:

- A group abstraction that can be implemented with scalar fallback.
- An optimized path for the current target if Zig 0.17 supports it cleanly.

Preferred implementation:

- 16-byte group probing for aarch64 using Zig vector operations where practical.
- Portable scalar fallback that remains correct on all targets.

Do not add unsafe target-specific code unless:

- it is isolated behind a small abstraction,
- it has tests,
- it has a documented fallback,
- it measurably improves the benchmark.

## Correctness Requirements

All Goal 0002 correctness behavior must remain intact.

Add or expand tests for:

- group boundary probing near the end of the control array
- cloned control bytes after insert, delete, rehash, shrink, and clear
- tombstone reuse
- unsuccessful lookup with long probe chains
- repeated insert/delete churn
- high load factor lookup
- iterator coverage with sparse tables, dense tables, tombstones, and after rehash
- randomized model tests after group-probing changes
- custom hash/equality contexts
- byte-slice keys
- node map pointer stability after multiple flat-index rehashes
- allocation failure during rehash and growth

Correctness gates that must pass:

```sh
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build test
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build -Doptimize=ReleaseSafe test
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build -Doptimize=ReleaseFast test
```

Use this exact Zig toolchain unless a newer goal explicitly changes it.

## Benchmark Requirements

Benchmark both Zig and C++ after every meaningful optimization.

Required commands:

```sh
g++ -O3 -DNDEBUG -std=c++17 -I .deps/parallel-hashmap-2.0.0 .deps/parallel_hashmap_bench.cc -o .deps/parallel_hashmap_bench
/home/wr/gh/zig_tree/.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build -Doptimize=ReleaseFast bench
.deps/parallel_hashmap_bench
```

Required workloads:

- `u64 -> u64` reserved insert
- successful lookup
- unsuccessful lookup
- iteration
- mixed insert/lookup/remove
- remove
- byte-slice or string key insert
- byte-slice or string key lookup
- tombstone churn workload
- high-load-factor unsuccessful lookup

If the benchmark does not currently include tombstone churn and high-load-factor misses, add them.

## Checkpoint Requirements

Update `checkpoints.md` after every meaningful step.

Each checkpoint must include:

- timestamp
- short description
- files changed
- source-inspection notes when relevant
- benchmark commands
- Zig benchmark results
- C++ benchmark results
- percentage gaps for all measured operations
- correctness commands and pass/fail status
- notes on regressions
- next optimization hypothesis

Required checkpoints:

- upstream source inspection
- pre-change baseline rerun
- first group-control implementation
- first passing correctness milestone
- first benchmark after group probing
- iterator optimization milestone
- miss-lookup optimization milestone
- tombstone cleanup/churn milestone
- final parity audit

## Performance Analysis Requirements

Do not only report benchmark numbers. Explain why the numbers changed.

For each optimization, identify which factor moved:

- fewer control-byte checks
- fewer equality checks
- fewer branches
- fewer cache misses
- reduced modulo or masking overhead
- better tombstone cleanup
- fewer allocations
- faster iteration over full-slot masks
- better compiler vectorization
- explicit vector operations

If an optimization fails, keep the result in `checkpoints.md` and explain why it was rejected or reverted.

## Completion Criteria

This goal is complete only when:

- `lookup_hit` is on-par with or faster than C++.
- `lookup_miss` is on-par with or faster than C++.
- `iterate` is on-par with or faster than C++.
- no other benchmark operation regresses by more than 10% from the Goal 0002 Zig baseline without documented justification.
- all required correctness gates pass.
- `checkpoints.md` contains the final parity audit.
- the implementation remains allocator-aware and API-compatible with Goal 0002.

Final checkpoint must include:

- starting Zig and C++ numbers
- ending Zig and C++ numbers
- percentage gap for each operation
- correctness verification
- allocation-failure verification
- summary of implemented layout changes
- known remaining bottlenecks, if any
- explicit statement whether the on-par target was achieved

## Stop Conditions

Do not stop just because one benchmark run looks good.

Stop only when either:

- the parity target is met on stable repeated benchmark runs, or
- further improvement requires a larger redesign that is clearly documented with evidence, including why the current group-control architecture cannot reach parity.

If parity is not reached, the final checkpoint must propose the next concrete architecture, not a vague "optimize more" note.
