# Goal 0002: Port `parallel-hashmap` to Zig

Implement a production-quality Zig port of Greg Popovitch's `parallel-hashmap` v2.0.0.

## Objective

Build a Zig hash table package based on `parallel-hashmap`, with excellent correctness and performance. This must not be a toy or proof of concept. The implementation should be suitable as a serious container library: well-tested, allocation-safe, API-stable, and benchmarked against the original C++ implementation on the same machine and workload.

## Source Material

- Reference archive: `.deps/parallel-hashmap-v2.0.0.tar.gz`
- Upstream URL: `https://github.com/greg7mdp/parallel-hashmap/archive/refs/tags/v2.0.0.tar.gz`
- Extract or inspect the archive locally before implementation. Do not rely on memory or summaries of the project.
- Important upstream areas to inspect:
  - `parallel_hashmap/phmap.h`
  - `parallel_hashmap/btree.h`
  - `parallel_hashmap/phmap_utils.h`
  - `README.md`
  - `examples/`
  - `tests/`
  - `benchmark/`

## Target Deliverable

Create a Zig package for hash maps and hash sets inspired by `parallel-hashmap`, including:

- `FlatHashMap`
- `FlatHashSet`
- `NodeHashMap`
- `NodeHashSet`
- Parallel or sharded variants when the design is stable enough:
  - `ParallelFlatHashMap`
  - `ParallelFlatHashSet`
  - `ParallelNodeHashMap`
  - `ParallelNodeHashSet`

If the full parallel API requires more work than the first pass allows, implement the non-parallel flat/node containers first, then document the precise sharding/locking design and remaining work in `zig_phmap/checkpoints.md`.

## Required Design Properties

Preserve the core design ideas that make `parallel-hashmap` fast:

- Swiss-table style open addressing for flat containers.
- Control-byte or metadata groups that allow fast probing.
- Cache-friendly contiguous storage for flat maps/sets.
- High load factor without pathological probe lengths.
- Efficient insert, lookup, remove, iteration, reserve, and rehash.
- Tombstone/deleted-slot handling that does not degrade long-running workloads.
- Support for custom hash and equality functions.
- Allocator-aware operation using Zig allocators.
- Clear ownership semantics for keys and values.
- Node containers with pointer/reference stability where appropriate.
- Parallel/sharded containers with per-shard locking or equivalent safe synchronization when implemented.

Do not remove correctness checks, tests, or safety-oriented code purely to improve benchmark numbers.

## API Requirements

The Zig API should feel idiomatic while exposing the expected container operations:

- `init`, `initContext`, and `deinit`
- `clear`, `clearRetainingCapacity`
- `len`, `capacity`, `isEmpty`
- `contains`
- `get`, `getConst`, `getEntry`, `getEntryConst`
- `put`, `insert`, `getOrPut`, `getOrPutValue`
- `remove`, `fetchRemove`
- `reserve`, `ensureTotalCapacity`, `shrinkAndFree` or equivalent
- forward iteration over entries and keys
- mutable and const access where appropriate
- configurable hash/equality context
- map and set variants sharing implementation where reasonable

Document intentional API differences from the C++ library in `README.md` or a dedicated design note.

## Correctness Requirements

Implement a broad test suite, not only smoke tests:

- deterministic unit tests for all public operations
- randomized model tests against Zig standard containers or a simple sorted/reference model
- insertion of duplicate keys
- deletion of present and absent keys
- repeated insert/delete workloads that exercise tombstones and rehashing
- reserve/rehash/shrink behavior
- iterator coverage before and after mutations
- custom hash/equality contexts
- byte slice and integer key coverage
- large value and zero-sized value coverage where applicable
- allocation failure tests using Zig's failing allocator
- validation/invariant checks for control bytes, capacity, growth, tombstones, and probe sequence assumptions
- parallel/sharded tests under concurrent workloads if parallel containers are implemented

Correctness gates that must stay green:

```sh
.toolchains/zig-aarch64-linux-0.16.0/zig build test
.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseSafe test
.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseFast test
```

Use the exact Zig toolchain above unless a newer goal explicitly changes it.

## Performance Requirements

Create comparable benchmarks for both the Zig port and the original C++ `parallel-hashmap` implementation.

Benchmark workloads should include at least:

- `u64 -> u64` random insert
- `u64 -> u64` successful lookup
- `u64 -> u64` unsuccessful lookup
- ordered or raw iteration over all entries
- erase/remove of all inserted keys
- mixed insert/lookup/remove workload
- string or byte-slice keys
- reserve-before-insert and no-reserve variants
- high-load-factor behavior
- repeated churn workload to measure tombstone cleanup
- multi-threaded insert/lookup benchmarks for parallel containers when implemented

Use the same key generation, operation ordering, and item counts for C++ and Zig. Start with 1,000,000 entries for the main benchmark.

Benchmark commands should be available through build steps, for example:

```sh
.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseFast bench
.deps/parallel_hashmap_bench
```

The exact C++ benchmark binary name may differ, but it must be recorded in `zig_phmap/checkpoints.md`.

## Performance Target

The Zig implementation should aim to be within approximately 20% of `parallel-hashmap` on most single-threaded operations for equivalent containers and workloads.

For parallel containers, target competitive scaling versus the upstream sharded containers on the same machine. If parity is not reached, document whether the bottleneck is hashing, probing, allocation, tombstone cleanup, cache layout, synchronization, or compiler/codegen behavior.

## Implementation Guidance

Prefer production architecture over quick benchmark tricks:

- Use compact metadata/control-byte arrays.
- Keep hot probe paths branch-light.
- Separate control metadata from slots if that matches the upstream design.
- Avoid unnecessary hashing, equality checks, and modulo operations.
- Use power-of-two capacities where appropriate.
- Use SIMD/group probing only if it is correct and portable enough for the target Zig version.
- Keep resize and tombstone cleanup behavior deterministic and well-tested.
- Avoid benchmark-only special cases.
- Add comments only where the algorithm is subtle.

## Record Keeping

Create or update `zig_phmap/checkpoints.md` after every meaningful step.

Each checkpoint must include:

- timestamp
- short description of the change
- files changed
- benchmark command used
- Zig benchmark results
- C++ `parallel-hashmap` benchmark results when rerun
- correctness commands run and pass/fail status
- notes on regressions, hypotheses, and next optimization target

Use checkpoint entries for:

- initial source inspection
- first compiling implementation
- each major API milestone
- each correctness milestone
- each benchmark baseline
- each meaningful optimization
- final audit

## Completion Criteria

Produce a final `zig_phmap/checkpoints.md` entry summarizing:

- implemented containers and APIs
- intentionally missing APIs, if any
- starting Zig performance
- ending Zig performance
- C++ `parallel-hashmap` comparison
- percentage gap for insert, lookup, unsuccessful lookup, iteration, remove, and mixed workload
- correctness verification
- allocation-failure verification
- remaining known bottlenecks
- proposed next architecture if parity is not reached

Stop only when either:

- Zig is within approximately 20% of `parallel-hashmap` on most required operations, with correctness gates green, or
- further improvements require a major redesign, and `zig_phmap/checkpoints.md` clearly explains the blocker and proposed next architecture.

## Completion Audit

Before marking the goal complete:

- Restate the objective as concrete deliverables.
- Map every requirement in this file to actual code, tests, benchmark output, or documented rationale.
- Run the required correctness gates with the exact Zig toolchain.
- Run the Zig and C++ benchmarks and record results.
- Verify `zig_phmap/checkpoints.md` includes the final summary and any remaining bottlenecks.
- Treat uncertainty as incomplete; either verify more or document the blocker precisely.
