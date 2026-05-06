Implement a performance parity pass for the Zig B-tree against Abseil’s absl::btree_map.                                                                   
                                                                                                                                                             
  Objective:                                                                                                                                                 
  Bring the Zig B-tree port’s benchmark performance as close as practical to Abseil’s B-tree on the same machine and workload, without sacrificing           
  correctness, API safety, or invariant validation.                                                                                                          
                                                                                                                                                             
  Baseline:                                                                                                                                                  
                                                                                                                                                             
  - Abseil benchmark: .deps/abseil_btree_bench                                                                                                               
  - Zig benchmark: zig_btree/bench/btree_bench.zig                                                                                                           
  - Workload: 1,000,000 random u64 -> u64 entries, measuring insert, lookup, ordered iteration, and remove.                                                  
  - Current Abseil target range:                                                                                                                             
      - insert: ~106-123 ns/op                                                                                                                               
      - lookup: ~119-121 ns/op                                                                                                                               
      - iterate: ~3.2-3.6 ns/item                                                                                                                            
      - remove: ~118-124 ns/op                                                                                                                               

  Requirements:

  1. Use the exact Zig toolchain at .toolchains/zig-aarch64-linux-0.16.0/zig.
  2. Keep correctness green:
      - zig build test
      - zig build -Doptimize=ReleaseSafe test
      - zig build -Doptimize=ReleaseFast test
  3. Benchmark after each meaningful optimization:
      - zig build -Doptimize=ReleaseFast bench
      - .deps/abseil_btree_bench
  4. Optimize production code, not just the benchmark.
  5. Do not remove invariant checks or tests to gain speed.
  6. Prefer Abseil-like design improvements:
      - cache-sized nodes
      - lower allocation count
      - compact node layout
      - faster in-node search
      - fewer pointer indirections
      - efficient deletion rebalancing
      - iteration with minimal branch/pointer overhead
  7. Maintain API behavior and existing tests.
  8. Add or update tests if an optimization changes internal invariants or edge behavior.

  Record keeping:
  Create or update zig_btree/checkpoints.md after every meaningful step. Each checkpoint must include:

  - timestamp
  - short description of the change
  - files changed
  - benchmark command used
  - Zig benchmark results
  - Abseil benchmark results when rerun 
  - correctness commands run and pass/fail status
  - notes on any regressions, hypotheses, or next optimization target


Completion criteria:

  - Produce a final zig_btree/checkpoints.md entry summarizing:
      - starting Zig performance
      - ending Zig performance
      - Abseil comparison
      - percentage gap for insert, lookup, iteration, and remove
      - correctness verification
      - remaining known bottlenecks
  - Stop only when either:
      - Zig is within ~20% of Abseil on most operations, or
      - further improvements require a major redesign, and zig_btree/checkpoints.md clearly explains the blocker and proposed next architecture.