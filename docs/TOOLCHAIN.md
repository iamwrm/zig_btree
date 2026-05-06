# Toolchain

This repository is built and tested with a pinned Zig stable toolchain.
Use the pinned version for local validation, CI parity, benchmark comparisons,
and goal work unless a goal explicitly says otherwise.

## Zig Version

Pinned version:

```text
0.16.0
```

CI downloads the Linux x86_64 archive:

```text
https://ziglang.org/download/0.16.0/zig-x86_64-linux-0.16.0.tar.xz
```

Local aarch64 benchmark history uses:

```text
https://ziglang.org/download/0.16.0/zig-aarch64-linux-0.16.0.tar.xz
```

The source archive is listed in [resources.md](../resources.md).

## Local Install Layout

Keep downloaded toolchains under the repo-local `.toolchains/` directory. This
directory is generated state and should not be committed.

Expected paths:

```text
.toolchains/zig-x86_64-linux-0.16.0/zig
.toolchains/zig-aarch64-linux-0.16.0/zig
```

Install on x86_64 Linux:

```sh
mkdir -p .toolchains
curl -fsSL \
  https://ziglang.org/download/0.16.0/zig-x86_64-linux-0.16.0.tar.xz \
  -o .toolchains/zig-x86_64-linux-0.16.0.tar.xz
tar -C .toolchains -xf .toolchains/zig-x86_64-linux-0.16.0.tar.xz
.toolchains/zig-x86_64-linux-0.16.0/zig version
```

Install on aarch64 Linux:

```sh
mkdir -p .toolchains
curl -fsSL \
  https://ziglang.org/download/0.16.0/zig-aarch64-linux-0.16.0.tar.xz \
  -o .toolchains/zig-aarch64-linux-0.16.0.tar.xz
tar -C .toolchains -xf .toolchains/zig-aarch64-linux-0.16.0.tar.xz
.toolchains/zig-aarch64-linux-0.16.0/zig version
```

## CI Coverage

GitHub Actions use the pinned x86_64 Linux toolchain in:

- `.github/workflows/zig-btree.yml`
- `.github/workflows/zig-parquet.yml`
- `.github/workflows/zig-phmap-bench.yml`
- `.github/workflows/zig-jinja.yml`

Do not rely on an unpinned `zig` from `PATH` when reproducing CI failures. Run
the matching `.toolchains/.../zig` binary directly.

## Verification Commands

Repository-wide correctness gate:

```sh
.toolchains/zig-x86_64-linux-0.16.0/zig build test --summary all
```

Package-specific gates:

```sh
.toolchains/zig-x86_64-linux-0.16.0/zig build parquet-test --summary all
.toolchains/zig-x86_64-linux-0.16.0/zig build jinja-test --summary all
cd zig_btree && ../.toolchains/zig-x86_64-linux-0.16.0/zig build test --summary all
cd zig_phmap && ../.toolchains/zig-x86_64-linux-0.16.0/zig build test --summary all
```

For release-mode validation, add `-Doptimize=ReleaseSafe` or
`-Doptimize=ReleaseFast`.

## Benchmark Notes

Performance goals and historical checkpoints use aarch64 local benchmark
results unless stated otherwise. Hosted GitHub Actions benchmark output is a
repository health signal only; do not use hosted runner timings as the final
acceptance gate for local performance goals.

Use the pinned aarch64 toolchain for local benchmark work on aarch64 machines:

```sh
.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseFast bench
```

Run package benchmarks from the package directory when the package has its own
`bench` step, for example:

```sh
cd zig_btree
../.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseFast bench
```
