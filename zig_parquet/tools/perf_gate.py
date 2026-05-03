#!/usr/bin/env python3
import argparse
import os
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]
BENCH = ROOT / "zig_parquet" / "tools" / "bench_parquet.py"
FULL_CASES = (
    "read-pyarrow-none",
    "read-pyarrow-snappy",
    "read-pyarrow-gzip",
    "read-pyarrow-lz4",
    "read-pyarrow-zstd",
)
QUICK_CASES = (
    "read-pyarrow-none",
    "read-pyarrow-snappy",
)
WRITE_CASES = (
    "write-",
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Zig/PyArrow throughput gates used for README perf claims.")
    parser.add_argument("--profile", choices=("quick", "full", "write"), default="quick")
    parser.add_argument("--rows", type=int, default=None)
    parser.add_argument("--read-iterations", type=int, default=None)
    parser.add_argument("--min-read-ratio", type=float, default=0.70)
    parser.add_argument("--min-write-ratio", type=float, default=0.70)
    parser.add_argument("--case-filter", action="append", default=None)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--no-evict-os-cache", action="store_true", help="skip POSIX_FADV_DONTNEED even when available")
    args = parser.parse_args()

    rows = args.rows if args.rows is not None else (250_000 if args.profile in ("full", "write") else 50_000)
    iterations = args.read_iterations if args.read_iterations is not None else (20 if args.profile == "full" else 1 if args.profile == "write" else 3)
    cases = tuple(args.case_filter) if args.case_filter is not None else (WRITE_CASES if args.profile == "write" else FULL_CASES if args.profile == "full" else QUICK_CASES)
    evict = args.profile == "full" and not args.no_evict_os_cache
    if evict and (not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED")):
        raise SystemExit("full perf gate requires POSIX_FADV_DONTNEED; pass --no-evict-os-cache to run without cache eviction")

    cmd = [
        sys.executable,
        str(BENCH),
        "--rows",
        str(rows),
        "--read-iterations",
        str(iterations),
    ]
    if args.profile == "write":
        cmd.extend(("--min-write-ratio", f"{args.min_write_ratio:.6g}"))
    else:
        cmd.extend((
            "--fresh-reader",
            "--pyarrow-use-threads",
            "--zig-parallel-columns",
            "--min-read-ratio",
            f"{args.min_read_ratio:.6g}",
        ))
        if evict:
            cmd.append("--evict-os-cache")
    if args.skip_build:
        cmd.append("--skip-build")
    for case in cases:
        cmd.extend(("--case-filter", case))

    print("perf-gate-command " + " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)
    if args.profile == "write":
        print(f"perf-gate-ok profile={args.profile} rows={rows} min_write_ratio={args.min_write_ratio:.2f}")
    else:
        print(f"perf-gate-ok profile={args.profile} rows={rows} read_iterations={iterations} min_read_ratio={args.min_read_ratio:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
