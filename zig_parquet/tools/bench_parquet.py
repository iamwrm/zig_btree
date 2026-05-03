#!/usr/bin/env python3
import argparse
import pathlib
import subprocess
import time

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = pathlib.Path(__file__).resolve().parents[2]
TMP = ROOT / "zig_parquet" / "tmp"
ZIG = ROOT / ".toolchains" / "zig-aarch64-linux-0.17.0-dev.135+9df02121d" / "zig"
ZIG_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_sequence"
ZIG_BENCH_READER = ROOT / "zig-out" / "bin" / "parquet_bench_read"


def run(cmd: list[str]) -> float:
    start = time.perf_counter()
    subprocess.run(cmd, cwd=ROOT, check=True)
    return time.perf_counter() - start


def run_capture(cmd: list[str]) -> str:
    result = subprocess.run(cmd, cwd=ROOT, check=True, text=True, capture_output=True)
    return result.stdout


def parse_elapsed_seconds(output: str) -> float:
    elapsed_ns = None
    for line in output.splitlines():
        key, value = line.split("=", 1)
        if key == "elapsed_ns":
            elapsed_ns = int(value)
            break
    if elapsed_ns is None:
        raise AssertionError(f"missing elapsed_ns in benchmark output: {output!r}")
    return elapsed_ns / 1_000_000_000


def run_zig_read_bench(path: pathlib.Path, rows: int, mode: str, iterations: int) -> float:
    return parse_elapsed_seconds(run_capture([str(ZIG_BENCH_READER), str(path), str(rows), mode, str(iterations)]))


def write_pyarrow(path: pathlib.Path, rows: int, compression: str) -> float:
    start = time.perf_counter()
    table = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "score": pa.array((float(i) * 0.25 for i in range(rows)), type=pa.float64()),
            "name": pa.array((None if i % 7 == 0 else f"name-{i & 15}" for i in range(rows)), type=pa.string()),
        }
    )
    pq.write_table(
        table,
        path,
        compression=compression,
        use_dictionary=True,
        data_page_version="1.0",
        data_page_size=64 * 1024,
        row_group_size=64 * 1024,
    )
    return time.perf_counter() - start


def read_pyarrow(path: pathlib.Path, rows: int, columns: tuple[int, ...] | None, iterations: int, use_threads: bool) -> float:
    parquet_file = pq.ParquetFile(path)
    names = None if columns is None else [parquet_file.schema_arrow.names[idx] for idx in columns]

    table = parquet_file.read(columns=names, use_threads=use_threads)
    validate_pyarrow_ids(table, rows)

    start = time.perf_counter()
    for _ in range(iterations):
        table = parquet_file.read(columns=names, use_threads=use_threads)
        validate_pyarrow_ids(table, rows)
    return time.perf_counter() - start


def validate_pyarrow_ids(table: pa.Table, rows: int) -> None:
    if table.num_rows != rows:
        raise AssertionError(f"PyArrow read {table.num_rows} rows, expected {rows}")
    if rows == 0 or "id" not in table.schema.names:
        return
    ids = table.column("id")
    if ids[0].as_py() != 0 or ids[rows - 1].as_py() != rows - 1:
        raise AssertionError("PyArrow id validation failed")


def parquet_uncompressed_mib(path: pathlib.Path, columns: tuple[int, ...] | None = None) -> float:
    metadata = pq.ParquetFile(path).metadata
    total = 0
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        selected = columns if columns is not None else range(row_group.num_columns)
        for col_idx in selected:
            total += row_group.column(col_idx).total_uncompressed_size
    return total / (1024 * 1024)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=250_000)
    parser.add_argument("--read-iterations", type=int, default=5)
    parser.add_argument("--min-read-ratio", type=float, default=0.0, help="fail if any Zig read throughput is below this fraction of PyArrow for the same file")
    parser.add_argument("--pyarrow-use-threads", action="store_true", help="compare against PyArrow's threaded reader; default is single-threaded PyArrow for comparable decode-path ratios")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    if args.read_iterations <= 0:
        raise ValueError("--read-iterations must be positive")

    TMP.mkdir(parents=True, exist_ok=True)
    if not args.skip_build:
        run([str(ZIG), "build", "-Doptimize=ReleaseFast"])

    rows = args.rows
    cases: list[tuple[str, pathlib.Path, float, tuple[int, ...] | None, int]] = []
    read_cases: list[tuple[str, pathlib.Path, float, float, tuple[int, ...] | None, int]] = []

    def add_zig_reads(label: str, path: pathlib.Path) -> None:
        all_seconds = run_zig_read_bench(path, rows, "all", args.read_iterations)
        cases.append((f"zig-read-zig-{label}", path, all_seconds, None, args.read_iterations))
        pyarrow_all_seconds = read_pyarrow(path, rows, None, args.read_iterations, args.pyarrow_use_threads)
        cases.append((f"pyarrow-read-{label}", path, pyarrow_all_seconds, None, args.read_iterations))
        read_cases.append((f"read-{label}", path, all_seconds, pyarrow_all_seconds, None, args.read_iterations))

        id_seconds = run_zig_read_bench(path, rows, "ids", args.read_iterations)
        cases.append((f"zig-read-id-zig-{label}", path, id_seconds, (0,), args.read_iterations))
        pyarrow_id_seconds = read_pyarrow(path, rows, (0,), args.read_iterations, args.pyarrow_use_threads)
        cases.append((f"pyarrow-read-id-{label}", path, pyarrow_id_seconds, (0,), args.read_iterations))
        read_cases.append((f"read-id-{label}", path, id_seconds, pyarrow_id_seconds, (0,), args.read_iterations))

    for codec in ("uncompressed", "snappy", "gzip", "zstd"):
        path = TMP / f"bench_zig_{codec}.parquet"
        seconds = run([str(ZIG_WRITER), str(path), str(rows), codec, "65536", "16384"])
        cases.append((f"zig-write-{codec}", path, seconds, None, 1))
        add_zig_reads(codec, path)

    path = TMP / "bench_zig_zstd_bss.parquet"
    seconds = run([str(ZIG_WRITER), str(path), str(rows), "zstd", "65536", "16384", "v1", "byte_stream_split"])
    cases.append(("zig-write-zstd-bss", path, seconds, None, 1))
    add_zig_reads("zstd-bss", path)

    path = TMP / "bench_zig_zstd_delta.parquet"
    seconds = run([str(ZIG_WRITER), str(path), str(rows), "zstd", "65536", "16384", "v1", "delta_binary_packed"])
    cases.append(("zig-write-zstd-delta", path, seconds, None, 1))
    add_zig_reads("zstd-delta", path)

    path = TMP / "bench_zig_zstd_delta_bss.parquet"
    seconds = run([str(ZIG_WRITER), str(path), str(rows), "zstd", "65536", "16384", "v1", "delta_binary_packed+byte_stream_split"])
    cases.append(("zig-write-zstd-delta-bss", path, seconds, None, 1))
    add_zig_reads("zstd-delta-bss", path)

    for encoding, case_suffix in (
        ("delta_length_byte_array", "delta-len"),
        ("delta_byte_array", "delta-ba"),
    ):
        path = TMP / f"bench_zig_zstd_{case_suffix}.parquet"
        seconds = run([str(ZIG_WRITER), str(path), str(rows), "zstd", "65536", "16384", "v1", encoding])
        cases.append((f"zig-write-zstd-{case_suffix}", path, seconds, None, 1))
        add_zig_reads(f"zstd-{case_suffix}", path)

    for compression in ("NONE", "SNAPPY", "GZIP", "ZSTD"):
        path = TMP / f"bench_pyarrow_{compression.lower()}.parquet"
        seconds = write_pyarrow(path, rows, compression)
        label = f"pyarrow-{compression.lower()}"
        cases.append((f"pyarrow-write-{compression.lower()}", path, seconds, None, 1))
        add_zig_reads(label, path)

    print("case,file_mib,parquet_uncompressed_mib,iterations,seconds,file_mib_per_s,uncompressed_mib_per_s")
    for name, path, seconds, columns, iterations in cases:
        uncompressed_mib = parquet_uncompressed_mib(path, columns)
        work_file_mib = path.stat().st_size / (1024 * 1024) * iterations
        work_uncompressed_mib = uncompressed_mib * iterations
        file_rate = work_file_mib / seconds if seconds > 0 else float("inf")
        uncompressed_rate = work_uncompressed_mib / seconds if seconds > 0 else float("inf")
        print(
            f"{name},{path.stat().st_size / (1024 * 1024):.2f},"
            f"{uncompressed_mib:.2f},{iterations},{seconds:.4f},{file_rate:.2f},{uncompressed_rate:.2f}"
        )

    failures = []
    if read_cases:
        print("comparison,zig_uncompressed_mib_per_s,pyarrow_uncompressed_mib_per_s,zig_vs_pyarrow")
    for name, path, zig_seconds, pyarrow_seconds, columns, iterations in read_cases:
        uncompressed_mib = parquet_uncompressed_mib(path, columns) * iterations
        zig_rate = uncompressed_mib / zig_seconds if zig_seconds > 0 else float("inf")
        pyarrow_rate = uncompressed_mib / pyarrow_seconds if pyarrow_seconds > 0 else float("inf")
        ratio = zig_rate / pyarrow_rate if pyarrow_rate > 0 else float("inf")
        print(f"{name},{zig_rate:.2f},{pyarrow_rate:.2f},{ratio:.3f}")
        if args.min_read_ratio > 0 and ratio < args.min_read_ratio:
            failures.append(f"{name}: {ratio:.3f} < {args.min_read_ratio:.3f}")

    if failures:
        raise SystemExit("read throughput below threshold:\n" + "\n".join(failures))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
