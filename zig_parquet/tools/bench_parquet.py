#!/usr/bin/env python3
import argparse
import os
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


def evict_file_cache(path: pathlib.Path) -> None:
    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED"):
        raise RuntimeError("--evict-os-cache requires os.posix_fadvise and POSIX_FADV_DONTNEED")
    fd = os.open(path, os.O_RDONLY)
    try:
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)


def run_zig_read_bench(path: pathlib.Path, rows: int, mode: str, iterations: int, cache_dictionaries: bool, fresh_reader: bool, evict_os_cache: bool, parallel_columns: bool = False) -> float:
    cache_mode = "cache-dictionaries" if cache_dictionaries else "no-cache"
    reader_state = "fresh-reader" if fresh_reader else "reuse-reader"
    os_cache_mode = "evict-os-cache" if evict_os_cache else "keep-os-cache"
    column_execution = "parallel-columns" if parallel_columns else "serial-columns"
    return parse_elapsed_seconds(run_capture([str(ZIG_BENCH_READER), str(path), str(rows), mode, str(iterations), cache_mode, reader_state, os_cache_mode, column_execution]))


def run_zig_parallel_columns_bench(path: pathlib.Path, rows: int, iterations: int, cache_dictionaries: bool, evict_os_cache: bool) -> float:
    return run_zig_read_bench(path, rows, "all", iterations, cache_dictionaries, True, evict_os_cache, parallel_columns=True)


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


def read_pyarrow(path: pathlib.Path, rows: int, columns: tuple[int, ...] | None, iterations: int, use_threads: bool, fresh_reader: bool, evict_os_cache: bool) -> float:
    parquet_file = pq.ParquetFile(path)
    names = None if columns is None else [parquet_file.schema_arrow.names[idx] for idx in columns]

    table = parquet_file.read(columns=names, use_threads=use_threads)
    validate_pyarrow_ids(table, rows)

    if not evict_os_cache:
        start = time.perf_counter()
        for _ in range(iterations):
            if fresh_reader:
                parquet_file = pq.ParquetFile(path)
            table = parquet_file.read(columns=names, use_threads=use_threads)
            validate_pyarrow_ids(table, rows)
        return time.perf_counter() - start

    elapsed = 0.0
    for _ in range(iterations):
        evict_file_cache(path)
        start = time.perf_counter()
        if fresh_reader:
            parquet_file = pq.ParquetFile(path)
        table = parquet_file.read(columns=names, use_threads=use_threads)
        validate_pyarrow_ids(table, rows)
        elapsed += time.perf_counter() - start
    return elapsed


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
    parser.add_argument("--min-write-ratio", type=float, default=0.0, help="fail if any selected Zig writer throughput is below this fraction of PyArrow for the comparable codec")
    parser.add_argument("--pyarrow-use-threads", action="store_true", help="compare against PyArrow's threaded reader; default is single-threaded PyArrow for comparable decode-path ratios")
    parser.add_argument("--column-read-details", action="store_true", help="also compare score and name single-column reads; id reads are always included")
    parser.add_argument("--dictionary-cache", action="store_true", help="enable the Zig reader's decoded dictionary cache during repeated read timing")
    parser.add_argument("--zig-page-version", choices=("v1", "v2"), default="v1", help="data page version for Zig-written benchmark fixtures")
    parser.add_argument("--fresh-reader", action="store_true", help="reopen and reparse Zig and PyArrow readers for every timed read iteration")
    parser.add_argument("--evict-os-cache", action="store_true", help="best-effort POSIX_FADV_DONTNEED before every timed read iteration; combine with --fresh-reader for fresh library state plus OS page-cache eviction")
    parser.add_argument("--zig-parallel-columns", action="store_true", help="for PyArrow-compatible Zig timings, use the in-process StreamFileReader row-group/column-parallel APIs; requires --fresh-reader")
    parser.add_argument("--case-filter", action="append", default=[], help="only run benchmark cases whose generated case or comparison name contains this substring; may be passed more than once")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    if args.read_iterations <= 0:
        raise ValueError("--read-iterations must be positive")
    if args.evict_os_cache and (not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED")):
        raise RuntimeError("--evict-os-cache requires os.posix_fadvise and POSIX_FADV_DONTNEED")
    if args.zig_parallel_columns and not args.fresh_reader:
        raise ValueError("--zig-parallel-columns requires --fresh-reader")

    TMP.mkdir(parents=True, exist_ok=True)
    if not args.skip_build:
        run([str(ZIG), "build", "-Doptimize=ReleaseFast"])

    rows = args.rows
    cases: list[tuple[str, pathlib.Path, float, tuple[int, ...] | None, int]] = []
    read_cases: list[tuple[str, pathlib.Path, float, float, tuple[int, ...] | None, int]] = []
    filters = tuple(part.lower() for part in args.case_filter)

    def selected(name: str) -> bool:
        return not filters or any(part in name.lower() for part in filters)

    def append_case(name: str, path: pathlib.Path, seconds: float, columns: tuple[int, ...] | None, iterations: int) -> None:
        if selected(name):
            cases.append((name, path, seconds, columns, iterations))

    def detail_modes_for_fixture(include_name_details: bool) -> list[tuple[str, str, tuple[int, ...]]]:
        detail_modes: list[tuple[str, str, tuple[int, ...]]] = [("id", "ids", (0,))]
        if args.column_read_details:
            detail_modes.append(("score", "score", (1,)))
            if include_name_details:
                detail_modes.append(("name", "name", (2,)))
        return detail_modes

    def read_names_for_fixture(label: str, include_name_details: bool) -> list[str]:
        names = [f"zig-read-zig-{label}", f"pyarrow-read-{label}", f"read-{label}"]
        for output_name, _, _ in detail_modes_for_fixture(include_name_details):
            names.extend((
                f"zig-read-{output_name}-zig-{label}",
                f"pyarrow-read-{output_name}-{label}",
                f"read-{output_name}-{label}",
            ))
        return names

    def fixture_selected(write_name: str, label: str, include_name_details: bool) -> bool:
        return selected(write_name) or any(selected(name) for name in read_names_for_fixture(label, include_name_details))

    def add_zig_reads(label: str, path: pathlib.Path, include_name_details: bool) -> None:
        full_names = (f"zig-read-zig-{label}", f"pyarrow-read-{label}", f"read-{label}")
        if not any(selected(name) for name in full_names):
            all_seconds = None
            pyarrow_all_seconds = None
        elif args.zig_parallel_columns and include_name_details:
            all_seconds = run_zig_parallel_columns_bench(path, rows, args.read_iterations, args.dictionary_cache, args.evict_os_cache)
            pyarrow_all_seconds = read_pyarrow(path, rows, None, args.read_iterations, args.pyarrow_use_threads, args.fresh_reader, args.evict_os_cache)
        else:
            all_seconds = run_zig_read_bench(path, rows, "all", args.read_iterations, args.dictionary_cache, args.fresh_reader, args.evict_os_cache)
            pyarrow_all_seconds = read_pyarrow(path, rows, None, args.read_iterations, args.pyarrow_use_threads, args.fresh_reader, args.evict_os_cache)
        if all_seconds is not None and pyarrow_all_seconds is not None:
            append_case(f"zig-read-zig-{label}", path, all_seconds, None, args.read_iterations)
            append_case(f"pyarrow-read-{label}", path, pyarrow_all_seconds, None, args.read_iterations)
            if selected(f"read-{label}"):
                read_cases.append((f"read-{label}", path, all_seconds, pyarrow_all_seconds, None, args.read_iterations))

        for output_name, zig_mode, columns in detail_modes_for_fixture(include_name_details):
            detail_names = (
                f"zig-read-{output_name}-zig-{label}",
                f"pyarrow-read-{output_name}-{label}",
                f"read-{output_name}-{label}",
            )
            if not any(selected(name) for name in detail_names):
                continue
            zig_seconds = run_zig_read_bench(path, rows, zig_mode, args.read_iterations, args.dictionary_cache, args.fresh_reader, args.evict_os_cache, parallel_columns=args.zig_parallel_columns)
            pyarrow_seconds = read_pyarrow(path, rows, columns, args.read_iterations, args.pyarrow_use_threads, args.fresh_reader, args.evict_os_cache)
            append_case(f"zig-read-{output_name}-zig-{label}", path, zig_seconds, columns, args.read_iterations)
            append_case(f"pyarrow-read-{output_name}-{label}", path, pyarrow_seconds, columns, args.read_iterations)
            if selected(f"read-{output_name}-{label}"):
                read_cases.append((f"read-{output_name}-{label}", path, zig_seconds, pyarrow_seconds, columns, args.read_iterations))

    def add_written_fixture(write_name: str, label: str, path: pathlib.Path, write_cmd: list[str], include_name_details: bool) -> None:
        if not fixture_selected(write_name, label, include_name_details):
            return
        seconds = run(write_cmd)
        append_case(write_name, path, seconds, None, 1)
        add_zig_reads(label, path, include_name_details)

    def add_pyarrow_fixture(compression: str) -> None:
        label = f"pyarrow-{compression.lower()}"
        write_name = f"pyarrow-write-{compression.lower()}"
        if not fixture_selected(write_name, label, include_name_details=True):
            return
        path = TMP / f"bench_pyarrow_{compression.lower()}.parquet"
        seconds = write_pyarrow(path, rows, compression)
        append_case(write_name, path, seconds, None, 1)
        add_zig_reads(label, path, include_name_details=True)

    def add_zig_fixture(write_name: str, label: str, filename: str, codec: str, encoding: str | None = None) -> None:
        path = TMP / filename
        cmd = [str(ZIG_WRITER), str(path), str(rows), codec, "65536", "16384", args.zig_page_version]
        if encoding is not None:
            cmd.append(encoding)
        add_written_fixture(write_name, label, path, cmd, include_name_details=False)

    def add_zig_core_fixture(codec: str) -> None:
        add_zig_fixture(f"zig-write-{codec}", codec, f"bench_zig_{codec}.parquet", codec)

    def add_zig_zstd_fixture(suffix: str, encoding: str) -> None:
        add_zig_fixture(f"zig-write-zstd-{suffix}", f"zstd-{suffix}", f"bench_zig_zstd_{suffix}.parquet", "zstd", encoding)

    for codec in ("uncompressed", "snappy", "gzip", "lz4_raw", "zstd"):
        add_zig_core_fixture(codec)

    add_zig_zstd_fixture("bss", "byte_stream_split")
    add_zig_zstd_fixture("delta", "delta_binary_packed")
    add_zig_fixture("zig-write-zstd-delta-bss", "zstd-delta-bss", "bench_zig_zstd_delta_bss.parquet", "zstd", "delta_binary_packed+byte_stream_split")
    for encoding, case_suffix in (
        ("delta_length_byte_array", "delta-len"),
        ("delta_byte_array", "delta-ba"),
    ):
        add_zig_zstd_fixture(case_suffix, encoding)

    for compression in ("NONE", "SNAPPY", "GZIP", "LZ4", "ZSTD"):
        add_pyarrow_fixture(compression)

    print("case,file_mib,parquet_uncompressed_mib,iterations,seconds,file_mib_per_s,uncompressed_mib_per_s")
    write_rates: dict[str, float] = {}
    for name, path, seconds, columns, iterations in cases:
        uncompressed_mib = parquet_uncompressed_mib(path, columns)
        work_file_mib = path.stat().st_size / (1024 * 1024) * iterations
        work_uncompressed_mib = uncompressed_mib * iterations
        file_rate = work_file_mib / seconds if seconds > 0 else float("inf")
        uncompressed_rate = work_uncompressed_mib / seconds if seconds > 0 else float("inf")
        if name.startswith(("zig-write-", "pyarrow-write-")) and columns is None and iterations == 1:
            write_rates[name] = uncompressed_rate
        print(
            f"{name},{path.stat().st_size / (1024 * 1024):.2f},"
            f"{uncompressed_mib:.2f},{iterations},{seconds:.4f},{file_rate:.2f},{uncompressed_rate:.2f}"
        )

    failures = []
    write_pairs = (
        ("write-uncompressed", "zig-write-uncompressed", "pyarrow-write-none"),
        ("write-snappy", "zig-write-snappy", "pyarrow-write-snappy"),
        ("write-gzip", "zig-write-gzip", "pyarrow-write-gzip"),
        ("write-lz4", "zig-write-lz4_raw", "pyarrow-write-lz4"),
        ("write-zstd", "zig-write-zstd", "pyarrow-write-zstd"),
    )
    selected_write_pairs: list[tuple[str, str, str, float, float, float]] = []
    for comparison_name, zig_name, pyarrow_name in write_pairs:
        if zig_name not in write_rates or pyarrow_name not in write_rates:
            continue
        zig_rate = write_rates[zig_name]
        pyarrow_rate = write_rates[pyarrow_name]
        ratio = zig_rate / pyarrow_rate if pyarrow_rate > 0 else float("inf")
        selected_write_pairs.append((comparison_name, zig_name, pyarrow_name, zig_rate, pyarrow_rate, ratio))
        if args.min_write_ratio > 0 and ratio < args.min_write_ratio:
            failures.append(f"{comparison_name}: {ratio:.3f} < {args.min_write_ratio:.3f}")
    if selected_write_pairs:
        print("write_comparison,zig_uncompressed_mib_per_s,pyarrow_uncompressed_mib_per_s,zig_vs_pyarrow")
        for comparison_name, _, _, zig_rate, pyarrow_rate, ratio in selected_write_pairs:
            print(f"{comparison_name},{zig_rate:.2f},{pyarrow_rate:.2f},{ratio:.3f}")

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
        raise SystemExit("throughput below threshold:\n" + "\n".join(failures))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
