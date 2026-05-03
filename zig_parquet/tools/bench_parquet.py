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
ZIG_READER = ROOT / "zig-out" / "bin" / "parquet_validate_sequence"
ZIG_ID_READER = ROOT / "zig-out" / "bin" / "parquet_validate_ids"


def run(cmd: list[str]) -> float:
    start = time.perf_counter()
    subprocess.run(cmd, cwd=ROOT, check=True)
    return time.perf_counter() - start


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


def mib_per_s(path: pathlib.Path, seconds: float) -> float:
    return path.stat().st_size / (1024 * 1024) / seconds if seconds > 0 else float("inf")


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
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    TMP.mkdir(parents=True, exist_ok=True)
    if not args.skip_build:
        run([str(ZIG), "build", "-Doptimize=ReleaseFast"])

    rows = args.rows
    cases: list[tuple[str, pathlib.Path, float, tuple[int, ...] | None]] = []

    for codec in ("uncompressed", "zstd"):
        path = TMP / f"bench_zig_{codec}.parquet"
        seconds = run([str(ZIG_WRITER), str(path), str(rows), codec, "65536", "16384"])
        cases.append((f"zig-write-{codec}", path, seconds, None))
        read_seconds = run([str(ZIG_READER), str(path), str(rows)])
        cases.append((f"zig-read-zig-{codec}", path, read_seconds, None))
        id_read_seconds = run([str(ZIG_ID_READER), str(path), str(rows)])
        cases.append((f"zig-read-id-zig-{codec}", path, id_read_seconds, (0,)))

    path = TMP / "bench_zig_zstd_bss.parquet"
    seconds = run([str(ZIG_WRITER), str(path), str(rows), "zstd", "65536", "16384", "v1", "byte_stream_split"])
    cases.append(("zig-write-zstd-bss", path, seconds, None))
    read_seconds = run([str(ZIG_READER), str(path), str(rows)])
    cases.append(("zig-read-zig-zstd-bss", path, read_seconds, None))
    id_read_seconds = run([str(ZIG_ID_READER), str(path), str(rows)])
    cases.append(("zig-read-id-zig-zstd-bss", path, id_read_seconds, (0,)))

    for compression in ("NONE", "SNAPPY", "ZSTD"):
        path = TMP / f"bench_pyarrow_{compression.lower()}.parquet"
        seconds = write_pyarrow(path, rows, compression)
        cases.append((f"pyarrow-write-{compression.lower()}", path, seconds, None))
        read_seconds = run([str(ZIG_READER), str(path), str(rows)])
        cases.append((f"zig-read-pyarrow-{compression.lower()}", path, read_seconds, None))
        id_read_seconds = run([str(ZIG_ID_READER), str(path), str(rows)])
        cases.append((f"zig-read-id-pyarrow-{compression.lower()}", path, id_read_seconds, (0,)))

    print("case,file_mib,parquet_uncompressed_mib,seconds,file_mib_per_s,uncompressed_mib_per_s")
    for name, path, seconds, columns in cases:
        uncompressed_mib = parquet_uncompressed_mib(path, columns)
        uncompressed_rate = uncompressed_mib / seconds if seconds > 0 else float("inf")
        print(
            f"{name},{path.stat().st_size / (1024 * 1024):.2f},"
            f"{uncompressed_mib:.2f},{seconds:.4f},{mib_per_s(path, seconds):.2f},{uncompressed_rate:.2f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
