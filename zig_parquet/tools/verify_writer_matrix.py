#!/usr/bin/env python3
import pathlib
import sys
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq

from fuzz_digest import digest_table, run_zig_digest


ROOT = pathlib.Path(__file__).resolve().parents[2]
TMP = ROOT / "zig_parquet" / "tmp" / "writer_matrix"
ZIG_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_matrix"

LABELS = [
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
]


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def expected_table(rows: int) -> pa.Table:
    flag = []
    i32 = []
    i64 = []
    f32 = []
    f64 = []
    name = []
    payload = []
    fixed = []
    for row in range(rows):
        flag.append(((row + (row % 3)) % 3) == 0)
        i32.append(None if row % 5 == 0 else (row % 97) - 48)
        i64.append(row * 17 - 12345)
        f32.append((row % 101) * 0.5 - 17.25)
        f64.append(None if row % 7 == 0 else row * 0.125 - 99.5)
        name.append(None if row % 11 == 0 else f"prefix-{row // 3:06d}-{LABELS[row & (len(LABELS) - 1)]}")
        payload.append(bytes(((row + j * 13 + 7) & 0xFF) for j in range(row % 12)))
        fixed.append(bytes((((row >> (j * 2)) + j * 41) & 0xFF) for j in range(4)))

    schema = pa.schema(
        [
            pa.field("flag", pa.bool_(), nullable=False),
            pa.field("i32", pa.int32(), nullable=True),
            pa.field("i64", pa.int64(), nullable=False),
            pa.field("f32", pa.float32(), nullable=False),
            pa.field("f64", pa.float64(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("payload", pa.binary(), nullable=False),
            pa.field("fixed", pa.binary(4), nullable=False),
        ]
    )
    arrays = [
        pa.array(flag, type=pa.bool_()),
        pa.array(i32, type=pa.int32()),
        pa.array(i64, type=pa.int64()),
        pa.array(f32, type=pa.float32()),
        pa.array(f64, type=pa.float64()),
        pa.array(name, type=pa.string()),
        pa.array(payload, type=pa.binary()),
        pa.array(fixed, type=pa.binary(4)),
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def assert_encodings(path: pathlib.Path, encoding: str) -> None:
    metadata = pq.ParquetFile(path).metadata
    if metadata.num_row_groups == 0:
        return
    row_group = metadata.row_group(0)
    if encoding == "byte_stream_split":
        for idx in (3, 4):
            if "BYTE_STREAM_SPLIT" not in row_group.column(idx).encodings:
                raise AssertionError(f"{path.name} column {idx} missing BYTE_STREAM_SPLIT")
    elif encoding == "delta_binary_packed":
        for idx in (1, 2):
            if "DELTA_BINARY_PACKED" not in row_group.column(idx).encodings:
                raise AssertionError(f"{path.name} column {idx} missing DELTA_BINARY_PACKED")
    elif encoding == "delta_binary_packed+byte_stream_split":
        for idx in (1, 2):
            if "DELTA_BINARY_PACKED" not in row_group.column(idx).encodings:
                raise AssertionError(f"{path.name} column {idx} missing DELTA_BINARY_PACKED")
        for idx in (3, 4):
            if "BYTE_STREAM_SPLIT" not in row_group.column(idx).encodings:
                raise AssertionError(f"{path.name} column {idx} missing BYTE_STREAM_SPLIT")
    elif encoding == "delta_length_byte_array":
        for idx in (5, 6):
            if "DELTA_LENGTH_BYTE_ARRAY" not in row_group.column(idx).encodings:
                raise AssertionError(f"{path.name} column {idx} missing DELTA_LENGTH_BYTE_ARRAY")
    elif encoding == "delta_byte_array":
        for idx in (5, 6, 7):
            if "DELTA_BYTE_ARRAY" not in row_group.column(idx).encodings:
                raise AssertionError(f"{path.name} column {idx} missing DELTA_BYTE_ARRAY")


def main() -> int:
    if not ZIG_WRITER.exists():
        raise FileNotFoundError(f"{ZIG_WRITER} does not exist; run zig build -Doptimize=ReleaseFast first")
    TMP.mkdir(parents=True, exist_ok=True)

    codecs = ["uncompressed", "snappy", "gzip", "zstd"]
    page_versions = ["v1", "v2"]
    encodings = [
        "plain",
        "byte_stream_split",
        "delta_binary_packed",
        "delta_binary_packed+byte_stream_split",
        "delta_length_byte_array",
        "delta_byte_array",
    ]
    dictionary_modes = ["dict", "nodict"]
    checksum_modes = ["nocrc", "crc"]
    row_counts = [0, 1, 17, 1025]
    cases = 0

    for rows in row_counts:
        expected = digest_table(expected_table(rows))
        for codec in codecs:
            for page_version in page_versions:
                for encoding in encodings:
                    for dictionary_mode in dictionary_modes:
                        for checksum_mode in checksum_modes:
                            path = TMP / (
                                f"matrix_rows{rows}_{codec}_{page_version}_{encoding.replace('+', '_')}_"
                                f"{dictionary_mode}_{checksum_mode}.parquet"
                            )
                            run(
                                [
                                    str(ZIG_WRITER),
                                    str(path),
                                    str(rows),
                                    codec,
                                    page_version,
                                    encoding,
                                    "257",
                                    "64",
                                    dictionary_mode,
                                    checksum_mode,
                                ]
                            )

                            table = pq.read_table(path)
                            pyarrow_digest = digest_table(table)
                            if pyarrow_digest != expected:
                                raise AssertionError(f"PyArrow digest mismatch for {path.name}: {pyarrow_digest} != {expected}")

                            zig_rows, zig_digest = run_zig_digest(path)
                            if zig_rows != rows or zig_digest != expected:
                                raise AssertionError(f"Zig digest mismatch for {path.name}: rows={zig_rows} digest={zig_digest} expected={expected}")
                            assert_encodings(path, encoding)
                            cases += 1

    print(f"writer-matrix-ok cases={cases}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
