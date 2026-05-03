#!/usr/bin/env python3
import pathlib
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq

from fuzz_digest import digest_table, run_zig_digest


ROOT = pathlib.Path(__file__).resolve().parents[2]
TMP = ROOT / "zig_parquet" / "tmp" / "corpus_smoke"
ZIG_DIGEST = ROOT / "zig-out" / "bin" / "parquet_digest"
ZIG_WRITE_SEQUENCE = ROOT / "zig-out" / "bin" / "parquet_write_sequence"
ZIG_VALIDATE_SEQUENCE = ROOT / "zig-out" / "bin" / "parquet_validate_sequence"
ZIG_VALIDATE_IDS = ROOT / "zig-out" / "bin" / "parquet_validate_ids"
ZIG_VALIDATE_TRIPLETS = ROOT / "zig-out" / "bin" / "parquet_validate_triplets"
ZIG_VALIDATE_LIST = ROOT / "zig-out" / "bin" / "parquet_validate_list"
ZIG_VALIDATE_MAP = ROOT / "zig-out" / "bin" / "parquet_validate_map"
ZIG_VALIDATE_SCHEMA_PATHS = ROOT / "zig-out" / "bin" / "parquet_validate_schema_paths"
ZIG_VALIDATE_NESTED_LOGICAL = ROOT / "zig-out" / "bin" / "parquet_validate_nested_logical"
ZIG_VALIDATE_NESTED_MAP_PAIR = ROOT / "zig-out" / "bin" / "parquet_validate_nested_map_pair"


def run(argv: list[str | pathlib.Path]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(arg) for arg in argv],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def supported_case(
    name: str,
    table: pa.Table,
    *,
    compression: str,
    use_dictionary: bool | list[str],
    data_page_version: str,
    row_group_size: int,
    data_page_size: int,
) -> None:
    path = TMP / f"{name}.parquet"
    pq.write_table(
        table,
        path,
        compression=compression,
        use_dictionary=use_dictionary,
        data_page_version=data_page_version,
        row_group_size=row_group_size,
        data_page_size=data_page_size,
    )
    expected_table = pq.read_table(path)
    expected = digest_table(expected_table)
    actual_rows, actual = run_zig_digest(path)
    if actual_rows != expected_table.num_rows or actual != expected:
        raise AssertionError(
            f"digest mismatch {name}: rows={actual_rows} expected_rows={expected_table.num_rows} "
            f"actual={actual} expected={expected}"
        )


def zig_zero_row_case() -> None:
    path = TMP / "zig_zero_rows.parquet"
    run([ZIG_WRITE_SEQUENCE, path, "0", "uncompressed", "100", "10"])
    run([ZIG_VALIDATE_SEQUENCE, path, "0"])
    run([ZIG_VALIDATE_IDS, path, "0"])

    metadata = pq.ParquetFile(path).metadata
    if metadata.num_rows != 0 or metadata.num_row_groups != 0:
        raise AssertionError(
            f"zig zero-row metadata mismatch: rows={metadata.num_rows} row_groups={metadata.num_row_groups}"
        )
    table = pq.read_table(path)
    actual_rows, actual = run_zig_digest(path)
    if table.num_rows != 0 or actual_rows != 0 or not actual:
        raise AssertionError(
            f"zig zero-row read mismatch: table_rows={table.num_rows} zig_rows={actual_rows} digest={actual}"
        )


def int96_row_count_case() -> None:
    path = TMP / "int96_timestamp.parquet"
    table = pa.table({"ts": pa.array([1_700_000_000_000_000, None, 1_700_000_000_000_017], type=pa.timestamp("us"))})
    pq.write_table(
        table,
        path,
        compression="NONE",
        use_deprecated_int96_timestamps=True,
    )
    parquet_file = pq.ParquetFile(path)
    if parquet_file.schema.column(0).physical_type != "INT96":
        raise AssertionError("PyArrow did not emit INT96 for int96_timestamp corpus case")
    actual_rows, actual = run_zig_digest(path)
    if actual_rows != parquet_file.metadata.num_rows or not actual:
        raise AssertionError(f"INT96 row-count smoke mismatch: rows={actual_rows} digest={actual}")


def nested_struct_row_count_case() -> None:
    path = TMP / "nested_struct.parquet"
    table = pa.table(
        {
            "point": pa.array(
                [{"x": 1, "y": 2}, None, {"x": 3, "y": 4}],
                type=pa.struct([pa.field("x", pa.int32()), pa.field("y", pa.int32())]),
            )
        }
    )
    pq.write_table(table, path, compression="NONE")
    parquet_file = pq.ParquetFile(path)
    actual_rows, actual = run_zig_digest(path)
    if actual_rows != parquet_file.metadata.num_rows or not actual:
        raise AssertionError(f"nested struct row-count smoke mismatch: rows={actual_rows} digest={actual}")


def selected_flat_from_nested_case() -> None:
    path = TMP / "selected_flat_from_nested.parquet"
    table = pa.table(
        {
            "id": pa.array(range(12), type=pa.int64()),
            "items": pa.array([[1, 2], None, [], [3], [4, 5, 6], [], [7], [8, 9], None, [10], [], [11]], type=pa.list_(pa.int32())),
        }
    )
    pq.write_table(
        table,
        path,
        compression="SNAPPY",
        data_page_version="2.0",
        row_group_size=5,
        data_page_size=128,
    )
    run([ZIG_VALIDATE_IDS, path, str(table.num_rows)])
    run([ZIG_VALIDATE_SCHEMA_PATHS, path, "id", "items.list.element"])
    run([ZIG_VALIDATE_TRIPLETS, path, "items.list.element", str(table.num_rows), "16", "11", str(table.num_rows), "66"])
    run([ZIG_VALIDATE_LIST, path, "items.list.element", str(table.num_rows), "11", "11", "2", "0", "66"])
    run([ZIG_VALIDATE_NESTED_LOGICAL, path, "items.list.element", str(table.num_rows), "11", "2", "11", "11", "0", "66"])


def standard_map_case() -> None:
    path = TMP / "standard_map.parquet"
    table = pa.table(
        {
            "attrs": pa.array(
                [None, [], [("aa", 1), ("b", None)], [("ccc", 3)]],
                type=pa.map_(pa.string(), pa.int32()),
            )
        }
    )
    pq.write_table(
        table,
        path,
        compression="SNAPPY",
        data_page_version="2.0",
        row_group_size=2,
        data_page_size=128,
    )
    run([ZIG_VALIDATE_MAP, path, "attrs.key_value.key", "attrs.key_value.value", str(table.num_rows), "3", "3", "2", "1", "1", "6", "4"])
    run([ZIG_VALIDATE_NESTED_LOGICAL, path, "attrs.key_value.key", str(table.num_rows), "3", "1", "3", "3", "0", "6"])
    run([ZIG_VALIDATE_NESTED_LOGICAL, path, "attrs.key_value.value", str(table.num_rows), "3", "1", "3", "2", "1", "4"])
    run([ZIG_VALIDATE_NESTED_MAP_PAIR, path, "attrs.key_value.key", "attrs.key_value.value", str(table.num_rows), "4", "3", "3", "3", "2", "1", "1", "6", "4"])


def expect_clean_failure(name: str, table: pa.Table, *, expected_errors: tuple[str, ...], **write_kwargs) -> bool:
    path = TMP / f"{name}.parquet"
    try:
        pq.write_table(table, path, **write_kwargs)
    except (pa.ArrowException, OSError):
        return False

    proc = subprocess.run(
        [str(ZIG_DIGEST), str(path)],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    combined = proc.stdout + proc.stderr
    if proc.returncode == 0:
        raise AssertionError(f"{name} unexpectedly succeeded")
    if proc.returncode < 0:
        raise AssertionError(f"{name} terminated by signal {proc.returncode}: {combined!r}")
    lowered = combined.lower()
    if "panic" in lowered or "segmentation fault" in lowered or "core dumped" in lowered:
        raise AssertionError(f"{name} did not fail cleanly: {combined!r}")
    if not any(token in combined for token in expected_errors):
        raise AssertionError(f"{name} failed with unexpected error: {combined!r}")
    return True


def required_table(rows: int) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("flag", pa.bool_(), nullable=False),
            pa.field("i32", pa.int32(), nullable=False),
            pa.field("i64", pa.int64(), nullable=False),
            pa.field("f32", pa.float32(), nullable=False),
            pa.field("f64", pa.float64(), nullable=False),
            pa.field("fixed", pa.binary(4), nullable=False),
        ]
    )
    arrays = [
        pa.array((i % 3 == 0 for i in range(rows)), type=pa.bool_()),
        pa.array(((i * 17) - 123 for i in range(rows)), type=pa.int32()),
        pa.array((i * 1_000_003 for i in range(rows)), type=pa.int64()),
        pa.array((float(i) * 0.5 - 7.25 for i in range(rows)), type=pa.float32()),
        pa.array((float(i) * -0.25 + 99.5 for i in range(rows)), type=pa.float64()),
        pa.array((bytes([(i + j * 19) & 0xFF for j in range(4)]) for i in range(rows)), type=pa.binary(4)),
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def optional_sparse_table(rows: int) -> pa.Table:
    return pa.table(
        {
            "flag": pa.array((None if i % 5 == 0 else i % 2 == 0 for i in range(rows)), type=pa.bool_()),
            "name": pa.array((None if i % 3 == 0 else f"word-{i & 7}" for i in range(rows)), type=pa.string()),
            "payload": pa.array((None if i % 4 == 0 else bytes([(i * 7 + j) & 0xFF for j in range(i % 33)]) for i in range(rows)), type=pa.binary()),
            "fixed": pa.array((None if i % 6 == 0 else bytes([(i + j) & 0xFF for j in range(4)]) for i in range(rows)), type=pa.binary(4)),
        }
    )


def all_null_table(rows: int) -> pa.Table:
    return pa.table(
        {
            "name": pa.array([None] * rows, type=pa.string()),
            "payload": pa.array([None] * rows, type=pa.binary()),
            "fixed": pa.array([None] * rows, type=pa.binary(4)),
        }
    )


def zero_row_table() -> pa.Table:
    schema = pa.schema(
        [
            pa.field("i64", pa.int64()),
            pa.field("f64", pa.float64()),
            pa.field("name", pa.string()),
            pa.field("payload", pa.binary()),
        ]
    )
    return pa.Table.from_arrays(
        [
            pa.array([], type=pa.int64()),
            pa.array([], type=pa.float64()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.binary()),
        ],
        schema=schema,
    )


def main() -> int:
    for binary in (
        ZIG_DIGEST,
        ZIG_WRITE_SEQUENCE,
        ZIG_VALIDATE_SEQUENCE,
        ZIG_VALIDATE_IDS,
        ZIG_VALIDATE_TRIPLETS,
        ZIG_VALIDATE_LIST,
        ZIG_VALIDATE_MAP,
        ZIG_VALIDATE_SCHEMA_PATHS,
    ):
        if not binary.exists():
            raise FileNotFoundError(f"{binary} does not exist; run zig build -Doptimize=ReleaseFast first")
    TMP.mkdir(parents=True, exist_ok=True)

    supported_case(
        "required_mixed_zstd_v1",
        required_table(257),
        compression="ZSTD",
        use_dictionary=False,
        data_page_version="1.0",
        row_group_size=64,
        data_page_size=256,
    )
    supported_case(
        "required_mixed_lz4_v2",
        required_table(129),
        compression="LZ4",
        use_dictionary=False,
        data_page_version="2.0",
        row_group_size=31,
        data_page_size=128,
    )
    supported_case(
        "optional_sparse_snappy_v2",
        optional_sparse_table(513),
        compression="SNAPPY",
        use_dictionary=True,
        data_page_version="2.0",
        row_group_size=17,
        data_page_size=256,
    )
    supported_case(
        "all_null_dictionary_zstd_v1",
        all_null_table(64),
        compression="ZSTD",
        use_dictionary=True,
        data_page_version="1.0",
        row_group_size=1,
        data_page_size=128,
    )
    supported_case(
        "zero_rows_uncompressed",
        zero_row_table(),
        compression="NONE",
        use_dictionary=False,
        data_page_version="1.0",
        row_group_size=8,
        data_page_size=128,
    )
    zig_zero_row_case()
    int96_row_count_case()
    nested_struct_row_count_case()
    selected_flat_from_nested_case()
    standard_map_case()

    unsupported = 0
    unsupported += int(
        expect_clean_failure(
            "nested_list",
            pa.table({"items": pa.array([[1, 2], None, [], [3]], type=pa.list_(pa.int64()))}),
            expected_errors=("UnsupportedNestedSchema",),
            compression="NONE",
        )
    )
    unsupported += int(
        expect_clean_failure(
            "brotli_codec",
            pa.table({"i64": pa.array(range(32), type=pa.int64())}),
            expected_errors=("UnsupportedCompression",),
            compression="BROTLI",
        )
    )

    print(f"corpus-smoke-ok supported=9 map_checked=1 nested_logical_checked=3 nested_map_pair_checked=1 unsupported={unsupported}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
