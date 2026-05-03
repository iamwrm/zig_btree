#!/usr/bin/env python3
import pathlib
import subprocess
import sys
import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = pathlib.Path(__file__).resolve().parents[2]
TMP = ROOT / "zig_parquet" / "tmp"
ZIG_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_fixture"
ZIG_LIST_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_list_fixture"
ZIG_NESTED_LIST_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_nested_list_fixture"
ZIG_MAP_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_map_fixture"
ZIG_NESTED_MAP_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_nested_map_fixture"
ZIG_LIST_MAP_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_list_map_fixture"
ZIG_MIXED_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_mixed_fixture"
ZIG_REPEATED_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_repeated_fixture"
ZIG_SEQUENCE_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_sequence"
ZIG_READER = ROOT / "zig-out" / "bin" / "parquet_read_fixture"
ZIG_SEQUENCE = ROOT / "zig-out" / "bin" / "parquet_validate_sequence"
ZIG_IDS = ROOT / "zig-out" / "bin" / "parquet_validate_ids"
ZIG_NESTED_LOGICAL = ROOT / "zig-out" / "bin" / "parquet_validate_nested_logical"
ZIG_NESTED_MAP_PAIR = ROOT / "zig-out" / "bin" / "parquet_validate_nested_map_pair"
ZIG_ZSTD_FAST = ROOT / "zig-out" / "bin" / "parquet_verify_zstd_fast"
ZIG_BENCH_READER = ROOT / "zig-out" / "bin" / "parquet_bench_read"


EXPECTED = [
    {"id": 1, "score": 10.5, "name": "ann", "blob": b"aaaa", "amount": Decimal("123.45")},
    {"id": 2, "score": 20.25, "name": None, "blob": b"bbbb", "amount": Decimal("-6.78")},
    {"id": 3, "score": 30.75, "name": "cat", "blob": b"cccc", "amount": Decimal("0.00")},
    {"id": 4, "score": 40.0, "name": "dan", "blob": b"dddd", "amount": Decimal("9999.99")},
    {"id": 5, "score": 50.5, "name": "eve", "blob": b"eeee", "amount": Decimal("-1.00")},
]

LABELS = [
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliet",
    "kilo",
    "lima",
    "mike",
    "november",
    "oscar",
    "papa",
]


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def run_quiet(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True, stdout=subprocess.DEVNULL)


def validate_sequence_file(path: pathlib.Path, rows: int) -> None:
    table = pq.read_table(path)
    ids = table.column("id").to_pylist()
    if ids[0] != 0 or ids[-1] != rows - 1 or len(ids) != rows:
        raise AssertionError(f"PyArrow misread Zig sequence ids for {path.name}")
    names = table.column("name").to_pylist()
    for i in (0, 1, 7, 15, 16, rows - 1):
        expected_name = None if i % 7 == 0 else LABELS[i & (len(LABELS) - 1)]
        if names[i] != expected_name:
            raise AssertionError(f"PyArrow misread Zig dictionary string at row {i} in {path.name}: {names[i]!r}")
    run([str(ZIG_SEQUENCE), str(path), str(rows)])
    run([str(ZIG_IDS), str(path), str(rows)])


def assert_page_indexes(path: pathlib.Path) -> None:
    metadata = pq.ParquetFile(path).metadata
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        for col_idx in range(row_group.num_columns):
            column = row_group.column(col_idx)
            if not column.has_offset_index:
                raise AssertionError(f"Zig output missing OffsetIndex for {path.name} rg={rg_idx} column={column.path_in_schema}")
            if not column.has_column_index:
                raise AssertionError(f"Zig output missing ColumnIndex for {path.name} rg={rg_idx} column={column.path_in_schema}")


def verify_zstd_fast_path(path: pathlib.Path) -> None:
    run([str(ZIG_ZSTD_FAST), str(path)])


def main() -> int:
    TMP.mkdir(parents=True, exist_ok=True)
    zig_file = TMP / "zig_fixture.parquet"
    pyarrow_file = TMP / "pyarrow_fixture.parquet"
    pyarrow_large = TMP / "pyarrow_large.parquet"
    pyarrow_page_index = TMP / "pyarrow_page_index.parquet"
    pyarrow_byte_stream_split = TMP / "pyarrow_byte_stream_split.parquet"
    pyarrow_delta_binary = TMP / "pyarrow_delta_binary.parquet"
    pyarrow_delta_length = TMP / "pyarrow_delta_length.parquet"
    pyarrow_delta_byte_array = TMP / "pyarrow_delta_byte_array.parquet"
    pyarrow_dictionary = TMP / "pyarrow_dictionary.parquet"
    pyarrow_default = TMP / "pyarrow_default.parquet"
    pyarrow_gzip = TMP / "pyarrow_gzip.parquet"
    pyarrow_lz4 = TMP / "pyarrow_lz4.parquet"
    pyarrow_v2 = TMP / "pyarrow_v2.parquet"
    pyarrow_v2_gzip = TMP / "pyarrow_v2_gzip.parquet"
    pyarrow_v2_lz4 = TMP / "pyarrow_v2_lz4.parquet"
    pyarrow_v2_zstd = TMP / "pyarrow_v2_zstd.parquet"
    pyarrow_temporal = TMP / "pyarrow_temporal.parquet"
    zig_zstd = TMP / "zig_zstd.parquet"
    zig_snappy = TMP / "zig_snappy.parquet"
    zig_gzip = TMP / "zig_gzip.parquet"
    zig_lz4 = TMP / "zig_lz4.parquet"
    zig_list = TMP / "zig_list.parquet"
    zig_list_v2_zstd = TMP / "zig_list_v2_zstd.parquet"
    zig_nested_list = TMP / "zig_nested_list.parquet"
    zig_nested_list_v2_zstd = TMP / "zig_nested_list_v2_zstd.parquet"
    zig_map = TMP / "zig_map.parquet"
    zig_map_v2_zstd = TMP / "zig_map_v2_zstd.parquet"
    zig_nested_map = TMP / "zig_nested_map.parquet"
    zig_nested_map_v2_zstd = TMP / "zig_nested_map_v2_zstd.parquet"
    zig_list_map = TMP / "zig_list_map.parquet"
    zig_list_map_v2_zstd = TMP / "zig_list_map_v2_zstd.parquet"
    zig_mixed = TMP / "zig_mixed.parquet"
    zig_mixed_v2_zstd = TMP / "zig_mixed_v2_zstd.parquet"
    zig_repeated = TMP / "zig_repeated.parquet"
    zig_repeated_v2_zstd = TMP / "zig_repeated_v2_zstd.parquet"
    zig_sequence_uncompressed = TMP / "zig_sequence_uncompressed.parquet"
    zig_sequence_zstd = TMP / "zig_sequence_zstd.parquet"
    zig_sequence_zstd_bss = TMP / "zig_sequence_zstd_bss.parquet"
    zig_sequence_zstd_delta = TMP / "zig_sequence_zstd_delta.parquet"
    zig_sequence_zstd_delta_bss = TMP / "zig_sequence_zstd_delta_bss.parquet"
    zig_sequence_zstd_delta_len = TMP / "zig_sequence_zstd_delta_len.parquet"
    zig_sequence_zstd_delta_ba = TMP / "zig_sequence_zstd_delta_ba.parquet"
    zig_sequence_v2_zstd = TMP / "zig_sequence_v2_zstd.parquet"
    pyarrow_zstd = TMP / "pyarrow_zstd.parquet"

    run([str(ZIG_WRITER), str(zig_file)])
    zig_table = pq.read_table(zig_file)
    if zig_table.to_pylist() != EXPECTED:
        raise AssertionError(f"PyArrow misread Zig output: {zig_table.to_pylist()!r}")
    if zig_table.schema.field("amount").type != pa.decimal128(9, 2):
        raise AssertionError(f"PyArrow did not see Zig decimal metadata: {zig_table.schema}")
    zig_metadata = pq.ParquetFile(zig_file).metadata
    assert_page_indexes(zig_file)
    id_stats = zig_metadata.row_group(0).column(0).statistics
    if id_stats is None or not id_stats.has_min_max or id_stats.min != 1 or id_stats.max != 3 or id_stats.null_count != 0:
        raise AssertionError(f"PyArrow did not see Zig id statistics: {id_stats}")
    second_id_stats = zig_metadata.row_group(1).column(0).statistics
    if second_id_stats is None or not second_id_stats.has_min_max or second_id_stats.min != 4 or second_id_stats.max != 5:
        raise AssertionError(f"PyArrow did not see Zig second row-group id statistics: {second_id_stats}")
    name_stats = zig_metadata.row_group(0).column(2).statistics
    if (
        name_stats is None
        or not name_stats.has_min_max
        or name_stats.min != "ann"
        or name_stats.max != "cat"
        or name_stats.null_count != 1
    ):
        raise AssertionError(f"PyArrow did not see Zig name null statistics: {name_stats}")
    blob_stats = zig_metadata.row_group(0).column(3).statistics
    if blob_stats is None or not blob_stats.has_min_max or blob_stats.min != b"aaaa" or blob_stats.max != b"cccc":
        raise AssertionError(f"PyArrow did not see Zig fixed binary statistics: {blob_stats}")

    run([str(ZIG_WRITER), str(zig_zstd), "zstd"])
    verify_zstd_fast_path(zig_zstd)
    zig_zstd_table = pq.read_table(zig_zstd)
    if zig_zstd_table.to_pylist() != EXPECTED:
        raise AssertionError(f"PyArrow misread Zig zstd output: {zig_zstd_table.to_pylist()!r}")

    run([str(ZIG_WRITER), str(zig_snappy), "snappy"])
    zig_snappy_table = pq.read_table(zig_snappy)
    if zig_snappy_table.to_pylist() != EXPECTED:
        raise AssertionError(f"PyArrow misread Zig snappy output: {zig_snappy_table.to_pylist()!r}")

    run([str(ZIG_WRITER), str(zig_gzip), "gzip"])
    zig_gzip_table = pq.read_table(zig_gzip)
    if zig_gzip_table.to_pylist() != EXPECTED:
        raise AssertionError(f"PyArrow misread Zig gzip output: {zig_gzip_table.to_pylist()!r}")

    run([str(ZIG_WRITER), str(zig_lz4), "lz4_raw"])
    zig_lz4_table = pq.read_table(zig_lz4)
    if zig_lz4_table.to_pylist() != EXPECTED:
        raise AssertionError(f"PyArrow misread Zig lz4_raw output: {zig_lz4_table.to_pylist()!r}")

    expected_repeated = [{"items": [10, 11]}, {"items": []}, {"items": [20]}, {"items": []}]
    for path, args in (
        (zig_repeated, []),
        (zig_repeated_v2_zstd, ["zstd", "v2"]),
    ):
        run([str(ZIG_REPEATED_WRITER), str(path), *args])
        table = pq.read_table(path)
        if table.to_pylist() != expected_repeated:
            raise AssertionError(f"PyArrow misread Zig repeated primitive output {path.name}: {table.to_pylist()!r}")
        assert_page_indexes(path)
        metadata = pq.ParquetFile(path).metadata.row_group(0).column(0)
        if metadata.num_values != 5:
            raise AssertionError(f"PyArrow saw wrong repeated primitive level count for {path.name}: {metadata.num_values}")
        stats = metadata.statistics
        if stats is None or not stats.has_min_max or stats.min != 10 or stats.max != 20 or stats.null_count != 2:
            raise AssertionError(f"PyArrow saw wrong repeated primitive stats for {path.name}: {stats}")

    expected_list = [{"items": None}, {"items": []}, {"items": [10, None, 11]}, {"items": [20]}]
    for path, args in (
        (zig_list, []),
        (zig_list_v2_zstd, ["zstd", "v2"]),
    ):
        run([str(ZIG_LIST_WRITER), str(path), *args])
        table = pq.read_table(path)
        if table.to_pylist() != expected_list:
            raise AssertionError(f"PyArrow misread Zig LIST output {path.name}: {table.to_pylist()!r}")
        assert_page_indexes(path)
        metadata = pq.ParquetFile(path).metadata.row_group(0).column(0)
        if metadata.path_in_schema != "items.list.element" or metadata.num_values != 6:
            raise AssertionError(f"PyArrow saw wrong LIST column metadata for {path.name}: {metadata.path_in_schema} {metadata.num_values}")
        stats = metadata.statistics
        if stats is None or not stats.has_min_max or stats.min != 10 or stats.max != 20 or stats.null_count != 3:
            raise AssertionError(f"PyArrow saw wrong LIST stats for {path.name}: {stats}")

    expected_nested_list = [{"a": None}, {"a": []}, {"a": [None, [], [1, None, 2]]}, {"a": [[3]]}]
    for path, args in (
        (zig_nested_list, []),
        (zig_nested_list_v2_zstd, ["zstd", "v2"]),
    ):
        run([str(ZIG_NESTED_LIST_WRITER), str(path), *args])
        table = pq.read_table(path)
        if table.to_pylist() != expected_nested_list:
            raise AssertionError(f"PyArrow misread Zig nested LIST output {path.name}: {table.to_pylist()!r}")
        metadata = pq.ParquetFile(path).metadata.row_group(0).column(0)
        if not metadata.has_offset_index:
            raise AssertionError(f"Zig output missing nested LIST OffsetIndex for {path.name}")
        if metadata.path_in_schema != "a.list.element.list.element" or metadata.num_values != 8:
            raise AssertionError(f"PyArrow saw wrong nested LIST metadata for {path.name}: {metadata.path_in_schema} {metadata.num_values}")
        stats = metadata.statistics
        if stats is None or not stats.has_min_max or stats.min != 1 or stats.max != 3 or stats.null_count != 5:
            raise AssertionError(f"PyArrow saw wrong nested LIST stats for {path.name}: {stats}")
        run(
            [
                str(ZIG_NESTED_LOGICAL),
                str(path),
                "a.list.element.list.element",
                "4",
                "4,4",
                "1,1",
                "4",
                "3",
                "1",
                "6",
            ]
        )

    expected_map = [{"attrs": None}, {"attrs": []}, {"attrs": [(1, 10), (2, None)]}, {"attrs": [(3, 20)]}]
    for path, args in (
        (zig_map, []),
        (zig_map_v2_zstd, ["zstd", "v2"]),
    ):
        run([str(ZIG_MAP_WRITER), str(path), *args])
        table = pq.read_table(path)
        if table.to_pylist() != expected_map:
            raise AssertionError(f"PyArrow misread Zig MAP output {path.name}: {table.to_pylist()!r}")
        assert_page_indexes(path)
        metadata = pq.ParquetFile(path).metadata.row_group(0)
        key_column = metadata.column(0)
        value_column = metadata.column(1)
        if key_column.path_in_schema != "attrs.key_value.key" or value_column.path_in_schema != "attrs.key_value.value":
            raise AssertionError(f"PyArrow saw wrong MAP column paths for {path.name}: {key_column.path_in_schema} {value_column.path_in_schema}")
        if key_column.num_values != 5 or value_column.num_values != 5:
            raise AssertionError(f"PyArrow saw wrong MAP level counts for {path.name}: {key_column.num_values} {value_column.num_values}")
        key_stats = key_column.statistics
        value_stats = value_column.statistics
        if key_stats is None or not key_stats.has_min_max or key_stats.min != 1 or key_stats.max != 3 or key_stats.null_count != 2:
            raise AssertionError(f"PyArrow saw wrong MAP key stats for {path.name}: {key_stats}")
        if value_stats is None or not value_stats.has_min_max or value_stats.min != 10 or value_stats.max != 20 or value_stats.null_count != 3:
            raise AssertionError(f"PyArrow saw wrong MAP value stats for {path.name}: {value_stats}")

    expected_nested_map = [
        {"a": None},
        {"a": []},
        {"a": [("aa", None), ("b", []), ("ccc", [(1, True), (2, False)])]},
        {"a": [("d", [(3, True)])]},
    ]
    for path, args in (
        (zig_nested_map, []),
        (zig_nested_map_v2_zstd, ["zstd", "v2"]),
    ):
        run([str(ZIG_NESTED_MAP_WRITER), str(path), *args])
        table = pq.read_table(path)
        if table.to_pylist() != expected_nested_map:
            raise AssertionError(f"PyArrow misread Zig nested MAP output {path.name}: {table.to_pylist()!r}")
        metadata = pq.ParquetFile(path).metadata.row_group(0)
        expected_paths = [
            "a.key_value.key",
            "a.key_value.value.key_value.key",
            "a.key_value.value.key_value.value",
        ]
        paths = [metadata.column(i).path_in_schema for i in range(metadata.num_columns)]
        if paths != expected_paths:
            raise AssertionError(f"PyArrow saw wrong nested MAP paths for {path.name}: {paths!r}")
        counts = [metadata.column(i).num_values for i in range(metadata.num_columns)]
        if counts != [6, 7, 7]:
            raise AssertionError(f"PyArrow saw wrong nested MAP level counts for {path.name}: {counts!r}")
        run([str(ZIG_NESTED_MAP_PAIR), str(path), "a.key_value.key", "a.key_value.value.key_value.key", "4", "4", "4", "4", "3", "3", "1", "0", "7", "6"])
        run([str(ZIG_NESTED_MAP_PAIR), str(path), "a.key_value.value.key_value.key", "a.key_value.value.key_value.value", "4", "4", "3", "3", "3", "3", "1", "0", "6", "2"])

    expected_list_map = [
        {"a": None},
        {"a": []},
        {"a": [None, [], [("aa", 1), ("b", None)]]},
        {"a": [[("c", 3)]]},
    ]
    for path, args in (
        (zig_list_map, []),
        (zig_list_map_v2_zstd, ["zstd", "v2"]),
    ):
        run([str(ZIG_LIST_MAP_WRITER), str(path), *args])
        table = pq.read_table(path)
        if table.to_pylist() != expected_list_map:
            raise AssertionError(f"PyArrow misread Zig LIST<MAP> output {path.name}: {table.to_pylist()!r}")
        metadata = pq.ParquetFile(path).metadata.row_group(0)
        expected_paths = [
            "a.list.element.key_value.key",
            "a.list.element.key_value.value",
        ]
        paths = [metadata.column(i).path_in_schema for i in range(metadata.num_columns)]
        if paths != expected_paths:
            raise AssertionError(f"PyArrow saw wrong LIST<MAP> paths for {path.name}: {paths!r}")
        counts = [metadata.column(i).num_values for i in range(metadata.num_columns)]
        if counts != [7, 7]:
            raise AssertionError(f"PyArrow saw wrong LIST<MAP> level counts for {path.name}: {counts!r}")
        run([str(ZIG_NESTED_LOGICAL), str(path), "a.list.element.key_value.key", "4", "4,3", "1,1", "3", "3", "0", "4"])
        run([str(ZIG_NESTED_LOGICAL), str(path), "a.list.element.key_value.value", "4", "4,3", "1,1", "3", "2", "1", "4"])
        run([str(ZIG_NESTED_MAP_PAIR), str(path), "a.list.element.key_value.key", "a.list.element.key_value.value", "4", "4", "3", "3", "3", "2", "1", "1", "4", "4"])

    expected_mixed = [
        {"id": 100, "items": None, "attrs": None, "nested_attrs": None, "list_attrs": None},
        {"id": 101, "items": [], "attrs": [], "nested_attrs": [], "list_attrs": []},
        {
            "id": 102,
            "items": [10, None, 11],
            "attrs": [(1, 10), (2, None)],
            "nested_attrs": [("aa", None), ("b", []), ("ccc", [(1, True), (2, False)])],
            "list_attrs": [None, [], [("aa", 1), ("b", None)]],
        },
        {
            "id": 103,
            "items": [20],
            "attrs": [(3, 20)],
            "nested_attrs": [("d", [(3, True)])],
            "list_attrs": [[("c", 3)]],
        },
    ]
    for path, args in (
        (zig_mixed, []),
        (zig_mixed_v2_zstd, ["zstd", "v2"]),
    ):
        run([str(ZIG_MIXED_WRITER), str(path), *args])
        table = pq.read_table(path)
        if table.to_pylist() != expected_mixed:
            raise AssertionError(f"PyArrow misread Zig mixed output {path.name}: {table.to_pylist()!r}")
        assert_page_indexes(path)
        metadata = pq.ParquetFile(path).metadata.row_group(0)
        paths = [metadata.column(i).path_in_schema for i in range(metadata.num_columns)]
        expected_paths = [
            "id",
            "items.list.element",
            "attrs.key_value.key",
            "attrs.key_value.value",
            "nested_attrs.key_value.key",
            "nested_attrs.key_value.value.key_value.key",
            "nested_attrs.key_value.value.key_value.value",
            "list_attrs.list.element.key_value.key",
            "list_attrs.list.element.key_value.value",
        ]
        if paths != expected_paths:
            raise AssertionError(f"PyArrow saw wrong mixed paths for {path.name}: {paths!r}")
        counts = [metadata.column(i).num_values for i in range(metadata.num_columns)]
        if counts != [4, 6, 5, 5, 6, 7, 7, 7, 7]:
            raise AssertionError(f"PyArrow saw wrong mixed level counts for {path.name}: {counts!r}")
        id_stats = metadata.column(0).statistics
        list_stats = metadata.column(1).statistics
        key_stats = metadata.column(2).statistics
        value_stats = metadata.column(3).statistics
        if id_stats is None or not id_stats.has_min_max or id_stats.min != 100 or id_stats.max != 103 or id_stats.null_count != 0:
            raise AssertionError(f"PyArrow saw wrong mixed id stats for {path.name}: {id_stats}")
        if list_stats is None or not list_stats.has_min_max or list_stats.min != 10 or list_stats.max != 20 or list_stats.null_count != 3:
            raise AssertionError(f"PyArrow saw wrong mixed LIST stats for {path.name}: {list_stats}")
        if key_stats is None or not key_stats.has_min_max or key_stats.min != 1 or key_stats.max != 3 or key_stats.null_count != 2:
            raise AssertionError(f"PyArrow saw wrong mixed MAP key stats for {path.name}: {key_stats}")
        if value_stats is None or not value_stats.has_min_max or value_stats.min != 10 or value_stats.max != 20 or value_stats.null_count != 3:
            raise AssertionError(f"PyArrow saw wrong mixed MAP value stats for {path.name}: {value_stats}")
        run([str(ZIG_NESTED_MAP_PAIR), str(path), "nested_attrs.key_value.key", "nested_attrs.key_value.value.key_value.key", "4", "4", "4", "4", "3", "3", "1", "0", "7", "6"])
        run([str(ZIG_NESTED_MAP_PAIR), str(path), "nested_attrs.key_value.value.key_value.key", "nested_attrs.key_value.value.key_value.value", "4", "4", "3", "3", "3", "3", "1", "0", "6", "2"])
        run([str(ZIG_NESTED_LOGICAL), str(path), "list_attrs.list.element.key_value.key", "4", "4,3", "1,1", "3", "3", "0", "4"])
        run([str(ZIG_NESTED_LOGICAL), str(path), "list_attrs.list.element.key_value.value", "4", "4,3", "1,1", "3", "2", "1", "4"])
        run([str(ZIG_NESTED_MAP_PAIR), str(path), "list_attrs.list.element.key_value.key", "list_attrs.list.element.key_value.value", "4", "4", "3", "3", "3", "2", "1", "1", "4", "4"])

    run([str(ZIG_SEQUENCE_WRITER), str(zig_sequence_uncompressed), "20000", "uncompressed", "20000", "512"])
    run([str(ZIG_SEQUENCE_WRITER), str(zig_sequence_zstd), "20000", "zstd", "20000", "512"])
    run([str(ZIG_SEQUENCE_WRITER), str(zig_sequence_zstd_bss), "20000", "zstd", "20000", "512", "v1", "byte_stream_split"])
    run([str(ZIG_SEQUENCE_WRITER), str(zig_sequence_zstd_delta), "20000", "zstd", "20000", "512", "v1", "delta_binary_packed"])
    run([str(ZIG_SEQUENCE_WRITER), str(zig_sequence_zstd_delta_len), "20000", "zstd", "20000", "512", "v1", "delta_length_byte_array"])
    run([str(ZIG_SEQUENCE_WRITER), str(zig_sequence_zstd_delta_ba), "20000", "zstd", "20000", "512", "v1", "delta_byte_array"])
    run(
        [
            str(ZIG_SEQUENCE_WRITER),
            str(zig_sequence_zstd_delta_bss),
            "20000",
            "zstd",
            "20000",
            "512",
            "v1",
            "delta_binary_packed+byte_stream_split",
        ]
    )
    assert_page_indexes(zig_sequence_uncompressed)
    assert_page_indexes(zig_sequence_zstd)
    assert_page_indexes(zig_sequence_zstd_bss)
    assert_page_indexes(zig_sequence_zstd_delta)
    assert_page_indexes(zig_sequence_zstd_delta_bss)
    assert_page_indexes(zig_sequence_zstd_delta_len)
    assert_page_indexes(zig_sequence_zstd_delta_ba)
    if "BYTE_STREAM_SPLIT" not in pq.ParquetFile(zig_sequence_zstd_bss).metadata.row_group(0).column(1).encodings:
        raise AssertionError("Zig byte-stream-split sequence did not mark score pages as BYTE_STREAM_SPLIT")
    if "DELTA_BINARY_PACKED" not in pq.ParquetFile(zig_sequence_zstd_delta).metadata.row_group(0).column(0).encodings:
        raise AssertionError("Zig delta sequence did not mark id pages as DELTA_BINARY_PACKED")
    if "DELTA_LENGTH_BYTE_ARRAY" not in pq.ParquetFile(zig_sequence_zstd_delta_len).metadata.row_group(0).column(2).encodings:
        raise AssertionError("Zig delta-length sequence did not mark name pages as DELTA_LENGTH_BYTE_ARRAY")
    if "DELTA_BYTE_ARRAY" not in pq.ParquetFile(zig_sequence_zstd_delta_ba).metadata.row_group(0).column(2).encodings:
        raise AssertionError("Zig delta-byte-array sequence did not mark name pages as DELTA_BYTE_ARRAY")
    delta_bss_metadata = pq.ParquetFile(zig_sequence_zstd_delta_bss).metadata
    if "DELTA_BINARY_PACKED" not in delta_bss_metadata.row_group(0).column(0).encodings:
        raise AssertionError("Zig delta+bss sequence did not mark id pages as DELTA_BINARY_PACKED")
    if "BYTE_STREAM_SPLIT" not in delta_bss_metadata.row_group(0).column(1).encodings:
        raise AssertionError("Zig delta+bss sequence did not mark score pages as BYTE_STREAM_SPLIT")
    if zig_sequence_zstd.stat().st_size >= zig_sequence_uncompressed.stat().st_size:
        raise AssertionError(
            f"Zig zstd output did not shrink sequence fixture: {zig_sequence_zstd.stat().st_size} >= {zig_sequence_uncompressed.stat().st_size}"
        )
    if zig_sequence_zstd_bss.stat().st_size >= zig_sequence_uncompressed.stat().st_size:
        raise AssertionError(
            f"Zig byte-stream-split zstd output did not shrink sequence fixture: {zig_sequence_zstd_bss.stat().st_size} >= {zig_sequence_uncompressed.stat().st_size}"
        )
    if zig_sequence_zstd_delta.stat().st_size >= zig_sequence_uncompressed.stat().st_size:
        raise AssertionError(
            f"Zig delta zstd output did not shrink sequence fixture: {zig_sequence_zstd_delta.stat().st_size} >= {zig_sequence_uncompressed.stat().st_size}"
        )
    if zig_sequence_zstd_delta_bss.stat().st_size >= zig_sequence_uncompressed.stat().st_size:
        raise AssertionError(
            f"Zig delta+bss zstd output did not shrink sequence fixture: {zig_sequence_zstd_delta_bss.stat().st_size} >= {zig_sequence_uncompressed.stat().st_size}"
        )
    validate_sequence_file(zig_sequence_zstd, 20000)
    validate_sequence_file(zig_sequence_zstd_bss, 20000)
    validate_sequence_file(zig_sequence_zstd_delta, 20000)
    validate_sequence_file(zig_sequence_zstd_delta_len, 20000)
    validate_sequence_file(zig_sequence_zstd_delta_ba, 20000)
    validate_sequence_file(zig_sequence_zstd_delta_bss, 20000)
    for path in (
        zig_sequence_zstd,
        zig_sequence_zstd_bss,
        zig_sequence_zstd_delta,
        zig_sequence_zstd_delta_len,
        zig_sequence_zstd_delta_ba,
        zig_sequence_zstd_delta_bss,
    ):
        verify_zstd_fast_path(path)

    run([str(ZIG_SEQUENCE_WRITER), str(zig_sequence_v2_zstd), "20000", "zstd", "20000", "512", "v2"])
    verify_zstd_fast_path(zig_sequence_v2_zstd)
    assert_page_indexes(zig_sequence_v2_zstd)
    zig_sequence_v2_table = pq.read_table(zig_sequence_v2_zstd)
    if zig_sequence_v2_table.column("id").to_pylist()[19999] != 19999:
        raise AssertionError("PyArrow misread Zig data page v2 id output")
    if zig_sequence_v2_table.column("name").to_pylist()[16] != LABELS[0]:
        raise AssertionError("PyArrow misread Zig data page v2 dictionary string output")
    validate_sequence_file(zig_sequence_v2_zstd, 20000)

    for codec, page_version, row_group_rows, max_page_rows in (
        ("uncompressed", "v1", 3333, 257),
        ("uncompressed", "v2", 4096, 128),
        ("snappy", "v1", 4096, 128),
        ("snappy", "v2", 3333, 257),
        ("gzip", "v1", 4096, 128),
        ("gzip", "v2", 3333, 257),
        ("lz4_raw", "v1", 4096, 128),
        ("lz4_raw", "v2", 3333, 257),
        ("zstd", "v1", 4096, 128),
        ("zstd", "v2", 3333, 257),
    ):
        path = TMP / f"zig_sequence_{codec}_{page_version}_rg{row_group_rows}_pg{max_page_rows}.parquet"
        run(
            [
                str(ZIG_SEQUENCE_WRITER),
                str(path),
                "12345",
                codec,
                str(row_group_rows),
                str(max_page_rows),
                page_version,
            ]
        )
        validate_sequence_file(path, 12345)

    pyarrow_table = pa.table(
        {
            "id": pa.array([row["id"] for row in EXPECTED], type=pa.int64()),
            "score": pa.array([row["score"] for row in EXPECTED], type=pa.float64()),
            "name": pa.array([row["name"] for row in EXPECTED], type=pa.string()),
            "blob": pa.array([row["blob"] for row in EXPECTED], type=pa.binary(4)),
            "amount": pa.array([row["amount"] for row in EXPECTED], type=pa.decimal128(9, 2)),
        }
    )
    pq.write_table(
        pyarrow_table,
        pyarrow_file,
        compression="NONE",
        use_dictionary=False,
        data_page_version="1.0",
        row_group_size=3,
    )
    run([str(ZIG_READER), str(pyarrow_file)])

    pq.write_table(
        pyarrow_table,
        pyarrow_zstd,
        compression="ZSTD",
        use_dictionary=False,
        data_page_version="1.0",
        row_group_size=3,
    )
    verify_zstd_fast_path(pyarrow_zstd)
    run([str(ZIG_READER), str(pyarrow_zstd)])

    pq.write_table(
        pyarrow_table,
        pyarrow_gzip,
        compression="GZIP",
        use_dictionary=False,
        data_page_version="1.0",
        row_group_size=3,
    )
    run([str(ZIG_READER), str(pyarrow_gzip)])

    pq.write_table(
        pyarrow_table,
        pyarrow_lz4,
        compression="LZ4",
        use_dictionary=False,
        data_page_version="1.0",
        row_group_size=3,
    )
    run([str(ZIG_READER), str(pyarrow_lz4)])

    pq.write_table(
        pyarrow_table,
        pyarrow_v2,
        compression="NONE",
        use_dictionary=False,
        data_page_version="2.0",
        row_group_size=3,
    )
    run([str(ZIG_READER), str(pyarrow_v2)])

    pq.write_table(
        pyarrow_table,
        pyarrow_v2_zstd,
        compression="ZSTD",
        use_dictionary=False,
        data_page_version="2.0",
        row_group_size=3,
    )
    verify_zstd_fast_path(pyarrow_v2_zstd)
    run([str(ZIG_READER), str(pyarrow_v2_zstd)])

    pq.write_table(
        pyarrow_table,
        pyarrow_v2_gzip,
        compression="GZIP",
        use_dictionary=False,
        data_page_version="2.0",
        row_group_size=3,
    )
    run([str(ZIG_READER), str(pyarrow_v2_gzip)])

    pq.write_table(
        pyarrow_table,
        pyarrow_v2_lz4,
        compression="LZ4",
        use_dictionary=False,
        data_page_version="2.0",
        row_group_size=3,
    )
    run([str(ZIG_READER), str(pyarrow_v2_lz4)])

    temporal_rows = 1024
    temporal_table = pa.table(
        {
            "id": pa.array(range(temporal_rows), type=pa.int64()),
            "day": pa.array(
                (dt.date(2024, 1, 1) + dt.timedelta(days=i) for i in range(temporal_rows)),
                type=pa.date32(),
            ),
            "ts_us": pa.array(
                (dt.datetime(2024, 1, 1) + dt.timedelta(microseconds=i * 17) for i in range(temporal_rows)),
                type=pa.timestamp("us"),
            ),
        }
    )
    pq.write_table(
        temporal_table,
        pyarrow_temporal,
        compression="ZSTD",
        use_dictionary=False,
        data_page_version="2.0",
        row_group_size=temporal_rows,
    )
    run([str(ZIG_SEQUENCE), str(pyarrow_temporal), str(temporal_rows)])

    large_rows = 20_000
    large_table = pa.table(
        {
            "id": pa.array(range(large_rows), type=pa.int64()),
            "score": pa.array((float(i) * 0.25 for i in range(large_rows)), type=pa.float64()),
            "name": pa.array((None if i % 7 == 0 else f"name-{i}" for i in range(large_rows)), type=pa.string()),
        }
    )
    pq.write_table(
        large_table,
        pyarrow_large,
        compression="NONE",
        use_dictionary=False,
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    run([str(ZIG_SEQUENCE), str(pyarrow_large), str(large_rows)])

    pq.write_table(
        large_table,
        pyarrow_page_index,
        compression="NONE",
        use_dictionary=False,
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
        write_page_index=True,
    )
    assert_page_indexes(pyarrow_page_index)
    run([str(ZIG_SEQUENCE), str(pyarrow_page_index), str(large_rows)])
    run([str(ZIG_IDS), str(pyarrow_page_index), str(large_rows)])

    byte_stream_split_table = pa.table(
        {
            "id": pa.array(range(large_rows), type=pa.int64()),
            "score": pa.array((float(i) * 0.25 for i in range(large_rows)), type=pa.float64()),
        }
    )
    pq.write_table(
        byte_stream_split_table,
        pyarrow_byte_stream_split,
        compression="ZSTD",
        use_dictionary=False,
        use_byte_stream_split=True,
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    if "BYTE_STREAM_SPLIT" not in pq.ParquetFile(pyarrow_byte_stream_split).metadata.row_group(0).column(0).encodings:
        raise AssertionError("PyArrow byte-stream-split fixture did not use BYTE_STREAM_SPLIT")
    run([str(ZIG_SEQUENCE), str(pyarrow_byte_stream_split), str(large_rows)])
    run([str(ZIG_IDS), str(pyarrow_byte_stream_split), str(large_rows)])

    delta_table = pa.table({"id": pa.array(range(large_rows), type=pa.int64())})
    pq.write_table(
        delta_table,
        pyarrow_delta_binary,
        compression="ZSTD",
        use_dictionary=False,
        column_encoding={"id": "DELTA_BINARY_PACKED"},
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    if "DELTA_BINARY_PACKED" not in pq.ParquetFile(pyarrow_delta_binary).metadata.row_group(0).column(0).encodings:
        raise AssertionError("PyArrow delta fixture did not use DELTA_BINARY_PACKED")
    run([str(ZIG_SEQUENCE), str(pyarrow_delta_binary), str(large_rows)])
    run([str(ZIG_IDS), str(pyarrow_delta_binary), str(large_rows)])

    delta_string_table = pa.table(
        {
            "id": pa.array(range(large_rows), type=pa.int64()),
            "name": pa.array((None if i % 11 == 0 else f"prefix-{i // 3:06d}-suffix" for i in range(large_rows)), type=pa.string()),
        }
    )
    pq.write_table(
        delta_string_table,
        pyarrow_delta_length,
        compression="ZSTD",
        use_dictionary=False,
        column_encoding={"name": "DELTA_LENGTH_BYTE_ARRAY"},
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    if "DELTA_LENGTH_BYTE_ARRAY" not in pq.ParquetFile(pyarrow_delta_length).metadata.row_group(0).column(1).encodings:
        raise AssertionError("PyArrow delta-length fixture did not use DELTA_LENGTH_BYTE_ARRAY")
    run([str(ZIG_SEQUENCE), str(pyarrow_delta_length), str(large_rows)])
    run([str(ZIG_IDS), str(pyarrow_delta_length), str(large_rows)])

    pq.write_table(
        delta_string_table,
        pyarrow_delta_byte_array,
        compression="ZSTD",
        use_dictionary=False,
        column_encoding={"name": "DELTA_BYTE_ARRAY"},
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    if "DELTA_BYTE_ARRAY" not in pq.ParquetFile(pyarrow_delta_byte_array).metadata.row_group(0).column(1).encodings:
        raise AssertionError("PyArrow delta-byte-array fixture did not use DELTA_BYTE_ARRAY")
    run([str(ZIG_SEQUENCE), str(pyarrow_delta_byte_array), str(large_rows)])
    run([str(ZIG_IDS), str(pyarrow_delta_byte_array), str(large_rows)])

    pq.write_table(
        large_table,
        pyarrow_dictionary,
        compression="NONE",
        use_dictionary=True,
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    run([str(ZIG_SEQUENCE), str(pyarrow_dictionary), str(large_rows)])

    pq.write_table(
        large_table,
        pyarrow_default,
        compression="SNAPPY",
        use_dictionary=True,
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    run([str(ZIG_SEQUENCE), str(pyarrow_default), str(large_rows)])

    pyarrow_large_zstd = TMP / "pyarrow_large_zstd.parquet"
    pyarrow_large_gzip = TMP / "pyarrow_large_gzip.parquet"
    pyarrow_large_lz4 = TMP / "pyarrow_large_lz4.parquet"
    pyarrow_multi_rg_zstd_dictionary = TMP / "pyarrow_multi_rg_zstd_dictionary.parquet"
    pq.write_table(
        large_table,
        pyarrow_large_zstd,
        compression="ZSTD",
        use_dictionary=True,
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    verify_zstd_fast_path(pyarrow_large_zstd)
    run([str(ZIG_SEQUENCE), str(pyarrow_large_zstd), str(large_rows)])
    pq.write_table(
        large_table,
        pyarrow_multi_rg_zstd_dictionary,
        compression="ZSTD",
        use_dictionary=True,
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=4096,
    )
    if pq.ParquetFile(pyarrow_multi_rg_zstd_dictionary).metadata.num_row_groups < 2:
        raise AssertionError("PyArrow multi-row-group dictionary fixture did not split row groups")
    verify_zstd_fast_path(pyarrow_multi_rg_zstd_dictionary)
    run([str(ZIG_SEQUENCE), str(pyarrow_multi_rg_zstd_dictionary), str(large_rows)])
    run([str(ZIG_IDS), str(pyarrow_multi_rg_zstd_dictionary), str(large_rows)])
    run_quiet([str(ZIG_BENCH_READER), str(pyarrow_multi_rg_zstd_dictionary), str(large_rows), "score", "3", "no-cache"])
    pq.write_table(
        large_table,
        pyarrow_large_gzip,
        compression="GZIP",
        use_dictionary=True,
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    run([str(ZIG_SEQUENCE), str(pyarrow_large_gzip), str(large_rows)])
    run_quiet([str(ZIG_BENCH_READER), str(pyarrow_large_gzip), str(large_rows), "score", "3", "no-cache"])
    pq.write_table(
        large_table,
        pyarrow_large_lz4,
        compression="LZ4",
        use_dictionary=True,
        data_page_version="1.0",
        data_page_size=512,
        row_group_size=large_rows,
    )
    run([str(ZIG_SEQUENCE), str(pyarrow_large_lz4), str(large_rows)])
    run_quiet([str(ZIG_BENCH_READER), str(pyarrow_large_lz4), str(large_rows), "score", "3", "no-cache"])
    print("pyarrow-compat-ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
