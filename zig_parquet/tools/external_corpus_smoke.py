#!/usr/bin/env python3
import decimal
import hashlib
import pathlib
import struct
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = pathlib.Path(__file__).resolve().parents[2]
CORPUS = ROOT / "deps" / "parquet-testing" / "data"
ZIG_DIGEST = ROOT / "zig-out" / "bin" / "parquet_digest"
ZIG_TRIPLETS = ROOT / "zig-out" / "bin" / "parquet_validate_triplets"
ZIG_LIST = ROOT / "zig-out" / "bin" / "parquet_validate_list"
ZIG_MAP = ROOT / "zig-out" / "bin" / "parquet_validate_map"
ZIG_SCHEMA_PATHS = ROOT / "zig-out" / "bin" / "parquet_validate_schema_paths"
ZIG_NESTED_LOGICAL = ROOT / "zig-out" / "bin" / "parquet_validate_nested_logical"
ZIG_NESTED_MAP_PAIR = ROOT / "zig-out" / "bin" / "parquet_validate_nested_map_pair"

PHYSICAL = {
    "BOOLEAN": 0,
    "INT32": 1,
    "INT64": 2,
    "INT96": 3,
    "FLOAT": 4,
    "DOUBLE": 5,
    "BYTE_ARRAY": 6,
    "FIXED_LEN_BYTE_ARRAY": 7,
}

LOGICAL_NONE = 0
LOGICAL_STRING = 1
LOGICAL_DECIMAL = 2
LOGICAL_DATE = 3
LOGICAL_TIMESTAMP_MILLIS = 4
LOGICAL_TIMESTAMP_MICROS = 5
LOGICAL_TIMESTAMP_NANOS = 6


class UnsupportedDigest(Exception):
    pass


def pack_u8(value: int) -> bytes:
    return bytes([value])


def pack_i32(value: int) -> bytes:
    return struct.pack("<i", value)


def pack_u32(value: int) -> bytes:
    return struct.pack("<I", value)


def pack_i64(value: int) -> bytes:
    return struct.pack("<q", value)


def pack_u64(value: int) -> bytes:
    return struct.pack("<Q", value)


def update_bytes(h: "hashlib._Hash", data: bytes) -> None:
    h.update(pack_u64(len(data)))
    h.update(data)


def run_zig_digest(path: pathlib.Path) -> tuple[int, str]:
    result = subprocess.run(
        [str(ZIG_DIGEST), str(path)],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    rows = -1
    digest = ""
    for line in result.stdout.splitlines():
        key, value = line.split("=", 1)
        if key == "rows":
            rows = int(value)
        elif key == "digest":
            digest = value
    if rows < 0 or not digest:
        raise AssertionError(f"bad digest output for {path}: {result.stdout!r}")
    return rows, digest


def clean_failure(path: pathlib.Path) -> str:
    result = subprocess.run(
        [str(ZIG_DIGEST), str(path)],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode == 0:
        raise AssertionError(f"{path} unexpectedly succeeded")
    if result.returncode < 0:
        raise AssertionError(f"{path} terminated by signal {result.returncode}")
    combined = result.stdout + result.stderr
    lowered = combined.lower()
    for token in ("panic", "segmentation fault", "core dumped", "illegal instruction"):
        if token in lowered:
            raise AssertionError(f"{path} did not fail cleanly: {combined!r}")
    return combined


def run_zig_triplets(
    path: pathlib.Path,
    column_index: int | str,
    rows: int,
    levels: int,
    values: int,
    rep_zeroes: int,
    value_sum: int,
    repeated_level_counts: str | None = None,
) -> None:
    cmd = [
        str(ZIG_TRIPLETS),
        str(path),
        str(column_index),
        str(rows),
        str(levels),
        str(values),
        str(rep_zeroes),
        str(value_sum),
    ]
    if repeated_level_counts is not None:
        cmd.append(repeated_level_counts)
    subprocess.run(
        cmd,
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def run_zig_list(
    path: pathlib.Path,
    column_index: int,
    rows: int,
    elements: int,
    values: int,
    null_lists: int,
    null_elements: int,
    payload_sum: int,
) -> None:
    subprocess.run(
        [
            str(ZIG_LIST),
            str(path),
            str(column_index),
            str(rows),
            str(elements),
            str(values),
            str(null_lists),
            str(null_elements),
            str(payload_sum),
        ],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def run_zig_map(
    path: pathlib.Path,
    key_column_index: int,
    value_column_index: int | None,
    rows: int,
    entries: int,
    key_values: int,
    value_values: int,
    null_maps: int,
    null_values: int,
    key_payload_sum: int,
    value_payload_sum: int,
) -> None:
    subprocess.run(
        [
            str(ZIG_MAP),
            str(path),
            str(key_column_index),
            "none" if value_column_index is None else str(value_column_index),
            str(rows),
            str(entries),
            str(key_values),
            str(value_values),
            str(null_maps),
            str(null_values),
            str(key_payload_sum),
            str(value_payload_sum),
        ],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def run_zig_schema_paths(path: pathlib.Path, expected_paths: tuple[str, ...]) -> None:
    subprocess.run(
        [str(ZIG_SCHEMA_PATHS), str(path), *expected_paths],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def run_zig_nested_logical(
    path: pathlib.Path,
    column_index: int | str,
    rows: int,
    level_totals: str,
    level_nulls: str,
    leaf_slots: int,
    values: int,
    leaf_nulls: int,
    payload_sum: int,
) -> None:
    subprocess.run(
        [
            str(ZIG_NESTED_LOGICAL),
            str(path),
            str(column_index),
            str(rows),
            level_totals,
            level_nulls,
            str(leaf_slots),
            str(values),
            str(leaf_nulls),
            str(payload_sum),
        ],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def run_zig_nested_map_pair(
    path: pathlib.Path,
    key_column_index: int | str,
    value_column_index: int | str | None,
    rows: int,
    parents: int,
    entries: int,
    key_values: int,
    value_slots: int,
    value_values: int,
    null_maps: int,
    null_values: int,
    key_payload_sum: int,
    value_payload_sum: int,
) -> None:
    subprocess.run(
        [
            str(ZIG_NESTED_MAP_PAIR),
            str(path),
            str(key_column_index),
            "none" if value_column_index is None else str(value_column_index),
            str(rows),
            str(parents),
            str(entries),
            str(key_values),
            str(value_slots),
            str(value_values),
            str(null_maps),
            str(null_values),
            str(key_payload_sum),
            str(value_payload_sum),
        ],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def logical_id(column: pq.ColumnSchema) -> int:
    logical = str(column.logical_type)
    if logical == "None" or logical == "Undefined":
        return LOGICAL_NONE
    if logical == "String":
        return LOGICAL_STRING
    if logical.startswith("Decimal("):
        return LOGICAL_DECIMAL
    if logical == "Date":
        return LOGICAL_DATE
    if logical.startswith("Timestamp("):
        if "timeUnit=milliseconds" in logical:
            return LOGICAL_TIMESTAMP_MILLIS
        if "timeUnit=microseconds" in logical:
            return LOGICAL_TIMESTAMP_MICROS
        if "timeUnit=nanoseconds" in logical:
            return LOGICAL_TIMESTAMP_NANOS
    raise UnsupportedDigest(f"unsupported logical type {logical!r} on {column.path}")


def column_digest_info(schema: pq.ParquetSchema) -> list[tuple[pq.ColumnSchema, int, int, int, int, int, int]]:
    infos = []
    for i in range(len(schema.names)):
        column = schema.column(i)
        physical = PHYSICAL[column.physical_type]
        if column.max_repetition_level != 0:
            raise UnsupportedDigest(f"nested column {column.path}")
        if column.max_definition_level > 1:
            raise UnsupportedDigest(f"nested optional column {column.path}")
        if physical == PHYSICAL["INT96"]:
            raise UnsupportedDigest(f"INT96 physical digest is row-count checked on {column.path}")
        repetition = 0 if column.max_definition_level == 0 else 1
        logical = logical_id(column)
        type_length = column.length if column.physical_type == "FIXED_LEN_BYTE_ARRAY" else -1
        precision = column.precision if logical == LOGICAL_DECIMAL and column.precision != -1 else -1
        scale = column.scale if logical == LOGICAL_DECIMAL and column.scale != -1 else -1
        infos.append((column, physical, logical, repetition, type_length, precision, scale))
    return infos


def unscaled_decimal(value: decimal.Decimal, scale: int) -> int:
    shifted = value.scaleb(scale)
    return int(shifted.to_integral_exact())


def minimal_signed_bytes(value: int) -> bytes:
    width = 1
    while True:
        try:
            encoded = value.to_bytes(width, "big", signed=True)
        except OverflowError:
            width += 1
            continue
        if int.from_bytes(encoded, "big", signed=True) == value:
            return encoded
        width += 1


def cell_bytes(column: pq.ColumnSchema, physical: int, logical: int, value) -> bytes:
    if physical == PHYSICAL["BOOLEAN"]:
        return pack_u8(1 if value else 0)
    if physical == PHYSICAL["INT32"]:
        if logical == LOGICAL_DECIMAL:
            value = unscaled_decimal(value, column.scale)
        return pack_i32(value)
    if physical == PHYSICAL["INT64"]:
        if logical == LOGICAL_DECIMAL:
            value = unscaled_decimal(value, column.scale)
        return pack_i64(value)
    if physical == PHYSICAL["FLOAT"]:
        return struct.pack("<f", value)
    if physical == PHYSICAL["DOUBLE"]:
        return struct.pack("<d", value)
    if physical == PHYSICAL["BYTE_ARRAY"]:
        if logical == LOGICAL_DECIMAL:
            return minimal_signed_bytes(unscaled_decimal(value, column.scale))
        if logical == LOGICAL_STRING:
            return value.encode()
        if isinstance(value, str):
            raise UnsupportedDigest(f"string value for non-string physical digest on {column.path}")
        return bytes(value)
    if physical == PHYSICAL["FIXED_LEN_BYTE_ARRAY"]:
        if logical == LOGICAL_DECIMAL:
            return unscaled_decimal(value, column.scale).to_bytes(column.length, "big", signed=True)
        if str(column.logical_type) != "None":
            raise UnsupportedDigest(f"unsupported fixed logical type {column.logical_type} on {column.path}")
        return bytes(value)
    raise UnsupportedDigest(f"unsupported physical type {column.physical_type} on {column.path}")


def pyarrow_digest(path: pathlib.Path) -> tuple[int, str]:
    parquet_file = pq.ParquetFile(path)
    infos = column_digest_info(parquet_file.schema)
    table = parquet_file.read()

    h = hashlib.sha256()
    h.update(b"zig-parquet-digest-v1\x00")
    h.update(pack_u64(table.num_rows))
    h.update(pack_u64(len(infos)))
    for column, physical, logical, repetition, type_length, precision, scale in infos:
        update_bytes(h, column.name.encode())
        h.update(pack_i32(physical))
        h.update(pack_u8(logical))
        h.update(pack_i32(repetition))
        h.update(pack_i32(type_length))
        h.update(pack_i32(precision))
        h.update(pack_i32(scale))

    columns = [table.column(i).to_pylist() for i in range(len(infos))]
    for row in range(table.num_rows):
        for values, (column, physical, logical, _repetition, _type_length, _precision, _scale) in zip(columns, infos):
            value = values[row]
            if value is None:
                h.update(pack_u8(0))
                continue
            h.update(pack_u8(1))
            payload = cell_bytes(column, physical, logical, value)
            if physical in (PHYSICAL["BYTE_ARRAY"], PHYSICAL["FIXED_LEN_BYTE_ARRAY"]):
                update_bytes(h, payload)
            else:
                h.update(payload)

    return table.num_rows, h.hexdigest()


def main() -> int:
    if not CORPUS.exists():
        raise FileNotFoundError(f"{CORPUS} does not exist; clone https://github.com/apache/parquet-testing to deps/")
    if not ZIG_DIGEST.exists():
        raise FileNotFoundError(f"{ZIG_DIGEST} does not exist; run zig build -Doptimize=ReleaseFast first")
    if not ZIG_TRIPLETS.exists():
        raise FileNotFoundError(f"{ZIG_TRIPLETS} does not exist; run zig build -Doptimize=ReleaseFast first")
    if not ZIG_LIST.exists():
        raise FileNotFoundError(f"{ZIG_LIST} does not exist; run zig build -Doptimize=ReleaseFast first")
    if not ZIG_MAP.exists():
        raise FileNotFoundError(f"{ZIG_MAP} does not exist; run zig build -Doptimize=ReleaseFast first")
    if not ZIG_SCHEMA_PATHS.exists():
        raise FileNotFoundError(f"{ZIG_SCHEMA_PATHS} does not exist; run zig build -Doptimize=ReleaseFast first")
    if not ZIG_NESTED_LOGICAL.exists():
        raise FileNotFoundError(f"{ZIG_NESTED_LOGICAL} does not exist; run zig build -Doptimize=ReleaseFast first")
    if not ZIG_NESTED_MAP_PAIR.exists():
        raise FileNotFoundError(f"{ZIG_NESTED_MAP_PAIR} does not exist; run zig build -Doptimize=ReleaseFast first")

    files = sorted(CORPUS.glob("*.parquet"))
    supported = 0
    digest_checked = 0
    row_checked = 0
    unsupported = 0
    corrupt = 0
    triplet_checked = 0
    list_checked = 0
    map_checked = 0
    schema_path_checked = 0
    nested_logical_checked = 0
    nested_map_pair_checked = 0
    by_error: dict[str, int] = {}

    triplet_cases = {
        "list_columns.parquet": (
            ("int64_list.list.item", 3, 6, 5, 3, 11, "7"),
        ),
        "nested_lists.snappy.parquet": (
            ("a.list.element.list.element.list.element", 3, 18, 15, 3, 15, "7,14,19"),
        ),
        "nested_maps.snappy.parquet": (
            ("a.key_value.value.key_value.key", 6, 9, 7, 6, 17, "7,10"),
            ("a.key_value.value.key_value.value", 6, 9, 7, 6, 5, "7,10"),
        ),
        "repeated_primitive_no_list.parquet": (
            ("Int32_list", 4, 10, 9, 4, 36, "11"),
        ),
    }
    list_cases = {
        "list_columns.parquet": (
            ("int64_list.list.item", 3, 6, 5, 0, 1, 11),
            ("utf8_list.list.item", 3, 7, 6, 1, 1, 18),
        ),
        "repeated_primitive_no_list.parquet": (
            ("Int32_list", 4, 9, 9, 0, 0, 36),
            ("String_list", 4, 10, 10, 0, 0, 39),
            ("group_of_lists.Int32_list_in_group", 4, 9, 9, 0, 0, 36),
            ("group_of_lists.String_list_in_group", 4, 10, 10, 0, 0, 39),
        ),
    }
    map_cases = {
        "map_no_value.parquet": (
            ("my_map.key_value.key", "my_map.key_value.value", 3, 9, 9, 0, 0, 9, 45, 0),
            ("my_map_no_v.key_value.key", None, 3, 9, 9, 0, 0, 0, 45, 0),
        ),
    }
    schema_path_cases = {
        "list_columns.parquet": (
            "int64_list.list.item",
            "utf8_list.list.item",
        ),
        "map_no_value.parquet": (
            "my_map.key_value.key",
            "my_map.key_value.value",
            "my_map_no_v.key_value.key",
            "my_list.list.element",
        ),
        "nested_maps.snappy.parquet": (
            "a.key_value.key",
            "a.key_value.value.key_value.key",
            "a.key_value.value.key_value.value",
            "b",
            "c",
        ),
    }
    nested_logical_cases = {
        "list_columns.parquet": (
            ("int64_list.list.item", 3, "6", "0", 6, 5, 1, 11),
            ("utf8_list.list.item", 3, "7", "1", 7, 6, 1, 18),
        ),
        "map_no_value.parquet": (
            ("my_map.key_value.value", 3, "9", "0", 9, 0, 9, 0),
            ("my_map_no_v.key_value.key", 3, "9", "0", 9, 9, 0, 45),
        ),
        "nested_lists.snappy.parquet": (
            ("a.list.element.list.element.list.element", 3, "6,13,15", "0,0,3", 15, 15, 0, 15),
        ),
        "nested_maps.snappy.parquet": (
            ("a.key_value.value.key_value.key", 6, "6,7", "0,1", 7, 7, 0, 17),
            ("a.key_value.value.key_value.value", 6, "6,7", "0,1", 7, 7, 0, 5),
        ),
    }
    nested_map_pair_cases = {
        "map_no_value.parquet": (
            ("my_map.key_value.key", "my_map.key_value.value", 3, 3, 9, 9, 9, 0, 0, 9, 45, 0),
            ("my_map_no_v.key_value.key", None, 3, 3, 9, 9, 0, 0, 0, 0, 45, 0),
        ),
        "nested_maps.snappy.parquet": (
            ("a.key_value.key", "a.key_value.value.key_value.key", 6, 6, 6, 6, 7, 7, 0, 0, 6, 17),
            ("a.key_value.value.key_value.key", "a.key_value.value.key_value.value", 6, 6, 7, 7, 7, 7, 1, 0, 17, 5),
        ),
    }

    for path in files:
        if path.name in schema_path_cases:
            run_zig_schema_paths(path, schema_path_cases[path.name])
            schema_path_checked += 1
        if path.name in triplet_cases:
            for case in triplet_cases[path.name]:
                run_zig_triplets(path, *case)
                triplet_checked += 1
        if path.name in list_cases:
            for case in list_cases[path.name]:
                run_zig_list(path, *case)
                list_checked += 1
        if path.name in map_cases:
            for case in map_cases[path.name]:
                run_zig_map(path, *case)
                map_checked += 1
        if path.name in nested_logical_cases:
            for case in nested_logical_cases[path.name]:
                run_zig_nested_logical(path, *case)
                nested_logical_checked += 1
        if path.name in nested_map_pair_cases:
            for case in nested_map_pair_cases[path.name]:
                run_zig_nested_map_pair(path, *case)
                nested_map_pair_checked += 1

        try:
            zig_rows, zig_digest = run_zig_digest(path)
        except subprocess.CalledProcessError:
            combined = clean_failure(path)
            if "Corrupt" in combined:
                corrupt += 1
            elif "Unsupported" in combined:
                unsupported += 1
            else:
                raise AssertionError(f"{path} failed with unexpected error: {combined!r}")
            token = combined.strip().split()[-1] if combined.strip() else "unknown"
            by_error[token] = by_error.get(token, 0) + 1
            continue

        supported += 1
        try:
            pyarrow_rows, pyarrow_digest_value = pyarrow_digest(path)
        except (UnsupportedDigest, pa.ArrowException, OSError):
            pyarrow_rows = pq.ParquetFile(path).metadata.num_rows
            if pyarrow_rows != zig_rows:
                raise AssertionError(f"{path} row mismatch: zig={zig_rows} pyarrow={pyarrow_rows}")
            row_checked += 1
            continue

        if pyarrow_rows != zig_rows or pyarrow_digest_value != zig_digest:
            raise AssertionError(
                f"{path} digest mismatch: zig_rows={zig_rows} pyarrow_rows={pyarrow_rows} "
                f"zig={zig_digest} pyarrow={pyarrow_digest_value}"
            )
        digest_checked += 1

    if supported < 47 or digest_checked < 36 or row_checked < 11 or triplet_checked < 5 or list_checked < 6 or map_checked < 2 or schema_path_checked < 3 or nested_logical_checked < 7 or nested_map_pair_checked < 4:
        raise AssertionError(
            f"external corpus coverage regressed: supported={supported} "
            f"digest_checked={digest_checked} row_checked={row_checked} "
            f"triplet_checked={triplet_checked} list_checked={list_checked} "
            f"map_checked={map_checked} schema_path_checked={schema_path_checked} "
            f"nested_logical_checked={nested_logical_checked} "
            f"nested_map_pair_checked={nested_map_pair_checked}"
        )

    errors = " ".join(f"{name}={count}" for name, count in sorted(by_error.items()))
    print(
        f"external-corpus-smoke-ok files={len(files)} supported={supported} "
        f"digest_checked={digest_checked} row_checked={row_checked} "
        f"triplet_checked={triplet_checked} list_checked={list_checked} "
        f"map_checked={map_checked} schema_path_checked={schema_path_checked} "
        f"nested_logical_checked={nested_logical_checked} "
        f"nested_map_pair_checked={nested_map_pair_checked} "
        f"unsupported={unsupported} corrupt={corrupt}"
    )
    if errors:
        print(f"external-corpus-errors {errors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
