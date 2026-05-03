#!/usr/bin/env python3
import argparse
import hashlib
import math
import pathlib
import random
import struct
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = pathlib.Path(__file__).resolve().parents[2]
TMP = ROOT / "zig_parquet" / "tmp" / "fuzz_digest"
ZIG_DIGEST = ROOT / "zig-out" / "bin" / "parquet_digest"


TYPE_INFO = {
    "flag": ("bool", pa.bool_(), 0, 0, -1),
    "i32": ("int32", pa.int32(), 1, 0, -1),
    "i64": ("int64", pa.int64(), 2, 0, -1),
    "f32": ("float32", pa.float32(), 4, 0, -1),
    "f64": ("float64", pa.float64(), 5, 0, -1),
    "name": ("string", pa.string(), 6, 1, -1),
    "payload": ("binary", pa.binary(), 6, 0, -1),
    "fixed": ("fixed", pa.binary(4), 7, 0, 4),
}


def pack_u8(value: int) -> bytes:
    return bytes([value])


def pack_i32(value: int) -> bytes:
    return struct.pack("<i", value)


def pack_u64(value: int) -> bytes:
    return struct.pack("<Q", value)


def pack_i64(value: int) -> bytes:
    return struct.pack("<q", value)


def update_bytes(h: "hashlib._Hash", data: bytes) -> None:
    h.update(pack_u64(len(data)))
    h.update(data)


def digest_table(table: pa.Table) -> str:
    h = hashlib.sha256()
    h.update(b"zig-parquet-digest-v1\x00")
    h.update(pack_u64(table.num_rows))
    h.update(pack_u64(len(table.schema)))

    kinds: list[str] = []
    for field in table.schema:
        kind, _arrow_type, physical, logical, type_length = TYPE_INFO[field.name]
        kinds.append(kind)
        update_bytes(h, field.name.encode())
        h.update(pack_i32(physical))
        h.update(pack_u8(logical))
        h.update(pack_i32(1 if field.nullable else 0))
        h.update(pack_i32(type_length))
        h.update(pack_i32(-1))
        h.update(pack_i32(-1))

    columns = [table.column(i).to_pylist() for i in range(len(table.schema))]
    for row in range(table.num_rows):
        for kind, values in zip(kinds, columns):
            value = values[row]
            if value is None:
                h.update(pack_u8(0))
                continue
            h.update(pack_u8(1))
            if kind == "bool":
                h.update(pack_u8(1 if value else 0))
            elif kind == "int32":
                h.update(pack_i32(value))
            elif kind == "int64":
                h.update(pack_i64(value))
            elif kind == "float32":
                h.update(struct.pack("<f", value))
            elif kind == "float64":
                h.update(struct.pack("<d", value))
            elif kind == "string":
                update_bytes(h, value.encode())
            elif kind in ("binary", "fixed"):
                update_bytes(h, bytes(value))
            else:
                raise AssertionError(kind)

    return h.hexdigest()


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
        raise AssertionError(f"bad parquet_digest output: {result.stdout!r}")
    return rows, digest


def case_label(
    case: int,
    rows: int,
    selected: list[str],
    null_rate: float,
    compression: str,
    data_page_version: str,
    use_dictionary: bool,
    use_byte_stream_split: bool,
    use_delta_binary: bool,
    use_delta_length: bool,
    use_delta_byte_array: bool,
    row_group_size: int,
    data_page_size: int,
) -> str:
    return (
        f"case={case} rows={rows} columns={selected} null_rate={null_rate} "
        f"compression={compression} page={data_page_version} dict={use_dictionary} "
        f"bss={use_byte_stream_split} delta={use_delta_binary} "
        f"delta_len={use_delta_length} delta_ba={use_delta_byte_array} "
        f"row_group={row_group_size} page_size={data_page_size}"
    )


def maybe_null(rng: random.Random, value, null_rate: float):
    return None if rng.random() < null_rate else value


def make_values(rng: random.Random, kind: str, rows: int, null_rate: float, allow_i64_extremes: bool) -> list:
    values = []
    words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]
    for i in range(rows):
        if kind == "bool":
            value = (i + rng.randrange(3)) % 3 == 0
        elif kind == "int32":
            edge = [-(2**31), -1, 0, 1, 2**31 - 1]
            value = edge[i % len(edge)] if i % 29 == 0 else ((i % 97) - 48 if rng.random() < 0.7 else rng.randint(-(2**30), 2**30 - 1))
        elif kind == "int64":
            edge = [-(2**63), -1, 0, 1, 2**63 - 1]
            value = edge[i % len(edge)] if allow_i64_extremes and i % 31 == 0 else (i * 17 - 12345 if rng.random() < 0.7 else rng.randint(-(2**45), 2**45 - 1))
        elif kind == "float32":
            edge = [0.0, -0.0, math.inf, -math.inf, math.nan]
            value = edge[i % len(edge)] if i % 37 == 0 else (i % 101) * 0.5 - 17.25
        elif kind == "float64":
            edge = [0.0, -0.0, math.inf, -math.inf, math.nan]
            value = edge[i % len(edge)] if i % 41 == 0 else i * 0.125 - 99.5
        elif kind == "string":
            if i % 23 == 0:
                value = ""
            elif i % 31 == 0:
                value = f"prefix-{i // 3:06d}-" + ("x" * 96)
            else:
                value = f"prefix-{i // 3:06d}-{words[(i + rng.randrange(len(words))) & (len(words) - 1)]}"
        elif kind == "binary":
            if i % 19 == 0:
                n = 0
            elif i % 29 == 0:
                n = 257
            else:
                n = rng.randrange(0, 32)
            value = bytes(((i + j * 13 + rng.randrange(17)) & 0xFF) for j in range(n))
        elif kind == "fixed":
            value = bytes(((i >> (j * 2)) + j * 41) & 0xFF for j in range(4))
        else:
            raise AssertionError(kind)
        values.append(maybe_null(rng, value, null_rate))
    return values


def make_table(rng: random.Random, rows: int, names: list[str], null_rate: float, allow_i64_extremes: bool) -> pa.Table:
    arrays = {}
    fields = []
    for name in names:
        kind, arrow_type, _physical, _logical, _type_length = TYPE_INFO[name]
        arrays[name] = pa.array(make_values(rng, kind, rows, null_rate, allow_i64_extremes), type=arrow_type)
        fields.append(pa.field(name, arrow_type, nullable=True))
    return pa.Table.from_arrays([arrays[name] for name in names], schema=pa.schema(fields))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", type=int, default=96)
    args = parser.parse_args()

    if not ZIG_DIGEST.exists():
        raise FileNotFoundError(f"{ZIG_DIGEST} does not exist; run zig build -Doptimize=ReleaseFast first")
    TMP.mkdir(parents=True, exist_ok=True)
    rng = random.Random(0x5EED)
    names = list(TYPE_INFO)
    cases = args.cases

    for case in range(cases):
        rows = rng.choice([1, 2, 7, 31, 128, 513, 2048])
        selected = [name for name in names if rng.random() < 0.65]
        if not selected:
            selected = [rng.choice(names)]
        null_rate = rng.choice([0.0, 0.05, 0.25, 0.8])
        compression = rng.choice(["NONE", "SNAPPY", "GZIP", "LZ4", "ZSTD"])
        data_page_version = rng.choice(["1.0", "2.0"])
        use_dictionary = rng.choice([False, True])
        row_group_size = rng.choice([1, 3, 17, 128, rows])
        data_page_size = rng.choice([128, 512, 4096])
        use_byte_stream_split = (
            not use_dictionary
            and all(TYPE_INFO[name][0] in ("int32", "int64", "float32", "float64", "fixed") for name in selected)
            and rng.choice([False, True])
        )
        use_delta_binary = (
            not use_dictionary
            and not use_byte_stream_split
            and all(TYPE_INFO[name][0] in ("int32", "int64") for name in selected)
            and rng.choice([False, True])
        )
        use_delta_length = (
            not use_dictionary
            and not use_byte_stream_split
            and not use_delta_binary
            and all(TYPE_INFO[name][0] in ("string", "binary") for name in selected)
            and rng.choice([False, True])
        )
        use_delta_byte_array = (
            not use_dictionary
            and not use_byte_stream_split
            and not use_delta_binary
            and not use_delta_length
            and all(TYPE_INFO[name][0] in ("string", "binary", "fixed") for name in selected)
            and rng.choice([False, True])
        )
        if use_delta_binary:
            column_encoding = {name: "DELTA_BINARY_PACKED" for name in selected}
        elif use_delta_length:
            column_encoding = {name: "DELTA_LENGTH_BYTE_ARRAY" for name in selected}
        elif use_delta_byte_array:
            column_encoding = {name: "DELTA_BYTE_ARRAY" for name in selected}
        else:
            column_encoding = None

        table = make_table(rng, rows, selected, null_rate, allow_i64_extremes=not use_delta_binary)
        path = TMP / f"case_{case:03d}.parquet"
        pq.write_table(
            table,
            path,
            compression=compression,
            use_dictionary=use_dictionary,
            use_byte_stream_split=use_byte_stream_split,
            column_encoding=column_encoding,
            data_page_version=data_page_version,
            data_page_size=data_page_size,
            row_group_size=row_group_size,
        )

        pyarrow_table = pq.read_table(path)
        expected = digest_table(pyarrow_table)
        label = case_label(
            case,
            rows,
            selected,
            null_rate,
            compression,
            data_page_version,
            use_dictionary,
            use_byte_stream_split,
            use_delta_binary,
            use_delta_length,
            use_delta_byte_array,
            row_group_size,
            data_page_size,
        )
        try:
            actual_rows, actual = run_zig_digest(path)
        except subprocess.CalledProcessError as exc:
            raise AssertionError(
                f"parquet_digest failed {label}: "
                f"stdout={exc.stdout!r} stderr={exc.stderr!r}"
            ) from exc
        if actual_rows != pyarrow_table.num_rows or actual != expected:
            raise AssertionError(
                f"digest mismatch {label}: {actual_rows=} {actual=} expected={expected}"
            )

    print(f"fuzz-digest-ok cases={cases}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
