# zig_parquet

Pure Zig Parquet reader/writer for flat, primitive schemas.

## Current Format Surface

- Parquet file magic and compact Thrift footer/page metadata.
- Flat required and optional columns.
- Physical types: `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`,
  `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`.
- UTF-8 string annotation for `BYTE_ARRAY`; decimal annotations with precision
  and scale over `INT32`, `INT64`, `BYTE_ARRAY`, and `FIXED_LEN_BYTE_ARRAY`.
- `DATA_PAGE` v1/v2 pages with `PLAIN` values and `RLE` definition levels.
- `RLE`-encoded `BOOLEAN` data pages produced by common writers.
- `BYTE_STREAM_SPLIT` data pages for fixed-width values when reading, and for
  float/double columns when writing (`writer.Options.use_byte_stream_split`).
- `DELTA_BINARY_PACKED` integer pages when reading.
- `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY` pages when reading.
- Dictionary pages and `RLE_DICTIONARY` / `PLAIN_DICTIONARY` data pages for reading.
- Writer-side `BYTE_ARRAY` dictionary pages when repeated values make them useful
  (`writer.Options.use_dictionary`, enabled by default).
- Writer-side `DATA_PAGE` v1 or v2 (`writer.Options.data_page_version`).
- Optional page CRC checksums (`writer.Options.page_checksum`) are validated by
  both memory-backed and streaming readers when present.
- Row-group and page null counts, plus min/max statistics for boolean, integer,
  float, double, non-decimal byte-array, and non-decimal fixed-length byte-array
  columns. Float/double min/max are omitted if a page or row group contains NaN
  values.
- Writer-side `OffsetIndex` and `ColumnIndex` footer sections for pages with
  supported statistics, plus streaming reader parsing of those page indexes for
  page row ranges, offsets, sizes, and page-level statistics.
- Snappy-compressed pages for reading.
- Zstandard-compressed pages for reading and writing. The writer uses a bounded
  pure-Zig zstd encoder with RLE blocks and raw-literal/repeated-sequence
  compressed blocks, with raw-block fallback when compression is not beneficial.
- Multiple row groups. The writer emits bounded data pages within each row group
  (`writer.Options.max_page_rows`, default 64K rows) plus footer metadata.
- File-backed reader that loads the footer and one requested column page at a time.

Unsupported codecs other than Snappy/Zstandard, nested/repeated schema, and legacy INT96 currently return explicit errors instead of being decoded incorrectly.

## Production Readiness

This library is usable for controlled flat-schema pipelines covered by the test
matrix below, but it is not a general production Parquet implementation yet.
The biggest remaining gaps are nested/repeated schemas, the wider Parquet
encoding and codec surface, deeper fuzz/corpus coverage, and a full entropy
Zstandard writer comparable to mature native implementations.

## Streaming Pattern

Write large datasets by repeatedly calling `StreamWriter.writeRowGroup` with bounded column batches, then `finish`. Tune `writer.Options.max_page_rows` to cap per-page encode buffers independently of row-group size.

Read large datasets with `reader.StreamFileReader`. Iterate `metadata.row_groups`, then call `readRowGroupColumns` or `readColumn`; memory use is bounded by footer metadata plus the selected row group's page data. Use `columnPageInfoIterator` to scan page row ranges, byte offsets, encodings, sizes, and page statistics without decoding values. When a column chunk has an `OffsetIndex` and `ColumnIndex`, the iterator uses those footer indexes; otherwise it falls back to page-header scanning.

Raw statistic byte slices can be decoded with `Statistics.minPhysical` and
`Statistics.maxPhysical` for the supported physical type.

## Verification

Run Zig tests:

```sh
../.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build test
```

From the repo root, build fixtures and run PyArrow compatibility:

```sh
.toolchains/zig-aarch64-linux-0.17.0-dev.135+9df02121d/zig build -Doptimize=ReleaseFast
uv run --with pyarrow python zig_parquet/tools/verify_pyarrow.py
uv run --with pyarrow python zig_parquet/tools/fuzz_digest.py
uv run python zig_parquet/tools/corrupt_smoke.py
```

Run the local throughput harness:

```sh
uv run --with pyarrow python zig_parquet/tools/bench_parquet.py --rows 250000
```
