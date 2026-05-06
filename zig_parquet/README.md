# zig_parquet

Pure Zig Parquet reader/writer for flat primitive schemas, with limited
nested/repeated read and write support.

## Current Format Surface

- Parquet file magic and compact Thrift footer/page metadata.
- Flat required and optional columns. The reader also accepts non-repeated
  required/optional group paths and exposes primitive leaves as flat columns.
  Column metadata preserves the full schema path segments for nested leaves so
  duplicate leaf names can be addressed unambiguously; row-group column chunk
  paths are validated against those schema paths before reads are exposed.
  Repeated ancestor paths are also preserved in `Column.repeated_level_info`,
  keyed by repetition level, so callers can map triplet boundary offsets back to
  the repeated schema nodes that produced them. LIST/MAP annotation ancestors
  are preserved in `Column.nested_logical_info`, including their schema paths and
  definition/repetition levels, so callers can map decoded levels back to
  nullable logical containers.
  Repeated primitive leaves can be read through `StreamFileReader.readColumnTriplets`,
  which returns values, definition/repetition levels, row offsets, value offsets,
  and per-repeated-level boundary offsets, including multi-level nested leaves
  that are addressed by full schema path. `readColumnNestedLogical` assembles
  one primitive leaf's LIST/MAP ancestor offsets/nullability plus leaf-slot
  validity from those triplets. `readColumnNestedMapPair` validates and bundles
  sibling MAP key/value leaves at their deepest common MAP ancestor. Standard one-level
  `LIST` leaves can be read through `StreamFileReader.readColumnList`, which
  returns row offsets, parent validity, element validity, and decoded leaf
  values. Legacy repeated primitive fields with `max_def=1/max_rep=1` can also
  use `readColumnList` as an always-present row-offset view. Standard one-level
  `MAP` key/value leaves can be read through `StreamFileReader.readColumnMap`,
  which returns row offsets, optional map validity, required keys, and optional
  or required values. The writer can emit legacy repeated primitive leaves
  through `StreamWriter.writeRowGroupTriplets` when callers provide exact
  definition/repetition levels. Canonical `LIST<MAP<K, V>>` shapes can use
  `StreamWriter.writeRowGroupListMaps` with list offsets, map offsets,
  optional list/map validity, required keys, and optional or required values.
  Canonical list-only multi-level `LIST` leaves can use
  `StreamWriter.writeRowGroupNestedLists` with one offsets/validity pair per
  list level. Standard one-level primitive `LIST` leaves can use
  `StreamWriter.writeRowGroupLists`, and standard one-level primitive `MAP`
  leaves can use `StreamWriter.writeRowGroupMaps`. Canonical two-level
  `MAP<K, MAP<K, V>>` shapes can use `StreamWriter.writeRowGroupNestedMaps`
  with one offsets/validity pair per map level. Mixed flat/LIST/MAP/nested
  LIST/LIST-MAP/nested MAP row groups can use `StreamWriter.writeRowGroupMixed`.
  Full multi-column nested object assembly and general writer-side nested
  LIST/MAP combinations outside these documented slices are not implemented yet.
- Physical types: `BOOLEAN`, `INT32`, `INT64`, `INT96` as raw 12-byte values,
  `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`.
- UTF-8 string annotation for `BYTE_ARRAY`; decimal annotations with precision
  and scale over `INT32`, `INT64`, `BYTE_ARRAY`, and `FIXED_LEN_BYTE_ARRAY`.
- `DATA_PAGE` v1/v2 pages with `PLAIN` values, v1 `RLE` or legacy
  `BIT_PACKED` definition levels, v2 level bodies, flat v2 zero-width
  repetition-level sections emitted by some writers, and writer-side RLE
  definition/repetition levels for legacy repeated primitive and standard
  one-level primitive `LIST` pages.
- `RLE`-encoded `BOOLEAN` data pages produced by common writers.
- `BYTE_STREAM_SPLIT` data pages for fixed-width values when reading, and for
  float/double columns when writing (`writer.Options.use_byte_stream_split`).
- `DELTA_BINARY_PACKED` integer pages when reading and writing
  (`writer.Options.use_delta_binary_packed`).
- `DELTA_LENGTH_BYTE_ARRAY` byte-array pages when reading and writing
  (`writer.Options.use_delta_length_byte_array`).
- `DELTA_BYTE_ARRAY` byte-array and fixed-length byte-array pages when reading
  and writing (`writer.Options.use_delta_byte_array`).
- Dictionary pages and `RLE_DICTIONARY` / `PLAIN_DICTIONARY` data pages for
  reading, including legacy streams that place a dictionary page at the first
  page offset while omitting `dictionary_page_offset`.
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
- Snappy-, Gzip-, LZ4_RAW-, and legacy LZ4-compressed pages for reading.
  Snappy, Gzip, and LZ4_RAW are supported for writing. The legacy LZ4 reader
  accepts Hadoop LZ4 framing, LZ4 frame blocks, and raw-block fallback for older
  Parquet files. The LZ4_RAW writer uses a pure-Zig hash-table match search with
  literal fallback and skips short matches that hurt decode throughput more than
  they help size.
- Zstandard-compressed pages for reading and writing. The writer uses a bounded
  pure-Zig zstd encoder with RLE blocks and raw-literal/repeated-sequence
  compressed blocks, with raw-block fallback when compression is not beneficial.
  The reader sizes fallback Zstd scratch space from each frame's advertised
  window, bounded by the page-size limit, so valid non-single-segment frames with
  small page output are accepted.
- Multiple row groups. The writer emits bounded data pages within each row group
  (`writer.Options.max_page_rows`, default 64K rows) plus footer metadata.
- File-backed reader that loads the footer and one requested column page at a
  time, with optional row-group and column parallelism for seekable files.

Unsupported read codecs other than Snappy/Gzip/LZ4/LZ4_RAW/Zstandard,
unsupported write codecs other than Snappy/Gzip/LZ4_RAW/Zstandard and nested
schema assembly outside the documented LIST/MAP slices currently return explicit
errors instead of being decoded incorrectly.

## Production Readiness

This library is usable for controlled flat-schema pipelines covered by the test
matrix below, plus narrow non-repeated nested, one-level LIST/MAP read slices,
one-level primitive LIST/MAP writing, leaf-oriented multi-level LIST/MAP layout
reconstruction, low-level legacy repeated primitive triplet writing, and
canonical LIST-of-MAP, list-only multi-level LIST, and two-level MAP writing
from offsets/nullability, but it is not a general production Parquet
implementation yet. The biggest remaining gaps are full multi-column nested
object assembly, high-level writer-side nested LIST/MAP combinations beyond the
documented canonical slices, the remaining
Parquet codec surface, deeper fuzz/corpus coverage, and stronger LZ4 plus full
entropy Zstandard writers comparable to mature native implementations.

Current completion audit against the production target:

| Requirement | Current evidence | Status |
| --- | --- | --- |
| Pure Zig streaming read/write | `StreamFileReader` and `StreamWriter` cover flat primitive schemas with bounded row-group/page buffers; the reader also handles non-repeated nested primitive leaves as flat outputs with full schema paths, exposes repeated primitive leaves as definition/repetition triplets plus per-row, per-value, and per-repeated-level boundary offsets, preserves repeated ancestor paths in `Column.repeated_level_info` plus LIST/MAP annotation ancestors in `Column.nested_logical_info`, provides `readColumnNestedTriplets` to bundle owned triplets with owned nested path metadata, provides `readColumnNestedLogical` to assemble per-ancestor LIST/MAP offsets/nullability plus leaf-slot validity for one primitive leaf, and provides `readColumnNestedMapPair` to validate and bundle sibling MAP key/value leaves at a shared MAP level. It exposes standard one-level LIST plus simple legacy repeated primitive leaves as row offsets plus parent/element validity where applicable, and exposes standard one-level MAP key/value leaves as row offsets plus map/value validity. `writeRowGroupTriplets` emits legacy repeated primitive leaves from caller-provided definition/repetition levels, `writeRowGroupNestedLists` emits canonical list-only multi-level LIST leaves from offsets/nullability, `writeRowGroupListMaps` emits canonical LIST-of-MAP leaves from offsets/nullability, `writeRowGroupNestedMaps` emits canonical two-level MAP leaves from offsets/nullability, `writeRowGroupLists` emits standard one-level primitive LIST leaves from offsets plus optional parent/element validity, `writeRowGroupMaps` emits standard one-level primitive MAP leaves from offsets plus optional map/value validity, and `writeRowGroupMixed` emits heterogeneous flat/LIST/MAP/nested-LIST/LIST-MAP/nested-MAP row groups. | Partial: full recursive nested object materialization and high-level writer-side nested LIST/MAP combinations outside the documented canonical slices are still unsupported. |
| Zstd compression support | Pure-Zig Zstd read/write is implemented, `parquet_verify_zstd_fast` byte-checks accepted fast-path pages against Zig stdlib Zstd, and fallback scratch now follows bounded advertised frame windows. | Partial: reads cover the current PyArrow/Zig corpus and upstream non-single-segment frames; writer is bounded and useful but not a full entropy encoder. |
| IO perf within 30% of SOTA | Reuse-reader, Zig-written/fixed-width paths, and row-group+column-parallel fresh PyArrow uncompressed/Snappy/Gzip/LZ4/Zstd scans meet or beat the 0.70x threaded PyArrow floor in the benchmark below. The direct Zig/PyArrow write comparison covers uncompressed, Snappy, Gzip, LZ4_RAW/PyArrow LZ4, and Zstd output, and is gated by `perf_gate.py --profile write`. | Partial: broader disk-cold/storage coverage and some encoding-specific fresh scans are still missing. |
| Production reference | `deps/arrow-rs` is pinned locally and used for codec/page IO architecture comparison. | In place. |
| Verification | Zig tests, PyArrow compatibility, deterministic corpus smoke, Apache `parquet-testing` external corpus smoke, 960-case writer matrix, fuzz digest, corrupt smoke, and Zstd fast-path verifier are listed below. | Partial: external corpus still exposes unsupported repeated/nested assembly and malformed-file cases. No supported flat or non-repeated nested corpus file now fails at footer encoding validation. |

## Reference Baseline

`deps/arrow-rs` is kept as a local reference checkout for production behavior
and architecture comparisons. The current checkout used while updating this
snapshot is Apache Arrow-rs commit
`70e4069faeea69b5252c6145cf5600e3434a0852`. The most relevant files are
`parquet/src/file/serialized_reader.rs` for page IO/decompression boundaries,
`parquet/src/column/page.rs` for compressed vs decoded page ownership,
`parquet/src/compression.rs` for codec contracts, and
`parquet/src/arrow/decoder/dictionary_index.rs` for chunked dictionary-index
decode structure. Arrow-rs delegates Zstd to the `zstd::bulk` API with reusable
compressor/decompressor contexts; `zig_parquet` keeps the implementation pure
Zig, so the remaining Zstd perf work is mostly decoder and first-scan cache
efficiency rather than binding to the same native backend.
The legacy `LZ4` read path follows Arrow-rs' compatibility behavior: try Hadoop
LZ4 framing first, then LZ4 frame blocks, then raw-block fallback for older
Parquet files whose metadata used the deprecated codec enum.

`deps/parquet-testing` is used as an upstream interoperability corpus when
present. The local corpus used for this snapshot is Apache parquet-testing
commit `e74785d85a4ecee829e1e405444d6a1b24b8bc9c`; clone it with
`git clone --depth 1 https://github.com/apache/parquet-testing deps/parquet-testing`
before running `external_corpus_smoke.py`.

## Performance Snapshot

Local `ReleaseFast` samples with the pinned toolchain and `250000` rows; most
rows use `--read-iterations 30`, while the LZ4 rows use a focused
`--case-filter lz4 --read-iterations 20` sample. Rates are logical Parquet
uncompressed MiB/s. Write rates in the table and direct Zig/PyArrow write
comparison use the focused `--case-filter write-` harness slice and include
end-to-end fixture generation plus file writes. PyArrow read rates use the
benchmark's default single-thread mode; pass `--pyarrow-use-threads` to compare
against PyArrow's threaded reader. The default benchmark performs one validated
warmup before timing repeated scans through one parsed reader. The Zig read
harness uses a 1 KiB file-reader buffer so direct page-body reads do not copy
large uncompressed pages through the IO buffer. The default Zig reader keeps
bounded exact identity-page and fixed-dictionary caches for identity
dictionary-index pages; `--dictionary-cache` below is a separate opt-in cache for
the general dictionary decode path.

| Case | Writer | Write | Zig read | PyArrow read | Zig/PyArrow read |
| --- | --- | ---: | ---: | ---: | ---: |
| Zig uncompressed | Zig | 552 | 2242 | 1041 | 2.15x |
| Zig Snappy | Zig | 282 | 1027 | 696 | 1.47x |
| Zig Gzip | Zig | 120 | 187 | 504 | 0.37x |
| Zig LZ4_RAW | Zig | 201 | 2067 | 983 | 2.10x |
| Zig Zstd plain | Zig | 363 | 1026 | 800 | 1.28x |
| Zig Zstd delta-binary | Zig | 257 | 881 | 552 | 1.60x |
| PyArrow uncompressed input | PyArrow | 56 | 2246 | 937 | 2.40x |
| PyArrow Snappy input | PyArrow | 56 | 2278 | 833 | 2.74x |
| PyArrow Gzip input | PyArrow | 2 | 1973 | 433 | 4.56x |
| PyArrow LZ4 input | PyArrow | 58 | 2395 | 1012 | 2.37x |
| PyArrow Zstd input | PyArrow | 55 | 2253 | 761 | 2.96x |

### Zig/PyArrow Write Comparison

Direct write comparison from the focused write-only harness slice:

```sh
uv run --with pyarrow python zig_parquet/tools/bench_parquet.py --rows 250000 --read-iterations 1 --case-filter write-
```

| Codec | Zig write (MiB/s) | PyArrow write (MiB/s) | Zig/PyArrow write |
| --- | ---: | ---: | ---: |
| Uncompressed | 552 | 56 | 9.84x |
| Snappy | 282 | 56 | 5.02x |
| Gzip | 120 | 2 | 59.6x |
| LZ4_RAW / PyArrow LZ4 | 201 | 58 | 3.47x |
| Zstd plain | 363 | 55 | 6.58x |

The Zstd delta-binary row above is a separate Zig-only encoding path, so it is
not included in the direct writer comparison. The LZ4 row compares Zig's
Parquet `LZ4_RAW` writer against PyArrow's `compression="LZ4"` output. The
LZ4_RAW writer uses a simple single-entry hash-table match search and
intentionally skips short matches, so it is tuned for the current read-throughput
target more than maximum LZ4 ratio.
Gate the same write-comparison slice with the 0.70x target using:

```sh
uv run --with pyarrow python zig_parquet/tools/perf_gate.py --profile write
```

A focused PyArrow Zstd column breakdown from the same setup shows the bounded
identity page and fixed-dictionary caches turning repeated scans of validated
identity dictionary-index pages into direct fixed-value clones. Optional
all-valid fixed-width identity columns keep `validity = null`, matching the
library's all-present representation and avoiding per-row bool bitmap clones.
The full-column and numeric single-column reads are now well above the
single-thread PyArrow target in this repeated-scan sample:

| PyArrow Zstd selection | Zig read | PyArrow read | Zig/PyArrow read |
| --- | ---: | ---: | ---: |
| All columns | 2344 | 754 | 3.11x |
| `id` int64 | 13949 | 1219 | 11.44x |
| `score` double | 14462 | 1157 | 12.50x |
| `name` string | 70 | 58 | 1.20x |

With `--dictionary-cache`, repeated scans use the general decoded dictionary
cache instead of the identity-column fast path. It remains above the 0.70x
target for PyArrow Zstd, but the default identity cache is faster for these
fixed-width identity-index columns:

| Cached PyArrow Zstd selection | Zig read | PyArrow read | Zig/PyArrow read |
| --- | ---: | ---: | ---: |
| All columns | 984 | 765 | 1.29x |
| `id` int64 | 1516 | 1229 | 1.23x |
| `score` double | 1494 | 1153 | 1.30x |
| `name` string | 70 | 58 | 1.20x |

Using `--pyarrow-use-threads` on the same 250K-row fixture shape with
`--read-iterations 20` gives a stricter baseline. Selected full-column threaded
ratios:

| Threaded PyArrow case | Zig read | PyArrow read | Zig/PyArrow read |
| --- | ---: | ---: | ---: |
| Zig uncompressed | 2293 | 1085 | 2.11x |
| Zig Snappy | 1056 | 1137 | 0.93x |
| Zig Gzip | 192 | 983 | 0.20x |
| Zig Zstd plain | 1020 | 1105 | 0.92x |
| Zig Zstd byte-stream-split | 889 | 1130 | 0.79x |
| Zig Zstd delta-binary | 891 | 641 | 1.39x |
| PyArrow Zstd input | 2309 | 1960 | 1.18x |

Using `--fresh-reader --evict-os-cache --pyarrow-use-threads` with
`--read-iterations 10` gives a stricter fresh-library-state baseline. The
eviction is best-effort Linux/POSIX `POSIX_FADV_DONTNEED`, not a controlled
disk-cold run:

| Fresh + evict case | Zig read | PyArrow read | Zig/PyArrow read |
| --- | ---: | ---: | ---: |
| Zig uncompressed | 1990 | 997 | 2.00x |
| Zig Snappy | 1054 | 1039 | 1.02x |
| Zig Gzip | 184 | 970 | 0.19x |
| Zig Zstd plain | 1014 | 1064 | 0.95x |
| Zig Zstd byte-stream-split | 648 | 1081 | 0.60x |
| Zig Zstd delta-binary | 903 | 637 | 1.42x |
| PyArrow uncompressed input | 1447 | 1588 | 0.91x |
| PyArrow Snappy input | 983 | 1634 | 0.60x |
| PyArrow Gzip input | 210 | 984 | 0.21x |
| PyArrow Zstd input | 524 | 1722 | 0.30x |

For PyArrow-compatible fixtures, `--zig-parallel-columns` measures the
in-process `StreamFileReader.readRowGroupsColumnsParallel` and
`readRowGroupsSelectedColumnsParallel` APIs. These APIs share the parent
reader's validated metadata and give each worker its own positional file reader,
page scratch space, and dictionaries. Worker-owned one-shot readers disable the
identity page and identity dictionary caches so the parallel path does not spend
time filling caches that are dropped immediately. The throughput harness uses
`std.heap.smp_allocator` for timed Zig reads. With
`--fresh-reader --evict-os-cache --pyarrow-use-threads --zig-parallel-columns --read-iterations 20`,
selected fresh first-scan ratios are:

| Fresh + evict + row-group/column parallel | Zig read | PyArrow read | Zig/PyArrow read |
| --- | ---: | ---: | ---: |
| PyArrow uncompressed input | 1727 | 1632 | 1.06x |
| PyArrow Snappy input | 1668 | 1725 | 0.97x |
| PyArrow Gzip input | 828 | 987 | 0.84x |
| PyArrow LZ4 input | 1721 | 1747 | 0.99x |
| PyArrow Zstd input | 1352 | 1643 | 0.82x |
| PyArrow Gzip `id` | 741 | 531 | 1.40x |
| PyArrow Zstd `id` | 1522 | 1031 | 1.48x |

These numbers show the current flat-schema fast paths are competitive for
uncompressed, Snappy, LZ4_RAW, and Zig-written Zstd files. Lazy dictionary loading plus
byte-aligned and identity dictionary-index fast paths put PyArrow-written Snappy
full-column and numeric reads above the 0.70x single-thread target. Raw/RLE,
raw-literal FSE sequence, compressed-literal sequence, scratch-backed literal
predecode, faster overlapping match copy, and direct byte-stream-split numeric
decode put Zig-written full-column Zstd reads above the same target, including
byte-stream-split and delta-binary-plus-byte-stream-split fixtures. Direct
accumulator decode for `DATA_PAGE` v1/v2 byte-stream-split fixed-width values,
delta-binary integer pages, and fixed-width dictionary pages avoids temporary
per-page columns before row-group assembly. Direct required PLAIN fixed-width
reads avoid an extra copy on uncompressed pages, and optional all-valid
dictionary columns avoid fallback materialization in first-scan PyArrow Zstd
reads. Direct fixed dictionary decode removes one copy from that path. The exact
compressed identity page cache skips repeated
dictionary-index validation after a byte-for-byte checked page has already been
validated, and the bounded identity fixed-dictionary cache skips repeated
dictionary-page decompression for validated identity columns. Optional
all-valid fixed-width identity columns use the same null-validity representation
as required all-present columns, avoiding bool bitmap allocation and clones on
the hot repeated-scan path. PyArrow-written Gzip and Zstd repeated scans now
clear the target in this harness. Fresh PyArrow Gzip/Zstd full scans and
selected numeric Gzip/Zstd scans clear the threaded target with row-group
parallelism. Gzip decode uses std inflate with an explicit history buffer plus a
portable slicing-by-8 CRC-32 footer check; the previous byte-at-a-time CRC path
was a large first-scan cost. Fresh-reader/cache-evicted threaded comparisons
still need broader production-level work: byte-stream-split numeric fresh reads,
controlled disk-cold storage scans, and wider parallel IO coverage need more
decoder, first-scan, or scheduling verification.

## Streaming Pattern

Write large flat datasets by repeatedly calling `StreamWriter.writeRowGroup`
with bounded column batches, then `finish`. For legacy repeated primitive leaf
columns, call `StreamWriter.writeRowGroupTriplets` with non-null decoded values
plus exact definition/repetition levels; this emits v1 or v2 triplet pages and
is verified against PyArrow as a repeated primitive list view. For canonical
`LIST<MAP<K, V>>` columns, call `StreamWriter.writeRowGroupListMaps` with list
offsets/validity, map offsets/validity, required keys, and optional or required
values; the schema must use `list.element.key_value` paths plus LIST and MAP
logical ancestors in `Column.nested_logical_info`. For canonical list-only
multi-level LIST columns, call `StreamWriter.writeRowGroupNestedLists` with one
offsets/validity pair per list level plus leaf values; the column schema must
include the full leaf path plus LIST logical ancestors in
`Column.nested_logical_info`. For standard one-level primitive LIST columns,
call `StreamWriter.writeRowGroupLists` with list offsets, optional parent list
validity, optional element validity embedded in the element `ColumnData`, and
non-null decoded values. For standard one-level primitive MAP columns, call
`StreamWriter.writeRowGroupMaps` with entry offsets,
optional parent map validity, required keys, and optional or required values.
For canonical two-level map-only shapes such as `MAP<K, MAP<K, V>>`, call
`StreamWriter.writeRowGroupNestedMaps` with one offsets/validity pair per map
level, outer keys, inner keys, and optional or required inner values; the schema
must use standard `key_value` paths and MAP logical ancestors.
Use `StreamWriter.writeRowGroupMixed` with `ColumnWriteData` when one row group
contains a mix of flat columns, low-level triplet leaves, one-level LIST leaves,
list-only nested LIST leaves, one-level MAP key/value pairs, and canonical
LIST-of-MAP or two-level nested MAP leaves.
Tune `writer.Options.max_page_rows` to cap per-page encode buffers independently
of row-group size.
The writer validates schema shape up front and rejects nested path metadata,
unsupported annotations, and non-flat max definition/repetition levels that its
flat and one-level LIST/MAP footer writers cannot represent yet.

Read large datasets with `reader.StreamFileReader`. Iterate
`metadata.row_groups`, then call `readRowGroupColumns`, `readColumn`,
`readRowGroupColumnsParallel`, `readRowGroupsColumnsParallel`, or
`readRowGroupsSelectedColumnsParallel`; memory use is bounded by footer metadata, the
selected row group's page data, and small internal exact-page and
fixed-dictionary caches for validated identity dictionary-index pages.
Use `metadata.schema.columns[i].path` when nested schemas need stable
path-based addressing; `name` remains the leaf field name. Use
`metadata.schema.columns[i].repeated_level_info` to map each non-zero repetition
level to the repeated ancestor path that introduced it, and
`metadata.schema.columns[i].nested_logical_info` to map LIST/MAP annotation
ancestors to their definition and repetition levels. `columnIndexByPath`
finds a dotted path, and the streaming reader also exposes `readColumnByPath`,
`readColumnTripletsByPath`, `readColumnNestedTripletsByPath`,
`readColumnNestedLogicalByPath`, `readColumnListByPath`, and
`readColumnMapByPath` wrappers for path-addressed reads. Path-selected row-group reads are available
through `readRowGroupSelectedColumnsByPath`,
`readRowGroupSelectedColumnsParallelByPath`, and
`readRowGroupsSelectedColumnsParallelByPath` for flat leaves inside nested
schemas. Page scanning and index access can also use dotted paths through
`columnPageIteratorByPath`, `columnPageInfoIteratorByPath`, and
`readColumnPageIndexByPath`.
For repeated primitive or nested leaves, call `readColumnTriplets` or
`readColumnTripletsByPath` to get non-null decoded values plus one
definition-level and repetition-level entry per encoded triplet. The returned
`row_offsets` slice indexes the triplet-level arrays per row, so callers can
preserve null and empty repeated rows while building higher-level views.
`value_offsets` indexes the decoded non-null value stream per row using the same
row boundaries. `repeated_level_offsets` gives boundary offsets for every
repetition level above the row level: entry `N-1` contains starts for repeated
level `N`, followed by the terminal level count. This exposes multi-level nested
leaves without reconstructing their logical parent arrays. Use
`readColumnNestedTriplets` or `readColumnNestedTripletsByPath` when the same
result should bundle owned triplets with owned column/repeated-level path
metadata plus owned LIST/MAP logical ancestor metadata.
Use `readColumnNestedLogical` or `readColumnNestedLogicalByPath` for a
leaf-oriented logical view: it returns one offset array and optional validity
array per LIST/MAP ancestor, plus decoded leaf values with leaf-slot validity
attached when the primitive leaf or an intermediate leaf-side node is nullable.
This reconstructs the logical layout for one primitive leaf. Use
`readColumnNestedMapPair` or `readColumnNestedMapPairByPath` to combine sibling
MAP key/value leaves; the reader chooses their deepest common MAP ancestor,
checks that key and value offsets/nullability match at that level, and returns
the owned key/value logical columns plus the selected level indexes. Full
recursive object materialization remains outside the flat-column APIs.
For standard one-level LIST leaves and simple legacy repeated primitive fields,
call `readColumnList`; it returns row offsets, parent list validity when
nullable lists are present, element validity when nullable elements are present,
and the decoded physical values. For standard one-level MAP key/value leaves,
call `readColumnMap`; it returns row offsets, map validity when nullable maps
are present, required key values, and optional or required value columns. Full
multi-column nested object reconstruction remains outside the flat-column APIs.
The parallel read APIs require a seekable file reader still in positional mode
and a thread-safe allocator; the parent reader's metadata remains borrowed until
all worker threads join. Worker-owned one-shot readers skip reusable identity
fast-path caches. Release
that internal page-validation state with `clearIdentityPageCache` and fixed
identity dictionaries with `clearIdentityDictionaryCache` if a long-lived reader
must drop all reusable state between scans. Use `columnPageInfoIterator` to scan
page row ranges, byte offsets, encodings, sizes, and page statistics without
decoding values. When a column chunk has an `OffsetIndex` and `ColumnIndex`, the
iterator uses those footer indexes; otherwise it falls back to page-header
scanning.

Repeated scans of the same file can enable decoded dictionary reuse with
`StreamFileReader.setDictionaryCacheEnabled(true)`. The cache is explicit and
can be released with `clearDictionaryCache`; leave it disabled when decoded
dictionaries should not persist across row groups.

Raw statistic byte slices can be decoded with `Statistics.minPhysical` and
`Statistics.maxPhysical` for the supported physical type.

## Verification

Run Zig tests:

```sh
../.toolchains/zig-aarch64-linux-0.16.0/zig build test
```

From the repo root, build fixtures and run PyArrow compatibility:

```sh
.toolchains/zig-aarch64-linux-0.16.0/zig build -Doptimize=ReleaseFast
uv run --with pyarrow python zig_parquet/tools/verify_pyarrow.py
uv run --with pyarrow python zig_parquet/tools/corpus_smoke.py
uv run --with pyarrow python zig_parquet/tools/external_corpus_smoke.py
uv run --with pyarrow python zig_parquet/tools/verify_writer_matrix.py
uv run --with pyarrow python zig_parquet/tools/fuzz_digest.py
uv run python zig_parquet/tools/corrupt_smoke.py
```

`corpus_smoke.py` runs deterministic corpus cases, including required mixed
fixed-width values, optional sparse byte arrays, all-null dictionary pages,
PyArrow zero-row row groups, Zig zero-row no-row-group files, an INT96 row-count
case, a non-repeated nested struct row-count case, selected flat-column reads
from a mixed flat-plus-list file, repeated-list triplet and row-offset list
validation, a standard one-level MAP row-offset/key/value validation, three
leaf-oriented nested logical layout checks, and clean failure checks for
currently unsupported repeated schema and Brotli files.
`external_corpus_smoke.py` scans Apache
parquet-testing `data/*.parquet`; with the pinned corpus above it currently
reads 47 of 64 files successfully, including legacy `BIT_PACKED`
definition-level files, empty-encoding metadata files whose page headers are
supported, BYTE_ARRAY decimal files, raw INT96 files, non-repeated nested struct
files, flat v2 optional boolean pages with explicit zero-width repetition levels,
flat v2 all-null optional pages with empty value sections, upstream
`DELTA_BINARY_PACKED` and `DELTA_BYTE_ARRAY` fixtures with compact or padded
final miniblocks, and legacy dictionary streams whose metadata omits
`dictionary_page_offset`. It also covers legacy LZ4 files written with Hadoop
framing or raw fallback under the deprecated `LZ4` codec enum. It checks 36
files against a PyArrow physical-value digest, row-checks 11 logical, nested, or
INT96 cases that PyArrow exposes through converted values or unsafe timestamps,
runs 5 repeated/list/map triplet checks including multi-level nested LIST and
MAP leaves with per-row, per-value, exact per-repeated-level triplet offset
counts, and schema repeated-level path linkage, runs 6 one-level LIST or legacy
repeated primitive row-offset checks, runs 2 one-level MAP key/value row-offset
checks, runs 7 nested logical layout checks across one-level and multi-level
LIST/MAP leaves, runs 4 nested MAP key/value pair assembly checks, runs 3 schema-path and repeated-level-path checks across list/map/nested-map files, exercises
path-addressed triplet/list/map, selected-column, page-info, and page-index
reads, and verifies the remaining 12 repeated/nested-assembly, 1 unsupported
compression, and 4 corrupt files fail cleanly.
`verify_writer_matrix.py`
runs 960 Zig writer cases across page versions, codecs including LZ4_RAW,
encodings, dictionary modes, checksums, and row-count edges. `fuzz_digest.py`
runs 96 deterministic edge-heavy PyArrow writer cases by default across
PyArrow's `NONE`, `SNAPPY`, `GZIP`, `LZ4`, and `ZSTD` codecs; pass `--cases N`
for a longer corpus sweep. `corrupt_smoke.py` mutates Zig-written fixtures for
every supported writer codec and asserts failures are reported without panics or
crashes. `verify_pyarrow.py`
checks Zig-written standard LIST/MAP fixtures, high-level LIST-of-MAP writer
fixtures, mixed flat/LIST/MAP/LIST-MAP/nested-MAP fixtures, canonical nested
LIST writer fixtures, canonical nested MAP writer fixtures,
legacy repeated primitive fixtures, and multi-row-group PyArrow Zstd dictionary
fixtures to cover row-local dictionary index fast paths. It also runs
`parquet_verify_zstd_fast`
so every Zstd page accepted by the custom fast path is byte-for-byte checked
against the standard Zig Zstd decoder. Run `parquet_verify_zstd_fast <file> --verbose` to
see why unsupported Zstd pages fall back and whether they are dictionary or data
pages. The current PyArrow Zstd fixtures have no unsupported Zstd pages in this
verifier: the large fixture reports `63/63` fast pages, and the multi-row-group
dictionary fixture reports `75/75` fast pages.

Run the local throughput harness:

```sh
uv run --with pyarrow python zig_parquet/tools/bench_parquet.py --rows 250000
```

The benchmark builds Zig-written and PyArrow-written fixtures, times repeated
in-process Zig and PyArrow reads, and prints per-case throughput ratios. Use
`--read-iterations N` to amortize timing noise and `--column-read-details` to
include `score` plus PyArrow-compatible `name` single-column comparisons.
Use `--dictionary-cache` to measure repeated Zig scans with decoded dictionary
reuse, `--fresh-reader` to reopen and reparse Zig and PyArrow readers for every
timed read iteration, and `--zig-page-version v2` to generate Zig-written
fixtures with `DATA_PAGE` v2 instead of v1. `--fresh-reader` measures fresh
library state on the current OS page cache; combine it with `--evict-os-cache`
for a Linux/POSIX best-effort `POSIX_FADV_DONTNEED` before each timed read. The
cache-eviction path excludes the fadvise call itself from read timing, but it
still depends on filesystem and kernel cooperation and is not a substitute for
controlled disk-cold storage benchmarking. `--zig-parallel-columns` requires
`--fresh-reader` and uses the in-process row-group/column-parallel reader APIs
for PyArrow-compatible full-column and selected-column fixture comparisons.
The standard PyArrow-compatible fresh/threaded full-scan gate now passes with:

```sh
uv run --with pyarrow python zig_parquet/tools/perf_gate.py --profile full
```

That wrapper runs the equivalent explicit benchmark gate:

```sh
uv run --with pyarrow python zig_parquet/tools/bench_parquet.py --rows 250000 --read-iterations 20 --fresh-reader --evict-os-cache --pyarrow-use-threads --zig-parallel-columns --case-filter read-pyarrow-none --case-filter read-pyarrow-snappy --case-filter read-pyarrow-gzip --case-filter read-pyarrow-lz4 --case-filter read-pyarrow-zstd --min-read-ratio 0.70
```

For shorter local checks while iterating, use:

```sh
uv run --with pyarrow python zig_parquet/tools/perf_gate.py
```

The quick profile intentionally gates only the stable uncompressed and Snappy
PyArrow fixture scans; the full profile is the all-codec 0.70x gate used for the
table above. The write profile gates the focused Zig/PyArrow write comparison
for uncompressed, Snappy, Gzip, LZ4_RAW/PyArrow LZ4, and Zstd output.

Use `--case-filter SUBSTRING` to run only matching generated case or comparison
names, such as `--case-filter pyarrow-zstd` or `--case-filter read-score`.
PyArrow reads are single-threaded by default for decode-path comparisons; pass
`--pyarrow-use-threads` for the threaded baseline. `--min-read-ratio 0.70` gates
every printed comparison; combine it with `--case-filter` to gate only the
benchmark slice whose current support and parallelism are expected to meet that
floor. `--min-write-ratio 0.70` gates comparable selected writer pairs.
