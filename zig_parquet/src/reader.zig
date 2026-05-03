const std = @import("std");
const types = @import("types.zig");
const thrift = @import("thrift.zig");
const plain = @import("plain.zig");
const snappy = @import("snappy.zig");
const zstd = @import("zstd.zig");

const max_footer_size = 64 * 1024 * 1024;
const max_page_size = 256 * 1024 * 1024;
const max_page_index_size = 64 * 1024 * 1024;

pub const ParsedFile = struct {
    arena: std.heap.ArenaAllocator,
    metadata: types.FileMetaData,
    bytes: []const u8,

    pub fn deinit(self: *ParsedFile) void {
        self.arena.deinit();
    }

    pub fn readColumn(self: *ParsedFile, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !types.OwnedColumn {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        const column = row_group.columns[column_index];
        const schema_col = self.metadata.schema.columns[column_index];
        var offset = std.math.cast(usize, column.data_page_offset) orelse return error.CorruptMetadata;
        if (offset >= self.bytes.len) return error.CorruptPage;

        const row_count = std.math.cast(usize, row_group.num_rows) orelse return error.CorruptMetadata;
        var rows_seen: usize = 0;
        var acc = ColumnAccumulator.init(column.physical_type, schema_col.repetition == .optional);
        errdefer acc.deinit(allocator);
        try acc.reserve(allocator, row_count);
        var scratch: PageDecodeScratch = .{};
        defer scratch.deinit(allocator);
        var dictionary = try self.readDictionaryFromMemory(allocator, column, schema_col, &scratch);
        defer if (dictionary) |*dict| dict.deinit(allocator);

        while (rows_seen < row_count) {
            var fixed = std.Io.Reader.fixed(self.bytes[offset..]);
            const page_header = try thrift.readPageHeader(&fixed);
            const page_rows = try pageRowCount(page_header);
            if (rows_seen + page_rows > row_count) return error.CorruptPage;
            const page_size = try checkedPageSize(page_header.compressed_page_size);
            const data_start = std.math.add(usize, offset, fixed.seek) catch return error.CorruptPage;
            const data_end = std.math.add(usize, data_start, page_size) catch return error.CorruptPage;
            if (data_end > self.bytes.len) return error.CorruptPage;
            try validatePageCrc(page_header, self.bytes[data_start..data_end]);

            var page_column = try decodeColumnPage(allocator, column, schema_col, page_header, self.bytes[data_start..data_end], if (dictionary) |*dict| dict else null, &scratch);
            defer page_column.deinit(allocator);
            try acc.append(allocator, page_column);

            rows_seen += page_rows;
            offset = data_end;
        }

        return acc.finish(allocator);
    }

    fn readDictionaryFromMemory(self: *ParsedFile, allocator: std.mem.Allocator, column: types.ColumnChunkMeta, schema_col: types.Column, scratch: *PageDecodeScratch) !?types.OwnedColumn {
        const dict_offset_i64 = column.dictionary_page_offset orelse return null;
        const offset = std.math.cast(usize, dict_offset_i64) orelse return error.CorruptMetadata;
        if (offset >= self.bytes.len) return error.CorruptPage;
        var fixed = std.Io.Reader.fixed(self.bytes[offset..]);
        const header = try thrift.readPageHeader(&fixed);
        if (header.page_type != .dictionary_page) return error.CorruptPage;
        const dict_header = header.dictionary_page_header orelse return error.CorruptPage;
        if (dict_header.encoding != .plain) return error.UnsupportedEncoding;
        const page_size = try checkedPageSize(header.compressed_page_size);
        const data_start = std.math.add(usize, offset, fixed.seek) catch return error.CorruptPage;
        const data_end = std.math.add(usize, data_start, page_size) catch return error.CorruptPage;
        if (data_end > self.bytes.len) return error.CorruptPage;
        try validatePageCrc(header, self.bytes[data_start..data_end]);
        const count = std.math.cast(usize, dict_header.num_values) orelse return error.CorruptPage;
        const page_bytes = try preparePageBytes(allocator, column.codec, header, self.bytes[data_start..data_end], scratch);
        defer page_bytes.deinit(allocator);
        return try plain.decodeValues(allocator, schema_col.column_type, count, count, null, page_bytes.data);
    }

    pub fn readRowGroupColumns(self: *ParsedFile, allocator: std.mem.Allocator, row_group_index: usize) ![]types.OwnedColumn {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const count = self.metadata.schema.columns.len;
        const columns = try allocator.alloc(types.OwnedColumn, count);
        errdefer allocator.free(columns);
        var initialized: usize = 0;
        errdefer {
            for (columns[0..initialized]) |*column| column.deinit(allocator);
        }
        while (initialized < count) : (initialized += 1) {
            columns[initialized] = try self.readColumn(allocator, row_group_index, initialized);
        }
        return columns;
    }
};

pub const StreamFileReader = struct {
    allocator: std.mem.Allocator,
    file_reader: *std.Io.File.Reader,
    file_size: u64,
    arena: std.heap.ArenaAllocator,
    metadata: types.FileMetaData,

    pub fn init(allocator: std.mem.Allocator, file_reader: *std.Io.File.Reader) !StreamFileReader {
        const file_size = try file_reader.getSize();
        if (file_size < 12) return error.InvalidParquetFile;

        var magic: [4]u8 = undefined;
        try file_reader.seekTo(0);
        try file_reader.interface.readSliceAll(&magic);
        if (!std.mem.eql(u8, &magic, "PAR1")) return error.InvalidParquetFile;

        var tail: [8]u8 = undefined;
        try file_reader.seekTo(file_size - 8);
        try file_reader.interface.readSliceAll(&tail);
        if (!std.mem.eql(u8, tail[4..8], "PAR1")) return error.InvalidParquetFile;
        const footer_len = std.mem.readInt(u32, tail[0..4], .little);
        if (footer_len > file_size - 12) return error.InvalidParquetFile;
        if (footer_len > max_footer_size) return error.CorruptMetadata;
        const footer_start = file_size - 8 - @as(u64, footer_len);

        const footer = try allocator.alloc(u8, footer_len);
        defer allocator.free(footer);
        try file_reader.seekTo(footer_start);
        try file_reader.interface.readSliceAll(footer);

        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const metadata = try thrift.readFileMetaData(arena.allocator(), footer);
        try validateSupported(metadata, file_size);

        return .{
            .allocator = allocator,
            .file_reader = file_reader,
            .file_size = file_size,
            .arena = arena,
            .metadata = metadata,
        };
    }

    pub fn deinit(self: *StreamFileReader) void {
        self.arena.deinit();
    }

    pub fn readColumn(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !types.OwnedColumn {
        var pages = try self.columnPageIterator(allocator, row_group_index, column_index);
        defer pages.deinit();

        var acc = ColumnAccumulator.init(pages.column.physical_type, pages.schema_col.repetition == .optional);
        errdefer acc.deinit(allocator);
        try acc.reserve(allocator, pages.row_count);

        while (try pages.next()) |page| {
            var page_column = page;
            defer page_column.deinit(allocator);
            try acc.append(allocator, page_column);
        }

        return acc.finish(allocator);
    }

    fn readDictionary(self: *StreamFileReader, allocator: std.mem.Allocator, column: types.ColumnChunkMeta, schema_col: types.Column, scratch: *PageDecodeScratch) !?types.OwnedColumn {
        const dict_offset = column.dictionary_page_offset orelse return null;
        try self.file_reader.seekTo(try checkedFileOffset(dict_offset));
        const header = try thrift.readPageHeader(&self.file_reader.interface);
        if (header.page_type != .dictionary_page) return error.CorruptPage;
        const dict_header = header.dictionary_page_header orelse return error.CorruptPage;
        if (dict_header.encoding != .plain) return error.UnsupportedEncoding;
        const page_size = try checkedPageSize(header.compressed_page_size);
        const page_data = try allocator.alloc(u8, page_size);
        defer allocator.free(page_data);
        try self.file_reader.interface.readSliceAll(page_data);
        try validatePageCrc(header, page_data);
        const count = std.math.cast(usize, dict_header.num_values) orelse return error.CorruptPage;
        const page_bytes = try preparePageBytes(allocator, column.codec, header, page_data, scratch);
        defer page_bytes.deinit(allocator);
        return try plain.decodeValues(allocator, schema_col.column_type, count, count, null, page_bytes.data);
    }

    pub fn readRowGroupColumns(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize) ![]types.OwnedColumn {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const count = self.metadata.schema.columns.len;
        const columns = try allocator.alloc(types.OwnedColumn, count);
        errdefer allocator.free(columns);
        var initialized: usize = 0;
        errdefer {
            for (columns[0..initialized]) |*column| column.deinit(allocator);
        }
        while (initialized < count) : (initialized += 1) {
            columns[initialized] = try self.readColumn(allocator, row_group_index, initialized);
        }
        return columns;
    }

    pub fn columnPageIterator(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !ColumnPageIterator {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;

        var iter: ColumnPageIterator = .{
            .reader = self,
            .allocator = allocator,
            .column = row_group.columns[column_index],
            .schema_col = self.metadata.schema.columns[column_index],
            .row_count = std.math.cast(usize, row_group.num_rows) orelse return error.CorruptMetadata,
        };
        errdefer iter.deinit();

        iter.dictionary = try self.readDictionary(allocator, iter.column, iter.schema_col, &iter.scratch);
        try self.file_reader.seekTo(try checkedFileOffset(iter.column.data_page_offset));
        return iter;
    }

    pub fn columnPageInfoIterator(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !ColumnPageInfoIterator {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;

        var iter: ColumnPageInfoIterator = .{
            .reader = self,
            .allocator = allocator,
            .column = row_group.columns[column_index],
            .row_count = std.math.cast(usize, row_group.num_rows) orelse return error.CorruptMetadata,
            .next_offset = try checkedFileOffset(row_group.columns[column_index].data_page_offset),
        };
        errdefer iter.deinit();

        if (try self.readColumnPageIndex(allocator, row_group_index, column_index)) |entries| {
            iter.page_index_entries = entries;
            iter.owns_page_index_entries = true;
        }
        return iter;
    }

    pub fn readColumnPageIndex(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !?[]types.PageIndexEntry {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        const column = row_group.columns[column_index];

        const offset_index_offset = column.offset_index_offset orelse {
            if (column.offset_index_length != null) return error.CorruptMetadata;
            return null;
        };
        const offset_index_length = column.offset_index_length orelse return error.CorruptMetadata;
        const offset_index_bytes = try self.readIndexBytes(allocator, offset_index_offset, offset_index_length);
        defer allocator.free(offset_index_bytes);

        const entries = try thrift.readOffsetIndex(allocator, offset_index_bytes);
        errdefer thrift.freePageIndexEntries(allocator, entries);
        try hydratePageIndexRowCounts(entries, row_group.num_rows, self.file_size);

        if (column.column_index_offset) |column_index_offset| {
            const column_index_length = column.column_index_length orelse return error.CorruptMetadata;
            const column_index_bytes = try self.readIndexBytes(allocator, column_index_offset, column_index_length);
            defer allocator.free(column_index_bytes);
            try thrift.readColumnIndexInto(allocator, column_index_bytes, entries);
        } else if (column.column_index_length != null) {
            return error.CorruptMetadata;
        }

        return entries;
    }

    fn readIndexBytes(self: *StreamFileReader, allocator: std.mem.Allocator, offset: i64, length: i32) ![]u8 {
        const start = try checkedFileOffset(offset);
        const len = std.math.cast(usize, length) orelse return error.CorruptMetadata;
        if (len == 0 or len > max_page_index_size) return error.CorruptMetadata;
        const end = std.math.add(u64, start, len) catch return error.CorruptMetadata;
        if (end > self.file_size) return error.CorruptMetadata;

        const bytes = try allocator.alloc(u8, len);
        errdefer allocator.free(bytes);
        try self.file_reader.seekTo(start);
        try self.file_reader.interface.readSliceAll(bytes);
        return bytes;
    }
};

pub const PageInfo = struct {
    header_offset: u64,
    data_offset: u64,
    row_start: usize,
    row_count: usize,
    page_type: types.PageType,
    encoding: types.Encoding,
    compressed_page_size: usize,
    uncompressed_page_size: usize,
    statistics: types.Statistics = .{},

    pub fn deinit(self: *PageInfo, allocator: std.mem.Allocator) void {
        thrift.freeStatistics(allocator, self.statistics);
        self.statistics = .{};
    }
};

pub const ColumnPageInfoIterator = struct {
    reader: *StreamFileReader,
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    row_count: usize,
    rows_seen: usize = 0,
    next_offset: u64,
    page_index_entries: []types.PageIndexEntry = &.{},
    page_index_position: usize = 0,
    owns_page_index_entries: bool = false,

    pub fn deinit(self: *ColumnPageInfoIterator) void {
        if (self.owns_page_index_entries) {
            thrift.freePageIndexEntries(self.allocator, self.page_index_entries);
            self.page_index_entries = &.{};
            self.owns_page_index_entries = false;
        }
    }

    pub fn next(self: *ColumnPageInfoIterator) !?PageInfo {
        if (self.rows_seen >= self.row_count) return null;
        if (self.page_index_entries.len != 0) return try self.nextFromPageIndex();

        const header_offset = self.next_offset;
        try self.reader.file_reader.seekTo(header_offset);
        const page_header = try thrift.readPageHeaderAlloc(self.allocator, &self.reader.file_reader.interface);
        const stats = pageStatistics(page_header);
        errdefer thrift.freeStatistics(self.allocator, stats);

        const page_rows = try pageRowCount(page_header);
        if (self.rows_seen + page_rows > self.row_count) return error.CorruptPage;
        const page_size = try checkedPageSize(page_header.compressed_page_size);
        const uncompressed_size = try checkedPageSize(page_header.uncompressed_page_size);
        const data_offset = self.reader.file_reader.logicalPos();
        const next_offset = std.math.add(u64, data_offset, page_size) catch return error.CorruptPage;
        try self.reader.file_reader.seekTo(next_offset);

        const info: PageInfo = .{
            .header_offset = header_offset,
            .data_offset = data_offset,
            .row_start = self.rows_seen,
            .row_count = page_rows,
            .page_type = page_header.page_type,
            .encoding = try pageEncoding(page_header),
            .compressed_page_size = page_size,
            .uncompressed_page_size = uncompressed_size,
            .statistics = stats,
        };
        self.rows_seen += page_rows;
        self.next_offset = next_offset;
        return info;
    }

    fn nextFromPageIndex(self: *ColumnPageInfoIterator) !?PageInfo {
        if (self.page_index_position >= self.page_index_entries.len) return error.CorruptMetadata;

        const entry = self.page_index_entries[self.page_index_position];
        const header_offset = try checkedFileOffset(entry.offset);
        try self.reader.file_reader.seekTo(header_offset);
        const page_header = try thrift.readPageHeaderAlloc(self.allocator, &self.reader.file_reader.interface);
        var stats = pageStatistics(page_header);
        var header_stats_owned = true;
        errdefer if (header_stats_owned) thrift.freeStatistics(self.allocator, stats);

        const page_rows = std.math.cast(usize, entry.row_count) orelse return error.CorruptPage;
        if (self.rows_seen + page_rows > self.row_count) return error.CorruptPage;
        const page_body_size = try checkedPageSize(page_header.compressed_page_size);
        const uncompressed_size = try checkedPageSize(page_header.uncompressed_page_size);
        const data_offset = self.reader.file_reader.logicalPos();
        const page_end = std.math.add(u64, data_offset, page_body_size) catch return error.CorruptPage;
        const indexed_page_end = std.math.add(u64, header_offset, @as(u64, @intCast(entry.compressed_page_size))) catch return error.CorruptPage;
        if (page_end != indexed_page_end) return error.CorruptPage;
        try self.reader.file_reader.seekTo(page_end);

        try overlayStatistics(self.allocator, &stats, entry.statistics);
        header_stats_owned = false;
        errdefer thrift.freeStatistics(self.allocator, stats);

        const info: PageInfo = .{
            .header_offset = header_offset,
            .data_offset = data_offset,
            .row_start = self.rows_seen,
            .row_count = page_rows,
            .page_type = page_header.page_type,
            .encoding = try pageEncoding(page_header),
            .compressed_page_size = page_body_size,
            .uncompressed_page_size = uncompressed_size,
            .statistics = stats,
        };
        self.rows_seen += page_rows;
        self.next_offset = page_end;
        self.page_index_position += 1;
        return info;
    }
};

pub const ColumnPageIterator = struct {
    reader: *StreamFileReader,
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    schema_col: types.Column,
    row_count: usize,
    rows_seen: usize = 0,
    dictionary: ?types.OwnedColumn = null,
    scratch: PageDecodeScratch = .{},

    pub fn deinit(self: *ColumnPageIterator) void {
        if (self.dictionary) |*dict| dict.deinit(self.allocator);
        self.scratch.deinit(self.allocator);
    }

    pub fn next(self: *ColumnPageIterator) !?types.OwnedColumn {
        if (self.rows_seen >= self.row_count) return null;

        const page_header = try thrift.readPageHeader(&self.reader.file_reader.interface);
        const page_rows = try pageRowCount(page_header);
        if (self.rows_seen + page_rows > self.row_count) return error.CorruptPage;
        const page_size = try checkedPageSize(page_header.compressed_page_size);
        const page_data = try self.allocator.alloc(u8, page_size);
        defer self.allocator.free(page_data);
        try self.reader.file_reader.interface.readSliceAll(page_data);
        try validatePageCrc(page_header, page_data);

        const page_column = try decodeColumnPage(
            self.allocator,
            self.column,
            self.schema_col,
            page_header,
            page_data,
            if (self.dictionary) |*dict| dict else null,
            &self.scratch,
        );
        self.rows_seen += page_rows;
        return page_column;
    }
};

pub fn readFileFromMemory(allocator: std.mem.Allocator, bytes: []const u8) !ParsedFile {
    if (bytes.len < 12) return error.InvalidParquetFile;
    if (!std.mem.eql(u8, bytes[0..4], "PAR1")) return error.InvalidParquetFile;
    if (!std.mem.eql(u8, bytes[bytes.len - 4 ..], "PAR1")) return error.InvalidParquetFile;

    const footer_len = std.mem.readInt(u32, bytes[bytes.len - 8 ..][0..4], .little);
    if (footer_len > bytes.len - 12) return error.InvalidParquetFile;
    if (footer_len > max_footer_size) return error.CorruptMetadata;
    const footer_start = bytes.len - 8 - @as(usize, footer_len);
    if (footer_start < 4 or footer_start > bytes.len - 8) return error.InvalidParquetFile;

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const metadata = try thrift.readFileMetaData(arena.allocator(), bytes[footer_start .. bytes.len - 8]);
    try validateSupported(metadata, bytes.len);
    return .{ .arena = arena, .metadata = metadata, .bytes = bytes };
}

fn decodeColumnPage(
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    schema_col: types.Column,
    page_header: types.PageHeader,
    page_data: []const u8,
    dictionary: ?*const types.OwnedColumn,
    scratch: *PageDecodeScratch,
) !types.OwnedColumn {
    if (page_header.page_type == .data_page_v2) {
        return decodeColumnPageV2(allocator, column, schema_col, page_header, page_data, dictionary, scratch);
    }
    if (page_header.page_type != .data_page) return error.UnsupportedPageType;
    const page_bytes = try preparePageBytes(allocator, column.codec, page_header, page_data, scratch);
    defer page_bytes.deinit(allocator);
    const data_header = page_header.data_page_header orelse return error.CorruptPage;
    const dictionary_encoded = data_header.encoding == .rle_dictionary or data_header.encoding == .plain_dictionary;
    const rle_boolean = data_header.encoding == .rle and schema_col.column_type.physical == .boolean;
    const byte_stream_split = data_header.encoding == .byte_stream_split;
    const delta_binary_packed = data_header.encoding == .delta_binary_packed;
    const delta_length_byte_array = data_header.encoding == .delta_length_byte_array;
    const delta_byte_array = data_header.encoding == .delta_byte_array;
    if (data_header.encoding != .plain and !dictionary_encoded and !rle_boolean and !byte_stream_split and !delta_binary_packed and !delta_length_byte_array and !delta_byte_array) return error.UnsupportedEncoding;

    var data = page_bytes.data;
    const row_count = std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage;

    var validity: ?[]bool = null;
    var non_null_count = row_count;
    if (schema_col.repetition == .optional) {
        const decoded = try plain.decodeDefinitionLevels(allocator, data, row_count);
        validity = decoded.levels;
        data = data[decoded.consumed..];
        non_null_count = 0;
        for (validity.?) |valid| {
            if (valid) non_null_count += 1;
        }
    }

    errdefer if (validity) |v| allocator.free(v);
    if (rle_boolean) {
        return decodeRleBooleans(allocator, row_count, non_null_count, validity, data);
    }
    if (!dictionary_encoded) {
        if (byte_stream_split) {
            return plain.decodeByteStreamSplitValues(allocator, schema_col.column_type, row_count, non_null_count, validity, data);
        }
        if (delta_binary_packed) {
            return plain.decodeDeltaBinaryPackedValues(allocator, schema_col.column_type, row_count, non_null_count, validity, data);
        }
        if (delta_length_byte_array) {
            return plain.decodeDeltaLengthByteArrayValues(allocator, schema_col.column_type, row_count, non_null_count, validity, data);
        }
        if (delta_byte_array) {
            return plain.decodeDeltaByteArrayValues(allocator, schema_col.column_type, row_count, non_null_count, validity, data);
        }
        return plain.decodeValues(allocator, schema_col.column_type, row_count, non_null_count, validity, data);
    }

    const dict = dictionary orelse return error.CorruptPage;
    if (data.len == 0) return error.CorruptPage;
    const bit_width = data[0];
    const indexes = try plain.decodeRleBitPackedUint32(allocator, data[1..], bit_width, non_null_count);
    defer allocator.free(indexes);
    return plain.materializeDictionary(allocator, column.physical_type, dict, indexes, validity);
}

fn decodeColumnPageV2(
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    schema_col: types.Column,
    page_header: types.PageHeader,
    page_data: []const u8,
    dictionary: ?*const types.OwnedColumn,
    scratch: *PageDecodeScratch,
) !types.OwnedColumn {
    const data_header = page_header.data_page_header_v2 orelse return error.CorruptPage;
    const dictionary_encoded = data_header.encoding == .rle_dictionary or data_header.encoding == .plain_dictionary;
    const rle_boolean = data_header.encoding == .rle and schema_col.column_type.physical == .boolean;
    const byte_stream_split = data_header.encoding == .byte_stream_split;
    const delta_binary_packed = data_header.encoding == .delta_binary_packed;
    const delta_length_byte_array = data_header.encoding == .delta_length_byte_array;
    const delta_byte_array = data_header.encoding == .delta_byte_array;
    if (data_header.encoding != .plain and !dictionary_encoded and !rle_boolean and !byte_stream_split and !delta_binary_packed and !delta_length_byte_array and !delta_byte_array) return error.UnsupportedEncoding;
    if (data_header.repetition_levels_byte_length != 0) return error.UnsupportedNestedSchema;

    const row_count = std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage;
    const num_rows = std.math.cast(usize, data_header.num_rows) orelse return error.CorruptPage;
    if (num_rows != row_count) return error.UnsupportedNestedSchema;
    const num_nulls = std.math.cast(usize, data_header.num_nulls) orelse return error.CorruptPage;
    if (num_nulls > row_count) return error.CorruptPage;

    const def_len = try checkedPageSize(data_header.definition_levels_byte_length);
    const rep_len = try checkedPageSize(data_header.repetition_levels_byte_length);
    const levels_len = std.math.add(usize, rep_len, def_len) catch return error.CorruptPage;
    if (levels_len > page_data.len) return error.CorruptPage;

    const def_start = rep_len;
    const value_start = levels_len;
    var validity: ?[]bool = null;
    if (schema_col.repetition == .optional) {
        validity = try plain.decodeDefinitionLevelsBody(allocator, page_data[def_start..value_start], row_count);
    } else if (def_len != 0 or num_nulls != 0) {
        return error.CorruptPage;
    }

    var non_null_count = row_count - num_nulls;
    if (validity) |v| {
        var counted: usize = 0;
        for (v) |valid| {
            if (valid) counted += 1;
        }
        if (counted != non_null_count) return error.CorruptPage;
        non_null_count = counted;
    }

    errdefer if (validity) |v| allocator.free(v);
    const values_compressed_size = page_data.len - value_start;
    const total_uncompressed_size = try checkedPageSize(page_header.uncompressed_page_size);
    if (levels_len > total_uncompressed_size) return error.CorruptPage;
    const values_uncompressed_size = total_uncompressed_size - levels_len;
    const value_bytes = try prepareValueBytes(
        allocator,
        column.codec,
        data_header.is_compressed,
        values_uncompressed_size,
        values_compressed_size,
        page_data[value_start..],
        scratch,
    );
    defer value_bytes.deinit(allocator);

    if (rle_boolean) {
        return decodeRleBooleans(allocator, row_count, non_null_count, validity, value_bytes.data);
    }
    if (!dictionary_encoded) {
        if (byte_stream_split) {
            return plain.decodeByteStreamSplitValues(allocator, schema_col.column_type, row_count, non_null_count, validity, value_bytes.data);
        }
        if (delta_binary_packed) {
            return plain.decodeDeltaBinaryPackedValues(allocator, schema_col.column_type, row_count, non_null_count, validity, value_bytes.data);
        }
        if (delta_length_byte_array) {
            return plain.decodeDeltaLengthByteArrayValues(allocator, schema_col.column_type, row_count, non_null_count, validity, value_bytes.data);
        }
        if (delta_byte_array) {
            return plain.decodeDeltaByteArrayValues(allocator, schema_col.column_type, row_count, non_null_count, validity, value_bytes.data);
        }
        return plain.decodeValues(allocator, schema_col.column_type, row_count, non_null_count, validity, value_bytes.data);
    }

    const dict = dictionary orelse return error.CorruptPage;
    if (value_bytes.data.len == 0) return error.CorruptPage;
    const bit_width = value_bytes.data[0];
    const indexes = try plain.decodeRleBitPackedUint32(allocator, value_bytes.data[1..], bit_width, non_null_count);
    defer allocator.free(indexes);
    return plain.materializeDictionary(allocator, column.physical_type, dict, indexes, validity);
}

fn decodeRleBooleans(allocator: std.mem.Allocator, row_count: usize, non_null_count: usize, validity: ?[]bool, data: []const u8) !types.OwnedColumn {
    _ = row_count;
    if (data.len >= 4) {
        const len = std.mem.readInt(u32, data[0..4], .little);
        const end = std.math.add(usize, 4, @as(usize, len)) catch return error.CorruptPage;
        if (end == data.len) {
            return try decodeRleBooleanPayload(allocator, non_null_count, validity, data[4..end]);
        }
    }
    if (data.len > 0 and data[0] == 1) {
        if (decodeRleBooleanPayload(allocator, non_null_count, validity, data[1..])) |column| {
            return column;
        } else |_| {}
    }
    const plain_len = (non_null_count + 7) / 8;
    if (data.len == plain_len) {
        return plain.decodeValues(allocator, .{ .physical = .boolean }, non_null_count, non_null_count, validity, data);
    }
    return decodeRleBooleanPayload(allocator, non_null_count, validity, data) catch |err| {
        return err;
    };
}

fn decodeRleBooleanPayload(allocator: std.mem.Allocator, non_null_count: usize, validity: ?[]bool, data: []const u8) !types.OwnedColumn {
    const indexes = try plain.decodeRleBitPackedUint32(allocator, data, 1, non_null_count);
    defer allocator.free(indexes);

    const values = try allocator.alloc(bool, non_null_count);
    errdefer allocator.free(values);
    for (indexes, values) |index, *value| {
        if (index > 1) return error.CorruptPage;
        value.* = index != 0;
    }
    return .{ .boolean = .{ .values = values, .validity = validity } };
}

fn pageRowCount(page_header: types.PageHeader) !usize {
    switch (page_header.page_type) {
        .data_page => {
            const data_header = page_header.data_page_header orelse return error.CorruptPage;
            return std.math.cast(usize, data_header.num_values) orelse error.CorruptPage;
        },
        .data_page_v2 => {
            const data_header = page_header.data_page_header_v2 orelse return error.CorruptPage;
            return std.math.cast(usize, data_header.num_values) orelse error.CorruptPage;
        },
        else => return error.UnsupportedPageType,
    }
}

fn pageEncoding(page_header: types.PageHeader) !types.Encoding {
    return switch (page_header.page_type) {
        .data_page => (page_header.data_page_header orelse return error.CorruptPage).encoding,
        .data_page_v2 => (page_header.data_page_header_v2 orelse return error.CorruptPage).encoding,
        else => error.UnsupportedPageType,
    };
}

fn pageStatistics(page_header: types.PageHeader) types.Statistics {
    return switch (page_header.page_type) {
        .data_page => if (page_header.data_page_header) |dp| dp.statistics else .{},
        .data_page_v2 => if (page_header.data_page_header_v2) |dp| dp.statistics else .{},
        else => .{},
    };
}

fn overlayStatistics(allocator: std.mem.Allocator, target: *types.Statistics, overlay: types.Statistics) !void {
    if (overlay.null_count) |null_count| target.null_count = null_count;
    if (overlay.min_value) |value| try replaceStatBytes(allocator, &target.min_value, value);
    if (overlay.max_value) |value| try replaceStatBytes(allocator, &target.max_value, value);
}

fn replaceStatBytes(allocator: std.mem.Allocator, target: *?[]const u8, value: []const u8) !void {
    const copy = try allocator.dupe(u8, value);
    if (target.*) |old| allocator.free(old);
    target.* = copy;
}

fn hydratePageIndexRowCounts(entries: []types.PageIndexEntry, row_group_rows: i64, file_size: u64) !void {
    if (row_group_rows < 0) return error.CorruptMetadata;
    if (entries.len == 0) {
        if (row_group_rows == 0) return;
        return error.CorruptMetadata;
    }

    for (entries, 0..) |*entry, idx| {
        if (entry.first_row_index < 0 or entry.first_row_index >= row_group_rows) return error.CorruptMetadata;
        const offset = std.math.cast(u64, entry.offset) orelse return error.CorruptMetadata;
        const compressed_size = std.math.cast(u64, entry.compressed_page_size) orelse return error.CorruptMetadata;
        const page_end = std.math.add(u64, offset, compressed_size) catch return error.CorruptMetadata;
        if (page_end > file_size) return error.CorruptMetadata;

        const next_first_row = if (idx + 1 < entries.len) blk: {
            const next = entries[idx + 1].first_row_index;
            if (next <= entry.first_row_index) return error.CorruptMetadata;
            if (entries[idx + 1].offset <= entry.offset) return error.CorruptMetadata;
            break :blk next;
        } else row_group_rows;
        const row_count = next_first_row - entry.first_row_index;
        if (row_count <= 0) return error.CorruptMetadata;
        entry.row_count = row_count;
    }
}

const ByteRange = struct {
    start: usize,
    len: usize,
};

const ColumnAccumulator = union(enum) {
    boolean: struct { values: std.ArrayList(bool) = .empty, validity: std.ArrayList(bool) = .empty, optional: bool },
    int32: struct { values: std.ArrayList(i32) = .empty, validity: std.ArrayList(bool) = .empty, optional: bool },
    int64: struct { values: std.ArrayList(i64) = .empty, validity: std.ArrayList(bool) = .empty, optional: bool },
    float: struct { values: std.ArrayList(f32) = .empty, validity: std.ArrayList(bool) = .empty, optional: bool },
    double: struct { values: std.ArrayList(f64) = .empty, validity: std.ArrayList(bool) = .empty, optional: bool },
    byte_array: struct {
        values: std.ArrayList(ByteRange) = .empty,
        bytes: std.ArrayList(u8) = .empty,
        validity: std.ArrayList(bool) = .empty,
        optional: bool,
    },
    fixed_len_byte_array: struct {
        values: std.ArrayList(ByteRange) = .empty,
        bytes: std.ArrayList(u8) = .empty,
        validity: std.ArrayList(bool) = .empty,
        optional: bool,
    },

    fn init(physical_type: types.Type, optional: bool) ColumnAccumulator {
        return switch (physical_type) {
            .boolean => .{ .boolean = .{ .optional = optional } },
            .int32 => .{ .int32 = .{ .optional = optional } },
            .int64 => .{ .int64 = .{ .optional = optional } },
            .float => .{ .float = .{ .optional = optional } },
            .double => .{ .double = .{ .optional = optional } },
            .byte_array => .{ .byte_array = .{ .optional = optional } },
            .fixed_len_byte_array => .{ .fixed_len_byte_array = .{ .optional = optional } },
            else => unreachable,
        };
    }

    fn deinit(self: *ColumnAccumulator, allocator: std.mem.Allocator) void {
        switch (self.*) {
            inline .boolean, .int32, .int64, .float, .double => |*acc| {
                acc.values.deinit(allocator);
                acc.validity.deinit(allocator);
            },
            .byte_array => |*acc| {
                acc.values.deinit(allocator);
                acc.bytes.deinit(allocator);
                acc.validity.deinit(allocator);
            },
            .fixed_len_byte_array => |*acc| {
                acc.values.deinit(allocator);
                acc.bytes.deinit(allocator);
                acc.validity.deinit(allocator);
            },
        }
    }

    fn reserve(self: *ColumnAccumulator, allocator: std.mem.Allocator, row_count: usize) !void {
        switch (self.*) {
            inline .boolean, .int32, .int64, .float, .double => |*acc| {
                try acc.values.ensureTotalCapacity(allocator, row_count);
                if (acc.optional) try acc.validity.ensureTotalCapacity(allocator, row_count);
            },
            .byte_array => |*acc| {
                try acc.values.ensureTotalCapacity(allocator, row_count);
                if (acc.optional) try acc.validity.ensureTotalCapacity(allocator, row_count);
            },
            .fixed_len_byte_array => |*acc| {
                try acc.values.ensureTotalCapacity(allocator, row_count);
                if (acc.optional) try acc.validity.ensureTotalCapacity(allocator, row_count);
            },
        }
    }

    fn append(self: *ColumnAccumulator, allocator: std.mem.Allocator, column: types.OwnedColumn) !void {
        switch (self.*) {
            .boolean => |*acc| switch (column) {
                .boolean => |page| {
                    try acc.values.appendSlice(allocator, page.values);
                    try appendValidity(allocator, &acc.validity, acc.optional, page.validity);
                },
                else => return error.CorruptPage,
            },
            .int32 => |*acc| switch (column) {
                .int32 => |page| {
                    try acc.values.appendSlice(allocator, page.values);
                    try appendValidity(allocator, &acc.validity, acc.optional, page.validity);
                },
                else => return error.CorruptPage,
            },
            .int64 => |*acc| switch (column) {
                .int64 => |page| {
                    try acc.values.appendSlice(allocator, page.values);
                    try appendValidity(allocator, &acc.validity, acc.optional, page.validity);
                },
                else => return error.CorruptPage,
            },
            .float => |*acc| switch (column) {
                .float => |page| {
                    try acc.values.appendSlice(allocator, page.values);
                    try appendValidity(allocator, &acc.validity, acc.optional, page.validity);
                },
                else => return error.CorruptPage,
            },
            .double => |*acc| switch (column) {
                .double => |page| {
                    try acc.values.appendSlice(allocator, page.values);
                    try appendValidity(allocator, &acc.validity, acc.optional, page.validity);
                },
                else => return error.CorruptPage,
            },
            .byte_array => |*acc| switch (column) {
                .byte_array => |page| {
                    for (page.values) |value| {
                        const start = acc.bytes.items.len;
                        try acc.bytes.appendSlice(allocator, value);
                        try acc.values.append(allocator, .{ .start = start, .len = value.len });
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, page.validity);
                },
                else => return error.CorruptPage,
            },
            .fixed_len_byte_array => |*acc| switch (column) {
                .fixed_len_byte_array => |page| {
                    for (page.values) |value| {
                        const start = acc.bytes.items.len;
                        try acc.bytes.appendSlice(allocator, value);
                        try acc.values.append(allocator, .{ .start = start, .len = value.len });
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, page.validity);
                },
                else => return error.CorruptPage,
            },
        }
    }

    fn finish(self: *ColumnAccumulator, allocator: std.mem.Allocator) !types.OwnedColumn {
        return switch (self.*) {
            .boolean => |*acc| .{ .boolean = .{
                .values = try acc.values.toOwnedSlice(allocator),
                .validity = try finishValidity(allocator, &acc.validity, acc.optional),
            } },
            .int32 => |*acc| .{ .int32 = .{
                .values = try acc.values.toOwnedSlice(allocator),
                .validity = try finishValidity(allocator, &acc.validity, acc.optional),
            } },
            .int64 => |*acc| .{ .int64 = .{
                .values = try acc.values.toOwnedSlice(allocator),
                .validity = try finishValidity(allocator, &acc.validity, acc.optional),
            } },
            .float => |*acc| .{ .float = .{
                .values = try acc.values.toOwnedSlice(allocator),
                .validity = try finishValidity(allocator, &acc.validity, acc.optional),
            } },
            .double => |*acc| .{ .double = .{
                .values = try acc.values.toOwnedSlice(allocator),
                .validity = try finishValidity(allocator, &acc.validity, acc.optional),
            } },
            .byte_array => |*acc| blk: {
                const ranges = try acc.values.toOwnedSlice(allocator);
                defer allocator.free(ranges);
                const bytes = try acc.bytes.toOwnedSlice(allocator);
                errdefer allocator.free(bytes);
                const values = try allocator.alloc([]const u8, ranges.len);
                errdefer allocator.free(values);
                for (ranges, values) |range, *value| {
                    value.* = bytes[range.start..][0..range.len];
                }
                break :blk .{ .byte_array = .{
                    .values = values,
                    .data = bytes,
                    .validity = try finishValidity(allocator, &acc.validity, acc.optional),
                } };
            },
            .fixed_len_byte_array => |*acc| blk: {
                const ranges = try acc.values.toOwnedSlice(allocator);
                defer allocator.free(ranges);
                const bytes = try acc.bytes.toOwnedSlice(allocator);
                errdefer allocator.free(bytes);
                const values = try allocator.alloc([]const u8, ranges.len);
                errdefer allocator.free(values);
                for (ranges, values) |range, *value| {
                    value.* = bytes[range.start..][0..range.len];
                }
                break :blk .{ .fixed_len_byte_array = .{
                    .values = values,
                    .data = bytes,
                    .validity = try finishValidity(allocator, &acc.validity, acc.optional),
                } };
            },
        };
    }
};

fn appendValidity(allocator: std.mem.Allocator, dest: *std.ArrayList(bool), optional: bool, validity: ?[]bool) !void {
    if (!optional) return;
    try dest.appendSlice(allocator, validity orelse return error.CorruptPage);
}

fn finishValidity(allocator: std.mem.Allocator, list: *std.ArrayList(bool), optional: bool) !?[]bool {
    if (!optional) return null;
    return try list.toOwnedSlice(allocator);
}

fn validateSupported(metadata: types.FileMetaData, file_size: u64) !void {
    if (metadata.version != 1 and metadata.version != 2) return error.CorruptMetadata;
    if (metadata.num_rows < 0) return error.CorruptMetadata;
    for (metadata.schema.columns) |column| {
        if (column.repetition == .repeated) return error.UnsupportedNestedSchema;
        switch (column.column_type.physical) {
            .boolean, .int32, .int64, .float, .double, .byte_array => {},
            .fixed_len_byte_array => {
                _ = try types.physicalTypeWidth(column.column_type.physical, column.column_type.type_length);
            },
            else => return error.UnsupportedType,
        }
        try types.validateDecimalType(column.column_type);
    }
    for (metadata.row_groups) |row_group| {
        if (row_group.num_rows < 0) return error.CorruptMetadata;
        if (row_group.columns.len != metadata.schema.columns.len) return error.CorruptMetadata;
        for (row_group.columns, metadata.schema.columns) |column, schema_col| {
            if (column.physical_type != schema_col.column_type.physical) return error.CorruptMetadata;
            if (column.num_values < 0 or column.total_uncompressed_size < 0 or column.total_compressed_size < 0) return error.CorruptMetadata;
            if (column.data_page_offset < 0) return error.CorruptMetadata;
            const data_page_offset = std.math.cast(u64, column.data_page_offset) orelse return error.CorruptMetadata;
            if (data_page_offset >= file_size) return error.CorruptMetadata;
            if (column.dictionary_page_offset) |offset| {
                if (offset < 0) return error.CorruptMetadata;
                const dictionary_page_offset = std.math.cast(u64, offset) orelse return error.CorruptMetadata;
                if (dictionary_page_offset >= file_size) return error.CorruptMetadata;
            }
            try validateIndexReference(file_size, column.offset_index_offset, column.offset_index_length);
            try validateIndexReference(file_size, column.column_index_offset, column.column_index_length);
            switch (column.codec) {
                .uncompressed, .snappy, .zstd => {},
                else => return error.UnsupportedCompression,
            }
            var has_supported_value_encoding = false;
            for (column.encodings) |encoding| {
                switch (encoding) {
                    .plain, .byte_stream_split, .delta_binary_packed, .delta_length_byte_array, .delta_byte_array => has_supported_value_encoding = true,
                    .rle, .rle_dictionary, .plain_dictionary => {},
                    else => return error.UnsupportedEncoding,
                }
            }
            if (!has_supported_value_encoding and schema_col.column_type.physical != .boolean) return error.UnsupportedEncoding;
        }
    }
}

fn validateIndexReference(file_size: u64, maybe_offset: ?i64, maybe_length: ?i32) !void {
    if (maybe_offset == null and maybe_length == null) return;
    const offset_i64 = maybe_offset orelse return error.CorruptMetadata;
    const length_i32 = maybe_length orelse return error.CorruptMetadata;
    const offset = std.math.cast(u64, offset_i64) orelse return error.CorruptMetadata;
    const length = std.math.cast(u64, length_i32) orelse return error.CorruptMetadata;
    if (length == 0 or length > max_page_index_size) return error.CorruptMetadata;
    const end = std.math.add(u64, offset, length) catch return error.CorruptMetadata;
    if (end > file_size) return error.CorruptMetadata;
}

const PageBytes = struct {
    data: []const u8,
    owned: ?[]u8 = null,

    fn deinit(self: PageBytes, allocator: std.mem.Allocator) void {
        if (self.owned) |bytes| allocator.free(bytes);
    }
};

const PageDecodeScratch = struct {
    zstd_buffer: std.ArrayList(u8) = .empty,

    fn deinit(self: *PageDecodeScratch, allocator: std.mem.Allocator) void {
        self.zstd_buffer.deinit(allocator);
    }

    fn zstdScratch(self: *PageDecodeScratch, allocator: std.mem.Allocator, uncompressed_size: usize) ![]u8 {
        const window_len = @max(uncompressed_size, 1);
        const scratch_len = std.math.add(usize, window_len, std.compress.zstd.block_size_max) catch return error.CorruptPage;
        try self.zstd_buffer.ensureTotalCapacity(allocator, scratch_len);
        return self.zstd_buffer.allocatedSlice()[0..scratch_len];
    }
};

fn preparePageBytes(allocator: std.mem.Allocator, codec: types.CompressionCodec, header: types.PageHeader, compressed: []const u8, scratch: *PageDecodeScratch) !PageBytes {
    const compressed_size = try checkedPageSize(header.compressed_page_size);
    const uncompressed_size = try checkedPageSize(header.uncompressed_page_size);
    if (compressed.len != compressed_size) return error.CorruptPage;
    return prepareValueBytes(allocator, codec, true, uncompressed_size, compressed_size, compressed, scratch);
}

fn validatePageCrc(header: types.PageHeader, page_data: []const u8) !void {
    const expected = header.crc orelse return;
    const actual: i32 = @bitCast(std.hash.Crc32.hash(page_data));
    if (actual != expected) return error.CorruptPage;
}

fn prepareValueBytes(
    allocator: std.mem.Allocator,
    codec: types.CompressionCodec,
    is_compressed: bool,
    uncompressed_size: usize,
    compressed_size: usize,
    compressed: []const u8,
    scratch: *PageDecodeScratch,
) !PageBytes {
    if (compressed.len != compressed_size) return error.CorruptPage;
    if (!is_compressed) {
        if (compressed_size != uncompressed_size) return error.CorruptPage;
        return .{ .data = compressed };
    }
    switch (codec) {
        .uncompressed => {
            if (compressed_size != uncompressed_size) return error.CorruptPage;
            return .{ .data = compressed };
        },
        .snappy => {
            const out = try allocator.alloc(u8, uncompressed_size);
            errdefer allocator.free(out);
            try snappy.decompress(compressed, out);
            return .{ .data = out, .owned = out };
        },
        .zstd => {
            const out = try allocator.alloc(u8, uncompressed_size);
            errdefer allocator.free(out);
            try zstd.decompressWithScratch(compressed, out, try scratch.zstdScratch(allocator, uncompressed_size), @max(uncompressed_size, 1));
            return .{ .data = out, .owned = out };
        },
        else => return error.UnsupportedCompression,
    }
}

fn checkedFileOffset(offset: i64) !u64 {
    return std.math.cast(u64, offset) orelse error.CorruptMetadata;
}

fn checkedPageSize(size: i32) !usize {
    const value = std.math.cast(usize, size) orelse return error.CorruptPage;
    if (value > max_page_size) return error.CorruptPage;
    return value;
}

test "reader decodes writer output" {
    const testing = std.testing;
    const writer_mod = @import("writer.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = writer_mod.StreamWriter.init(testing.allocator, &out.writer, schema);
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 10, 11, 12 };
    const labels = [_][]const u8{ "a", "c" };
    const validity = [_]bool{ true, false, true };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .byte_array = .{ .values = labels[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(3, batch[0..]);
    try w.finish();

    var parsed = try readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    var id_col = try parsed.readColumn(testing.allocator, 0, 0);
    defer id_col.deinit(testing.allocator);
    try testing.expectEqualSlices(i64, ids[0..], id_col.int64.values);

    var label_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer label_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], label_col.byte_array.validity.?);
    try testing.expectEqualStrings("a", label_col.byte_array.values[0]);
    try testing.expectEqualStrings("c", label_col.byte_array.values[1]);
}

test "reader rejects page checksum mismatch" {
    const testing = std.testing;
    const writer_mod = @import("writer.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = writer_mod.StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{ .page_checksum = true });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 10, 11, 12 };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    const bytes = try testing.allocator.dupe(u8, out.written());
    defer testing.allocator.free(bytes);

    var parsed = try readFileFromMemory(testing.allocator, bytes);
    const data_page_offset = @as(usize, @intCast(parsed.metadata.row_groups[0].columns[0].data_page_offset));
    parsed.deinit();

    var fixed = std.Io.Reader.fixed(bytes[data_page_offset..]);
    const header = try thrift.readPageHeader(&fixed);
    try testing.expect(header.crc != null);
    const data_start = data_page_offset + fixed.seek;
    try testing.expect(data_start < bytes.len);
    bytes[data_start] ^= 0xff;

    var corrupted = try readFileFromMemory(testing.allocator, bytes);
    defer corrupted.deinit();
    try testing.expectError(error.CorruptPage, corrupted.readColumn(testing.allocator, 0, 0));
}
