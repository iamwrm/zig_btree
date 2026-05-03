const std = @import("std");
const types = @import("types.zig");
const thrift = @import("thrift.zig");
const plain = @import("plain.zig");
const snappy = @import("snappy.zig");
const gzip = @import("gzip.zig");
const zstd = @import("zstd.zig");

const page_encodings = [_]types.Encoding{ .plain, .rle };
const byte_stream_split_page_encodings = [_]types.Encoding{ .byte_stream_split, .rle };
const delta_binary_page_encodings = [_]types.Encoding{ .delta_binary_packed, .rle };
const delta_length_page_encodings = [_]types.Encoding{ .delta_length_byte_array, .rle };
const delta_byte_array_page_encodings = [_]types.Encoding{ .delta_byte_array, .rle };
const dictionary_page_encodings = [_]types.Encoding{ .plain, .rle, .rle_dictionary };

pub const DataPageVersion = enum {
    v1,
    v2,
};

pub const Options = struct {
    compression: types.CompressionCodec = .uncompressed,
    max_page_rows: usize = 64 * 1024,
    use_dictionary: bool = true,
    data_page_version: DataPageVersion = .v1,
    page_checksum: bool = false,
    use_byte_stream_split: bool = false,
    use_delta_binary_packed: bool = false,
    use_delta_length_byte_array: bool = false,
    use_delta_byte_array: bool = false,
};

pub const StreamWriter = struct {
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    schema: types.Schema,
    options: Options = .{},
    row_groups: std.ArrayList(types.RowGroup) = .empty,
    offset: u64 = 0,
    started: bool = false,
    finished: bool = false,

    pub fn init(allocator: std.mem.Allocator, output: *std.Io.Writer, schema: types.Schema) StreamWriter {
        return .{
            .allocator = allocator,
            .output = output,
            .schema = schema,
        };
    }

    pub fn initOptions(allocator: std.mem.Allocator, output: *std.Io.Writer, schema: types.Schema, options: Options) StreamWriter {
        return .{
            .allocator = allocator,
            .output = output,
            .schema = schema,
            .options = options,
        };
    }

    pub fn deinit(self: *StreamWriter) void {
        for (self.row_groups.items) |rg| {
            for (rg.columns) |column| self.freeColumnMeta(column);
            self.allocator.free(rg.columns);
        }
        self.row_groups.deinit(self.allocator);
    }

    pub fn start(self: *StreamWriter) !void {
        if (self.started) return error.InvalidParquetFile;
        try validateSchema(self.schema);
        try validateOptions(self.options);
        try self.writeAll("PAR1");
        self.started = true;
    }

    pub fn writeRowGroup(self: *StreamWriter, row_count: usize, columns: []const types.ColumnData) !void {
        if (!self.started or self.finished) return error.InvalidParquetFile;
        if (columns.len != self.schema.columns.len) return error.InvalidColumnData;

        const metas = try self.allocator.alloc(types.ColumnChunkMeta, columns.len);
        var metas_len: usize = 0;
        errdefer {
            for (metas[0..metas_len]) |meta| self.freeColumnMeta(meta);
            self.allocator.free(metas);
        }

        var total_uncompressed: i64 = 0;
        var total_compressed: i64 = 0;
        for (columns, self.schema.columns, 0..) |column_data, column, idx| {
            try column_data.validate(column, row_count);
            metas[idx] = try self.writeColumnChunk(row_count, column, column_data);
            metas_len += 1;
            total_uncompressed = std.math.add(i64, total_uncompressed, metas[idx].total_uncompressed_size) catch return error.RowCountOverflow;
            total_compressed = std.math.add(i64, total_compressed, metas[idx].total_compressed_size) catch return error.RowCountOverflow;
        }

        try self.row_groups.append(self.allocator, .{
            .columns = metas,
            .total_byte_size = total_uncompressed,
            .total_compressed_size = total_compressed,
            .num_rows = try types.intCastI64(row_count),
        });
    }

    pub fn finish(self: *StreamWriter) !void {
        if (!self.started or self.finished) return error.InvalidParquetFile;

        try self.writePageIndexes();

        var total_rows: i64 = 0;
        for (self.row_groups.items) |rg| {
            total_rows = std.math.add(i64, total_rows, rg.num_rows) catch return error.RowCountOverflow;
        }

        var footer: std.Io.Writer.Allocating = .init(self.allocator);
        defer footer.deinit();
        try thrift.writeFileMetaData(&footer.writer, .{
            .schema = self.schema,
            .num_rows = total_rows,
            .row_groups = self.row_groups.items,
            .created_by = "zig-parquet version 0.1",
        });
        const footer_bytes = footer.written();
        try self.writeAll(footer_bytes);
        try self.output.writeInt(u32, std.math.cast(u32, footer_bytes.len) orelse return error.RowCountOverflow, .little);
        self.offset += 4;
        try self.writeAll("PAR1");
        try self.output.flush();
        self.finished = true;
    }

    fn writeColumnChunk(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnData) !types.ColumnChunkMeta {
        const null_count = if (column.repetition == .optional)
            try countNulls(data.validity().?)
        else
            0;
        const column_stats = try self.buildStatistics(column, data, null_count);
        errdefer self.freeStatistics(column_stats);

        var total_uncompressed: i64 = 0;
        var total_compressed: i64 = 0;
        var page_entries: std.ArrayList(types.PageIndexEntry) = .empty;
        defer page_entries.deinit(self.allocator);
        errdefer self.freePageIndexEntries(page_entries.items);
        var dictionary = try ByteArrayDictionary.build(self.allocator, self.options, column, data);
        defer if (dictionary) |*dict| dict.deinit(self.allocator);

        const dictionary_page_offset: ?i64 = if (dictionary) |*dict| blk: {
            const offset = try types.intCastI64(@intCast(self.offset));
            const page_sizes = try self.writeDictionaryPage(dict);
            total_uncompressed = std.math.add(i64, total_uncompressed, page_sizes.uncompressed_size) catch return error.RowCountOverflow;
            total_compressed = std.math.add(i64, total_compressed, page_sizes.compressed_size) catch return error.RowCountOverflow;
            break :blk offset;
        } else null;

        const first_page_offset = try types.intCastI64(@intCast(self.offset));
        var value_offset: usize = 0;
        var row_start: usize = 0;
        while (row_start < row_count or (row_count == 0 and row_start == 0)) {
            const page_rows = if (row_count == 0) 0 else @min(self.options.max_page_rows, row_count - row_start);
            const page_data = try sliceColumnData(data, column, row_start, page_rows, &value_offset);
            const page_sizes = if (dictionary) |*dict|
                try self.writeDictionaryColumnPage(page_rows, column, page_data, dict)
            else
                try self.writeColumnPage(page_rows, column, page_data);
            total_uncompressed = std.math.add(i64, total_uncompressed, page_sizes.uncompressed_size) catch return error.RowCountOverflow;
            total_compressed = std.math.add(i64, total_compressed, page_sizes.compressed_size) catch return error.RowCountOverflow;
            if (page_rows > 0) {
                const page_null_count = if (column.repetition == .optional)
                    try countNulls(page_data.validity().?)
                else
                    0;
                const page_stats = try self.buildStatistics(column, page_data, page_null_count);
                errdefer self.freeStatistics(page_stats);
                try page_entries.append(self.allocator, .{
                    .offset = page_sizes.data_page_offset,
                    .compressed_page_size = std.math.cast(i32, page_sizes.compressed_size) orelse return error.RowCountOverflow,
                    .first_row_index = try types.intCastI64(row_start),
                    .row_count = try types.intCastI64(page_rows),
                    .statistics = page_stats,
                });
            }
            if (row_count == 0) break;
            row_start += page_rows;
        }

        const owned_page_entries = try page_entries.toOwnedSlice(self.allocator);
        const value_encoding = self.columnValueEncoding(column);
        return .{
            .physical_type = column.column_type.physical,
            .encodings = if (dictionary_page_offset != null)
                &dictionary_page_encodings
            else if (value_encoding == .byte_stream_split)
                &byte_stream_split_page_encodings
            else if (value_encoding == .delta_binary_packed)
                &delta_binary_page_encodings
            else if (value_encoding == .delta_length_byte_array)
                &delta_length_page_encodings
            else if (value_encoding == .delta_byte_array)
                &delta_byte_array_page_encodings
            else
                &page_encodings,
            .path = column.name,
            .codec = self.options.compression,
            .num_values = try types.intCastI64(row_count),
            .total_uncompressed_size = total_uncompressed,
            .total_compressed_size = total_compressed,
            .data_page_offset = first_page_offset,
            .dictionary_page_offset = dictionary_page_offset,
            .statistics = column_stats,
            .page_index_entries = owned_page_entries,
        };
    }

    fn writeColumnPage(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnData) !PageSizes {
        return switch (self.options.data_page_version) {
            .v1 => try self.writeColumnPageV1(row_count, column, data),
            .v2 => try self.writeColumnPageV2(row_count, column, data),
        };
    }

    fn writeColumnPageV1(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnData) !PageSizes {
        const page_null_count = if (column.repetition == .optional)
            try countNulls(data.validity().?)
        else
            0;
        const page_stats = try self.buildStatistics(column, data, page_null_count);
        defer self.freeStatistics(page_stats);

        var page_body: std.Io.Writer.Allocating = .init(self.allocator);
        defer page_body.deinit();

        if (column.repetition == .optional) {
            try plain.encodeDefinitionLevels(self.allocator, &page_body.writer, data.validity().?);
        }
        const encoding = self.pageValueEncoding(column, data);
        try self.encodePageValues(&page_body.writer, data, column.column_type, encoding);

        const body = page_body.written();
        const compressed = try compressPage(self.allocator, self.options.compression, body);
        defer compressed.deinit(self.allocator);

        const uncompressed_size = try types.intCastI32(body.len);
        const compressed_size = try types.intCastI32(compressed.data.len);
        const page_header: types.PageHeader = .{
            .page_type = .data_page,
            .uncompressed_page_size = uncompressed_size,
            .compressed_page_size = compressed_size,
            .crc = optionalPageCrc(self.options, &.{compressed.data}),
            .data_page_header = .{
                .num_values = try types.intCastI32(row_count),
                .encoding = encoding,
                .definition_level_encoding = .rle,
                .repetition_level_encoding = .rle,
                .statistics = page_stats,
            },
        };

        var header_buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer header_buf.deinit();
        try thrift.writePageHeader(&header_buf.writer, page_header);

        const data_page_offset = try types.intCastI64(@intCast(self.offset));
        try self.writeAll(header_buf.written());
        try self.writeAll(compressed.data);

        const uncompressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, body.len) catch return error.RowCountOverflow);
        const compressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, compressed.data.len) catch return error.RowCountOverflow);
        return .{
            .data_page_offset = data_page_offset,
            .uncompressed_size = uncompressed_total_size,
            .compressed_size = compressed_total_size,
        };
    }

    fn writeColumnPageV2(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnData) !PageSizes {
        const page_null_count = if (column.repetition == .optional)
            try countNulls(data.validity().?)
        else
            0;
        const page_stats = try self.buildStatistics(column, data, page_null_count);
        defer self.freeStatistics(page_stats);

        var definition_levels: std.Io.Writer.Allocating = .init(self.allocator);
        defer definition_levels.deinit();
        if (column.repetition == .optional) {
            try plain.encodeDefinitionLevelsBody(&definition_levels.writer, data.validity().?);
        }

        var values_body: std.Io.Writer.Allocating = .init(self.allocator);
        defer values_body.deinit();
        const encoding = self.pageValueEncoding(column, data);
        try self.encodePageValues(&values_body.writer, data, column.column_type, encoding);

        const values = values_body.written();
        const compressed_values = try compressPage(self.allocator, self.options.compression, values);
        defer compressed_values.deinit(self.allocator);

        const levels_len = definition_levels.written().len;
        const uncompressed_size = try types.intCastI32(std.math.add(usize, levels_len, values.len) catch return error.RowCountOverflow);
        const compressed_size = try types.intCastI32(std.math.add(usize, levels_len, compressed_values.data.len) catch return error.RowCountOverflow);
        const page_header: types.PageHeader = .{
            .page_type = .data_page_v2,
            .uncompressed_page_size = uncompressed_size,
            .compressed_page_size = compressed_size,
            .crc = optionalPageCrc(self.options, &.{ definition_levels.written(), compressed_values.data }),
            .data_page_header_v2 = .{
                .num_values = try types.intCastI32(row_count),
                .num_nulls = std.math.cast(i32, page_null_count) orelse return error.RowCountOverflow,
                .num_rows = try types.intCastI32(row_count),
                .encoding = encoding,
                .definition_levels_byte_length = try types.intCastI32(levels_len),
                .repetition_levels_byte_length = 0,
                .is_compressed = self.options.compression != .uncompressed,
                .statistics = page_stats,
            },
        };

        var header_buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer header_buf.deinit();
        try thrift.writePageHeader(&header_buf.writer, page_header);

        const data_page_offset = try types.intCastI64(@intCast(self.offset));
        try self.writeAll(header_buf.written());
        try self.writeAll(definition_levels.written());
        try self.writeAll(compressed_values.data);

        const uncompressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, @as(usize, @intCast(uncompressed_size))) catch return error.RowCountOverflow);
        const compressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, @as(usize, @intCast(compressed_size))) catch return error.RowCountOverflow);
        return .{
            .data_page_offset = data_page_offset,
            .uncompressed_size = uncompressed_total_size,
            .compressed_size = compressed_total_size,
        };
    }

    fn writeDictionaryPage(self: *StreamWriter, dictionary: *const ByteArrayDictionary) !PageSizes {
        var page_body: std.Io.Writer.Allocating = .init(self.allocator);
        defer page_body.deinit();

        try plain.encodeValues(&page_body.writer, .{ .byte_array = .{ .values = dictionary.values.items } }, .{ .physical = .byte_array });

        const body = page_body.written();
        const compressed = try compressPage(self.allocator, self.options.compression, body);
        defer compressed.deinit(self.allocator);

        const uncompressed_size = try types.intCastI32(body.len);
        const compressed_size = try types.intCastI32(compressed.data.len);
        const page_header: types.PageHeader = .{
            .page_type = .dictionary_page,
            .uncompressed_page_size = uncompressed_size,
            .compressed_page_size = compressed_size,
            .crc = optionalPageCrc(self.options, &.{compressed.data}),
            .dictionary_page_header = .{
                .num_values = try types.intCastI32(dictionary.values.items.len),
                .encoding = .plain,
            },
        };

        var header_buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer header_buf.deinit();
        try thrift.writePageHeader(&header_buf.writer, page_header);

        const dictionary_page_offset = try types.intCastI64(@intCast(self.offset));
        try self.writeAll(header_buf.written());
        try self.writeAll(compressed.data);

        const uncompressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, body.len) catch return error.RowCountOverflow);
        const compressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, compressed.data.len) catch return error.RowCountOverflow);
        return .{
            .data_page_offset = dictionary_page_offset,
            .uncompressed_size = uncompressed_total_size,
            .compressed_size = compressed_total_size,
        };
    }

    fn writeDictionaryColumnPage(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnData, dictionary: *const ByteArrayDictionary) !PageSizes {
        return switch (self.options.data_page_version) {
            .v1 => try self.writeDictionaryColumnPageV1(row_count, column, data, dictionary),
            .v2 => try self.writeDictionaryColumnPageV2(row_count, column, data, dictionary),
        };
    }

    fn writeDictionaryColumnPageV1(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnData, dictionary: *const ByteArrayDictionary) !PageSizes {
        const page_null_count = if (column.repetition == .optional)
            try countNulls(data.validity().?)
        else
            0;
        const page_stats = try self.buildStatistics(column, data, page_null_count);
        defer self.freeStatistics(page_stats);

        var page_body: std.Io.Writer.Allocating = .init(self.allocator);
        defer page_body.deinit();

        if (column.repetition == .optional) {
            try plain.encodeDefinitionLevels(self.allocator, &page_body.writer, data.validity().?);
        }

        try self.writeDictionaryIndexes(&page_body.writer, data, dictionary);

        const body = page_body.written();
        const compressed = try compressPage(self.allocator, self.options.compression, body);
        defer compressed.deinit(self.allocator);

        const uncompressed_size = try types.intCastI32(body.len);
        const compressed_size = try types.intCastI32(compressed.data.len);
        const page_header: types.PageHeader = .{
            .page_type = .data_page,
            .uncompressed_page_size = uncompressed_size,
            .compressed_page_size = compressed_size,
            .crc = optionalPageCrc(self.options, &.{compressed.data}),
            .data_page_header = .{
                .num_values = try types.intCastI32(row_count),
                .encoding = .rle_dictionary,
                .definition_level_encoding = .rle,
                .repetition_level_encoding = .rle,
                .statistics = page_stats,
            },
        };

        var header_buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer header_buf.deinit();
        try thrift.writePageHeader(&header_buf.writer, page_header);

        const data_page_offset = try types.intCastI64(@intCast(self.offset));
        try self.writeAll(header_buf.written());
        try self.writeAll(compressed.data);

        const uncompressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, body.len) catch return error.RowCountOverflow);
        const compressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, compressed.data.len) catch return error.RowCountOverflow);
        return .{
            .data_page_offset = data_page_offset,
            .uncompressed_size = uncompressed_total_size,
            .compressed_size = compressed_total_size,
        };
    }

    fn writeDictionaryColumnPageV2(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnData, dictionary: *const ByteArrayDictionary) !PageSizes {
        const page_null_count = if (column.repetition == .optional)
            try countNulls(data.validity().?)
        else
            0;
        const page_stats = try self.buildStatistics(column, data, page_null_count);
        defer self.freeStatistics(page_stats);

        var definition_levels: std.Io.Writer.Allocating = .init(self.allocator);
        defer definition_levels.deinit();
        if (column.repetition == .optional) {
            try plain.encodeDefinitionLevelsBody(&definition_levels.writer, data.validity().?);
        }

        var values_body: std.Io.Writer.Allocating = .init(self.allocator);
        defer values_body.deinit();
        try self.writeDictionaryIndexes(&values_body.writer, data, dictionary);

        const values = values_body.written();
        const compressed_values = try compressPage(self.allocator, self.options.compression, values);
        defer compressed_values.deinit(self.allocator);

        const levels_len = definition_levels.written().len;
        const uncompressed_size = try types.intCastI32(std.math.add(usize, levels_len, values.len) catch return error.RowCountOverflow);
        const compressed_size = try types.intCastI32(std.math.add(usize, levels_len, compressed_values.data.len) catch return error.RowCountOverflow);
        const page_header: types.PageHeader = .{
            .page_type = .data_page_v2,
            .uncompressed_page_size = uncompressed_size,
            .compressed_page_size = compressed_size,
            .crc = optionalPageCrc(self.options, &.{ definition_levels.written(), compressed_values.data }),
            .data_page_header_v2 = .{
                .num_values = try types.intCastI32(row_count),
                .num_nulls = std.math.cast(i32, page_null_count) orelse return error.RowCountOverflow,
                .num_rows = try types.intCastI32(row_count),
                .encoding = .rle_dictionary,
                .definition_levels_byte_length = try types.intCastI32(levels_len),
                .repetition_levels_byte_length = 0,
                .is_compressed = self.options.compression != .uncompressed,
                .statistics = page_stats,
            },
        };

        var header_buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer header_buf.deinit();
        try thrift.writePageHeader(&header_buf.writer, page_header);

        const data_page_offset = try types.intCastI64(@intCast(self.offset));
        try self.writeAll(header_buf.written());
        try self.writeAll(definition_levels.written());
        try self.writeAll(compressed_values.data);

        const uncompressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, @as(usize, @intCast(uncompressed_size))) catch return error.RowCountOverflow);
        const compressed_total_size = try types.intCastI64(std.math.add(usize, header_buf.written().len, @as(usize, @intCast(compressed_size))) catch return error.RowCountOverflow);
        return .{
            .data_page_offset = data_page_offset,
            .uncompressed_size = uncompressed_total_size,
            .compressed_size = compressed_total_size,
        };
    }

    fn writeDictionaryIndexes(self: *StreamWriter, writer: *std.Io.Writer, data: types.ColumnData, dictionary: *const ByteArrayDictionary) !void {
        const values = switch (data) {
            .byte_array => |d| d.values,
            else => return error.InvalidColumnData,
        };
        const indexes = try self.allocator.alloc(u32, values.len);
        defer self.allocator.free(indexes);
        for (values, indexes) |value, *index| index.* = try dictionary.indexOf(value);

        const bit_width = bitWidthForDictionary(dictionary.values.items.len);
        try writer.writeByte(bit_width);
        try plain.encodeRleBitPackedUint32(writer, indexes, bit_width);
    }

    fn columnValueEncoding(self: *StreamWriter, column: types.Column) types.Encoding {
        if (column.column_type.physical == .boolean) return .rle;
        if (self.options.use_delta_binary_packed) {
            switch (column.column_type.physical) {
                .int32, .int64 => return .delta_binary_packed,
                else => {},
            }
        }
        if (self.options.use_delta_byte_array) {
            switch (column.column_type.physical) {
                .byte_array, .fixed_len_byte_array => return .delta_byte_array,
                else => {},
            }
        }
        if (self.options.use_delta_length_byte_array) {
            switch (column.column_type.physical) {
                .byte_array => return .delta_length_byte_array,
                else => {},
            }
        }
        if (self.options.use_byte_stream_split) {
            switch (column.column_type.physical) {
                .float, .double => return .byte_stream_split,
                else => {},
            }
        }
        return .plain;
    }

    fn pageValueEncoding(self: *StreamWriter, column: types.Column, data: types.ColumnData) types.Encoding {
        if (data.valueCount() == 0) return .plain;
        return self.columnValueEncoding(column);
    }

    fn encodePageValues(self: *StreamWriter, page_writer: *std.Io.Writer, data: types.ColumnData, column_type: types.ColumnType, encoding: types.Encoding) !void {
        return switch (encoding) {
            .plain => plain.encodeValues(page_writer, data, column_type),
            .rle => plain.encodeRleBooleanValues(self.allocator, page_writer, data, column_type),
            .byte_stream_split => plain.encodeByteStreamSplitValues(page_writer, data, column_type),
            .delta_binary_packed => plain.encodeDeltaBinaryPackedValues(page_writer, data, column_type),
            .delta_length_byte_array => plain.encodeDeltaLengthByteArrayValues(self.allocator, page_writer, data, column_type),
            .delta_byte_array => plain.encodeDeltaByteArrayValues(self.allocator, page_writer, data, column_type),
            else => error.UnsupportedEncoding,
        };
    }

    fn writePageIndexes(self: *StreamWriter) !void {
        for (self.row_groups.items) |*row_group| {
            const columns = @constCast(row_group.columns);
            for (columns) |*column| {
                if (column.page_index_entries.len == 0) continue;

                const offset_index_offset = try checkedOffsetI64(self.offset);
                var offset_index: std.Io.Writer.Allocating = .init(self.allocator);
                defer offset_index.deinit();
                try thrift.writeOffsetIndex(&offset_index.writer, column.page_index_entries);
                const offset_index_bytes = offset_index.written();
                column.offset_index_offset = offset_index_offset;
                column.offset_index_length = try types.intCastI32(offset_index_bytes.len);
                try self.writeAll(offset_index_bytes);

                if (columnIndexSupported(column.page_index_entries)) {
                    const column_index_offset = try checkedOffsetI64(self.offset);
                    var column_index: std.Io.Writer.Allocating = .init(self.allocator);
                    defer column_index.deinit();
                    try thrift.writeColumnIndex(&column_index.writer, column.page_index_entries);
                    const column_index_bytes = column_index.written();
                    column.column_index_offset = column_index_offset;
                    column.column_index_length = try types.intCastI32(column_index_bytes.len);
                    try self.writeAll(column_index_bytes);
                }
            }
        }
    }

    fn buildStatistics(self: *StreamWriter, column: types.Column, data: types.ColumnData, null_count: i64) !types.Statistics {
        return buildPrimitiveStatistics(self.allocator, column, data, null_count);
    }

    fn freeColumnMeta(self: *StreamWriter, column: types.ColumnChunkMeta) void {
        self.freeStatistics(column.statistics);
        self.freePageIndexEntries(column.page_index_entries);
    }

    fn freeStatistics(self: *StreamWriter, stats: types.Statistics) void {
        if (stats.min_value) |value| self.allocator.free(value);
        if (stats.max_value) |value| self.allocator.free(value);
    }

    fn freePageIndexEntries(self: *StreamWriter, entries: []const types.PageIndexEntry) void {
        for (entries) |entry| self.freeStatistics(entry.statistics);
        if (entries.len > 0) self.allocator.free(entries);
    }

    fn writeAll(self: *StreamWriter, bytes: []const u8) !void {
        try self.output.writeAll(bytes);
        self.offset = std.math.add(u64, self.offset, bytes.len) catch return error.RowCountOverflow;
    }
};

const PageSizes = struct {
    data_page_offset: i64,
    uncompressed_size: i64,
    compressed_size: i64,
};

const ByteArrayDictionary = struct {
    map: std.StringHashMap(u32),
    values: std.ArrayList([]const u8) = .empty,

    fn build(allocator: std.mem.Allocator, options: Options, column: types.Column, data: types.ColumnData) !?ByteArrayDictionary {
        if (!options.use_dictionary or column.column_type.physical != .byte_array) return null;
        if (options.use_delta_length_byte_array or options.use_delta_byte_array) return null;
        const values = switch (data) {
            .byte_array => |d| d.values,
            else => return null,
        };
        if (values.len == 0) return null;

        var dictionary: ByteArrayDictionary = .{ .map = std.StringHashMap(u32).init(allocator) };
        errdefer dictionary.deinit(allocator);

        for (values) |value| {
            const entry = try dictionary.map.getOrPut(value);
            if (!entry.found_existing) {
                const index = std.math.cast(u32, dictionary.values.items.len) orelse return error.RowCountOverflow;
                entry.value_ptr.* = index;
                try dictionary.values.append(allocator, value);
            }
        }

        if (dictionary.values.items.len >= values.len) {
            dictionary.deinit(allocator);
            return null;
        }
        return dictionary;
    }

    fn deinit(self: *ByteArrayDictionary, allocator: std.mem.Allocator) void {
        self.map.deinit();
        self.values.deinit(allocator);
    }

    fn indexOf(self: *const ByteArrayDictionary, value: []const u8) !u32 {
        return self.map.get(value) orelse error.InvalidColumnData;
    }
};

fn bitWidthForDictionary(value_count: usize) u8 {
    if (value_count <= 1) return 0;
    const max_index: u32 = @intCast(value_count - 1);
    return @intCast(@bitSizeOf(u32) - @clz(max_index));
}

fn optionalPageCrc(options: Options, chunks: []const []const u8) ?i32 {
    if (!options.page_checksum) return null;
    var crc: std.hash.Crc32 = .init();
    for (chunks) |chunk| crc.update(chunk);
    return @bitCast(crc.final());
}

fn sliceColumnData(data: types.ColumnData, column: types.Column, row_start: usize, row_count: usize, value_offset: *usize) !types.ColumnData {
    return switch (column.repetition) {
        .required => switch (data) {
            .boolean => |d| .{ .boolean = .{ .values = d.values[row_start..][0..row_count] } },
            .int32 => |d| .{ .int32 = .{ .values = d.values[row_start..][0..row_count] } },
            .int64 => |d| .{ .int64 = .{ .values = d.values[row_start..][0..row_count] } },
            .float => |d| .{ .float = .{ .values = d.values[row_start..][0..row_count] } },
            .double => |d| .{ .double = .{ .values = d.values[row_start..][0..row_count] } },
            .byte_array => |d| .{ .byte_array = .{ .values = d.values[row_start..][0..row_count] } },
            .fixed_len_byte_array => |d| .{ .fixed_len_byte_array = .{ .values = d.values[row_start..][0..row_count] } },
        },
        .optional => switch (data) {
            .boolean => |d| blk: {
                const validity = d.validity.?[row_start..][0..row_count];
                const values = try optionalValueSlice(bool, d.values, validity, value_offset);
                break :blk .{ .boolean = .{ .values = values, .validity = validity } };
            },
            .int32 => |d| blk: {
                const validity = d.validity.?[row_start..][0..row_count];
                const values = try optionalValueSlice(i32, d.values, validity, value_offset);
                break :blk .{ .int32 = .{ .values = values, .validity = validity } };
            },
            .int64 => |d| blk: {
                const validity = d.validity.?[row_start..][0..row_count];
                const values = try optionalValueSlice(i64, d.values, validity, value_offset);
                break :blk .{ .int64 = .{ .values = values, .validity = validity } };
            },
            .float => |d| blk: {
                const validity = d.validity.?[row_start..][0..row_count];
                const values = try optionalValueSlice(f32, d.values, validity, value_offset);
                break :blk .{ .float = .{ .values = values, .validity = validity } };
            },
            .double => |d| blk: {
                const validity = d.validity.?[row_start..][0..row_count];
                const values = try optionalValueSlice(f64, d.values, validity, value_offset);
                break :blk .{ .double = .{ .values = values, .validity = validity } };
            },
            .byte_array => |d| blk: {
                const validity = d.validity.?[row_start..][0..row_count];
                const values = try optionalValueSlice([]const u8, d.values, validity, value_offset);
                break :blk .{ .byte_array = .{ .values = values, .validity = validity } };
            },
            .fixed_len_byte_array => |d| blk: {
                const validity = d.validity.?[row_start..][0..row_count];
                const values = try optionalValueSlice([]const u8, d.values, validity, value_offset);
                break :blk .{ .fixed_len_byte_array = .{ .values = values, .validity = validity } };
            },
        },
        .repeated => error.UnsupportedNestedSchema,
    };
}

fn optionalValueSlice(comptime T: type, values: []const T, validity: []const bool, value_offset: *usize) ![]const T {
    const non_null = try countValid(validity);
    const end = std.math.add(usize, value_offset.*, non_null) catch return error.InvalidColumnData;
    if (end > values.len) return error.InvalidColumnData;
    defer value_offset.* = end;
    return values[value_offset.*..end];
}

fn countValid(validity: []const bool) !usize {
    var n: usize = 0;
    for (validity) |valid| {
        if (valid) n += 1;
    }
    return n;
}

const EncodedPage = struct {
    data: []const u8,
    owned: ?[]u8 = null,

    fn deinit(self: EncodedPage, allocator: std.mem.Allocator) void {
        if (self.owned) |bytes| allocator.free(bytes);
    }
};

fn compressPage(allocator: std.mem.Allocator, codec: types.CompressionCodec, body: []const u8) !EncodedPage {
    return switch (codec) {
        .uncompressed => .{ .data = body },
        .snappy => blk: {
            const compressed = try snappy.compress(allocator, body);
            break :blk .{ .data = compressed, .owned = compressed };
        },
        .gzip => blk: {
            const compressed = try gzip.compress(allocator, body);
            break :blk .{ .data = compressed, .owned = compressed };
        },
        .zstd => blk: {
            const compressed = try zstd.compressFrame(allocator, body);
            break :blk .{ .data = compressed, .owned = compressed };
        },
        else => error.UnsupportedCompression,
    };
}

fn validateSchema(schema: types.Schema) !void {
    if (schema.name.len == 0) return error.InvalidSchema;
    if (schema.columns.len == 0) return error.InvalidSchema;
    for (schema.columns) |column| {
        if (column.name.len == 0) return error.InvalidSchema;
        if (column.repetition == .repeated) return error.UnsupportedNestedSchema;
        switch (column.column_type.physical) {
            .boolean, .int32, .int64, .float, .double, .byte_array => {},
            .fixed_len_byte_array => {
                _ = try types.physicalTypeWidth(column.column_type.physical, column.column_type.type_length);
            },
            .int96 => return error.UnsupportedType,
        }
        switch (column.column_type.logical) {
            .none => {},
            .string => if (column.column_type.physical != .byte_array) return error.InvalidSchema,
            .decimal => try types.validateDecimalType(column.column_type),
            .date => if (column.column_type.physical != .int32) return error.InvalidSchema,
            .timestamp_millis, .timestamp_micros, .timestamp_nanos => if (column.column_type.physical != .int64) return error.InvalidSchema,
        }
    }
}

fn validateOptions(options: Options) !void {
    if (options.max_page_rows == 0) return error.InvalidColumnData;
    switch (options.compression) {
        .uncompressed, .snappy, .gzip, .zstd => {},
        else => return error.UnsupportedCompression,
    }
}

fn countNulls(validity: []const bool) !i64 {
    var n: i64 = 0;
    for (validity) |valid| {
        if (!valid) n += 1;
    }
    return n;
}

fn checkedOffsetI64(offset: u64) !i64 {
    return std.math.cast(i64, offset) orelse return error.RowCountOverflow;
}

fn columnIndexSupported(entries: []const types.PageIndexEntry) bool {
    if (entries.len == 0) return false;
    for (entries) |entry| {
        const null_count = entry.statistics.null_count orelse return false;
        if (null_count < 0 or null_count > entry.row_count) return false;
        const null_page = null_count == entry.row_count;
        if (!null_page and !entry.statistics.hasMinMax()) return false;
    }
    return true;
}

fn buildPrimitiveStatistics(allocator: std.mem.Allocator, column: types.Column, data: types.ColumnData, null_count: i64) !types.Statistics {
    var stats: types.Statistics = .{ .null_count = null_count };
    switch (data) {
        .boolean => |d| try setBoolStatistics(allocator, &stats, d.values),
        .int32 => |d| try setInt32Statistics(allocator, &stats, d.values),
        .int64 => |d| try setInt64Statistics(allocator, &stats, d.values),
        .float => |d| try setFloatStatistics(allocator, &stats, d.values),
        .double => |d| try setDoubleStatistics(allocator, &stats, d.values),
        .byte_array => |d| if (column.column_type.logical != .decimal) try setByteArrayStatistics(allocator, &stats, d.values),
        .fixed_len_byte_array => |d| if (column.column_type.logical != .decimal) try setByteArrayStatistics(allocator, &stats, d.values),
    }
    return stats;
}

fn setBoolStatistics(allocator: std.mem.Allocator, stats: *types.Statistics, values: []const bool) !void {
    if (values.len == 0) return;

    var min = values[0];
    var max = values[0];
    for (values[1..]) |value| {
        min = min and value;
        max = max or value;
    }

    const min_buf = [_]u8{if (min) 1 else 0};
    const max_buf = [_]u8{if (max) 1 else 0};
    try setStatBytes(allocator, stats, min_buf[0..], max_buf[0..]);
}

fn setInt32Statistics(allocator: std.mem.Allocator, stats: *types.Statistics, values: []const i32) !void {
    if (values.len == 0) return;

    var min = values[0];
    var max = values[0];
    for (values[1..]) |value| {
        if (value < min) min = value;
        if (value > max) max = value;
    }

    var min_buf: [4]u8 = undefined;
    var max_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &min_buf, @bitCast(min), .little);
    std.mem.writeInt(u32, &max_buf, @bitCast(max), .little);
    try setStatBytes(allocator, stats, min_buf[0..], max_buf[0..]);
}

fn setInt64Statistics(allocator: std.mem.Allocator, stats: *types.Statistics, values: []const i64) !void {
    if (values.len == 0) return;

    var min = values[0];
    var max = values[0];
    for (values[1..]) |value| {
        if (value < min) min = value;
        if (value > max) max = value;
    }

    var min_buf: [8]u8 = undefined;
    var max_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &min_buf, @bitCast(min), .little);
    std.mem.writeInt(u64, &max_buf, @bitCast(max), .little);
    try setStatBytes(allocator, stats, min_buf[0..], max_buf[0..]);
}

fn setFloatStatistics(allocator: std.mem.Allocator, stats: *types.Statistics, values: []const f32) !void {
    if (values.len == 0) return;

    var min = values[0];
    var max = values[0];
    if (min != min) return;
    for (values[1..]) |value| {
        if (value != value) return;
        if (value < min) min = value;
        if (value > max) max = value;
    }

    var min_buf: [4]u8 = undefined;
    var max_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &min_buf, @bitCast(min), .little);
    std.mem.writeInt(u32, &max_buf, @bitCast(max), .little);
    try setStatBytes(allocator, stats, min_buf[0..], max_buf[0..]);
}

fn setDoubleStatistics(allocator: std.mem.Allocator, stats: *types.Statistics, values: []const f64) !void {
    if (values.len == 0) return;

    var min = values[0];
    var max = values[0];
    if (min != min) return;
    for (values[1..]) |value| {
        if (value != value) return;
        if (value < min) min = value;
        if (value > max) max = value;
    }

    var min_buf: [8]u8 = undefined;
    var max_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &min_buf, @bitCast(min), .little);
    std.mem.writeInt(u64, &max_buf, @bitCast(max), .little);
    try setStatBytes(allocator, stats, min_buf[0..], max_buf[0..]);
}

fn setByteArrayStatistics(allocator: std.mem.Allocator, stats: *types.Statistics, values: []const []const u8) !void {
    if (values.len == 0) return;

    var min = values[0];
    var max = values[0];
    for (values[1..]) |value| {
        if (std.mem.order(u8, value, min) == .lt) min = value;
        if (std.mem.order(u8, value, max) == .gt) max = value;
    }

    try setStatBytes(allocator, stats, min, max);
}

fn setStatBytes(allocator: std.mem.Allocator, stats: *types.Statistics, min: []const u8, max: []const u8) !void {
    const min_value = try allocator.dupe(u8, min);
    errdefer allocator.free(min_value);
    const max_value = try allocator.dupe(u8, max);

    stats.min_value = min_value;
    stats.max_value = max_value;
}

test "writer creates parseable in-memory footer" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.init(testing.allocator, &out.writer, schema);
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

    try testing.expectEqualSlices(u8, "PAR1", out.written()[0..4]);
    try testing.expectEqualSlices(u8, "PAR1", out.written()[out.written().len - 4 ..]);

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    const id_stats = parsed.metadata.row_groups[0].columns[0].statistics;
    const min_id = [_]u8{ 10, 0, 0, 0, 0, 0, 0, 0 };
    const max_id = [_]u8{ 12, 0, 0, 0, 0, 0, 0, 0 };
    try testing.expectEqual(@as(i64, 0), id_stats.null_count.?);
    try testing.expectEqualSlices(u8, min_id[0..], id_stats.min_value.?);
    try testing.expectEqualSlices(u8, max_id[0..], id_stats.max_value.?);

    const label_stats = parsed.metadata.row_groups[0].columns[1].statistics;
    try testing.expectEqual(@as(i64, 1), label_stats.null_count.?);
    try testing.expectEqualStrings("a", label_stats.min_value.?);
    try testing.expectEqualStrings("c", label_stats.max_value.?);

    const id_meta = parsed.metadata.row_groups[0].columns[0];
    try testing.expect(id_meta.offset_index_offset != null);
    try testing.expect(id_meta.offset_index_length.? > 0);
    try testing.expect(id_meta.column_index_offset != null);
    try testing.expect(id_meta.column_index_length.? > 0);
}

test "writer creates zstd-compressed pages readable by reader" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{ .compression = .zstd });
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

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    try testing.expectEqual(types.CompressionCodec.zstd, parsed.metadata.row_groups[0].columns[0].codec);

    var id_col = try parsed.readColumn(testing.allocator, 0, 0);
    defer id_col.deinit(testing.allocator);
    try testing.expectEqualSlices(i64, ids[0..], id_col.int64.values);
}

test "writer creates snappy-compressed pages readable by reader" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{ .compression = .snappy });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 10, 11, 12, 13, 14 };
    const labels = [_][]const u8{ "prefix-000001-suffix", "prefix-000002-suffix", "prefix-000001-suffix", "prefix-000002-suffix" };
    const validity = [_]bool{ true, false, true, true, true };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .byte_array = .{ .values = labels[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    try testing.expectEqual(types.CompressionCodec.snappy, parsed.metadata.row_groups[0].columns[0].codec);

    var label_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer label_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], label_col.byte_array.validity.?);
    for (labels, label_col.byte_array.values) |expected, actual| {
        try testing.expectEqualStrings(expected, actual);
    }
}

test "writer creates gzip-compressed pages readable by reader" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{ .compression = .gzip });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 10, 11, 12, 13, 14 };
    const labels = [_][]const u8{ "prefix-000001-suffix", "prefix-000002-suffix", "prefix-000001-suffix", "prefix-000002-suffix" };
    const validity = [_]bool{ true, false, true, true, true };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .byte_array = .{ .values = labels[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    try testing.expectEqual(types.CompressionCodec.gzip, parsed.metadata.row_groups[0].columns[0].codec);

    var label_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer label_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], label_col.byte_array.validity.?);
    for (labels, label_col.byte_array.values) |expected, actual| {
        try testing.expectEqualStrings(expected, actual);
    }
}

test "writer splits large row groups into bounded pages" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{ .max_page_rows = 2 });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 1, 2, 3, 4, 5 };
    const labels = [_][]const u8{ "one", "three", "four" };
    const validity = [_]bool{ true, false, true, true, false };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .byte_array = .{ .values = labels[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();

    var id_col = try parsed.readColumn(testing.allocator, 0, 0);
    defer id_col.deinit(testing.allocator);
    try testing.expectEqualSlices(i64, ids[0..], id_col.int64.values);

    var label_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer label_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], label_col.byte_array.validity.?);
    try testing.expectEqualStrings("one", label_col.byte_array.values[0]);
    try testing.expectEqualStrings("three", label_col.byte_array.values[1]);
    try testing.expectEqualStrings("four", label_col.byte_array.values[2]);
}

test "writer dictionary-encodes repeated byte arrays" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{ .max_page_rows = 3 });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 1, 2, 3, 4, 5, 6 };
    const labels = [_][]const u8{ "alpha", "bravo", "alpha", "bravo", "alpha" };
    const validity = [_]bool{ true, true, false, true, true, true };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .byte_array = .{ .values = labels[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    const meta = parsed.metadata.row_groups[0].columns[1];
    try testing.expect(meta.dictionary_page_offset != null);
    try testing.expectEqual(types.Encoding.rle_dictionary, meta.encodings[2]);

    var label_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer label_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], label_col.byte_array.validity.?);
    try testing.expectEqualStrings("alpha", label_col.byte_array.values[0]);
    try testing.expectEqualStrings("bravo", label_col.byte_array.values[1]);
    try testing.expectEqualStrings("alpha", label_col.byte_array.values[2]);
    try testing.expectEqualStrings("bravo", label_col.byte_array.values[3]);
    try testing.expectEqualStrings("alpha", label_col.byte_array.values[4]);
}

test "writer emits data page v2 for plain and dictionary pages" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{
        .compression = .zstd,
        .max_page_rows = 3,
        .data_page_version = .v2,
    });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 1, 2, 3, 4, 5, 6 };
    const labels = [_][]const u8{ "alpha", "bravo", "alpha", "bravo", "alpha" };
    const validity = [_]bool{ true, true, false, true, true, true };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .byte_array = .{ .values = labels[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();

    const id_meta = parsed.metadata.row_groups[0].columns[0];
    var id_fixed = std.Io.Reader.fixed(out.written()[@as(usize, @intCast(id_meta.data_page_offset))..]);
    const id_header = try thrift.readPageHeader(&id_fixed);
    try testing.expectEqual(types.PageType.data_page_v2, id_header.page_type);
    try testing.expectEqual(types.Encoding.plain, id_header.data_page_header_v2.?.encoding);

    const label_meta = parsed.metadata.row_groups[0].columns[1];
    try testing.expect(label_meta.dictionary_page_offset != null);
    var label_fixed = std.Io.Reader.fixed(out.written()[@as(usize, @intCast(label_meta.data_page_offset))..]);
    const label_header = try thrift.readPageHeader(&label_fixed);
    try testing.expectEqual(types.PageType.data_page_v2, label_header.page_type);
    try testing.expectEqual(types.Encoding.rle_dictionary, label_header.data_page_header_v2.?.encoding);

    var id_col = try parsed.readColumn(testing.allocator, 0, 0);
    defer id_col.deinit(testing.allocator);
    try testing.expectEqualSlices(i64, ids[0..], id_col.int64.values);

    var label_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer label_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], label_col.byte_array.validity.?);
    try testing.expectEqualStrings("alpha", label_col.byte_array.values[0]);
    try testing.expectEqualStrings("bravo", label_col.byte_array.values[1]);
    try testing.expectEqualStrings("alpha", label_col.byte_array.values[2]);
    try testing.expectEqualStrings("bravo", label_col.byte_array.values[3]);
    try testing.expectEqualStrings("alpha", label_col.byte_array.values[4]);
}

test "writer emits delta binary packed integer pages" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "day", .column_type = .{ .physical = .int32, .logical = .date } },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var ids: [260]i64 = undefined;
    var days: [260]i32 = undefined;
    for (&ids, &days, 0..) |*id, *day, i| {
        id.* = @as(i64, @intCast(i)) * 10 - @as(i64, @intCast(i % 13));
        day.* = 19_000 + @as(i32, @intCast(i / 2));
    }

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{
        .compression = .zstd,
        .use_delta_binary_packed = true,
    });
    defer w.deinit();
    try w.start();

    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .int32 = .{ .values = days[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();

    const id_meta = parsed.metadata.row_groups[0].columns[0];
    try testing.expectEqual(types.Encoding.delta_binary_packed, id_meta.encodings[0]);
    var id_fixed = std.Io.Reader.fixed(out.written()[@as(usize, @intCast(id_meta.data_page_offset))..]);
    const id_header = try thrift.readPageHeader(&id_fixed);
    try testing.expectEqual(types.Encoding.delta_binary_packed, id_header.data_page_header.?.encoding);

    var id_col = try parsed.readColumn(testing.allocator, 0, 0);
    defer id_col.deinit(testing.allocator);
    try testing.expectEqualSlices(i64, ids[0..], id_col.int64.values);

    var day_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer day_col.deinit(testing.allocator);
    try testing.expectEqualSlices(i32, days[0..], day_col.int32.values);
}

test "writer emits delta length byte array pages" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{
        .compression = .zstd,
        .use_delta_length_byte_array = true,
    });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 1, 2, 3, 4, 5 };
    const labels = [_][]const u8{
        "prefix-000001-suffix",
        "prefix-000002-suffix",
        "prefix-000002-tail",
        "other",
    };
    const validity = [_]bool{ true, true, false, true, true };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .byte_array = .{ .values = labels[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();

    const label_meta = parsed.metadata.row_groups[0].columns[1];
    try testing.expectEqual(types.Encoding.delta_length_byte_array, label_meta.encodings[0]);
    try testing.expect(label_meta.dictionary_page_offset == null);
    var label_fixed = std.Io.Reader.fixed(out.written()[@as(usize, @intCast(label_meta.data_page_offset))..]);
    const label_header = try thrift.readPageHeader(&label_fixed);
    try testing.expectEqual(types.Encoding.delta_length_byte_array, label_header.data_page_header.?.encoding);

    var label_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer label_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], label_col.byte_array.validity.?);
    for (labels, label_col.byte_array.values) |expected, actual| {
        try testing.expectEqualStrings(expected, actual);
    }
}

test "writer emits delta byte array pages" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
        .{ .name = "blob", .column_type = .{ .physical = .fixed_len_byte_array, .type_length = 4 } },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{
        .compression = .zstd,
        .use_delta_byte_array = true,
    });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 1, 2, 3, 4, 5 };
    const labels = [_][]const u8{
        "prefix-000001-suffix",
        "prefix-000002-suffix",
        "prefix-000002-tail",
        "other",
    };
    const validity = [_]bool{ true, true, false, true, true };
    const blobs = [_][]const u8{ "abcd", "abce", "abzz", "bbzz", "bbzy" };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .byte_array = .{ .values = labels[0..], .validity = validity[0..] } },
        .{ .fixed_len_byte_array = .{ .values = blobs[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();

    const label_meta = parsed.metadata.row_groups[0].columns[1];
    try testing.expectEqual(types.Encoding.delta_byte_array, label_meta.encodings[0]);
    try testing.expect(label_meta.dictionary_page_offset == null);
    var label_fixed = std.Io.Reader.fixed(out.written()[@as(usize, @intCast(label_meta.data_page_offset))..]);
    const label_header = try thrift.readPageHeader(&label_fixed);
    try testing.expectEqual(types.Encoding.delta_byte_array, label_header.data_page_header.?.encoding);

    const blob_meta = parsed.metadata.row_groups[0].columns[2];
    try testing.expectEqual(types.Encoding.delta_byte_array, blob_meta.encodings[0]);

    var label_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer label_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], label_col.byte_array.validity.?);
    for (labels, label_col.byte_array.values) |expected, actual| {
        try testing.expectEqualStrings(expected, actual);
    }

    var blob_col = try parsed.readColumn(testing.allocator, 0, 2);
    defer blob_col.deinit(testing.allocator);
    for (blobs, blob_col.fixed_len_byte_array.values) |expected, actual| {
        try testing.expectEqualStrings(expected, actual);
    }
}

test "writer round-trips fixed-length byte arrays" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "blob", .column_type = .{ .physical = .fixed_len_byte_array, .type_length = 4 }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{
        .compression = .zstd,
        .data_page_version = .v2,
    });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 1, 2, 3, 4 };
    const blobs = [_][]const u8{
        "\x00\x01\x02\x03",
        "\x04\x05\x06\x07",
        "\x08\x09\x0a\x0b",
    };
    const validity = [_]bool{ true, false, true, true };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .fixed_len_byte_array = .{ .values = blobs[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    try testing.expectEqual(types.Type.fixed_len_byte_array, parsed.metadata.schema.columns[1].column_type.physical);
    try testing.expectEqual(@as(?i32, 4), parsed.metadata.schema.columns[1].column_type.type_length);

    var blob_col = try parsed.readColumn(testing.allocator, 0, 1);
    defer blob_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], blob_col.fixed_len_byte_array.validity.?);
    try testing.expectEqualStrings(blobs[0], blob_col.fixed_len_byte_array.values[0]);
    try testing.expectEqualStrings(blobs[1], blob_col.fixed_len_byte_array.values[1]);
    try testing.expectEqualStrings(blobs[2], blob_col.fixed_len_byte_array.values[2]);
}

test "writer preserves decimal logical annotations" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "amount64", .column_type = .{ .physical = .int64, .logical = .decimal, .decimal_precision = 9, .decimal_scale = 2 } },
        .{ .name = "amount_fixed", .column_type = .{ .physical = .fixed_len_byte_array, .logical = .decimal, .type_length = 8, .decimal_precision = 18, .decimal_scale = 4 } },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.init(testing.allocator, &out.writer, schema);
    defer w.deinit();
    try w.start();

    const amounts = [_]i64{ 12345, -678 };
    const fixed = [_][]const u8{
        "\x00\x00\x00\x00\x00\x0009",
        "\xff\xff\xff\xff\xff\xff\xfdZ",
    };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = amounts[0..] } },
        .{ .fixed_len_byte_array = .{ .values = fixed[0..] } },
    };
    try w.writeRowGroup(amounts.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    try testing.expectEqual(types.LogicalType.decimal, parsed.metadata.schema.columns[0].column_type.logical);
    try testing.expectEqual(@as(?i32, 9), parsed.metadata.schema.columns[0].column_type.decimal_precision);
    try testing.expectEqual(@as(?i32, 2), parsed.metadata.schema.columns[0].column_type.decimal_scale);
    try testing.expectEqual(types.LogicalType.decimal, parsed.metadata.schema.columns[1].column_type.logical);
    try testing.expectEqual(@as(?i32, 18), parsed.metadata.schema.columns[1].column_type.decimal_precision);
    try testing.expectEqual(@as(?i32, 4), parsed.metadata.schema.columns[1].column_type.decimal_scale);
}

test "writer preserves flat temporal logical annotations" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "day", .column_type = .{ .physical = .int32, .logical = .date } },
        .{ .name = "ts_ms", .column_type = .{ .physical = .int64, .logical = .timestamp_millis } },
        .{ .name = "ts_us", .column_type = .{ .physical = .int64, .logical = .timestamp_micros } },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.init(testing.allocator, &out.writer, schema);
    defer w.deinit();
    try w.start();

    const days = [_]i32{ 19_000, 19_001 };
    const millis = [_]i64{ 1_640_995_200_000, 1_640_995_201_000 };
    const micros = [_]i64{ 1_640_995_200_000_000, 1_640_995_201_000_000 };
    const batch = [_]types.ColumnData{
        .{ .int32 = .{ .values = days[0..] } },
        .{ .int64 = .{ .values = millis[0..] } },
        .{ .int64 = .{ .values = micros[0..] } },
    };
    try w.writeRowGroup(days.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    try testing.expectEqual(types.LogicalType.date, parsed.metadata.schema.columns[0].column_type.logical);
    try testing.expectEqual(types.LogicalType.timestamp_millis, parsed.metadata.schema.columns[1].column_type.logical);
    try testing.expectEqual(types.LogicalType.timestamp_micros, parsed.metadata.schema.columns[2].column_type.logical);
}
