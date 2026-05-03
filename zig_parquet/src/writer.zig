const std = @import("std");
const types = @import("types.zig");
const thrift = @import("thrift.zig");
const plain = @import("plain.zig");
const snappy = @import("snappy.zig");
const gzip = @import("gzip.zig");
const lz4 = @import("lz4.zig");
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

    pub fn writeRowGroupTriplets(self: *StreamWriter, row_count: usize, columns: []const types.ColumnTripletData) !void {
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
            try validateTripletColumnData(column, row_count, column_data);
            metas[idx] = try self.writeTripletColumnChunk(row_count, column, column_data);
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

    pub fn writeRowGroupLists(self: *StreamWriter, row_count: usize, columns: []const types.ColumnListData) !void {
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
            try validateListColumnData(column, row_count, column_data);
            metas[idx] = try self.writeListColumnChunk(row_count, column, column_data);
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

    pub fn writeRowGroupNestedLists(self: *StreamWriter, row_count: usize, columns: []const types.ColumnNestedListData) !void {
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
            var triplets = try buildNestedListTripletData(self.allocator, column, row_count, column_data);
            defer freeBuiltTripletData(self.allocator, &triplets);
            metas[idx] = try self.writeTripletColumnChunk(row_count, column, triplets);
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

    pub fn writeRowGroupNestedMaps(self: *StreamWriter, row_count: usize, maps: []const types.ColumnNestedMapData) !void {
        if (!self.started or self.finished) return error.InvalidParquetFile;

        const metas = try self.allocator.alloc(types.ColumnChunkMeta, self.schema.columns.len);
        var metas_len: usize = 0;
        errdefer {
            for (metas[0..metas_len]) |meta| self.freeColumnMeta(meta);
            self.allocator.free(metas);
        }

        var total_uncompressed: i64 = 0;
        var total_compressed: i64 = 0;
        var column_index: usize = 0;
        var map_index: usize = 0;
        while (column_index < self.schema.columns.len) {
            if (map_index >= maps.len) return error.InvalidColumnData;
            const span = nestedMapColumnSpan(self.schema.columns, column_index) orelse return error.UnsupportedNestedSchema;
            var triplets = try buildNestedMapTripletData(self.allocator, self.schema.columns[column_index..][0..span], row_count, maps[map_index]);
            defer triplets.deinit(self.allocator);

            metas[column_index] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index], triplets.outer_key);
            metas_len += 1;
            total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index].total_uncompressed_size) catch return error.RowCountOverflow;
            total_compressed = std.math.add(i64, total_compressed, metas[column_index].total_compressed_size) catch return error.RowCountOverflow;

            metas[column_index + 1] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index + 1], triplets.inner_key);
            metas_len += 1;
            total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index + 1].total_uncompressed_size) catch return error.RowCountOverflow;
            total_compressed = std.math.add(i64, total_compressed, metas[column_index + 1].total_compressed_size) catch return error.RowCountOverflow;

            if (triplets.inner_value) |value_triplets| {
                metas[column_index + 2] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index + 2], value_triplets);
                metas_len += 1;
                total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index + 2].total_uncompressed_size) catch return error.RowCountOverflow;
                total_compressed = std.math.add(i64, total_compressed, metas[column_index + 2].total_compressed_size) catch return error.RowCountOverflow;
            }

            column_index += span;
            map_index += 1;
        }
        if (map_index != maps.len or metas_len != metas.len) return error.InvalidColumnData;

        try self.row_groups.append(self.allocator, .{
            .columns = metas,
            .total_byte_size = total_uncompressed,
            .total_compressed_size = total_compressed,
            .num_rows = try types.intCastI64(row_count),
        });
    }

    pub fn writeRowGroupListMaps(self: *StreamWriter, row_count: usize, maps: []const types.ColumnListMapData) !void {
        if (!self.started or self.finished) return error.InvalidParquetFile;

        const metas = try self.allocator.alloc(types.ColumnChunkMeta, self.schema.columns.len);
        var metas_len: usize = 0;
        errdefer {
            for (metas[0..metas_len]) |meta| self.freeColumnMeta(meta);
            self.allocator.free(metas);
        }

        var total_uncompressed: i64 = 0;
        var total_compressed: i64 = 0;
        var column_index: usize = 0;
        var map_index: usize = 0;
        while (column_index < self.schema.columns.len) {
            if (map_index >= maps.len) return error.InvalidColumnData;
            const span = listMapColumnSpan(self.schema.columns, column_index) orelse return error.UnsupportedNestedSchema;
            var triplets = try buildListMapTripletData(self.allocator, self.schema.columns[column_index..][0..span], row_count, maps[map_index]);
            defer triplets.deinit(self.allocator);

            metas[column_index] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index], triplets.key);
            metas_len += 1;
            total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index].total_uncompressed_size) catch return error.RowCountOverflow;
            total_compressed = std.math.add(i64, total_compressed, metas[column_index].total_compressed_size) catch return error.RowCountOverflow;

            if (triplets.value) |value_triplets| {
                metas[column_index + 1] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index + 1], value_triplets);
                metas_len += 1;
                total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index + 1].total_uncompressed_size) catch return error.RowCountOverflow;
                total_compressed = std.math.add(i64, total_compressed, metas[column_index + 1].total_compressed_size) catch return error.RowCountOverflow;
            }

            column_index += span;
            map_index += 1;
        }
        if (map_index != maps.len or metas_len != metas.len) return error.InvalidColumnData;

        try self.row_groups.append(self.allocator, .{
            .columns = metas,
            .total_byte_size = total_uncompressed,
            .total_compressed_size = total_compressed,
            .num_rows = try types.intCastI64(row_count),
        });
    }

    pub fn writeRowGroupMaps(self: *StreamWriter, row_count: usize, maps: []const types.ColumnMapData) !void {
        if (!self.started or self.finished) return error.InvalidParquetFile;

        const metas = try self.allocator.alloc(types.ColumnChunkMeta, self.schema.columns.len);
        var metas_len: usize = 0;
        errdefer {
            for (metas[0..metas_len]) |meta| self.freeColumnMeta(meta);
            self.allocator.free(metas);
        }

        var total_uncompressed: i64 = 0;
        var total_compressed: i64 = 0;
        var column_index: usize = 0;
        var map_index: usize = 0;
        while (column_index < self.schema.columns.len) {
            if (map_index >= maps.len) return error.InvalidColumnData;
            const key_column = self.schema.columns[column_index];
            const value_index = mapValueColumnIndex(self.schema.columns, column_index);
            const value_column = if (value_index) |idx| self.schema.columns[idx] else null;
            const map_metas = try self.writeMapColumnChunks(row_count, key_column, value_column, maps[map_index]);
            metas[column_index] = map_metas.key;
            metas_len += 1;
            total_uncompressed = std.math.add(i64, total_uncompressed, map_metas.key.total_uncompressed_size) catch return error.RowCountOverflow;
            total_compressed = std.math.add(i64, total_compressed, map_metas.key.total_compressed_size) catch return error.RowCountOverflow;
            if (map_metas.value) |value_meta| {
                const idx = value_index orelse return error.InvalidColumnData;
                metas[idx] = value_meta;
                metas_len += 1;
                total_uncompressed = std.math.add(i64, total_uncompressed, value_meta.total_uncompressed_size) catch return error.RowCountOverflow;
                total_compressed = std.math.add(i64, total_compressed, value_meta.total_compressed_size) catch return error.RowCountOverflow;
            }
            column_index += if (value_index != null) 2 else 1;
            map_index += 1;
        }
        if (map_index != maps.len or metas_len != metas.len) return error.InvalidColumnData;

        try self.row_groups.append(self.allocator, .{
            .columns = metas,
            .total_byte_size = total_uncompressed,
            .total_compressed_size = total_compressed,
            .num_rows = try types.intCastI64(row_count),
        });
    }

    pub fn writeRowGroupMixed(self: *StreamWriter, row_count: usize, columns: []const types.ColumnWriteData) !void {
        if (!self.started or self.finished) return error.InvalidParquetFile;

        const metas = try self.allocator.alloc(types.ColumnChunkMeta, self.schema.columns.len);
        var metas_len: usize = 0;
        errdefer {
            for (metas[0..metas_len]) |meta| self.freeColumnMeta(meta);
            self.allocator.free(metas);
        }

        var total_uncompressed: i64 = 0;
        var total_compressed: i64 = 0;
        var column_index: usize = 0;
        var input_index: usize = 0;
        while (column_index < self.schema.columns.len) {
            if (input_index >= columns.len) return error.InvalidColumnData;
            const schema_column = self.schema.columns[column_index];
            switch (columns[input_index]) {
                .flat => |column_data| {
                    try column_data.validate(schema_column, row_count);
                    metas[column_index] = try self.writeColumnChunk(row_count, schema_column, column_data);
                    metas_len += 1;
                    total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index].total_uncompressed_size) catch return error.RowCountOverflow;
                    total_compressed = std.math.add(i64, total_compressed, metas[column_index].total_compressed_size) catch return error.RowCountOverflow;
                    column_index += 1;
                },
                .triplets => |column_data| {
                    try validateTripletColumnData(schema_column, row_count, column_data);
                    metas[column_index] = try self.writeTripletColumnChunk(row_count, schema_column, column_data);
                    metas_len += 1;
                    total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index].total_uncompressed_size) catch return error.RowCountOverflow;
                    total_compressed = std.math.add(i64, total_compressed, metas[column_index].total_compressed_size) catch return error.RowCountOverflow;
                    column_index += 1;
                },
                .list => |column_data| {
                    try validateListColumnData(schema_column, row_count, column_data);
                    metas[column_index] = try self.writeListColumnChunk(row_count, schema_column, column_data);
                    metas_len += 1;
                    total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index].total_uncompressed_size) catch return error.RowCountOverflow;
                    total_compressed = std.math.add(i64, total_compressed, metas[column_index].total_compressed_size) catch return error.RowCountOverflow;
                    column_index += 1;
                },
                .nested_list => |column_data| {
                    var triplets = try buildNestedListTripletData(self.allocator, schema_column, row_count, column_data);
                    defer freeBuiltTripletData(self.allocator, &triplets);
                    metas[column_index] = try self.writeTripletColumnChunk(row_count, schema_column, triplets);
                    metas_len += 1;
                    total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index].total_uncompressed_size) catch return error.RowCountOverflow;
                    total_compressed = std.math.add(i64, total_compressed, metas[column_index].total_compressed_size) catch return error.RowCountOverflow;
                    column_index += 1;
                },
                .nested_map => |column_data| {
                    const span = nestedMapColumnSpan(self.schema.columns, column_index) orelse return error.UnsupportedNestedSchema;
                    var triplets = try buildNestedMapTripletData(self.allocator, self.schema.columns[column_index..][0..span], row_count, column_data);
                    defer triplets.deinit(self.allocator);

                    metas[column_index] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index], triplets.outer_key);
                    metas_len += 1;
                    total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index].total_uncompressed_size) catch return error.RowCountOverflow;
                    total_compressed = std.math.add(i64, total_compressed, metas[column_index].total_compressed_size) catch return error.RowCountOverflow;

                    metas[column_index + 1] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index + 1], triplets.inner_key);
                    metas_len += 1;
                    total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index + 1].total_uncompressed_size) catch return error.RowCountOverflow;
                    total_compressed = std.math.add(i64, total_compressed, metas[column_index + 1].total_compressed_size) catch return error.RowCountOverflow;

                    if (triplets.inner_value) |value_triplets| {
                        metas[column_index + 2] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index + 2], value_triplets);
                        metas_len += 1;
                        total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index + 2].total_uncompressed_size) catch return error.RowCountOverflow;
                        total_compressed = std.math.add(i64, total_compressed, metas[column_index + 2].total_compressed_size) catch return error.RowCountOverflow;
                    }
                    column_index += span;
                },
                .list_map => |column_data| {
                    const span = listMapColumnSpan(self.schema.columns, column_index) orelse return error.UnsupportedNestedSchema;
                    var triplets = try buildListMapTripletData(self.allocator, self.schema.columns[column_index..][0..span], row_count, column_data);
                    defer triplets.deinit(self.allocator);

                    metas[column_index] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index], triplets.key);
                    metas_len += 1;
                    total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index].total_uncompressed_size) catch return error.RowCountOverflow;
                    total_compressed = std.math.add(i64, total_compressed, metas[column_index].total_compressed_size) catch return error.RowCountOverflow;

                    if (triplets.value) |value_triplets| {
                        metas[column_index + 1] = try self.writeTripletColumnChunk(row_count, self.schema.columns[column_index + 1], value_triplets);
                        metas_len += 1;
                        total_uncompressed = std.math.add(i64, total_uncompressed, metas[column_index + 1].total_uncompressed_size) catch return error.RowCountOverflow;
                        total_compressed = std.math.add(i64, total_compressed, metas[column_index + 1].total_compressed_size) catch return error.RowCountOverflow;
                    }
                    column_index += span;
                },
                .map => |column_data| {
                    const value_index = mapValueColumnIndex(self.schema.columns, column_index);
                    const value_column = if (value_index) |idx| self.schema.columns[idx] else null;
                    const map_metas = try self.writeMapColumnChunks(row_count, schema_column, value_column, column_data);
                    metas[column_index] = map_metas.key;
                    metas_len += 1;
                    total_uncompressed = std.math.add(i64, total_uncompressed, map_metas.key.total_uncompressed_size) catch return error.RowCountOverflow;
                    total_compressed = std.math.add(i64, total_compressed, map_metas.key.total_compressed_size) catch return error.RowCountOverflow;
                    if (map_metas.value) |value_meta| {
                        const idx = value_index orelse return error.InvalidColumnData;
                        metas[idx] = value_meta;
                        metas_len += 1;
                        total_uncompressed = std.math.add(i64, total_uncompressed, value_meta.total_uncompressed_size) catch return error.RowCountOverflow;
                        total_compressed = std.math.add(i64, total_compressed, value_meta.total_compressed_size) catch return error.RowCountOverflow;
                    }
                    column_index += if (value_index != null) 2 else 1;
                },
            }
            input_index += 1;
        }
        if (input_index != columns.len or metas_len != metas.len) return error.InvalidColumnData;

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
                    .value_count = try types.intCastI64(page_rows),
                    .statistics = page_stats,
                });
            }
            if (row_count == 0) break;
            row_start += page_rows;
        }

        const owned_page_entries = try page_entries.toOwnedSlice(self.allocator);
        const column_path = try self.columnChunkPath(column);
        errdefer if (column_path.owned) |path| self.allocator.free(path);
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
            .path = column_path.bytes,
            .owned_path = column_path.owned,
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

    fn writeTripletColumnChunk(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnTripletData) !types.ColumnChunkMeta {
        const max_definition_level = writerMaxDefinitionLevel(column);
        const max_repetition_level = writerMaxRepetitionLevel(column);
        const value_count = data.values.valueCount();
        const null_count = try types.intCastI64(data.definition_levels.len - value_count);
        const column_stats = try self.buildStatistics(column, data.values, null_count);
        errdefer self.freeStatistics(column_stats);

        const row_offsets = try buildTripletRowOffsets(self.allocator, data.repetition_levels, max_repetition_level, row_count);
        defer self.allocator.free(row_offsets);
        const value_offsets = try buildTripletValueOffsets(self.allocator, data.definition_levels, row_offsets, max_definition_level);
        defer self.allocator.free(value_offsets);

        var total_uncompressed: i64 = 0;
        var total_compressed: i64 = 0;
        var page_entries: std.ArrayList(types.PageIndexEntry) = .empty;
        defer page_entries.deinit(self.allocator);
        errdefer self.freePageIndexEntries(page_entries.items);

        const first_page_offset = try types.intCastI64(@intCast(self.offset));
        var row_start: usize = 0;
        while (row_start < row_count or (row_count == 0 and row_start == 0)) {
            const page_rows = if (row_count == 0) 0 else @min(self.options.max_page_rows, row_count - row_start);
            const page_data = try sliceTripletData(data, column, row_offsets, value_offsets, row_start, page_rows);
            const page_sizes = try self.writeTripletColumnPage(page_rows, column, page_data);
            total_uncompressed = std.math.add(i64, total_uncompressed, page_sizes.uncompressed_size) catch return error.RowCountOverflow;
            total_compressed = std.math.add(i64, total_compressed, page_sizes.compressed_size) catch return error.RowCountOverflow;
            if (page_rows > 0) {
                const page_value_count = page_data.values.valueCount();
                const page_null_count = try types.intCastI64(page_data.definition_levels.len - page_value_count);
                const page_stats = try self.buildStatistics(column, page_data.values, page_null_count);
                errdefer self.freeStatistics(page_stats);
                try page_entries.append(self.allocator, .{
                    .offset = page_sizes.data_page_offset,
                    .compressed_page_size = std.math.cast(i32, page_sizes.compressed_size) orelse return error.RowCountOverflow,
                    .first_row_index = try types.intCastI64(row_start),
                    .row_count = try types.intCastI64(page_rows),
                    .value_count = try types.intCastI64(page_data.definition_levels.len),
                    .statistics = page_stats,
                });
            }
            if (row_count == 0) break;
            row_start += page_rows;
        }

        const owned_page_entries = try page_entries.toOwnedSlice(self.allocator);
        const column_path = try self.columnChunkPath(column);
        errdefer if (column_path.owned) |path| self.allocator.free(path);
        return .{
            .physical_type = column.column_type.physical,
            .encodings = &page_encodings,
            .path = column_path.bytes,
            .owned_path = column_path.owned,
            .codec = self.options.compression,
            .num_values = try types.intCastI64(data.definition_levels.len),
            .total_uncompressed_size = total_uncompressed,
            .total_compressed_size = total_compressed,
            .data_page_offset = first_page_offset,
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

    fn writeTripletColumnPage(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnTripletData) !PageSizes {
        return switch (self.options.data_page_version) {
            .v1 => try self.writeTripletColumnPageV1(row_count, column, data),
            .v2 => try self.writeTripletColumnPageV2(row_count, column, data),
        };
    }

    fn writeListColumnChunk(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnListData) !types.ColumnChunkMeta {
        const triplets = try buildListTripletData(self.allocator, column, row_count, data);
        defer self.allocator.free(triplets.definition_levels);
        defer self.allocator.free(triplets.repetition_levels);
        return self.writeTripletColumnChunk(row_count, column, triplets);
    }

    fn writeMapColumnChunks(self: *StreamWriter, row_count: usize, key_column: types.Column, value_column: ?types.Column, data: types.ColumnMapData) !MapColumnMetas {
        const triplets = try buildMapTripletData(self.allocator, key_column, value_column, row_count, data);
        defer self.allocator.free(triplets.key_definition_levels);
        defer if (triplets.value_definition_levels) |levels| self.allocator.free(levels);
        defer self.allocator.free(triplets.repetition_levels);

        const key_triplets: types.ColumnTripletData = .{
            .values = data.keys,
            .definition_levels = triplets.key_definition_levels,
            .repetition_levels = triplets.repetition_levels,
        };
        const key_meta = try self.writeTripletColumnChunk(row_count, key_column, key_triplets);
        errdefer self.freeColumnMeta(key_meta);

        const value_meta = if (value_column) |column| blk: {
            const values = data.values orelse return error.InvalidColumnData;
            const value_definition_levels = triplets.value_definition_levels orelse return error.InvalidColumnData;
            const value_triplets: types.ColumnTripletData = .{
                .values = stripColumnValidity(values),
                .definition_levels = value_definition_levels,
                .repetition_levels = triplets.repetition_levels,
            };
            break :blk try self.writeTripletColumnChunk(row_count, column, value_triplets);
        } else null;

        return .{ .key = key_meta, .value = value_meta };
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

    fn writeTripletColumnPageV1(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnTripletData) !PageSizes {
        const max_definition_level = writerMaxDefinitionLevel(column);
        const max_repetition_level = writerMaxRepetitionLevel(column);
        const page_null_count = try types.intCastI64(data.definition_levels.len - data.values.valueCount());
        const page_stats = try self.buildStatistics(column, data.values, page_null_count);
        defer self.freeStatistics(page_stats);

        var page_body: std.Io.Writer.Allocating = .init(self.allocator);
        defer page_body.deinit();

        if (max_repetition_level > 0) {
            try plain.encodeLevels(self.allocator, &page_body.writer, data.repetition_levels, max_repetition_level);
        }
        if (max_definition_level > 0) {
            try plain.encodeLevels(self.allocator, &page_body.writer, data.definition_levels, max_definition_level);
        }
        const encoding = self.pageValueEncoding(column, data.values);
        try self.encodePageValues(&page_body.writer, data.values, column.column_type, encoding);

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
                .num_values = try types.intCastI32(data.definition_levels.len),
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
        _ = row_count;
        return .{
            .data_page_offset = data_page_offset,
            .uncompressed_size = uncompressed_total_size,
            .compressed_size = compressed_total_size,
        };
    }

    fn writeTripletColumnPageV2(self: *StreamWriter, row_count: usize, column: types.Column, data: types.ColumnTripletData) !PageSizes {
        const max_definition_level = writerMaxDefinitionLevel(column);
        const max_repetition_level = writerMaxRepetitionLevel(column);
        const page_null_count = try types.intCastI64(data.definition_levels.len - data.values.valueCount());
        const page_stats = try self.buildStatistics(column, data.values, page_null_count);
        defer self.freeStatistics(page_stats);

        var repetition_levels: std.Io.Writer.Allocating = .init(self.allocator);
        defer repetition_levels.deinit();
        if (max_repetition_level > 0) {
            try plain.encodeLevelsBody(&repetition_levels.writer, data.repetition_levels, max_repetition_level);
        }

        var definition_levels: std.Io.Writer.Allocating = .init(self.allocator);
        defer definition_levels.deinit();
        if (max_definition_level > 0) {
            try plain.encodeLevelsBody(&definition_levels.writer, data.definition_levels, max_definition_level);
        }

        var values_body: std.Io.Writer.Allocating = .init(self.allocator);
        defer values_body.deinit();
        const encoding = self.pageValueEncoding(column, data.values);
        try self.encodePageValues(&values_body.writer, data.values, column.column_type, encoding);

        const values = values_body.written();
        const compressed_values = try compressPage(self.allocator, self.options.compression, values);
        defer compressed_values.deinit(self.allocator);

        const rep_len = repetition_levels.written().len;
        const def_len = definition_levels.written().len;
        const levels_len = std.math.add(usize, rep_len, def_len) catch return error.RowCountOverflow;
        const uncompressed_size = try types.intCastI32(std.math.add(usize, levels_len, values.len) catch return error.RowCountOverflow);
        const compressed_size = try types.intCastI32(std.math.add(usize, levels_len, compressed_values.data.len) catch return error.RowCountOverflow);
        const page_header: types.PageHeader = .{
            .page_type = .data_page_v2,
            .uncompressed_page_size = uncompressed_size,
            .compressed_page_size = compressed_size,
            .crc = optionalPageCrc(self.options, &.{ repetition_levels.written(), definition_levels.written(), compressed_values.data }),
            .data_page_header_v2 = .{
                .num_values = try types.intCastI32(data.definition_levels.len),
                .num_nulls = std.math.cast(i32, page_null_count) orelse return error.RowCountOverflow,
                .num_rows = try types.intCastI32(row_count),
                .encoding = encoding,
                .definition_levels_byte_length = try types.intCastI32(def_len),
                .repetition_levels_byte_length = try types.intCastI32(rep_len),
                .is_compressed = self.options.compression != .uncompressed,
                .statistics = page_stats,
            },
        };

        var header_buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer header_buf.deinit();
        try thrift.writePageHeader(&header_buf.writer, page_header);

        const data_page_offset = try types.intCastI64(@intCast(self.offset));
        try self.writeAll(header_buf.written());
        try self.writeAll(repetition_levels.written());
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
        if (column.owned_path) |path| self.allocator.free(path);
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

    fn columnChunkPath(self: *StreamWriter, column: types.Column) !ColumnChunkPath {
        if (column.path.len == 0) return .{ .bytes = column.name };
        if (column.path.len == 1 and std.mem.eql(u8, column.path[0], column.name)) return .{ .bytes = column.name };

        var len: usize = column.path.len - 1;
        for (column.path) |part| {
            len = std.math.add(usize, len, part.len) catch return error.RowCountOverflow;
        }
        const path = try self.allocator.alloc(u8, len);
        var offset: usize = 0;
        for (column.path, 0..) |part, index| {
            if (index > 0) {
                path[offset] = '.';
                offset += 1;
            }
            @memcpy(path[offset..][0..part.len], part);
            offset += part.len;
        }
        return .{ .bytes = path, .owned = path };
    }
};

const ColumnChunkPath = struct {
    bytes: []const u8,
    owned: ?[]u8 = null,
};

const MapColumnMetas = struct {
    key: types.ColumnChunkMeta,
    value: ?types.ColumnChunkMeta = null,
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
            .int96 => |d| .{ .int96 = .{ .values = d.values[row_start..][0..row_count] } },
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
            .int96 => |d| blk: {
                const validity = d.validity.?[row_start..][0..row_count];
                const values = try optionalValueSlice([]const u8, d.values, validity, value_offset);
                break :blk .{ .int96 = .{ .values = values, .validity = validity } };
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

fn sliceTripletData(data: types.ColumnTripletData, column: types.Column, row_offsets: []const usize, value_offsets: []const usize, row_start: usize, row_count: usize) !types.ColumnTripletData {
    const row_end = std.math.add(usize, row_start, row_count) catch return error.InvalidColumnData;
    if (row_end >= row_offsets.len or row_end >= value_offsets.len) return error.InvalidColumnData;

    const level_start = row_offsets[row_start];
    const level_end = row_offsets[row_end];
    const value_start = value_offsets[row_start];
    const value_end = value_offsets[row_end];
    if (level_start > level_end or level_end > data.definition_levels.len or level_end > data.repetition_levels.len) return error.InvalidColumnData;
    if (value_start > value_end or value_end > data.values.valueCount()) return error.InvalidColumnData;

    return .{
        .values = try sliceRequiredValues(data.values, column, value_start, value_end - value_start),
        .definition_levels = data.definition_levels[level_start..level_end],
        .repetition_levels = data.repetition_levels[level_start..level_end],
    };
}

fn sliceRequiredValues(data: types.ColumnData, column: types.Column, value_start: usize, value_count: usize) !types.ColumnData {
    _ = column;
    return switch (data) {
        .boolean => |d| .{ .boolean = .{ .values = d.values[value_start..][0..value_count] } },
        .int32 => |d| .{ .int32 = .{ .values = d.values[value_start..][0..value_count] } },
        .int64 => |d| .{ .int64 = .{ .values = d.values[value_start..][0..value_count] } },
        .int96 => |d| .{ .int96 = .{ .values = d.values[value_start..][0..value_count] } },
        .float => |d| .{ .float = .{ .values = d.values[value_start..][0..value_count] } },
        .double => |d| .{ .double = .{ .values = d.values[value_start..][0..value_count] } },
        .byte_array => |d| .{ .byte_array = .{ .values = d.values[value_start..][0..value_count] } },
        .fixed_len_byte_array => |d| .{ .fixed_len_byte_array = .{ .values = d.values[value_start..][0..value_count] } },
    };
}

fn stripColumnValidity(data: types.ColumnData) types.ColumnData {
    return switch (data) {
        .boolean => |d| .{ .boolean = .{ .values = d.values } },
        .int32 => |d| .{ .int32 = .{ .values = d.values } },
        .int64 => |d| .{ .int64 = .{ .values = d.values } },
        .int96 => |d| .{ .int96 = .{ .values = d.values } },
        .float => |d| .{ .float = .{ .values = d.values } },
        .double => |d| .{ .double = .{ .values = d.values } },
        .byte_array => |d| .{ .byte_array = .{ .values = d.values } },
        .fixed_len_byte_array => |d| .{ .fixed_len_byte_array = .{ .values = d.values } },
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
        .lz4_raw => blk: {
            const compressed = try lz4.compress(allocator, body);
            break :blk .{ .data = compressed, .owned = compressed };
        },
        .zstd => blk: {
            const compressed = try zstd.compressFrame(allocator, body);
            break :blk .{ .data = compressed, .owned = compressed };
        },
        else => error.UnsupportedCompression,
    };
}

fn validateTripletColumnData(column: types.Column, row_count: usize, data: types.ColumnTripletData) !void {
    try validateRequiredColumnValues(column, data.values);
    if (data.definition_levels.len != data.repetition_levels.len) return error.InvalidColumnData;

    const max_definition_level = writerMaxDefinitionLevel(column);
    const max_repetition_level = writerMaxRepetitionLevel(column);
    if (column.list_info) |list_info| {
        try validateSupportedListColumnShape(column, list_info);
    } else if (column.map_info) |map_info| {
        try validateSupportedMapTripletColumnShape(column, map_info);
    } else if (column.repetition == .repeated and (max_definition_level != 1 or max_repetition_level != 1)) {
        return error.UnsupportedNestedSchema;
    }
    if (column.repetition != .repeated and max_repetition_level != 0) return error.InvalidSchema;

    const rows = try tripletRowCount(data.repetition_levels, max_repetition_level);
    if (rows != row_count) return error.InvalidColumnData;

    var present_count: usize = 0;
    for (data.definition_levels) |level| {
        if (level > max_definition_level) return error.InvalidColumnData;
        if (level == max_definition_level) present_count += 1;
    }
    for (data.repetition_levels) |level| {
        if (level > max_repetition_level) return error.InvalidColumnData;
    }
    if (data.values.valueCount() != present_count) return error.InvalidColumnData;
}

fn validateListColumnData(column: types.Column, row_count: usize, data: types.ColumnListData) !void {
    const list_info = column.list_info orelse return error.UnsupportedNestedSchema;
    try validateSupportedOneLevelListColumnShape(column, list_info);
    if (data.values.physicalType() != column.column_type.physical) return error.InvalidColumnData;
    if (data.offsets.len != row_count + 1) return error.InvalidColumnData;
    if (data.offsets[0] != 0) return error.InvalidColumnData;

    const list_optional = list_info.list_definition_level > 0;
    if (list_optional) {
        const validity = data.validity orelse return error.InvalidColumnData;
        if (validity.len != row_count) return error.InvalidColumnData;
    } else if (data.validity != null) {
        return error.InvalidColumnData;
    }

    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        if (data.offsets[row] > data.offsets[row + 1]) return error.InvalidColumnData;
        if (data.validity) |validity| {
            if (!validity[row] and data.offsets[row] != data.offsets[row + 1]) return error.InvalidColumnData;
        }
    }

    const element_count = data.offsets[row_count];
    const element_optional = column.max_definition_level > list_info.list_definition_level + 1;
    if (element_optional) {
        const validity = data.values.validity() orelse return error.InvalidColumnData;
        if (validity.len != element_count) return error.InvalidColumnData;
        const valid_count = try countValid(validity);
        if (valid_count != data.values.valueCount()) return error.InvalidColumnData;
    } else {
        if (data.values.validity() != null) return error.InvalidColumnData;
        if (data.values.valueCount() != element_count) return error.InvalidColumnData;
    }

    var element_column = column;
    element_column.repetition = if (element_optional) .optional else .required;
    element_column.max_definition_level = 0;
    element_column.max_repetition_level = 0;
    element_column.list_info = null;
    element_column.path = &.{};
    try data.values.validate(element_column, element_count);
}

fn buildListTripletData(allocator: std.mem.Allocator, column: types.Column, row_count: usize, data: types.ColumnListData) !types.ColumnTripletData {
    const list_info = column.list_info orelse return error.UnsupportedNestedSchema;
    try validateSupportedOneLevelListColumnShape(column, list_info);
    const element_count = data.offsets[row_count];
    var placeholder_count: usize = 0;
    var placeholder_row: usize = 0;
    while (placeholder_row < row_count) : (placeholder_row += 1) {
        const list_present = if (data.validity) |validity| validity[placeholder_row] else true;
        if (!list_present or data.offsets[placeholder_row] == data.offsets[placeholder_row + 1]) placeholder_count += 1;
    }
    const level_count = std.math.add(usize, element_count, placeholder_count) catch return error.RowCountOverflow;
    const definition_levels = try allocator.alloc(u16, level_count);
    errdefer allocator.free(definition_levels);
    const repetition_levels = try allocator.alloc(u16, level_count);
    errdefer allocator.free(repetition_levels);

    const element_optional = column.max_definition_level > list_info.list_definition_level + 1;
    const element_validity = data.values.validity();

    var level_index: usize = 0;
    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        const start = data.offsets[row];
        const end = data.offsets[row + 1];
        const list_present = if (data.validity) |validity| validity[row] else true;
        if (!list_present) {
            definition_levels[level_index] = list_info.list_definition_level - 1;
            repetition_levels[level_index] = 0;
            level_index += 1;
            continue;
        }
        if (start == end) {
            definition_levels[level_index] = list_info.list_definition_level;
            repetition_levels[level_index] = 0;
            level_index += 1;
            continue;
        }

        var element = start;
        while (element < end) : (element += 1) {
            repetition_levels[level_index] = if (element == start) 0 else 1;
            definition_levels[level_index] = if (element_optional and !element_validity.?[element])
                column.max_definition_level - 1
            else
                column.max_definition_level;
            level_index += 1;
        }
    }
    if (level_index != level_count) return error.InvalidColumnData;

    return .{
        .values = stripColumnValidity(data.values),
        .definition_levels = definition_levels,
        .repetition_levels = repetition_levels,
    };
}

fn validateNestedListColumnData(column: types.Column, row_count: usize, data: types.ColumnNestedListData) !void {
    const list_info = column.list_info orelse return error.UnsupportedNestedSchema;
    try validateSupportedListColumnShape(column, list_info);
    const level_count = writerListLevelCount(column);
    if (data.levels.len != level_count) return error.InvalidColumnData;
    if (data.values.physicalType() != column.column_type.physical) return error.InvalidColumnData;

    var parent_count = row_count;
    for (data.levels, 0..) |level, level_index| {
        if (level.offsets.len != parent_count + 1) return error.InvalidColumnData;
        if (level.offsets[0] != 0) return error.InvalidColumnData;
        const list_optional = writerListLevelOptional(column, level_index);
        if (list_optional) {
            const validity = level.validity orelse return error.InvalidColumnData;
            if (validity.len != parent_count) return error.InvalidColumnData;
        } else if (level.validity != null) {
            return error.InvalidColumnData;
        }

        var parent_index: usize = 0;
        while (parent_index < parent_count) : (parent_index += 1) {
            if (level.offsets[parent_index] > level.offsets[parent_index + 1]) return error.InvalidColumnData;
            if (level.validity) |validity| {
                if (!validity[parent_index] and level.offsets[parent_index] != level.offsets[parent_index + 1]) return error.InvalidColumnData;
            }
        }
        parent_count = level.offsets[parent_count];
    }

    const leaf_slot_count = parent_count;
    const leaf_optional = writerNestedListLeafOptional(column);
    if (leaf_optional) {
        const validity = data.values.validity() orelse return error.InvalidColumnData;
        if (validity.len != leaf_slot_count) return error.InvalidColumnData;
        const valid_count = try countValid(validity);
        if (valid_count != data.values.valueCount()) return error.InvalidColumnData;
    } else {
        if (data.values.validity() != null) return error.InvalidColumnData;
        if (data.values.valueCount() != leaf_slot_count) return error.InvalidColumnData;
    }

    var leaf_column = column;
    leaf_column.repetition = if (leaf_optional) .optional else .required;
    leaf_column.max_definition_level = 0;
    leaf_column.max_repetition_level = 0;
    leaf_column.list_info = null;
    leaf_column.nested_logical_info = &.{};
    leaf_column.repeated_level_info = &.{};
    leaf_column.path = &.{};
    try data.values.validate(leaf_column, leaf_slot_count);
}

const NestedListTripletBuildContext = struct {
    allocator: std.mem.Allocator,
    column: types.Column,
    data: types.ColumnNestedListData,
    definition_levels: std.ArrayList(u16) = .empty,
    repetition_levels: std.ArrayList(u16) = .empty,
    leaf_optional: bool,
    leaf_validity: ?[]const bool,

    fn appendLevel(self: *NestedListTripletBuildContext, definition_level: u16, repetition_level: u16) !void {
        try self.definition_levels.append(self.allocator, definition_level);
        try self.repetition_levels.append(self.allocator, repetition_level);
    }

    fn emitParent(self: *NestedListTripletBuildContext, level_index: usize, parent_index: usize, repetition_level: u16) !void {
        const level = self.data.levels[level_index];
        const start = level.offsets[parent_index];
        const end = level.offsets[parent_index + 1];
        const list_definition_level = writerListLevelDefinition(self.column, level_index);
        if (writerListLevelOptional(self.column, level_index) and !level.validity.?[parent_index]) {
            try self.appendLevel(list_definition_level - 1, repetition_level);
            return;
        }
        if (start == end) {
            try self.appendLevel(list_definition_level, repetition_level);
            return;
        }

        var child = start;
        while (child < end) : (child += 1) {
            const child_repetition_level: u16 = if (child == start) repetition_level else @intCast(level_index + 1);
            if (level_index + 1 < self.data.levels.len) {
                try self.emitParent(level_index + 1, child, child_repetition_level);
            } else {
                const present = !self.leaf_optional or self.leaf_validity.?[child];
                try self.appendLevel(if (present) self.column.max_definition_level else self.column.max_definition_level - 1, child_repetition_level);
            }
        }
    }
};

fn buildNestedListTripletData(allocator: std.mem.Allocator, column: types.Column, row_count: usize, data: types.ColumnNestedListData) !types.ColumnTripletData {
    try validateNestedListColumnData(column, row_count, data);
    var ctx = NestedListTripletBuildContext{
        .allocator = allocator,
        .column = column,
        .data = data,
        .leaf_optional = writerNestedListLeafOptional(column),
        .leaf_validity = data.values.validity(),
    };
    errdefer ctx.definition_levels.deinit(allocator);
    errdefer ctx.repetition_levels.deinit(allocator);

    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        try ctx.emitParent(0, row, 0);
    }

    const definition_levels = try ctx.definition_levels.toOwnedSlice(allocator);
    errdefer allocator.free(definition_levels);
    const repetition_levels = try ctx.repetition_levels.toOwnedSlice(allocator);

    return .{
        .values = stripColumnValidity(data.values),
        .definition_levels = definition_levels,
        .repetition_levels = repetition_levels,
    };
}

fn freeBuiltTripletData(allocator: std.mem.Allocator, data: *types.ColumnTripletData) void {
    allocator.free(data.definition_levels);
    allocator.free(data.repetition_levels);
    data.definition_levels = &.{};
    data.repetition_levels = &.{};
}

const MapTripletLevels = struct {
    key_definition_levels: []u16,
    value_definition_levels: ?[]u16 = null,
    repetition_levels: []u16,
};

const ListMapTripletData = struct {
    key: types.ColumnTripletData,
    value: ?types.ColumnTripletData = null,

    fn deinit(self: *ListMapTripletData, allocator: std.mem.Allocator) void {
        freeBuiltTripletData(allocator, &self.key);
        if (self.value) |*value| freeBuiltTripletData(allocator, value);
    }
};

const NestedMapTripletData = struct {
    outer_key: types.ColumnTripletData,
    inner_key: types.ColumnTripletData,
    inner_value: ?types.ColumnTripletData = null,

    fn deinit(self: *NestedMapTripletData, allocator: std.mem.Allocator) void {
        freeBuiltTripletData(allocator, &self.outer_key);
        freeBuiltTripletData(allocator, &self.inner_key);
        if (self.inner_value) |*inner_value| freeBuiltTripletData(allocator, inner_value);
    }
};

fn validateListMapColumnData(columns: []const types.Column, row_count: usize, data: types.ColumnListMapData) !void {
    if (columns.len != 1 and columns.len != 2) return error.UnsupportedNestedSchema;
    try validateSupportedListMapColumnGroup(columns);
    if (columns.len == 2 and data.values == null) return error.InvalidColumnData;
    if (columns.len == 1 and data.values != null) return error.InvalidColumnData;

    const key_column = columns[0];
    const map_info = key_column.map_info orelse return error.UnsupportedNestedSchema;
    const list_info = key_column.nested_logical_info[0];
    const map_logical_info = key_column.nested_logical_info[1];

    if (data.list.offsets.len != row_count + 1) return error.InvalidColumnData;
    if (data.list.offsets[0] != 0) return error.InvalidColumnData;
    if (list_info.optional) {
        const validity = data.list.validity orelse return error.InvalidColumnData;
        if (validity.len != row_count) return error.InvalidColumnData;
    } else if (data.list.validity != null) {
        return error.InvalidColumnData;
    }
    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        if (data.list.offsets[row] > data.list.offsets[row + 1]) return error.InvalidColumnData;
        if (data.list.validity) |validity| {
            if (!validity[row] and data.list.offsets[row] != data.list.offsets[row + 1]) return error.InvalidColumnData;
        }
    }
    const list_entry_count = data.list.offsets[row_count];

    if (data.map.keys.physicalType() != key_column.column_type.physical) return error.InvalidColumnData;
    if (data.map.keys.validity() != null) return error.InvalidColumnData;
    if (data.map.offsets.len != list_entry_count + 1) return error.InvalidColumnData;
    if (data.map.offsets[0] != 0) return error.InvalidColumnData;
    if (map_logical_info.optional) {
        const validity = data.map.validity orelse return error.InvalidColumnData;
        if (validity.len != list_entry_count) return error.InvalidColumnData;
    } else if (data.map.validity != null) {
        return error.InvalidColumnData;
    }
    var list_entry: usize = 0;
    while (list_entry < list_entry_count) : (list_entry += 1) {
        if (data.map.offsets[list_entry] > data.map.offsets[list_entry + 1]) return error.InvalidColumnData;
        if (data.map.validity) |validity| {
            if (!validity[list_entry] and data.map.offsets[list_entry] != data.map.offsets[list_entry + 1]) return error.InvalidColumnData;
        }
    }
    const map_entry_count = data.map.offsets[list_entry_count];
    if (data.map.keys.valueCount() != map_entry_count) return error.InvalidColumnData;
    try validateRequiredColumnDataForSlotCount(key_column, data.map.keys, map_entry_count);

    if (columns.len == 2) {
        const value_column = columns[1];
        const value_data = data.values orelse return error.InvalidColumnData;
        if (value_data.physicalType() != value_column.column_type.physical) return error.InvalidColumnData;
        const value_optional = value_column.max_definition_level > map_info.map_definition_level + 1;
        if (value_optional) {
            const validity = value_data.validity() orelse return error.InvalidColumnData;
            if (validity.len != map_entry_count) return error.InvalidColumnData;
            const valid_count = try countValid(validity);
            if (valid_count != value_data.valueCount()) return error.InvalidColumnData;
        } else {
            if (value_data.validity() != null) return error.InvalidColumnData;
            if (value_data.valueCount() != map_entry_count) return error.InvalidColumnData;
        }
        var leaf_column = value_column;
        leaf_column.repetition = if (value_optional) .optional else .required;
        leaf_column.max_definition_level = 0;
        leaf_column.max_repetition_level = 0;
        leaf_column.map_info = null;
        leaf_column.nested_logical_info = &.{};
        leaf_column.repeated_level_info = &.{};
        leaf_column.path = &.{};
        try value_data.validate(leaf_column, map_entry_count);
    }
}

fn buildListMapTripletData(allocator: std.mem.Allocator, columns: []const types.Column, row_count: usize, data: types.ColumnListMapData) !ListMapTripletData {
    try validateListMapColumnData(columns, row_count, data);
    const key_column = columns[0];
    const value_column: ?types.Column = if (columns.len == 2) columns[1] else null;
    const list_info = key_column.nested_logical_info[0];
    const map_logical_info = key_column.nested_logical_info[1];
    const map_info = key_column.map_info orelse return error.UnsupportedNestedSchema;

    var key_def: std.ArrayList(u16) = .empty;
    errdefer key_def.deinit(allocator);
    var key_rep: std.ArrayList(u16) = .empty;
    errdefer key_rep.deinit(allocator);
    var value_def: std.ArrayList(u16) = .empty;
    errdefer value_def.deinit(allocator);
    var value_rep: std.ArrayList(u16) = .empty;
    errdefer value_rep.deinit(allocator);

    const value_optional = if (value_column) |column| column.max_definition_level > map_info.map_definition_level + 1 else false;
    const value_validity = if (data.values) |values| values.validity() else null;

    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        const list_start = data.list.offsets[row];
        const list_end = data.list.offsets[row + 1];
        const list_present = if (data.list.validity) |validity| validity[row] else true;
        if (!list_present) {
            try appendNestedMapLevel(allocator, &key_def, &key_rep, list_info.definition_level - 1, 0);
            if (value_column != null) try appendNestedMapLevel(allocator, &value_def, &value_rep, list_info.definition_level - 1, 0);
            continue;
        }
        if (list_start == list_end) {
            try appendNestedMapLevel(allocator, &key_def, &key_rep, list_info.definition_level, 0);
            if (value_column != null) try appendNestedMapLevel(allocator, &value_def, &value_rep, list_info.definition_level, 0);
            continue;
        }

        var list_entry = list_start;
        while (list_entry < list_end) : (list_entry += 1) {
            const list_repetition_level: u16 = if (list_entry == list_start) 0 else 1;
            const map_start = data.map.offsets[list_entry];
            const map_end = data.map.offsets[list_entry + 1];
            const map_present = if (data.map.validity) |validity| validity[list_entry] else true;
            if (!map_present) {
                try appendNestedMapLevel(allocator, &key_def, &key_rep, map_logical_info.definition_level - 1, list_repetition_level);
                if (value_column != null) try appendNestedMapLevel(allocator, &value_def, &value_rep, map_logical_info.definition_level - 1, list_repetition_level);
                continue;
            }
            if (map_start == map_end) {
                try appendNestedMapLevel(allocator, &key_def, &key_rep, map_logical_info.definition_level, list_repetition_level);
                if (value_column != null) try appendNestedMapLevel(allocator, &value_def, &value_rep, map_logical_info.definition_level, list_repetition_level);
                continue;
            }

            var map_entry = map_start;
            while (map_entry < map_end) : (map_entry += 1) {
                const entry_repetition_level: u16 = if (map_entry == map_start) list_repetition_level else 2;
                try appendNestedMapLevel(allocator, &key_def, &key_rep, key_column.max_definition_level, entry_repetition_level);
                if (value_column) |column| {
                    const value_present = !value_optional or value_validity.?[map_entry];
                    try appendNestedMapLevel(allocator, &value_def, &value_rep, if (value_present) column.max_definition_level else column.max_definition_level - 1, entry_repetition_level);
                }
            }
        }
    }

    const key_definition_levels = try key_def.toOwnedSlice(allocator);
    errdefer allocator.free(key_definition_levels);
    const key_repetition_levels = try key_rep.toOwnedSlice(allocator);
    errdefer allocator.free(key_repetition_levels);
    const value_definition_levels = if (value_column != null) try value_def.toOwnedSlice(allocator) else null;
    errdefer if (value_definition_levels) |levels| allocator.free(levels);
    const value_repetition_levels = if (value_column != null) try value_rep.toOwnedSlice(allocator) else null;
    errdefer if (value_repetition_levels) |levels| allocator.free(levels);

    return .{
        .key = .{
            .values = stripColumnValidity(data.map.keys),
            .definition_levels = key_definition_levels,
            .repetition_levels = key_repetition_levels,
        },
        .value = if (value_column != null) .{
            .values = stripColumnValidity(data.values.?),
            .definition_levels = value_definition_levels.?,
            .repetition_levels = value_repetition_levels.?,
        } else null,
    };
}

fn validateNestedMapColumnData(columns: []const types.Column, row_count: usize, data: types.ColumnNestedMapData) !void {
    if (columns.len != 2 and columns.len != 3) return error.UnsupportedNestedSchema;
    try validateSupportedNestedMapColumnGroup(columns);
    if (data.levels.len != 2) return error.InvalidColumnData;
    if (columns.len == 3 and data.values == null) return error.InvalidColumnData;
    if (columns.len == 2 and data.values != null) return error.InvalidColumnData;

    const outer_key = columns[0];
    const inner_key = columns[1];
    const outer_level = data.levels[0];
    const inner_level = data.levels[1];
    const outer_info = outer_key.map_info orelse return error.UnsupportedNestedSchema;
    const inner_info = inner_key.map_info orelse return error.UnsupportedNestedSchema;

    if (outer_level.keys.physicalType() != outer_key.column_type.physical) return error.InvalidColumnData;
    if (outer_level.keys.validity() != null) return error.InvalidColumnData;
    if (outer_level.offsets.len != row_count + 1) return error.InvalidColumnData;
    if (outer_level.offsets[0] != 0) return error.InvalidColumnData;
    if (outer_info.map_definition_level > 0) {
        const validity = outer_level.validity orelse return error.InvalidColumnData;
        if (validity.len != row_count) return error.InvalidColumnData;
    } else if (outer_level.validity != null) {
        return error.InvalidColumnData;
    }
    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        if (outer_level.offsets[row] > outer_level.offsets[row + 1]) return error.InvalidColumnData;
        if (outer_level.validity) |validity| {
            if (!validity[row] and outer_level.offsets[row] != outer_level.offsets[row + 1]) return error.InvalidColumnData;
        }
    }
    const outer_entry_count = outer_level.offsets[row_count];
    if (outer_level.keys.valueCount() != outer_entry_count) return error.InvalidColumnData;
    try validateRequiredColumnDataForSlotCount(outer_key, outer_level.keys, outer_entry_count);

    if (inner_level.keys.physicalType() != inner_key.column_type.physical) return error.InvalidColumnData;
    if (inner_level.keys.validity() != null) return error.InvalidColumnData;
    if (inner_level.offsets.len != outer_entry_count + 1) return error.InvalidColumnData;
    if (inner_level.offsets[0] != 0) return error.InvalidColumnData;
    if (inner_info.map_definition_level > outer_key.max_definition_level) {
        const validity = inner_level.validity orelse return error.InvalidColumnData;
        if (validity.len != outer_entry_count) return error.InvalidColumnData;
    } else if (inner_level.validity != null) {
        return error.InvalidColumnData;
    }
    var outer_entry: usize = 0;
    while (outer_entry < outer_entry_count) : (outer_entry += 1) {
        if (inner_level.offsets[outer_entry] > inner_level.offsets[outer_entry + 1]) return error.InvalidColumnData;
        if (inner_level.validity) |validity| {
            if (!validity[outer_entry] and inner_level.offsets[outer_entry] != inner_level.offsets[outer_entry + 1]) return error.InvalidColumnData;
        }
    }
    const inner_entry_count = inner_level.offsets[outer_entry_count];
    if (inner_level.keys.valueCount() != inner_entry_count) return error.InvalidColumnData;
    try validateRequiredColumnDataForSlotCount(inner_key, inner_level.keys, inner_entry_count);

    if (columns.len == 3) {
        const value_column = columns[2];
        const value_data = data.values orelse return error.InvalidColumnData;
        if (value_data.physicalType() != value_column.column_type.physical) return error.InvalidColumnData;
        const value_optional = value_column.max_definition_level > inner_info.map_definition_level + 1;
        if (value_optional) {
            const validity = value_data.validity() orelse return error.InvalidColumnData;
            if (validity.len != inner_entry_count) return error.InvalidColumnData;
            const valid_count = try countValid(validity);
            if (valid_count != value_data.valueCount()) return error.InvalidColumnData;
        } else {
            if (value_data.validity() != null) return error.InvalidColumnData;
            if (value_data.valueCount() != inner_entry_count) return error.InvalidColumnData;
        }
        var leaf_column = value_column;
        leaf_column.repetition = if (value_optional) .optional else .required;
        leaf_column.max_definition_level = 0;
        leaf_column.max_repetition_level = 0;
        leaf_column.map_info = null;
        leaf_column.nested_logical_info = &.{};
        leaf_column.repeated_level_info = &.{};
        leaf_column.path = &.{};
        try value_data.validate(leaf_column, inner_entry_count);
    }
}

fn validateRequiredColumnDataForSlotCount(column: types.Column, data: types.ColumnData, slot_count: usize) !void {
    var required_column = column;
    required_column.repetition = .required;
    required_column.max_definition_level = 0;
    required_column.max_repetition_level = 0;
    required_column.map_info = null;
    required_column.nested_logical_info = &.{};
    required_column.repeated_level_info = &.{};
    required_column.path = &.{};
    try data.validate(required_column, slot_count);
}

fn buildNestedMapTripletData(allocator: std.mem.Allocator, columns: []const types.Column, row_count: usize, data: types.ColumnNestedMapData) !NestedMapTripletData {
    try validateNestedMapColumnData(columns, row_count, data);
    const outer_key = columns[0];
    const inner_key = columns[1];
    const inner_value_column: ?types.Column = if (columns.len == 3) columns[2] else null;
    const outer_info = outer_key.map_info orelse return error.UnsupportedNestedSchema;
    const inner_info = inner_key.map_info orelse return error.UnsupportedNestedSchema;
    const outer_level = data.levels[0];
    const inner_level = data.levels[1];

    var outer_def: std.ArrayList(u16) = .empty;
    errdefer outer_def.deinit(allocator);
    var outer_rep: std.ArrayList(u16) = .empty;
    errdefer outer_rep.deinit(allocator);
    var inner_def: std.ArrayList(u16) = .empty;
    errdefer inner_def.deinit(allocator);
    var inner_rep: std.ArrayList(u16) = .empty;
    errdefer inner_rep.deinit(allocator);
    var value_def: std.ArrayList(u16) = .empty;
    errdefer value_def.deinit(allocator);
    var value_rep: std.ArrayList(u16) = .empty;
    errdefer value_rep.deinit(allocator);

    const value_optional = if (inner_value_column) |column| column.max_definition_level > inner_info.map_definition_level + 1 else false;
    const value_validity = if (data.values) |values| values.validity() else null;

    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        const outer_start = outer_level.offsets[row];
        const outer_end = outer_level.offsets[row + 1];
        const outer_present = if (outer_level.validity) |validity| validity[row] else true;
        if (!outer_present) {
            try appendNestedMapLevel(allocator, &outer_def, &outer_rep, outer_info.map_definition_level - 1, 0);
            try appendNestedMapLevel(allocator, &inner_def, &inner_rep, outer_info.map_definition_level - 1, 0);
            if (inner_value_column != null) try appendNestedMapLevel(allocator, &value_def, &value_rep, outer_info.map_definition_level - 1, 0);
            continue;
        }
        if (outer_start == outer_end) {
            try appendNestedMapLevel(allocator, &outer_def, &outer_rep, outer_info.map_definition_level, 0);
            try appendNestedMapLevel(allocator, &inner_def, &inner_rep, outer_info.map_definition_level, 0);
            if (inner_value_column != null) try appendNestedMapLevel(allocator, &value_def, &value_rep, outer_info.map_definition_level, 0);
            continue;
        }

        var outer_entry = outer_start;
        while (outer_entry < outer_end) : (outer_entry += 1) {
            const outer_repetition_level: u16 = if (outer_entry == outer_start) 0 else 1;
            try appendNestedMapLevel(allocator, &outer_def, &outer_rep, outer_key.max_definition_level, outer_repetition_level);

            const inner_start = inner_level.offsets[outer_entry];
            const inner_end = inner_level.offsets[outer_entry + 1];
            const inner_present = if (inner_level.validity) |validity| validity[outer_entry] else true;
            if (!inner_present) {
                try appendNestedMapLevel(allocator, &inner_def, &inner_rep, inner_info.map_definition_level - 1, outer_repetition_level);
                if (inner_value_column != null) try appendNestedMapLevel(allocator, &value_def, &value_rep, inner_info.map_definition_level - 1, outer_repetition_level);
                continue;
            }
            if (inner_start == inner_end) {
                try appendNestedMapLevel(allocator, &inner_def, &inner_rep, inner_info.map_definition_level, outer_repetition_level);
                if (inner_value_column != null) try appendNestedMapLevel(allocator, &value_def, &value_rep, inner_info.map_definition_level, outer_repetition_level);
                continue;
            }

            var inner_entry = inner_start;
            while (inner_entry < inner_end) : (inner_entry += 1) {
                const inner_repetition_level: u16 = if (inner_entry == inner_start) outer_repetition_level else 2;
                try appendNestedMapLevel(allocator, &inner_def, &inner_rep, inner_key.max_definition_level, inner_repetition_level);
                if (inner_value_column) |value_column| {
                    const value_present = !value_optional or value_validity.?[inner_entry];
                    try appendNestedMapLevel(allocator, &value_def, &value_rep, if (value_present) value_column.max_definition_level else value_column.max_definition_level - 1, inner_repetition_level);
                }
            }
        }
    }

    const outer_definition_levels = try outer_def.toOwnedSlice(allocator);
    errdefer allocator.free(outer_definition_levels);
    const outer_repetition_levels = try outer_rep.toOwnedSlice(allocator);
    errdefer allocator.free(outer_repetition_levels);
    const inner_definition_levels = try inner_def.toOwnedSlice(allocator);
    errdefer allocator.free(inner_definition_levels);
    const inner_repetition_levels = try inner_rep.toOwnedSlice(allocator);
    errdefer allocator.free(inner_repetition_levels);
    const value_definition_levels = if (inner_value_column != null) try value_def.toOwnedSlice(allocator) else null;
    errdefer if (value_definition_levels) |levels| allocator.free(levels);
    const value_repetition_levels = if (inner_value_column != null) try value_rep.toOwnedSlice(allocator) else null;
    errdefer if (value_repetition_levels) |levels| allocator.free(levels);

    return .{
        .outer_key = .{
            .values = stripColumnValidity(outer_level.keys),
            .definition_levels = outer_definition_levels,
            .repetition_levels = outer_repetition_levels,
        },
        .inner_key = .{
            .values = stripColumnValidity(inner_level.keys),
            .definition_levels = inner_definition_levels,
            .repetition_levels = inner_repetition_levels,
        },
        .inner_value = if (inner_value_column != null) .{
            .values = stripColumnValidity(data.values.?),
            .definition_levels = value_definition_levels.?,
            .repetition_levels = value_repetition_levels.?,
        } else null,
    };
}

fn appendNestedMapLevel(allocator: std.mem.Allocator, definition_levels: *std.ArrayList(u16), repetition_levels: *std.ArrayList(u16), definition_level: u16, repetition_level: u16) !void {
    try definition_levels.append(allocator, definition_level);
    try repetition_levels.append(allocator, repetition_level);
}

fn validateMapColumnData(key_column: types.Column, value_column: ?types.Column, row_count: usize, data: types.ColumnMapData) !void {
    const map_info = key_column.map_info orelse return error.UnsupportedNestedSchema;
    try validateSupportedMapKeyColumnShape(key_column, map_info);
    if (data.keys.physicalType() != key_column.column_type.physical) return error.InvalidColumnData;
    if (data.keys.validity() != null) return error.InvalidColumnData;
    if (data.offsets.len != row_count + 1) return error.InvalidColumnData;
    if (data.offsets[0] != 0) return error.InvalidColumnData;

    const map_optional = map_info.map_definition_level > 0;
    if (map_optional) {
        const validity = data.validity orelse return error.InvalidColumnData;
        if (validity.len != row_count) return error.InvalidColumnData;
    } else if (data.validity != null) {
        return error.InvalidColumnData;
    }

    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        if (data.offsets[row] > data.offsets[row + 1]) return error.InvalidColumnData;
        if (data.validity) |validity| {
            if (!validity[row] and data.offsets[row] != data.offsets[row + 1]) return error.InvalidColumnData;
        }
    }

    const entry_count = data.offsets[row_count];
    if (data.keys.valueCount() != entry_count) return error.InvalidColumnData;
    var key_required_column = key_column;
    key_required_column.repetition = .required;
    key_required_column.max_definition_level = 0;
    key_required_column.max_repetition_level = 0;
    key_required_column.map_info = null;
    key_required_column.path = &.{};
    try data.keys.validate(key_required_column, entry_count);

    if (value_column) |column| {
        const value_data = data.values orelse return error.InvalidColumnData;
        try validateSupportedMapValueColumnShape(column, map_info);
        if (value_data.physicalType() != column.column_type.physical) return error.InvalidColumnData;
        const value_optional = column.max_definition_level > map_info.map_definition_level + 1;
        if (value_optional) {
            const validity = value_data.validity() orelse return error.InvalidColumnData;
            if (validity.len != entry_count) return error.InvalidColumnData;
            const valid_count = try countValid(validity);
            if (valid_count != value_data.valueCount()) return error.InvalidColumnData;
        } else {
            if (value_data.validity() != null) return error.InvalidColumnData;
            if (value_data.valueCount() != entry_count) return error.InvalidColumnData;
        }

        var value_required_column = column;
        value_required_column.repetition = if (value_optional) .optional else .required;
        value_required_column.max_definition_level = 0;
        value_required_column.max_repetition_level = 0;
        value_required_column.map_info = null;
        value_required_column.path = &.{};
        try value_data.validate(value_required_column, entry_count);
    } else if (data.values != null) {
        return error.InvalidColumnData;
    }
}

fn buildMapTripletData(allocator: std.mem.Allocator, key_column: types.Column, value_column: ?types.Column, row_count: usize, data: types.ColumnMapData) !MapTripletLevels {
    try validateMapColumnData(key_column, value_column, row_count, data);
    const map_info = key_column.map_info orelse return error.UnsupportedNestedSchema;
    const entry_count = data.offsets[row_count];
    var placeholder_count: usize = 0;
    var placeholder_row: usize = 0;
    while (placeholder_row < row_count) : (placeholder_row += 1) {
        const map_present = if (data.validity) |validity| validity[placeholder_row] else true;
        if (!map_present or data.offsets[placeholder_row] == data.offsets[placeholder_row + 1]) placeholder_count += 1;
    }
    const level_count = std.math.add(usize, entry_count, placeholder_count) catch return error.RowCountOverflow;
    const key_definition_levels = try allocator.alloc(u16, level_count);
    errdefer allocator.free(key_definition_levels);
    const value_definition_levels = if (value_column != null) try allocator.alloc(u16, level_count) else null;
    errdefer if (value_definition_levels) |levels| allocator.free(levels);
    const repetition_levels = try allocator.alloc(u16, level_count);
    errdefer allocator.free(repetition_levels);

    const value_optional = if (value_column) |column| column.max_definition_level > map_info.map_definition_level + 1 else false;
    const value_validity = if (data.values) |values| values.validity() else null;
    var level_index: usize = 0;
    var row: usize = 0;
    while (row < row_count) : (row += 1) {
        const start = data.offsets[row];
        const end = data.offsets[row + 1];
        const map_present = if (data.validity) |validity| validity[row] else true;
        if (!map_present) {
            key_definition_levels[level_index] = map_info.map_definition_level - 1;
            if (value_definition_levels) |levels| levels[level_index] = map_info.map_definition_level - 1;
            repetition_levels[level_index] = 0;
            level_index += 1;
            continue;
        }
        if (start == end) {
            key_definition_levels[level_index] = map_info.map_definition_level;
            if (value_definition_levels) |levels| levels[level_index] = map_info.map_definition_level;
            repetition_levels[level_index] = 0;
            level_index += 1;
            continue;
        }

        var entry = start;
        while (entry < end) : (entry += 1) {
            key_definition_levels[level_index] = key_column.max_definition_level;
            if (value_column) |column| {
                value_definition_levels.?[level_index] = if (value_optional and !value_validity.?[entry])
                    column.max_definition_level - 1
                else
                    column.max_definition_level;
            }
            repetition_levels[level_index] = if (entry == start) 0 else 1;
            level_index += 1;
        }
    }
    if (level_index != level_count) return error.InvalidColumnData;

    return .{ .key_definition_levels = key_definition_levels, .value_definition_levels = value_definition_levels, .repetition_levels = repetition_levels };
}

fn validateRequiredColumnValues(column: types.Column, data: types.ColumnData) !void {
    var required_column = column;
    required_column.repetition = .required;
    required_column.max_definition_level = 0;
    required_column.max_repetition_level = 0;
    try data.validate(required_column, data.valueCount());
}

fn tripletRowCount(repetition_levels: []const u16, max_repetition_level: u16) !usize {
    if (max_repetition_level == 0) return repetition_levels.len;
    var rows: usize = 0;
    for (repetition_levels, 0..) |level, idx| {
        if (level > max_repetition_level) return error.InvalidColumnData;
        if (idx == 0 and level != 0) return error.InvalidColumnData;
        if (level == 0) rows += 1;
    }
    return rows;
}

fn buildTripletRowOffsets(allocator: std.mem.Allocator, repetition_levels: []const u16, max_repetition_level: u16, row_count: usize) ![]usize {
    const offsets = try allocator.alloc(usize, std.math.add(usize, row_count, 1) catch return error.RowCountOverflow);
    errdefer allocator.free(offsets);
    offsets[0] = 0;

    if (row_count == 0) {
        if (repetition_levels.len != 0) return error.InvalidColumnData;
        return offsets;
    }
    if (repetition_levels.len == 0) return error.InvalidColumnData;

    var row_index: usize = 0;
    var row_started = false;
    for (repetition_levels, 0..) |level, level_index| {
        if (level > max_repetition_level) return error.InvalidColumnData;
        if (level == 0) {
            if (row_started) {
                offsets[row_index + 1] = level_index;
                row_index += 1;
            }
            if (row_index >= row_count) return error.InvalidColumnData;
            row_started = true;
        } else if (!row_started) {
            return error.InvalidColumnData;
        }
    }
    if (!row_started) return error.InvalidColumnData;
    offsets[row_index + 1] = repetition_levels.len;
    row_index += 1;
    if (row_index != row_count) return error.InvalidColumnData;
    return offsets;
}

fn buildTripletValueOffsets(allocator: std.mem.Allocator, definition_levels: []const u16, row_offsets: []const usize, max_definition_level: u16) ![]usize {
    if (row_offsets.len == 0) return error.InvalidColumnData;
    if (row_offsets[row_offsets.len - 1] != definition_levels.len) return error.InvalidColumnData;

    const offsets = try allocator.alloc(usize, row_offsets.len);
    errdefer allocator.free(offsets);
    offsets[0] = 0;

    var value_index: usize = 0;
    var row_index: usize = 0;
    while (row_index + 1 < row_offsets.len) : (row_index += 1) {
        const start = row_offsets[row_index];
        const end = row_offsets[row_index + 1];
        if (start > end or end > definition_levels.len) return error.InvalidColumnData;
        for (definition_levels[start..end]) |level| {
            if (level > max_definition_level) return error.InvalidColumnData;
            if (level == max_definition_level) value_index += 1;
        }
        offsets[row_index + 1] = value_index;
    }

    return offsets;
}

fn writerMaxDefinitionLevel(column: types.Column) u16 {
    if (column.max_definition_level != 0) return column.max_definition_level;
    return switch (column.repetition) {
        .required => 0,
        .optional, .repeated => 1,
    };
}

fn writerMaxRepetitionLevel(column: types.Column) u16 {
    if (column.max_repetition_level != 0) return column.max_repetition_level;
    return switch (column.repetition) {
        .required, .optional => 0,
        .repeated => 1,
    };
}

fn validateSchema(schema: types.Schema) !void {
    if (schema.name.len == 0) return error.InvalidSchema;
    if (schema.columns.len == 0) return error.InvalidSchema;
    for (schema.columns) |column| {
        if (column.name.len == 0) return error.InvalidSchema;
        try validateWriterColumnShape(column);
        if (column.list_info == null and column.map_info == null and column.repetition == .repeated and (writerMaxDefinitionLevel(column) != 1 or writerMaxRepetitionLevel(column) != 1)) return error.UnsupportedNestedSchema;
        switch (column.column_type.physical) {
            .boolean, .int32, .int64, .float, .double, .byte_array => {},
            .int96 => {
                _ = try types.physicalTypeWidth(column.column_type.physical, column.column_type.type_length);
            },
            .fixed_len_byte_array => {
                _ = try types.physicalTypeWidth(column.column_type.physical, column.column_type.type_length);
            },
        }
        switch (column.column_type.logical) {
            .none => {},
            .string => if (column.column_type.physical != .byte_array) return error.InvalidSchema,
            .decimal => try types.validateDecimalType(column.column_type),
            .date => if (column.column_type.physical != .int32) return error.InvalidSchema,
            .timestamp_millis, .timestamp_micros, .timestamp_nanos => if (column.column_type.physical != .int64) return error.InvalidSchema,
        }
    }
    try validateWriterMapColumnGrouping(schema.columns);
}

fn validateWriterMapColumnGrouping(columns: []const types.Column) !void {
    var column_index: usize = 0;
    while (column_index < columns.len) {
        const column = columns[column_index];
        if (listMapColumnSpan(columns, column_index)) |span| {
            try validateSupportedListMapColumnGroup(columns[column_index..][0..span]);
            column_index += span;
            continue;
        }
        if (nestedMapColumnSpan(columns, column_index)) |span| {
            try validateSupportedNestedMapColumnGroup(columns[column_index..][0..span]);
            column_index += span;
            continue;
        }
        if (column.map_info == null) {
            column_index += 1;
            continue;
        }
        if (column.path.len != 3 or !std.mem.eql(u8, column.path[2], "key")) return error.UnsupportedNestedSchema;
        const value_index = mapValueColumnIndex(columns, column_index);
        if (value_index) |idx| {
            const value = columns[idx];
            if (value.map_info.?.map_definition_level != column.map_info.?.map_definition_level) return error.UnsupportedNestedSchema;
        }
        column_index += if (value_index != null) 2 else 1;
    }
}

fn validateWriterColumnShape(column: types.Column) !void {
    if (column.map_info) |map_info| {
        if (column.path.len == 5 and std.mem.eql(u8, column.path[1], "list") and std.mem.eql(u8, column.path[4], "key")) {
            try validateSupportedListMapKeyColumnShape(column, map_info);
        } else if (column.path.len == 5 and std.mem.eql(u8, column.path[1], "list") and std.mem.eql(u8, column.path[4], "value")) {
            try validateSupportedListMapValueColumnShape(column, map_info);
        } else if (column.path.len == 5 and std.mem.eql(u8, column.path[4], "key")) {
            try validateSupportedNestedMapKeyColumnShape(column, map_info);
        } else if (column.path.len == 5 and std.mem.eql(u8, column.path[4], "value")) {
            try validateSupportedNestedMapValueColumnShape(column, map_info);
        } else if (column.path.len == 3 and std.mem.eql(u8, column.path[2], "key")) {
            try validateSupportedMapKeyColumnShape(column, map_info);
        } else {
            try validateSupportedMapValueColumnShape(column, map_info);
        }
        return;
    }
    if (column.list_info) |list_info| {
        try validateSupportedListColumnShape(column, list_info);
        return;
    }
    if (column.path.len > 0) {
        if (column.path.len != 1 or !std.mem.eql(u8, column.path[0], column.name)) return error.UnsupportedNestedSchema;
    }

    const implied_definition_level: u16 = switch (column.repetition) {
        .required => 0,
        .optional, .repeated => 1,
    };
    const implied_repetition_level: u16 = switch (column.repetition) {
        .required, .optional => 0,
        .repeated => 1,
    };
    if (column.max_definition_level != 0 and column.max_definition_level != implied_definition_level) return error.UnsupportedNestedSchema;
    if (column.max_repetition_level != 0 and column.max_repetition_level != implied_repetition_level) return error.UnsupportedNestedSchema;
}

fn validateSupportedListColumnShape(column: types.Column, list_info: types.ListInfo) !void {
    if (column.nested_logical_info.len == 0 or column.path.len == 3) return validateSupportedOneLevelListColumnShape(column, list_info);
    return validateSupportedNestedListColumnShape(column, list_info);
}

fn validateSupportedOneLevelListColumnShape(column: types.Column, list_info: types.ListInfo) !void {
    if (column.path.len != 3) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[1], "list")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[2], column.name)) return error.UnsupportedNestedSchema;
    if (column.repetition != .repeated) return error.UnsupportedNestedSchema;
    if (column.max_repetition_level != 1) return error.UnsupportedNestedSchema;
    if (list_info.list_definition_level > 1) return error.UnsupportedNestedSchema;
    if (column.max_definition_level <= list_info.list_definition_level) return error.UnsupportedNestedSchema;
    if (column.max_definition_level > list_info.list_definition_level + 2) return error.UnsupportedNestedSchema;
}

fn validateSupportedNestedListColumnShape(column: types.Column, list_info: types.ListInfo) !void {
    if (column.repetition != .repeated) return error.UnsupportedNestedSchema;
    if (column.nested_logical_info.len < 2) return error.UnsupportedNestedSchema;
    if (column.path.len != column.nested_logical_info.len * 2 + 1) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[column.path.len - 1], column.name)) return error.UnsupportedNestedSchema;

    var expected_definition_level: u16 = 0;
    var expected_repetition_level: u16 = 0;
    for (column.nested_logical_info, 0..) |logical_info, idx| {
        if (logical_info.kind != .list) return error.UnsupportedNestedSchema;
        if (column.path[idx * 2 + 1].len == 0 or !std.mem.eql(u8, column.path[idx * 2 + 1], "list")) return error.UnsupportedNestedSchema;
        if (logical_info.path.len != idx * 2 + 1) return error.UnsupportedNestedSchema;
        for (logical_info.path, 0..) |part, part_index| {
            if (!std.mem.eql(u8, part, column.path[part_index])) return error.UnsupportedNestedSchema;
        }
        if (logical_info.optional) expected_definition_level = std.math.add(u16, expected_definition_level, 1) catch return error.UnsupportedNestedSchema;
        if (logical_info.definition_level != expected_definition_level) return error.UnsupportedNestedSchema;
        if (logical_info.repetition_level != expected_repetition_level) return error.UnsupportedNestedSchema;
        expected_definition_level = std.math.add(u16, expected_definition_level, 1) catch return error.UnsupportedNestedSchema;
        expected_repetition_level = std.math.add(u16, expected_repetition_level, 1) catch return error.UnsupportedNestedSchema;
    }
    if (column.max_repetition_level != expected_repetition_level) return error.UnsupportedNestedSchema;
    if (column.max_definition_level < expected_definition_level) return error.UnsupportedNestedSchema;
    if (column.max_definition_level > expected_definition_level + 1) return error.UnsupportedNestedSchema;
    if (list_info.list_definition_level != column.nested_logical_info[column.nested_logical_info.len - 1].definition_level) return error.UnsupportedNestedSchema;
}

fn writerListLevelCount(column: types.Column) usize {
    return if (column.nested_logical_info.len == 0) 1 else column.nested_logical_info.len;
}

fn writerListLevelDefinition(column: types.Column, level_index: usize) u16 {
    if (column.nested_logical_info.len > 0) return column.nested_logical_info[level_index].definition_level;
    const list_info = column.list_info orelse unreachable;
    return list_info.list_definition_level;
}

fn writerListLevelOptional(column: types.Column, level_index: usize) bool {
    if (column.nested_logical_info.len > 0) return column.nested_logical_info[level_index].optional;
    return writerListLevelDefinition(column, level_index) > 0;
}

fn writerNestedListRequiredLeafDefinitionLevel(column: types.Column) u16 {
    if (column.nested_logical_info.len == 0) return writerListLevelDefinition(column, 0) + 1;
    var definition_level: u16 = 0;
    for (column.nested_logical_info) |logical_info| {
        if (logical_info.optional) definition_level += 1;
        definition_level += 1;
    }
    return definition_level;
}

fn writerNestedListLeafOptional(column: types.Column) bool {
    return column.max_definition_level > writerNestedListRequiredLeafDefinitionLevel(column);
}

fn validateSupportedMapKeyColumnShape(column: types.Column, map_info: types.MapInfo) !void {
    if (column.path.len != 3) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[1], "key_value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[2], "key")) return error.UnsupportedNestedSchema;
    if (column.repetition != .repeated) return error.UnsupportedNestedSchema;
    if (column.max_repetition_level != 1) return error.UnsupportedNestedSchema;
    if (map_info.map_definition_level > 1) return error.UnsupportedNestedSchema;
    if (column.max_definition_level != map_info.map_definition_level + 1) return error.UnsupportedNestedSchema;
}

fn validateSupportedMapValueColumnShape(column: types.Column, map_info: types.MapInfo) !void {
    if (column.path.len != 3) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[1], "key_value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[2], "value")) return error.UnsupportedNestedSchema;
    if (column.repetition != .repeated) return error.UnsupportedNestedSchema;
    if (column.max_repetition_level != 1) return error.UnsupportedNestedSchema;
    if (map_info.map_definition_level > 1) return error.UnsupportedNestedSchema;
    if (column.max_definition_level < map_info.map_definition_level + 1) return error.UnsupportedNestedSchema;
    if (column.max_definition_level > map_info.map_definition_level + 2) return error.UnsupportedNestedSchema;
}

fn validateSupportedNestedMapColumnGroup(columns: []const types.Column) !void {
    if (columns.len != 2 and columns.len != 3) return error.UnsupportedNestedSchema;
    try validateSupportedMapKeyColumnShape(columns[0], columns[0].map_info orelse return error.UnsupportedNestedSchema);
    try validateSupportedNestedMapKeyColumnShape(columns[1], columns[1].map_info orelse return error.UnsupportedNestedSchema);
    if (!std.mem.eql(u8, columns[1].path[0], columns[0].path[0]) or
        !std.mem.eql(u8, columns[1].path[1], columns[0].path[1]) or
        !std.mem.eql(u8, columns[1].path[2], "value"))
        return error.UnsupportedNestedSchema;
    if (columns.len == 3) {
        try validateSupportedNestedMapValueColumnShape(columns[2], columns[2].map_info orelse return error.UnsupportedNestedSchema);
        if (!nestedMapLeafPathsShareMap(columns[1], columns[2])) return error.UnsupportedNestedSchema;
    }
}

fn validateSupportedListMapColumnGroup(columns: []const types.Column) !void {
    if (columns.len != 1 and columns.len != 2) return error.UnsupportedNestedSchema;
    try validateSupportedListMapKeyColumnShape(columns[0], columns[0].map_info orelse return error.UnsupportedNestedSchema);
    if (columns.len == 2) {
        try validateSupportedListMapValueColumnShape(columns[1], columns[1].map_info orelse return error.UnsupportedNestedSchema);
        if (!listMapLeafPathsShareMap(columns[0], columns[1])) return error.UnsupportedNestedSchema;
        if (columns[1].map_info.?.map_definition_level != columns[0].map_info.?.map_definition_level) return error.UnsupportedNestedSchema;
    }
}

fn validateSupportedListMapKeyColumnShape(column: types.Column, map_info: types.MapInfo) !void {
    if (column.path.len != 5) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[1], "list")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[2], "element")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[3], "key_value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[4], "key")) return error.UnsupportedNestedSchema;
    if (column.repetition != .repeated) return error.UnsupportedNestedSchema;
    if (column.max_repetition_level != 2) return error.UnsupportedNestedSchema;
    if (column.max_definition_level != map_info.map_definition_level + 1) return error.UnsupportedNestedSchema;
    try validateListMapLogicalInfo(column, map_info);
}

fn validateSupportedListMapValueColumnShape(column: types.Column, map_info: types.MapInfo) !void {
    if (column.path.len != 5) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[1], "list")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[2], "element")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[3], "key_value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[4], "value")) return error.UnsupportedNestedSchema;
    if (column.repetition != .repeated) return error.UnsupportedNestedSchema;
    if (column.max_repetition_level != 2) return error.UnsupportedNestedSchema;
    if (column.max_definition_level < map_info.map_definition_level + 1) return error.UnsupportedNestedSchema;
    if (column.max_definition_level > map_info.map_definition_level + 2) return error.UnsupportedNestedSchema;
    try validateListMapLogicalInfo(column, map_info);
}

fn validateListMapLogicalInfo(column: types.Column, map_info: types.MapInfo) !void {
    if (column.nested_logical_info.len != 2) return error.UnsupportedNestedSchema;
    const list = column.nested_logical_info[0];
    const map = column.nested_logical_info[1];
    if (list.kind != .list or map.kind != .map) return error.UnsupportedNestedSchema;
    if (list.path.len != 1 or !std.mem.eql(u8, list.path[0], column.path[0])) return error.UnsupportedNestedSchema;
    if (map.path.len != 3 or
        !std.mem.eql(u8, map.path[0], column.path[0]) or
        !std.mem.eql(u8, map.path[1], column.path[1]) or
        !std.mem.eql(u8, map.path[2], column.path[2]))
        return error.UnsupportedNestedSchema;
    if (list.repetition_level != 0 or map.repetition_level != 1) return error.UnsupportedNestedSchema;

    var expected_map_definition_level: u16 = if (list.optional) 1 else 0;
    if (list.definition_level != expected_map_definition_level) return error.UnsupportedNestedSchema;
    expected_map_definition_level += 1;
    if (map.optional) expected_map_definition_level += 1;
    if (map.definition_level != expected_map_definition_level) return error.UnsupportedNestedSchema;
    if (map.definition_level != map_info.map_definition_level) return error.UnsupportedNestedSchema;
}

fn validateSupportedMapTripletColumnShape(column: types.Column, map_info: types.MapInfo) !void {
    if (column.path.len == 5 and std.mem.eql(u8, column.path[1], "list") and std.mem.eql(u8, column.path[4], "key")) {
        return validateSupportedListMapKeyColumnShape(column, map_info);
    }
    if (column.path.len == 5 and std.mem.eql(u8, column.path[1], "list") and std.mem.eql(u8, column.path[4], "value")) {
        return validateSupportedListMapValueColumnShape(column, map_info);
    }
    if (column.path.len == 5 and std.mem.eql(u8, column.path[4], "key")) {
        return validateSupportedNestedMapKeyColumnShape(column, map_info);
    }
    if (column.path.len == 5 and std.mem.eql(u8, column.path[4], "value")) {
        return validateSupportedNestedMapValueColumnShape(column, map_info);
    }
    if (column.path.len == 3 and std.mem.eql(u8, column.path[2], "key")) {
        return validateSupportedMapKeyColumnShape(column, map_info);
    }
    return validateSupportedMapValueColumnShape(column, map_info);
}

fn validateSupportedNestedMapKeyColumnShape(column: types.Column, map_info: types.MapInfo) !void {
    if (column.path.len != 5) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[1], "key_value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[2], "value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[3], "key_value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[4], "key")) return error.UnsupportedNestedSchema;
    if (column.repetition != .repeated) return error.UnsupportedNestedSchema;
    if (column.max_repetition_level != 2) return error.UnsupportedNestedSchema;
    if (map_info.map_definition_level < 2) return error.UnsupportedNestedSchema;
    if (column.max_definition_level != map_info.map_definition_level + 1) return error.UnsupportedNestedSchema;
    try validateNestedMapLogicalInfo(column, map_info);
}

fn validateSupportedNestedMapValueColumnShape(column: types.Column, map_info: types.MapInfo) !void {
    if (column.path.len != 5) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[1], "key_value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[2], "value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[3], "key_value")) return error.UnsupportedNestedSchema;
    if (!std.mem.eql(u8, column.path[4], "value")) return error.UnsupportedNestedSchema;
    if (column.repetition != .repeated) return error.UnsupportedNestedSchema;
    if (column.max_repetition_level != 2) return error.UnsupportedNestedSchema;
    if (map_info.map_definition_level < 2) return error.UnsupportedNestedSchema;
    if (column.max_definition_level < map_info.map_definition_level + 1) return error.UnsupportedNestedSchema;
    if (column.max_definition_level > map_info.map_definition_level + 2) return error.UnsupportedNestedSchema;
    try validateNestedMapLogicalInfo(column, map_info);
}

fn validateNestedMapLogicalInfo(column: types.Column, map_info: types.MapInfo) !void {
    if (column.nested_logical_info.len != 2) return error.UnsupportedNestedSchema;
    const outer = column.nested_logical_info[0];
    const inner = column.nested_logical_info[1];
    if (outer.kind != .map or inner.kind != .map) return error.UnsupportedNestedSchema;
    if (outer.path.len != 1 or !std.mem.eql(u8, outer.path[0], column.path[0])) return error.UnsupportedNestedSchema;
    if (inner.path.len != 3 or !std.mem.eql(u8, inner.path[0], column.path[0]) or !std.mem.eql(u8, inner.path[1], column.path[1]) or !std.mem.eql(u8, inner.path[2], column.path[2])) return error.UnsupportedNestedSchema;
    if (outer.repetition_level != 0 or inner.repetition_level != 1) return error.UnsupportedNestedSchema;
    if (inner.definition_level != map_info.map_definition_level) return error.UnsupportedNestedSchema;
}

fn mapValueColumnIndex(columns: []const types.Column, key_index: usize) ?usize {
    if (key_index + 1 >= columns.len) return null;
    const key = columns[key_index];
    const value = columns[key_index + 1];
    if (key.map_info == null or value.map_info == null) return null;
    if (key.path.len != 3 or value.path.len != 3) return null;
    if (!std.mem.eql(u8, key.path[0], value.path[0])) return null;
    if (!std.mem.eql(u8, key.path[1], value.path[1])) return null;
    if (!std.mem.eql(u8, key.path[2], "key") or !std.mem.eql(u8, value.path[2], "value")) return null;
    return key_index + 1;
}

fn listMapColumnSpan(columns: []const types.Column, key_index: usize) ?usize {
    const key = columns[key_index];
    if (key.map_info == null or key.path.len != 5) return null;
    if (!std.mem.eql(u8, key.path[1], "list") or
        !std.mem.eql(u8, key.path[2], "element") or
        !std.mem.eql(u8, key.path[3], "key_value") or
        !std.mem.eql(u8, key.path[4], "key"))
        return null;
    if (key_index + 1 < columns.len and columns[key_index + 1].map_info != null and listMapLeafPathsShareMap(key, columns[key_index + 1])) return 2;
    return 1;
}

fn nestedMapColumnSpan(columns: []const types.Column, key_index: usize) ?usize {
    if (key_index + 1 >= columns.len) return null;
    const outer_key = columns[key_index];
    const inner_key = columns[key_index + 1];
    if (outer_key.map_info == null or inner_key.map_info == null) return null;
    if (outer_key.path.len != 3 or inner_key.path.len != 5) return null;
    if (!std.mem.eql(u8, outer_key.path[1], "key_value") or !std.mem.eql(u8, outer_key.path[2], "key")) return null;
    if (!std.mem.eql(u8, inner_key.path[0], outer_key.path[0]) or
        !std.mem.eql(u8, inner_key.path[1], outer_key.path[1]) or
        !std.mem.eql(u8, inner_key.path[2], "value") or
        !std.mem.eql(u8, inner_key.path[3], "key_value") or
        !std.mem.eql(u8, inner_key.path[4], "key"))
        return null;
    if (key_index + 2 < columns.len and columns[key_index + 2].map_info != null and nestedMapLeafPathsShareMap(inner_key, columns[key_index + 2])) return 3;
    return 2;
}

fn listMapLeafPathsShareMap(key: types.Column, value: types.Column) bool {
    return value.path.len == 5 and
        std.mem.eql(u8, value.path[0], key.path[0]) and
        std.mem.eql(u8, value.path[1], key.path[1]) and
        std.mem.eql(u8, value.path[2], key.path[2]) and
        std.mem.eql(u8, value.path[3], key.path[3]) and
        std.mem.eql(u8, value.path[4], "value");
}

fn nestedMapLeafPathsShareMap(key: types.Column, value: types.Column) bool {
    return value.path.len == 5 and
        std.mem.eql(u8, value.path[0], key.path[0]) and
        std.mem.eql(u8, value.path[1], key.path[1]) and
        std.mem.eql(u8, value.path[2], key.path[2]) and
        std.mem.eql(u8, value.path[3], key.path[3]) and
        std.mem.eql(u8, value.path[4], "value");
}

fn validateOptions(options: Options) !void {
    if (options.max_page_rows == 0) return error.InvalidColumnData;
    switch (options.compression) {
        .uncompressed, .snappy, .gzip, .lz4_raw, .zstd => {},
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
        const value_count = if (entry.value_count > 0) entry.value_count else entry.row_count;
        if (null_count < 0 or null_count > value_count) return false;
        const null_page = null_count == value_count;
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
        .int96 => {},
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

test "writer creates legacy repeated primitive triplet pages readable by reader" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "items", .column_type = .{ .physical = .int32 }, .repetition = .repeated },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    inline for ([_]DataPageVersion{ .v1, .v2 }) |page_version| {
        var out: std.Io.Writer.Allocating = .init(testing.allocator);
        defer out.deinit();
        var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{
            .max_page_rows = 2,
            .data_page_version = page_version,
            .compression = if (page_version == .v2) .zstd else .uncompressed,
        });
        defer w.deinit();
        try w.start();

        const values = [_]i32{ 10, 11, 20 };
        const definition_levels = [_]u16{ 1, 1, 0, 1, 0 };
        const repetition_levels = [_]u16{ 0, 1, 0, 0, 0 };
        const batch = [_]types.ColumnTripletData{
            .{
                .values = .{ .int32 = .{ .values = values[0..] } },
                .definition_levels = definition_levels[0..],
                .repetition_levels = repetition_levels[0..],
            },
        };
        try w.writeRowGroupTriplets(4, batch[0..]);
        try w.finish();

        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();
        {
            var file = try tmp.dir.createFile(testing.io, "repeated.parquet", .{ .truncate = true });
            defer file.close(testing.io);
            var writer_buffer: [256]u8 = undefined;
            var file_writer = file.writer(testing.io, &writer_buffer);
            try file_writer.interface.writeAll(out.written());
            try file_writer.interface.flush();
        }

        var file = try tmp.dir.openFile(testing.io, "repeated.parquet", .{});
        defer file.close(testing.io);
        var reader_buffer: [1024]u8 = undefined;
        var file_reader = file.reader(testing.io, &reader_buffer);
        var parsed = try reader_mod.StreamFileReader.init(testing.allocator, &file_reader);
        defer parsed.deinit();
        try testing.expectEqual(types.Repetition.repeated, parsed.metadata.schema.columns[0].repetition);
        try testing.expectEqual(@as(u16, 1), parsed.metadata.schema.columns[0].max_definition_level);
        try testing.expectEqual(@as(u16, 1), parsed.metadata.schema.columns[0].max_repetition_level);
        try testing.expectEqual(@as(i64, definition_levels.len), parsed.metadata.row_groups[0].columns[0].num_values);

        var triplets = try parsed.readColumnTriplets(testing.allocator, 0, 0);
        defer triplets.deinit(testing.allocator);
        try testing.expectEqualSlices(u16, definition_levels[0..], triplets.definition_levels);
        try testing.expectEqualSlices(u16, repetition_levels[0..], triplets.repetition_levels);
        try testing.expectEqualSlices(i32, values[0..], triplets.values.int32.values);
        try testing.expectEqualSlices(usize, &[_]usize{ 0, 2, 3, 4, 5 }, triplets.row_offsets);
        try testing.expectEqualSlices(usize, &[_]usize{ 0, 2, 2, 3, 3 }, triplets.value_offsets);

        var nested_triplets = try parsed.readColumnNestedTriplets(testing.allocator, 0, 0);
        defer nested_triplets.deinit(testing.allocator);
        try testing.expectEqualStrings("items", nested_triplets.column_path[0]);
        try testing.expectEqual(@as(usize, 1), nested_triplets.repeated_levels.len);
        try testing.expectEqual(@as(u16, 1), nested_triplets.repeated_levels[0].repetition_level);
        try testing.expectEqualStrings("items", nested_triplets.repeated_levels[0].path[0]);
        try testing.expectEqualSlices(usize, &[_]usize{ 0, 1, 2, 3, 4, 5 }, nested_triplets.repeated_levels[0].offsets);

        var list = try parsed.readColumnList(testing.allocator, 0, 0);
        defer list.deinit(testing.allocator);
        try testing.expectEqualSlices(usize, &[_]usize{ 0, 2, 2, 3, 3 }, list.offsets);
        try testing.expectEqualSlices(i32, values[0..], list.values.int32.values);
        try testing.expect(list.validity == null);
    }
}

test "writer creates standard one-level list pages readable by reader" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const list_path = [_][]const u8{ "items", "list", "element" };
    const cols = [_]types.Column{
        .{
            .name = "element",
            .path = list_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 3,
            .max_repetition_level = 1,
            .list_info = .{ .list_definition_level = 1 },
        },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    inline for ([_]DataPageVersion{ .v1, .v2 }) |page_version| {
        var out: std.Io.Writer.Allocating = .init(testing.allocator);
        defer out.deinit();
        var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{
            .max_page_rows = 2,
            .data_page_version = page_version,
            .compression = if (page_version == .v2) .zstd else .uncompressed,
        });
        defer w.deinit();
        try w.start();

        const values = [_]i32{ 10, 11, 20 };
        const offsets = [_]usize{ 0, 0, 0, 3, 4 };
        const list_validity = [_]bool{ false, true, true, true };
        const element_validity = [_]bool{ true, false, true, true };
        const batch = [_]types.ColumnListData{
            .{
                .values = .{ .int32 = .{ .values = values[0..], .validity = element_validity[0..] } },
                .offsets = offsets[0..],
                .validity = list_validity[0..],
            },
        };
        try w.writeRowGroupLists(4, batch[0..]);
        try w.finish();

        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();
        {
            var file = try tmp.dir.createFile(testing.io, "list.parquet", .{ .truncate = true });
            defer file.close(testing.io);
            var writer_buffer: [256]u8 = undefined;
            var file_writer = file.writer(testing.io, &writer_buffer);
            try file_writer.interface.writeAll(out.written());
            try file_writer.interface.flush();
        }

        var file = try tmp.dir.openFile(testing.io, "list.parquet", .{});
        defer file.close(testing.io);
        var reader_buffer: [1024]u8 = undefined;
        var file_reader = file.reader(testing.io, &reader_buffer);
        var parsed = try reader_mod.StreamFileReader.init(testing.allocator, &file_reader);
        defer parsed.deinit();
        try testing.expectEqualStrings("items.list.element", parsed.metadata.row_groups[0].columns[0].path);
        try testing.expectEqual(@as(u16, 3), parsed.metadata.schema.columns[0].max_definition_level);
        try testing.expectEqual(@as(u16, 1), parsed.metadata.schema.columns[0].max_repetition_level);
        try testing.expect(parsed.metadata.schema.columns[0].list_info != null);

        var nested_triplets = try parsed.readColumnNestedTripletsByPath(testing.allocator, 0, "items.list.element");
        defer nested_triplets.deinit(testing.allocator);
        try testing.expectEqual(@as(usize, 1), nested_triplets.logical_levels.len);
        try testing.expectEqual(types.NestedLogicalKind.list, nested_triplets.logical_levels[0].kind);
        try testing.expectEqual(@as(u16, 1), nested_triplets.logical_levels[0].definition_level);
        try testing.expectEqual(@as(u16, 0), nested_triplets.logical_levels[0].repetition_level);
        try testing.expectEqualStrings("items", nested_triplets.logical_levels[0].path[0]);

        var list = try parsed.readColumnListByPath(testing.allocator, 0, "items.list.element");
        defer list.deinit(testing.allocator);
        try testing.expectEqualSlices(usize, offsets[0..], list.offsets);
        try testing.expectEqualSlices(bool, list_validity[0..], list.validity.?);
        try testing.expectEqualSlices(bool, element_validity[0..], list.values.int32.validity.?);
        try testing.expectEqualSlices(i32, values[0..], list.values.int32.values);
    }
}

test "writer creates standard one-level map pages readable by reader" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const key_path = [_][]const u8{ "attrs", "key_value", "key" };
    const value_path = [_][]const u8{ "attrs", "key_value", "value" };
    const cols = [_]types.Column{
        .{
            .name = "key",
            .path = key_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 2,
            .max_repetition_level = 1,
            .map_info = .{ .map_definition_level = 1 },
        },
        .{
            .name = "value",
            .path = value_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 3,
            .max_repetition_level = 1,
            .map_info = .{ .map_definition_level = 1 },
        },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    inline for ([_]DataPageVersion{ .v1, .v2 }) |page_version| {
        var out: std.Io.Writer.Allocating = .init(testing.allocator);
        defer out.deinit();
        var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{
            .max_page_rows = 2,
            .data_page_version = page_version,
            .compression = if (page_version == .v2) .zstd else .uncompressed,
        });
        defer w.deinit();
        try w.start();

        const keys = [_]i32{ 1, 2, 3 };
        const values = [_]i32{ 10, 20 };
        const offsets = [_]usize{ 0, 0, 0, 2, 3 };
        const map_validity = [_]bool{ false, true, true, true };
        const value_validity = [_]bool{ true, false, true };
        const batch = [_]types.ColumnMapData{
            .{
                .keys = .{ .int32 = .{ .values = keys[0..] } },
                .values = .{ .int32 = .{ .values = values[0..], .validity = value_validity[0..] } },
                .offsets = offsets[0..],
                .validity = map_validity[0..],
            },
        };
        try w.writeRowGroupMaps(4, batch[0..]);
        try w.finish();

        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();
        {
            var file = try tmp.dir.createFile(testing.io, "map.parquet", .{ .truncate = true });
            defer file.close(testing.io);
            var writer_buffer: [256]u8 = undefined;
            var file_writer = file.writer(testing.io, &writer_buffer);
            try file_writer.interface.writeAll(out.written());
            try file_writer.interface.flush();
        }

        var file = try tmp.dir.openFile(testing.io, "map.parquet", .{});
        defer file.close(testing.io);
        var reader_buffer: [1024]u8 = undefined;
        var file_reader = file.reader(testing.io, &reader_buffer);
        var parsed = try reader_mod.StreamFileReader.init(testing.allocator, &file_reader);
        defer parsed.deinit();
        try testing.expectEqualStrings("attrs.key_value.key", parsed.metadata.row_groups[0].columns[0].path);
        try testing.expectEqualStrings("attrs.key_value.value", parsed.metadata.row_groups[0].columns[1].path);

        var map = try parsed.readColumnMapByPath(testing.allocator, 0, "attrs.key_value.key", "attrs.key_value.value");
        defer map.deinit(testing.allocator);
        try testing.expectEqualSlices(usize, offsets[0..], map.offsets);
        try testing.expectEqualSlices(bool, map_validity[0..], map.validity.?);
        try testing.expectEqualSlices(i32, keys[0..], map.keys.int32.values);
        try testing.expectEqualSlices(bool, value_validity[0..], map.values.?.int32.validity.?);
        try testing.expectEqualSlices(i32, values[0..], map.values.?.int32.values);
    }
}

test "writer creates mixed flat list and map row groups readable by reader" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const list_path = [_][]const u8{ "items", "list", "element" };
    const key_path = [_][]const u8{ "attrs", "key_value", "key" };
    const value_path = [_][]const u8{ "attrs", "key_value", "value" };
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{
            .name = "element",
            .path = list_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 3,
            .max_repetition_level = 1,
            .list_info = .{ .list_definition_level = 1 },
        },
        .{
            .name = "key",
            .path = key_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 2,
            .max_repetition_level = 1,
            .map_info = .{ .map_definition_level = 1 },
        },
        .{
            .name = "value",
            .path = value_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 3,
            .max_repetition_level = 1,
            .map_info = .{ .map_definition_level = 1 },
        },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    inline for ([_]DataPageVersion{ .v1, .v2 }) |page_version| {
        var out: std.Io.Writer.Allocating = .init(testing.allocator);
        defer out.deinit();
        var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{
            .max_page_rows = 2,
            .data_page_version = page_version,
            .compression = if (page_version == .v2) .zstd else .uncompressed,
        });
        defer w.deinit();
        try w.start();

        const ids = [_]i64{ 100, 101, 102, 103 };
        const list_values = [_]i32{ 10, 11, 20 };
        const list_offsets = [_]usize{ 0, 0, 0, 3, 4 };
        const list_validity = [_]bool{ false, true, true, true };
        const element_validity = [_]bool{ true, false, true, true };
        const map_keys = [_]i32{ 1, 2, 3 };
        const map_values = [_]i32{ 10, 20 };
        const map_offsets = [_]usize{ 0, 0, 0, 2, 3 };
        const map_validity = [_]bool{ false, true, true, true };
        const value_validity = [_]bool{ true, false, true };
        const batch = [_]types.ColumnWriteData{
            .{ .flat = .{ .int64 = .{ .values = ids[0..] } } },
            .{ .list = .{
                .values = .{ .int32 = .{ .values = list_values[0..], .validity = element_validity[0..] } },
                .offsets = list_offsets[0..],
                .validity = list_validity[0..],
            } },
            .{ .map = .{
                .keys = .{ .int32 = .{ .values = map_keys[0..] } },
                .values = .{ .int32 = .{ .values = map_values[0..], .validity = value_validity[0..] } },
                .offsets = map_offsets[0..],
                .validity = map_validity[0..],
            } },
        };
        try w.writeRowGroupMixed(4, batch[0..]);
        try w.finish();

        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();
        {
            var file = try tmp.dir.createFile(testing.io, "mixed.parquet", .{ .truncate = true });
            defer file.close(testing.io);
            var writer_buffer: [256]u8 = undefined;
            var file_writer = file.writer(testing.io, &writer_buffer);
            try file_writer.interface.writeAll(out.written());
            try file_writer.interface.flush();
        }

        var file = try tmp.dir.openFile(testing.io, "mixed.parquet", .{});
        defer file.close(testing.io);
        var reader_buffer: [1024]u8 = undefined;
        var file_reader = file.reader(testing.io, &reader_buffer);
        var parsed = try reader_mod.StreamFileReader.init(testing.allocator, &file_reader);
        defer parsed.deinit();
        try testing.expectEqualStrings("id", parsed.metadata.row_groups[0].columns[0].path);
        try testing.expectEqualStrings("items.list.element", parsed.metadata.row_groups[0].columns[1].path);
        try testing.expectEqualStrings("attrs.key_value.key", parsed.metadata.row_groups[0].columns[2].path);
        try testing.expectEqualStrings("attrs.key_value.value", parsed.metadata.row_groups[0].columns[3].path);

        var read_ids = try parsed.readColumn(testing.allocator, 0, 0);
        defer read_ids.deinit(testing.allocator);
        try testing.expectEqualSlices(i64, ids[0..], read_ids.int64.values);

        var list = try parsed.readColumnListByPath(testing.allocator, 0, "items.list.element");
        defer list.deinit(testing.allocator);
        try testing.expectEqualSlices(usize, list_offsets[0..], list.offsets);
        try testing.expectEqualSlices(bool, list_validity[0..], list.validity.?);
        try testing.expectEqualSlices(bool, element_validity[0..], list.values.int32.validity.?);
        try testing.expectEqualSlices(i32, list_values[0..], list.values.int32.values);

        var map = try parsed.readColumnMapByPath(testing.allocator, 0, "attrs.key_value.key", "attrs.key_value.value");
        defer map.deinit(testing.allocator);
        try testing.expectEqualSlices(usize, map_offsets[0..], map.offsets);
        try testing.expectEqualSlices(bool, map_validity[0..], map.validity.?);
        try testing.expectEqualSlices(i32, map_keys[0..], map.keys.int32.values);
        try testing.expectEqualSlices(bool, value_validity[0..], map.values.?.int32.validity.?);
        try testing.expectEqualSlices(i32, map_values[0..], map.values.?.int32.values);
    }
}

test "writer rejects unsupported nested schema metadata before writing" {
    const testing = std.testing;
    const nested_path = [_][]const u8{ "items", "bag", "element" };
    const schemas = [_]types.Schema{
        types.Schema.init("schema", &[_]types.Column{
            .{
                .name = "element",
                .path = nested_path[0..],
                .column_type = .{ .physical = .int32 },
                .repetition = .repeated,
                .max_definition_level = 3,
                .max_repetition_level = 1,
                .list_info = .{ .list_definition_level = 1 },
            },
        }),
        types.Schema.init("schema", &[_]types.Column{
            .{
                .name = "value",
                .column_type = .{ .physical = .int64 },
                .repetition = .optional,
                .max_definition_level = 2,
            },
        }),
        types.Schema.init("schema", &[_]types.Column{
            .{
                .name = "key",
                .column_type = .{ .physical = .int32 },
                .repetition = .repeated,
                .map_info = .{ .map_definition_level = 1 },
            },
        }),
    };

    inline for (schemas) |schema| {
        var out: std.Io.Writer.Allocating = .init(testing.allocator);
        defer out.deinit();
        var w = StreamWriter.init(testing.allocator, &out.writer, schema);
        defer w.deinit();
        try testing.expectError(error.UnsupportedNestedSchema, w.start());
    }
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

test "writer creates lz4-raw-compressed pages readable by reader" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{ .compression = .lz4_raw });
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
    try testing.expectEqual(types.CompressionCodec.lz4_raw, parsed.metadata.row_groups[0].columns[0].codec);

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

test "writer round-trips raw int96 values" {
    const testing = std.testing;
    const reader_mod = @import("reader.zig");
    const cols = [_]types.Column{
        .{ .name = "ts_raw", .column_type = .{ .physical = .int96 }, .repetition = .optional },
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

    const values = [_][]const u8{
        "abcdefghijkl",
        "mnopqrstuvwx",
        "yz0123456789",
    };
    const validity = [_]bool{ true, false, true, true };
    const batch = [_]types.ColumnData{
        .{ .int96 = .{ .values = values[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(validity.len, batch[0..]);
    try w.finish();

    var parsed = try reader_mod.readFileFromMemory(testing.allocator, out.written());
    defer parsed.deinit();
    try testing.expectEqual(types.Type.int96, parsed.metadata.schema.columns[0].column_type.physical);
    try testing.expectEqual(@as(?i64, 1), parsed.metadata.row_groups[0].columns[0].statistics.null_count);
    try testing.expect(!parsed.metadata.row_groups[0].columns[0].statistics.hasMinMax());

    var raw_col = try parsed.readColumn(testing.allocator, 0, 0);
    defer raw_col.deinit(testing.allocator);
    try testing.expectEqualSlices(bool, validity[0..], raw_col.int96.validity.?);
    for (values, raw_col.int96.values) |expected, actual| {
        try testing.expectEqualStrings(expected, actual);
    }
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
