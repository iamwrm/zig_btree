const std = @import("std");
const types = @import("types.zig");
const thrift = @import("thrift.zig");
const plain = @import("plain.zig");
const snappy = @import("snappy.zig");
const gzip = @import("gzip.zig");
const lz4 = @import("lz4.zig");
const zstd = @import("zstd.zig");
const native_endian = @import("builtin").target.cpu.arch.endian();

const max_footer_size = 64 * 1024 * 1024;
const max_page_size = 256 * 1024 * 1024;
const max_page_index_size = 64 * 1024 * 1024;
const max_identity_page_cache_entries = 16;
const max_identity_page_cache_bytes = 1024 * 1024;
const max_identity_page_cache_entry_size = 256 * 1024;
const max_identity_dictionary_cache_entries = 16;
const max_identity_dictionary_cache_bytes = 8 * 1024 * 1024;
const max_identity_dictionary_cache_entry_size = 1024 * 1024;

pub const ParsedFile = struct {
    arena: std.heap.ArenaAllocator,
    metadata: types.FileMetaData,
    bytes: []const u8,

    pub fn deinit(self: *ParsedFile) void {
        self.arena.deinit();
    }

    pub fn columnIndexByPath(self: *const ParsedFile, dotted_path: []const u8) ?usize {
        return schemaColumnIndexByPath(self.metadata.schema, dotted_path);
    }

    pub fn readColumnByPath(self: *ParsedFile, allocator: std.mem.Allocator, row_group_index: usize, dotted_path: []const u8) !types.OwnedColumn {
        const column_index = self.columnIndexByPath(dotted_path) orelse return error.InvalidColumnData;
        return self.readColumn(allocator, row_group_index, column_index);
    }

    pub fn readColumn(self: *ParsedFile, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !types.OwnedColumn {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        const column = row_group.columns[column_index];
        const schema_col = self.metadata.schema.columns[column_index];
        try validateReadableColumnSchema(schema_col);
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
            const page_size = try checkedPageSize(page_header.compressed_page_size);
            const data_start = std.math.add(usize, offset, fixed.seek) catch return error.CorruptPage;
            const data_end = std.math.add(usize, data_start, page_size) catch return error.CorruptPage;
            if (data_end > self.bytes.len) return error.CorruptPage;
            try validatePageCrc(page_header, self.bytes[data_start..data_end]);

            if (page_header.page_type == .dictionary_page) {
                if (rows_seen != 0 or dictionary != null) return error.CorruptPage;
                dictionary = try decodeDictionaryPageData(allocator, column, schema_col, page_header, self.bytes[data_start..data_end], &scratch);
                offset = data_end;
                continue;
            }

            const page_rows = try pageRowCount(page_header);
            if (rows_seen + page_rows > row_count) return error.CorruptPage;

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
        if (!plainDictionaryPageEncoding(dict_header.encoding)) return error.UnsupportedEncoding;
        const page_size = try checkedPageSize(header.compressed_page_size);
        const data_start = std.math.add(usize, offset, fixed.seek) catch return error.CorruptPage;
        const data_end = std.math.add(usize, data_start, page_size) catch return error.CorruptPage;
        if (data_end > self.bytes.len) return error.CorruptPage;
        try validatePageCrc(header, self.bytes[data_start..data_end]);
        return try decodeDictionaryPageData(allocator, column, schema_col, header, self.bytes[data_start..data_end], scratch);
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

    pub fn readRowGroupSelectedColumnsByPath(self: *ParsedFile, allocator: std.mem.Allocator, row_group_index: usize, column_paths: []const []const u8) ![]types.OwnedColumn {
        const column_indexes = try parsedColumnIndexesByPath(self, allocator, column_paths);
        defer allocator.free(column_indexes);
        const columns = try allocator.alloc(types.OwnedColumn, column_indexes.len);
        errdefer allocator.free(columns);
        var initialized: usize = 0;
        errdefer {
            for (columns[0..initialized]) |*column| column.deinit(allocator);
        }
        while (initialized < column_indexes.len) : (initialized += 1) {
            columns[initialized] = try self.readColumn(allocator, row_group_index, column_indexes[initialized]);
        }
        return columns;
    }
};

pub const ParallelColumnReadOptions = struct {
    /// 0 means one worker per selected column.
    max_threads: usize = 0,
    /// Each worker owns its file-reader buffer. Keep this small so direct page
    /// reads do not copy large page bodies through the IO buffer.
    reader_buffer_len: usize = 1024,
    cache_dictionaries: bool = false,
};

pub const RowGroupColumns = struct {
    row_group_index: usize,
    columns: []types.OwnedColumn,

    pub fn deinit(self: *RowGroupColumns, allocator: std.mem.Allocator) void {
        for (self.columns) |*column| column.deinit(allocator);
        allocator.free(self.columns);
        self.columns = &.{};
    }
};

pub const StreamFileReader = struct {
    allocator: std.mem.Allocator,
    file_reader: *std.Io.File.Reader,
    file_size: u64,
    arena: std.heap.ArenaAllocator,
    metadata: types.FileMetaData,
    cache_dictionaries: bool = false,
    cache_identity_pages: bool = true,
    cache_identity_dictionaries: bool = true,
    dictionary_cache: std.ArrayList(DictionaryCacheEntry) = .empty,
    identity_page_cache: std.ArrayList(IdentityPageCacheEntry) = .empty,
    identity_page_cache_bytes: usize = 0,
    identity_dictionary_cache: std.ArrayList(IdentityDictionaryCacheEntry) = .empty,
    identity_dictionary_cache_bytes: usize = 0,

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
        self.clearDictionaryCache();
        self.clearIdentityPageCache();
        self.clearIdentityDictionaryCache();
        self.arena.deinit();
    }

    pub fn setDictionaryCacheEnabled(self: *StreamFileReader, enabled: bool) void {
        self.cache_dictionaries = enabled;
        if (!enabled) self.clearDictionaryCache();
    }

    pub fn clearDictionaryCache(self: *StreamFileReader) void {
        for (self.dictionary_cache.items) |*entry| entry.value.deinit(self.allocator);
        self.dictionary_cache.clearAndFree(self.allocator);
    }

    pub fn clearIdentityPageCache(self: *StreamFileReader) void {
        for (self.identity_page_cache.items) |entry| self.allocator.free(entry.compressed);
        self.identity_page_cache.clearAndFree(self.allocator);
        self.identity_page_cache_bytes = 0;
    }

    pub fn clearIdentityDictionaryCache(self: *StreamFileReader) void {
        for (self.identity_dictionary_cache.items) |*entry| entry.value.deinit(self.allocator);
        self.identity_dictionary_cache.clearAndFree(self.allocator);
        self.identity_dictionary_cache_bytes = 0;
    }

    pub fn setIdentityFastPathCacheEnabled(self: *StreamFileReader, enabled: bool) void {
        self.cache_identity_pages = enabled;
        self.cache_identity_dictionaries = enabled;
        if (!enabled) {
            self.clearIdentityPageCache();
            self.clearIdentityDictionaryCache();
        }
    }

    pub fn columnIndexByPath(self: *const StreamFileReader, dotted_path: []const u8) ?usize {
        return schemaColumnIndexByPath(self.metadata.schema, dotted_path);
    }

    pub fn readColumnByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, dotted_path: []const u8) !types.OwnedColumn {
        const column_index = self.columnIndexByPath(dotted_path) orelse return error.InvalidColumnData;
        return self.readColumn(allocator, row_group_index, column_index);
    }

    pub fn readColumn(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !types.OwnedColumn {
        if (!self.cache_dictionaries) {
            if (try self.readIdentityDictionaryColumn(allocator, row_group_index, column_index)) |column| return column;
        }
        if (try self.readPlainFixedColumnDirect(allocator, row_group_index, column_index)) |column| return column;

        var pages = try self.columnPageIterator(allocator, row_group_index, column_index);
        defer pages.deinit();

        var acc = ColumnAccumulator.init(pages.column.physical_type, pages.schema_col.repetition == .optional);
        errdefer acc.deinit(allocator);
        try acc.reserve(allocator, pages.row_count);

        while (try pages.nextInto(&acc)) {}

        return acc.finish(allocator);
    }

    pub fn readColumnTripletsByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, dotted_path: []const u8) !types.OwnedColumnTriplets {
        const column_index = self.columnIndexByPath(dotted_path) orelse return error.InvalidColumnData;
        return self.readColumnTriplets(allocator, row_group_index, column_index);
    }

    pub fn readColumnTriplets(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !types.OwnedColumnTriplets {
        var pages = try self.columnPageIteratorForTriplets(allocator, row_group_index, column_index);
        defer pages.deinit();

        const total_values = std.math.cast(usize, pages.column.num_values) orelse return error.CorruptMetadata;
        var values_acc = ColumnAccumulator.init(pages.column.physical_type, false);
        errdefer values_acc.deinit(allocator);
        try values_acc.reserve(allocator, total_values);

        var definition_levels: std.ArrayList(u16) = .empty;
        errdefer definition_levels.deinit(allocator);
        var repetition_levels: std.ArrayList(u16) = .empty;
        errdefer repetition_levels.deinit(allocator);
        try definition_levels.ensureTotalCapacity(allocator, total_values);
        try repetition_levels.ensureTotalCapacity(allocator, total_values);

        while (pages.rows_seen < pages.row_count) {
            const page = try pages.readNextDataPage();
            const dict = if (pageUsesDictionary(page.header, pages.column, pages.dictionary_ref != null))
                try pages.ensureDictionary(self.file_reader.logicalPos())
            else
                null;

            var page_triplets = try decodeColumnPageTriplets(
                allocator,
                pages.column,
                pages.schema_col,
                page.header,
                page.data,
                dict,
                &pages.scratch,
            );
            defer page_triplets.deinit(allocator);

            const page_rows = try tripletPageRowCount(page.header, page_triplets.repetition_levels, pages.schema_col.max_repetition_level);
            if (page_rows == 0 or pages.rows_seen + page_rows > pages.row_count) return error.CorruptPage;
            try values_acc.append(allocator, page_triplets.values);
            try definition_levels.appendSlice(allocator, page_triplets.definition_levels);
            try repetition_levels.appendSlice(allocator, page_triplets.repetition_levels);
            pages.rows_seen += page_rows;
        }

        var values = try values_acc.finish(allocator);
        errdefer values.deinit(allocator);
        const owned_definition_levels = try definition_levels.toOwnedSlice(allocator);
        errdefer allocator.free(owned_definition_levels);
        const owned_repetition_levels = try repetition_levels.toOwnedSlice(allocator);
        errdefer allocator.free(owned_repetition_levels);
        const row_offsets = try buildTripletRowOffsets(allocator, owned_repetition_levels, pages.schema_col.max_repetition_level, pages.row_count);
        errdefer allocator.free(row_offsets);
        const value_offsets = try buildTripletValueOffsets(allocator, owned_definition_levels, row_offsets, pages.schema_col.max_definition_level);
        errdefer allocator.free(value_offsets);
        const repeated_level_offsets = try buildTripletRepeatedLevelOffsets(allocator, owned_repetition_levels, pages.schema_col.max_repetition_level);
        errdefer freeTripletLevelOffsets(allocator, repeated_level_offsets);
        if (ownedColumnValueCount(values) != value_offsets[value_offsets.len - 1]) return error.CorruptPage;

        return .{
            .values = values,
            .definition_levels = owned_definition_levels,
            .repetition_levels = owned_repetition_levels,
            .row_offsets = row_offsets,
            .value_offsets = value_offsets,
            .repeated_level_offsets = repeated_level_offsets,
            .max_definition_level = pages.schema_col.max_definition_level,
            .max_repetition_level = pages.schema_col.max_repetition_level,
        };
    }

    pub fn readColumnNestedTripletsByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, dotted_path: []const u8) !types.OwnedNestedColumnTriplets {
        const column_index = self.columnIndexByPath(dotted_path) orelse return error.InvalidColumnData;
        return self.readColumnNestedTriplets(allocator, row_group_index, column_index);
    }

    pub fn readColumnNestedTriplets(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !types.OwnedNestedColumnTriplets {
        if (column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        const schema_col = self.metadata.schema.columns[column_index];
        var triplets = try self.readColumnTriplets(allocator, row_group_index, column_index);
        errdefer triplets.deinit(allocator);

        if (schema_col.repeated_level_info.len != triplets.repeated_level_offsets.len) return error.CorruptMetadata;
        const column_path = try dupePath(allocator, schema_col.path);
        errdefer freePath(allocator, column_path);

        const repeated_levels = try allocator.alloc(types.NestedTripletLevel, triplets.repeated_level_offsets.len);
        var initialized_levels: usize = 0;
        errdefer {
            for (repeated_levels[0..initialized_levels]) |level| freePath(allocator, level.path);
            allocator.free(repeated_levels);
        }

        for (triplets.repeated_level_offsets, schema_col.repeated_level_info, 0..) |level_offsets, level_info, idx| {
            if (level_offsets.repetition_level != level_info.repetition_level) return error.CorruptMetadata;
            if (level_info.path.len == 0 or level_info.path.len > schema_col.path.len) return error.CorruptMetadata;
            for (level_info.path, 0..) |part, part_index| {
                if (!std.mem.eql(u8, part, schema_col.path[part_index])) return error.CorruptMetadata;
            }
            const level_path = try dupePath(allocator, level_info.path);
            repeated_levels[idx] = .{
                .repetition_level = level_offsets.repetition_level,
                .path = level_path,
                .offsets = level_offsets.offsets,
            };
            initialized_levels += 1;
        }

        const logical_levels = try allocator.alloc(types.NestedLogicalInfo, schema_col.nested_logical_info.len);
        var initialized_logical_levels: usize = 0;
        errdefer {
            for (logical_levels[0..initialized_logical_levels]) |level| freePath(allocator, level.path);
            allocator.free(logical_levels);
        }
        var has_list_logical = false;
        var has_map_logical = false;
        for (schema_col.nested_logical_info, 0..) |logical_info, idx| {
            if (logical_info.definition_level > triplets.max_definition_level) return error.CorruptMetadata;
            if (logical_info.repetition_level > triplets.max_repetition_level) return error.CorruptMetadata;
            if (logical_info.path.len == 0 or logical_info.path.len > schema_col.path.len) return error.CorruptMetadata;
            for (logical_info.path, 0..) |part, part_index| {
                if (!std.mem.eql(u8, part, schema_col.path[part_index])) return error.CorruptMetadata;
            }
            switch (logical_info.kind) {
                .list => has_list_logical = true,
                .map => has_map_logical = true,
            }
            const logical_path = try dupePath(allocator, logical_info.path);
            logical_levels[idx] = .{
                .kind = logical_info.kind,
                .definition_level = logical_info.definition_level,
                .repetition_level = logical_info.repetition_level,
                .path = logical_path,
                .optional = logical_info.optional,
            };
            initialized_logical_levels += 1;
        }
        if (schema_col.list_info != null and !has_list_logical) return error.CorruptMetadata;
        if (schema_col.map_info != null and !has_map_logical) return error.CorruptMetadata;

        return .{
            .triplets = triplets,
            .column_path = column_path,
            .repeated_levels = repeated_levels,
            .logical_levels = logical_levels,
        };
    }

    pub fn readColumnNestedLogicalByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, dotted_path: []const u8) !types.OwnedNestedLogicalColumn {
        const column_index = self.columnIndexByPath(dotted_path) orelse return error.InvalidColumnData;
        return self.readColumnNestedLogical(allocator, row_group_index, column_index);
    }

    pub fn readColumnNestedLogical(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !types.OwnedNestedLogicalColumn {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        const schema_col = self.metadata.schema.columns[column_index];
        if (schema_col.nested_logical_info.len == 0) return error.UnsupportedNestedSchema;

        var triplets = try self.readColumnTriplets(allocator, row_group_index, column_index);
        var transferred_values = false;
        errdefer if (!transferred_values) triplets.deinit(allocator);

        var nested = try buildNestedLogicalColumn(allocator, &triplets, schema_col.nested_logical_info);
        errdefer nested.deinit(allocator);
        try attachElementValidity(&triplets.values, nested.leaf_validity, nested.leaf_present_count, nested.leaf_slot_count);
        nested.leaf_validity = null;

        const values = triplets.values;
        transferred_values = true;
        freeTripletScaffolding(allocator, &triplets);

        return .{
            .values = values,
            .levels = nested.levels,
            .max_definition_level = triplets.max_definition_level,
            .max_repetition_level = triplets.max_repetition_level,
        };
    }

    pub fn readColumnNestedMapPairByPath(
        self: *StreamFileReader,
        allocator: std.mem.Allocator,
        row_group_index: usize,
        key_path: []const u8,
        value_path: ?[]const u8,
    ) !types.OwnedNestedMapPair {
        const key_column_index = self.columnIndexByPath(key_path) orelse return error.InvalidColumnData;
        const value_column_index: ?usize = if (value_path) |path|
            self.columnIndexByPath(path) orelse return error.InvalidColumnData
        else
            null;
        return self.readColumnNestedMapPair(allocator, row_group_index, key_column_index, value_column_index);
    }

    pub fn readColumnNestedMapPair(
        self: *StreamFileReader,
        allocator: std.mem.Allocator,
        row_group_index: usize,
        key_column_index: usize,
        value_column_index: ?usize,
    ) !types.OwnedNestedMapPair {
        if (key_column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        const key_schema_col = self.metadata.schema.columns[key_column_index];
        const value_schema_col = if (value_column_index) |index| blk: {
            if (index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
            break :blk self.metadata.schema.columns[index];
        } else null;
        const map_indexes = try nestedMapPairLevelIndexes(key_schema_col.nested_logical_info, if (value_schema_col) |column| column.nested_logical_info else null);

        var keys = try self.readColumnNestedLogical(allocator, row_group_index, key_column_index);
        errdefer keys.deinit(allocator);

        if (value_column_index) |index| {
            var values = try self.readColumnNestedLogical(allocator, row_group_index, index);
            errdefer values.deinit(allocator);
            try validateNestedMapPair(&keys, &values, map_indexes);
            return .{
                .keys = keys,
                .values = values,
                .key_map_level_index = map_indexes.key,
                .value_map_level_index = map_indexes.value,
            };
        }

        try validateNestedMapPair(&keys, null, map_indexes);
        return .{
            .keys = keys,
            .key_map_level_index = map_indexes.key,
        };
    }

    pub fn readColumnListByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, dotted_path: []const u8) !types.OwnedListColumn {
        const column_index = self.columnIndexByPath(dotted_path) orelse return error.InvalidColumnData;
        return self.readColumnList(allocator, row_group_index, column_index);
    }

    pub fn readColumnList(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !types.OwnedListColumn {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        const schema_col = self.metadata.schema.columns[column_index];
        const list_info = schema_col.list_info orelse legacyRepeatedPrimitiveListInfo(schema_col) orelse return error.UnsupportedNestedSchema;
        const row_count = std.math.cast(usize, row_group.num_rows) orelse return error.CorruptMetadata;

        var triplets = try self.readColumnTriplets(allocator, row_group_index, column_index);
        var transferred_values = false;
        errdefer if (!transferred_values) triplets.deinit(allocator);

        const out = try assembleListColumn(allocator, &triplets, row_count, list_info);
        transferred_values = true;
        allocator.free(triplets.definition_levels);
        allocator.free(triplets.repetition_levels);
        allocator.free(triplets.row_offsets);
        allocator.free(triplets.value_offsets);
        freeTripletLevelOffsets(allocator, triplets.repeated_level_offsets);
        return out;
    }

    pub fn readColumnMapByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, key_path: []const u8, value_path: ?[]const u8) !types.OwnedMapColumn {
        const key_column_index = self.columnIndexByPath(key_path) orelse return error.InvalidColumnData;
        const value_column_index: ?usize = if (value_path) |path|
            self.columnIndexByPath(path) orelse return error.InvalidColumnData
        else
            null;
        return self.readColumnMap(allocator, row_group_index, key_column_index, value_column_index);
    }

    pub fn readColumnMap(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, key_column_index: usize, value_column_index: ?usize) !types.OwnedMapColumn {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (key_column_index >= row_group.columns.len or key_column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        const key_schema_col = self.metadata.schema.columns[key_column_index];
        const map_info = key_schema_col.map_info orelse return error.UnsupportedNestedSchema;
        const row_count = std.math.cast(usize, row_group.num_rows) orelse return error.CorruptMetadata;

        var key_triplets = try self.readColumnTriplets(allocator, row_group_index, key_column_index);
        var transferred_keys = false;
        errdefer if (!transferred_keys) key_triplets.deinit(allocator);

        if (value_column_index) |value_index| {
            if (value_index >= row_group.columns.len or value_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
            const value_schema_col = self.metadata.schema.columns[value_index];
            const value_map_info = value_schema_col.map_info orelse return error.UnsupportedNestedSchema;
            if (value_map_info.map_definition_level != map_info.map_definition_level) return error.UnsupportedNestedSchema;

            var value_triplets = try self.readColumnTriplets(allocator, row_group_index, value_index);
            var transferred_values = false;
            errdefer if (!transferred_values) value_triplets.deinit(allocator);

            const out = try assembleMapColumn(allocator, &key_triplets, &value_triplets, row_count, map_info);
            transferred_keys = true;
            transferred_values = true;
            allocator.free(key_triplets.definition_levels);
            allocator.free(key_triplets.repetition_levels);
            allocator.free(key_triplets.row_offsets);
            allocator.free(key_triplets.value_offsets);
            freeTripletLevelOffsets(allocator, key_triplets.repeated_level_offsets);
            allocator.free(value_triplets.definition_levels);
            allocator.free(value_triplets.repetition_levels);
            allocator.free(value_triplets.row_offsets);
            allocator.free(value_triplets.value_offsets);
            freeTripletLevelOffsets(allocator, value_triplets.repeated_level_offsets);
            return out;
        }

        const out = try assembleMapColumn(allocator, &key_triplets, null, row_count, map_info);
        transferred_keys = true;
        allocator.free(key_triplets.definition_levels);
        allocator.free(key_triplets.repetition_levels);
        allocator.free(key_triplets.row_offsets);
        allocator.free(key_triplets.value_offsets);
        freeTripletLevelOffsets(allocator, key_triplets.repeated_level_offsets);
        return out;
    }

    fn legacyRepeatedPrimitiveListInfo(column: types.Column) ?types.ListInfo {
        if (column.repetition == .repeated and column.max_definition_level == 1 and column.max_repetition_level == 1) {
            return .{ .list_definition_level = 0 };
        }
        return null;
    }

    fn assembleListColumn(allocator: std.mem.Allocator, triplets: *types.OwnedColumnTriplets, row_count: usize, list_info: types.ListInfo) !types.OwnedListColumn {
        if (triplets.max_repetition_level != 1) return error.UnsupportedNestedSchema;
        if (list_info.list_definition_level > triplets.max_definition_level) return error.CorruptPage;
        if (triplets.max_definition_level <= list_info.list_definition_level) return error.UnsupportedNestedSchema;
        if (triplets.max_definition_level > list_info.list_definition_level + 2) return error.UnsupportedNestedSchema;
        if (triplets.definition_levels.len != triplets.repetition_levels.len) return error.CorruptPage;

        const offsets_len = std.math.add(usize, row_count, 1) catch return error.CorruptPage;
        const offsets = try allocator.alloc(usize, offsets_len);
        errdefer allocator.free(offsets);
        offsets[0] = 0;

        const list_validity: ?[]bool = if (list_info.list_definition_level > 0)
            try allocator.alloc(bool, row_count)
        else
            null;
        errdefer if (list_validity) |validity| allocator.free(validity);

        const element_optional = triplets.max_definition_level > list_info.list_definition_level + 1;
        var element_validity: std.ArrayList(bool) = .empty;
        errdefer element_validity.deinit(allocator);

        if (row_count == 0) {
            if (triplets.definition_levels.len != 0) return error.CorruptPage;
            if (ownedColumnValueCount(triplets.values) != 0) return error.CorruptPage;
            const final_element_validity = if (element_optional) try element_validity.toOwnedSlice(allocator) else null;
            errdefer if (final_element_validity) |validity| allocator.free(validity);
            try attachElementValidity(&triplets.values, final_element_validity, 0, 0);
            return .{
                .values = triplets.values,
                .offsets = offsets,
                .validity = list_validity,
                .max_definition_level = triplets.max_definition_level,
                .max_repetition_level = triplets.max_repetition_level,
            };
        }
        if (triplets.definition_levels.len == 0) return error.CorruptPage;

        var row_index: usize = 0;
        var row_started = false;
        var element_count: usize = 0;
        var present_value_count: usize = 0;
        for (triplets.definition_levels, triplets.repetition_levels) |definition_level, repetition_level| {
            if (definition_level > triplets.max_definition_level or repetition_level > triplets.max_repetition_level) return error.CorruptPage;
            if (repetition_level == 0) {
                if (row_started) {
                    offsets[row_index + 1] = element_count;
                    row_index += 1;
                }
                if (row_index >= row_count) return error.CorruptPage;
                row_started = true;
                if (list_validity) |validity| validity[row_index] = definition_level >= list_info.list_definition_level;
            } else if (!row_started) {
                return error.CorruptPage;
            }

            if (definition_level > list_info.list_definition_level) {
                element_count += 1;
                const present = definition_level == triplets.max_definition_level;
                if (element_optional) {
                    try element_validity.append(allocator, present);
                } else if (!present) {
                    return error.CorruptPage;
                }
                if (present) present_value_count += 1;
            }
        }
        if (!row_started) return error.CorruptPage;
        offsets[row_index + 1] = element_count;
        row_index += 1;
        if (row_index != row_count) return error.CorruptPage;
        if (ownedColumnValueCount(triplets.values) != present_value_count) return error.CorruptPage;

        const final_element_validity = if (element_optional) try element_validity.toOwnedSlice(allocator) else null;
        errdefer if (final_element_validity) |validity| allocator.free(validity);
        try attachElementValidity(&triplets.values, final_element_validity, present_value_count, element_count);

        return .{
            .values = triplets.values,
            .offsets = offsets,
            .validity = list_validity,
            .max_definition_level = triplets.max_definition_level,
            .max_repetition_level = triplets.max_repetition_level,
        };
    }

    fn assembleMapColumn(
        allocator: std.mem.Allocator,
        key_triplets: *types.OwnedColumnTriplets,
        value_triplets: ?*types.OwnedColumnTriplets,
        row_count: usize,
        map_info: types.MapInfo,
    ) !types.OwnedMapColumn {
        if (key_triplets.max_repetition_level != 1) return error.UnsupportedNestedSchema;
        if (map_info.map_definition_level > key_triplets.max_definition_level) return error.CorruptPage;
        if (key_triplets.max_definition_level != map_info.map_definition_level + 1) return error.UnsupportedNestedSchema;
        if (key_triplets.definition_levels.len != key_triplets.repetition_levels.len) return error.CorruptPage;

        const value_optional = if (value_triplets) |values| blk: {
            if (values.max_repetition_level != key_triplets.max_repetition_level) return error.UnsupportedNestedSchema;
            if (values.definition_levels.len != key_triplets.definition_levels.len or values.repetition_levels.len != key_triplets.repetition_levels.len) return error.CorruptPage;
            if (values.max_definition_level < map_info.map_definition_level + 1) return error.UnsupportedNestedSchema;
            if (values.max_definition_level > map_info.map_definition_level + 2) return error.UnsupportedNestedSchema;
            break :blk values.max_definition_level == map_info.map_definition_level + 2;
        } else false;

        const offsets_len = std.math.add(usize, row_count, 1) catch return error.CorruptPage;
        const offsets = try allocator.alloc(usize, offsets_len);
        errdefer allocator.free(offsets);
        offsets[0] = 0;

        const map_validity: ?[]bool = if (map_info.map_definition_level > 0)
            try allocator.alloc(bool, row_count)
        else
            null;
        errdefer if (map_validity) |validity| allocator.free(validity);

        var value_validity: std.ArrayList(bool) = .empty;
        errdefer value_validity.deinit(allocator);

        if (row_count == 0) {
            if (key_triplets.definition_levels.len != 0) return error.CorruptPage;
            if (ownedColumnValueCount(key_triplets.values) != 0) return error.CorruptPage;
            if (value_triplets) |values| {
                if (ownedColumnValueCount(values.values) != 0) return error.CorruptPage;
                const final_value_validity = if (value_optional) try value_validity.toOwnedSlice(allocator) else null;
                errdefer if (final_value_validity) |validity| allocator.free(validity);
                try attachElementValidity(&values.values, final_value_validity, 0, 0);
            }
            const values_column: ?types.OwnedColumn = if (value_triplets) |values| values.values else null;
            return .{
                .keys = key_triplets.values,
                .values = values_column,
                .offsets = offsets,
                .validity = map_validity,
                .max_definition_level = key_triplets.max_definition_level,
                .max_repetition_level = key_triplets.max_repetition_level,
            };
        }
        if (key_triplets.definition_levels.len == 0) return error.CorruptPage;

        var row_index: usize = 0;
        var row_started = false;
        var entry_count: usize = 0;
        var present_value_count: usize = 0;
        for (key_triplets.definition_levels, key_triplets.repetition_levels, 0..) |key_definition_level, key_repetition_level, level_index| {
            if (key_definition_level > key_triplets.max_definition_level or key_repetition_level > key_triplets.max_repetition_level) return error.CorruptPage;
            if (key_repetition_level == 0) {
                if (row_started) {
                    offsets[row_index + 1] = entry_count;
                    row_index += 1;
                }
                if (row_index >= row_count) return error.CorruptPage;
                row_started = true;
                if (map_validity) |validity| validity[row_index] = key_definition_level >= map_info.map_definition_level;
            } else if (!row_started) {
                return error.CorruptPage;
            }

            const key_has_entry = key_definition_level > map_info.map_definition_level;
            if (key_has_entry and key_definition_level != key_triplets.max_definition_level) return error.CorruptPage;

            if (value_triplets) |values| {
                const value_definition_level = values.definition_levels[level_index];
                const value_repetition_level = values.repetition_levels[level_index];
                if (value_definition_level > values.max_definition_level or value_repetition_level > values.max_repetition_level) return error.CorruptPage;
                if (value_repetition_level != key_repetition_level) return error.CorruptPage;
                const value_has_entry = value_definition_level > map_info.map_definition_level;
                if (value_has_entry != key_has_entry) return error.CorruptPage;
                if (key_has_entry) {
                    const value_present = value_definition_level == values.max_definition_level;
                    if (value_optional) {
                        try value_validity.append(allocator, value_present);
                    } else if (!value_present) {
                        return error.CorruptPage;
                    }
                    if (value_present) present_value_count += 1;
                }
            }

            if (key_has_entry) entry_count += 1;
        }
        if (!row_started) return error.CorruptPage;
        offsets[row_index + 1] = entry_count;
        row_index += 1;
        if (row_index != row_count) return error.CorruptPage;
        if (ownedColumnValueCount(key_triplets.values) != entry_count) return error.CorruptPage;

        if (value_triplets) |values| {
            if (ownedColumnValueCount(values.values) != present_value_count) return error.CorruptPage;
            const final_value_validity = if (value_optional) try value_validity.toOwnedSlice(allocator) else null;
            errdefer if (final_value_validity) |validity| allocator.free(validity);
            try attachElementValidity(&values.values, final_value_validity, present_value_count, entry_count);
        }

        const values_column: ?types.OwnedColumn = if (value_triplets) |values| values.values else null;
        return .{
            .keys = key_triplets.values,
            .values = values_column,
            .offsets = offsets,
            .validity = map_validity,
            .max_definition_level = key_triplets.max_definition_level,
            .max_repetition_level = key_triplets.max_repetition_level,
        };
    }

    fn attachElementValidity(column: *types.OwnedColumn, validity: ?[]bool, present_value_count: usize, element_count: usize) !void {
        if (validity) |values_validity| {
            if (values_validity.len != element_count) return error.CorruptPage;
            var valid_count: usize = 0;
            for (values_validity) |valid| {
                if (valid) valid_count += 1;
            }
            if (valid_count != present_value_count) return error.CorruptPage;
        } else if (present_value_count != element_count) {
            return error.CorruptPage;
        }

        switch (column.*) {
            inline else => |*values| {
                if (values.values.len != present_value_count) return error.CorruptPage;
                if (values.validity != null) return error.CorruptPage;
                values.validity = validity;
            },
        }
    }

    fn ownedColumnValueCount(column: types.OwnedColumn) usize {
        return switch (column) {
            inline else => |values| values.values.len,
        };
    }

    const NestedLogicalBuild = struct {
        levels: []types.NestedLogicalColumnLevel,
        leaf_validity: ?[]bool,
        leaf_present_count: usize,
        leaf_slot_count: usize,

        fn deinit(self: *NestedLogicalBuild, allocator: std.mem.Allocator) void {
            freeNestedLogicalColumnLevels(allocator, self.levels);
            if (self.leaf_validity) |validity| allocator.free(validity);
        }
    };

    fn buildNestedLogicalColumn(
        allocator: std.mem.Allocator,
        triplets: *const types.OwnedColumnTriplets,
        logical_infos: []const types.NestedLogicalInfo,
    ) !NestedLogicalBuild {
        if (logical_infos.len == 0) return error.UnsupportedNestedSchema;
        if (triplets.definition_levels.len != triplets.repetition_levels.len) return error.CorruptPage;
        if (triplets.row_offsets.len == 0 or triplets.value_offsets.len == 0) return error.CorruptPage;

        const levels = try allocator.alloc(types.NestedLogicalColumnLevel, logical_infos.len);
        var initialized_levels: usize = 0;
        errdefer {
            for (levels[0..initialized_levels]) |level| {
                freePath(allocator, level.path);
                allocator.free(level.offsets);
                if (level.validity) |validity| allocator.free(validity);
            }
            allocator.free(levels);
        }

        var parent_spans = try tripletSpansFromOffsets(allocator, triplets.row_offsets);
        defer allocator.free(parent_spans);
        for (logical_infos, 0..) |logical_info, level_index| {
            if (logical_info.definition_level > triplets.max_definition_level) return error.CorruptMetadata;
            if (logical_info.repetition_level >= triplets.max_repetition_level) return error.UnsupportedNestedSchema;
            if (level_index > 0 and logical_info.repetition_level < logical_infos[level_index - 1].repetition_level) return error.UnsupportedNestedSchema;

            const child_offsets = try logicalChildOffsets(triplets, logical_info.repetition_level);
            const layout = try buildNestedLogicalLevel(allocator, triplets.definition_levels, parent_spans, child_offsets, logical_info);
            errdefer {
                freePath(allocator, layout.path);
                allocator.free(layout.offsets);
                if (layout.validity) |validity| allocator.free(validity);
                allocator.free(layout.child_spans);
            }
            levels[level_index] = layout.level;
            initialized_levels += 1;
            allocator.free(parent_spans);
            parent_spans = layout.child_spans;
        }

        const leaf = try buildNestedLeafValidity(allocator, triplets, logical_infos[logical_infos.len - 1]);
        errdefer if (leaf.validity) |validity| allocator.free(validity);
        const last_level = levels[levels.len - 1];
        if (last_level.offsets.len == 0 or last_level.offsets[last_level.offsets.len - 1] != leaf.slot_count) return error.CorruptPage;
        if (parent_spans.len != leaf.slot_count) return error.CorruptPage;

        return .{
            .levels = levels,
            .leaf_validity = leaf.validity,
            .leaf_present_count = leaf.present_count,
            .leaf_slot_count = leaf.slot_count,
        };
    }

    fn logicalChildOffsets(triplets: *const types.OwnedColumnTriplets, repetition_level: u16) ![]const usize {
        const child_level = std.math.add(u16, repetition_level, 1) catch return error.CorruptPage;
        const level_index: usize = child_level - 1;
        if (level_index >= triplets.repeated_level_offsets.len) return error.UnsupportedNestedSchema;
        const offsets = triplets.repeated_level_offsets[level_index];
        if (offsets.repetition_level != child_level) return error.CorruptPage;
        return offsets.offsets;
    }

    const TripletSpan = struct {
        start: usize,
        end: usize,
    };

    fn tripletSpansFromOffsets(allocator: std.mem.Allocator, offsets: []const usize) ![]TripletSpan {
        if (offsets.len == 0) return error.CorruptPage;
        const spans = try allocator.alloc(TripletSpan, offsets.len - 1);
        errdefer allocator.free(spans);
        for (spans, 0..) |*span, idx| {
            if (offsets[idx] >= offsets[idx + 1]) return error.CorruptPage;
            span.* = .{ .start = offsets[idx], .end = offsets[idx + 1] };
        }
        return spans;
    }

    const NestedLogicalLevelBuild = struct {
        level: types.NestedLogicalColumnLevel,
        child_spans: []TripletSpan,
    };

    fn buildNestedLogicalLevel(
        allocator: std.mem.Allocator,
        definition_levels: []const u16,
        parent_spans: []const TripletSpan,
        child_offsets: []const usize,
        logical_info: types.NestedLogicalInfo,
    ) !NestedLogicalLevelBuild {
        if (child_offsets.len == 0) return error.CorruptPage;
        if (child_offsets[child_offsets.len - 1] != definition_levels.len) return error.CorruptPage;

        const parent_count = parent_spans.len;
        const offsets = try allocator.alloc(usize, parent_count + 1);
        errdefer allocator.free(offsets);
        offsets[0] = 0;

        const validity: ?[]bool = if (logical_info.optional)
            try allocator.alloc(bool, parent_count)
        else
            null;
        errdefer if (validity) |owned_validity| allocator.free(owned_validity);

        var child_spans: std.ArrayList(TripletSpan) = .empty;
        errdefer child_spans.deinit(allocator);
        var child_slot_count: usize = 0;
        var child_index: usize = 0;
        for (parent_spans, 0..) |parent_span, parent_index| {
            const start = parent_span.start;
            const end = parent_span.end;
            if (start >= end or end > definition_levels.len) return error.CorruptPage;
            if (validity) |owned_validity| {
                owned_validity[parent_index] = definition_levels[start] >= logical_info.definition_level;
            } else if (definition_levels[start] < logical_info.definition_level) {
                return error.CorruptPage;
            }

            while (child_index + 1 < child_offsets.len and child_offsets[child_index + 1] <= start) : (child_index += 1) {}
            var scan = child_index;
            while (scan + 1 < child_offsets.len and child_offsets[scan] < end) : (scan += 1) {
                const child_start = child_offsets[scan];
                const child_end = child_offsets[scan + 1];
                if (child_start < start or child_end > end or child_start >= child_end) return error.CorruptPage;
                if (definition_levels[child_start] > logical_info.definition_level) {
                    try child_spans.append(allocator, .{ .start = child_start, .end = child_end });
                    child_slot_count += 1;
                }
            }
            child_index = scan;
            offsets[parent_index + 1] = child_slot_count;
        }

        const path = try dupePath(allocator, logical_info.path);
        errdefer freePath(allocator, path);
        const final_child_spans = try child_spans.toOwnedSlice(allocator);
        errdefer allocator.free(final_child_spans);

        return .{
            .level = .{
                .kind = logical_info.kind,
                .definition_level = logical_info.definition_level,
                .repetition_level = logical_info.repetition_level,
                .path = path,
                .offsets = offsets,
                .validity = validity,
            },
            .child_spans = final_child_spans,
        };
    }

    const LeafValidityBuild = struct {
        validity: ?[]bool,
        present_count: usize,
        slot_count: usize,
    };

    fn buildNestedLeafValidity(
        allocator: std.mem.Allocator,
        triplets: *const types.OwnedColumnTriplets,
        last_logical: types.NestedLogicalInfo,
    ) !LeafValidityBuild {
        const leaf_parent_definition_level = std.math.add(u16, last_logical.definition_level, 1) catch return error.CorruptPage;
        if (triplets.max_definition_level < leaf_parent_definition_level) return error.CorruptPage;
        const leaf_optional = triplets.max_definition_level > leaf_parent_definition_level;

        var slot_count: usize = 0;
        var present_count: usize = 0;
        for (triplets.definition_levels) |definition_level| {
            if (definition_level > triplets.max_definition_level) return error.CorruptPage;
            if (definition_level > last_logical.definition_level) {
                slot_count += 1;
                if (definition_level == triplets.max_definition_level) present_count += 1;
            }
        }
        if (ownedColumnValueCount(triplets.values) != present_count) return error.CorruptPage;
        if (!leaf_optional and present_count != slot_count) return error.CorruptPage;

        const validity: ?[]bool = if (leaf_optional)
            try allocator.alloc(bool, slot_count)
        else
            null;
        errdefer if (validity) |owned_validity| allocator.free(owned_validity);
        if (validity) |owned_validity| {
            var index: usize = 0;
            for (triplets.definition_levels) |definition_level| {
                if (definition_level > last_logical.definition_level) {
                    owned_validity[index] = definition_level == triplets.max_definition_level;
                    index += 1;
                }
            }
            if (index != slot_count) return error.CorruptPage;
        }

        return .{
            .validity = validity,
            .present_count = present_count,
            .slot_count = slot_count,
        };
    }

    fn freeNestedLogicalColumnLevels(allocator: std.mem.Allocator, levels: []types.NestedLogicalColumnLevel) void {
        for (levels) |level| {
            freePath(allocator, level.path);
            allocator.free(level.offsets);
            if (level.validity) |validity| allocator.free(validity);
        }
        allocator.free(levels);
    }

    fn freeTripletScaffolding(allocator: std.mem.Allocator, triplets: *types.OwnedColumnTriplets) void {
        allocator.free(triplets.definition_levels);
        allocator.free(triplets.repetition_levels);
        allocator.free(triplets.row_offsets);
        allocator.free(triplets.value_offsets);
        freeTripletLevelOffsets(allocator, triplets.repeated_level_offsets);
        triplets.definition_levels = &.{};
        triplets.repetition_levels = &.{};
        triplets.row_offsets = &.{};
        triplets.value_offsets = &.{};
        triplets.repeated_level_offsets = &.{};
    }

    const NestedMapPairLevelIndexes = struct {
        key: usize,
        value: ?usize = null,
    };

    fn nestedMapPairLevelIndexes(key_infos: []const types.NestedLogicalInfo, value_infos: ?[]const types.NestedLogicalInfo) !NestedMapPairLevelIndexes {
        if (value_infos) |values| {
            var found: ?NestedMapPairLevelIndexes = null;
            for (key_infos, 0..) |key_info, key_index| {
                if (key_info.kind != .map) continue;
                for (values, 0..) |value_info, value_index| {
                    if (value_info.kind != .map) continue;
                    if (!pathPartsEqual(key_info.path, value_info.path)) continue;
                    if (key_info.definition_level != value_info.definition_level or key_info.repetition_level != value_info.repetition_level) return error.CorruptMetadata;
                    found = .{ .key = key_index, .value = value_index };
                }
            }
            return found orelse error.UnsupportedNestedSchema;
        }

        var index = key_infos.len;
        while (index > 0) {
            index -= 1;
            if (key_infos[index].kind == .map) return .{ .key = index };
        }
        return error.UnsupportedNestedSchema;
    }

    fn validateNestedMapPair(
        keys: *const types.OwnedNestedLogicalColumn,
        values: ?*const types.OwnedNestedLogicalColumn,
        indexes: NestedMapPairLevelIndexes,
    ) !void {
        if (indexes.key >= keys.levels.len) return error.CorruptMetadata;
        const key_level = keys.levels[indexes.key];
        if (key_level.kind != .map) return error.CorruptMetadata;
        if (indexes.key + 1 != keys.levels.len) return error.UnsupportedNestedSchema;
        const entry_count = nestedLogicalLevelChildCount(key_level);
        if (ownedColumnSlotCount(keys.values) != entry_count) return error.CorruptPage;
        if (ownedColumnValidityLen(keys.values) != null) return error.CorruptPage;

        if (values) |value_column| {
            const value_index = indexes.value orelse return error.CorruptMetadata;
            if (value_index >= value_column.levels.len) return error.CorruptMetadata;
            const value_level = value_column.levels[value_index];
            if (value_level.kind != .map) return error.CorruptMetadata;
            if (!nestedLogicalLevelsMatch(key_level, value_level)) return error.CorruptPage;
        } else if (indexes.value != null) {
            return error.CorruptMetadata;
        }
    }

    fn nestedLogicalLevelChildCount(level: types.NestedLogicalColumnLevel) usize {
        if (level.offsets.len == 0) return 0;
        return level.offsets[level.offsets.len - 1];
    }

    fn nestedLogicalLevelsMatch(a: types.NestedLogicalColumnLevel, b: types.NestedLogicalColumnLevel) bool {
        if (a.kind != b.kind or a.definition_level != b.definition_level or a.repetition_level != b.repetition_level) return false;
        if (!pathPartsEqual(a.path, b.path)) return false;
        if (!std.mem.eql(usize, a.offsets, b.offsets)) return false;
        if (a.validity == null or b.validity == null) return a.validity == null and b.validity == null;
        return std.mem.eql(bool, a.validity.?, b.validity.?);
    }

    fn pathPartsEqual(a: []const []const u8, b: []const []const u8) bool {
        if (a.len != b.len) return false;
        for (a, b) |a_part, b_part| {
            if (!std.mem.eql(u8, a_part, b_part)) return false;
        }
        return true;
    }

    fn ownedColumnSlotCount(column: types.OwnedColumn) usize {
        return switch (column) {
            inline else => |values| if (values.validity) |validity| validity.len else values.values.len,
        };
    }

    fn ownedColumnValidityLen(column: types.OwnedColumn) ?usize {
        return switch (column) {
            inline else => |values| if (values.validity) |validity| validity.len else null,
        };
    }

    fn readIdentityDictionaryColumn(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !?types.OwnedColumn {
        var pages = try self.columnPageIterator(allocator, row_group_index, column_index);
        defer pages.deinit();
        if (pages.schema_col.repetition == .repeated or pages.column.dictionary_page_offset == null) return null;
        switch (pages.column.physical_type) {
            .int32, .int64, .float, .double => {},
            else => return null,
        }
        const optional = pages.schema_col.repetition == .optional;

        while (pages.rows_seen < pages.row_count) {
            const page_header = try thrift.readPageHeader(&self.file_reader.interface);
            if (page_header.page_type != .data_page) return null;
            const data_header = page_header.data_page_header orelse return error.CorruptPage;
            if (data_header.encoding != .rle_dictionary and data_header.encoding != .plain_dictionary) return null;

            const page_rows = std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage;
            if (pages.rows_seen + page_rows > pages.row_count) return error.CorruptPage;
            const page_size = try checkedPageSize(page_header.compressed_page_size);

            const page_data = try pages.scratch.pageData(allocator, page_size);
            try self.file_reader.interface.readSliceAll(page_data);
            try validatePageCrc(page_header, page_data);

            const page_uncompressed_size = try checkedPageSize(page_header.uncompressed_page_size);
            if (self.cache_identity_pages and self.cachedIdentityPage(pages.column.codec, page_uncompressed_size, page_rows, pages.rows_seen, optional, page_data)) {
                pages.rows_seen += page_rows;
                continue;
            }

            const page_bytes = try preparePageBytes(allocator, pages.column.codec, page_header, page_data, &pages.scratch);
            defer page_bytes.deinit(allocator);
            var data = page_bytes.data;
            if (optional) {
                const levels = try plain.definitionLevelsAllValidForEncodingMax(data_header.definition_level_encoding, data, page_rows, pages.schema_col.max_definition_level);
                if (!levels.all_valid) return null;
                data = data[levels.consumed..];
            }
            if (data.len == 0) return error.CorruptPage;
            const bit_width = data[0];
            if (!try plain.rleBitPackedUint32IdentityFrom(data[1..], bit_width, page_rows, pages.rows_seen)) return null;
            if (self.cache_identity_pages) self.rememberIdentityPage(pages.column.codec, page_uncompressed_size, page_rows, pages.rows_seen, optional, page_data);
            pages.rows_seen += page_rows;
        }

        if (self.cache_identity_dictionaries) {
            if (self.cachedIdentityDictionary(row_group_index, column_index)) |cached| {
                return try cloneFixedOwnedColumn(allocator, cached.*);
            }
        }

        var out = (try self.readFixedDictionaryOwnedDirect(allocator, pages.column, pages.row_count, &pages.scratch)) orelse return null;
        errdefer out.deinit(allocator);
        if (self.cache_identity_dictionaries) self.rememberIdentityDictionary(row_group_index, column_index, out);
        return out;
    }

    fn readPlainFixedColumnDirect(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !?types.OwnedColumn {
        if (native_endian != .little) return null;
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        if (!plainFixedDirectCandidate(row_group.columns[column_index], self.metadata.schema.columns[column_index])) return null;

        var pages = try self.columnPageIterator(allocator, row_group_index, column_index);
        defer pages.deinit();

        return switch (pages.column.physical_type) {
            .int32 => blk: {
                const values = readPlainFixedColumnDirectValues(i32, allocator, &pages) catch |err| switch (err) {
                    error.UnsupportedFastPath => return null,
                    else => |e| return e,
                };
                break :blk .{ .int32 = .{ .values = values } };
            },
            .int64 => blk: {
                const values = readPlainFixedColumnDirectValues(i64, allocator, &pages) catch |err| switch (err) {
                    error.UnsupportedFastPath => return null,
                    else => |e| return e,
                };
                break :blk .{ .int64 = .{ .values = values } };
            },
            .float => blk: {
                const values = readPlainFixedColumnDirectValues(f32, allocator, &pages) catch |err| switch (err) {
                    error.UnsupportedFastPath => return null,
                    else => |e| return e,
                };
                break :blk .{ .float = .{ .values = values } };
            },
            .double => blk: {
                const values = readPlainFixedColumnDirectValues(f64, allocator, &pages) catch |err| switch (err) {
                    error.UnsupportedFastPath => return null,
                    else => |e| return e,
                };
                break :blk .{ .double = .{ .values = values } };
            },
            else => null,
        };
    }

    fn readFixedDictionaryOwnedDirect(self: *StreamFileReader, allocator: std.mem.Allocator, column: types.ColumnChunkMeta, expected_count: ?usize, scratch: *PageDecodeScratch) !?types.OwnedColumn {
        if (native_endian != .little) return null;
        const dict_offset = column.dictionary_page_offset orelse return null;
        try self.file_reader.seekTo(try checkedFileOffset(dict_offset));
        const header = try thrift.readPageHeader(&self.file_reader.interface);
        if (header.page_type != .dictionary_page) return error.CorruptPage;
        const dict_header = header.dictionary_page_header orelse return error.CorruptPage;
        if (!plainDictionaryPageEncoding(dict_header.encoding)) return null;

        const count = std.math.cast(usize, dict_header.num_values) orelse return error.CorruptPage;
        if (expected_count) |expected| {
            if (count != expected) return null;
        }
        const value_width: usize = switch (column.physical_type) {
            .int32, .float => 4,
            .int64, .double => 8,
            else => return null,
        };
        const value_bytes_len = std.math.mul(usize, value_width, count) catch return error.CorruptPage;
        if (try checkedPageSize(header.uncompressed_page_size) != value_bytes_len) return null;

        const page_size = try checkedPageSize(header.compressed_page_size);
        const page_data = try scratch.pageData(allocator, page_size);
        try self.file_reader.interface.readSliceAll(page_data);
        try validatePageCrc(header, page_data);

        return switch (column.physical_type) {
            .int32 => blk: {
                const values = try allocator.alloc(i32, count);
                errdefer allocator.free(values);
                try preparePageBytesInto(allocator, column.codec, header, page_data, std.mem.sliceAsBytes(values), scratch);
                break :blk .{ .int32 = .{ .values = values } };
            },
            .int64 => blk: {
                const values = try allocator.alloc(i64, count);
                errdefer allocator.free(values);
                try preparePageBytesInto(allocator, column.codec, header, page_data, std.mem.sliceAsBytes(values), scratch);
                break :blk .{ .int64 = .{ .values = values } };
            },
            .float => blk: {
                const values = try allocator.alloc(f32, count);
                errdefer allocator.free(values);
                try preparePageBytesInto(allocator, column.codec, header, page_data, std.mem.sliceAsBytes(values), scratch);
                break :blk .{ .float = .{ .values = values } };
            },
            .double => blk: {
                const values = try allocator.alloc(f64, count);
                errdefer allocator.free(values);
                try preparePageBytesInto(allocator, column.codec, header, page_data, std.mem.sliceAsBytes(values), scratch);
                break :blk .{ .double = .{ .values = values } };
            },
            else => null,
        };
    }

    fn cachedIdentityPage(
        self: *const StreamFileReader,
        codec: types.CompressionCodec,
        uncompressed_size: usize,
        row_count: usize,
        row_start: usize,
        optional: bool,
        compressed: []const u8,
    ) bool {
        for (self.identity_page_cache.items) |entry| {
            if (entry.codec == codec and
                entry.uncompressed_size == uncompressed_size and
                entry.row_count == row_count and
                entry.row_start == row_start and
                entry.optional == optional and
                std.mem.eql(u8, entry.compressed, compressed))
            {
                return true;
            }
        }
        return false;
    }

    fn rememberIdentityPage(
        self: *StreamFileReader,
        codec: types.CompressionCodec,
        uncompressed_size: usize,
        row_count: usize,
        row_start: usize,
        optional: bool,
        compressed: []const u8,
    ) void {
        if (compressed.len == 0 or compressed.len > max_identity_page_cache_entry_size) return;
        if (self.cachedIdentityPage(codec, uncompressed_size, row_count, row_start, optional, compressed)) return;

        while (self.identity_page_cache.items.len >= max_identity_page_cache_entries or self.identity_page_cache_bytes + compressed.len > max_identity_page_cache_bytes) {
            if (self.identity_page_cache.items.len == 0) return;
            const evicted = self.identity_page_cache.orderedRemove(0);
            self.identity_page_cache_bytes -= evicted.compressed.len;
            self.allocator.free(evicted.compressed);
        }

        const copy = self.allocator.dupe(u8, compressed) catch return;
        self.identity_page_cache.append(self.allocator, .{
            .codec = codec,
            .uncompressed_size = uncompressed_size,
            .row_count = row_count,
            .row_start = row_start,
            .optional = optional,
            .compressed = copy,
        }) catch {
            self.allocator.free(copy);
            return;
        };
        self.identity_page_cache_bytes += copy.len;
    }

    fn cachedIdentityDictionary(self: *const StreamFileReader, row_group_index: usize, column_index: usize) ?*const types.OwnedColumn {
        for (self.identity_dictionary_cache.items) |*entry| {
            if (entry.row_group_index == row_group_index and entry.column_index == column_index) return &entry.value;
        }
        return null;
    }

    fn rememberIdentityDictionary(self: *StreamFileReader, row_group_index: usize, column_index: usize, value: types.OwnedColumn) void {
        if (self.cachedIdentityDictionary(row_group_index, column_index) != null) return;
        const byte_size = fixedOwnedColumnByteSize(value) orelse return;
        if (byte_size == 0 or byte_size > max_identity_dictionary_cache_entry_size) return;

        while (self.identity_dictionary_cache.items.len >= max_identity_dictionary_cache_entries or self.identity_dictionary_cache_bytes + byte_size > max_identity_dictionary_cache_bytes) {
            if (self.identity_dictionary_cache.items.len == 0) return;
            var evicted = self.identity_dictionary_cache.orderedRemove(0);
            self.identity_dictionary_cache_bytes -= evicted.byte_size;
            evicted.value.deinit(self.allocator);
        }

        var copy = cloneFixedOwnedColumn(self.allocator, value) catch return;
        self.identity_dictionary_cache.append(self.allocator, .{
            .row_group_index = row_group_index,
            .column_index = column_index,
            .byte_size = byte_size,
            .value = copy,
        }) catch {
            copy.deinit(self.allocator);
            return;
        };
        self.identity_dictionary_cache_bytes += byte_size;
    }

    fn readDictionaryOwned(self: *StreamFileReader, allocator: std.mem.Allocator, column: types.ColumnChunkMeta, schema_col: types.Column, scratch: *PageDecodeScratch) !?types.OwnedColumn {
        if (try self.readFixedDictionaryOwnedDirect(allocator, column, null, scratch)) |dictionary| return dictionary;

        const dict_offset = column.dictionary_page_offset orelse return null;
        try self.file_reader.seekTo(try checkedFileOffset(dict_offset));
        const header = try thrift.readPageHeader(&self.file_reader.interface);
        if (header.page_type != .dictionary_page) return error.CorruptPage;
        const dict_header = header.dictionary_page_header orelse return error.CorruptPage;
        if (!plainDictionaryPageEncoding(dict_header.encoding)) return error.UnsupportedEncoding;
        const page_size = try checkedPageSize(header.compressed_page_size);
        const page_data = try scratch.pageData(allocator, page_size);
        try self.file_reader.interface.readSliceAll(page_data);
        try validatePageCrc(header, page_data);
        return try decodeDictionaryPageData(allocator, column, schema_col, header, page_data, scratch);
    }

    fn cachedDictionary(self: *StreamFileReader, row_group_index: usize, column_index: usize) !?*const types.OwnedColumn {
        for (self.dictionary_cache.items) |*entry| {
            if (entry.row_group_index == row_group_index and entry.column_index == column_index) return &entry.value;
        }

        const row_group = self.metadata.row_groups[row_group_index];
        const column = row_group.columns[column_index];
        const schema_col = self.metadata.schema.columns[column_index];
        var scratch: PageDecodeScratch = .{};
        defer scratch.deinit(self.allocator);
        var dictionary = (try self.readDictionaryOwned(self.allocator, column, schema_col, &scratch)) orelse return null;
        errdefer dictionary.deinit(self.allocator);

        try self.dictionary_cache.append(self.allocator, .{
            .row_group_index = row_group_index,
            .column_index = column_index,
            .value = dictionary,
        });
        return &self.dictionary_cache.items[self.dictionary_cache.items.len - 1].value;
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

    pub fn readRowGroupSelectedColumns(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_indexes: []const usize) ![]types.OwnedColumn {
        try self.validateColumnSelection(row_group_index, column_indexes);
        const columns = try allocator.alloc(types.OwnedColumn, column_indexes.len);
        errdefer allocator.free(columns);
        var initialized: usize = 0;
        errdefer {
            for (columns[0..initialized]) |*column| column.deinit(allocator);
        }
        while (initialized < column_indexes.len) : (initialized += 1) {
            columns[initialized] = try self.readColumn(allocator, row_group_index, column_indexes[initialized]);
        }
        return columns;
    }

    pub fn readRowGroupSelectedColumnsByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_paths: []const []const u8) ![]types.OwnedColumn {
        const column_indexes = try self.columnIndexesByPath(allocator, column_paths);
        defer allocator.free(column_indexes);
        return self.readRowGroupSelectedColumns(allocator, row_group_index, column_indexes);
    }

    pub fn readRowGroupColumnsParallel(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, options: ParallelColumnReadOptions) ![]types.OwnedColumn {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const count = self.metadata.schema.columns.len;
        const column_indexes = try allocator.alloc(usize, count);
        defer allocator.free(column_indexes);
        for (column_indexes, 0..) |*column_index, idx| column_index.* = idx;
        return self.readRowGroupSelectedColumnsParallel(allocator, row_group_index, column_indexes, options);
    }

    pub fn readRowGroupSelectedColumnsParallel(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_indexes: []const usize, options: ParallelColumnReadOptions) ![]types.OwnedColumn {
        try self.validateColumnSelection(row_group_index, column_indexes);
        if (column_indexes.len == 0) return try allocator.alloc(types.OwnedColumn, 0);
        if (options.reader_buffer_len == 0) return error.InvalidColumnData;

        const max_threads = resolveParallelThreadCount(column_indexes.len, options.max_threads);
        if (max_threads == 1) {
            return self.readRowGroupSelectedColumns(allocator, row_group_index, column_indexes);
        }

        switch (self.file_reader.mode) {
            .positional, .positional_simple => {},
            else => return error.UnsupportedParallelRead,
        }

        return try readSelectedColumnsParallelFromFile(
            allocator,
            self.file_reader.io,
            self.file_reader.file,
            self.file_size,
            self.metadata,
            row_group_index,
            column_indexes,
            max_threads,
            options,
        );
    }

    pub fn readRowGroupSelectedColumnsParallelByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_paths: []const []const u8, options: ParallelColumnReadOptions) ![]types.OwnedColumn {
        const column_indexes = try self.columnIndexesByPath(allocator, column_paths);
        defer allocator.free(column_indexes);
        return self.readRowGroupSelectedColumnsParallel(allocator, row_group_index, column_indexes, options);
    }

    pub fn readRowGroupsColumnsParallel(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_indexes: []const usize, options: ParallelColumnReadOptions) ![]RowGroupColumns {
        const count = self.metadata.schema.columns.len;
        const column_indexes = try allocator.alloc(usize, count);
        defer allocator.free(column_indexes);
        for (column_indexes, 0..) |*column_index, idx| column_index.* = idx;
        return self.readRowGroupsSelectedColumnsParallel(allocator, row_group_indexes, column_indexes, options);
    }

    pub fn readRowGroupsSelectedColumnsParallel(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_indexes: []const usize, column_indexes: []const usize, options: ParallelColumnReadOptions) ![]RowGroupColumns {
        if (options.reader_buffer_len == 0) return error.InvalidColumnData;
        if (row_group_indexes.len == 0) return try allocator.alloc(RowGroupColumns, 0);
        for (row_group_indexes) |row_group_index| {
            try self.validateColumnSelection(row_group_index, column_indexes);
        }

        if (column_indexes.len == 0) {
            const batches = try allocator.alloc(RowGroupColumns, row_group_indexes.len);
            errdefer allocator.free(batches);
            for (batches, row_group_indexes) |*batch, row_group_index| {
                batch.* = .{
                    .row_group_index = row_group_index,
                    .columns = try allocator.alloc(types.OwnedColumn, 0),
                };
            }
            return batches;
        }

        const total_tasks = std.math.mul(usize, row_group_indexes.len, column_indexes.len) catch return error.InvalidColumnData;
        const max_threads = resolveParallelThreadCount(total_tasks, options.max_threads);
        if (max_threads == 1) {
            const batches = try allocator.alloc(RowGroupColumns, row_group_indexes.len);
            errdefer allocator.free(batches);
            var initialized: usize = 0;
            errdefer {
                for (batches[0..initialized]) |*batch| batch.deinit(allocator);
            }
            for (batches, row_group_indexes) |*batch, row_group_index| {
                batch.* = .{
                    .row_group_index = row_group_index,
                    .columns = try self.readRowGroupSelectedColumns(allocator, row_group_index, column_indexes),
                };
                initialized += 1;
            }
            return batches;
        }

        switch (self.file_reader.mode) {
            .positional, .positional_simple => {},
            else => return error.UnsupportedParallelRead,
        }

        return try readSelectedRowGroupColumnsParallelFromFile(
            allocator,
            self.file_reader.io,
            self.file_reader.file,
            self.file_size,
            self.metadata,
            row_group_indexes,
            column_indexes,
            max_threads,
            options,
        );
    }

    pub fn readRowGroupsSelectedColumnsParallelByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_indexes: []const usize, column_paths: []const []const u8, options: ParallelColumnReadOptions) ![]RowGroupColumns {
        const column_indexes = try self.columnIndexesByPath(allocator, column_paths);
        defer allocator.free(column_indexes);
        return self.readRowGroupsSelectedColumnsParallel(allocator, row_group_indexes, column_indexes, options);
    }

    fn validateColumnSelection(self: *const StreamFileReader, row_group_index: usize, column_indexes: []const usize) !void {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        for (column_indexes) |column_index| {
            if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
            try validateReadableColumnSchema(self.metadata.schema.columns[column_index]);
        }
    }

    fn columnIndexesByPath(self: *const StreamFileReader, allocator: std.mem.Allocator, column_paths: []const []const u8) ![]usize {
        const column_indexes = try allocator.alloc(usize, column_paths.len);
        errdefer allocator.free(column_indexes);
        for (column_paths, 0..) |path, index| {
            column_indexes[index] = self.columnIndexByPath(path) orelse return error.InvalidColumnData;
        }
        return column_indexes;
    }

    pub fn columnPageIterator(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !ColumnPageIterator {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        try validateReadableColumnSchema(self.metadata.schema.columns[column_index]);

        var iter: ColumnPageIterator = .{
            .reader = self,
            .allocator = allocator,
            .column = row_group.columns[column_index],
            .schema_col = self.metadata.schema.columns[column_index],
            .row_group_index = row_group_index,
            .column_index = column_index,
            .row_count = std.math.cast(usize, row_group.num_rows) orelse return error.CorruptMetadata,
        };
        errdefer iter.deinit();

        try self.file_reader.seekTo(try checkedFileOffset(iter.column.data_page_offset));
        return iter;
    }

    pub fn columnPageIteratorByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, dotted_path: []const u8) !ColumnPageIterator {
        const column_index = self.columnIndexByPath(dotted_path) orelse return error.InvalidColumnData;
        return self.columnPageIterator(allocator, row_group_index, column_index);
    }

    fn columnPageIteratorForTriplets(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !ColumnPageIterator {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;

        var iter: ColumnPageIterator = .{
            .reader = self,
            .allocator = allocator,
            .column = row_group.columns[column_index],
            .schema_col = self.metadata.schema.columns[column_index],
            .row_group_index = row_group_index,
            .column_index = column_index,
            .row_count = std.math.cast(usize, row_group.num_rows) orelse return error.CorruptMetadata,
        };
        errdefer iter.deinit();

        try self.file_reader.seekTo(try checkedFileOffset(iter.column.data_page_offset));
        return iter;
    }

    pub fn columnPageInfoIterator(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, column_index: usize) !ColumnPageInfoIterator {
        if (row_group_index >= self.metadata.row_groups.len) return error.InvalidColumnData;
        const row_group = self.metadata.row_groups[row_group_index];
        if (column_index >= row_group.columns.len or column_index >= self.metadata.schema.columns.len) return error.InvalidColumnData;
        try validateReadableColumnSchema(self.metadata.schema.columns[column_index]);

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

    pub fn columnPageInfoIteratorByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, dotted_path: []const u8) !ColumnPageInfoIterator {
        const column_index = self.columnIndexByPath(dotted_path) orelse return error.InvalidColumnData;
        return self.columnPageInfoIterator(allocator, row_group_index, column_index);
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

    pub fn readColumnPageIndexByPath(self: *StreamFileReader, allocator: std.mem.Allocator, row_group_index: usize, dotted_path: []const u8) !?[]types.PageIndexEntry {
        const column_index = self.columnIndexByPath(dotted_path) orelse return error.InvalidColumnData;
        return self.readColumnPageIndex(allocator, row_group_index, column_index);
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

const ParallelColumnReadContext = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    file: std.Io.File,
    file_size: u64,
    metadata: types.FileMetaData,
    row_group_index: usize,
    column_index: usize,
    reader_buffer_len: usize,
    cache_dictionaries: bool,
    result: ?types.OwnedColumn = null,
    err: ?anyerror = null,
};

fn resolveParallelThreadCount(column_count: usize, requested_threads: usize) usize {
    const requested = if (requested_threads == 0) column_count else requested_threads;
    return @max(@as(usize, 1), @min(requested, column_count));
}

fn readSelectedColumnsParallelFromFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    file: std.Io.File,
    file_size: u64,
    metadata: types.FileMetaData,
    row_group_index: usize,
    column_indexes: []const usize,
    max_threads: usize,
    options: ParallelColumnReadOptions,
) ![]types.OwnedColumn {
    const columns = try allocator.alloc(types.OwnedColumn, column_indexes.len);
    errdefer allocator.free(columns);

    var initialized: usize = 0;
    errdefer {
        for (columns[0..initialized]) |*column| column.deinit(allocator);
    }

    const threads = try allocator.alloc(std.Thread, max_threads);
    defer allocator.free(threads);
    const contexts = try allocator.alloc(ParallelColumnReadContext, max_threads);
    defer allocator.free(contexts);

    var next_column: usize = 0;
    while (next_column < column_indexes.len) {
        const batch_len = @min(max_threads, column_indexes.len - next_column);
        var spawned: usize = 0;
        var first_err: ?anyerror = null;
        while (spawned < batch_len) : (spawned += 1) {
            contexts[spawned] = .{
                .allocator = allocator,
                .io = io,
                .file = file,
                .file_size = file_size,
                .metadata = metadata,
                .row_group_index = row_group_index,
                .column_index = column_indexes[next_column + spawned],
                .reader_buffer_len = options.reader_buffer_len,
                .cache_dictionaries = options.cache_dictionaries,
            };
            threads[spawned] = std.Thread.spawn(.{}, parallelReadColumnWorker, .{&contexts[spawned]}) catch |err| {
                first_err = err;
                break;
            };
        }

        for (threads[0..spawned]) |thread| thread.join();

        for (contexts[0..spawned], 0..) |*context, batch_index| {
            if (context.err) |err| {
                if (first_err == null) first_err = err;
            }
            if (context.result != null) {
                var column = context.result.?;
                if (first_err == null and context.err == null) {
                    columns[next_column + batch_index] = column;
                    initialized += 1;
                } else {
                    column.deinit(allocator);
                }
            } else if (context.err == null and first_err == null) {
                first_err = error.InvalidColumnData;
            }
        }

        if (first_err) |err| return err;
        next_column += batch_len;
    }

    return columns;
}

fn readSelectedRowGroupColumnsParallelFromFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    file: std.Io.File,
    file_size: u64,
    metadata: types.FileMetaData,
    row_group_indexes: []const usize,
    column_indexes: []const usize,
    max_threads: usize,
    options: ParallelColumnReadOptions,
) ![]RowGroupColumns {
    const batches = try allocator.alloc(RowGroupColumns, row_group_indexes.len);
    errdefer allocator.free(batches);

    for (batches, row_group_indexes) |*batch, row_group_index| {
        batch.* = .{
            .row_group_index = row_group_index,
            .columns = try allocator.alloc(types.OwnedColumn, column_indexes.len),
        };
    }
    errdefer {
        for (batches) |*batch| allocator.free(batch.columns);
    }

    const total_tasks = std.math.mul(usize, row_group_indexes.len, column_indexes.len) catch return error.InvalidColumnData;
    const initialized = try allocator.alloc(bool, total_tasks);
    defer allocator.free(initialized);
    @memset(initialized, false);
    errdefer {
        for (initialized, 0..) |done, task_index| {
            if (!done) continue;
            const row_group_slot = task_index / column_indexes.len;
            const column_slot = task_index % column_indexes.len;
            batches[row_group_slot].columns[column_slot].deinit(allocator);
        }
    }

    const threads = try allocator.alloc(std.Thread, max_threads);
    defer allocator.free(threads);
    const contexts = try allocator.alloc(ParallelColumnReadContext, max_threads);
    defer allocator.free(contexts);

    var next_task: usize = 0;
    while (next_task < total_tasks) {
        const batch_len = @min(max_threads, total_tasks - next_task);
        var spawned: usize = 0;
        var first_err: ?anyerror = null;
        while (spawned < batch_len) : (spawned += 1) {
            const task_index = next_task + spawned;
            const row_group_slot = task_index / column_indexes.len;
            const column_slot = task_index % column_indexes.len;
            contexts[spawned] = .{
                .allocator = allocator,
                .io = io,
                .file = file,
                .file_size = file_size,
                .metadata = metadata,
                .row_group_index = row_group_indexes[row_group_slot],
                .column_index = column_indexes[column_slot],
                .reader_buffer_len = options.reader_buffer_len,
                .cache_dictionaries = options.cache_dictionaries,
            };
            threads[spawned] = std.Thread.spawn(.{}, parallelReadColumnWorker, .{&contexts[spawned]}) catch |err| {
                first_err = err;
                break;
            };
        }

        for (threads[0..spawned]) |thread| thread.join();

        for (contexts[0..spawned], 0..) |*context, batch_index| {
            const task_index = next_task + batch_index;
            const row_group_slot = task_index / column_indexes.len;
            const column_slot = task_index % column_indexes.len;
            if (context.err) |err| {
                if (first_err == null) first_err = err;
            }
            if (context.result != null) {
                var column = context.result.?;
                if (first_err == null and context.err == null) {
                    batches[row_group_slot].columns[column_slot] = column;
                    initialized[task_index] = true;
                } else {
                    column.deinit(allocator);
                }
            } else if (context.err == null and first_err == null) {
                first_err = error.InvalidColumnData;
            }
        }

        if (first_err) |err| return err;
        next_task += batch_len;
    }

    return batches;
}

fn parallelReadColumnWorker(context: *ParallelColumnReadContext) void {
    const reader_buffer = context.allocator.alloc(u8, context.reader_buffer_len) catch |err| {
        context.err = err;
        return;
    };
    defer context.allocator.free(reader_buffer);

    var file_reader = context.file.reader(context.io, reader_buffer);
    var parsed: StreamFileReader = .{
        .allocator = context.allocator,
        .file_reader = &file_reader,
        .file_size = context.file_size,
        .arena = std.heap.ArenaAllocator.init(context.allocator),
        .metadata = context.metadata,
    };
    defer parsed.deinit();
    parsed.setDictionaryCacheEnabled(context.cache_dictionaries);
    parsed.setIdentityFastPathCacheEnabled(false);

    context.result = parsed.readColumn(context.allocator, context.row_group_index, context.column_index) catch |err| {
        context.err = err;
        return;
    };
}

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

const DictionaryCacheEntry = struct {
    row_group_index: usize,
    column_index: usize,
    value: types.OwnedColumn,
};

const IdentityPageCacheEntry = struct {
    codec: types.CompressionCodec,
    uncompressed_size: usize,
    row_count: usize,
    row_start: usize,
    optional: bool,
    compressed: []u8,
};

const IdentityDictionaryCacheEntry = struct {
    row_group_index: usize,
    column_index: usize,
    byte_size: usize,
    value: types.OwnedColumn,
};

fn readPlainFixedColumnDirectValues(comptime T: type, allocator: std.mem.Allocator, pages: *ColumnPageIterator) ![]T {
    const values = try allocator.alloc(T, pages.row_count);
    errdefer allocator.free(values);

    while (pages.rows_seen < pages.row_count) {
        const page_header = try thrift.readPageHeader(&pages.reader.file_reader.interface);
        const page_info = try directPlainFixedPageInfo(page_header);
        if (pages.rows_seen + page_info.row_count > pages.row_count) return error.CorruptPage;

        const expected_bytes = std.math.mul(usize, page_info.row_count, @sizeOf(T)) catch return error.CorruptPage;
        if (page_info.uncompressed_value_size != expected_bytes) return error.UnsupportedFastPath;
        const out = std.mem.sliceAsBytes(values[pages.rows_seen..][0..page_info.row_count]);
        if (pages.column.codec == .uncompressed and page_header.crc == null and page_info.value_start == 0 and page_info.compressed_value_size == expected_bytes) {
            try pages.reader.file_reader.interface.readSliceAll(out);
            pages.rows_seen += page_info.row_count;
            continue;
        }

        const page_size = try checkedPageSize(page_header.compressed_page_size);
        const page_data = try pages.scratch.pageData(allocator, page_size);
        try pages.reader.file_reader.interface.readSliceAll(page_data);
        try validatePageCrc(page_header, page_data);

        try prepareValueBytesInto(
            allocator,
            pages.column.codec,
            page_info.is_compressed,
            expected_bytes,
            page_info.compressed_value_size,
            page_data[page_info.value_start..],
            out,
            &pages.scratch,
        );
        pages.rows_seen += page_info.row_count;
    }

    return values;
}

const DirectPlainFixedPageInfo = struct {
    row_count: usize,
    value_start: usize,
    compressed_value_size: usize,
    uncompressed_value_size: usize,
    is_compressed: bool,
};

fn directPlainFixedPageInfo(page_header: types.PageHeader) !DirectPlainFixedPageInfo {
    switch (page_header.page_type) {
        .data_page => {
            const data_header = page_header.data_page_header orelse return error.CorruptPage;
            if (data_header.encoding != .plain) return error.UnsupportedFastPath;
            return .{
                .row_count = std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage,
                .value_start = 0,
                .compressed_value_size = try checkedPageSize(page_header.compressed_page_size),
                .uncompressed_value_size = try checkedPageSize(page_header.uncompressed_page_size),
                .is_compressed = true,
            };
        },
        .data_page_v2 => {
            const data_header = page_header.data_page_header_v2 orelse return error.CorruptPage;
            if (data_header.encoding != .plain) return error.UnsupportedFastPath;
            if (data_header.repetition_levels_byte_length != 0 or data_header.definition_levels_byte_length != 0 or data_header.num_nulls != 0) return error.UnsupportedFastPath;
            const row_count = std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage;
            const num_rows = std.math.cast(usize, data_header.num_rows) orelse return error.CorruptPage;
            if (row_count != num_rows) return error.UnsupportedFastPath;
            return .{
                .row_count = row_count,
                .value_start = 0,
                .compressed_value_size = try checkedPageSize(page_header.compressed_page_size),
                .uncompressed_value_size = try checkedPageSize(page_header.uncompressed_page_size),
                .is_compressed = data_header.is_compressed,
            };
        },
        else => return error.UnsupportedFastPath,
    }
}

fn ownedColumnHasValueCount(column: types.OwnedColumn, physical_type: types.Type, count: usize) bool {
    return switch (physical_type) {
        .int32 => switch (column) {
            .int32 => |values| values.values.len == count and values.validity == null,
            else => false,
        },
        .int64 => switch (column) {
            .int64 => |values| values.values.len == count and values.validity == null,
            else => false,
        },
        .float => switch (column) {
            .float => |values| values.values.len == count and values.validity == null,
            else => false,
        },
        .double => switch (column) {
            .double => |values| values.values.len == count and values.validity == null,
            else => false,
        },
        else => false,
    };
}

fn attachAllValidValidity(allocator: std.mem.Allocator, column: *types.OwnedColumn, physical_type: types.Type, count: usize) !void {
    const validity = try allocator.alloc(bool, count);
    errdefer allocator.free(validity);
    @memset(validity, true);

    switch (physical_type) {
        .int32 => switch (column.*) {
            .int32 => |*values| values.validity = validity,
            else => return error.CorruptPage,
        },
        .int64 => switch (column.*) {
            .int64 => |*values| values.validity = validity,
            else => return error.CorruptPage,
        },
        .float => switch (column.*) {
            .float => |*values| values.validity = validity,
            else => return error.CorruptPage,
        },
        .double => switch (column.*) {
            .double => |*values| values.validity = validity,
            else => return error.CorruptPage,
        },
        else => return error.CorruptPage,
    }
}

fn fixedOwnedColumnByteSize(column: types.OwnedColumn) ?usize {
    return switch (column) {
        .int32 => |values| fixedValueByteSize(i32, values.values.len, values.validity),
        .int64 => |values| fixedValueByteSize(i64, values.values.len, values.validity),
        .float => |values| fixedValueByteSize(f32, values.values.len, values.validity),
        .double => |values| fixedValueByteSize(f64, values.values.len, values.validity),
        else => null,
    };
}

fn fixedValueByteSize(comptime T: type, value_count: usize, validity: ?[]const bool) usize {
    return value_count * @sizeOf(T) + if (validity) |bits| bits.len * @sizeOf(bool) else 0;
}

fn cloneFixedOwnedColumn(allocator: std.mem.Allocator, column: types.OwnedColumn) !types.OwnedColumn {
    return switch (column) {
        .int32 => |values| blk: {
            const cloned_values = try allocator.dupe(i32, values.values);
            errdefer allocator.free(cloned_values);
            break :blk .{ .int32 = .{
                .values = cloned_values,
                .validity = try cloneValidity(allocator, values.validity),
            } };
        },
        .int64 => |values| blk: {
            const cloned_values = try allocator.dupe(i64, values.values);
            errdefer allocator.free(cloned_values);
            break :blk .{ .int64 = .{
                .values = cloned_values,
                .validity = try cloneValidity(allocator, values.validity),
            } };
        },
        .float => |values| blk: {
            const cloned_values = try allocator.dupe(f32, values.values);
            errdefer allocator.free(cloned_values);
            break :blk .{ .float = .{
                .values = cloned_values,
                .validity = try cloneValidity(allocator, values.validity),
            } };
        },
        .double => |values| blk: {
            const cloned_values = try allocator.dupe(f64, values.values);
            errdefer allocator.free(cloned_values);
            break :blk .{ .double = .{
                .values = cloned_values,
                .validity = try cloneValidity(allocator, values.validity),
            } };
        },
        else => error.CorruptPage,
    };
}

fn cloneValidity(allocator: std.mem.Allocator, validity: ?[]const bool) !?[]bool {
    const values = validity orelse return null;
    return try allocator.dupe(bool, values);
}

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
    row_group_index: usize,
    column_index: usize,
    row_count: usize,
    rows_seen: usize = 0,
    dictionary: ?types.OwnedColumn = null,
    dictionary_ref: ?*const types.OwnedColumn = null,
    scratch: PageDecodeScratch = .{},

    pub fn deinit(self: *ColumnPageIterator) void {
        if (self.dictionary) |*dict| dict.deinit(self.allocator);
        self.scratch.deinit(self.allocator);
    }

    pub fn next(self: *ColumnPageIterator) !?types.OwnedColumn {
        if (self.rows_seen >= self.row_count) return null;

        const page = try self.readNextDataPage();
        const page_header = page.header;
        const page_rows = try pageRowCount(page_header);
        if (self.rows_seen + page_rows > self.row_count) return error.CorruptPage;
        const dict = if (pageUsesDictionary(page_header, self.column, self.dictionary_ref != null))
            try self.ensureDictionary(self.reader.file_reader.logicalPos())
        else
            null;

        const page_column = try decodeColumnPage(
            self.allocator,
            self.column,
            self.schema_col,
            page_header,
            page.data,
            dict,
            &self.scratch,
        );
        self.rows_seen += page_rows;
        return page_column;
    }

    fn nextInto(self: *ColumnPageIterator, acc: *ColumnAccumulator) !bool {
        if (self.rows_seen >= self.row_count) return false;

        const page = try self.readNextDataPage();
        const page_header = page.header;
        const page_rows = try pageRowCount(page_header);
        if (self.rows_seen + page_rows > self.row_count) return error.CorruptPage;
        const dict = if (pageUsesDictionary(page_header, self.column, self.dictionary_ref != null))
            try self.ensureDictionary(self.reader.file_reader.logicalPos())
        else
            null;

        const appended = try decodeColumnPageIntoAccumulator(
            self.allocator,
            self.column,
            self.schema_col,
            page_header,
            page.data,
            dict,
            &self.scratch,
            self.rows_seen,
            acc,
        );
        if (!appended) {
            var page_column = try decodeColumnPage(
                self.allocator,
                self.column,
                self.schema_col,
                page_header,
                page.data,
                dict,
                &self.scratch,
            );
            defer page_column.deinit(self.allocator);
            try acc.append(self.allocator, page_column);
        }
        self.rows_seen += page_rows;
        return true;
    }

    fn readNextDataPage(self: *ColumnPageIterator) !ReadPage {
        while (true) {
            const page_header = try thrift.readPageHeader(&self.reader.file_reader.interface);
            const page_size = try checkedPageSize(page_header.compressed_page_size);
            const data_offset = self.reader.file_reader.logicalPos();
            if (page_header.page_type != .dictionary_page and
                pageUsesDictionary(page_header, self.column, self.dictionary_ref != null) and
                self.dictionary_ref == null and
                self.column.dictionary_page_offset != null)
            {
                _ = try self.ensureDictionary(data_offset);
            }
            const page_data = try self.scratch.pageData(self.allocator, page_size);
            try self.reader.file_reader.interface.readSliceAll(page_data);
            try validatePageCrc(page_header, page_data);

            if (page_header.page_type == .dictionary_page) {
                if (self.rows_seen != 0 or self.dictionary_ref != null) return error.CorruptPage;
                self.dictionary = try decodeDictionaryPageData(self.allocator, self.column, self.schema_col, page_header, page_data, &self.scratch);
                self.dictionary_ref = if (self.dictionary) |*dict| dict else unreachable;
                continue;
            }

            return .{ .header = page_header, .data = page_data };
        }
    }

    fn ensureDictionary(self: *ColumnPageIterator, resume_offset: u64) !*const types.OwnedColumn {
        if (self.dictionary_ref == null) {
            if (self.reader.cache_dictionaries) {
                self.dictionary_ref = (try self.reader.cachedDictionary(self.row_group_index, self.column_index)) orelse return error.CorruptPage;
            } else {
                self.dictionary = (try self.reader.readDictionaryOwned(self.allocator, self.column, self.schema_col, &self.scratch)) orelse return error.CorruptPage;
                self.dictionary_ref = if (self.dictionary) |*dict| dict else unreachable;
            }
            try self.reader.file_reader.seekTo(resume_offset);
        }
        return self.dictionary_ref orelse unreachable;
    }
};

const ReadPage = struct {
    header: types.PageHeader,
    data: []const u8,
};

fn pageUsesDictionary(page_header: types.PageHeader, column: types.ColumnChunkMeta, dictionary_available: bool) bool {
    return switch (page_header.page_type) {
        .data_page => blk: {
            const data_header = page_header.data_page_header orelse break :blk false;
            break :blk data_header.encoding == .rle_dictionary or
                data_header.encoding == .plain_dictionary or
                (data_header.encoding == .bit_packed and (column.dictionary_page_offset != null or dictionary_available));
        },
        .data_page_v2 => blk: {
            const data_header = page_header.data_page_header_v2 orelse break :blk false;
            break :blk data_header.encoding == .rle_dictionary or data_header.encoding == .plain_dictionary;
        },
        else => false,
    };
}

fn decodeDictionaryPageData(
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    schema_col: types.Column,
    header: types.PageHeader,
    page_data: []const u8,
    scratch: *PageDecodeScratch,
) !types.OwnedColumn {
    if (header.page_type != .dictionary_page) return error.CorruptPage;
    const dict_header = header.dictionary_page_header orelse return error.CorruptPage;
    if (!plainDictionaryPageEncoding(dict_header.encoding)) return error.UnsupportedEncoding;
    const count = std.math.cast(usize, dict_header.num_values) orelse return error.CorruptPage;
    const page_bytes = try preparePageBytes(allocator, column.codec, header, page_data, scratch);
    defer page_bytes.deinit(allocator);
    return try plain.decodeValues(allocator, schema_col.column_type, count, count, null, page_bytes.data);
}

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
    const legacy_dictionary_bit_packed = data_header.encoding == .bit_packed and dictionary != null;
    const dictionary_encoded = data_header.encoding == .rle_dictionary or data_header.encoding == .plain_dictionary or legacy_dictionary_bit_packed;
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
        const decoded = try plain.decodeDefinitionLevelsForEncodingMax(allocator, data_header.definition_level_encoding, data, row_count, schema_col.max_definition_level);
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
    if (non_null_count == 0) {
        const empty_indexes: [0]u32 = .{};
        return plain.materializeDictionary(allocator, column.physical_type, dict, empty_indexes[0..], validity);
    }
    const bit_width = if (legacy_dictionary_bit_packed) try dictionaryIndexBitWidth(dict.*) else blk: {
        if (data.len == 0) return error.CorruptPage;
        break :blk data[0];
    };
    const indexes = if (legacy_dictionary_bit_packed)
        try plain.decodeBitPackedUint32(allocator, data, bit_width, non_null_count)
    else
        try plain.decodeRleBitPackedUint32(allocator, data[1..], bit_width, non_null_count);
    defer allocator.free(indexes);
    return plain.materializeDictionary(allocator, column.physical_type, dict, indexes, validity);
}

fn decodeColumnPageTriplets(
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    schema_col: types.Column,
    page_header: types.PageHeader,
    page_data: []const u8,
    dictionary: ?*const types.OwnedColumn,
    scratch: *PageDecodeScratch,
) !types.OwnedColumnTriplets {
    if (page_header.page_type == .data_page_v2) {
        return decodeColumnPageV2Triplets(allocator, column, schema_col, page_header, page_data, dictionary, scratch);
    }
    if (page_header.page_type != .data_page) return error.UnsupportedPageType;

    const page_bytes = try preparePageBytes(allocator, column.codec, page_header, page_data, scratch);
    defer page_bytes.deinit(allocator);
    const data_header = page_header.data_page_header orelse return error.CorruptPage;
    const level_count = std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage;

    var data = page_bytes.data;
    const repetition_levels_result = try decodePageLevelsV1(allocator, data_header.repetition_level_encoding, data, level_count, schema_col.max_repetition_level);
    const repetition_levels = repetition_levels_result.levels;
    errdefer allocator.free(repetition_levels);
    data = data[repetition_levels_result.consumed..];

    const definition_levels_result = try decodePageLevelsV1(allocator, data_header.definition_level_encoding, data, level_count, schema_col.max_definition_level);
    const definition_levels = definition_levels_result.levels;
    errdefer allocator.free(definition_levels);
    data = data[definition_levels_result.consumed..];

    const non_null_count = countPresentLevels(definition_levels, schema_col.max_definition_level);
    var values = try decodeColumnValues(allocator, column, schema_col, data_header.encoding, data, dictionary, non_null_count, data_header.encoding == .bit_packed and dictionary != null);
    errdefer values.deinit(allocator);

    return .{
        .values = values,
        .definition_levels = definition_levels,
        .repetition_levels = repetition_levels,
        .max_definition_level = schema_col.max_definition_level,
        .max_repetition_level = schema_col.max_repetition_level,
    };
}

fn decodeColumnPageV2Triplets(
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    schema_col: types.Column,
    page_header: types.PageHeader,
    page_data: []const u8,
    dictionary: ?*const types.OwnedColumn,
    scratch: *PageDecodeScratch,
) !types.OwnedColumnTriplets {
    const data_header = page_header.data_page_header_v2 orelse return error.CorruptPage;
    const level_count = std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage;
    const num_rows = std.math.cast(usize, data_header.num_rows) orelse return error.CorruptPage;
    if (num_rows == 0 and level_count != 0) return error.CorruptPage;

    const def_len = try checkedPageSize(data_header.definition_levels_byte_length);
    const rep_len = try checkedPageSize(data_header.repetition_levels_byte_length);
    const levels_len = std.math.add(usize, rep_len, def_len) catch return error.CorruptPage;
    if (levels_len > page_data.len) return error.CorruptPage;

    const repetition_levels = try plain.decodeLevelsBodyMax(allocator, page_data[0..rep_len], level_count, schema_col.max_repetition_level);
    errdefer allocator.free(repetition_levels);
    const definition_levels = try plain.decodeLevelsBodyMax(allocator, page_data[rep_len..levels_len], level_count, schema_col.max_definition_level);
    errdefer allocator.free(definition_levels);
    if (try rowCountFromRepetitionLevels(repetition_levels, schema_col.max_repetition_level) != num_rows) return error.CorruptPage;

    const non_null_count = countPresentLevels(definition_levels, schema_col.max_definition_level);
    if (schema_col.max_repetition_level == 0) {
        const num_nulls = std.math.cast(usize, data_header.num_nulls) orelse return error.CorruptPage;
        if (num_nulls > level_count or level_count - num_nulls != non_null_count) return error.CorruptPage;
    }

    const values_compressed_size = page_data.len - levels_len;
    const total_uncompressed_size = try checkedPageSize(page_header.uncompressed_page_size);
    if (levels_len > total_uncompressed_size) return error.CorruptPage;
    const values_uncompressed_size = total_uncompressed_size - levels_len;
    if (non_null_count == 0 and values_compressed_size == 0 and values_uncompressed_size == 0) {
        var values = try plain.decodeValues(allocator, schema_col.column_type, 0, 0, null, &.{});
        errdefer values.deinit(allocator);
        return .{
            .values = values,
            .definition_levels = definition_levels,
            .repetition_levels = repetition_levels,
            .max_definition_level = schema_col.max_definition_level,
            .max_repetition_level = schema_col.max_repetition_level,
        };
    }
    const value_bytes = try prepareValueBytes(
        allocator,
        column.codec,
        data_header.is_compressed,
        values_uncompressed_size,
        values_compressed_size,
        page_data[levels_len..],
        scratch,
    );
    defer value_bytes.deinit(allocator);

    var values = try decodeColumnValues(allocator, column, schema_col, data_header.encoding, value_bytes.data, dictionary, non_null_count, false);
    errdefer values.deinit(allocator);

    return .{
        .values = values,
        .definition_levels = definition_levels,
        .repetition_levels = repetition_levels,
        .max_definition_level = schema_col.max_definition_level,
        .max_repetition_level = schema_col.max_repetition_level,
    };
}

fn decodePageLevelsV1(allocator: std.mem.Allocator, encoding: types.Encoding, data: []const u8, level_count: usize, max_level: u16) !plain.LevelValues {
    if (max_level == 0) {
        const levels = try allocator.alloc(u16, level_count);
        @memset(levels, 0);
        return .{ .levels = levels, .consumed = 0 };
    }
    return plain.decodeLevelsForEncodingMax(allocator, encoding, data, level_count, max_level);
}

fn decodeColumnValues(
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    schema_col: types.Column,
    encoding: types.Encoding,
    data: []const u8,
    dictionary: ?*const types.OwnedColumn,
    value_count: usize,
    legacy_dictionary_bit_packed: bool,
) !types.OwnedColumn {
    const dictionary_encoded = encoding == .rle_dictionary or encoding == .plain_dictionary or legacy_dictionary_bit_packed;
    const rle_boolean = encoding == .rle and schema_col.column_type.physical == .boolean;
    if (rle_boolean) return decodeRleBooleans(allocator, value_count, value_count, null, data);
    if (!dictionary_encoded) {
        return switch (encoding) {
            .plain => plain.decodeValues(allocator, schema_col.column_type, value_count, value_count, null, data),
            .byte_stream_split => plain.decodeByteStreamSplitValues(allocator, schema_col.column_type, value_count, value_count, null, data),
            .delta_binary_packed => plain.decodeDeltaBinaryPackedValues(allocator, schema_col.column_type, value_count, value_count, null, data),
            .delta_length_byte_array => plain.decodeDeltaLengthByteArrayValues(allocator, schema_col.column_type, value_count, value_count, null, data),
            .delta_byte_array => plain.decodeDeltaByteArrayValues(allocator, schema_col.column_type, value_count, value_count, null, data),
            else => error.UnsupportedEncoding,
        };
    }

    const dict = dictionary orelse return error.CorruptPage;
    if (value_count == 0) {
        const empty_indexes: [0]u32 = .{};
        return plain.materializeDictionary(allocator, column.physical_type, dict, empty_indexes[0..], null);
    }
    const bit_width = if (legacy_dictionary_bit_packed) try dictionaryIndexBitWidth(dict.*) else blk: {
        if (data.len == 0) return error.CorruptPage;
        break :blk data[0];
    };
    const indexes = if (legacy_dictionary_bit_packed)
        try plain.decodeBitPackedUint32(allocator, data, bit_width, value_count)
    else
        try plain.decodeRleBitPackedUint32(allocator, data[1..], bit_width, value_count);
    defer allocator.free(indexes);
    return plain.materializeDictionary(allocator, column.physical_type, dict, indexes, null);
}

fn countPresentLevels(definition_levels: []const u16, max_definition_level: u16) usize {
    var count: usize = 0;
    for (definition_levels) |level| {
        if (level == max_definition_level) count += 1;
    }
    return count;
}

fn rowCountFromRepetitionLevels(repetition_levels: []const u16, max_repetition_level: u16) !usize {
    if (max_repetition_level == 0) return repetition_levels.len;
    var rows: usize = 0;
    for (repetition_levels, 0..) |level, idx| {
        if (level > max_repetition_level) return error.CorruptPage;
        if (idx == 0 and level != 0) return error.CorruptPage;
        if (level == 0) rows += 1;
    }
    return rows;
}

fn buildTripletRowOffsets(allocator: std.mem.Allocator, repetition_levels: []const u16, max_repetition_level: u16, row_count: usize) ![]usize {
    const offsets = try allocator.alloc(usize, std.math.add(usize, row_count, 1) catch return error.CorruptPage);
    errdefer allocator.free(offsets);
    offsets[0] = 0;

    if (row_count == 0) {
        if (repetition_levels.len != 0) return error.CorruptPage;
        return offsets;
    }
    if (repetition_levels.len == 0) return error.CorruptPage;

    var row_index: usize = 0;
    var row_started = false;
    for (repetition_levels, 0..) |level, level_index| {
        if (level > max_repetition_level) return error.CorruptPage;
        if (level == 0) {
            if (row_started) {
                offsets[row_index + 1] = level_index;
                row_index += 1;
            }
            if (row_index >= row_count) return error.CorruptPage;
            row_started = true;
        } else if (!row_started) {
            return error.CorruptPage;
        }
    }
    if (!row_started) return error.CorruptPage;
    offsets[row_index + 1] = repetition_levels.len;
    row_index += 1;
    if (row_index != row_count) return error.CorruptPage;
    return offsets;
}

fn buildTripletValueOffsets(allocator: std.mem.Allocator, definition_levels: []const u16, row_offsets: []const usize, max_definition_level: u16) ![]usize {
    if (row_offsets.len == 0) return error.CorruptPage;
    if (row_offsets[row_offsets.len - 1] != definition_levels.len) return error.CorruptPage;

    const offsets = try allocator.alloc(usize, row_offsets.len);
    errdefer allocator.free(offsets);
    offsets[0] = 0;

    var value_index: usize = 0;
    var row_index: usize = 0;
    while (row_index + 1 < row_offsets.len) : (row_index += 1) {
        const start = row_offsets[row_index];
        const end = row_offsets[row_index + 1];
        if (start > end or end > definition_levels.len) return error.CorruptPage;
        for (definition_levels[start..end]) |level| {
            if (level > max_definition_level) return error.CorruptPage;
            if (level == max_definition_level) value_index += 1;
        }
        offsets[row_index + 1] = value_index;
    }

    return offsets;
}

fn buildTripletRepeatedLevelOffsets(allocator: std.mem.Allocator, repetition_levels: []const u16, max_repetition_level: u16) ![]types.TripletLevelOffsets {
    if (max_repetition_level == 0) return allocator.alloc(types.TripletLevelOffsets, 0);
    const level_count: usize = max_repetition_level;
    if (repetition_levels.len == 0) {
        const level_offsets = try allocator.alloc(types.TripletLevelOffsets, level_count);
        var initialized_levels: usize = 0;
        errdefer {
            for (level_offsets[0..initialized_levels]) |entry| allocator.free(entry.offsets);
            allocator.free(level_offsets);
        }
        var level: u16 = 1;
        while (level <= max_repetition_level) : (level += 1) {
            const offsets = try allocator.alloc(usize, 1);
            errdefer allocator.free(offsets);
            offsets[0] = 0;
            level_offsets[@as(usize, level - 1)] = .{ .repetition_level = level, .offsets = offsets };
            initialized_levels += 1;
        }
        return level_offsets;
    }
    if (repetition_levels[0] != 0) return error.CorruptPage;

    const level_offsets = try allocator.alloc(types.TripletLevelOffsets, level_count);
    var initialized_levels: usize = 0;
    errdefer {
        for (level_offsets[0..initialized_levels]) |entry| allocator.free(entry.offsets);
        allocator.free(level_offsets);
    }

    var level: u16 = 1;
    while (level <= max_repetition_level) : (level += 1) {
        var boundary_count: usize = 2;
        for (repetition_levels[1..]) |repetition_level| {
            if (repetition_level > max_repetition_level) return error.CorruptPage;
            if (repetition_level <= level) boundary_count += 1;
        }

        const offsets = try allocator.alloc(usize, boundary_count);
        errdefer allocator.free(offsets);
        offsets[0] = 0;
        var offset_index: usize = 1;
        for (repetition_levels[1..], 1..) |repetition_level, level_index| {
            if (repetition_level <= level) {
                offsets[offset_index] = level_index;
                offset_index += 1;
            }
        }
        offsets[offset_index] = repetition_levels.len;
        offset_index += 1;
        if (offset_index != offsets.len) return error.CorruptPage;

        level_offsets[@as(usize, level - 1)] = .{ .repetition_level = level, .offsets = offsets };
        initialized_levels += 1;
    }
    return level_offsets;
}

fn freeTripletLevelOffsets(allocator: std.mem.Allocator, level_offsets: []types.TripletLevelOffsets) void {
    for (level_offsets) |entry| allocator.free(entry.offsets);
    allocator.free(level_offsets);
}

fn tripletPageRowCount(page_header: types.PageHeader, repetition_levels: []const u16, max_repetition_level: u16) !usize {
    const rows_from_levels = try rowCountFromRepetitionLevels(repetition_levels, max_repetition_level);
    if (page_header.page_type == .data_page_v2) {
        const data_header = page_header.data_page_header_v2 orelse return error.CorruptPage;
        const num_rows = std.math.cast(usize, data_header.num_rows) orelse return error.CorruptPage;
        if (rows_from_levels != num_rows) return error.CorruptPage;
    }
    return rows_from_levels;
}

fn decodeColumnPageIntoAccumulator(
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    schema_col: types.Column,
    page_header: types.PageHeader,
    page_data: []const u8,
    dictionary: ?*const types.OwnedColumn,
    scratch: *PageDecodeScratch,
    dictionary_index_start: usize,
    acc: *ColumnAccumulator,
) !bool {
    if (page_header.page_type == .data_page_v2) {
        return try decodeColumnPageV2IntoAccumulator(
            allocator,
            column,
            schema_col,
            page_header,
            page_data,
            dictionary,
            scratch,
            dictionary_index_start,
            acc,
        );
    }
    if (page_header.page_type != .data_page) return false;

    const data_header = page_header.data_page_header orelse return error.CorruptPage;
    const legacy_dictionary_bit_packed = data_header.encoding == .bit_packed and dictionary != null;
    const dictionary_encoded = data_header.encoding == .rle_dictionary or data_header.encoding == .plain_dictionary or legacy_dictionary_bit_packed;
    const plain_fixed = data_header.encoding == .plain and fixedAccumulatorSupported(schema_col.column_type.physical);
    const byte_stream_split_fixed = data_header.encoding == .byte_stream_split and fixedAccumulatorSupported(schema_col.column_type.physical);
    const delta_binary_fixed = data_header.encoding == .delta_binary_packed and deltaBinaryAccumulatorSupported(schema_col.column_type.physical);
    const dictionary_supported = dictionary_encoded and dictionaryAccumulatorSupported(schema_col.column_type.physical);
    if (!plain_fixed and !byte_stream_split_fixed and !delta_binary_fixed and !dictionary_supported) return false;

    const row_count = std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage;
    const page_bytes = try preparePageBytes(allocator, column.codec, page_header, page_data, scratch);
    defer page_bytes.deinit(allocator);

    var data = page_bytes.data;
    var validity: ?[]bool = null;
    defer if (validity) |v| allocator.free(v);
    var non_null_count = row_count;
    if (schema_col.repetition == .optional) {
        const decoded = try plain.decodeDefinitionLevelsForEncodingMax(allocator, data_header.definition_level_encoding, data, row_count, schema_col.max_definition_level);
        validity = decoded.levels;
        data = data[decoded.consumed..];
        non_null_count = 0;
        for (validity.?) |valid| {
            if (valid) non_null_count += 1;
        }
    }

    switch (data_header.encoding) {
        .plain => return try acc.appendPlainFixedValues(allocator, schema_col.column_type, non_null_count, data, validity),
        .byte_stream_split => return try acc.appendByteStreamSplitFixedValues(allocator, schema_col.column_type, non_null_count, data, validity),
        .delta_binary_packed => return try acc.appendDeltaBinaryPackedValues(allocator, schema_col.column_type, non_null_count, data, validity),
        .rle_dictionary, .plain_dictionary, .bit_packed => {
            const dict = dictionary orelse return error.CorruptPage;
            if (non_null_count == 0) {
                const empty_indexes: [0]u32 = .{};
                return try acc.appendDictionaryValues(allocator, column.physical_type, dict, empty_indexes[0..], validity);
            }
            const bit_width = if (legacy_dictionary_bit_packed) try dictionaryIndexBitWidth(dict.*) else blk: {
                if (data.len == 0) return error.CorruptPage;
                break :blk data[0];
            };
            if (!legacy_dictionary_bit_packed and try acc.appendDictionaryEncodedValues(allocator, column.physical_type, dict, data[1..], bit_width, non_null_count, validity, dictionary_index_start)) return true;
            const indexes = if (legacy_dictionary_bit_packed)
                try plain.decodeBitPackedUint32(allocator, data, bit_width, non_null_count)
            else
                try plain.decodeRleBitPackedUint32(allocator, data[1..], bit_width, non_null_count);
            defer allocator.free(indexes);
            return try acc.appendDictionaryValues(allocator, column.physical_type, dict, indexes, validity);
        },
        else => return false,
    }
}

fn decodeColumnPageV2IntoAccumulator(
    allocator: std.mem.Allocator,
    column: types.ColumnChunkMeta,
    schema_col: types.Column,
    page_header: types.PageHeader,
    page_data: []const u8,
    dictionary: ?*const types.OwnedColumn,
    scratch: *PageDecodeScratch,
    dictionary_index_start: usize,
    acc: *ColumnAccumulator,
) !bool {
    const data_header = page_header.data_page_header_v2 orelse return error.CorruptPage;
    const dictionary_encoded = data_header.encoding == .rle_dictionary or data_header.encoding == .plain_dictionary;
    const plain_fixed = data_header.encoding == .plain and fixedAccumulatorSupported(schema_col.column_type.physical);
    const byte_stream_split_fixed = data_header.encoding == .byte_stream_split and fixedAccumulatorSupported(schema_col.column_type.physical);
    const delta_binary_fixed = data_header.encoding == .delta_binary_packed and deltaBinaryAccumulatorSupported(schema_col.column_type.physical);
    const dictionary_supported = dictionary_encoded and dictionaryAccumulatorSupported(schema_col.column_type.physical);
    if (!plain_fixed and !byte_stream_split_fixed and !delta_binary_fixed and !dictionary_supported) return false;

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
    try validateFlatRepetitionLevels(page_data[0..rep_len], row_count, schema_col.max_repetition_level);
    var validity: ?[]bool = null;
    defer if (validity) |v| allocator.free(v);
    if (schema_col.repetition == .optional) {
        validity = try plain.decodeDefinitionLevelsBodyMax(allocator, page_data[def_start..value_start], row_count, schema_col.max_definition_level);
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

    const values_compressed_size = page_data.len - value_start;
    const total_uncompressed_size = try checkedPageSize(page_header.uncompressed_page_size);
    if (levels_len > total_uncompressed_size) return error.CorruptPage;
    const values_uncompressed_size = total_uncompressed_size - levels_len;
    if (non_null_count == 0 and values_compressed_size == 0 and values_uncompressed_size == 0) return false;
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

    switch (data_header.encoding) {
        .plain => return try acc.appendPlainFixedValues(allocator, schema_col.column_type, non_null_count, value_bytes.data, validity),
        .byte_stream_split => return try acc.appendByteStreamSplitFixedValues(allocator, schema_col.column_type, non_null_count, value_bytes.data, validity),
        .delta_binary_packed => return try acc.appendDeltaBinaryPackedValues(allocator, schema_col.column_type, non_null_count, value_bytes.data, validity),
        .rle_dictionary, .plain_dictionary => {
            const dict = dictionary orelse return error.CorruptPage;
            if (non_null_count == 0) {
                const empty_indexes: [0]u32 = .{};
                return try acc.appendDictionaryValues(allocator, column.physical_type, dict, empty_indexes[0..], validity);
            }
            if (value_bytes.data.len == 0) return error.CorruptPage;
            const bit_width = value_bytes.data[0];
            if (try acc.appendDictionaryEncodedValues(allocator, column.physical_type, dict, value_bytes.data[1..], bit_width, non_null_count, validity, dictionary_index_start)) return true;
            const indexes = try plain.decodeRleBitPackedUint32(allocator, value_bytes.data[1..], bit_width, non_null_count);
            defer allocator.free(indexes);
            return try acc.appendDictionaryValues(allocator, column.physical_type, dict, indexes, validity);
        },
        else => return false,
    }
}

fn fixedAccumulatorSupported(physical_type: types.Type) bool {
    return switch (physical_type) {
        .int32, .int64, .float, .double => true,
        else => false,
    };
}

fn plainFixedDirectCandidate(column: types.ColumnChunkMeta, schema_col: types.Column) bool {
    if (schema_col.repetition != .required) return false;
    if (!fixedAccumulatorSupported(schema_col.column_type.physical)) return false;
    if (column.dictionary_page_offset != null) return false;

    var has_plain = false;
    for (column.encodings) |encoding| {
        switch (encoding) {
            .plain => has_plain = true,
            .rle => {},
            else => return false,
        }
    }
    return has_plain;
}

fn dictionaryAccumulatorSupported(physical_type: types.Type) bool {
    return switch (physical_type) {
        .int32, .int64, .int96, .float, .double, .byte_array, .fixed_len_byte_array => true,
        else => false,
    };
}

fn deltaBinaryAccumulatorSupported(physical_type: types.Type) bool {
    return switch (physical_type) {
        .int32, .int64 => true,
        else => false,
    };
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
    try validateFlatRepetitionLevels(page_data[0..rep_len], row_count, schema_col.max_repetition_level);
    var validity: ?[]bool = null;
    if (schema_col.repetition == .optional) {
        validity = try plain.decodeDefinitionLevelsBodyMax(allocator, page_data[def_start..value_start], row_count, schema_col.max_definition_level);
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
    if (non_null_count == 0 and values_compressed_size == 0 and values_uncompressed_size == 0) {
        return plain.decodeValues(allocator, schema_col.column_type, row_count, 0, validity, &.{});
    }
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
    if (non_null_count == 0) {
        const empty_indexes: [0]u32 = .{};
        return plain.materializeDictionary(allocator, column.physical_type, dict, empty_indexes[0..], validity);
    }
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

fn validateFlatRepetitionLevels(data: []const u8, value_count: usize, max_repetition_level: u16) !void {
    if (max_repetition_level != 0) return error.UnsupportedNestedSchema;
    if (!try plain.levelsBodyAllEqualMax(data, value_count, 0, 0)) return error.CorruptPage;
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
    int96: struct {
        values: std.ArrayList(ByteRange) = .empty,
        bytes: std.ArrayList(u8) = .empty,
        validity: std.ArrayList(bool) = .empty,
        optional: bool,
    },
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
            .int96 => .{ .int96 = .{ .optional = optional } },
            .float => .{ .float = .{ .optional = optional } },
            .double => .{ .double = .{ .optional = optional } },
            .byte_array => .{ .byte_array = .{ .optional = optional } },
            .fixed_len_byte_array => .{ .fixed_len_byte_array = .{ .optional = optional } },
        };
    }

    fn deinit(self: *ColumnAccumulator, allocator: std.mem.Allocator) void {
        switch (self.*) {
            inline .boolean, .int32, .int64, .float, .double => |*acc| {
                acc.values.deinit(allocator);
                acc.validity.deinit(allocator);
            },
            .int96 => |*acc| {
                acc.values.deinit(allocator);
                acc.bytes.deinit(allocator);
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
            .int96 => |*acc| {
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
            .int96 => |*acc| switch (column) {
                .int96 => |page| {
                    for (page.values) |value| {
                        const start = acc.bytes.items.len;
                        try acc.bytes.appendSlice(allocator, value);
                        try acc.values.append(allocator, .{ .start = start, .len = value.len });
                    }
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

    fn appendPlainFixedValues(self: *ColumnAccumulator, allocator: std.mem.Allocator, column_type: types.ColumnType, count: usize, data: []const u8, validity: ?[]const bool) !bool {
        return switch (column_type.physical) {
            .int32 => switch (self.*) {
                .int32 => |*acc| blk: {
                    try appendPlainIntValues(i32, allocator, &acc.values, count, data);
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .int64 => switch (self.*) {
                .int64 => |*acc| blk: {
                    try appendPlainIntValues(i64, allocator, &acc.values, count, data);
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .float => switch (self.*) {
                .float => |*acc| blk: {
                    try appendPlainFloatValues(f32, u32, allocator, &acc.values, count, data);
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .double => switch (self.*) {
                .double => |*acc| blk: {
                    try appendPlainFloatValues(f64, u64, allocator, &acc.values, count, data);
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            else => false,
        };
    }

    fn appendByteStreamSplitFixedValues(self: *ColumnAccumulator, allocator: std.mem.Allocator, column_type: types.ColumnType, count: usize, data: []const u8, validity: ?[]const bool) !bool {
        return switch (column_type.physical) {
            .int32 => switch (self.*) {
                .int32 => |*acc| blk: {
                    const dest = try acc.values.addManyAsSlice(allocator, count);
                    try plain.decodeByteStreamSplitIntsInto(i32, dest, data);
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .int64 => switch (self.*) {
                .int64 => |*acc| blk: {
                    const dest = try acc.values.addManyAsSlice(allocator, count);
                    try plain.decodeByteStreamSplitIntsInto(i64, dest, data);
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .float => switch (self.*) {
                .float => |*acc| blk: {
                    const dest = try acc.values.addManyAsSlice(allocator, count);
                    try plain.decodeByteStreamSplitFloatsInto(f32, dest, data);
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .double => switch (self.*) {
                .double => |*acc| blk: {
                    const dest = try acc.values.addManyAsSlice(allocator, count);
                    try plain.decodeByteStreamSplitFloatsInto(f64, dest, data);
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            else => false,
        };
    }

    fn appendDeltaBinaryPackedValues(self: *ColumnAccumulator, allocator: std.mem.Allocator, column_type: types.ColumnType, count: usize, data: []const u8, validity: ?[]const bool) !bool {
        return switch (column_type.physical) {
            .int32 => switch (self.*) {
                .int32 => |*acc| blk: {
                    const dest = try acc.values.addManyAsSlice(allocator, count);
                    const consumed = try plain.decodeDeltaBinaryPackedIntsInto(i32, dest, data);
                    if (consumed != data.len) return error.CorruptPage;
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .int64 => switch (self.*) {
                .int64 => |*acc| blk: {
                    const dest = try acc.values.addManyAsSlice(allocator, count);
                    const consumed = try plain.decodeDeltaBinaryPackedIntsInto(i64, dest, data);
                    if (consumed != data.len) return error.CorruptPage;
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            else => false,
        };
    }

    fn appendDictionaryValues(self: *ColumnAccumulator, allocator: std.mem.Allocator, physical_type: types.Type, dictionary: *const types.OwnedColumn, indexes: []const u32, validity: ?[]const bool) !bool {
        return switch (physical_type) {
            .int32 => switch (self.*) {
                .int32 => |*acc| blk: {
                    switch (dictionary.*) {
                        .int32 => |dict| try appendDictionaryFixed(i32, allocator, &acc.values, dict.values, indexes),
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .int64 => switch (self.*) {
                .int64 => |*acc| blk: {
                    switch (dictionary.*) {
                        .int64 => |dict| try appendDictionaryFixed(i64, allocator, &acc.values, dict.values, indexes),
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .float => switch (self.*) {
                .float => |*acc| blk: {
                    switch (dictionary.*) {
                        .float => |dict| try appendDictionaryFixed(f32, allocator, &acc.values, dict.values, indexes),
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .double => switch (self.*) {
                .double => |*acc| blk: {
                    switch (dictionary.*) {
                        .double => |dict| try appendDictionaryFixed(f64, allocator, &acc.values, dict.values, indexes),
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .byte_array => switch (self.*) {
                .byte_array => |*acc| blk: {
                    switch (dictionary.*) {
                        .byte_array => |dict| try appendDictionaryByteRanges(allocator, acc, dict.values, indexes),
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .int96 => switch (self.*) {
                .int96 => |*acc| blk: {
                    switch (dictionary.*) {
                        .int96 => |dict| try appendDictionaryByteRanges(allocator, acc, dict.values, indexes),
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .fixed_len_byte_array => switch (self.*) {
                .fixed_len_byte_array => |*acc| blk: {
                    switch (dictionary.*) {
                        .fixed_len_byte_array => |dict| try appendDictionaryByteRanges(allocator, acc, dict.values, indexes),
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            else => false,
        };
    }

    fn appendDictionaryEncodedValues(self: *ColumnAccumulator, allocator: std.mem.Allocator, physical_type: types.Type, dictionary: *const types.OwnedColumn, data: []const u8, bit_width: u8, count: usize, validity: ?[]const bool, dictionary_index_start: usize) !bool {
        const identity_start: ?usize = if (validity == null) dictionary_index_start else null;
        return switch (physical_type) {
            .int32 => switch (self.*) {
                .int32 => |*acc| blk: {
                    switch (dictionary.*) {
                        .int32 => |dict| {
                            const identity_appended = if (identity_start) |start|
                                try appendDictionaryIdentitySlice(i32, allocator, &acc.values, dict.values, data, bit_width, count, start)
                            else
                                false;
                            if (!identity_appended) {
                                try appendDictionaryFixedEncoded(i32, allocator, &acc.values, dict.values, data, bit_width, count);
                            }
                        },
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .int64 => switch (self.*) {
                .int64 => |*acc| blk: {
                    switch (dictionary.*) {
                        .int64 => |dict| {
                            const identity_appended = if (identity_start) |start|
                                try appendDictionaryIdentitySlice(i64, allocator, &acc.values, dict.values, data, bit_width, count, start)
                            else
                                false;
                            if (!identity_appended) {
                                try appendDictionaryFixedEncoded(i64, allocator, &acc.values, dict.values, data, bit_width, count);
                            }
                        },
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .float => switch (self.*) {
                .float => |*acc| blk: {
                    switch (dictionary.*) {
                        .float => |dict| {
                            const identity_appended = if (identity_start) |start|
                                try appendDictionaryIdentitySlice(f32, allocator, &acc.values, dict.values, data, bit_width, count, start)
                            else
                                false;
                            if (!identity_appended) {
                                try appendDictionaryFixedEncoded(f32, allocator, &acc.values, dict.values, data, bit_width, count);
                            }
                        },
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .double => switch (self.*) {
                .double => |*acc| blk: {
                    switch (dictionary.*) {
                        .double => |dict| {
                            const identity_appended = if (identity_start) |start|
                                try appendDictionaryIdentitySlice(f64, allocator, &acc.values, dict.values, data, bit_width, count, start)
                            else
                                false;
                            if (!identity_appended) {
                                try appendDictionaryFixedEncoded(f64, allocator, &acc.values, dict.values, data, bit_width, count);
                            }
                        },
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            else => false,
        };
    }

    fn dictionaryIdentityStart(self: *ColumnAccumulator, physical_type: types.Type) ?usize {
        return switch (physical_type) {
            .int32 => switch (self.*) {
                .int32 => |*acc| acc.values.items.len,
                else => null,
            },
            .int64 => switch (self.*) {
                .int64 => |*acc| acc.values.items.len,
                else => null,
            },
            .float => switch (self.*) {
                .float => |*acc| acc.values.items.len,
                else => null,
            },
            .double => switch (self.*) {
                .double => |*acc| acc.values.items.len,
                else => null,
            },
            else => null,
        };
    }

    fn appendDictionaryIdentityValues(self: *ColumnAccumulator, allocator: std.mem.Allocator, physical_type: types.Type, dictionary: *const types.OwnedColumn, start: usize, count: usize, validity: ?[]const bool) !bool {
        return switch (physical_type) {
            .int32 => switch (self.*) {
                .int32 => |*acc| blk: {
                    switch (dictionary.*) {
                        .int32 => |dict| {
                            const end = std.math.add(usize, start, count) catch return error.CorruptPage;
                            if (end > dict.values.len) return error.CorruptPage;
                            try acc.values.appendSlice(allocator, dict.values[start..end]);
                        },
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .int64 => switch (self.*) {
                .int64 => |*acc| blk: {
                    switch (dictionary.*) {
                        .int64 => |dict| {
                            const end = std.math.add(usize, start, count) catch return error.CorruptPage;
                            if (end > dict.values.len) return error.CorruptPage;
                            try acc.values.appendSlice(allocator, dict.values[start..end]);
                        },
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .float => switch (self.*) {
                .float => |*acc| blk: {
                    switch (dictionary.*) {
                        .float => |dict| {
                            const end = std.math.add(usize, start, count) catch return error.CorruptPage;
                            if (end > dict.values.len) return error.CorruptPage;
                            try acc.values.appendSlice(allocator, dict.values[start..end]);
                        },
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            .double => switch (self.*) {
                .double => |*acc| blk: {
                    switch (dictionary.*) {
                        .double => |dict| {
                            const end = std.math.add(usize, start, count) catch return error.CorruptPage;
                            if (end > dict.values.len) return error.CorruptPage;
                            try acc.values.appendSlice(allocator, dict.values[start..end]);
                        },
                        else => return error.CorruptPage,
                    }
                    try appendValidity(allocator, &acc.validity, acc.optional, validity);
                    break :blk true;
                },
                else => false,
            },
            else => false,
        };
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
            .int96 => |*acc| blk: {
                const ranges = try acc.values.toOwnedSlice(allocator);
                defer allocator.free(ranges);
                const bytes = try acc.bytes.toOwnedSlice(allocator);
                errdefer allocator.free(bytes);
                const values = try allocator.alloc([]const u8, ranges.len);
                errdefer allocator.free(values);
                for (ranges, values) |range, *value| {
                    value.* = bytes[range.start..][0..range.len];
                }
                break :blk .{ .int96 = .{
                    .values = values,
                    .data = bytes,
                    .validity = try finishValidity(allocator, &acc.validity, acc.optional),
                } };
            },
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

fn appendPlainIntValues(comptime T: type, allocator: std.mem.Allocator, values: *std.ArrayList(T), count: usize, data: []const u8) !void {
    const bytes = std.math.mul(usize, @sizeOf(T), count) catch return error.CorruptPage;
    if (data.len < bytes) return error.CorruptPage;
    const dest = try values.addManyAsSlice(allocator, count);
    if (native_endian == .little) {
        @memcpy(std.mem.sliceAsBytes(dest), data[0..bytes]);
    } else {
        for (dest, 0..) |*value, i| value.* = std.mem.readInt(T, data[i * @sizeOf(T) ..][0..@sizeOf(T)], .little);
    }
}

fn appendPlainFloatValues(comptime T: type, comptime U: type, allocator: std.mem.Allocator, values: *std.ArrayList(T), count: usize, data: []const u8) !void {
    const bytes = std.math.mul(usize, @sizeOf(T), count) catch return error.CorruptPage;
    if (data.len < bytes) return error.CorruptPage;
    const dest = try values.addManyAsSlice(allocator, count);
    if (native_endian == .little) {
        @memcpy(std.mem.sliceAsBytes(dest), data[0..bytes]);
    } else {
        for (dest, 0..) |*value, i| value.* = @bitCast(std.mem.readInt(U, data[i * @sizeOf(T) ..][0..@sizeOf(T)], .little));
    }
}

fn appendDictionaryFixed(comptime T: type, allocator: std.mem.Allocator, values: *std.ArrayList(T), dictionary: []const T, indexes: []const u32) !void {
    const dest = try values.addManyAsSlice(allocator, indexes.len);
    for (indexes, dest) |idx, *value| {
        if (idx >= dictionary.len) return error.CorruptPage;
        value.* = dictionary[idx];
    }
}

fn appendDictionaryIdentitySlice(comptime T: type, allocator: std.mem.Allocator, values: *std.ArrayList(T), dictionary: []const T, data: []const u8, bit_width: u8, count: usize, start: usize) !bool {
    if (!try plain.rleBitPackedUint32IdentityFrom(data, bit_width, count, start)) return false;
    const end = std.math.add(usize, start, count) catch return error.CorruptPage;
    if (end > dictionary.len) return error.CorruptPage;
    try values.appendSlice(allocator, dictionary[start..end]);
    return true;
}

fn appendDictionaryFixedEncoded(comptime T: type, allocator: std.mem.Allocator, values: *std.ArrayList(T), dictionary: []const T, data: []const u8, bit_width: u8, count: usize) !void {
    if (bit_width > 32) return error.CorruptPage;
    const dest = try values.addManyAsSlice(allocator, count);
    if (bit_width == 0) {
        if (dictionary.len == 0) return error.CorruptPage;
        fillDictionaryRun(T, dest, dictionary[0]);
        return;
    }

    const width_bytes = (@as(usize, bit_width) + 7) / 8;
    var pos: usize = 0;
    var idx: usize = 0;
    while (pos < data.len and idx < count) {
        const header = try readRleVarUint(data, &pos);
        if ((header & 1) == 0) {
            const run_len = std.math.cast(usize, header >> 1) orelse return error.CorruptPage;
            if (width_bytes > data.len - pos) return error.CorruptPage;
            const dict_idx = try readRleIndexValue(data[pos..][0..width_bytes]);
            pos += width_bytes;
            if (dict_idx >= dictionary.len or run_len > count - idx) return error.CorruptPage;
            fillDictionaryRun(T, dest[idx..][0..run_len], dictionary[dict_idx]);
            idx += run_len;
        } else {
            const group_count = std.math.cast(usize, header >> 1) orelse return error.CorruptPage;
            const total = std.math.mul(usize, group_count, 8) catch return error.CorruptPage;
            const bit_len = std.math.mul(usize, total, @as(usize, bit_width)) catch return error.CorruptPage;
            const byte_len = try ceilDiv8Local(bit_len);
            if (byte_len > data.len - pos) return error.CorruptPage;

            const decode_count = @min(total, count - idx);
            try appendDictionaryBitPackedRun(T, dest[idx..][0..decode_count], dictionary, data[pos..][0..byte_len], bit_width);
            idx += decode_count;
            pos += byte_len;
        }
    }
    if (idx != count) return error.CorruptPage;
}

fn fillDictionaryRun(comptime T: type, dest: []T, value: T) void {
    for (dest) |*slot| slot.* = value;
}

fn appendDictionaryBitPackedRun(comptime T: type, dest: []T, dictionary: []const T, data: []const u8, bit_width: u8) !void {
    switch (bit_width) {
        8 => {
            for (dest, data[0..dest.len]) |*value, idx| {
                if (idx >= dictionary.len) return error.CorruptPage;
                value.* = dictionary[idx];
            }
        },
        16 => {
            for (dest, 0..) |*value, i| {
                const offset = i * 2;
                const idx = @as(u32, data[offset]) |
                    (@as(u32, data[offset + 1]) << 8);
                if (idx >= dictionary.len) return error.CorruptPage;
                value.* = dictionary[idx];
            }
        },
        24 => {
            for (dest, 0..) |*value, i| {
                const offset = i * 3;
                const idx = @as(u32, data[offset]) |
                    (@as(u32, data[offset + 1]) << 8) |
                    (@as(u32, data[offset + 2]) << 16);
                if (idx >= dictionary.len) return error.CorruptPage;
                value.* = dictionary[idx];
            }
        },
        32 => {
            for (dest, 0..) |*value, i| {
                const idx = std.mem.readInt(u32, data[i * 4 ..][0..4], .little);
                if (idx >= dictionary.len) return error.CorruptPage;
                value.* = dictionary[idx];
            }
        },
        else => {
            var bit_pos: usize = 0;
            for (dest) |*value| {
                const idx = readPackedIndex(data, bit_pos, bit_width);
                if (idx >= dictionary.len) return error.CorruptPage;
                value.* = dictionary[idx];
                bit_pos += bit_width;
            }
        },
    }
}

fn readRleVarUint(data: []const u8, pos: *usize) !u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (true) {
        if (pos.* >= data.len) return error.CorruptPage;
        const byte = data[pos.*];
        pos.* += 1;
        result |= (@as(u64, byte & 0x7f) << shift);
        if ((byte & 0x80) == 0) return result;
        if (shift >= 63) return error.CorruptPage;
        shift += 7;
    }
}

fn readRleIndexValue(data: []const u8) !usize {
    var value: u32 = 0;
    for (data, 0..) |byte, i| value |= @as(u32, byte) << @intCast(i * 8);
    return std.math.cast(usize, value) orelse error.CorruptPage;
}

fn readPackedIndex(data: []const u8, start_bit: usize, bit_width: u8) u32 {
    var value: u32 = 0;
    var bit: u8 = 0;
    while (bit < bit_width) : (bit += 1) {
        const absolute = start_bit + bit;
        const source = (data[absolute / 8] >> @intCast(absolute & 7)) & 1;
        value |= @as(u32, source) << @intCast(bit);
    }
    return value;
}

fn ceilDiv8Local(value: usize) !usize {
    const adjusted = std.math.add(usize, value, 7) catch return error.CorruptPage;
    return adjusted / 8;
}

fn appendDictionaryByteRanges(allocator: std.mem.Allocator, acc: anytype, dictionary: []const []const u8, indexes: []const u32) !void {
    var total_bytes: usize = 0;
    for (indexes) |idx| {
        if (idx >= dictionary.len) return error.CorruptPage;
        total_bytes = std.math.add(usize, total_bytes, dictionary[idx].len) catch return error.CorruptPage;
    }

    const ranges = try acc.values.addManyAsSlice(allocator, indexes.len);
    try acc.bytes.ensureUnusedCapacity(allocator, total_bytes);
    for (indexes, ranges) |idx, *range| {
        const value = dictionary[idx];
        const start = acc.bytes.items.len;
        acc.bytes.appendSliceAssumeCapacity(value);
        range.* = .{ .start = start, .len = value.len };
    }
}

fn appendValidity(allocator: std.mem.Allocator, dest: *std.ArrayList(bool), optional: bool, validity: ?[]const bool) !void {
    if (!optional) return;
    try dest.appendSlice(allocator, validity orelse return error.CorruptPage);
}

fn finishValidity(allocator: std.mem.Allocator, list: *std.ArrayList(bool), optional: bool) !?[]bool {
    if (!optional) return null;
    return try list.toOwnedSlice(allocator);
}

fn dupePath(allocator: std.mem.Allocator, path: []const []const u8) ![]const []const u8 {
    const out = try allocator.alloc([]const u8, path.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |part| allocator.free(part);
        allocator.free(out);
    }
    for (path, 0..) |part, idx| {
        out[idx] = try allocator.dupe(u8, part);
        initialized += 1;
    }
    return out;
}

fn freePath(allocator: std.mem.Allocator, path: []const []const u8) void {
    for (path) |part| allocator.free(part);
    allocator.free(path);
}

fn validateReadableColumnSchema(column: types.Column) !void {
    if (column.repetition == .repeated or column.max_repetition_level != 0) return error.UnsupportedNestedSchema;
}

fn schemaColumnIndexByPath(schema: types.Schema, dotted_path: []const u8) ?usize {
    for (schema.columns, 0..) |column, index| {
        if (columnPathEquals(column, dotted_path)) return index;
    }
    return null;
}

fn parsedColumnIndexesByPath(parsed: *const ParsedFile, allocator: std.mem.Allocator, column_paths: []const []const u8) ![]usize {
    const column_indexes = try allocator.alloc(usize, column_paths.len);
    errdefer allocator.free(column_indexes);
    for (column_paths, 0..) |path, index| {
        column_indexes[index] = parsed.columnIndexByPath(path) orelse return error.InvalidColumnData;
    }
    return column_indexes;
}

fn columnPathEquals(column: types.Column, dotted_path: []const u8) bool {
    if (column.path.len == 0) return std.mem.eql(u8, column.name, dotted_path);

    var parts = std.mem.splitScalar(u8, dotted_path, '.');
    var index: usize = 0;
    while (parts.next()) |part| : (index += 1) {
        if (index >= column.path.len) return false;
        if (!std.mem.eql(u8, column.path[index], part)) return false;
    }
    return index == column.path.len;
}

fn validateSupported(metadata: types.FileMetaData, file_size: u64) !void {
    if (metadata.version != 1 and metadata.version != 2) return error.CorruptMetadata;
    if (metadata.num_rows < 0) return error.CorruptMetadata;
    for (metadata.schema.columns) |column| {
        switch (column.column_type.physical) {
            .boolean, .int32, .int64, .float, .double, .byte_array => {},
            .int96 => {
                _ = try types.physicalTypeWidth(column.column_type.physical, column.column_type.type_length);
            },
            .fixed_len_byte_array => {
                _ = try types.physicalTypeWidth(column.column_type.physical, column.column_type.type_length);
            },
        }
        try types.validateDecimalType(column.column_type);
    }
    for (metadata.row_groups) |row_group| {
        if (row_group.num_rows < 0) return error.CorruptMetadata;
        if (row_group.columns.len != metadata.schema.columns.len) return error.CorruptMetadata;
        for (row_group.columns, metadata.schema.columns) |column, schema_col| {
            if (column.physical_type != schema_col.column_type.physical) return error.CorruptMetadata;
            if (!columnChunkPathMatchesSchema(column.path, schema_col)) return error.CorruptMetadata;
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
                .uncompressed, .snappy, .gzip, .lz4, .lz4_raw, .zstd => {},
                else => return error.UnsupportedCompression,
            }
            var has_supported_value_encoding = false;
            for (column.encodings) |encoding| {
                switch (encoding) {
                    .plain, .byte_stream_split, .delta_binary_packed, .delta_length_byte_array, .delta_byte_array => has_supported_value_encoding = true,
                    .rle_dictionary, .plain_dictionary => has_supported_value_encoding = true,
                    .rle, .bit_packed => {},
                }
            }
            if (column.encodings.len > 0 and !has_supported_value_encoding and schema_col.column_type.physical != .boolean and row_group.num_rows != 0) return error.UnsupportedEncoding;
        }
    }
}

fn columnChunkPathMatchesSchema(column_path: []const u8, schema_col: types.Column) bool {
    if (schema_col.path.len == 0) return std.mem.eql(u8, column_path, schema_col.name);
    var offset: usize = 0;
    for (schema_col.path, 0..) |part, index| {
        if (index > 0) {
            if (offset >= column_path.len or column_path[offset] != '.') return false;
            offset += 1;
        }
        if (offset + part.len > column_path.len) return false;
        if (!std.mem.eql(u8, column_path[offset..][0..part.len], part)) return false;
        offset += part.len;
    }
    return offset == column_path.len;
}

test "column chunk path matches nested schema path" {
    const nested_path = [_][]const u8{ "root", "items", "element" };
    const nested_col: types.Column = .{
        .name = "element",
        .path = nested_path[0..],
        .column_type = .{ .physical = .int32 },
    };
    try std.testing.expect(columnChunkPathMatchesSchema("root.items.element", nested_col));
    try std.testing.expect(!columnChunkPathMatchesSchema("root.items.value", nested_col));

    const flat_col: types.Column = .{
        .name = "id",
        .column_type = .{ .physical = .int64 },
    };
    try std.testing.expect(columnChunkPathMatchesSchema("id", flat_col));
    try std.testing.expect(!columnChunkPathMatchesSchema("other", flat_col));
}

fn plainDictionaryPageEncoding(encoding: types.Encoding) bool {
    return encoding == .plain or encoding == .plain_dictionary or encoding == .bit_packed;
}

fn dictionaryIndexBitWidth(dictionary: types.OwnedColumn) !u8 {
    const count = switch (dictionary) {
        inline else => |values| values.values.len,
    };
    if (count <= 1) return 0;
    return std.math.log2_int_ceil(usize, count);
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
    page_buffer: std.ArrayList(u8) = .empty,
    value_buffer: std.ArrayList(u8) = .empty,
    zstd_buffer: std.ArrayList(u8) = .empty,
    zstd_cache: zstd.DecodeCache = .{},

    fn deinit(self: *PageDecodeScratch, allocator: std.mem.Allocator) void {
        self.page_buffer.deinit(allocator);
        self.value_buffer.deinit(allocator);
        self.zstd_buffer.deinit(allocator);
    }

    fn pageData(self: *PageDecodeScratch, allocator: std.mem.Allocator, len: usize) ![]u8 {
        try self.page_buffer.ensureTotalCapacity(allocator, len);
        return self.page_buffer.allocatedSlice()[0..len];
    }

    fn valueData(self: *PageDecodeScratch, allocator: std.mem.Allocator, len: usize) ![]u8 {
        try self.value_buffer.ensureTotalCapacity(allocator, len);
        return self.value_buffer.allocatedSlice()[0..len];
    }

    fn zstdScratch(self: *PageDecodeScratch, allocator: std.mem.Allocator, window_len: usize) ![]u8 {
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

fn preparePageBytesInto(allocator: std.mem.Allocator, codec: types.CompressionCodec, header: types.PageHeader, compressed: []const u8, out: []u8, scratch: *PageDecodeScratch) !void {
    const compressed_size = try checkedPageSize(header.compressed_page_size);
    const uncompressed_size = try checkedPageSize(header.uncompressed_page_size);
    try prepareValueBytesInto(allocator, codec, true, uncompressed_size, compressed_size, compressed, out, scratch);
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
            const out = try scratch.valueData(allocator, uncompressed_size);
            try snappy.decompress(compressed, out);
            return .{ .data = out };
        },
        .gzip => {
            const out = try scratch.valueData(allocator, uncompressed_size);
            try gzip.decompress(compressed, out);
            return .{ .data = out };
        },
        .lz4_raw => {
            const out = try scratch.valueData(allocator, uncompressed_size);
            try lz4.decompress(compressed, out);
            return .{ .data = out };
        },
        .lz4 => {
            const out = try scratch.valueData(allocator, uncompressed_size);
            try lz4.decompressLegacy(compressed, out);
            return .{ .data = out };
        },
        .zstd => {
            const out = try scratch.valueData(allocator, uncompressed_size);
            const window_len = try zstd.decoderWindowLenBounded(compressed, uncompressed_size, max_page_size);
            try zstd.decompressWithScratchAndCache(compressed, out, try scratch.zstdScratch(allocator, window_len), window_len, &scratch.zstd_cache);
            return .{ .data = out };
        },
        else => return error.UnsupportedCompression,
    }
}

fn prepareValueBytesInto(
    allocator: std.mem.Allocator,
    codec: types.CompressionCodec,
    is_compressed: bool,
    uncompressed_size: usize,
    compressed_size: usize,
    compressed: []const u8,
    out: []u8,
    scratch: *PageDecodeScratch,
) !void {
    if (compressed.len != compressed_size or out.len != uncompressed_size) return error.CorruptPage;
    if (!is_compressed) {
        if (compressed_size != uncompressed_size) return error.CorruptPage;
        @memcpy(out, compressed);
        return;
    }
    switch (codec) {
        .uncompressed => {
            if (compressed_size != uncompressed_size) return error.CorruptPage;
            @memcpy(out, compressed);
        },
        .snappy => try snappy.decompress(compressed, out),
        .gzip => try gzip.decompress(compressed, out),
        .lz4_raw => try lz4.decompress(compressed, out),
        .lz4 => try lz4.decompressLegacy(compressed, out),
        .zstd => {
            const window_len = try zstd.decoderWindowLenBounded(compressed, uncompressed_size, max_page_size);
            try zstd.decompressWithScratchAndCache(compressed, out, try scratch.zstdScratch(allocator, window_len), window_len, &scratch.zstd_cache);
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

test "stream reader reads row-group columns in parallel" {
    const testing = std.testing;
    const writer_mod = @import("writer.zig");
    const cols = [_]types.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{ .name = "score", .column_type = .{ .physical = .double } },
        .{ .name = "label", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = types.Schema.init("schema", cols[0..]);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var w = writer_mod.StreamWriter.initOptions(testing.allocator, &out.writer, schema, .{ .compression = .zstd });
    defer w.deinit();
    try w.start();

    const ids = [_]i64{ 10, 11, 12, 13 };
    const scores = [_]f64{ 2.5, 2.75, 3.0, 3.25 };
    const labels = [_][]const u8{ "a", "c" };
    const validity = [_]bool{ true, false, true, false };
    const batch = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .double = .{ .values = scores[0..] } },
        .{ .byte_array = .{ .values = labels[0..], .validity = validity[0..] } },
    };
    try w.writeRowGroup(ids.len, batch[0..]);

    const ids_2 = [_]i64{ 20, 21, 22 };
    const scores_2 = [_]f64{ 4.5, 4.75, 5.0 };
    const labels_2 = [_][]const u8{ "x", "z" };
    const validity_2 = [_]bool{ true, true, false };
    const batch_2 = [_]types.ColumnData{
        .{ .int64 = .{ .values = ids_2[0..] } },
        .{ .double = .{ .values = scores_2[0..] } },
        .{ .byte_array = .{ .values = labels_2[0..], .validity = validity_2[0..] } },
    };
    try w.writeRowGroup(ids_2.len, batch_2[0..]);
    try w.finish();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var file = try tmp.dir.createFile(testing.io, "parallel.parquet", .{ .truncate = true });
        defer file.close(testing.io);
        var writer_buffer: [256]u8 = undefined;
        var file_writer = file.writer(testing.io, &writer_buffer);
        try file_writer.interface.writeAll(out.written());
        try file_writer.interface.flush();
    }

    var file = try tmp.dir.openFile(testing.io, "parallel.parquet", .{});
    defer file.close(testing.io);
    var reader_buffer: [1024]u8 = undefined;
    var file_reader = file.reader(testing.io, &reader_buffer);
    var parsed = try StreamFileReader.init(testing.allocator, &file_reader);
    defer parsed.deinit();

    const columns = try parsed.readRowGroupColumnsParallel(testing.allocator, 0, .{ .max_threads = 2 });
    defer {
        for (columns) |*column| column.deinit(testing.allocator);
        testing.allocator.free(columns);
    }

    try testing.expectEqual(@as(usize, 3), columns.len);
    try testing.expectEqualSlices(i64, ids[0..], columns[0].int64.values);
    try testing.expectEqualSlices(f64, scores[0..], columns[1].double.values);
    try testing.expectEqualSlices(bool, validity[0..], columns[2].byte_array.validity.?);
    try testing.expectEqualStrings("a", columns[2].byte_array.values[0]);
    try testing.expectEqualStrings("c", columns[2].byte_array.values[1]);

    const row_group_indexes = [_]usize{ 0, 1 };
    const batches = try parsed.readRowGroupsColumnsParallel(testing.allocator, row_group_indexes[0..], .{ .max_threads = 4 });
    defer {
        for (batches) |*row_group| row_group.deinit(testing.allocator);
        testing.allocator.free(batches);
    }

    try testing.expectEqual(@as(usize, 2), batches.len);
    try testing.expectEqual(@as(usize, 0), batches[0].row_group_index);
    try testing.expectEqual(@as(usize, 1), batches[1].row_group_index);
    try testing.expectEqualSlices(i64, ids[0..], batches[0].columns[0].int64.values);
    try testing.expectEqualSlices(f64, scores[0..], batches[0].columns[1].double.values);
    try testing.expectEqualSlices(bool, validity[0..], batches[0].columns[2].byte_array.validity.?);
    try testing.expectEqualSlices(i64, ids_2[0..], batches[1].columns[0].int64.values);
    try testing.expectEqualSlices(f64, scores_2[0..], batches[1].columns[1].double.values);
    try testing.expectEqualSlices(bool, validity_2[0..], batches[1].columns[2].byte_array.validity.?);
    try testing.expectEqualStrings("x", batches[1].columns[2].byte_array.values[0]);
    try testing.expectEqualStrings("z", batches[1].columns[2].byte_array.values[1]);
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
