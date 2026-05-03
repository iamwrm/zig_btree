const std = @import("std");
const types = @import("types.zig");

const CompactType = enum(u8) {
    stop = 0,
    boolean_true = 1,
    boolean_false = 2,
    byte = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    struct_ = 12,
};

const Field = struct {
    id: i16,
    compact_type: CompactType,

    pub fn boolValue(self: Field) ?bool {
        return switch (self.compact_type) {
            .boolean_true => true,
            .boolean_false => false,
            else => null,
        };
    }
};

const ListHeader = struct {
    elem_type: CompactType,
    len: usize,
};

pub const CompactWriter = struct {
    writer: *std.Io.Writer,
    last_field_ids: [32]i16 = undefined,
    depth: usize = 0,

    pub fn init(writer: *std.Io.Writer) CompactWriter {
        return .{ .writer = writer };
    }

    pub fn beginStruct(self: *CompactWriter) !void {
        if (self.depth >= self.last_field_ids.len) return error.CorruptMetadata;
        self.last_field_ids[self.depth] = 0;
        self.depth += 1;
    }

    pub fn endStruct(self: *CompactWriter) !void {
        try self.writer.writeByte(@intFromEnum(CompactType.stop));
        self.depth -= 1;
    }

    pub fn fieldI32(self: *CompactWriter, field_id: i16, value: i32) !void {
        try self.writeFieldHeader(field_id, .i32);
        try self.writeI32(value);
    }

    pub fn fieldI64(self: *CompactWriter, field_id: i16, value: i64) !void {
        try self.writeFieldHeader(field_id, .i64);
        try self.writeI64(value);
    }

    pub fn fieldBool(self: *CompactWriter, field_id: i16, value: bool) !void {
        try self.writeFieldHeader(field_id, if (value) .boolean_true else .boolean_false);
    }

    pub fn writeBoolValue(self: *CompactWriter, value: bool) !void {
        try self.writer.writeByte(@intFromEnum(if (value) CompactType.boolean_true else CompactType.boolean_false));
    }

    pub fn fieldString(self: *CompactWriter, field_id: i16, value: []const u8) !void {
        try self.writeFieldHeader(field_id, .binary);
        try self.writeBinary(value);
    }

    pub fn fieldStructBegin(self: *CompactWriter, field_id: i16) !void {
        try self.writeFieldHeader(field_id, .struct_);
        try self.beginStruct();
    }

    pub fn fieldListBegin(self: *CompactWriter, field_id: i16, elem_type: CompactType, len: usize) !void {
        try self.writeFieldHeader(field_id, .list);
        try self.writeListHeader(elem_type, len);
    }

    pub fn writeI32(self: *CompactWriter, value: i32) !void {
        try self.writeVarUint(encodeZigZag(i32, value));
    }

    pub fn writeI64(self: *CompactWriter, value: i64) !void {
        try self.writeVarUint(encodeZigZag(i64, value));
    }

    pub fn writeEnum(self: *CompactWriter, value: anytype) !void {
        try self.writeI32(@intFromEnum(value));
    }

    pub fn writeBinary(self: *CompactWriter, value: []const u8) !void {
        try self.writeVarUint(value.len);
        try self.writer.writeAll(value);
    }

    pub fn writeListHeader(self: *CompactWriter, elem_type: CompactType, len: usize) !void {
        if (len <= 14) {
            try self.writer.writeByte((@as(u8, @intCast(len)) << 4) | @intFromEnum(elem_type));
        } else {
            try self.writer.writeByte(0xf0 | @intFromEnum(elem_type));
            try self.writeVarUint(len);
        }
    }

    fn writeFieldHeader(self: *CompactWriter, field_id: i16, compact_type: CompactType) !void {
        if (self.depth == 0) return error.CorruptMetadata;
        const last = &self.last_field_ids[self.depth - 1];
        const delta = field_id - last.*;
        if (delta > 0 and delta <= 15) {
            try self.writer.writeByte((@as(u8, @intCast(delta)) << 4) | @intFromEnum(compact_type));
        } else {
            try self.writer.writeByte(@intFromEnum(compact_type));
            try self.writeI16(field_id);
        }
        last.* = field_id;
    }

    fn writeI16(self: *CompactWriter, value: i16) !void {
        try self.writeVarUint(encodeZigZag(i16, value));
    }

    fn writeVarUint(self: *CompactWriter, value: anytype) !void {
        var v: u64 = @intCast(value);
        while (v >= 0x80) {
            try self.writer.writeByte(@as(u8, @intCast(v & 0x7f)) | 0x80);
            v >>= 7;
        }
        try self.writer.writeByte(@intCast(v));
    }
};

pub const CompactReader = struct {
    reader: *std.Io.Reader,
    last_field_ids: [32]i16 = undefined,
    depth: usize = 0,

    pub fn init(reader: *std.Io.Reader) CompactReader {
        return .{ .reader = reader };
    }

    pub fn beginStruct(self: *CompactReader) !void {
        if (self.depth >= self.last_field_ids.len) return error.CorruptMetadata;
        self.last_field_ids[self.depth] = 0;
        self.depth += 1;
    }

    pub fn endStruct(self: *CompactReader) void {
        self.depth -= 1;
    }

    pub fn nextField(self: *CompactReader) !?Field {
        const header = try self.reader.takeByte();
        const compact = try readCompactType(header & 0x0f);
        if (compact == .stop) return null;
        if (self.depth == 0) return error.CorruptMetadata;

        const modifier = header >> 4;
        const id = if (modifier == 0) try self.readI16() else blk: {
            const last = &self.last_field_ids[self.depth - 1];
            const next = last.* + @as(i16, @intCast(modifier));
            last.* = next;
            break :blk next;
        };
        if (modifier == 0) self.last_field_ids[self.depth - 1] = id;
        return .{ .id = id, .compact_type = compact };
    }

    pub fn readI32(self: *CompactReader) !i32 {
        return try decodeZigZag(i32, try self.readVarUint());
    }

    pub fn readI64(self: *CompactReader) !i64 {
        return try decodeZigZag(i64, try self.readVarUint());
    }

    pub fn readEnum(self: *CompactReader, comptime E: type) !E {
        const value = try self.readI32();
        return std.enums.fromInt(E, value) orelse error.CorruptMetadata;
    }

    pub fn readBinaryAlloc(self: *CompactReader, allocator: std.mem.Allocator) ![]u8 {
        const len64 = try self.readVarUint();
        const len = std.math.cast(usize, len64) orelse return error.CorruptMetadata;
        return self.reader.readAlloc(allocator, len);
    }

    pub fn readBoolValue(self: *CompactReader) !bool {
        const compact = try readCompactType(try self.reader.takeByte());
        return switch (compact) {
            .boolean_true => true,
            .boolean_false => false,
            else => error.CorruptMetadata,
        };
    }

    pub fn readListHeader(self: *CompactReader) !ListHeader {
        const header = try self.reader.takeByte();
        const elem_type = try readCompactType(header & 0x0f);
        const size = header >> 4;
        const len = if (size == 15) blk: {
            const len64 = try self.readVarUint();
            break :blk std.math.cast(usize, len64) orelse return error.CorruptMetadata;
        } else size;
        return .{ .elem_type = elem_type, .len = len };
    }

    pub fn skip(self: *CompactReader, compact_type: CompactType) !void {
        switch (compact_type) {
            .stop => {},
            .boolean_true, .boolean_false => {},
            .byte => _ = try self.reader.takeByte(),
            .i16 => _ = try self.readI16(),
            .i32 => _ = try self.readI32(),
            .i64 => _ = try self.readI64(),
            .double => _ = try self.reader.takeInt(u64, .little),
            .binary => {
                const len64 = try self.readVarUint();
                const len = std.math.cast(usize, len64) orelse return error.CorruptMetadata;
                try self.reader.discardAll(len);
            },
            .list, .set => {
                const hdr = try self.readListHeader();
                var i: usize = 0;
                while (i < hdr.len) : (i += 1) try self.skip(hdr.elem_type);
            },
            .map => {
                const len64 = try self.readVarUint();
                const len = std.math.cast(usize, len64) orelse return error.CorruptMetadata;
                if (len == 0) return;
                const types_byte = try self.reader.takeByte();
                const key_type = try readCompactType(types_byte >> 4);
                const value_type = try readCompactType(types_byte & 0x0f);
                var i: usize = 0;
                while (i < len) : (i += 1) {
                    try self.skip(key_type);
                    try self.skip(value_type);
                }
            },
            .struct_ => {
                try self.beginStruct();
                while (try self.nextField()) |field| try self.skip(field.compact_type);
                self.endStruct();
            },
        }
    }

    fn readI16(self: *CompactReader) !i16 {
        return try decodeZigZag(i16, try self.readVarUint());
    }

    fn readVarUint(self: *CompactReader) !u64 {
        var result: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            const byte = try self.reader.takeByte();
            result |= (@as(u64, byte & 0x7f) << shift);
            if ((byte & 0x80) == 0) return result;
            if (shift >= 63) return error.CorruptMetadata;
            shift += 7;
        }
    }
};

fn readCompactType(value: u8) !CompactType {
    return std.enums.fromInt(CompactType, value) orelse error.CorruptMetadata;
}

pub fn writePageHeader(writer: *std.Io.Writer, header: types.PageHeader) !void {
    var cw = CompactWriter.init(writer);
    try cw.beginStruct();
    try cw.fieldI32(1, @intFromEnum(header.page_type));
    try cw.fieldI32(2, header.uncompressed_page_size);
    try cw.fieldI32(3, header.compressed_page_size);
    if (header.crc) |crc| try cw.fieldI32(4, crc);
    if (header.data_page_header) |dp| {
        try cw.fieldStructBegin(5);
        try cw.fieldI32(1, dp.num_values);
        try cw.fieldI32(2, @intFromEnum(dp.encoding));
        try cw.fieldI32(3, @intFromEnum(dp.definition_level_encoding));
        try cw.fieldI32(4, @intFromEnum(dp.repetition_level_encoding));
        try writeStatistics(&cw, 5, dp.statistics);
        try cw.endStruct();
    }
    if (header.dictionary_page_header) |dp| {
        try cw.fieldStructBegin(7);
        try cw.fieldI32(1, dp.num_values);
        try cw.fieldI32(2, @intFromEnum(dp.encoding));
        if (dp.is_sorted) |is_sorted| try cw.fieldBool(3, is_sorted);
        try cw.endStruct();
    }
    if (header.data_page_header_v2) |dp| {
        try cw.fieldStructBegin(8);
        try cw.fieldI32(1, dp.num_values);
        try cw.fieldI32(2, dp.num_nulls);
        try cw.fieldI32(3, dp.num_rows);
        try cw.fieldI32(4, @intFromEnum(dp.encoding));
        try cw.fieldI32(5, dp.definition_levels_byte_length);
        try cw.fieldI32(6, dp.repetition_levels_byte_length);
        try cw.fieldBool(7, dp.is_compressed);
        try writeStatistics(&cw, 8, dp.statistics);
        try cw.endStruct();
    }
    try cw.endStruct();
}

pub fn readPageHeader(reader: *std.Io.Reader) !types.PageHeader {
    return readPageHeaderMaybeAlloc(null, reader);
}

pub fn readPageHeaderAlloc(allocator: std.mem.Allocator, reader: *std.Io.Reader) !types.PageHeader {
    return readPageHeaderMaybeAlloc(allocator, reader);
}

fn readPageHeaderMaybeAlloc(allocator: ?std.mem.Allocator, reader: *std.Io.Reader) !types.PageHeader {
    var cr = CompactReader.init(reader);
    var out: types.PageHeader = .{
        .page_type = .data_page,
        .uncompressed_page_size = 0,
        .compressed_page_size = 0,
        .crc = null,
    };
    errdefer if (allocator) |a| freePageHeaderStats(a, out);

    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => out.page_type = try cr.readEnum(types.PageType),
            2 => out.uncompressed_page_size = try cr.readI32(),
            3 => out.compressed_page_size = try cr.readI32(),
            4 => out.crc = try cr.readI32(),
            5 => out.data_page_header = try readDataPageHeader(allocator, &cr),
            7 => out.dictionary_page_header = try readDictionaryPageHeader(&cr),
            8 => out.data_page_header_v2 = try readDataPageHeaderV2(allocator, &cr),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn freePageHeaderStats(allocator: std.mem.Allocator, header: types.PageHeader) void {
    if (header.data_page_header) |dp| freeStatistics(allocator, dp.statistics);
    if (header.data_page_header_v2) |dp| freeStatistics(allocator, dp.statistics);
}

fn readDictionaryPageHeader(cr: *CompactReader) !types.DictionaryPageHeader {
    var out: types.DictionaryPageHeader = .{
        .num_values = 0,
        .encoding = .plain,
    };
    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => out.num_values = try cr.readI32(),
            2 => out.encoding = try cr.readEnum(types.Encoding),
            3 => out.is_sorted = field.boolValue() orelse return error.CorruptMetadata,
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn readDataPageHeaderV2(allocator: ?std.mem.Allocator, cr: *CompactReader) !types.DataPageHeaderV2 {
    var out: types.DataPageHeaderV2 = .{
        .num_values = 0,
        .num_nulls = 0,
        .num_rows = 0,
        .encoding = .plain,
        .definition_levels_byte_length = 0,
        .repetition_levels_byte_length = 0,
    };
    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => out.num_values = try cr.readI32(),
            2 => out.num_nulls = try cr.readI32(),
            3 => out.num_rows = try cr.readI32(),
            4 => out.encoding = try cr.readEnum(types.Encoding),
            5 => out.definition_levels_byte_length = try cr.readI32(),
            6 => out.repetition_levels_byte_length = try cr.readI32(),
            7 => out.is_compressed = field.boolValue() orelse return error.CorruptMetadata,
            8 => out.statistics = if (allocator) |a| try readStatisticsAlloc(a, cr) else try readStatistics(cr),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn readDataPageHeader(allocator: ?std.mem.Allocator, cr: *CompactReader) !types.DataPageHeader {
    var out: types.DataPageHeader = .{
        .num_values = 0,
        .encoding = .plain,
        .definition_level_encoding = .rle,
        .repetition_level_encoding = .rle,
    };
    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => out.num_values = try cr.readI32(),
            2 => out.encoding = try cr.readEnum(types.Encoding),
            3 => out.definition_level_encoding = try cr.readEnum(types.Encoding),
            4 => out.repetition_level_encoding = try cr.readEnum(types.Encoding),
            5 => out.statistics = if (allocator) |a| try readStatisticsAlloc(a, cr) else try readStatistics(cr),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn readStatistics(cr: *CompactReader) !types.Statistics {
    return readStatisticsMaybeAlloc(null, cr);
}

fn readStatisticsAlloc(allocator: std.mem.Allocator, cr: *CompactReader) !types.Statistics {
    return readStatisticsMaybeAlloc(allocator, cr);
}

fn readStatisticsMaybeAlloc(allocator: ?std.mem.Allocator, cr: *CompactReader) !types.Statistics {
    var out: types.Statistics = .{};
    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            3 => out.null_count = try cr.readI64(),
            1, 5 => {
                if (allocator) |a| {
                    const bytes = try cr.readBinaryAlloc(a);
                    if (out.max_value) |previous| a.free(previous);
                    out.max_value = bytes;
                } else {
                    try cr.skip(field.compact_type);
                }
            },
            2, 6 => {
                if (allocator) |a| {
                    const bytes = try cr.readBinaryAlloc(a);
                    if (out.min_value) |previous| a.free(previous);
                    out.min_value = bytes;
                } else {
                    try cr.skip(field.compact_type);
                }
            },
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn writeStatistics(cw: *CompactWriter, field_id: i16, stats: types.Statistics) !void {
    if (stats.null_count == null and stats.min_value == null and stats.max_value == null) return;

    try cw.fieldStructBegin(field_id);
    if (stats.max_value) |max_value| try cw.fieldString(1, max_value);
    if (stats.min_value) |min_value| try cw.fieldString(2, min_value);
    if (stats.null_count) |null_count| try cw.fieldI64(3, null_count);
    if (stats.max_value) |max_value| try cw.fieldString(5, max_value);
    if (stats.min_value) |min_value| try cw.fieldString(6, min_value);
    try cw.endStruct();
}

pub fn freeStatistics(allocator: std.mem.Allocator, stats: types.Statistics) void {
    if (stats.min_value) |value| allocator.free(value);
    if (stats.max_value) |value| allocator.free(value);
}

pub fn writeFileMetaData(writer: *std.Io.Writer, metadata: types.FileMetaData) !void {
    var cw = CompactWriter.init(writer);
    try cw.beginStruct();
    try cw.fieldI32(1, metadata.version);

    try cw.fieldListBegin(2, .struct_, schemaElementCount(metadata.schema));
    try writeSchemaRoot(&cw, metadata.schema);
    var schema_column_index: usize = 0;
    while (schema_column_index < metadata.schema.columns.len) {
        schema_column_index += try writeSchemaColumn(&cw, metadata.schema.columns, schema_column_index);
    }

    try cw.fieldI64(3, metadata.num_rows);

    try cw.fieldListBegin(4, .struct_, metadata.row_groups.len);
    for (metadata.row_groups) |rg| try writeRowGroup(&cw, rg);

    try cw.fieldString(6, metadata.created_by);

    try cw.fieldListBegin(7, .struct_, metadata.schema.columns.len);
    for (metadata.schema.columns) |_| try writeTypeDefinedColumnOrder(&cw);

    try cw.endStruct();
}

fn writeTypeDefinedColumnOrder(cw: *CompactWriter) !void {
    try cw.beginStruct();
    try cw.fieldStructBegin(1);
    try cw.endStruct();
    try cw.endStruct();
}

fn writeSchemaRoot(cw: *CompactWriter, schema: types.Schema) !void {
    try cw.beginStruct();
    try cw.fieldString(4, schema.name);
    try cw.fieldI32(5, @intCast(schemaTopLevelChildCount(schema)));
    try cw.endStruct();
}

fn schemaElementCount(schema: types.Schema) usize {
    var count: usize = 1;
    var column_index: usize = 0;
    while (column_index < schema.columns.len) {
        const column = schema.columns[column_index];
        if (listMapColumnSpan(schema.columns, column_index)) |span| {
            count += if (span == 2) 6 else 5;
            column_index += span;
        } else if (nestedMapColumnSpan(schema.columns, column_index)) |span| {
            count += if (span == 3) 7 else 6;
            column_index += span;
        } else if (column.map_info != null and mapValueColumnIndex(schema.columns, column_index) != null) {
            count += 4;
            column_index += 2;
        } else {
            count += if (column.list_info != null) schemaListElementCount(column) else 1;
            column_index += 1;
        }
    }
    return count;
}

fn schemaTopLevelChildCount(schema: types.Schema) usize {
    var count: usize = 0;
    var column_index: usize = 0;
    while (column_index < schema.columns.len) {
        count += 1;
        if (listMapColumnSpan(schema.columns, column_index)) |span| {
            column_index += span;
        } else if (nestedMapColumnSpan(schema.columns, column_index)) |span| {
            column_index += span;
        } else if (schema.columns[column_index].map_info != null and mapValueColumnIndex(schema.columns, column_index) != null) {
            column_index += 2;
        } else {
            column_index += 1;
        }
    }
    return count;
}

fn writeSchemaColumn(cw: *CompactWriter, columns: []const types.Column, column_index: usize) !usize {
    const column = columns[column_index];
    if (listMapColumnSpan(columns, column_index)) |span| {
        try writeSchemaListMapColumn(cw, columns[column_index..][0..span]);
        return span;
    }
    if (nestedMapColumnSpan(columns, column_index)) |span| {
        try writeSchemaNestedMapColumn(cw, columns[column_index..][0..span]);
        return span;
    }
    if (column.map_info != null) {
        if (column.path.len != 3 or !std.mem.eql(u8, column.path[2], "key")) return error.InvalidSchema;
        const value_index = mapValueColumnIndex(columns, column_index);
        try writeSchemaMapColumn(cw, column, if (value_index) |idx| columns[idx] else null);
        return if (value_index != null) 2 else 1;
    }
    if (column.list_info != null) {
        try writeSchemaListColumn(cw, column);
        return 1;
    }
    try writeSchemaLeaf(cw, column.name, column.repetition, column.column_type);
    return 1;
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

    if (key_index + 1 < columns.len) {
        const value = columns[key_index + 1];
        if (value.map_info != null and listMapLeafPathsShareMap(key, value)) return 2;
    }
    return 1;
}

fn nestedMapColumnSpan(columns: []const types.Column, key_index: usize) ?usize {
    if (key_index + 1 >= columns.len) return null;
    const outer_key = columns[key_index];
    const inner_key = columns[key_index + 1];
    if (outer_key.map_info == null or inner_key.map_info == null) return null;
    if (outer_key.path.len != 3 or inner_key.path.len != 5) return null;
    if (!std.mem.eql(u8, outer_key.path[1], "key_value") or !std.mem.eql(u8, outer_key.path[2], "key")) return null;
    if (!std.mem.eql(u8, inner_key.path[0], outer_key.path[0])) return null;
    if (!std.mem.eql(u8, inner_key.path[1], outer_key.path[1])) return null;
    if (!std.mem.eql(u8, inner_key.path[2], "value")) return null;
    if (!std.mem.eql(u8, inner_key.path[3], "key_value") or !std.mem.eql(u8, inner_key.path[4], "key")) return null;

    if (key_index + 2 < columns.len) {
        const inner_value = columns[key_index + 2];
        if (inner_value.map_info != null and inner_value.path.len == 5 and
            std.mem.eql(u8, inner_value.path[0], inner_key.path[0]) and
            std.mem.eql(u8, inner_value.path[1], inner_key.path[1]) and
            std.mem.eql(u8, inner_value.path[2], inner_key.path[2]) and
            std.mem.eql(u8, inner_value.path[3], inner_key.path[3]) and
            std.mem.eql(u8, inner_value.path[4], "value"))
        {
            return 3;
        }
    }
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

fn writeSchemaMapColumn(cw: *CompactWriter, key_column: types.Column, value_column: ?types.Column) !void {
    const map_info = key_column.map_info orelse return error.InvalidSchema;
    if (key_column.path.len != 3) return error.InvalidSchema;
    const map_repetition: types.Repetition = if (map_info.map_definition_level > 0) .optional else .required;

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(map_repetition));
    try cw.fieldString(4, key_column.path[0]);
    try cw.fieldI32(5, 1);
    try cw.fieldI32(6, @intFromEnum(types.ConvertedType.map));
    try cw.fieldStructBegin(10);
    try cw.fieldStructBegin(2);
    try cw.endStruct();
    try cw.endStruct();
    try cw.endStruct();

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(types.Repetition.repeated));
    try cw.fieldString(4, key_column.path[1]);
    try cw.fieldI32(5, if (value_column == null) 1 else 2);
    try cw.endStruct();

    try writeSchemaLeaf(cw, key_column.path[2], .required, key_column.column_type);
    if (value_column) |value| {
        const value_repetition: types.Repetition = if (value.max_definition_level > map_info.map_definition_level + 1) .optional else .required;
        try writeSchemaLeaf(cw, value.path[2], value_repetition, value.column_type);
    }
}

fn writeSchemaListMapColumn(cw: *CompactWriter, columns: []const types.Column) !void {
    if (columns.len != 1 and columns.len != 2) return error.InvalidSchema;
    const key_column = columns[0];
    const value_column: ?types.Column = if (columns.len == 2) columns[1] else null;
    if (key_column.path.len != 5 or key_column.nested_logical_info.len != 2) return error.InvalidSchema;
    const list_info = key_column.nested_logical_info[0];
    const map_logical_info = key_column.nested_logical_info[1];
    const map_info = key_column.map_info orelse return error.InvalidSchema;
    if (list_info.kind != .list or map_logical_info.kind != .map) return error.InvalidSchema;
    const list_repetition: types.Repetition = if (list_info.optional) .optional else .required;
    const map_repetition: types.Repetition = if (map_logical_info.optional) .optional else .required;

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(list_repetition));
    try cw.fieldString(4, key_column.path[0]);
    try cw.fieldI32(5, 1);
    try cw.fieldI32(6, @intFromEnum(types.ConvertedType.list));
    try cw.fieldStructBegin(10);
    try cw.fieldStructBegin(3);
    try cw.endStruct();
    try cw.endStruct();
    try cw.endStruct();

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(types.Repetition.repeated));
    try cw.fieldString(4, key_column.path[1]);
    try cw.fieldI32(5, 1);
    try cw.endStruct();

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(map_repetition));
    try cw.fieldString(4, key_column.path[2]);
    try cw.fieldI32(5, 1);
    try cw.fieldI32(6, @intFromEnum(types.ConvertedType.map));
    try cw.fieldStructBegin(10);
    try cw.fieldStructBegin(2);
    try cw.endStruct();
    try cw.endStruct();
    try cw.endStruct();

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(types.Repetition.repeated));
    try cw.fieldString(4, key_column.path[3]);
    try cw.fieldI32(5, if (value_column == null) 1 else 2);
    try cw.endStruct();

    try writeSchemaLeaf(cw, key_column.path[4], .required, key_column.column_type);
    if (value_column) |value| {
        const value_repetition: types.Repetition = if (value.max_definition_level > map_info.map_definition_level + 1) .optional else .required;
        try writeSchemaLeaf(cw, value.path[4], value_repetition, value.column_type);
    }
}

fn writeSchemaNestedMapColumn(cw: *CompactWriter, columns: []const types.Column) !void {
    if (columns.len != 2 and columns.len != 3) return error.InvalidSchema;
    const outer_key = columns[0];
    const inner_key = columns[1];
    const inner_value: ?types.Column = if (columns.len == 3) columns[2] else null;
    const outer_info = outer_key.map_info orelse return error.InvalidSchema;
    const inner_info = inner_key.map_info orelse return error.InvalidSchema;
    if (outer_key.path.len != 3 or inner_key.path.len != 5) return error.InvalidSchema;
    const outer_repetition: types.Repetition = if (outer_info.map_definition_level > 0) .optional else .required;
    const inner_repetition: types.Repetition = if (inner_info.map_definition_level > outer_key.max_definition_level) .optional else .required;

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(outer_repetition));
    try cw.fieldString(4, outer_key.path[0]);
    try cw.fieldI32(5, 1);
    try cw.fieldI32(6, @intFromEnum(types.ConvertedType.map));
    try cw.fieldStructBegin(10);
    try cw.fieldStructBegin(2);
    try cw.endStruct();
    try cw.endStruct();
    try cw.endStruct();

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(types.Repetition.repeated));
    try cw.fieldString(4, outer_key.path[1]);
    try cw.fieldI32(5, 2);
    try cw.endStruct();

    try writeSchemaLeaf(cw, outer_key.path[2], .required, outer_key.column_type);

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(inner_repetition));
    try cw.fieldString(4, inner_key.path[2]);
    try cw.fieldI32(5, 1);
    try cw.fieldI32(6, @intFromEnum(types.ConvertedType.map));
    try cw.fieldStructBegin(10);
    try cw.fieldStructBegin(2);
    try cw.endStruct();
    try cw.endStruct();
    try cw.endStruct();

    try cw.beginStruct();
    try cw.fieldI32(3, @intFromEnum(types.Repetition.repeated));
    try cw.fieldString(4, inner_key.path[3]);
    try cw.fieldI32(5, if (inner_value == null) 1 else 2);
    try cw.endStruct();

    try writeSchemaLeaf(cw, inner_key.path[4], .required, inner_key.column_type);
    if (inner_value) |value| {
        const value_repetition: types.Repetition = if (value.max_definition_level > inner_info.map_definition_level + 1) .optional else .required;
        try writeSchemaLeaf(cw, value.path[4], value_repetition, value.column_type);
    }
}

fn writeSchemaListColumn(cw: *CompactWriter, column: types.Column) !void {
    const list_info = column.list_info orelse return error.InvalidSchema;
    const list_count = schemaListCount(column);
    if (list_count == 0 or column.path.len != list_count * 2 + 1) return error.InvalidSchema;

    var list_index: usize = 0;
    while (list_index < list_count) : (list_index += 1) {
        const list_repetition: types.Repetition = if (schemaListOptional(column, list_index)) .optional else .required;

        try cw.beginStruct();
        try cw.fieldI32(3, @intFromEnum(list_repetition));
        try cw.fieldString(4, column.path[list_index * 2]);
        try cw.fieldI32(5, 1);
        try cw.fieldI32(6, @intFromEnum(types.ConvertedType.list));
        try cw.fieldStructBegin(10);
        try cw.fieldStructBegin(3);
        try cw.endStruct();
        try cw.endStruct();
        try cw.endStruct();

        try cw.beginStruct();
        try cw.fieldI32(3, @intFromEnum(types.Repetition.repeated));
        try cw.fieldString(4, column.path[list_index * 2 + 1]);
        try cw.fieldI32(5, 1);
        try cw.endStruct();
    }

    const element_repetition: types.Repetition = if (column.max_definition_level > list_info.list_definition_level + 1) .optional else .required;
    try writeSchemaLeaf(cw, column.path[list_count * 2], element_repetition, column.column_type);
}

fn schemaListElementCount(column: types.Column) usize {
    return schemaListCount(column) * 2 + 1;
}

fn schemaListCount(column: types.Column) usize {
    var count: usize = 0;
    for (column.nested_logical_info) |logical_info| {
        if (logical_info.kind == .list) count += 1;
    }
    if (count == 0 and column.list_info != null) return 1;
    return count;
}

fn schemaListOptional(column: types.Column, list_index: usize) bool {
    var seen: usize = 0;
    for (column.nested_logical_info) |logical_info| {
        if (logical_info.kind != .list) continue;
        if (seen == list_index) return logical_info.optional;
        seen += 1;
    }
    const list_info = column.list_info orelse return false;
    return list_index == 0 and list_info.list_definition_level > 0;
}

fn writeSchemaLeaf(cw: *CompactWriter, name: []const u8, repetition: types.Repetition, column_type: types.ColumnType) !void {
    try cw.beginStruct();
    try cw.fieldI32(1, @intFromEnum(column_type.physical));
    if (column_type.type_length) |len| try cw.fieldI32(2, len);
    try cw.fieldI32(3, @intFromEnum(repetition));
    try cw.fieldString(4, name);
    switch (column_type.logical) {
        .none => {},
        .string => {
            try cw.fieldI32(6, @intFromEnum(types.ConvertedType.utf8));
            try cw.fieldStructBegin(10);
            try cw.fieldStructBegin(1);
            try cw.endStruct();
            try cw.endStruct();
        },
        .decimal => {
            const precision = column_type.decimal_precision orelse return error.InvalidSchema;
            const scale = column_type.decimal_scale orelse 0;
            try cw.fieldI32(6, @intFromEnum(types.ConvertedType.decimal));
            try cw.fieldI32(7, scale);
            try cw.fieldI32(8, precision);
            try cw.fieldStructBegin(10);
            try cw.fieldStructBegin(5);
            try cw.fieldI32(1, scale);
            try cw.fieldI32(2, precision);
            try cw.endStruct();
            try cw.endStruct();
        },
        .date => {
            try cw.fieldI32(6, @intFromEnum(types.ConvertedType.date));
            try cw.fieldStructBegin(10);
            try cw.fieldStructBegin(6);
            try cw.endStruct();
            try cw.endStruct();
        },
        .timestamp_millis, .timestamp_micros, .timestamp_nanos => |logical| {
            switch (logical) {
                .timestamp_millis => try cw.fieldI32(6, @intFromEnum(types.ConvertedType.timestamp_millis)),
                .timestamp_micros => try cw.fieldI32(6, @intFromEnum(types.ConvertedType.timestamp_micros)),
                .timestamp_nanos => {},
                else => unreachable,
            }
            try cw.fieldStructBegin(10);
            try cw.fieldStructBegin(8);
            try cw.fieldBool(1, true);
            try cw.fieldStructBegin(2);
            try cw.fieldStructBegin(switch (logical) {
                .timestamp_millis => 1,
                .timestamp_micros => 2,
                .timestamp_nanos => 3,
                else => unreachable,
            });
            try cw.endStruct();
            try cw.endStruct();
            try cw.endStruct();
            try cw.endStruct();
        },
    }
    try cw.endStruct();
}

fn writeRowGroup(cw: *CompactWriter, row_group: types.RowGroup) !void {
    try cw.beginStruct();
    try cw.fieldListBegin(1, .struct_, row_group.columns.len);
    for (row_group.columns) |column| try writeColumnChunk(cw, column);
    try cw.fieldI64(2, row_group.total_byte_size);
    try cw.fieldI64(3, row_group.num_rows);
    try cw.fieldI64(6, row_group.total_compressed_size);
    try cw.endStruct();
}

fn writeColumnChunk(cw: *CompactWriter, column: types.ColumnChunkMeta) !void {
    try cw.beginStruct();
    try cw.fieldI64(2, 0);
    try cw.fieldStructBegin(3);
    try cw.fieldI32(1, @intFromEnum(column.physical_type));

    try cw.fieldListBegin(2, .i32, column.encodings.len);
    for (column.encodings) |encoding| try cw.writeEnum(encoding);

    try cw.fieldListBegin(3, .binary, 1);
    try cw.writeBinary(column.path);

    try cw.fieldI32(4, @intFromEnum(column.codec));
    try cw.fieldI64(5, column.num_values);
    try cw.fieldI64(6, column.total_uncompressed_size);
    try cw.fieldI64(7, column.total_compressed_size);
    try cw.fieldI64(9, column.data_page_offset);
    if (column.dictionary_page_offset) |offset| try cw.fieldI64(11, offset);
    try writeStatistics(cw, 12, column.statistics);
    try cw.endStruct();
    if (column.offset_index_offset) |offset| try cw.fieldI64(4, offset);
    if (column.offset_index_length) |length| try cw.fieldI32(5, length);
    if (column.column_index_offset) |offset| try cw.fieldI64(6, offset);
    if (column.column_index_length) |length| try cw.fieldI32(7, length);
    try cw.endStruct();
}

pub fn writeOffsetIndex(writer: *std.Io.Writer, entries: []const types.PageIndexEntry) !void {
    var cw = CompactWriter.init(writer);
    try cw.beginStruct();
    try cw.fieldListBegin(1, .struct_, entries.len);
    for (entries) |entry| {
        try cw.beginStruct();
        try cw.fieldI64(1, entry.offset);
        try cw.fieldI32(2, entry.compressed_page_size);
        try cw.fieldI64(3, entry.first_row_index);
        try cw.endStruct();
    }
    try cw.endStruct();
}

pub fn writeColumnIndex(writer: *std.Io.Writer, entries: []const types.PageIndexEntry) !void {
    var cw = CompactWriter.init(writer);
    try cw.beginStruct();

    try cw.fieldListBegin(1, .boolean_true, entries.len);
    for (entries) |entry| try cw.writeBoolValue(isNullPage(entry));

    try cw.fieldListBegin(2, .binary, entries.len);
    for (entries) |entry| {
        if (isNullPage(entry)) {
            try cw.writeBinary("");
        } else if (entry.statistics.min_value) |min_value| {
            try cw.writeBinary(min_value);
        } else {
            return error.InvalidColumnData;
        }
    }

    try cw.fieldListBegin(3, .binary, entries.len);
    for (entries) |entry| {
        if (isNullPage(entry)) {
            try cw.writeBinary("");
        } else if (entry.statistics.max_value) |max_value| {
            try cw.writeBinary(max_value);
        } else {
            return error.InvalidColumnData;
        }
    }

    try cw.fieldI32(4, @intFromEnum(types.BoundaryOrder.unordered));

    try cw.fieldListBegin(5, .i64, entries.len);
    for (entries) |entry| try cw.writeI64(entry.statistics.null_count orelse return error.InvalidColumnData);

    try cw.endStruct();
}

fn isNullPage(entry: types.PageIndexEntry) bool {
    const null_count = entry.statistics.null_count orelse return false;
    const value_count = if (entry.value_count > 0) entry.value_count else entry.row_count;
    return null_count == value_count;
}

pub fn readOffsetIndex(allocator: std.mem.Allocator, bytes: []const u8) ![]types.PageIndexEntry {
    var fixed = std.Io.Reader.fixed(bytes);
    var cr = CompactReader.init(&fixed);
    var entries: []types.PageIndexEntry = &.{};

    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => {
                freePageIndexEntries(allocator, entries);
                entries = try readPageLocations(allocator, &cr);
            },
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return entries;
}

pub fn readColumnIndexInto(allocator: std.mem.Allocator, bytes: []const u8, entries: []types.PageIndexEntry) !void {
    if (entries.len == 0) return;

    var fixed = std.Io.Reader.fixed(bytes);
    var cr = CompactReader.init(&fixed);
    var null_pages: []bool = &.{};
    var min_values: [][]u8 = &.{};
    var max_values: [][]u8 = &.{};
    var null_counts: []i64 = &.{};
    var transferred_stats = false;
    errdefer if (!transferred_stats) {
        allocator.free(null_pages);
        freeBinaryList(allocator, min_values);
        freeBinaryList(allocator, max_values);
        allocator.free(null_counts);
    };

    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => {
                allocator.free(null_pages);
                null_pages = try readBoolList(allocator, &cr);
            },
            2 => {
                freeBinaryList(allocator, min_values);
                min_values = try readBinaryList(allocator, &cr);
            },
            3 => {
                freeBinaryList(allocator, max_values);
                max_values = try readBinaryList(allocator, &cr);
            },
            4 => _ = try cr.readEnum(types.BoundaryOrder),
            5 => {
                allocator.free(null_counts);
                null_counts = try readI64List(allocator, &cr);
            },
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();

    if (null_pages.len != entries.len or null_counts.len != entries.len) return error.CorruptMetadata;
    const has_min_max = min_values.len != 0 or max_values.len != 0;
    if (has_min_max and (min_values.len != entries.len or max_values.len != entries.len)) return error.CorruptMetadata;

    for (entries, 0..) |*entry, idx| {
        if (null_counts[idx] < 0 or null_counts[idx] > entry.row_count) return error.CorruptMetadata;
        entry.statistics.null_count = null_counts[idx];
        if (has_min_max and !null_pages[idx]) {
            entry.statistics.min_value = min_values[idx];
            entry.statistics.max_value = max_values[idx];
            min_values[idx] = &.{};
            max_values[idx] = &.{};
        }
    }

    transferred_stats = true;
    allocator.free(null_pages);
    freeBinaryList(allocator, min_values);
    freeBinaryList(allocator, max_values);
    allocator.free(null_counts);
}

pub fn freePageIndexEntries(allocator: std.mem.Allocator, entries: []types.PageIndexEntry) void {
    for (entries) |entry| freeStatistics(allocator, entry.statistics);
    if (entries.len > 0) allocator.free(entries);
}

fn readPageLocations(allocator: std.mem.Allocator, cr: *CompactReader) ![]types.PageIndexEntry {
    const hdr = try cr.readListHeader();
    if (hdr.elem_type != .struct_) return error.CorruptMetadata;

    const entries = try allocator.alloc(types.PageIndexEntry, hdr.len);
    errdefer allocator.free(entries);
    for (entries) |*entry| entry.* = try readPageLocation(cr);
    return entries;
}

fn readPageLocation(cr: *CompactReader) !types.PageIndexEntry {
    var entry: types.PageIndexEntry = .{
        .offset = -1,
        .compressed_page_size = -1,
        .first_row_index = -1,
        .row_count = 0,
    };

    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => entry.offset = try cr.readI64(),
            2 => entry.compressed_page_size = try cr.readI32(),
            3 => entry.first_row_index = try cr.readI64(),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();

    if (entry.offset < 0 or entry.compressed_page_size <= 0 or entry.first_row_index < 0) return error.CorruptMetadata;
    return entry;
}

fn readBoolList(allocator: std.mem.Allocator, cr: *CompactReader) ![]bool {
    const hdr = try cr.readListHeader();
    if (hdr.elem_type != .boolean_true and hdr.elem_type != .boolean_false) return error.CorruptMetadata;

    const values = try allocator.alloc(bool, hdr.len);
    errdefer allocator.free(values);
    for (values) |*value| value.* = try cr.readBoolValue();
    return values;
}

fn readI64List(allocator: std.mem.Allocator, cr: *CompactReader) ![]i64 {
    const hdr = try cr.readListHeader();
    if (hdr.elem_type != .i64) return error.CorruptMetadata;

    const values = try allocator.alloc(i64, hdr.len);
    errdefer allocator.free(values);
    for (values) |*value| value.* = try cr.readI64();
    return values;
}

fn readBinaryList(allocator: std.mem.Allocator, cr: *CompactReader) ![][]u8 {
    const hdr = try cr.readListHeader();
    if (hdr.elem_type != .binary) return error.CorruptMetadata;

    const values = try allocator.alloc([]u8, hdr.len);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |value| allocator.free(value);
        allocator.free(values);
    }

    while (initialized < values.len) : (initialized += 1) {
        values[initialized] = try cr.readBinaryAlloc(allocator);
    }
    return values;
}

fn freeBinaryList(allocator: std.mem.Allocator, values: [][]u8) void {
    for (values) |value| {
        if (value.len > 0) allocator.free(value);
    }
    if (values.len > 0) allocator.free(values);
}

pub fn readFileMetaData(allocator: std.mem.Allocator, bytes: []const u8) !types.FileMetaData {
    var fixed = std.Io.Reader.fixed(bytes);
    var cr = CompactReader.init(&fixed);
    var version: i32 = 0;
    var schema: ?types.Schema = null;
    var num_rows: i64 = 0;
    var row_groups: []types.RowGroup = &.{};
    var created_by: []const u8 = "";

    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => version = try cr.readI32(),
            2 => schema = try readSchema(allocator, &cr),
            3 => num_rows = try cr.readI64(),
            4 => row_groups = try readRowGroups(allocator, &cr),
            6 => created_by = try cr.readBinaryAlloc(allocator),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();

    return .{
        .version = version,
        .schema = schema orelse return error.CorruptMetadata,
        .num_rows = num_rows,
        .row_groups = row_groups,
        .created_by = created_by,
    };
}

fn readSchema(allocator: std.mem.Allocator, cr: *CompactReader) !types.Schema {
    const hdr = try cr.readListHeader();
    if (hdr.elem_type != .struct_ or hdr.len == 0) return error.CorruptMetadata;

    const elements = try allocator.alloc(SchemaElement, hdr.len);
    errdefer allocator.free(elements);
    var idx: usize = 0;
    while (idx < hdr.len) : (idx += 1) {
        elements[idx] = try readSchemaElement(allocator, cr);
    }

    const root = elements[0];
    if (root.physical_type != null) return error.CorruptMetadata;
    const root_children = root.num_children orelse return error.CorruptMetadata;
    if (root_children < 0) return error.CorruptMetadata;

    var columns: std.ArrayList(types.Column) = .empty;
    errdefer columns.deinit(allocator);
    var path_stack: std.ArrayList([]const u8) = .empty;
    defer path_stack.deinit(allocator);
    var repeated_stack: std.ArrayList(types.RepeatedLevelInfo) = .empty;
    defer repeated_stack.deinit(allocator);
    var logical_stack: std.ArrayList(types.NestedLogicalInfo) = .empty;
    defer logical_stack.deinit(allocator);

    var element_index: usize = 1;
    var child: i32 = 0;
    while (child < root_children) : (child += 1) {
        try collectSchemaLeaves(allocator, elements, &element_index, 0, 0, null, null, &path_stack, &repeated_stack, &logical_stack, &columns);
    }
    if (element_index != elements.len) return error.CorruptMetadata;

    return .{ .name = root.name, .columns = try columns.toOwnedSlice(allocator) };
}

fn collectSchemaLeaves(
    allocator: std.mem.Allocator,
    elements: []const SchemaElement,
    element_index: *usize,
    parent_definition_level: u16,
    parent_repetition_level: u16,
    parent_list_info: ?types.ListInfo,
    parent_map_info: ?types.MapInfo,
    path_stack: *std.ArrayList([]const u8),
    repeated_stack: *std.ArrayList(types.RepeatedLevelInfo),
    logical_stack: *std.ArrayList(types.NestedLogicalInfo),
    columns: *std.ArrayList(types.Column),
) !void {
    if (element_index.* >= elements.len) return error.CorruptMetadata;
    const element = elements[element_index.*];
    element_index.* += 1;
    try path_stack.append(allocator, element.name);
    defer _ = path_stack.pop();

    const repetition = element.repetition orelse return error.CorruptMetadata;
    var definition_level = parent_definition_level;
    var repetition_level = parent_repetition_level;
    switch (repetition) {
        .required => {},
        .optional => definition_level = std.math.add(u16, definition_level, 1) catch return error.CorruptMetadata,
        .repeated => {
            definition_level = std.math.add(u16, definition_level, 1) catch return error.CorruptMetadata;
            repetition_level = std.math.add(u16, repetition_level, 1) catch return error.CorruptMetadata;
        },
    }
    const pushed_repeated = repetition == .repeated;
    if (pushed_repeated) {
        try repeated_stack.append(allocator, .{
            .repetition_level = repetition_level,
            .path = try allocator.dupe([]const u8, path_stack.items),
        });
    }
    defer {
        if (pushed_repeated) _ = repeated_stack.pop();
    }

    const list_info = if (element.is_list)
        types.ListInfo{ .list_definition_level = definition_level }
    else
        parent_list_info;
    const map_info = if (element.is_map)
        types.MapInfo{ .map_definition_level = definition_level }
    else
        parent_map_info;
    const pushed_logical = element.is_list or element.is_map;
    if (pushed_logical) {
        try logical_stack.append(allocator, .{
            .kind = if (element.is_list) .list else .map,
            .definition_level = definition_level,
            .repetition_level = repetition_level,
            .path = try allocator.dupe([]const u8, path_stack.items),
            .optional = repetition == .optional,
        });
    }
    defer {
        if (pushed_logical) _ = logical_stack.pop();
    }

    if (element.physical_type) |physical| {
        if (element.num_children != null) return error.CorruptMetadata;
        try columns.append(allocator, .{
            .name = element.name,
            .path = try allocator.dupe([]const u8, path_stack.items),
            .column_type = .{
                .physical = physical,
                .logical = element.logical,
                .type_length = element.type_length,
                .decimal_precision = element.decimal_precision,
                .decimal_scale = element.decimal_scale,
            },
            .repetition = if (repetition_level > 0) .repeated else if (definition_level > 0) .optional else .required,
            .max_definition_level = definition_level,
            .max_repetition_level = repetition_level,
            .repeated_level_info = try allocator.dupe(types.RepeatedLevelInfo, repeated_stack.items),
            .nested_logical_info = try allocator.dupe(types.NestedLogicalInfo, logical_stack.items),
            .list_info = list_info,
            .map_info = map_info,
        });
        return;
    }

    const children = element.num_children orelse return error.CorruptMetadata;
    if (children < 0) return error.CorruptMetadata;
    var child: i32 = 0;
    while (child < children) : (child += 1) {
        try collectSchemaLeaves(allocator, elements, element_index, definition_level, repetition_level, list_info, map_info, path_stack, repeated_stack, logical_stack, columns);
    }
}

const SchemaElement = struct {
    physical_type: ?types.Type = null,
    type_length: ?i32 = null,
    repetition: ?types.Repetition = null,
    name: []const u8 = "",
    num_children: ?i32 = null,
    logical: types.LogicalType = .none,
    decimal_precision: ?i32 = null,
    decimal_scale: ?i32 = null,
    is_list: bool = false,
    is_map: bool = false,
};

fn readSchemaElement(allocator: std.mem.Allocator, cr: *CompactReader) !SchemaElement {
    var out: SchemaElement = .{};
    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => out.physical_type = try cr.readEnum(types.Type),
            2 => out.type_length = try cr.readI32(),
            3 => out.repetition = try cr.readEnum(types.Repetition),
            4 => out.name = try cr.readBinaryAlloc(allocator),
            5 => out.num_children = try cr.readI32(),
            6 => {
                const converted = try cr.readEnum(types.ConvertedType);
                out.logical = switch (converted) {
                    .utf8 => .string,
                    .decimal => .decimal,
                    .date => .date,
                    .timestamp_millis => .timestamp_millis,
                    .timestamp_micros => .timestamp_micros,
                    else => out.logical,
                };
                if (converted == .list) out.is_list = true;
                if (converted == .map) out.is_map = true;
            },
            7 => out.decimal_scale = try cr.readI32(),
            8 => out.decimal_precision = try cr.readI32(),
            10 => {
                const logical = try readLogicalType(cr);
                out.logical = logical.logical;
                if (logical.decimal_precision) |precision| out.decimal_precision = precision;
                if (logical.decimal_scale) |scale| out.decimal_scale = scale;
                if (logical.is_list) out.is_list = true;
                if (logical.is_map) out.is_map = true;
            },
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

const LogicalTypeRead = struct {
    logical: types.LogicalType = .none,
    decimal_precision: ?i32 = null,
    decimal_scale: ?i32 = null,
    is_list: bool = false,
    is_map: bool = false,
};

fn readLogicalType(cr: *CompactReader) !LogicalTypeRead {
    var out: LogicalTypeRead = .{};
    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => {
                try cr.skip(field.compact_type);
                out.logical = .string;
            },
            5 => {
                const decimal = try readDecimalType(cr);
                out.logical = .decimal;
                out.decimal_precision = decimal.precision;
                out.decimal_scale = decimal.scale;
            },
            3 => {
                try cr.skip(field.compact_type);
                out.is_list = true;
            },
            2 => {
                try cr.skip(field.compact_type);
                out.is_map = true;
            },
            6 => {
                try cr.skip(field.compact_type);
                out.logical = .date;
            },
            8 => out.logical = try readTimestampType(cr),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

const DecimalTypeRead = struct {
    scale: i32,
    precision: i32,
};

fn readDecimalType(cr: *CompactReader) !DecimalTypeRead {
    var out: DecimalTypeRead = .{ .scale = 0, .precision = 0 };
    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => out.scale = try cr.readI32(),
            2 => out.precision = try cr.readI32(),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn readTimestampType(cr: *CompactReader) !types.LogicalType {
    var out: types.LogicalType = .timestamp_micros;
    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => _ = field.boolValue() orelse return error.CorruptMetadata,
            2 => out = try readTimeUnit(cr),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn readTimeUnit(cr: *CompactReader) !types.LogicalType {
    var out: types.LogicalType = .timestamp_micros;
    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => {
                try cr.skip(field.compact_type);
                out = .timestamp_millis;
            },
            2 => {
                try cr.skip(field.compact_type);
                out = .timestamp_micros;
            },
            3 => {
                try cr.skip(field.compact_type);
                out = .timestamp_nanos;
            },
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn readRowGroups(allocator: std.mem.Allocator, cr: *CompactReader) ![]types.RowGroup {
    const hdr = try cr.readListHeader();
    if (hdr.elem_type != .struct_) return error.CorruptMetadata;
    const row_groups = try allocator.alloc(types.RowGroup, hdr.len);
    errdefer allocator.free(row_groups);
    for (row_groups) |*rg| rg.* = try readRowGroup(allocator, cr);
    return row_groups;
}

fn readRowGroup(allocator: std.mem.Allocator, cr: *CompactReader) !types.RowGroup {
    var out: types.RowGroup = .{
        .columns = &.{},
        .total_byte_size = 0,
        .total_compressed_size = 0,
        .num_rows = 0,
    };

    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => out.columns = try readColumnChunks(allocator, cr),
            2 => out.total_byte_size = try cr.readI64(),
            3 => out.num_rows = try cr.readI64(),
            6 => out.total_compressed_size = try cr.readI64(),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn readColumnChunks(allocator: std.mem.Allocator, cr: *CompactReader) ![]types.ColumnChunkMeta {
    const hdr = try cr.readListHeader();
    if (hdr.elem_type != .struct_) return error.CorruptMetadata;
    const chunks = try allocator.alloc(types.ColumnChunkMeta, hdr.len);
    errdefer allocator.free(chunks);
    for (chunks) |*chunk| chunk.* = try readColumnChunk(allocator, cr);
    return chunks;
}

fn readColumnChunk(allocator: std.mem.Allocator, cr: *CompactReader) !types.ColumnChunkMeta {
    var out: types.ColumnChunkMeta = .{
        .physical_type = .int32,
        .encodings = &.{},
        .path = "",
        .codec = .uncompressed,
        .num_values = 0,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .dictionary_page_offset = null,
    };

    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            3 => {
                const offset_index_offset = out.offset_index_offset;
                const offset_index_length = out.offset_index_length;
                const column_index_offset = out.column_index_offset;
                const column_index_length = out.column_index_length;
                out = try readColumnMetaData(allocator, cr);
                out.offset_index_offset = offset_index_offset;
                out.offset_index_length = offset_index_length;
                out.column_index_offset = column_index_offset;
                out.column_index_length = column_index_length;
            },
            4 => out.offset_index_offset = try cr.readI64(),
            5 => out.offset_index_length = try cr.readI32(),
            6 => out.column_index_offset = try cr.readI64(),
            7 => out.column_index_length = try cr.readI32(),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn readColumnMetaData(allocator: std.mem.Allocator, cr: *CompactReader) !types.ColumnChunkMeta {
    var out: types.ColumnChunkMeta = .{
        .physical_type = .int32,
        .encodings = &.{},
        .path = "",
        .codec = .uncompressed,
        .num_values = 0,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .dictionary_page_offset = null,
    };

    try cr.beginStruct();
    while (try cr.nextField()) |field| {
        switch (field.id) {
            1 => out.physical_type = try cr.readEnum(types.Type),
            2 => out.encodings = try readEncodingList(allocator, cr),
            3 => out.path = try readPathList(allocator, cr),
            4 => out.codec = try cr.readEnum(types.CompressionCodec),
            5 => out.num_values = try cr.readI64(),
            6 => out.total_uncompressed_size = try cr.readI64(),
            7 => out.total_compressed_size = try cr.readI64(),
            9 => out.data_page_offset = try cr.readI64(),
            11 => out.dictionary_page_offset = try cr.readI64(),
            12 => out.statistics = try readStatisticsAlloc(allocator, cr),
            else => try cr.skip(field.compact_type),
        }
    }
    cr.endStruct();
    return out;
}

fn readEncodingList(allocator: std.mem.Allocator, cr: *CompactReader) ![]types.Encoding {
    const hdr = try cr.readListHeader();
    if (hdr.elem_type != .i32) return error.CorruptMetadata;
    const values = try allocator.alloc(types.Encoding, hdr.len);
    errdefer allocator.free(values);
    for (values) |*value| value.* = try cr.readEnum(types.Encoding);
    return values;
}

fn readPathList(allocator: std.mem.Allocator, cr: *CompactReader) ![]const u8 {
    const hdr = try cr.readListHeader();
    if (hdr.elem_type != .binary or hdr.len == 0) return error.CorruptMetadata;
    var parts = try allocator.alloc([]const u8, hdr.len);
    errdefer allocator.free(parts);
    var total_len: usize = 0;
    var i: usize = 0;
    while (i < hdr.len) : (i += 1) {
        parts[i] = try cr.readBinaryAlloc(allocator);
        total_len = std.math.add(usize, total_len, parts[i].len) catch return error.CorruptMetadata;
        if (i > 0) total_len = std.math.add(usize, total_len, 1) catch return error.CorruptMetadata;
    }
    if (hdr.len == 1) {
        const only = parts[0];
        allocator.free(parts);
        return only;
    }

    const out = try allocator.alloc(u8, total_len);
    var offset: usize = 0;
    for (parts, 0..) |part, part_index| {
        if (part_index > 0) {
            out[offset] = '.';
            offset += 1;
        }
        @memcpy(out[offset..][0..part.len], part);
        offset += part.len;
    }
    allocator.free(parts);
    return out;
}

fn encodeZigZag(comptime T: type, value: T) u64 {
    const bits = @typeInfo(T).int.bits;
    const Wide = if (bits <= 32) i64 else i128;
    const wide: Wide = value;
    return @intCast((wide << 1) ^ (wide >> @intCast(bits - 1)));
}

fn decodeZigZag(comptime T: type, raw: u64) !T {
    const decoded_u = (raw >> 1) ^ (0 -% (raw & 1));
    const signed: i64 = @bitCast(decoded_u);
    return std.math.cast(T, signed) orelse error.CorruptMetadata;
}

test "compact protocol round-trips page header" {
    const testing = std.testing;
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try writePageHeader(&out.writer, .{
        .page_type = .data_page,
        .uncompressed_page_size = 42,
        .compressed_page_size = 42,
        .data_page_header = .{
            .num_values = 7,
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = .{ .null_count = 1 },
        },
    });

    var fixed = std.Io.Reader.fixed(out.written());
    const header = try readPageHeader(&fixed);
    try testing.expectEqual(types.PageType.data_page, header.page_type);
    try testing.expectEqual(@as(i32, 42), header.uncompressed_page_size);
    try testing.expectEqual(@as(i32, 7), header.data_page_header.?.num_values);
    try testing.expectEqual(@as(i64, 1), header.data_page_header.?.statistics.null_count.?);
}

test "compact protocol round-trips page indexes" {
    const testing = std.testing;
    const min_a = [_]u8{ 1, 0, 0, 0 };
    const max_a = [_]u8{ 9, 0, 0, 0 };
    const min_b = [_]u8{ 10, 0, 0, 0 };
    const max_b = [_]u8{ 19, 0, 0, 0 };
    const source = [_]types.PageIndexEntry{
        .{
            .offset = 100,
            .compressed_page_size = 64,
            .first_row_index = 0,
            .row_count = 10,
            .statistics = .{ .null_count = 0, .min_value = min_a[0..], .max_value = max_a[0..] },
        },
        .{
            .offset = 164,
            .compressed_page_size = 72,
            .first_row_index = 10,
            .row_count = 10,
            .statistics = .{ .null_count = 1, .min_value = min_b[0..], .max_value = max_b[0..] },
        },
    };

    var offset_bytes: std.Io.Writer.Allocating = .init(testing.allocator);
    defer offset_bytes.deinit();
    try writeOffsetIndex(&offset_bytes.writer, source[0..]);
    const parsed = try readOffsetIndex(testing.allocator, offset_bytes.written());
    defer freePageIndexEntries(testing.allocator, parsed);
    try testing.expectEqual(@as(usize, 2), parsed.len);
    try testing.expectEqual(@as(i64, 100), parsed[0].offset);
    try testing.expectEqual(@as(i32, 72), parsed[1].compressed_page_size);

    parsed[0].row_count = 10;
    parsed[1].row_count = 10;
    var column_bytes: std.Io.Writer.Allocating = .init(testing.allocator);
    defer column_bytes.deinit();
    try writeColumnIndex(&column_bytes.writer, source[0..]);
    try readColumnIndexInto(testing.allocator, column_bytes.written(), parsed);
    try testing.expectEqual(@as(i64, 1), parsed[1].statistics.null_count.?);
    try testing.expectEqualSlices(u8, min_b[0..], parsed[1].statistics.min_value.?);
    try testing.expectEqualSlices(u8, max_b[0..], parsed[1].statistics.max_value.?);
}

test "compact reader rejects invalid compact type tags" {
    var fixed = std.Io.Reader.fixed(&[_]u8{0x0f});
    try std.testing.expectError(error.CorruptMetadata, readPageHeader(&fixed));
}
