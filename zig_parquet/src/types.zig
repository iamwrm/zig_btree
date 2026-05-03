const std = @import("std");

pub const Error = error{
    InvalidParquetFile,
    InvalidSchema,
    InvalidColumnData,
    UnsupportedCompression,
    UnsupportedEncoding,
    UnsupportedNestedSchema,
    UnsupportedPageType,
    UnsupportedType,
    CorruptMetadata,
    CorruptPage,
    RowCountOverflow,
};

pub const Type = enum(i32) {
    boolean = 0,
    int32 = 1,
    int64 = 2,
    int96 = 3,
    float = 4,
    double = 5,
    byte_array = 6,
    fixed_len_byte_array = 7,
};

pub const ConvertedType = enum(i32) {
    utf8 = 0,
    map = 1,
    map_key_value = 2,
    list = 3,
    enum_ = 4,
    decimal = 5,
    date = 6,
    time_millis = 7,
    time_micros = 8,
    timestamp_millis = 9,
    timestamp_micros = 10,
    uint_8 = 11,
    uint_16 = 12,
    uint_32 = 13,
    uint_64 = 14,
    int_8 = 15,
    int_16 = 16,
    int_32 = 17,
    int_64 = 18,
    json = 19,
    bson = 20,
    interval = 21,
};

pub const Repetition = enum(i32) {
    required = 0,
    optional = 1,
    repeated = 2,
};

pub const Encoding = enum(i32) {
    plain = 0,
    plain_dictionary = 2,
    rle = 3,
    bit_packed = 4,
    delta_binary_packed = 5,
    delta_length_byte_array = 6,
    delta_byte_array = 7,
    rle_dictionary = 8,
    byte_stream_split = 9,
};

pub const CompressionCodec = enum(i32) {
    uncompressed = 0,
    snappy = 1,
    gzip = 2,
    lzo = 3,
    brotli = 4,
    lz4 = 5,
    zstd = 6,
    lz4_raw = 7,
};

pub const PageType = enum(i32) {
    data_page = 0,
    index_page = 1,
    dictionary_page = 2,
    data_page_v2 = 3,
};

pub const BoundaryOrder = enum(i32) {
    unordered = 0,
    ascending = 1,
    descending = 2,
};

pub const LogicalType = enum {
    none,
    string,
    decimal,
    date,
    timestamp_millis,
    timestamp_micros,
    timestamp_nanos,
};

pub const ColumnType = struct {
    physical: Type,
    logical: LogicalType = .none,
    type_length: ?i32 = null,
    decimal_precision: ?i32 = null,
    decimal_scale: ?i32 = null,
};

pub const Column = struct {
    name: []const u8,
    path: []const []const u8 = &.{},
    column_type: ColumnType,
    repetition: Repetition = .required,
    max_definition_level: u16 = 0,
    max_repetition_level: u16 = 0,
    repeated_level_info: []const RepeatedLevelInfo = &.{},
    nested_logical_info: []const NestedLogicalInfo = &.{},
    list_info: ?ListInfo = null,
    map_info: ?MapInfo = null,
};

pub const RepeatedLevelInfo = struct {
    repetition_level: u16,
    path: []const []const u8,
};

pub const NestedLogicalKind = enum {
    list,
    map,
};

pub const NestedLogicalInfo = struct {
    kind: NestedLogicalKind,
    definition_level: u16,
    repetition_level: u16,
    path: []const []const u8,
    optional: bool = false,
};

pub const ListInfo = struct {
    list_definition_level: u16,
};

pub const MapInfo = struct {
    map_definition_level: u16,
};

pub const Schema = struct {
    name: []const u8,
    columns: []const Column,

    pub fn init(name: []const u8, columns: []const Column) Schema {
        return .{ .name = name, .columns = columns };
    }
};

pub const BoolData = struct {
    values: []const bool,
    validity: ?[]const bool = null,
};

pub const Int32Data = struct {
    values: []const i32,
    validity: ?[]const bool = null,
};

pub const Int64Data = struct {
    values: []const i64,
    validity: ?[]const bool = null,
};

pub const FloatData = struct {
    values: []const f32,
    validity: ?[]const bool = null,
};

pub const DoubleData = struct {
    values: []const f64,
    validity: ?[]const bool = null,
};

pub const ByteArrayData = struct {
    values: []const []const u8,
    validity: ?[]const bool = null,
};

pub const ColumnData = union(enum) {
    boolean: BoolData,
    int32: Int32Data,
    int64: Int64Data,
    int96: ByteArrayData,
    float: FloatData,
    double: DoubleData,
    byte_array: ByteArrayData,
    fixed_len_byte_array: ByteArrayData,

    pub fn physicalType(self: ColumnData) Type {
        return switch (self) {
            .boolean => .boolean,
            .int32 => .int32,
            .int64 => .int64,
            .int96 => .int96,
            .float => .float,
            .double => .double,
            .byte_array => .byte_array,
            .fixed_len_byte_array => .fixed_len_byte_array,
        };
    }

    pub fn validity(self: ColumnData) ?[]const bool {
        return switch (self) {
            inline else => |d| d.validity,
        };
    }

    pub fn valueCount(self: ColumnData) usize {
        return switch (self) {
            inline else => |d| d.values.len,
        };
    }

    pub fn nonNullCount(self: ColumnData, row_count: usize) Error!usize {
        const valid = self.validity() orelse return self.valueCount();
        if (valid.len != row_count) return error.InvalidColumnData;
        var count: usize = 0;
        for (valid) |is_valid| {
            if (is_valid) count += 1;
        }
        if (count != self.valueCount()) return error.InvalidColumnData;
        return count;
    }

    pub fn validate(self: ColumnData, column: Column, row_count: usize) Error!void {
        if (column.repetition == .repeated) return error.UnsupportedNestedSchema;
        if (self.physicalType() != column.column_type.physical) return error.InvalidColumnData;

        switch (column.repetition) {
            .required => {
                if (self.validity() != null) return error.InvalidColumnData;
                if (self.valueCount() != row_count) return error.InvalidColumnData;
            },
            .optional => {
                _ = try self.nonNullCount(row_count);
            },
            .repeated => unreachable,
        }

        switch (self) {
            .int96 => |d| {
                const width = try physicalTypeWidth(column.column_type.physical, column.column_type.type_length);
                for (d.values) |value| {
                    if (value.len != width) return error.InvalidColumnData;
                }
            },
            .fixed_len_byte_array => |d| {
                const width = try physicalTypeWidth(column.column_type.physical, column.column_type.type_length);
                for (d.values) |value| {
                    if (value.len != width) return error.InvalidColumnData;
                }
            },
            else => {},
        }
    }
};

pub const Statistics = struct {
    null_count: ?i64 = null,
    min_value: ?[]const u8 = null,
    max_value: ?[]const u8 = null,

    pub fn hasMinMax(self: Statistics) bool {
        return self.min_value != null and self.max_value != null;
    }

    pub fn minPhysical(self: Statistics, column_type: ColumnType) Error!?StatisticsValue {
        const bytes = self.min_value orelse return null;
        return try decodePhysicalStatistic(column_type, bytes);
    }

    pub fn maxPhysical(self: Statistics, column_type: ColumnType) Error!?StatisticsValue {
        const bytes = self.max_value orelse return null;
        return try decodePhysicalStatistic(column_type, bytes);
    }
};

pub const ColumnTripletData = struct {
    values: ColumnData,
    definition_levels: []const u16,
    repetition_levels: []const u16,
};

pub const ColumnListData = struct {
    values: ColumnData,
    offsets: []const usize,
    validity: ?[]const bool = null,
};

pub const ColumnNestedListLevelData = struct {
    offsets: []const usize,
    validity: ?[]const bool = null,
};

pub const ColumnNestedListData = struct {
    values: ColumnData,
    levels: []const ColumnNestedListLevelData,
};

pub const ColumnMapData = struct {
    keys: ColumnData,
    values: ?ColumnData = null,
    offsets: []const usize,
    validity: ?[]const bool = null,
};

pub const ColumnNestedMapLevelData = struct {
    keys: ColumnData,
    offsets: []const usize,
    validity: ?[]const bool = null,
};

pub const ColumnNestedMapData = struct {
    levels: []const ColumnNestedMapLevelData,
    values: ?ColumnData = null,
};

pub const ColumnListMapData = struct {
    list: ColumnNestedListLevelData,
    map: ColumnNestedMapLevelData,
    values: ?ColumnData = null,
};

pub const ColumnWriteData = union(enum) {
    flat: ColumnData,
    triplets: ColumnTripletData,
    list: ColumnListData,
    nested_list: ColumnNestedListData,
    map: ColumnMapData,
    nested_map: ColumnNestedMapData,
    list_map: ColumnListMapData,
};

pub const StatisticsValue = union(enum) {
    boolean: bool,
    int32: i32,
    int64: i64,
    int96: []const u8,
    float: f32,
    double: f64,
    byte_array: []const u8,
    fixed_len_byte_array: []const u8,
};

pub const ColumnChunkMeta = struct {
    physical_type: Type,
    encodings: []const Encoding,
    path: []const u8,
    owned_path: ?[]u8 = null,
    codec: CompressionCodec,
    num_values: i64,
    total_uncompressed_size: i64,
    total_compressed_size: i64,
    data_page_offset: i64,
    dictionary_page_offset: ?i64 = null,
    statistics: Statistics = .{},
    page_index_entries: []const PageIndexEntry = &.{},
    offset_index_offset: ?i64 = null,
    offset_index_length: ?i32 = null,
    column_index_offset: ?i64 = null,
    column_index_length: ?i32 = null,
};

pub const PageIndexEntry = struct {
    offset: i64,
    compressed_page_size: i32,
    first_row_index: i64,
    row_count: i64,
    value_count: i64 = 0,
    statistics: Statistics = .{},
};

pub const RowGroup = struct {
    columns: []const ColumnChunkMeta,
    total_byte_size: i64,
    total_compressed_size: i64,
    num_rows: i64,
};

pub const FileMetaData = struct {
    version: i32 = 1,
    schema: Schema,
    num_rows: i64,
    row_groups: []const RowGroup,
    created_by: []const u8 = "zig-parquet",
};

pub const DataPageHeader = struct {
    num_values: i32,
    encoding: Encoding,
    definition_level_encoding: Encoding,
    repetition_level_encoding: Encoding,
    statistics: Statistics = .{},
};

pub const DataPageHeaderV2 = struct {
    num_values: i32,
    num_nulls: i32,
    num_rows: i32,
    encoding: Encoding,
    definition_levels_byte_length: i32,
    repetition_levels_byte_length: i32,
    is_compressed: bool = true,
    statistics: Statistics = .{},
};

pub const DictionaryPageHeader = struct {
    num_values: i32,
    encoding: Encoding,
    is_sorted: ?bool = null,
};

pub const PageHeader = struct {
    page_type: PageType,
    uncompressed_page_size: i32,
    compressed_page_size: i32,
    crc: ?i32 = null,
    data_page_header: ?DataPageHeader = null,
    data_page_header_v2: ?DataPageHeaderV2 = null,
    dictionary_page_header: ?DictionaryPageHeader = null,
};

pub const OwnedColumn = union(enum) {
    boolean: OwnedBool,
    int32: OwnedInt32,
    int64: OwnedInt64,
    int96: OwnedByteArray,
    float: OwnedFloat,
    double: OwnedDouble,
    byte_array: OwnedByteArray,
    fixed_len_byte_array: OwnedByteArray,

    pub fn deinit(self: *OwnedColumn, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .boolean => |*v| v.deinit(allocator),
            .int32 => |*v| v.deinit(allocator),
            .int64 => |*v| v.deinit(allocator),
            .int96 => |*v| v.deinit(allocator),
            .float => |*v| v.deinit(allocator),
            .double => |*v| v.deinit(allocator),
            .byte_array => |*v| v.deinit(allocator),
            .fixed_len_byte_array => |*v| v.deinit(allocator),
        }
    }
};

pub const OwnedColumnTriplets = struct {
    values: OwnedColumn,
    definition_levels: []u16,
    repetition_levels: []u16,
    row_offsets: []usize = &.{},
    value_offsets: []usize = &.{},
    repeated_level_offsets: []TripletLevelOffsets = &.{},
    max_definition_level: u16,
    max_repetition_level: u16,

    pub fn deinit(self: *OwnedColumnTriplets, allocator: std.mem.Allocator) void {
        self.values.deinit(allocator);
        allocator.free(self.definition_levels);
        allocator.free(self.repetition_levels);
        allocator.free(self.row_offsets);
        allocator.free(self.value_offsets);
        for (self.repeated_level_offsets) |level_offsets| allocator.free(level_offsets.offsets);
        allocator.free(self.repeated_level_offsets);
    }
};

pub const TripletLevelOffsets = struct {
    repetition_level: u16,
    offsets: []usize,
};

pub const NestedTripletLevel = struct {
    repetition_level: u16,
    path: []const []const u8,
    offsets: []const usize,
};

pub const OwnedNestedColumnTriplets = struct {
    triplets: OwnedColumnTriplets,
    column_path: []const []const u8,
    repeated_levels: []NestedTripletLevel,
    logical_levels: []NestedLogicalInfo,

    pub fn deinit(self: *OwnedNestedColumnTriplets, allocator: std.mem.Allocator) void {
        self.triplets.deinit(allocator);
        freeOwnedPath(allocator, self.column_path);
        for (self.repeated_levels) |level| freeOwnedPath(allocator, level.path);
        allocator.free(self.repeated_levels);
        for (self.logical_levels) |level| freeOwnedPath(allocator, level.path);
        allocator.free(self.logical_levels);
    }
};

pub const NestedLogicalColumnLevel = struct {
    kind: NestedLogicalKind,
    definition_level: u16,
    repetition_level: u16,
    path: []const []const u8,
    offsets: []usize,
    validity: ?[]bool = null,
};

pub const OwnedNestedLogicalColumn = struct {
    values: OwnedColumn,
    levels: []NestedLogicalColumnLevel,
    max_definition_level: u16,
    max_repetition_level: u16,

    pub fn deinit(self: *OwnedNestedLogicalColumn, allocator: std.mem.Allocator) void {
        self.values.deinit(allocator);
        for (self.levels) |level| {
            freeOwnedPath(allocator, level.path);
            allocator.free(level.offsets);
            if (level.validity) |validity| allocator.free(validity);
        }
        allocator.free(self.levels);
    }
};

pub const OwnedNestedMapPair = struct {
    keys: OwnedNestedLogicalColumn,
    values: ?OwnedNestedLogicalColumn = null,
    key_map_level_index: usize,
    value_map_level_index: ?usize = null,

    pub fn deinit(self: *OwnedNestedMapPair, allocator: std.mem.Allocator) void {
        self.keys.deinit(allocator);
        if (self.values) |*values| values.deinit(allocator);
    }
};

pub const OwnedListColumn = struct {
    values: OwnedColumn,
    offsets: []usize,
    validity: ?[]bool = null,
    max_definition_level: u16,
    max_repetition_level: u16,

    pub fn deinit(self: *OwnedListColumn, allocator: std.mem.Allocator) void {
        self.values.deinit(allocator);
        allocator.free(self.offsets);
        if (self.validity) |v| allocator.free(v);
    }
};

pub const OwnedMapColumn = struct {
    keys: OwnedColumn,
    values: ?OwnedColumn = null,
    offsets: []usize,
    validity: ?[]bool = null,
    max_definition_level: u16,
    max_repetition_level: u16,

    pub fn deinit(self: *OwnedMapColumn, allocator: std.mem.Allocator) void {
        self.keys.deinit(allocator);
        if (self.values) |*values| values.deinit(allocator);
        allocator.free(self.offsets);
        if (self.validity) |v| allocator.free(v);
    }
};

pub const OwnedBool = struct {
    values: []bool,
    validity: ?[]bool = null,

    pub fn deinit(self: *OwnedBool, allocator: std.mem.Allocator) void {
        allocator.free(self.values);
        if (self.validity) |v| allocator.free(v);
    }
};

pub const OwnedInt32 = struct {
    values: []i32,
    validity: ?[]bool = null,

    pub fn deinit(self: *OwnedInt32, allocator: std.mem.Allocator) void {
        allocator.free(self.values);
        if (self.validity) |v| allocator.free(v);
    }
};

pub const OwnedInt64 = struct {
    values: []i64,
    validity: ?[]bool = null,

    pub fn deinit(self: *OwnedInt64, allocator: std.mem.Allocator) void {
        allocator.free(self.values);
        if (self.validity) |v| allocator.free(v);
    }
};

pub const OwnedFloat = struct {
    values: []f32,
    validity: ?[]bool = null,

    pub fn deinit(self: *OwnedFloat, allocator: std.mem.Allocator) void {
        allocator.free(self.values);
        if (self.validity) |v| allocator.free(v);
    }
};

pub const OwnedDouble = struct {
    values: []f64,
    validity: ?[]bool = null,

    pub fn deinit(self: *OwnedDouble, allocator: std.mem.Allocator) void {
        allocator.free(self.values);
        if (self.validity) |v| allocator.free(v);
    }
};

pub const OwnedByteArray = struct {
    values: []const []const u8,
    data: []u8,
    validity: ?[]bool = null,

    pub fn deinit(self: *OwnedByteArray, allocator: std.mem.Allocator) void {
        allocator.free(self.values);
        allocator.free(self.data);
        if (self.validity) |v| allocator.free(v);
    }
};

fn freeOwnedPath(allocator: std.mem.Allocator, path: []const []const u8) void {
    for (path) |part| allocator.free(part);
    allocator.free(path);
}

pub fn physicalTypeWidth(t: Type, type_length: ?i32) Error!usize {
    return switch (t) {
        .boolean => 0,
        .int32, .float => 4,
        .int64, .double => 8,
        .int96 => 12,
        .fixed_len_byte_array => if (type_length) |len| blk: {
            if (len <= 0) return error.InvalidSchema;
            break :blk @intCast(len);
        } else error.InvalidSchema,
        .byte_array => 0,
    };
}

pub fn validateDecimalType(column_type: ColumnType) Error!void {
    if (column_type.logical != .decimal) return;
    const precision = column_type.decimal_precision orelse return error.InvalidSchema;
    const scale = column_type.decimal_scale orelse 0;
    if (precision <= 0 or scale < 0 or scale > precision) return error.InvalidSchema;

    switch (column_type.physical) {
        .int32 => if (precision > 9) return error.InvalidSchema,
        .int64 => if (precision > 18) return error.InvalidSchema,
        .byte_array => {},
        .fixed_len_byte_array => {
            const width = try physicalTypeWidth(column_type.physical, column_type.type_length);
            if (width <= decimal_precision_by_byte_width.len and precision > decimal_precision_by_byte_width[width]) return error.InvalidSchema;
        },
        else => return error.InvalidSchema,
    }
}

fn decodePhysicalStatistic(column_type: ColumnType, bytes: []const u8) Error!StatisticsValue {
    return switch (column_type.physical) {
        .boolean => blk: {
            if (bytes.len != 1 or bytes[0] > 1) return error.CorruptMetadata;
            break :blk .{ .boolean = bytes[0] != 0 };
        },
        .int32 => blk: {
            if (bytes.len != 4) return error.CorruptMetadata;
            break :blk .{ .int32 = std.mem.readInt(i32, bytes[0..4], .little) };
        },
        .int64 => blk: {
            if (bytes.len != 8) return error.CorruptMetadata;
            break :blk .{ .int64 = std.mem.readInt(i64, bytes[0..8], .little) };
        },
        .int96 => blk: {
            if (bytes.len != 12) return error.CorruptMetadata;
            break :blk .{ .int96 = bytes };
        },
        .float => blk: {
            if (bytes.len != 4) return error.CorruptMetadata;
            break :blk .{ .float = @bitCast(std.mem.readInt(u32, bytes[0..4], .little)) };
        },
        .double => blk: {
            if (bytes.len != 8) return error.CorruptMetadata;
            break :blk .{ .double = @bitCast(std.mem.readInt(u64, bytes[0..8], .little)) };
        },
        .byte_array => .{ .byte_array = bytes },
        .fixed_len_byte_array => blk: {
            const width = try physicalTypeWidth(column_type.physical, column_type.type_length);
            if (bytes.len != width) return error.CorruptMetadata;
            break :blk .{ .fixed_len_byte_array = bytes };
        },
    };
}

const decimal_precision_by_byte_width = [_]i32{
    0, // unused
    2, // 1 byte
    4, // 2 bytes
    6, // 3 bytes
    9, // 4 bytes
    11, // 5 bytes
    14, // 6 bytes
    16, // 7 bytes
    18, // 8 bytes
    21, // 9 bytes
    23, // 10 bytes
    26, // 11 bytes
    28, // 12 bytes
    31, // 13 bytes
    33, // 14 bytes
    35, // 15 bytes
    38, // 16 bytes
};

pub fn intCastI64(value: usize) Error!i64 {
    return std.math.cast(i64, value) orelse error.RowCountOverflow;
}

pub fn intCastI32(value: usize) Error!i32 {
    return std.math.cast(i32, value) orelse error.RowCountOverflow;
}

test "ColumnData validates optional value counts" {
    const testing = std.testing;
    const col: Column = .{ .name = "x", .column_type = .{ .physical = .int32 }, .repetition = .optional };
    const values = [_]i32{ 1, 2 };
    const validity = [_]bool{ true, false, true };
    try (ColumnData{ .int32 = .{ .values = values[0..], .validity = validity[0..] } }).validate(col, 3);
    try testing.expectError(error.InvalidColumnData, (ColumnData{ .int32 = .{ .values = values[0..1], .validity = validity[0..] } }).validate(col, 3));
}

test "Statistics decodes typed physical min and max values" {
    const testing = std.testing;

    const int_min = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    const int_max = [_]u8{ 42, 0, 0, 0 };
    const int_stats: Statistics = .{ .min_value = int_min[0..], .max_value = int_max[0..] };
    try testing.expect(int_stats.hasMinMax());
    try testing.expectEqual(@as(i32, -1), (try int_stats.minPhysical(.{ .physical = .int32 })).?.int32);
    try testing.expectEqual(@as(i32, 42), (try int_stats.maxPhysical(.{ .physical = .int32 })).?.int32);

    const bool_min = [_]u8{0};
    const bool_max = [_]u8{1};
    const bool_stats: Statistics = .{ .min_value = bool_min[0..], .max_value = bool_max[0..] };
    try testing.expectEqual(false, (try bool_stats.minPhysical(.{ .physical = .boolean })).?.boolean);
    try testing.expectEqual(true, (try bool_stats.maxPhysical(.{ .physical = .boolean })).?.boolean);

    const bytes_stats: Statistics = .{ .min_value = "alpha", .max_value = "zulu" };
    try testing.expectEqualStrings("alpha", (try bytes_stats.minPhysical(.{ .physical = .byte_array })).?.byte_array);
    try testing.expectEqualStrings("zulu", (try bytes_stats.maxPhysical(.{ .physical = .byte_array })).?.byte_array);

    try testing.expectError(error.CorruptMetadata, int_stats.minPhysical(.{ .physical = .fixed_len_byte_array, .type_length = 2 }));
}
