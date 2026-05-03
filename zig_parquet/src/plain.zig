const std = @import("std");
const types = @import("types.zig");

pub fn encodeDefinitionLevels(allocator: std.mem.Allocator, writer: *std.Io.Writer, validity: []const bool) !void {
    var body: std.Io.Writer.Allocating = .init(allocator);
    defer body.deinit();

    try encodeDefinitionLevelsBody(&body.writer, validity);

    try writer.writeInt(u32, @intCast(body.written().len), .little);
    try writer.writeAll(body.written());
}

pub fn encodeDefinitionLevelsBody(writer: *std.Io.Writer, validity: []const bool) !void {
    var i: usize = 0;
    while (i < validity.len) {
        const value = validity[i];
        var run_len: usize = 1;
        while (i + run_len < validity.len and validity[i + run_len] == value) : (run_len += 1) {}
        try writeVarUint(writer, run_len << 1);
        try writer.writeByte(if (value) 1 else 0);
        i += run_len;
    }
}

pub fn decodeDefinitionLevels(allocator: std.mem.Allocator, data: []const u8, value_count: usize) !struct { levels: []bool, consumed: usize } {
    if (data.len < 4) return error.CorruptPage;
    const len = std.mem.readInt(u32, data[0..4], .little);
    const end = std.math.add(usize, 4, @as(usize, len)) catch return error.CorruptPage;
    if (end > data.len) return error.CorruptPage;
    const out = try decodeDefinitionLevelsBody(allocator, data[4..end], value_count);
    return .{ .levels = out, .consumed = end };
}

pub fn decodeDefinitionLevelsBody(allocator: std.mem.Allocator, data: []const u8, value_count: usize) ![]bool {
    var out = try allocator.alloc(bool, value_count);
    errdefer allocator.free(out);
    var idx: usize = 0;
    var pos: usize = 0;
    const end = data.len;
    while (pos < end and idx < value_count) {
        const header = try readVarUint(data, &pos, end);
        if ((header & 1) == 0) {
            const run_len = std.math.cast(usize, header >> 1) orelse return error.CorruptPage;
            if (pos >= end) return error.CorruptPage;
            const bit = data[pos] & 1;
            pos += 1;
            if (run_len > value_count - idx) return error.CorruptPage;
            @memset(out[idx..][0..run_len], bit != 0);
            idx += run_len;
        } else {
            const group_count = std.math.cast(usize, header >> 1) orelse return error.CorruptPage;
            const total = std.math.mul(usize, group_count, 8) catch return error.CorruptPage;
            var j: usize = 0;
            while (j < total and idx < value_count) : (j += 1) {
                if (pos >= end) return error.CorruptPage;
                const byte = data[pos];
                const bit = (byte >> @intCast(j & 7)) & 1;
                out[idx] = bit != 0;
                idx += 1;
                if ((j & 7) == 7) pos += 1;
            }
            if ((j & 7) != 0) pos += 1;
        }
    }
    if (idx != value_count or pos != end) return error.CorruptPage;
    return out;
}

pub fn encodeValues(writer: *std.Io.Writer, data: types.ColumnData, column_type: types.ColumnType) !void {
    const fixed_width = switch (data) {
        .fixed_len_byte_array => try types.physicalTypeWidth(column_type.physical, column_type.type_length),
        else => 0,
    };

    switch (data) {
        .boolean => |d| try encodeBooleans(writer, d.values),
        .int32 => |d| for (d.values) |value| try writer.writeInt(i32, value, .little),
        .int64 => |d| for (d.values) |value| try writer.writeInt(i64, value, .little),
        .float => |d| for (d.values) |value| try writer.writeInt(u32, @bitCast(value), .little),
        .double => |d| for (d.values) |value| try writer.writeInt(u64, @bitCast(value), .little),
        .byte_array => |d| for (d.values) |value| {
            try writer.writeInt(u32, @intCast(value.len), .little);
            try writer.writeAll(value);
        },
        .fixed_len_byte_array => |d| for (d.values) |value| {
            if (value.len != fixed_width) return error.InvalidColumnData;
            try writer.writeAll(value);
        },
    }
}

pub fn encodeByteStreamSplitValues(writer: *std.Io.Writer, data: types.ColumnData, column_type: types.ColumnType) !void {
    switch (data) {
        .int32 => |d| try encodeByteStreamSplitInts(i32, writer, d.values),
        .int64 => |d| try encodeByteStreamSplitInts(i64, writer, d.values),
        .float => |d| try encodeByteStreamSplitFloats(f32, writer, d.values),
        .double => |d| try encodeByteStreamSplitFloats(f64, writer, d.values),
        .fixed_len_byte_array => |d| try encodeByteStreamSplitFixedBytes(writer, d.values, try types.physicalTypeWidth(column_type.physical, column_type.type_length)),
        else => return error.UnsupportedEncoding,
    }
}

pub fn decodeValues(
    allocator: std.mem.Allocator,
    column_type: types.ColumnType,
    row_count: usize,
    non_null_count: usize,
    validity: ?[]bool,
    data: []const u8,
) !types.OwnedColumn {
    _ = row_count;
    return switch (column_type.physical) {
        .boolean => .{ .boolean = try decodeBooleans(allocator, non_null_count, validity, data) },
        .int32 => .{ .int32 = try decodeFixed(i32, allocator, non_null_count, validity, data) },
        .int64 => .{ .int64 = try decodeFixed(i64, allocator, non_null_count, validity, data) },
        .float => .{ .float = try decodeFloats(allocator, non_null_count, validity, data) },
        .double => .{ .double = try decodeDoubles(allocator, non_null_count, validity, data) },
        .byte_array => .{ .byte_array = try decodeByteArrays(allocator, non_null_count, validity, data) },
        .fixed_len_byte_array => .{ .fixed_len_byte_array = try decodeFixedLenByteArrays(allocator, non_null_count, validity, data, try types.physicalTypeWidth(column_type.physical, column_type.type_length)) },
        else => error.UnsupportedType,
    };
}

pub fn decodeByteStreamSplitValues(
    allocator: std.mem.Allocator,
    column_type: types.ColumnType,
    row_count: usize,
    non_null_count: usize,
    validity: ?[]bool,
    data: []const u8,
) !types.OwnedColumn {
    _ = row_count;
    return switch (column_type.physical) {
        .int32 => .{ .int32 = try decodeByteStreamSplitInts(i32, allocator, non_null_count, validity, data) },
        .int64 => .{ .int64 = try decodeByteStreamSplitInts(i64, allocator, non_null_count, validity, data) },
        .float => .{ .float = try decodeByteStreamSplitFloats(f32, allocator, non_null_count, validity, data) },
        .double => .{ .double = try decodeByteStreamSplitFloats(f64, allocator, non_null_count, validity, data) },
        .fixed_len_byte_array => .{ .fixed_len_byte_array = try decodeByteStreamSplitFixedLenByteArrays(allocator, non_null_count, validity, data, try types.physicalTypeWidth(column_type.physical, column_type.type_length)) },
        else => error.UnsupportedEncoding,
    };
}

pub fn decodeDeltaBinaryPackedValues(
    allocator: std.mem.Allocator,
    column_type: types.ColumnType,
    row_count: usize,
    non_null_count: usize,
    validity: ?[]bool,
    data: []const u8,
) !types.OwnedColumn {
    _ = row_count;
    return switch (column_type.physical) {
        .int32 => .{ .int32 = try decodeDeltaBinaryPackedInts(i32, allocator, non_null_count, validity, data) },
        .int64 => .{ .int64 = try decodeDeltaBinaryPackedInts(i64, allocator, non_null_count, validity, data) },
        else => error.UnsupportedEncoding,
    };
}

pub fn decodeDeltaLengthByteArrayValues(
    allocator: std.mem.Allocator,
    column_type: types.ColumnType,
    row_count: usize,
    non_null_count: usize,
    validity: ?[]bool,
    data: []const u8,
) !types.OwnedColumn {
    _ = row_count;
    return switch (column_type.physical) {
        .byte_array => .{ .byte_array = try decodeDeltaLengthByteArrays(allocator, non_null_count, validity, data) },
        else => error.UnsupportedEncoding,
    };
}

pub fn decodeDeltaByteArrayValues(
    allocator: std.mem.Allocator,
    column_type: types.ColumnType,
    row_count: usize,
    non_null_count: usize,
    validity: ?[]bool,
    data: []const u8,
) !types.OwnedColumn {
    _ = row_count;
    return switch (column_type.physical) {
        .byte_array => .{ .byte_array = try decodeDeltaByteArrays(allocator, non_null_count, validity, data, null) },
        .fixed_len_byte_array => .{ .fixed_len_byte_array = try decodeDeltaByteArrays(allocator, non_null_count, validity, data, try types.physicalTypeWidth(column_type.physical, column_type.type_length)) },
        else => error.UnsupportedEncoding,
    };
}

pub fn encodeRleBitPackedUint32(writer: *std.Io.Writer, values: []const u32, bit_width: u8) !void {
    if (bit_width > 32) return error.InvalidColumnData;
    if (bit_width == 0) return;

    const width_bytes = (@as(usize, bit_width) + 7) / 8;
    var i: usize = 0;
    while (i < values.len) {
        const value = values[i];
        if (bit_width < 32 and value >= (@as(u32, 1) << @intCast(bit_width))) return error.InvalidColumnData;

        var run_len: usize = 1;
        while (i + run_len < values.len and values[i + run_len] == value) : (run_len += 1) {}
        if (run_len > std.math.maxInt(usize) / 2) return error.RowCountOverflow;
        try writeVarUint(writer, run_len << 1);

        var b: usize = 0;
        while (b < width_bytes) : (b += 1) {
            try writer.writeByte(@intCast((value >> @intCast(8 * b)) & 0xff));
        }
        i += run_len;
    }
}

pub fn decodeRleBitPackedUint32(allocator: std.mem.Allocator, data: []const u8, bit_width: u8, count: usize) ![]u32 {
    if (bit_width > 32) return error.CorruptPage;
    const values = try allocator.alloc(u32, count);
    errdefer allocator.free(values);
    if (bit_width == 0) {
        @memset(values, 0);
        return values;
    }

    const width_bytes = (@as(usize, bit_width) + 7) / 8;
    var pos: usize = 0;
    var idx: usize = 0;
    while (pos < data.len and idx < count) {
        const header = try readVarUint(data, &pos, data.len);
        if ((header & 1) == 0) {
            const run_len = std.math.cast(usize, header >> 1) orelse return error.CorruptPage;
            if (width_bytes > data.len - pos) return error.CorruptPage;
            var value: u32 = 0;
            var b: usize = 0;
            while (b < width_bytes) : (b += 1) value |= @as(u32, data[pos + b]) << @intCast(8 * b);
            pos += width_bytes;
            if (run_len > count - idx) return error.CorruptPage;
            @memset(values[idx..][0..run_len], value);
            idx += run_len;
        } else {
            const group_count = std.math.cast(usize, header >> 1) orelse return error.CorruptPage;
            const total = std.math.mul(usize, group_count, 8) catch return error.CorruptPage;
            var bit_pos: usize = 0;
            const bit_len = std.math.mul(usize, total, @as(usize, bit_width)) catch return error.CorruptPage;
            const byte_len = try ceilDiv8(bit_len);
            if (byte_len > data.len - pos) return error.CorruptPage;
            var j: usize = 0;
            while (j < total and idx < count) : (j += 1) {
                values[idx] = readPackedValue(data[pos..][0..byte_len], bit_pos, bit_width);
                bit_pos += bit_width;
                idx += 1;
            }
            pos += byte_len;
        }
    }
    if (idx != count) return error.CorruptPage;
    return values;
}

pub fn materializeDictionary(
    allocator: std.mem.Allocator,
    physical_type: types.Type,
    dictionary: *const types.OwnedColumn,
    indexes: []const u32,
    validity: ?[]bool,
) !types.OwnedColumn {
    return switch (physical_type) {
        .boolean => switch (dictionary.*) {
            .boolean => |dict| blk: {
                const values = try allocator.alloc(bool, indexes.len);
                errdefer allocator.free(values);
                for (indexes, values) |idx, *value| {
                    if (idx >= dict.values.len) return error.CorruptPage;
                    value.* = dict.values[idx];
                }
                break :blk .{ .boolean = .{ .values = values, .validity = validity } };
            },
            else => error.CorruptPage,
        },
        .int32 => switch (dictionary.*) {
            .int32 => |dict| blk: {
                const values = try allocator.alloc(i32, indexes.len);
                errdefer allocator.free(values);
                for (indexes, values) |idx, *value| {
                    if (idx >= dict.values.len) return error.CorruptPage;
                    value.* = dict.values[idx];
                }
                break :blk .{ .int32 = .{ .values = values, .validity = validity } };
            },
            else => error.CorruptPage,
        },
        .int64 => switch (dictionary.*) {
            .int64 => |dict| blk: {
                const values = try allocator.alloc(i64, indexes.len);
                errdefer allocator.free(values);
                for (indexes, values) |idx, *value| {
                    if (idx >= dict.values.len) return error.CorruptPage;
                    value.* = dict.values[idx];
                }
                break :blk .{ .int64 = .{ .values = values, .validity = validity } };
            },
            else => error.CorruptPage,
        },
        .float => switch (dictionary.*) {
            .float => |dict| blk: {
                const values = try allocator.alloc(f32, indexes.len);
                errdefer allocator.free(values);
                for (indexes, values) |idx, *value| {
                    if (idx >= dict.values.len) return error.CorruptPage;
                    value.* = dict.values[idx];
                }
                break :blk .{ .float = .{ .values = values, .validity = validity } };
            },
            else => error.CorruptPage,
        },
        .double => switch (dictionary.*) {
            .double => |dict| blk: {
                const values = try allocator.alloc(f64, indexes.len);
                errdefer allocator.free(values);
                for (indexes, values) |idx, *value| {
                    if (idx >= dict.values.len) return error.CorruptPage;
                    value.* = dict.values[idx];
                }
                break :blk .{ .double = .{ .values = values, .validity = validity } };
            },
            else => error.CorruptPage,
        },
        .byte_array => switch (dictionary.*) {
            .byte_array => |dict| blk: {
                var total: usize = 0;
                for (indexes) |idx| {
                    if (idx >= dict.values.len) return error.CorruptPage;
                    total += dict.values[idx].len;
                }
                const bytes = try allocator.alloc(u8, total);
                errdefer allocator.free(bytes);
                const values = try allocator.alloc([]const u8, indexes.len);
                errdefer allocator.free(values);
                var pos: usize = 0;
                for (indexes, values) |idx, *value| {
                    const src = dict.values[idx];
                    @memcpy(bytes[pos..][0..src.len], src);
                    value.* = bytes[pos..][0..src.len];
                    pos += src.len;
                }
                break :blk .{ .byte_array = .{ .values = values, .data = bytes, .validity = validity } };
            },
            else => error.CorruptPage,
        },
        .fixed_len_byte_array => switch (dictionary.*) {
            .fixed_len_byte_array => |dict| blk: {
                var total: usize = 0;
                for (indexes) |idx| {
                    if (idx >= dict.values.len) return error.CorruptPage;
                    total += dict.values[idx].len;
                }
                const bytes = try allocator.alloc(u8, total);
                errdefer allocator.free(bytes);
                const values = try allocator.alloc([]const u8, indexes.len);
                errdefer allocator.free(values);
                var pos: usize = 0;
                for (indexes, values) |idx, *value| {
                    const src = dict.values[idx];
                    @memcpy(bytes[pos..][0..src.len], src);
                    value.* = bytes[pos..][0..src.len];
                    pos += src.len;
                }
                break :blk .{ .fixed_len_byte_array = .{ .values = values, .data = bytes, .validity = validity } };
            },
            else => error.CorruptPage,
        },
        else => error.UnsupportedType,
    };
}

fn encodeBooleans(writer: *std.Io.Writer, values: []const bool) !void {
    var i: usize = 0;
    while (i < values.len) {
        var byte: u8 = 0;
        var bit: u3 = 0;
        while (bit < 8 and i < values.len) : ({
            bit += 1;
            i += 1;
        }) {
            if (values[i]) byte |= (@as(u8, 1) << bit);
        }
        try writer.writeByte(byte);
    }
}

fn decodeBooleans(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !types.OwnedBool {
    const byte_count = try ceilDiv8(count);
    if (data.len < byte_count) return error.CorruptPage;
    var values = try allocator.alloc(bool, count);
    errdefer allocator.free(values);
    var i: usize = 0;
    while (i < count) : (i += 1) {
        values[i] = ((data[i / 8] >> @intCast(i & 7)) & 1) != 0;
    }
    return .{ .values = values, .validity = validity };
}

fn decodeFixed(comptime T: type, allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !switch (T) {
    i32 => types.OwnedInt32,
    i64 => types.OwnedInt64,
    else => unreachable,
} {
    const bytes = std.math.mul(usize, @sizeOf(T), count) catch return error.CorruptPage;
    if (data.len < bytes) return error.CorruptPage;
    const values = try allocator.alloc(T, count);
    errdefer allocator.free(values);
    for (values, 0..) |*value, i| value.* = std.mem.readInt(T, data[i * @sizeOf(T) ..][0..@sizeOf(T)], .little);
    return .{ .values = values, .validity = validity };
}

fn decodeFloats(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !types.OwnedFloat {
    const bytes = std.math.mul(usize, 4, count) catch return error.CorruptPage;
    if (data.len < bytes) return error.CorruptPage;
    const values = try allocator.alloc(f32, count);
    errdefer allocator.free(values);
    for (values, 0..) |*value, i| value.* = @bitCast(std.mem.readInt(u32, data[i * 4 ..][0..4], .little));
    return .{ .values = values, .validity = validity };
}

fn decodeDoubles(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !types.OwnedDouble {
    const bytes = std.math.mul(usize, 8, count) catch return error.CorruptPage;
    if (data.len < bytes) return error.CorruptPage;
    const values = try allocator.alloc(f64, count);
    errdefer allocator.free(values);
    for (values, 0..) |*value, i| value.* = @bitCast(std.mem.readInt(u64, data[i * 8 ..][0..8], .little));
    return .{ .values = values, .validity = validity };
}

fn encodeByteStreamSplitInts(comptime T: type, writer: *std.Io.Writer, values: []const T) !void {
    const U = std.meta.Int(.unsigned, @bitSizeOf(T));
    var byte_idx: usize = 0;
    while (byte_idx < @sizeOf(T)) : (byte_idx += 1) {
        for (values) |value| {
            const raw: U = @bitCast(value);
            try writer.writeByte(@intCast((raw >> @intCast(byte_idx * 8)) & 0xff));
        }
    }
}

fn encodeByteStreamSplitFloats(comptime T: type, writer: *std.Io.Writer, values: []const T) !void {
    const U = std.meta.Int(.unsigned, @bitSizeOf(T));
    var byte_idx: usize = 0;
    while (byte_idx < @sizeOf(T)) : (byte_idx += 1) {
        for (values) |value| {
            const raw: U = @bitCast(value);
            try writer.writeByte(@intCast((raw >> @intCast(byte_idx * 8)) & 0xff));
        }
    }
}

fn encodeByteStreamSplitFixedBytes(writer: *std.Io.Writer, values: []const []const u8, width: usize) !void {
    var byte_idx: usize = 0;
    while (byte_idx < width) : (byte_idx += 1) {
        for (values) |value| {
            if (value.len != width) return error.InvalidColumnData;
            try writer.writeByte(value[byte_idx]);
        }
    }
}

fn decodeByteStreamSplitInts(comptime T: type, allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !switch (T) {
    i32 => types.OwnedInt32,
    i64 => types.OwnedInt64,
    else => unreachable,
} {
    const width = @sizeOf(T);
    const bytes = std.math.mul(usize, width, count) catch return error.CorruptPage;
    if (data.len < bytes) return error.CorruptPage;
    const values = try allocator.alloc(T, count);
    errdefer allocator.free(values);
    for (values, 0..) |*value, idx| {
        var raw: [width]u8 = undefined;
        for (&raw, 0..) |*byte, byte_idx| byte.* = data[byte_idx * count + idx];
        value.* = std.mem.readInt(T, raw[0..width], .little);
    }
    return .{ .values = values, .validity = validity };
}

fn decodeByteStreamSplitFloats(comptime T: type, allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !switch (T) {
    f32 => types.OwnedFloat,
    f64 => types.OwnedDouble,
    else => unreachable,
} {
    const U = std.meta.Int(.unsigned, @bitSizeOf(T));
    const width = @sizeOf(T);
    const bytes = std.math.mul(usize, width, count) catch return error.CorruptPage;
    if (data.len < bytes) return error.CorruptPage;
    const values = try allocator.alloc(T, count);
    errdefer allocator.free(values);
    for (values, 0..) |*value, idx| {
        var raw: [width]u8 = undefined;
        for (&raw, 0..) |*byte, byte_idx| byte.* = data[byte_idx * count + idx];
        value.* = @bitCast(std.mem.readInt(U, raw[0..width], .little));
    }
    return .{ .values = values, .validity = validity };
}

fn decodeByteStreamSplitFixedLenByteArrays(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8, width: usize) !types.OwnedByteArray {
    const total = std.math.mul(usize, count, width) catch return error.CorruptPage;
    if (data.len < total) return error.CorruptPage;
    const bytes = try allocator.alloc(u8, total);
    errdefer allocator.free(bytes);
    var idx: usize = 0;
    while (idx < count) : (idx += 1) {
        var byte_idx: usize = 0;
        while (byte_idx < width) : (byte_idx += 1) {
            bytes[idx * width + byte_idx] = data[byte_idx * count + idx];
        }
    }

    const values = try allocator.alloc([]const u8, count);
    errdefer allocator.free(values);
    for (values, 0..) |*value, i| {
        const start = i * width;
        value.* = bytes[start..][0..width];
    }
    return .{ .values = values, .data = bytes, .validity = validity };
}

fn decodeDeltaBinaryPackedInts(comptime T: type, allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !switch (T) {
    i32 => types.OwnedInt32,
    i64 => types.OwnedInt64,
    else => unreachable,
} {
    const decoded = try decodeDeltaBinaryPackedIntSlice(T, allocator, count, data);
    errdefer allocator.free(decoded.values);
    if (decoded.consumed != data.len) return error.CorruptPage;
    return .{ .values = decoded.values, .validity = validity };
}

fn decodeDeltaBinaryPackedIntSlice(comptime T: type, allocator: std.mem.Allocator, count: usize, data: []const u8) !struct { values: []T, consumed: usize } {
    const values = try allocator.alloc(T, count);
    errdefer allocator.free(values);
    if (count == 0) {
        if (data.len != 0) return error.CorruptPage;
        return .{ .values = values, .consumed = 0 };
    }

    var pos: usize = 0;
    const block_size = std.math.cast(usize, try readVarUint(data, &pos, data.len)) orelse return error.CorruptPage;
    const miniblocks_per_block = std.math.cast(usize, try readVarUint(data, &pos, data.len)) orelse return error.CorruptPage;
    const total_value_count = std.math.cast(usize, try readVarUint(data, &pos, data.len)) orelse return error.CorruptPage;
    if (block_size == 0 or block_size % 128 != 0) return error.CorruptPage;
    if (miniblocks_per_block == 0 or block_size % miniblocks_per_block != 0) return error.CorruptPage;
    const miniblock_values = block_size / miniblocks_per_block;
    if (miniblock_values == 0 or miniblock_values % 32 != 0) return error.CorruptPage;
    if (total_value_count != count) return error.CorruptPage;

    var previous = try readZigZagInt(i64, data, &pos);
    values[0] = std.math.cast(T, previous) orelse return error.CorruptPage;
    var value_idx: usize = 1;
    while (value_idx < count) {
        const min_delta = try readZigZagInt(i64, data, &pos);
        if (data.len - pos < miniblocks_per_block) return error.CorruptPage;
        const bit_widths = data[pos..][0..miniblocks_per_block];
        pos += miniblocks_per_block;

        for (bit_widths) |bit_width| {
            if (bit_width > 64) return error.CorruptPage;
            const bits = std.math.mul(usize, miniblock_values, bit_width) catch return error.CorruptPage;
            const bytes = try ceilDiv8(bits);
            if (bytes > data.len - pos) return error.CorruptPage;
            const packed_bytes = data[pos..][0..bytes];
            pos += bytes;

            var mini_idx: usize = 0;
            while (mini_idx < miniblock_values and value_idx < count) : (mini_idx += 1) {
                const raw_delta = readPackedValue64(packed_bytes, mini_idx * @as(usize, bit_width), bit_width);
                const adjusted = std.math.cast(i64, raw_delta) orelse return error.CorruptPage;
                const delta = std.math.add(i64, min_delta, adjusted) catch return error.CorruptPage;
                previous = std.math.add(i64, previous, delta) catch return error.CorruptPage;
                values[value_idx] = std.math.cast(T, previous) orelse return error.CorruptPage;
                value_idx += 1;
            }
        }
    }

    return .{ .values = values, .consumed = pos };
}

const ByteRange = struct {
    start: usize,
    len: usize,
};

fn decodeDeltaLengthByteArrays(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !types.OwnedByteArray {
    const decoded = try decodeDeltaLengthByteArraysInternal(allocator, count, validity, data);
    if (decoded.consumed != data.len) {
        var column = decoded.column;
        column.deinit(allocator);
        return error.CorruptPage;
    }
    return decoded.column;
}

fn decodeDeltaLengthByteArraysInternal(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !struct { column: types.OwnedByteArray, consumed: usize } {
    const lengths = try decodeDeltaBinaryPackedIntSlice(i32, allocator, count, data);
    defer allocator.free(lengths.values);

    var total: usize = 0;
    for (lengths.values) |len| {
        if (len < 0) return error.CorruptPage;
        total = std.math.add(usize, total, @as(usize, @intCast(len))) catch return error.CorruptPage;
    }

    const data_start = lengths.consumed;
    const data_end = std.math.add(usize, data_start, total) catch return error.CorruptPage;
    if (data_end > data.len) return error.CorruptPage;

    const bytes = try allocator.alloc(u8, total);
    errdefer allocator.free(bytes);
    @memcpy(bytes, data[data_start..data_end]);

    const values = try allocator.alloc([]const u8, count);
    errdefer allocator.free(values);
    var pos: usize = 0;
    for (values, lengths.values) |*value, len| {
        const width: usize = @intCast(len);
        value.* = bytes[pos..][0..width];
        pos += width;
    }

    return .{
        .column = .{ .values = values, .data = bytes, .validity = validity },
        .consumed = data_end,
    };
}

fn decodeDeltaByteArrays(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8, fixed_width: ?usize) !types.OwnedByteArray {
    const prefixes = try decodeDeltaBinaryPackedIntSlice(i32, allocator, count, data);
    defer allocator.free(prefixes.values);

    const suffixes = try decodeDeltaLengthByteArraysInternal(allocator, count, null, data[prefixes.consumed..]);
    var suffix_column = suffixes.column;
    defer suffix_column.deinit(allocator);
    if (prefixes.consumed + suffixes.consumed != data.len) return error.CorruptPage;

    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(allocator);
    const ranges = try allocator.alloc(ByteRange, count);
    defer allocator.free(ranges);

    var previous: []const u8 = "";
    for (prefixes.values, suffix_column.values, ranges) |prefix_len_i32, suffix, *range| {
        if (prefix_len_i32 < 0) return error.CorruptPage;
        const prefix_len: usize = @intCast(prefix_len_i32);
        if (prefix_len > previous.len) return error.CorruptPage;
        const start = bytes.items.len;
        try bytes.appendSlice(allocator, previous[0..prefix_len]);
        try bytes.appendSlice(allocator, suffix);
        const width = bytes.items.len - start;
        if (fixed_width) |expected| {
            if (width != expected) return error.CorruptPage;
        }
        range.* = .{ .start = start, .len = width };
        previous = bytes.items[start..][0..width];
    }

    const owned_bytes = try bytes.toOwnedSlice(allocator);
    errdefer allocator.free(owned_bytes);
    const values = try allocator.alloc([]const u8, count);
    errdefer allocator.free(values);
    for (ranges, values) |range, *value| {
        value.* = owned_bytes[range.start..][0..range.len];
    }

    return .{ .values = values, .data = owned_bytes, .validity = validity };
}

fn decodeByteArrays(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !types.OwnedByteArray {
    const values = try allocator.alloc([]const u8, count);
    errdefer allocator.free(values);

    var pos: usize = 0;
    var total: usize = 0;
    for (values) |*value| {
        if (4 > data.len - pos) return error.CorruptPage;
        const len = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        const end = std.math.add(usize, pos, @as(usize, len)) catch return error.CorruptPage;
        if (end > data.len) return error.CorruptPage;
        value.* = data[pos..end];
        total = std.math.add(usize, total, len) catch return error.CorruptPage;
        pos = end;
    }

    var bytes = try allocator.alloc(u8, total);
    errdefer allocator.free(bytes);
    var dst: usize = 0;
    for (values) |*value| {
        const len = value.len;
        @memcpy(bytes[dst..][0..len], value.*);
        value.* = bytes[dst..][0..len];
        dst += len;
    }
    return .{ .values = values, .data = bytes, .validity = validity };
}

fn decodeFixedLenByteArrays(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8, width: usize) !types.OwnedByteArray {
    const total = std.math.mul(usize, count, width) catch return error.CorruptPage;
    if (data.len < total) return error.CorruptPage;
    const bytes = try allocator.alloc(u8, total);
    errdefer allocator.free(bytes);
    @memcpy(bytes, data[0..total]);

    const values = try allocator.alloc([]const u8, count);
    errdefer allocator.free(values);
    for (values, 0..) |*value, i| {
        const start = i * width;
        value.* = bytes[start..][0..width];
    }
    return .{ .values = values, .data = bytes, .validity = validity };
}

fn writeVarUint(writer: *std.Io.Writer, value: usize) !void {
    var v: u64 = @intCast(value);
    while (v >= 0x80) {
        try writer.writeByte(@as(u8, @intCast(v & 0x7f)) | 0x80);
        v >>= 7;
    }
    try writer.writeByte(@intCast(v));
}

fn readVarUint(data: []const u8, pos: *usize, end: usize) !u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (true) {
        if (pos.* >= end or pos.* >= data.len) return error.CorruptPage;
        const byte = data[pos.*];
        pos.* += 1;
        result |= (@as(u64, byte & 0x7f) << shift);
        if ((byte & 0x80) == 0) return result;
        if (shift >= 63) return error.CorruptPage;
        shift += 7;
    }
}

fn readPackedValue(data: []const u8, start_bit: usize, bit_width: u8) u32 {
    var value: u32 = 0;
    var bit: u8 = 0;
    while (bit < bit_width) : (bit += 1) {
        const absolute = start_bit + bit;
        const source = (data[absolute / 8] >> @intCast(absolute & 7)) & 1;
        value |= @as(u32, source) << @intCast(bit);
    }
    return value;
}

fn readPackedValue64(data: []const u8, start_bit: usize, bit_width: u8) u64 {
    var value: u64 = 0;
    var bit: u8 = 0;
    while (bit < bit_width) : (bit += 1) {
        const absolute = start_bit + bit;
        const source = (data[absolute / 8] >> @intCast(absolute & 7)) & 1;
        value |= @as(u64, source) << @intCast(bit);
    }
    return value;
}

fn ceilDiv8(value: usize) !usize {
    const adjusted = std.math.add(usize, value, 7) catch return error.CorruptPage;
    return adjusted / 8;
}

fn readZigZagInt(comptime T: type, data: []const u8, pos: *usize) !T {
    const raw = try readVarUint(data, pos, data.len);
    const decoded_u = (raw >> 1) ^ (0 -% (raw & 1));
    const signed: i64 = @bitCast(decoded_u);
    return std.math.cast(T, signed) orelse error.CorruptPage;
}

test "definition levels round-trip rle runs" {
    const testing = std.testing;
    const validity = [_]bool{ true, true, false, false, false, true };
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try encodeDefinitionLevels(testing.allocator, &out.writer, validity[0..]);
    const decoded = try decodeDefinitionLevels(testing.allocator, out.written(), validity.len);
    defer testing.allocator.free(decoded.levels);
    try testing.expectEqualSlices(bool, validity[0..], decoded.levels);
}

test "byte stream split round-trips fixed width values" {
    const testing = std.testing;
    const scores = [_]f64{ 0.25, -1.5, 1024.75, 3.125 };
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try encodeByteStreamSplitValues(&out.writer, .{ .double = .{ .values = scores[0..] } }, .{ .physical = .double });

    var decoded = try decodeByteStreamSplitValues(testing.allocator, .{ .physical = .double }, scores.len, scores.len, null, out.written());
    defer decoded.deinit(testing.allocator);
    try testing.expectEqualSlices(f64, scores[0..], decoded.double.values);

    const ids = [_]i32{ -10, 0, 42, 9000 };
    var id_out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer id_out.deinit();
    try encodeByteStreamSplitValues(&id_out.writer, .{ .int32 = .{ .values = ids[0..] } }, .{ .physical = .int32 });
    var id_decoded = try decodeByteStreamSplitValues(testing.allocator, .{ .physical = .int32 }, ids.len, ids.len, null, id_out.written());
    defer id_decoded.deinit(testing.allocator);
    try testing.expectEqualSlices(i32, ids[0..], id_decoded.int32.values);
}
