const std = @import("std");
const types = @import("types.zig");
const native_endian = @import("builtin").target.cpu.arch.endian();

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

pub fn encodeDeltaBinaryPackedValues(writer: *std.Io.Writer, data: types.ColumnData, column_type: types.ColumnType) !void {
    _ = column_type;
    switch (data) {
        .int32 => |d| try encodeDeltaBinaryPackedInts(i32, writer, d.values),
        .int64 => |d| try encodeDeltaBinaryPackedInts(i64, writer, d.values),
        else => return error.UnsupportedEncoding,
    }
}

pub fn encodeDeltaLengthByteArrayValues(allocator: std.mem.Allocator, writer: *std.Io.Writer, data: types.ColumnData, column_type: types.ColumnType) !void {
    _ = column_type;
    switch (data) {
        .byte_array => |d| try encodeDeltaLengthByteArrays(allocator, writer, d.values),
        else => return error.UnsupportedEncoding,
    }
}

pub fn encodeDeltaByteArrayValues(allocator: std.mem.Allocator, writer: *std.Io.Writer, data: types.ColumnData, column_type: types.ColumnType) !void {
    switch (data) {
        .byte_array => |d| try encodeDeltaByteArrays(allocator, writer, d.values, null),
        .fixed_len_byte_array => |d| try encodeDeltaByteArrays(allocator, writer, d.values, try types.physicalTypeWidth(column_type.physical, column_type.type_length)),
        else => return error.UnsupportedEncoding,
    }
}

pub fn encodeRleBooleanValues(allocator: std.mem.Allocator, writer: *std.Io.Writer, data: types.ColumnData, column_type: types.ColumnType) !void {
    _ = column_type;
    const values = switch (data) {
        .boolean => |d| d.values,
        else => return error.UnsupportedEncoding,
    };

    const ints = try allocator.alloc(u32, values.len);
    defer allocator.free(ints);
    for (values, ints) |value, *int_value| int_value.* = if (value) 1 else 0;
    var body: std.Io.Writer.Allocating = .init(allocator);
    defer body.deinit();
    try encodeRleBitPackedUint32(&body.writer, ints, 1);
    try writer.writeInt(u32, @intCast(body.written().len), .little);
    try writer.writeAll(body.written());
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
            const bit_len = std.math.mul(usize, total, @as(usize, bit_width)) catch return error.CorruptPage;
            const byte_len = try ceilDiv8(bit_len);
            if (byte_len > data.len - pos) return error.CorruptPage;

            const decode_count = @min(total, count - idx);
            if (!decodePackedRunFast(values[idx..][0..decode_count], data[pos..][0..byte_len], bit_width)) {
                var bit_pos: usize = 0;
                var j: usize = 0;
                while (j < total and idx < count) : (j += 1) {
                    values[idx] = readPackedValue(data[pos..][0..byte_len], bit_pos, bit_width);
                    bit_pos += bit_width;
                    idx += 1;
                }
            } else {
                idx += decode_count;
            }
            pos += byte_len;
        }
    }
    if (idx != count) return error.CorruptPage;
    return values;
}

pub fn rleBitPackedUint32Identity(data: []const u8, bit_width: u8, count: usize) !bool {
    return rleBitPackedUint32IdentityFrom(data, bit_width, count, 0);
}

pub fn rleBitPackedUint32IdentityFrom(data: []const u8, bit_width: u8, count: usize, start: usize) !bool {
    if (bit_width > 32) return error.CorruptPage;
    if (bit_width == 0) return start == 0 and count <= 1;

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
            var run_idx: usize = 0;
            while (run_idx < run_len) : (run_idx += 1) {
                const expected = std.math.cast(u32, start + idx + run_idx) orelse return false;
                if (value != expected) return false;
            }
            idx += run_len;
        } else {
            const group_count = std.math.cast(usize, header >> 1) orelse return error.CorruptPage;
            const total = std.math.mul(usize, group_count, 8) catch return error.CorruptPage;
            const bit_len = std.math.mul(usize, total, @as(usize, bit_width)) catch return error.CorruptPage;
            const byte_len = try ceilDiv8(bit_len);
            if (byte_len > data.len - pos) return error.CorruptPage;

            const decode_count = @min(total, count - idx);
            if (!packedRunIsIdentity(data[pos..][0..byte_len], bit_width, start + idx, decode_count)) return false;
            idx += decode_count;
            pos += byte_len;
        }
    }
    if (idx != count) return error.CorruptPage;
    return true;
}

fn decodePackedRunFast(dest: []u32, data: []const u8, bit_width: u8) bool {
    switch (bit_width) {
        1 => {
            for (dest, 0..) |*value, i| {
                value.* = @intCast((data[i / 8] >> @intCast(i & 7)) & 1);
            }
            return true;
        },
        8 => {
            for (dest, data[0..dest.len]) |*value, byte| value.* = byte;
            return true;
        },
        16 => {
            for (dest, 0..) |*value, i| {
                const offset = i * 2;
                value.* = @as(u32, data[offset]) |
                    (@as(u32, data[offset + 1]) << 8);
            }
            return true;
        },
        24 => {
            for (dest, 0..) |*value, i| {
                const offset = i * 3;
                value.* = @as(u32, data[offset]) |
                    (@as(u32, data[offset + 1]) << 8) |
                    (@as(u32, data[offset + 2]) << 16);
            }
            return true;
        },
        32 => {
            const bytes = dest.len * 4;
            if (native_endian == .little) {
                @memcpy(std.mem.sliceAsBytes(dest), data[0..bytes]);
            } else {
                for (dest, 0..) |*value, i| value.* = std.mem.readInt(u32, data[i * 4 ..][0..4], .little);
            }
            return true;
        },
        else => return false,
    }
}

fn packedRunIsIdentity(data: []const u8, bit_width: u8, start: usize, count: usize) bool {
    switch (bit_width) {
        1 => {
            for (0..count) |i| {
                const value = (data[i / 8] >> @intCast(i & 7)) & 1;
                const expected = std.math.cast(u32, start + i) orelse return false;
                if (value != expected) return false;
            }
            return true;
        },
        8 => {
            for (data[0..count], 0..) |value, i| {
                const expected = std.math.cast(u32, start + i) orelse return false;
                if (value != expected) return false;
            }
            return true;
        },
        16 => {
            for (0..count) |i| {
                const offset = i * 2;
                const value = @as(u32, data[offset]) |
                    (@as(u32, data[offset + 1]) << 8);
                const expected = std.math.cast(u32, start + i) orelse return false;
                if (value != expected) return false;
            }
            return true;
        },
        24 => {
            for (0..count) |i| {
                const offset = i * 3;
                const value = @as(u32, data[offset]) |
                    (@as(u32, data[offset + 1]) << 8) |
                    (@as(u32, data[offset + 2]) << 16);
                const expected = std.math.cast(u32, start + i) orelse return false;
                if (value != expected) return false;
            }
            return true;
        },
        32 => {
            for (0..count) |i| {
                const value = std.mem.readInt(u32, data[i * 4 ..][0..4], .little);
                const expected = std.math.cast(u32, start + i) orelse return false;
                if (value != expected) return false;
            }
            return true;
        },
        else => {
            var bit_pos: usize = 0;
            var i: usize = 0;
            while (i < count) : (i += 1) {
                const expected = std.math.cast(u32, start + i) orelse return false;
                if (readPackedValue(data, bit_pos, bit_width) != expected) return false;
                bit_pos += bit_width;
            }
            return true;
        },
    }
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
    if (native_endian == .little) {
        @memcpy(std.mem.sliceAsBytes(values), data[0..bytes]);
    } else {
        for (values, 0..) |*value, i| value.* = std.mem.readInt(T, data[i * @sizeOf(T) ..][0..@sizeOf(T)], .little);
    }
    return .{ .values = values, .validity = validity };
}

fn decodeFloats(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !types.OwnedFloat {
    const bytes = std.math.mul(usize, 4, count) catch return error.CorruptPage;
    if (data.len < bytes) return error.CorruptPage;
    const values = try allocator.alloc(f32, count);
    errdefer allocator.free(values);
    if (native_endian == .little) {
        @memcpy(std.mem.sliceAsBytes(values), data[0..bytes]);
    } else {
        for (values, 0..) |*value, i| value.* = @bitCast(std.mem.readInt(u32, data[i * 4 ..][0..4], .little));
    }
    return .{ .values = values, .validity = validity };
}

fn decodeDoubles(allocator: std.mem.Allocator, count: usize, validity: ?[]bool, data: []const u8) !types.OwnedDouble {
    const bytes = std.math.mul(usize, 8, count) catch return error.CorruptPage;
    if (data.len < bytes) return error.CorruptPage;
    const values = try allocator.alloc(f64, count);
    errdefer allocator.free(values);
    if (native_endian == .little) {
        @memcpy(std.mem.sliceAsBytes(values), data[0..bytes]);
    } else {
        for (values, 0..) |*value, i| value.* = @bitCast(std.mem.readInt(u64, data[i * 8 ..][0..8], .little));
    }
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

const delta_block_size = 128;
const delta_miniblocks_per_block = 4;
const delta_miniblock_values = delta_block_size / delta_miniblocks_per_block;

fn encodeDeltaBinaryPackedInts(comptime T: type, writer: *std.Io.Writer, values: []const T) !void {
    try writeVarUint(writer, delta_block_size);
    try writeVarUint(writer, delta_miniblocks_per_block);
    try writeVarUint(writer, values.len);
    if (values.len == 0) return;

    var previous: i64 = @intCast(values[0]);
    try writeZigZagInt(writer, previous);

    var value_idx: usize = 1;
    while (value_idx < values.len) {
        var deltas: [delta_block_size]i64 = undefined;
        var adjusted: [delta_block_size]u64 = undefined;
        var block_count: usize = 0;
        var min_delta: i64 = std.math.maxInt(i64);

        while (block_count < delta_block_size and value_idx < values.len) : (block_count += 1) {
            const current: i64 = @intCast(values[value_idx]);
            const delta = try checkedDelta(current, previous);
            deltas[block_count] = delta;
            min_delta = @min(min_delta, delta);
            previous = current;
            value_idx += 1;
        }

        try writeZigZagInt(writer, min_delta);

        var idx: usize = 0;
        while (idx < block_count) : (idx += 1) {
            adjusted[idx] = try adjustedDelta(deltas[idx], min_delta);
        }
        while (idx < delta_block_size) : (idx += 1) {
            adjusted[idx] = 0;
        }

        var bit_widths: [delta_miniblocks_per_block]u8 = .{0} ** delta_miniblocks_per_block;
        var mini_idx: usize = 0;
        while (mini_idx < delta_miniblocks_per_block) : (mini_idx += 1) {
            const start = mini_idx * delta_miniblock_values;
            var max_adjusted: u64 = 0;
            for (adjusted[start..][0..delta_miniblock_values]) |value| max_adjusted = @max(max_adjusted, value);
            bit_widths[mini_idx] = bitWidth64(max_adjusted);
        }
        try writer.writeAll(bit_widths[0..]);

        mini_idx = 0;
        while (mini_idx < delta_miniblocks_per_block) : (mini_idx += 1) {
            const start = mini_idx * delta_miniblock_values;
            try writePackedValues64(writer, adjusted[start..][0..delta_miniblock_values], bit_widths[mini_idx]);
        }
    }
}

fn encodeDeltaLengthByteArrays(allocator: std.mem.Allocator, writer: *std.Io.Writer, values: []const []const u8) !void {
    const lengths = try allocator.alloc(i32, values.len);
    defer allocator.free(lengths);

    for (values, lengths) |value, *length| {
        length.* = std.math.cast(i32, value.len) orelse return error.InvalidColumnData;
    }

    try encodeDeltaBinaryPackedInts(i32, writer, lengths);
    for (values) |value| try writer.writeAll(value);
}

fn encodeDeltaByteArrays(allocator: std.mem.Allocator, writer: *std.Io.Writer, values: []const []const u8, fixed_width: ?usize) !void {
    const prefix_lengths = try allocator.alloc(i32, values.len);
    defer allocator.free(prefix_lengths);
    const suffixes = try allocator.alloc([]const u8, values.len);
    defer allocator.free(suffixes);

    var previous: []const u8 = "";
    for (values, prefix_lengths, suffixes) |value, *prefix_len, *suffix| {
        if (fixed_width) |expected| {
            if (value.len != expected) return error.InvalidColumnData;
        }
        const common = commonPrefixLen(previous, value);
        prefix_len.* = std.math.cast(i32, common) orelse return error.InvalidColumnData;
        suffix.* = value[common..];
        previous = value;
    }

    try encodeDeltaBinaryPackedInts(i32, writer, prefix_lengths);
    try encodeDeltaLengthByteArrays(allocator, writer, suffixes);
}

fn commonPrefixLen(a: []const u8, b: []const u8) usize {
    const max_len = @min(a.len, b.len);
    var idx: usize = 0;
    while (idx < max_len and a[idx] == b[idx]) : (idx += 1) {}
    return idx;
}

fn checkedDelta(current: i64, previous: i64) !i64 {
    const wide = @as(i128, current) - @as(i128, previous);
    return std.math.cast(i64, wide) orelse error.InvalidColumnData;
}

fn adjustedDelta(delta: i64, min_delta: i64) !u64 {
    const wide = @as(i128, delta) - @as(i128, min_delta);
    if (wide < 0) return error.InvalidColumnData;
    return std.math.cast(u64, wide) orelse error.InvalidColumnData;
}

fn bitWidth64(value: u64) u8 {
    if (value == 0) return 0;
    return @intCast(@bitSizeOf(u64) - @clz(value));
}

fn writePackedValues64(writer: *std.Io.Writer, values: []const u64, bit_width: u8) !void {
    if (bit_width > 64) return error.InvalidColumnData;
    if (bit_width == 0) return;

    var current_byte: u8 = 0;
    var bits_in_byte: u8 = 0;
    for (values) |value| {
        if (bit_width < 64 and value >= (@as(u64, 1) << @as(u6, @intCast(bit_width)))) return error.InvalidColumnData;

        var bit: u8 = 0;
        while (bit < bit_width) : (bit += 1) {
            if (((value >> @as(u6, @intCast(bit))) & 1) != 0) {
                current_byte |= @as(u8, 1) << @as(u3, @intCast(bits_in_byte));
            }
            bits_in_byte += 1;
            if (bits_in_byte == 8) {
                try writer.writeByte(current_byte);
                current_byte = 0;
                bits_in_byte = 0;
            }
        }
    }
    if (bits_in_byte != 0) try writer.writeByte(current_byte);
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
        var pos: usize = 0;
        const block_size = std.math.cast(usize, try readVarUint(data, &pos, data.len)) orelse return error.CorruptPage;
        const miniblocks_per_block = std.math.cast(usize, try readVarUint(data, &pos, data.len)) orelse return error.CorruptPage;
        const total_value_count = std.math.cast(usize, try readVarUint(data, &pos, data.len)) orelse return error.CorruptPage;
        if (block_size == 0 or block_size % 128 != 0) return error.CorruptPage;
        if (miniblocks_per_block == 0 or block_size % miniblocks_per_block != 0) return error.CorruptPage;
        const miniblock_values = block_size / miniblocks_per_block;
        if (miniblock_values == 0 or miniblock_values % 32 != 0) return error.CorruptPage;
        if (total_value_count != 0) return error.CorruptPage;
        return .{ .values = values, .consumed = pos };
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
    try writeVarUint64(writer, @intCast(value));
}

fn writeVarUint64(writer: *std.Io.Writer, value: u64) !void {
    var v = value;
    while (v >= 0x80) {
        try writer.writeByte(@as(u8, @intCast(v & 0x7f)) | 0x80);
        v >>= 7;
    }
    try writer.writeByte(@intCast(v));
}

fn writeZigZagInt(writer: *std.Io.Writer, value: i64) !void {
    const wide = @as(i128, value);
    const encoded_wide = (wide << 1) ^ (wide >> 63);
    if (encoded_wide < 0) return error.InvalidColumnData;
    try writeVarUint64(writer, std.math.cast(u64, encoded_wide) orelse return error.InvalidColumnData);
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

test "boolean rle values round-trip" {
    const testing = std.testing;
    const values = [_]bool{ true, false, false, true, true, true, false, false, true, false, true, true, false, true, false, false, true };
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try encodeRleBooleanValues(testing.allocator, &out.writer, .{ .boolean = .{ .values = values[0..] } }, .{ .physical = .boolean });

    const payload_len = std.mem.readInt(u32, out.written()[0..4], .little);
    try testing.expectEqual(out.written().len - 4, @as(usize, payload_len));
    const decoded = try decodeRleBitPackedUint32(testing.allocator, out.written()[4..], 1, values.len);
    defer testing.allocator.free(decoded);
    for (values, decoded) |expected, actual| {
        try testing.expectEqual(expected, actual != 0);
    }
}

test "bit-packed dictionary indexes decode byte-aligned run" {
    const testing = std.testing;
    const expected = [_]u32{ 0, 1, 7, 255, 256, 1024, 4096, 65535, 3, 5, 8, 13, 21 };
    const padded = expected ++ [_]u32{ 34, 55, 89 };

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try writeVarUint(&out.writer, (2 << 1) | 1);
    for (padded) |value| try out.writer.writeInt(u16, @intCast(value), .little);

    const decoded = try decodeRleBitPackedUint32(testing.allocator, out.written(), 16, expected.len);
    defer testing.allocator.free(decoded);
    try testing.expectEqualSlices(u32, expected[0..], decoded);
    try testing.expect(!(try rleBitPackedUint32Identity(out.written(), 16, expected.len)));

    var identity_out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer identity_out.deinit();
    try writeVarUint(&identity_out.writer, (2 << 1) | 1);
    for (0..16) |value| try identity_out.writer.writeInt(u16, @intCast(value), .little);
    try testing.expect(try rleBitPackedUint32Identity(identity_out.written(), 16, 13));

    var offset_out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer offset_out.deinit();
    try writeVarUint(&offset_out.writer, (2 << 1) | 1);
    for (8..24) |value| try offset_out.writer.writeInt(u16, @intCast(value), .little);
    try testing.expect(try rleBitPackedUint32IdentityFrom(offset_out.written(), 16, 13, 8));
}

test "delta binary packed round-trips integer values" {
    const testing = std.testing;

    var ids: [260]i64 = undefined;
    for (&ids, 0..) |*value, i| {
        const base: i64 = @intCast(i);
        value.* = base * 17 - @as(i64, @intCast(i % 11)) * 3;
    }

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try encodeDeltaBinaryPackedValues(&out.writer, .{ .int64 = .{ .values = ids[0..] } }, .{ .physical = .int64 });

    var decoded = try decodeDeltaBinaryPackedValues(testing.allocator, .{ .physical = .int64 }, ids.len, ids.len, null, out.written());
    defer decoded.deinit(testing.allocator);
    try testing.expectEqualSlices(i64, ids[0..], decoded.int64.values);

    const small = [_]i32{ -15, -12, -12, 8, 1024, 17, -2048 };
    var small_out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer small_out.deinit();
    try encodeDeltaBinaryPackedValues(&small_out.writer, .{ .int32 = .{ .values = small[0..] } }, .{ .physical = .int32 });

    var small_decoded = try decodeDeltaBinaryPackedValues(testing.allocator, .{ .physical = .int32 }, small.len, small.len, null, small_out.written());
    defer small_decoded.deinit(testing.allocator);
    try testing.expectEqualSlices(i32, small[0..], small_decoded.int32.values);

    var empty_out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer empty_out.deinit();
    try encodeDeltaBinaryPackedValues(&empty_out.writer, .{ .int32 = .{ .values = &.{} } }, .{ .physical = .int32 });

    var empty_decoded = try decodeDeltaBinaryPackedValues(testing.allocator, .{ .physical = .int32 }, 0, 0, null, empty_out.written());
    defer empty_decoded.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 0), empty_decoded.int32.values.len);
}

test "delta byte array encodings round-trip byte arrays" {
    const testing = std.testing;
    const values = [_][]const u8{
        "prefix-000001-suffix",
        "prefix-000002-suffix",
        "prefix-000002-tail",
        "prefix-000120-tail",
        "other",
    };

    var length_out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer length_out.deinit();
    try encodeDeltaLengthByteArrayValues(testing.allocator, &length_out.writer, .{ .byte_array = .{ .values = values[0..] } }, .{ .physical = .byte_array });

    var length_decoded = try decodeDeltaLengthByteArrayValues(testing.allocator, .{ .physical = .byte_array }, values.len, values.len, null, length_out.written());
    defer length_decoded.deinit(testing.allocator);
    for (values, length_decoded.byte_array.values) |expected, actual| {
        try testing.expectEqualStrings(expected, actual);
    }

    var delta_out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer delta_out.deinit();
    try encodeDeltaByteArrayValues(testing.allocator, &delta_out.writer, .{ .byte_array = .{ .values = values[0..] } }, .{ .physical = .byte_array });

    var delta_decoded = try decodeDeltaByteArrayValues(testing.allocator, .{ .physical = .byte_array }, values.len, values.len, null, delta_out.written());
    defer delta_decoded.deinit(testing.allocator);
    for (values, delta_decoded.byte_array.values) |expected, actual| {
        try testing.expectEqualStrings(expected, actual);
    }

    const fixed = [_][]const u8{ "abcd", "abce", "abzz" };
    var fixed_out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer fixed_out.deinit();
    try encodeDeltaByteArrayValues(testing.allocator, &fixed_out.writer, .{ .fixed_len_byte_array = .{ .values = fixed[0..] } }, .{ .physical = .fixed_len_byte_array, .type_length = 4 });

    var fixed_decoded = try decodeDeltaByteArrayValues(testing.allocator, .{ .physical = .fixed_len_byte_array, .type_length = 4 }, fixed.len, fixed.len, null, fixed_out.written());
    defer fixed_decoded.deinit(testing.allocator);
    for (fixed, fixed_decoded.fixed_len_byte_array.values) |expected, actual| {
        try testing.expectEqualStrings(expected, actual);
    }
}
