const std = @import("std");

pub fn compress(allocator: std.mem.Allocator, plain: []const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();

    try writeVarUint(&out.writer, plain.len);
    if (plain.len == 0) return out.toOwnedSlice();

    const table_size = 1 << 15;
    const table = try allocator.alloc(usize, table_size);
    defer allocator.free(table);
    @memset(table, std.math.maxInt(usize));

    var pos: usize = 0;
    var literal_start: usize = 0;
    while (pos + 4 <= plain.len) {
        const key = std.mem.readInt(u32, plain[pos..][0..4], .little);
        const slot = hashBytes(key, table_size);
        const candidate = table[slot];
        table[slot] = pos;

        if (candidate != std.math.maxInt(usize) and pos > candidate and pos - candidate <= std.math.maxInt(u16) and std.mem.eql(u8, plain[candidate..][0..4], plain[pos..][0..4])) {
            try writeLiteral(&out.writer, plain[literal_start..pos]);

            const offset = pos - candidate;
            var match_len: usize = 4;
            while (pos + match_len < plain.len and plain[candidate + match_len] == plain[pos + match_len]) : (match_len += 1) {}
            try writeCopy(&out.writer, offset, match_len);

            pos += match_len;
            literal_start = pos;
            continue;
        }

        pos += 1;
    }

    try writeLiteral(&out.writer, plain[literal_start..]);
    return out.toOwnedSlice();
}

pub fn decompress(compressed: []const u8, output: []u8) !void {
    var pos: usize = 0;
    const expected_len = try readVarUint(compressed, &pos);
    if (expected_len != output.len) return error.CorruptPage;

    var out_pos: usize = 0;
    while (pos < compressed.len) {
        const tag = compressed[pos];
        pos += 1;
        switch (tag & 0x03) {
            0 => {
                const len_code: usize = tag >> 2;
                var len: usize = undefined;
                if (len_code < 60) {
                    len = len_code + 1;
                } else {
                    const bytes = len_code - 59;
                    if (bytes > 4 or pos + bytes > compressed.len) return error.CorruptPage;
                    len = 0;
                    var i: usize = 0;
                    while (i < bytes) : (i += 1) len |= @as(usize, compressed[pos + i]) << @intCast(8 * i);
                    pos += bytes;
                    len += 1;
                }
                if (pos + len > compressed.len or out_pos + len > output.len) return error.CorruptPage;
                @memcpy(output[out_pos..][0..len], compressed[pos..][0..len]);
                pos += len;
                out_pos += len;
            },
            1 => {
                if (pos >= compressed.len) return error.CorruptPage;
                const len = ((tag >> 2) & 0x7) + 4;
                const offset = (@as(usize, tag & 0xe0) << 3) | compressed[pos];
                pos += 1;
                try copyFromSelf(output, &out_pos, offset, len);
            },
            2 => {
                if (pos + 2 > compressed.len) return error.CorruptPage;
                const len = (tag >> 2) + 1;
                const offset = std.mem.readInt(u16, compressed[pos..][0..2], .little);
                pos += 2;
                try copyFromSelf(output, &out_pos, offset, len);
            },
            3 => {
                if (pos + 4 > compressed.len) return error.CorruptPage;
                const len = (tag >> 2) + 1;
                const offset = std.mem.readInt(u32, compressed[pos..][0..4], .little);
                pos += 4;
                try copyFromSelf(output, &out_pos, offset, len);
            },
            else => unreachable,
        }
    }
    if (out_pos != output.len) return error.CorruptPage;
}

fn hashBytes(value: u32, table_size: usize) usize {
    const shift: u5 = @intCast(32 - std.math.log2_int(usize, table_size));
    return @intCast((value *% 0x1e35a7bd) >> shift);
}

fn writeLiteral(writer: *std.Io.Writer, literal: []const u8) !void {
    if (literal.len == 0) return;
    const len_minus_one = literal.len - 1;
    if (len_minus_one < 60) {
        try writer.writeByte(@intCast(len_minus_one << 2));
    } else {
        var value = len_minus_one;
        var bytes: [4]u8 = undefined;
        var byte_count: usize = 0;
        while (value != 0) : (byte_count += 1) {
            if (byte_count == bytes.len) return error.InvalidColumnData;
            bytes[byte_count] = @intCast(value & 0xff);
            value >>= 8;
        }
        try writer.writeByte(@intCast((59 + byte_count) << 2));
        try writer.writeAll(bytes[0..byte_count]);
    }
    try writer.writeAll(literal);
}

fn writeCopy(writer: *std.Io.Writer, offset: usize, len: usize) !void {
    if (offset == 0 or offset > std.math.maxInt(u16)) return error.InvalidColumnData;
    var remaining = len;
    while (remaining > 0) {
        const chunk: usize = @min(remaining, @as(usize, 64));
        try writer.writeByte(@intCast(((chunk - 1) << 2) | 0x02));
        try writer.writeInt(u16, @intCast(offset), .little);
        remaining -= chunk;
    }
}

fn writeVarUint(writer: *std.Io.Writer, value: usize) !void {
    var v = value;
    while (v >= 0x80) {
        try writer.writeByte(@as(u8, @intCast(v & 0x7f)) | 0x80);
        v >>= 7;
    }
    try writer.writeByte(@intCast(v));
}

fn copyFromSelf(output: []u8, out_pos: *usize, offset: usize, len: usize) !void {
    if (offset == 0 or offset > out_pos.* or out_pos.* + len > output.len) return error.CorruptPage;
    var i: usize = 0;
    while (i < len) : (i += 1) {
        output[out_pos.* + i] = output[out_pos.* - offset + i];
    }
    out_pos.* += len;
}

fn readVarUint(data: []const u8, pos: *usize) !usize {
    var result: usize = 0;
    var shift: u6 = 0;
    while (true) {
        if (pos.* >= data.len) return error.CorruptPage;
        const byte = data[pos.*];
        pos.* += 1;
        result |= (@as(usize, byte & 0x7f) << @intCast(shift));
        if ((byte & 0x80) == 0) return result;
        if (shift >= @bitSizeOf(usize) - 1) return error.CorruptPage;
        shift += 7;
    }
}

test "snappy literal-only block" {
    const compressed = [_]u8{ 5, 16, 'h', 'e', 'l', 'l', 'o' };
    var out: [5]u8 = undefined;
    try decompress(compressed[0..], out[0..]);
    try std.testing.expectEqualStrings("hello", out[0..]);
}

test "snappy compressor round-trips literals and copies" {
    const testing = std.testing;
    const input = "prefix-000001-suffix prefix-000002-suffix prefix-000003-suffix prefix-000004-suffix";
    const compressed = try compress(testing.allocator, input);
    defer testing.allocator.free(compressed);
    try testing.expect(compressed.len < input.len + 8);

    var out: [input.len]u8 = undefined;
    try decompress(compressed, out[0..]);
    try testing.expectEqualStrings(input, out[0..]);
}

test "snappy compressor round-trips parquet-like pages" {
    const testing = std.testing;
    const plain = @import("plain.zig");

    var ints: [512]u8 = undefined;
    for (0..64) |i| {
        std.mem.writeInt(u64, ints[i * 8 ..][0..8], i, .little);
    }
    const int_compressed = try compress(testing.allocator, ints[0..]);
    defer testing.allocator.free(int_compressed);
    var int_out: [ints.len]u8 = undefined;
    try decompress(int_compressed, int_out[0..]);
    try testing.expectEqualSlices(u8, ints[0..], int_out[0..]);

    const labels = [_][]const u8{
        "alpha", "bravo",    "charlie", "delta",
        "echo",  "foxtrot",  "golf",    "hotel",
        "india", "juliet",   "kilo",    "lima",
        "mike",  "november", "oscar",   "papa",
    };
    var dictionary: std.Io.Writer.Allocating = .init(testing.allocator);
    defer dictionary.deinit();
    for (labels) |label| {
        try dictionary.writer.writeInt(u32, @intCast(label.len), .little);
        try dictionary.writer.writeAll(label);
    }
    const dict_compressed = try compress(testing.allocator, dictionary.written());
    defer testing.allocator.free(dict_compressed);
    const dict_out = try testing.allocator.alloc(u8, dictionary.written().len);
    defer testing.allocator.free(dict_out);
    try decompress(dict_compressed, dict_out);
    try testing.expectEqualSlices(u8, dictionary.written(), dict_out);

    var validity: [64]bool = undefined;
    var indexes: [54]u32 = undefined;
    var value_idx: usize = 0;
    for (&validity, 0..) |*valid, row| {
        valid.* = row % 7 != 0;
        if (valid.*) {
            indexes[value_idx] = @intCast(row & 15);
            value_idx += 1;
        }
    }
    var dict_page: std.Io.Writer.Allocating = .init(testing.allocator);
    defer dict_page.deinit();
    try plain.encodeDefinitionLevels(testing.allocator, &dict_page.writer, validity[0..]);
    try dict_page.writer.writeByte(4);
    try plain.encodeRleBitPackedUint32(&dict_page.writer, indexes[0..], 4);

    const page_compressed = try compress(testing.allocator, dict_page.written());
    defer testing.allocator.free(page_compressed);
    const page_out = try testing.allocator.alloc(u8, dict_page.written().len);
    defer testing.allocator.free(page_out);
    try decompress(page_compressed, page_out);
    try testing.expectEqualSlices(u8, dict_page.written(), page_out);
}
