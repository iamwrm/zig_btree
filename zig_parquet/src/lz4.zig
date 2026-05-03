const std = @import("std");

const min_match = 4;
const min_emit_match = 8;
const max_distance = 64 * 1024 - 1;
const hash_bits = 16;
const hash_size = 1 << hash_bits;
const last_literals = 5;
const last_match_start_distance = 12;
const invalid_pos = std.math.maxInt(u32);

pub fn decompress(compressed: []const u8, out: []u8) !void {
    const written = try decompressBlock(compressed, out);
    if (written != out.len) return error.CorruptPage;
}

pub fn decompressLegacy(compressed: []const u8, out: []u8) !void {
    if (decompressHadoop(compressed, out)) |_| return else |_| {}
    if (decompressFrame(compressed, out)) |_| return else |_| {}
    return decompress(compressed, out);
}

pub fn decompressHadoop(compressed: []const u8, out: []u8) !void {
    const prefix_len = 8;
    if (compressed.len < prefix_len) return error.CorruptPage;

    var in_pos: usize = 0;
    var out_pos: usize = 0;
    while (in_pos < compressed.len) {
        if (compressed.len - in_pos < prefix_len) return error.CorruptPage;
        const decompressed_size = std.mem.readInt(u32, compressed[in_pos..][0..4], .big);
        const compressed_size = std.mem.readInt(u32, compressed[in_pos + 4 ..][0..4], .big);
        in_pos += prefix_len;

        if (compressed_size > compressed.len - in_pos) return error.CorruptPage;
        if (decompressed_size > out.len - out_pos) return error.CorruptPage;
        try decompress(compressed[in_pos..][0..compressed_size], out[out_pos..][0..decompressed_size]);
        in_pos += compressed_size;
        out_pos += decompressed_size;
    }

    if (out_pos != out.len) return error.CorruptPage;
}

pub fn decompressFrame(compressed: []const u8, out: []u8) !void {
    if (compressed.len < 7) return error.CorruptPage;
    if (std.mem.readInt(u32, compressed[0..4], .little) != 0x184D2204) return error.CorruptPage;

    const flags = compressed[4];
    const descriptor = compressed[5];
    _ = descriptor;
    if (((flags >> 6) & 0x03) != 1) return error.CorruptPage;
    if ((flags & 0x02) != 0) return error.CorruptPage;
    if ((flags & 0x20) == 0) return error.CorruptPage;

    const has_content_size = (flags & 0x08) != 0;
    const has_content_checksum = (flags & 0x04) != 0;
    const has_block_checksum = (flags & 0x10) != 0;
    const has_dict_id = (flags & 0x01) != 0;

    var pos: usize = 6;
    var declared_content_size: ?u64 = null;
    if (has_content_size) {
        if (compressed.len - pos < 8) return error.CorruptPage;
        declared_content_size = std.mem.readInt(u64, compressed[pos..][0..8], .little);
        pos += 8;
    }
    if (has_dict_id) {
        if (compressed.len - pos < 4) return error.CorruptPage;
        pos += 4;
    }
    if (pos >= compressed.len) return error.CorruptPage;
    pos += 1; // Header checksum.

    var out_pos: usize = 0;
    while (true) {
        if (compressed.len - pos < 4) return error.CorruptPage;
        const block_size_raw = std.mem.readInt(u32, compressed[pos..][0..4], .little);
        pos += 4;
        if (block_size_raw == 0) break;

        const uncompressed_block = (block_size_raw & 0x8000_0000) != 0;
        const block_size: usize = @intCast(block_size_raw & 0x7fff_ffff);
        if (block_size > compressed.len - pos) return error.CorruptPage;

        if (uncompressed_block) {
            if (block_size > out.len - out_pos) return error.CorruptPage;
            @memcpy(out[out_pos..][0..block_size], compressed[pos..][0..block_size]);
            out_pos += block_size;
        } else {
            const written = try decompressBlock(compressed[pos..][0..block_size], out[out_pos..]);
            out_pos += written;
        }
        pos += block_size;
        if (has_block_checksum) {
            if (compressed.len - pos < 4) return error.CorruptPage;
            pos += 4;
        }
    }

    if (has_content_checksum) {
        if (compressed.len - pos < 4) return error.CorruptPage;
        pos += 4;
    }
    if (pos != compressed.len or out_pos != out.len) return error.CorruptPage;
    if (declared_content_size) |size| {
        if (size != out.len) return error.CorruptPage;
    }
}

fn decompressBlock(compressed: []const u8, out: []u8) !usize {
    if (compressed.len == 0) {
        if (out.len == 0) return 0;
        return error.CorruptPage;
    }

    var in_pos: usize = 0;
    var out_pos: usize = 0;
    while (in_pos < compressed.len) {
        const token = compressed[in_pos];
        in_pos += 1;

        const literal_len = try readExtendedLen(compressed, &in_pos, token >> 4);
        if (literal_len > compressed.len - in_pos or literal_len > out.len - out_pos) return error.CorruptPage;
        @memcpy(out[out_pos..][0..literal_len], compressed[in_pos..][0..literal_len]);
        in_pos += literal_len;
        out_pos += literal_len;

        if (in_pos == compressed.len) break;
        if (compressed.len - in_pos < 2) return error.CorruptPage;
        const offset = std.mem.readInt(u16, compressed[in_pos..][0..2], .little);
        in_pos += 2;
        if (offset == 0 or offset > out_pos) return error.CorruptPage;

        const match_len = try readExtendedLen(compressed, &in_pos, token & 0x0f) + 4;
        if (match_len > out.len - out_pos) return error.CorruptPage;
        copyMatch(out, out_pos, offset, match_len);
        out_pos += match_len;
    }

    if (in_pos != compressed.len) return error.CorruptPage;
    return out_pos;
}

pub fn compress(allocator: std.mem.Allocator, plain: []const u8) ![]u8 {
    if (plain.len == 0) return try allocator.alloc(u8, 0);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    if (plain.len <= last_literals + last_match_start_distance) {
        try emitLastLiterals(allocator, &out, plain);
        return try out.toOwnedSlice(allocator);
    }

    const table = try allocator.alloc(u32, hash_size);
    defer allocator.free(table);
    @memset(table, invalid_pos);

    const match_search_end = plain.len - last_match_start_distance;
    const match_copy_end = plain.len - last_literals;
    var anchor: usize = 0;
    var pos: usize = 0;

    while (pos <= match_search_end) {
        const sequence = readSequence(plain, pos);
        const slot = hashSequence(sequence);
        const prev = table[slot];
        table[slot] = @intCast(pos);

        if (prev != invalid_pos) {
            const ref = @as(usize, prev);
            if (pos > ref and pos - ref <= max_distance and readSequence(plain, ref) == sequence) {
                var match_len: usize = min_match;
                while (pos + match_len < match_copy_end and plain[ref + match_len] == plain[pos + match_len]) {
                    match_len += 1;
                }
                if (match_len < min_emit_match) {
                    pos += 1;
                    continue;
                }

                try emitSequence(allocator, &out, plain[anchor..pos], pos - ref, match_len);
                pos += match_len;
                anchor = pos;
                continue;
            }
        }

        pos += 1;
    }

    try emitLastLiterals(allocator, &out, plain[anchor..]);
    return try out.toOwnedSlice(allocator);
}

fn readSequence(data: []const u8, pos: usize) u32 {
    return std.mem.readInt(u32, data[pos..][0..min_match], .little);
}

fn hashSequence(sequence: u32) usize {
    const mixed: u32 = sequence *% 2654435761;
    return @intCast(mixed >> (32 - hash_bits));
}

fn emitSequence(allocator: std.mem.Allocator, out: *std.ArrayList(u8), literals: []const u8, offset: usize, match_len: usize) !void {
    if (offset == 0 or offset > max_distance or match_len < min_match) return error.InvalidColumnData;

    const match_code = match_len - min_match;
    const literal_nibble: u8 = if (literals.len >= 15) 15 else @intCast(literals.len);
    const match_nibble: u8 = if (match_code >= 15) 15 else @intCast(match_code);
    try out.append(allocator, (literal_nibble << 4) | match_nibble);
    if (literals.len >= 15) try writeExtendedLen(allocator, out, literals.len - 15);
    try out.appendSlice(allocator, literals);
    try out.append(allocator, @intCast(offset & 0xff));
    try out.append(allocator, @intCast(offset >> 8));
    if (match_code >= 15) try writeExtendedLen(allocator, out, match_code - 15);
}

fn emitLastLiterals(allocator: std.mem.Allocator, out: *std.ArrayList(u8), literals: []const u8) !void {
    const literal_nibble: u8 = if (literals.len >= 15) 15 else @intCast(literals.len);
    try out.append(allocator, literal_nibble << 4);
    if (literals.len >= 15) try writeExtendedLen(allocator, out, literals.len - 15);
    try out.appendSlice(allocator, literals);
}

fn readExtendedLen(data: []const u8, pos: *usize, nibble: u8) !usize {
    var len: usize = nibble;
    if (nibble != 15) return len;
    while (true) {
        if (pos.* >= data.len) return error.CorruptPage;
        const byte = data[pos.*];
        pos.* += 1;
        len = std.math.add(usize, len, byte) catch return error.CorruptPage;
        if (byte != 255) return len;
    }
}

fn writeExtendedLen(allocator: std.mem.Allocator, out: *std.ArrayList(u8), len: usize) !void {
    var remaining = len;
    while (remaining >= 255) {
        try out.append(allocator, 255);
        remaining -= 255;
    }
    try out.append(allocator, @intCast(remaining));
}

fn copyMatch(out: []u8, out_pos: usize, offset: usize, len: usize) void {
    if (offset >= len) {
        @memcpy(out[out_pos..][0..len], out[out_pos - offset ..][0..len]);
        return;
    }

    @memcpy(out[out_pos..][0..offset], out[out_pos - offset ..][0..offset]);
    var copied = offset;
    while (copied < len) {
        const chunk = @min(copied, len - copied);
        @memcpy(out[out_pos + copied ..][0..chunk], out[out_pos..][0..chunk]);
        copied += chunk;
    }
}

test "lz4 raw literal-only roundtrip" {
    const input = "parquet lz4 raw literal-only block" ** 128;
    const compressed = try compress(std.testing.allocator, input);
    defer std.testing.allocator.free(compressed);

    const out = try std.testing.allocator.alloc(u8, input.len);
    defer std.testing.allocator.free(out);
    try decompress(compressed, out);
    try std.testing.expectEqualSlices(u8, input, out);
}

test "lz4 raw compressor emits matches for repeated input" {
    const input = "abcdefghijklmnopqrstuvwxyz012345" ** 64;
    const compressed = try compress(std.testing.allocator, input);
    defer std.testing.allocator.free(compressed);
    try std.testing.expect(compressed.len < input.len / 4);

    const out = try std.testing.allocator.alloc(u8, input.len);
    defer std.testing.allocator.free(out);
    try decompress(compressed, out);
    try std.testing.expectEqualSlices(u8, input, out);
}

test "lz4 raw decodes overlapping match" {
    const compressed = [_]u8{
        0x51, // 5 literals, match len 5
        'h',
        'e',
        'l',
        'l',
        'o',
        5, 0, // offset
    };
    var out: [10]u8 = undefined;
    try decompress(&compressed, &out);
    try std.testing.expectEqualStrings("hellohello", &out);
}

test "lz4 raw rejects truncated block" {
    const compressed = [_]u8{ 0x40, 'a', 'b' };
    var out: [4]u8 = undefined;
    try std.testing.expectError(error.CorruptPage, decompress(&compressed, &out));
}

test "lz4 hadoop wrapper decodes raw block" {
    const raw = [_]u8{
        0x51,
        'h',
        'e',
        'l',
        'l',
        'o',
        5,
        0,
    };
    const compressed = [_]u8{
        0, 0, 0, 10,
        0, 0, 0, raw.len,
    } ++ raw;

    var out: [10]u8 = undefined;
    try decompressHadoop(&compressed, &out);
    try std.testing.expectEqualStrings("hellohello", &out);
}

test "lz4 frame decodes uncompressed and compressed blocks" {
    const raw = [_]u8{
        0x51,
        'h',
        'e',
        'l',
        'l',
        'o',
        5,
        0,
    };
    const uncompressed_frame = [_]u8{
        0x04, 0x22, 0x4d, 0x18,
        0x60, 0x40, 0x00, 5,
        0,    0,    0x80, 'h',
        'e',  'l',  'l',  'o',
        0,    0,    0,    0,
    };
    const compressed_frame = [_]u8{
        0x04, 0x22, 0x4d, 0x18,
        0x60, 0x40, 0x00, raw.len,
        0,    0,    0,
    } ++ raw ++ [_]u8{ 0, 0, 0, 0 };

    var uncompressed_out: [5]u8 = undefined;
    try decompressFrame(&uncompressed_frame, &uncompressed_out);
    try std.testing.expectEqualStrings("hello", &uncompressed_out);

    var compressed_out: [10]u8 = undefined;
    try decompressFrame(&compressed_frame, &compressed_out);
    try std.testing.expectEqualStrings("hellohello", &compressed_out);
}
