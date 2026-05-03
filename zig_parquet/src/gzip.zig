const std = @import("std");

const flate = std.compress.flate;
const crc32_tables = initCrc32Tables();

pub fn compress(allocator: std.mem.Allocator, plain: []const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = try .initCapacity(allocator, @max(64, plain.len +| flate.Container.gzip.size()));
    errdefer out.deinit();

    const scratch = try allocator.alloc(u8, flate.max_window_len);
    defer allocator.free(scratch);

    var deflate = try flate.Compress.init(&out.writer, scratch, .gzip, flate.Compress.Options.fastest);
    try deflate.writer.writeAll(plain);
    try deflate.finish();

    return out.toOwnedSlice();
}

pub fn decompress(compressed: []const u8, output: []u8) !void {
    var in: std.Io.Reader = .fixed(compressed);
    var out: std.Io.Writer = .fixed(output);
    var inflate_buffer: [flate.max_window_len]u8 = undefined;
    var inflate: flate.Decompress = .init(&in, .gzip, &inflate_buffer);
    const written = inflate.reader.streamRemaining(&out) catch return error.CorruptPage;
    if (written != output.len or out.buffered().len != output.len) return error.CorruptPage;
    if (in.seek != compressed.len) return error.CorruptPage;
    const metadata = inflate.container_metadata.gzip;
    const output_len = std.math.cast(u32, output.len) orelse return error.CorruptPage;
    if (metadata.count != output_len) return error.CorruptPage;
    if (metadata.crc != crc32(output)) return error.CorruptPage;
}

fn crc32(bytes: []const u8) u32 {
    var crc: u32 = 0xffffffff;
    var pos: usize = 0;
    while (pos + 8 <= bytes.len) : (pos += 8) {
        const chunk = std.mem.readInt(u64, bytes[pos..][0..8], .little) ^ crc;
        crc = crc32_tables[7][@as(u8, @truncate(chunk))] ^
            crc32_tables[6][@as(u8, @truncate(chunk >> 8))] ^
            crc32_tables[5][@as(u8, @truncate(chunk >> 16))] ^
            crc32_tables[4][@as(u8, @truncate(chunk >> 24))] ^
            crc32_tables[3][@as(u8, @truncate(chunk >> 32))] ^
            crc32_tables[2][@as(u8, @truncate(chunk >> 40))] ^
            crc32_tables[1][@as(u8, @truncate(chunk >> 48))] ^
            crc32_tables[0][@as(u8, @truncate(chunk >> 56))];
    }
    for (bytes[pos..]) |byte| {
        crc = crc32_tables[0][@as(u8, @truncate(crc)) ^ byte] ^ (crc >> 8);
    }
    return ~crc;
}

fn initCrc32Tables() [8][256]u32 {
    @setEvalBranchQuota(20_000);
    const poly: u32 = 0xedb88320;
    var tables: [8][256]u32 = undefined;
    for (&tables[0], 0..) |*entry, idx| {
        var crc: u32 = @intCast(idx);
        for (0..8) |_| {
            crc = (crc >> 1) ^ (poly & (0 -% (crc & 1)));
        }
        entry.* = crc;
    }
    for (1..tables.len) |table_idx| {
        for (&tables[table_idx], 0..) |*entry, idx| {
            const previous = tables[table_idx - 1][idx];
            entry.* = (previous >> 8) ^ tables[0][@as(u8, @truncate(previous))];
        }
    }
    return tables;
}

test "gzip roundtrip" {
    const plain = "parquet page bytes" ** 64;
    const compressed = try compress(std.testing.allocator, plain);
    defer std.testing.allocator.free(compressed);

    var out: [plain.len]u8 = undefined;
    try decompress(compressed, &out);
    try std.testing.expectEqualSlices(u8, plain, &out);
}

test "gzip roundtrip empty page" {
    const compressed = try compress(std.testing.allocator, "");
    defer std.testing.allocator.free(compressed);

    try decompress(compressed, "");
}

test "gzip crc32 fast path matches standard crc" {
    const inputs = [_][]const u8{
        "",
        "123456789",
        "parquet gzip crc payload" ** 128,
    };
    for (inputs) |input| {
        try std.testing.expectEqual(std.hash.Crc32.hash(input), crc32(input));
    }
}

test "gzip rejects bad footer and trailing bytes" {
    const plain = "checked gzip payload";
    const compressed = try compress(std.testing.allocator, plain);
    defer std.testing.allocator.free(compressed);

    const bad_crc = try std.testing.allocator.dupe(u8, compressed);
    defer std.testing.allocator.free(bad_crc);
    bad_crc[bad_crc.len - 8] ^= 0xff;
    var out: [plain.len]u8 = undefined;
    try std.testing.expectError(error.CorruptPage, decompress(bad_crc, &out));

    const trailing = try std.testing.allocator.alloc(u8, compressed.len + 1);
    defer std.testing.allocator.free(trailing);
    @memcpy(trailing[0..compressed.len], compressed);
    trailing[compressed.len] = 0;
    try std.testing.expectError(error.CorruptPage, decompress(trailing, &out));
}
