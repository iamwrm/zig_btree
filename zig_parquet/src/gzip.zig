const std = @import("std");

const flate = std.compress.flate;

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
    var inflate: flate.Decompress = .init(&in, .gzip, &.{});
    const written = inflate.reader.streamRemaining(&out) catch return error.CorruptPage;
    if (written != output.len or out.buffered().len != output.len) return error.CorruptPage;
    if (in.seek != compressed.len) return error.CorruptPage;
    const metadata = inflate.container_metadata.gzip;
    const output_len = std.math.cast(u32, output.len) orelse return error.CorruptPage;
    if (metadata.count != output_len) return error.CorruptPage;
    if (metadata.crc != std.hash.Crc32.hash(output)) return error.CorruptPage;
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
