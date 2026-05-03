const std = @import("std");

const std_zstd = std.compress.zstd;
const block_size_max = std_zstd.block_size_max;

const BlockType = enum(u2) {
    raw = 0,
    rle = 1,
    compressed = 2,
};

const RepeatRun = struct {
    start: usize,
    count: usize,
    literal_len: u32,
    match_len: u32,
    offset: u32,
    literal_code: u8,
    literal_extra_bits: u5,
    literal_extra: u32,
    match_code: u8,
    match_extra_bits: u5,
    match_extra: u32,
    offset_code: u5,
    offset_extra: u32,

    fn sequenceLen(self: RepeatRun) usize {
        return @as(usize, self.literal_len) + @as(usize, self.match_len);
    }

    fn end(self: RepeatRun) usize {
        return self.start + self.count * self.sequenceLen();
    }
};

pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, output: []u8) !void {
    decompressDirect(compressed, output) catch {
        try decompressBuffered(allocator, compressed, output);
    };
}

pub fn decompressWithScratch(compressed: []const u8, output: []u8, scratch: []u8, window_len: usize) !void {
    try decompressBufferedScratch(compressed, output, scratch, window_len);
}

fn decompressDirect(compressed: []const u8, output: []u8) !void {
    var in: std.Io.Reader = .fixed(compressed);
    var out: std.Io.Writer = .fixed(output);
    var stream: std_zstd.Decompress = .init(&in, &.{}, .{});
    const written = stream.reader.streamRemaining(&out) catch return error.CorruptPage;
    if (written != output.len or out.buffered().len != output.len) return error.CorruptPage;
}

fn decompressBuffered(allocator: std.mem.Allocator, compressed: []const u8, output: []u8) !void {
    decompressBufferedWindow(allocator, compressed, output, @max(output.len, 1)) catch {
        try decompressBufferedWindow(allocator, compressed, output, std_zstd.default_window_len);
    };
}

fn decompressBufferedWindow(allocator: std.mem.Allocator, compressed: []const u8, output: []u8, window_len: usize) !void {
    const scratch_len = std.math.add(usize, window_len, std_zstd.block_size_max) catch return error.CorruptPage;
    const scratch = try allocator.alloc(u8, scratch_len);
    defer allocator.free(scratch);
    try decompressBufferedScratch(compressed, output, scratch, window_len);
}

fn decompressBufferedScratch(compressed: []const u8, output: []u8, scratch: []u8, window_len: usize) !void {
    const window_len_u32 = std.math.cast(u32, window_len) orelse return error.CorruptPage;
    if (scratch.len < window_len + std_zstd.block_size_max) return error.CorruptPage;
    var in: std.Io.Reader = .fixed(compressed);
    var stream: std_zstd.Decompress = .init(&in, scratch, .{ .window_len = window_len_u32 });
    stream.reader.readSliceAll(output) catch return error.CorruptPage;

    const extra = stream.reader.discardRemaining() catch return error.CorruptPage;
    if (extra != 0) return error.CorruptPage;
}

pub fn compressFrame(allocator: std.mem.Allocator, plain: []const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();

    try out.writer.writeAll("\x28\xb5\x2f\xfd");

    // Single-segment frame, 8-byte frame content size. This keeps the encoder
    // simple and lets readers validate the exact uncompressed page length.
    try out.writer.writeByte(0xe0);
    try out.writer.writeInt(u64, plain.len, .little);

    var pos: usize = 0;
    while (pos < plain.len or (plain.len == 0 and pos == 0)) {
        if (plain.len == 0) {
            try writeRawBlock(&out.writer, "", true);
            break;
        }

        if (findBestRepeatRun(plain, pos)) |run| {
            if (run.start > pos) {
                const raw_end = @min(run.start, pos + block_size_max);
                try writeBestRawBlock(&out.writer, plain[pos..raw_end], raw_end == plain.len);
                pos = raw_end;
                continue;
            }

            const compressed = try buildRepeatBlock(allocator, plain, run);
            defer allocator.free(compressed);
            const plain_len = run.end() - run.start;
            if (compressed.len < plain_len) {
                try writeBlockHeader(&out.writer, .compressed, compressed.len, run.end() == plain.len);
                try out.writer.writeAll(compressed);
                pos = run.end();
                continue;
            }
        }

        const raw_end = @min(plain.len, pos + block_size_max);
        try writeBestRawBlock(&out.writer, plain[pos..raw_end], raw_end == plain.len);
        pos = raw_end;
    }

    return out.toOwnedSlice();
}

pub fn compressRawFrame(allocator: std.mem.Allocator, plain: []const u8) ![]u8 {
    return compressFrame(allocator, plain);
}

fn writeBestRawBlock(writer: *std.Io.Writer, bytes: []const u8, last: bool) !void {
    if (bytes.len > 3 and allEqual(bytes)) {
        try writeBlockHeader(writer, .rle, bytes.len, last);
        try writer.writeByte(bytes[0]);
        return;
    }
    try writeRawBlock(writer, bytes, last);
}

fn writeRawBlock(writer: *std.Io.Writer, bytes: []const u8, last: bool) !void {
    try writeBlockHeader(writer, .raw, bytes.len, last);
    try writer.writeAll(bytes);
}

fn writeBlockHeader(writer: *std.Io.Writer, block_type: BlockType, size: usize, last: bool) !void {
    const header: u24 = (@as(u24, @intCast(size)) << 3) |
        (@as(u24, @intFromEnum(block_type)) << 1) |
        @as(u24, if (last) 1 else 0);
    try writer.writeInt(u24, header, .little);
}

fn allEqual(bytes: []const u8) bool {
    for (bytes[1..]) |byte| {
        if (byte != bytes[0]) return false;
    }
    return true;
}

fn findBestRepeatRun(plain: []const u8, pos: usize) ?RepeatRun {
    const scan_end = @min(plain.len, pos + 1024);
    var best: ?RepeatRun = null;
    var best_savings: usize = 0;

    var start = pos;
    while (start < scan_end) : (start += 1) {
        for (candidate_offsets) |offset| {
            if (offset > start + 16) continue;
            var literal_len: usize = 0;
            const max_literal_len = @min(@as(usize, 16), plain.len - start);
            while (literal_len <= max_literal_len) : (literal_len += 1) {
                const match_start = start + literal_len;
                if (match_start >= plain.len or match_start < offset) continue;

                const max_match_len = @min(block_size_max - literal_len, plain.len - match_start);
                const match_len = matchLenAt(plain, match_start, offset, max_match_len);
                if (match_len < 3) continue;

                const literal_code = lengthCode(std_zstd.literals_length_code_table[0..], @intCast(literal_len)) orelse continue;
                const match_code = lengthCode(std_zstd.match_length_code_table[0..], @intCast(match_len)) orelse continue;
                const offset_code_value = offsetCode(@intCast(offset));
                const seq_len = literal_len + match_len;
                if (seq_len == 0) continue;

                var count: usize = 0;
                var seq_start = start;
                while (seq_start + seq_len <= plain.len and seq_start + seq_len - start <= block_size_max) {
                    const current_match_start = seq_start + literal_len;
                    if (current_match_start < offset) break;
                    if (matchLenAt(plain, current_match_start, offset, match_len) < match_len) break;
                    count += 1;
                    seq_start += seq_len;
                }
                if (count == 0) continue;

                const source_len = count * seq_len;
                const encoded_len = estimatedRepeatBlockLen(count, literal_len, literal_code.extra_bits, match_code.extra_bits, offset_code_value.bits);
                if (encoded_len >= source_len) continue;

                const savings = source_len - encoded_len;
                if (savings > best_savings) {
                    best_savings = savings;
                    best = .{
                        .start = start,
                        .count = count,
                        .literal_len = @intCast(literal_len),
                        .match_len = @intCast(match_len),
                        .offset = @intCast(offset),
                        .literal_code = literal_code.code,
                        .literal_extra_bits = literal_code.extra_bits,
                        .literal_extra = @intCast(literal_len - literal_code.base),
                        .match_code = match_code.code,
                        .match_extra_bits = match_code.extra_bits,
                        .match_extra = @intCast(match_len - match_code.base),
                        .offset_code = offset_code_value.bits,
                        .offset_extra = offset_code_value.extra,
                    };
                    if (savings >= 1024 or (start == pos and savings >= 64)) return best;
                }
            }
        }
    }

    return best;
}

const candidate_offsets = [_]usize{
    1,  2,  3,  4,  5,  6,  7, 8,
    12, 16, 24, 32, 48, 64,
};

fn matchLenAt(bytes: []const u8, pos: usize, offset: usize, max_len: usize) usize {
    if (pos < offset) return 0;
    var len: usize = 0;
    while (len < max_len and bytes[pos + len] == bytes[pos + len - offset]) : (len += 1) {}
    return len;
}

const LengthCode = struct {
    code: u8,
    base: usize,
    extra_bits: u5,
};

fn lengthCode(table: anytype, value: usize) ?LengthCode {
    for (table, 0..) |entry, code| {
        const base: usize = entry[0];
        const extra_bits: u5 = entry[1];
        const range = @as(usize, 1) << extra_bits;
        if (value >= base and value < base + range) {
            return .{
                .code = @intCast(code),
                .base = base,
                .extra_bits = extra_bits,
            };
        }
    }
    return null;
}

const OffsetCode = struct {
    bits: u5,
    extra: u32,
};

fn offsetCode(offset: u32) OffsetCode {
    const offset_value = offset + 3;
    const bits: u5 = @intCast(std.math.log2_int(u32, offset_value));
    return .{
        .bits = bits,
        .extra = offset_value - (@as(u32, 1) << bits),
    };
}

fn estimatedRepeatBlockLen(sequence_count: usize, literal_len: usize, literal_extra_bits: u5, match_extra_bits: u5, offset_extra_bits: u5) usize {
    const literal_size = sequence_count * literal_len;
    const bit_count = sequence_count * (@as(usize, literal_extra_bits) + @as(usize, match_extra_bits) + @as(usize, offset_extra_bits));
    return rawLiteralsHeaderLen(literal_size) +
        literal_size +
        sequenceCountHeaderLen(sequence_count) +
        3 +
        reverseBitstreamLen(bit_count);
}

fn rawLiteralsHeaderLen(size: usize) usize {
    if (size < 32) return 1;
    if (size < 4096) return 2;
    return 3;
}

fn sequenceCountHeaderLen(count: usize) usize {
    if (count < 128) return 2;
    if (count < 0x7f00) return 3;
    return 4;
}

fn reverseBitstreamLen(data_bits: usize) usize {
    const padding = (8 - ((data_bits + 1) & 7)) & 7;
    return (padding + 1 + data_bits) / 8;
}

fn buildRepeatBlock(allocator: std.mem.Allocator, plain: []const u8, run: RepeatRun) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();

    const literal_size = run.count * @as(usize, run.literal_len);
    try writeRawLiteralsHeader(&out.writer, literal_size);
    var seq_start = run.start;
    var i: usize = 0;
    while (i < run.count) : (i += 1) {
        try out.writer.writeAll(plain[seq_start..][0..run.literal_len]);
        seq_start += run.sequenceLen();
    }

    try writeSequenceHeader(&out.writer, run.count);
    try out.writer.writeByte(run.literal_code);
    try out.writer.writeByte(run.offset_code);
    try out.writer.writeByte(run.match_code);
    try writeRepeatBitstream(allocator, &out.writer, run);

    return out.toOwnedSlice();
}

fn writeRawLiteralsHeader(writer: *std.Io.Writer, size: usize) !void {
    if (size < 32) {
        try writer.writeByte(@intCast(size << 3));
    } else if (size < 4096) {
        const byte0: u8 = @intCast(0b0100 | ((size & 0x0f) << 4));
        const byte1: u8 = @intCast(size >> 4);
        try writer.writeByte(byte0);
        try writer.writeByte(byte1);
    } else {
        const byte0: u8 = @intCast(0b1100 | ((size & 0x0f) << 4));
        const byte1: u8 = @intCast((size >> 4) & 0xff);
        const byte2: u8 = @intCast(size >> 12);
        try writer.writeByte(byte0);
        try writer.writeByte(byte1);
        try writer.writeByte(byte2);
    }
}

fn writeSequenceHeader(writer: *std.Io.Writer, count: usize) !void {
    if (count < 128) {
        try writer.writeByte(@intCast(count));
    } else if (count < 0x7f00) {
        try writer.writeByte(@intCast(128 + (count >> 8)));
        try writer.writeByte(@intCast(count & 0xff));
    } else {
        const adjusted = count - 0x7f00;
        try writer.writeByte(255);
        try writer.writeByte(@intCast(adjusted & 0xff));
        try writer.writeByte(@intCast((adjusted >> 8) & 0xff));
    }
    try writer.writeByte(0x54);
}

fn writeRepeatBitstream(allocator: std.mem.Allocator, writer: *std.Io.Writer, run: RepeatRun) !void {
    const bits_per_sequence = @as(usize, run.offset_code) +
        @as(usize, run.match_extra_bits) +
        @as(usize, run.literal_extra_bits);
    const data_bits = run.count * bits_per_sequence;
    const byte_len = reverseBitstreamLen(data_bits);
    const bytes = try allocator.alloc(u8, byte_len);
    defer allocator.free(bytes);
    @memset(bytes, 0);

    var bit_writer: ReverseBitWriter = .{ .bytes = bytes };
    const padding = (8 - ((data_bits + 1) & 7)) & 7;
    try bit_writer.writeZeroes(padding);
    try bit_writer.writeBit(true);
    var i: usize = 0;
    while (i < run.count) : (i += 1) {
        try bit_writer.writeBits(run.offset_extra, run.offset_code);
        try bit_writer.writeBits(run.match_extra, run.match_extra_bits);
        try bit_writer.writeBits(run.literal_extra, run.literal_extra_bits);
    }
    try writer.writeAll(bytes);
}

const ReverseBitWriter = struct {
    bytes: []u8,
    bit_index: usize = 0,

    fn writeZeroes(self: *ReverseBitWriter, count: usize) !void {
        var i: usize = 0;
        while (i < count) : (i += 1) try self.writeBit(false);
    }

    fn writeBits(self: *ReverseBitWriter, value: u32, count: u5) !void {
        var remaining = count;
        while (remaining > 0) {
            remaining -= 1;
            try self.writeBit(((value >> remaining) & 1) != 0);
        }
    }

    fn writeBit(self: *ReverseBitWriter, value: bool) !void {
        if (self.bit_index >= self.bytes.len * 8) return error.CorruptPage;
        const byte_from_end = self.bit_index / 8;
        const bit_in_byte: u3 = @intCast(7 - (self.bit_index & 7));
        if (value) self.bytes[self.bytes.len - 1 - byte_from_end] |= @as(u8, 1) << bit_in_byte;
        self.bit_index += 1;
    }
};

test "raw zstd frame round-trips through std decompressor" {
    const testing = std.testing;
    const input = "zig parquet zstd raw frame smoke";
    const compressed = try compressFrame(testing.allocator, input);
    defer testing.allocator.free(compressed);

    var output: [input.len]u8 = undefined;
    try decompress(testing.allocator, compressed, &output);
    try testing.expectEqualStrings(input, &output);
}

test "empty raw zstd frame round-trips" {
    const testing = std.testing;
    const compressed = try compressFrame(testing.allocator, "");
    defer testing.allocator.free(compressed);
    try decompress(testing.allocator, compressed, &.{});
}

test "zstd frame uses compressed repeat blocks when beneficial" {
    const testing = std.testing;
    const input = try testing.allocator.alloc(u8, 20000 * 8);
    defer testing.allocator.free(input);
    for (0..20000) |i| {
        std.mem.writeInt(u64, input[i * 8 ..][0..8], i, .little);
    }

    const compressed = try compressFrame(testing.allocator, input);
    defer testing.allocator.free(compressed);
    try testing.expect(compressed.len < input.len);

    const output = try testing.allocator.alloc(u8, input.len);
    defer testing.allocator.free(output);
    try decompress(testing.allocator, compressed, output);
    try testing.expectEqualSlices(u8, input, output);
}
