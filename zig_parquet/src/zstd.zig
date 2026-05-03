const std = @import("std");

const std_zstd = std.compress.zstd;
const block_size_max = std_zstd.block_size_max;
const repeat_scan_window = 128;

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
    if (decompressFrameFast(compressed, output, null, null)) |_| return else |err| switch (err) {
        error.UnsupportedFastPath, error.CorruptPage, error.EndOfStream => {},
    }
    if (decompressZigFrameFast(compressed, output)) |_| return else |err| switch (err) {
        error.UnsupportedFastPath, error.CorruptPage => {},
    }
    decompressDirect(compressed, output) catch {
        try decompressBuffered(allocator, compressed, output);
    };
}

pub fn decompressWithScratch(compressed: []const u8, output: []u8, scratch: []u8, window_len: usize) !void {
    try decompressWithScratchAndCache(compressed, output, scratch, window_len, null);
}

pub fn decompressWithScratchAndCache(compressed: []const u8, output: []u8, scratch: []u8, window_len: usize, cache: ?*DecodeCache) !void {
    if (decompressFrameFast(compressed, output, cache, scratch)) |_| return else |err| switch (err) {
        error.UnsupportedFastPath, error.CorruptPage, error.EndOfStream => {},
    }
    if (decompressZigFrameFast(compressed, output)) |_| return else |err| switch (err) {
        error.UnsupportedFastPath, error.CorruptPage => {},
    }
    try decompressBufferedScratch(compressed, output, scratch, window_len);
}

pub fn tryDecompressFastForTesting(compressed: []const u8, output: []u8, cache: ?*DecodeCache) !void {
    if (decompressFrameFast(compressed, output, cache, null)) |_| return else |err| switch (err) {
        error.UnsupportedFastPath, error.CorruptPage, error.EndOfStream => {},
    }
    if (decompressZigFrameFast(compressed, output)) |_| return else |err| switch (err) {
        error.UnsupportedFastPath, error.CorruptPage => {},
    }
    return error.UnsupportedFastPath;
}

pub fn tryDecompressFastWithScratchForTesting(compressed: []const u8, output: []u8, cache: ?*DecodeCache, scratch: []u8) !void {
    if (decompressFrameFast(compressed, output, cache, scratch)) |_| return else |err| switch (err) {
        error.UnsupportedFastPath, error.CorruptPage, error.EndOfStream => {},
    }
    if (decompressZigFrameFast(compressed, output)) |_| return else |err| switch (err) {
        error.UnsupportedFastPath, error.CorruptPage => {},
    }
    return error.UnsupportedFastPath;
}

fn decompressZigFrameFast(compressed: []const u8, output: []u8) !void {
    if (compressed.len < 4 + 1 + 8 + 3) return error.UnsupportedFastPath;
    if (!std.mem.eql(u8, compressed[0..4], "\x28\xb5\x2f\xfd")) return error.UnsupportedFastPath;
    if (compressed[4] != 0xe0) return error.UnsupportedFastPath;

    const declared_size = std.mem.readInt(u64, compressed[5..13], .little);
    if (declared_size != output.len) return error.CorruptPage;

    var pos: usize = 13;
    var out_pos: usize = 0;
    while (true) {
        if (pos + 3 > compressed.len) return error.CorruptPage;
        const header = std.mem.readInt(u24, compressed[pos..][0..3], .little);
        pos += 3;

        const last = (header & 1) != 0;
        const block_type = (header >> 1) & 0x3;
        const size: usize = @intCast(header >> 3);
        if (size > block_size_max) return error.CorruptPage;

        switch (block_type) {
            @intFromEnum(BlockType.raw) => {
                if (pos + size > compressed.len or out_pos + size > output.len) return error.CorruptPage;
                @memcpy(output[out_pos..][0..size], compressed[pos..][0..size]);
                pos += size;
                out_pos += size;
            },
            @intFromEnum(BlockType.rle) => {
                if (pos >= compressed.len or out_pos + size > output.len) return error.CorruptPage;
                @memset(output[out_pos..][0..size], compressed[pos]);
                pos += 1;
                out_pos += size;
            },
            @intFromEnum(BlockType.compressed) => return error.UnsupportedFastPath,
            else => return error.CorruptPage,
        }

        if (last) break;
    }

    if (pos != compressed.len or out_pos != output.len) return error.CorruptPage;
}

const LiteralBlockType = enum(u2) {
    raw = 0,
    rle = 1,
    compressed = 2,
    treeless = 3,
};

const FrameHeader = struct {
    pos: usize,
    content_size: usize,
};

const WindowHeader = struct {
    content_size: ?usize,
    window_len: usize,
};

pub fn decoderWindowLenBounded(compressed: []const u8, output_len: usize, max_window_len: usize) !usize {
    const header = try parseWindowHeader(compressed);
    if (header.content_size) |content_size| {
        if (content_size != output_len) return error.CorruptPage;
    }
    const window_len = @max(header.window_len, 1);
    if (window_len > max_window_len) return error.CorruptPage;
    return window_len;
}

fn decompressFrameFast(compressed: []const u8, output: []u8, cache: ?*DecodeCache, scratch: ?[]u8) !void {
    const frame = try parseSingleSegmentFrameHeader(compressed);
    if (frame.content_size != output.len) return error.CorruptPage;

    var pos = frame.pos;
    var out_pos: usize = 0;
    var huffman_state: FastHuffmanState = .{};
    var sequence_state: FastSequenceState = .{};
    while (true) {
        if (pos + 3 > compressed.len) return error.CorruptPage;
        const header = std.mem.readInt(u24, compressed[pos..][0..3], .little);
        pos += 3;

        const last = (header & 1) != 0;
        const block_type = (header >> 1) & 0x3;
        const size: usize = @intCast(header >> 3);
        if (size > block_size_max) return error.CorruptPage;

        switch (block_type) {
            @intFromEnum(BlockType.raw) => {
                if (pos + size > compressed.len or out_pos + size > output.len) return error.CorruptPage;
                @memcpy(output[out_pos..][0..size], compressed[pos..][0..size]);
                pos += size;
                out_pos += size;
            },
            @intFromEnum(BlockType.rle) => {
                if (pos >= compressed.len or out_pos + size > output.len) return error.CorruptPage;
                @memset(output[out_pos..][0..size], compressed[pos]);
                pos += 1;
                out_pos += size;
            },
            @intFromEnum(BlockType.compressed) => {
                if (pos + size > compressed.len) return error.CorruptPage;
                const written = try decodeCompressedBlockFast(compressed[pos..][0..size], output, out_pos, &huffman_state, &sequence_state, cache, scratch);
                pos += size;
                out_pos += written;
            },
            else => return error.CorruptPage,
        }

        if (last) break;
    }

    if (pos != compressed.len or out_pos != output.len) return error.CorruptPage;
}

fn parseSingleSegmentFrameHeader(compressed: []const u8) !FrameHeader {
    if (compressed.len < 4 + 1 + 3) return error.UnsupportedFastPath;
    if (!std.mem.eql(u8, compressed[0..4], "\x28\xb5\x2f\xfd")) return error.UnsupportedFastPath;

    var pos: usize = 4;
    const descriptor = compressed[pos];
    pos += 1;
    if ((descriptor & 0x08) != 0) return error.CorruptPage;
    if ((descriptor & 0x20) == 0) return error.UnsupportedFastPath;
    if ((descriptor & 0x04) != 0) return error.UnsupportedFastPath;
    if ((descriptor & 0x03) != 0) return error.UnsupportedFastPath;

    const fcs_flag = descriptor >> 6;
    const content_size_u64: u64 = switch (fcs_flag) {
        0 => blk: {
            if (pos + 1 > compressed.len) return error.CorruptPage;
            const value = compressed[pos];
            pos += 1;
            break :blk value;
        },
        1 => blk: {
            if (pos + 2 > compressed.len) return error.CorruptPage;
            const value = std.mem.readInt(u16, compressed[pos..][0..2], .little);
            pos += 2;
            break :blk @as(u64, value) + 256;
        },
        2 => blk: {
            if (pos + 4 > compressed.len) return error.CorruptPage;
            const value = std.mem.readInt(u32, compressed[pos..][0..4], .little);
            pos += 4;
            break :blk value;
        },
        3 => blk: {
            if (pos + 8 > compressed.len) return error.CorruptPage;
            const value = std.mem.readInt(u64, compressed[pos..][0..8], .little);
            pos += 8;
            break :blk value;
        },
        else => unreachable,
    };
    const content_size = std.math.cast(usize, content_size_u64) orelse return error.CorruptPage;
    return .{ .pos = pos, .content_size = content_size };
}

fn parseWindowHeader(compressed: []const u8) !WindowHeader {
    if (compressed.len < 4 + 1) return error.CorruptPage;
    if (!std.mem.eql(u8, compressed[0..4], "\x28\xb5\x2f\xfd")) return error.CorruptPage;

    var pos: usize = 4;
    const descriptor = compressed[pos];
    pos += 1;
    if ((descriptor & 0x08) != 0) return error.CorruptPage;

    const single_segment = (descriptor & 0x20) != 0;
    const window_len_u64: u64 = if (single_segment) 0 else blk: {
        if (pos >= compressed.len) return error.CorruptPage;
        const window_descriptor = compressed[pos];
        pos += 1;
        const exponent = (window_descriptor & 0b11111000) >> 3;
        const mantissa = window_descriptor & 0b00000111;
        const window_log: u6 = @intCast(10 + exponent);
        const window_base = @as(u64, 1) << window_log;
        const window_add = (window_base / 8) * mantissa;
        break :blk window_base + window_add;
    };

    const dictionary_id_flag = descriptor & 0x03;
    const dictionary_id_size: usize = if (dictionary_id_flag == 0) 0 else @as(usize, 1) << @intCast(dictionary_id_flag - 1);
    if (pos + dictionary_id_size > compressed.len) return error.CorruptPage;
    pos += dictionary_id_size;

    const content_size_flag = descriptor >> 6;
    const has_content_size = single_segment or content_size_flag != 0;
    const content_size: ?usize = if (has_content_size) blk: {
        const field_size: usize = @as(usize, 1) << @intCast(content_size_flag);
        if (pos + field_size > compressed.len) return error.CorruptPage;
        const raw: u64 = switch (field_size) {
            1 => compressed[pos],
            2 => std.mem.readInt(u16, compressed[pos..][0..2], .little),
            4 => std.mem.readInt(u32, compressed[pos..][0..4], .little),
            8 => std.mem.readInt(u64, compressed[pos..][0..8], .little),
            else => unreachable,
        };
        const adjusted = if (field_size == 2) raw + 256 else raw;
        break :blk std.math.cast(usize, adjusted) orelse return error.CorruptPage;
    } else null;

    const window_len = if (single_segment) content_size orelse return error.CorruptPage else std.math.cast(usize, window_len_u64) orelse return error.CorruptPage;
    return .{ .content_size = content_size, .window_len = window_len };
}

const LiteralsHeader = struct {
    block_type: LiteralBlockType,
    size_format: u2,
    regenerated_size: usize,
    compressed_size: ?usize,
    consumed: usize,
};

const SequenceMode = enum(u2) {
    predefined = 0,
    rle = 1,
    fse = 2,
    repeat = 3,
};

const SequencesHeader = struct {
    sequence_count: usize,
    literal_lengths: SequenceMode = .predefined,
    offsets: SequenceMode = .predefined,
    match_lengths: SequenceMode = .predefined,
};

fn readSequencesHeader(block: []const u8, pos: *usize) !SequencesHeader {
    if (pos.* >= block.len) return error.CorruptPage;
    const byte0 = block[pos.*];
    pos.* += 1;
    if (byte0 == 0) return .{ .sequence_count = 0 };

    const sequence_count: usize = if (byte0 < 128) byte0 else if (byte0 < 255) blk: {
        if (pos.* >= block.len) return error.CorruptPage;
        const low = block[pos.*];
        pos.* += 1;
        break :blk (@as(usize, byte0 - 128) << 8) + low;
    } else blk: {
        if (pos.* + 2 > block.len) return error.CorruptPage;
        const value = @as(usize, block[pos.*]) + (@as(usize, block[pos.* + 1]) << 8) + 0x7f00;
        pos.* += 2;
        break :blk value;
    };

    if (pos.* >= block.len) return error.CorruptPage;
    const modes = block[pos.*];
    pos.* += 1;
    if ((modes & 0x03) != 0) return error.UnsupportedFastPath;
    return .{
        .sequence_count = sequence_count,
        .literal_lengths = @enumFromInt((modes >> 6) & 0x03),
        .offsets = @enumFromInt((modes >> 4) & 0x03),
        .match_lengths = @enumFromInt((modes >> 2) & 0x03),
    };
}

fn decodeCompressedBlockFast(
    block: []const u8,
    output: []u8,
    out_pos: usize,
    huffman_state: *FastHuffmanState,
    sequence_state: *FastSequenceState,
    cache: ?*DecodeCache,
    scratch: ?[]u8,
) !usize {
    const header = try readLiteralsHeader(block);
    var pos = header.consumed;
    if (header.regenerated_size > block_size_max) return error.CorruptPage;

    const literal_start = pos;
    switch (header.block_type) {
        .raw => {
            if (pos + header.regenerated_size > block.len) return error.CorruptPage;
            pos += header.regenerated_size;
        },
        .rle => {
            if (pos >= block.len) return error.CorruptPage;
            pos += 1;
        },
        .compressed, .treeless => {
            const compressed_size = header.compressed_size orelse return error.CorruptPage;
            if (pos + compressed_size > block.len) return error.CorruptPage;
            pos += compressed_size;
        },
    }

    if (pos >= block.len) return error.CorruptPage;
    const sequences_header = try readSequencesHeader(block, &pos);
    if (sequences_header.sequence_count == 0) {
        if (pos != block.len) return error.UnsupportedFastPath;
        if (out_pos + header.regenerated_size > output.len) return error.CorruptPage;

        const out = output[out_pos..][0..header.regenerated_size];
        switch (header.block_type) {
            .raw => @memcpy(out, block[literal_start..][0..header.regenerated_size]),
            .rle => @memset(out, block[literal_start]),
            .compressed, .treeless => {
                const compressed_size = header.compressed_size orelse return error.CorruptPage;
                try decodeCompressedLiteralsFast(header, block[literal_start..][0..compressed_size], out, huffman_state, cache);
            },
        }
        return header.regenerated_size;
    }

    var literal_source = try prepareSequenceLiteralSource(block, literal_start, header, huffman_state, cache, scratch);
    if (sequences_header.literal_lengths == .rle and sequences_header.offsets == .rle and sequences_header.match_lengths == .rle) {
        if (pos + 3 > block.len) return error.CorruptPage;
        const literal_code = block[pos];
        const offset_code = std.math.cast(u5, block[pos + 1]) orelse return error.CorruptPage;
        const match_code = block[pos + 2];
        pos += 3;
        sequence_state.setTable(.literal, .{ .rle = literal_code }, 0);
        sequence_state.setTable(.offset, .{ .rle = offset_code }, 0);
        sequence_state.setTable(.match, .{ .rle = match_code }, 0);
        sequence_state.fse_tables_defined = true;
        return try decodeRleSequencesFast(
            &literal_source,
            block[pos..],
            output,
            out_pos,
            sequences_header.sequence_count,
            literal_code,
            offset_code,
            match_code,
            &sequence_state.repeat_offsets,
        );
    }
    try sequence_state.prepare(sequences_header, block, &pos);
    return try decodeSequencesFast(&literal_source, block[pos..], output, out_pos, sequences_header.sequence_count, sequence_state);
}

const SequenceLengthCode = struct {
    base: u32,
    extra_bits: u5,
};

const literal_length_code_table = initSequenceLengthCodes(std_zstd.literals_length_code_table);
const match_length_code_table = initSequenceLengthCodes(std_zstd.match_length_code_table);

fn initSequenceLengthCodes(comptime source: anytype) [source.len]SequenceLengthCode {
    var out: [source.len]SequenceLengthCode = undefined;
    for (source, 0..) |entry, idx| {
        out[idx] = .{ .base = entry[0], .extra_bits = entry[1] };
    }
    return out;
}

const FastLiteralSource = union(enum) {
    bytes: struct {
        data: []const u8,
        pos: usize = 0,
    },
    rle: struct {
        byte: u8,
        len: usize,
        pos: usize = 0,
    },
    huffman: FastHuffmanLiteralSource,

    fn len(self: *const FastLiteralSource) usize {
        return switch (self.*) {
            .bytes => |bytes| bytes.data.len,
            .rle => |rle| rle.len,
            .huffman => |huffman| huffman.regenerated_size,
        };
    }

    fn pos(self: *const FastLiteralSource) usize {
        return switch (self.*) {
            .bytes => |bytes| bytes.pos,
            .rle => |rle| rle.pos,
            .huffman => |huffman| huffman.written,
        };
    }

    fn remaining(self: *const FastLiteralSource) !usize {
        const total = self.len();
        const used = self.pos();
        if (used > total) return error.CorruptPage;
        return total - used;
    }

    fn copy(self: *FastLiteralSource, out: []u8) !void {
        switch (self.*) {
            .bytes => |*bytes| {
                if (bytes.pos + out.len > bytes.data.len) return error.CorruptPage;
                @memcpy(out, bytes.data[bytes.pos..][0..out.len]);
                bytes.pos += out.len;
            },
            .rle => |*rle| {
                if (rle.pos + out.len > rle.len) return error.CorruptPage;
                @memset(out, rle.byte);
                rle.pos += out.len;
            },
            .huffman => |*huffman| try huffman.copy(out),
        }
    }

    fn finish(self: *FastLiteralSource) !void {
        if (try self.remaining() != 0) return error.CorruptPage;
        switch (self.*) {
            .huffman => |*huffman| try huffman.finish(),
            else => {},
        }
    }
};

const FastHuffmanLiteralSource = struct {
    tree: *const FastHuffmanTree,
    streams: [4][]const u8,
    stream_count: usize,
    segment_len: usize,
    regenerated_size: usize,
    reader: FastReverseBitReader,
    stream_index: usize = 0,
    stream_written: usize = 0,
    written: usize = 0,

    fn init(header: LiteralsHeader, stream_data: []const u8, tree: *const FastHuffmanTree) !FastHuffmanLiteralSource {
        const streams = try literalStreams(header.size_format, stream_data);
        const stream_count: usize = if (header.size_format == 0) 1 else 4;
        const segment_len = if (stream_count == 1) header.regenerated_size else (header.regenerated_size + 3) / 4;
        if (stream_count == 4 and segment_len * 3 > header.regenerated_size) return error.CorruptPage;
        return .{
            .tree = tree,
            .streams = streams,
            .stream_count = stream_count,
            .segment_len = segment_len,
            .regenerated_size = header.regenerated_size,
            .reader = try FastReverseBitReader.init(streams[0]),
        };
    }

    fn copy(self: *FastHuffmanLiteralSource, out: []u8) !void {
        if (self.written + out.len > self.regenerated_size) return error.CorruptPage;
        for (out) |*byte| {
            if (self.stream_count == 4 and self.stream_written == self.currentStreamLen()) {
                try self.nextStream();
            }
            byte.* = try self.tree.readSymbol(&self.reader);
            self.written += 1;
            self.stream_written += 1;
        }
    }

    fn finish(self: *const FastHuffmanLiteralSource) !void {
        if (self.written != self.regenerated_size) return error.CorruptPage;
        if (!self.reader.isEmpty()) return error.CorruptPage;
    }

    fn currentStreamLen(self: *const FastHuffmanLiteralSource) usize {
        if (self.stream_count == 1 or self.stream_index < 3) return self.segment_len;
        return self.regenerated_size - 3 * self.segment_len;
    }

    fn nextStream(self: *FastHuffmanLiteralSource) !void {
        if (!self.reader.isEmpty()) return error.CorruptPage;
        if (self.stream_index + 1 >= self.stream_count) return error.CorruptPage;
        self.stream_index += 1;
        self.stream_written = 0;
        self.reader = try FastReverseBitReader.init(self.streams[self.stream_index]);
    }
};

fn prepareSequenceLiteralSource(
    block: []const u8,
    literal_start: usize,
    header: LiteralsHeader,
    huffman_state: *FastHuffmanState,
    cache: ?*DecodeCache,
    scratch: ?[]u8,
) !FastLiteralSource {
    return switch (header.block_type) {
        .raw => .{ .bytes = .{ .data = block[literal_start..][0..header.regenerated_size] } },
        .rle => .{ .rle = .{ .byte = block[literal_start], .len = header.regenerated_size } },
        .compressed, .treeless => blk: {
            const compressed_size = header.compressed_size orelse return error.CorruptPage;
            if (scratch) |bytes| {
                if (header.regenerated_size <= bytes.len) {
                    const decoded = bytes[0..header.regenerated_size];
                    try decodeCompressedLiteralsFast(
                        header,
                        block[literal_start..][0..compressed_size],
                        decoded,
                        huffman_state,
                        cache,
                    );
                    break :blk .{ .bytes = .{ .data = decoded } };
                }
            }
            break :blk .{ .huffman = try prepareCompressedLiteralSourceFast(
                header,
                block[literal_start..][0..compressed_size],
                huffman_state,
                cache,
            ) };
        },
    };
}

fn decodeRleSequencesFast(
    literal_source: *FastLiteralSource,
    sequence_bits: []const u8,
    output: []u8,
    out_pos: usize,
    sequence_count: usize,
    literal_code: u8,
    offset_code: u5,
    match_code: u8,
    repeat_offsets: *[3]u32,
) !usize {
    if (literal_source.* == .bytes) {
        return try decodeRleSequencesFastBytes(
            literal_source.bytes.data,
            &literal_source.bytes.pos,
            sequence_bits,
            output,
            out_pos,
            sequence_count,
            literal_code,
            offset_code,
            match_code,
            repeat_offsets,
        );
    }

    const literal_len_code = try literalLengthCode(literal_code);
    const match_len_code = try matchLengthCode(match_code);
    var bit_reader = try FastReverseBitReader.init(sequence_bits);
    var write_pos = out_pos;

    for (0..sequence_count) |_| {
        const offset_extra = try bit_reader.readBitsNoEof(u32, offset_code);
        const offset_value = (@as(u32, 1) << offset_code) + offset_extra;

        const match_extra = try bit_reader.readBitsNoEof(u32, match_len_code.extra_bits);
        const match_len = match_len_code.base + match_extra;

        const literal_extra = try bit_reader.readBitsNoEof(u32, literal_len_code.extra_bits);
        const literal_len = literal_len_code.base + literal_extra;
        const literal_len_usize: usize = @intCast(literal_len);
        const match_len_usize: usize = @intCast(match_len);
        const sequence_len = literal_len_usize + match_len_usize;
        if (write_pos + sequence_len > output.len) return error.CorruptPage;

        const offset = try decodeSequenceOffset(repeat_offsets, offset_value, literal_len);
        const copy_start = std.math.sub(usize, write_pos + literal_len_usize, @as(usize, @intCast(offset))) catch return error.CorruptPage;
        if (copy_start >= write_pos + literal_len_usize) return error.CorruptPage;

        try literal_source.copy(output[write_pos..][0..literal_len_usize]);
        copyMatch(output, write_pos + literal_len_usize, copy_start, match_len_usize);
        write_pos += sequence_len;
    }

    if (!bit_reader.isEmpty()) return error.CorruptPage;
    const remaining_literals = try literal_source.remaining();
    if (write_pos + remaining_literals > output.len) return error.CorruptPage;
    try literal_source.copy(output[write_pos..][0..remaining_literals]);
    try literal_source.finish();
    write_pos += remaining_literals;
    return write_pos - out_pos;
}

fn decodeRleSequencesFastBytes(
    literal_data: []const u8,
    literal_pos: *usize,
    sequence_bits: []const u8,
    output: []u8,
    out_pos: usize,
    sequence_count: usize,
    literal_code: u8,
    offset_code: u5,
    match_code: u8,
    repeat_offsets: *[3]u32,
) !usize {
    const literal_len_code = try literalLengthCode(literal_code);
    const match_len_code = try matchLengthCode(match_code);
    var bit_reader = try FastReverseBitReader.init(sequence_bits);
    var write_pos = out_pos;

    for (0..sequence_count) |_| {
        const offset_extra = try bit_reader.readBitsNoEof(u32, offset_code);
        const offset_value = (@as(u32, 1) << offset_code) + offset_extra;

        const match_extra = try bit_reader.readBitsNoEof(u32, match_len_code.extra_bits);
        const match_len = match_len_code.base + match_extra;

        const literal_extra = try bit_reader.readBitsNoEof(u32, literal_len_code.extra_bits);
        const literal_len = literal_len_code.base + literal_extra;
        const literal_len_usize: usize = @intCast(literal_len);
        const match_len_usize: usize = @intCast(match_len);
        const sequence_len = literal_len_usize + match_len_usize;
        if (write_pos + sequence_len > output.len) return error.CorruptPage;

        const offset = try decodeSequenceOffset(repeat_offsets, offset_value, literal_len);
        const copy_start = std.math.sub(usize, write_pos + literal_len_usize, @as(usize, @intCast(offset))) catch return error.CorruptPage;
        if (copy_start >= write_pos + literal_len_usize) return error.CorruptPage;

        try copyLiteralBytes(literal_data, literal_pos, output[write_pos..][0..literal_len_usize]);
        copyMatch(output, write_pos + literal_len_usize, copy_start, match_len_usize);
        write_pos += sequence_len;
    }

    if (!bit_reader.isEmpty()) return error.CorruptPage;
    const remaining_literals = literal_data.len - literal_pos.*;
    if (write_pos + remaining_literals > output.len) return error.CorruptPage;
    try copyLiteralBytes(literal_data, literal_pos, output[write_pos..][0..remaining_literals]);
    write_pos += remaining_literals;
    return write_pos - out_pos;
}

const FastSequenceKind = enum {
    literal,
    offset,
    match,
};

const FastSequenceTable = union(enum) {
    rle: u8,
    fse: []const FastFseEntry,
};

const FastSequenceState = struct {
    repeat_offsets: [3]u32 = .{
        std_zstd.start_repeated_offset_1,
        std_zstd.start_repeated_offset_2,
        std_zstd.start_repeated_offset_3,
    },

    literal_table: FastSequenceTable = .{ .rle = 0 },
    offset_table: FastSequenceTable = .{ .rle = 0 },
    match_table: FastSequenceTable = .{ .rle = 0 },

    literal_accuracy_log: u5 = 0,
    offset_accuracy_log: u5 = 0,
    match_accuracy_log: u5 = 0,

    literal_state: usize = 0,
    offset_state: usize = 0,
    match_state: usize = 0,

    literal_fse_buffer: [std_zstd.table_size_max.literal]FastFseEntry = undefined,
    offset_fse_buffer: [std_zstd.table_size_max.offset]FastFseEntry = undefined,
    match_fse_buffer: [std_zstd.table_size_max.match]FastFseEntry = undefined,

    fse_tables_defined: bool = false,

    fn prepare(self: *FastSequenceState, header: SequencesHeader, block: []const u8, pos: *usize) !void {
        try self.updateTable(.literal, header.literal_lengths, block, pos);
        try self.updateTable(.offset, header.offsets, block, pos);
        try self.updateTable(.match, header.match_lengths, block, pos);
        self.fse_tables_defined = true;
    }

    fn updateTable(self: *FastSequenceState, comptime kind: FastSequenceKind, mode: SequenceMode, block: []const u8, pos: *usize) !void {
        switch (mode) {
            .predefined => {
                const entries = predefinedSequenceTable(kind);
                self.setTable(kind, .{ .fse = entries }, @intCast(std.math.log2_int(usize, entries.len)));
            },
            .rle => {
                if (pos.* >= block.len) return error.CorruptPage;
                const symbol = block[pos.*];
                pos.* += 1;
                self.setTable(kind, .{ .rle = symbol }, 0);
            },
            .fse => {
                const entries = self.fseBuffer(kind);
                var bit_reader: FastBitReader = .{ .bytes = block[pos.*..] };
                const table_size = try decodeFseTable(
                    &bit_reader,
                    sequenceSymbolCountMax(kind),
                    sequenceAccuracyLogMax(kind),
                    entries,
                );
                pos.* += bit_reader.index;
                self.setTable(kind, .{ .fse = entries[0..table_size] }, @intCast(std.math.log2_int(usize, table_size)));
            },
            .repeat => {
                if (!self.fse_tables_defined) return error.UnsupportedFastPath;
            },
        }
    }

    fn readInitialFseState(self: *FastSequenceState, bit_reader: *FastReverseBitReader) !void {
        self.literal_state = try bit_reader.readBitsNoEof(usize, self.literal_accuracy_log);
        self.offset_state = try bit_reader.readBitsNoEof(usize, self.offset_accuracy_log);
        self.match_state = try bit_reader.readBitsNoEof(usize, self.match_accuracy_log);
        try self.validateState(.literal);
        try self.validateState(.offset);
        try self.validateState(.match);
    }

    fn code(self: *const FastSequenceState, comptime kind: FastSequenceKind) u32 {
        return switch (self.table(kind)) {
            .rle => |symbol| symbol,
            .fse => |entries| entries[self.state(kind)].symbol,
        };
    }

    fn update(self: *FastSequenceState, comptime kind: FastSequenceKind, bit_reader: *FastReverseBitReader) !void {
        switch (self.table(kind)) {
            .rle => {},
            .fse => |entries| {
                const state_value = self.state(kind);
                if (state_value >= entries.len) return error.CorruptPage;
                const entry = entries[state_value];
                const bits = try bit_reader.readBitsNoEof(u16, entry.bits);
                const next_state = @as(usize, entry.baseline) + bits;
                if (next_state >= entries.len) return error.CorruptPage;
                self.setState(kind, next_state);
            },
        }
    }

    fn validateState(self: *const FastSequenceState, comptime kind: FastSequenceKind) !void {
        switch (self.table(kind)) {
            .rle => {},
            .fse => |entries| if (self.state(kind) >= entries.len) return error.CorruptPage,
        }
    }

    fn fseBuffer(self: *FastSequenceState, comptime kind: FastSequenceKind) []FastFseEntry {
        return switch (kind) {
            .literal => &self.literal_fse_buffer,
            .offset => &self.offset_fse_buffer,
            .match => &self.match_fse_buffer,
        };
    }

    fn table(self: *const FastSequenceState, comptime kind: FastSequenceKind) FastSequenceTable {
        return switch (kind) {
            .literal => self.literal_table,
            .offset => self.offset_table,
            .match => self.match_table,
        };
    }

    fn state(self: *const FastSequenceState, comptime kind: FastSequenceKind) usize {
        return switch (kind) {
            .literal => self.literal_state,
            .offset => self.offset_state,
            .match => self.match_state,
        };
    }

    fn setState(self: *FastSequenceState, comptime kind: FastSequenceKind, value: usize) void {
        switch (kind) {
            .literal => self.literal_state = value,
            .offset => self.offset_state = value,
            .match => self.match_state = value,
        }
    }

    fn setTable(self: *FastSequenceState, comptime kind: FastSequenceKind, table_value: FastSequenceTable, accuracy_log: u5) void {
        switch (kind) {
            .literal => {
                self.literal_table = table_value;
                self.literal_accuracy_log = accuracy_log;
            },
            .offset => {
                self.offset_table = table_value;
                self.offset_accuracy_log = accuracy_log;
            },
            .match => {
                self.match_table = table_value;
                self.match_accuracy_log = accuracy_log;
            },
        }
    }
};

const literal_length_default_values = [_]u16{
    5, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 3, 2, 2, 2, 2, 2,
    0, 0, 0, 0,
};

const match_length_default_values = [_]u16{
    2, 5, 4, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0,
    0, 0, 0, 0, 0,
};

const offset_default_values = [_]u16{
    2, 2, 2, 2, 2, 2, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0,
};

fn defaultSequenceValues(comptime kind: FastSequenceKind) []const u16 {
    return switch (kind) {
        .literal => &literal_length_default_values,
        .offset => &offset_default_values,
        .match => &match_length_default_values,
    };
}

fn defaultSequenceTableSize(comptime kind: FastSequenceKind) usize {
    return @as(usize, 1) << switch (kind) {
        .literal => std_zstd.default_accuracy_log.literal,
        .offset => std_zstd.default_accuracy_log.offset,
        .match => std_zstd.default_accuracy_log.match,
    };
}

fn sequenceSymbolCountMax(comptime kind: FastSequenceKind) usize {
    return switch (kind) {
        .literal => std_zstd.table_symbol_count_max.literal,
        .offset => std_zstd.table_symbol_count_max.offset,
        .match => std_zstd.table_symbol_count_max.match,
    };
}

fn sequenceAccuracyLogMax(comptime kind: FastSequenceKind) u4 {
    return switch (kind) {
        .literal => std_zstd.table_accuracy_log_max.literal,
        .offset => std_zstd.table_accuracy_log_max.offset,
        .match => std_zstd.table_accuracy_log_max.match,
    };
}

fn decodeSequencesFast(
    literal_source: *FastLiteralSource,
    sequence_bits: []const u8,
    output: []u8,
    out_pos: usize,
    sequence_count: usize,
    state: *FastSequenceState,
) !usize {
    if (literal_source.* == .bytes) {
        return try decodeSequencesFastBytes(literal_source.bytes.data, &literal_source.bytes.pos, sequence_bits, output, out_pos, sequence_count, state);
    }

    var bit_reader = try FastReverseBitReader.init(sequence_bits);
    try state.readInitialFseState(&bit_reader);
    var write_pos = out_pos;

    for (0..sequence_count) |sequence_index| {
        const offset_code = std.math.cast(u5, state.code(.offset)) orelse return error.CorruptPage;
        const offset_extra = try bit_reader.readBitsNoEof(u32, offset_code);
        const offset_value = (@as(u32, 1) << offset_code) + offset_extra;

        const match_code = state.code(.match);
        const match_len_code = try matchLengthCode(match_code);
        const match_extra = try bit_reader.readBitsNoEof(u32, match_len_code.extra_bits);
        const match_len = match_len_code.base + match_extra;

        const literal_code = state.code(.literal);
        const literal_len_code = try literalLengthCode(literal_code);
        const literal_extra = try bit_reader.readBitsNoEof(u32, literal_len_code.extra_bits);
        const literal_len = literal_len_code.base + literal_extra;
        const literal_len_usize: usize = @intCast(literal_len);
        const match_len_usize: usize = @intCast(match_len);
        const sequence_len = literal_len_usize + match_len_usize;
        if (write_pos + sequence_len > output.len) return error.CorruptPage;

        const offset = try decodeSequenceOffset(&state.repeat_offsets, offset_value, literal_len);
        const copy_start = std.math.sub(usize, write_pos + literal_len_usize, @as(usize, @intCast(offset))) catch return error.CorruptPage;
        if (copy_start >= write_pos + literal_len_usize) return error.CorruptPage;

        try literal_source.copy(output[write_pos..][0..literal_len_usize]);
        copyMatch(output, write_pos + literal_len_usize, copy_start, match_len_usize);
        write_pos += sequence_len;

        if (sequence_index + 1 != sequence_count) {
            try state.update(.literal, &bit_reader);
            try state.update(.match, &bit_reader);
            try state.update(.offset, &bit_reader);
        }
    }

    if (!bit_reader.isEmpty()) return error.CorruptPage;
    const remaining_literals = try literal_source.remaining();
    if (write_pos + remaining_literals > output.len) return error.CorruptPage;
    try literal_source.copy(output[write_pos..][0..remaining_literals]);
    try literal_source.finish();
    write_pos += remaining_literals;
    return write_pos - out_pos;
}

fn decodeSequencesFastBytes(
    literal_data: []const u8,
    literal_pos: *usize,
    sequence_bits: []const u8,
    output: []u8,
    out_pos: usize,
    sequence_count: usize,
    state: *FastSequenceState,
) !usize {
    var bit_reader = try FastReverseBitReader.init(sequence_bits);
    try state.readInitialFseState(&bit_reader);
    var write_pos = out_pos;

    for (0..sequence_count) |sequence_index| {
        const offset_code = std.math.cast(u5, state.code(.offset)) orelse return error.CorruptPage;
        const offset_extra = try bit_reader.readBitsNoEof(u32, offset_code);
        const offset_value = (@as(u32, 1) << offset_code) + offset_extra;

        const match_code = state.code(.match);
        const match_len_code = try matchLengthCode(match_code);
        const match_extra = try bit_reader.readBitsNoEof(u32, match_len_code.extra_bits);
        const match_len = match_len_code.base + match_extra;

        const literal_code = state.code(.literal);
        const literal_len_code = try literalLengthCode(literal_code);
        const literal_extra = try bit_reader.readBitsNoEof(u32, literal_len_code.extra_bits);
        const literal_len = literal_len_code.base + literal_extra;
        const literal_len_usize: usize = @intCast(literal_len);
        const match_len_usize: usize = @intCast(match_len);
        const sequence_len = literal_len_usize + match_len_usize;
        if (write_pos + sequence_len > output.len) return error.CorruptPage;

        const offset = try decodeSequenceOffset(&state.repeat_offsets, offset_value, literal_len);
        const copy_start = std.math.sub(usize, write_pos + literal_len_usize, @as(usize, @intCast(offset))) catch return error.CorruptPage;
        if (copy_start >= write_pos + literal_len_usize) return error.CorruptPage;

        try copyLiteralBytes(literal_data, literal_pos, output[write_pos..][0..literal_len_usize]);
        copyMatch(output, write_pos + literal_len_usize, copy_start, match_len_usize);
        write_pos += sequence_len;

        if (sequence_index + 1 != sequence_count) {
            try state.update(.literal, &bit_reader);
            try state.update(.match, &bit_reader);
            try state.update(.offset, &bit_reader);
        }
    }

    if (!bit_reader.isEmpty()) return error.CorruptPage;
    const remaining_literals = literal_data.len - literal_pos.*;
    if (write_pos + remaining_literals > output.len) return error.CorruptPage;
    try copyLiteralBytes(literal_data, literal_pos, output[write_pos..][0..remaining_literals]);
    write_pos += remaining_literals;
    return write_pos - out_pos;
}

inline fn copyLiteralBytes(data: []const u8, pos: *usize, out: []u8) !void {
    if (pos.* + out.len > data.len) return error.CorruptPage;
    @memcpy(out, data[pos.*..][0..out.len]);
    pos.* += out.len;
}

inline fn copyMatch(output: []u8, dst_start: usize, copy_start: usize, len: usize) void {
    if (len == 0) return;
    const distance = dst_start - copy_start;
    if (distance >= len) {
        @memcpy(output[dst_start..][0..len], output[copy_start..][0..len]);
        return;
    }
    if (distance == 1) {
        @memset(output[dst_start..][0..len], output[copy_start]);
        return;
    }

    var copied = distance;
    @memcpy(output[dst_start..][0..copied], output[copy_start..][0..copied]);
    while (copied < len) {
        const n = @min(copied, len - copied);
        @memcpy(output[dst_start + copied ..][0..n], output[dst_start..][0..n]);
        copied += n;
    }
}

inline fn literalLengthCode(code: u32) !SequenceLengthCode {
    if (code >= literal_length_code_table.len) return error.CorruptPage;
    return literal_length_code_table[@intCast(code)];
}

inline fn matchLengthCode(code: u32) !SequenceLengthCode {
    if (code >= match_length_code_table.len) return error.CorruptPage;
    return match_length_code_table[@intCast(code)];
}

inline fn sequenceLengthCode(table: anytype, code: u32) !SequenceLengthCode {
    if (code >= table.len) return error.CorruptPage;
    const entry = table[@intCast(code)];
    return .{ .base = entry[0], .extra_bits = entry[1] };
}

inline fn decodeSequenceOffset(repeat_offsets: *[3]u32, offset_value: u32, literal_len: u32) !u32 {
    const offset = if (offset_value > 3) offset: {
        const decoded = offset_value - 3;
        updateRepeatOffset(repeat_offsets, decoded);
        break :offset decoded;
    } else offset: {
        if (literal_len == 0) {
            if (offset_value == 3) {
                const decoded = std.math.sub(u32, repeat_offsets[0], 1) catch return error.CorruptPage;
                updateRepeatOffset(repeat_offsets, decoded);
                break :offset decoded;
            }
            break :offset useRepeatOffset(repeat_offsets, offset_value);
        }
        break :offset useRepeatOffset(repeat_offsets, offset_value - 1);
    };
    if (offset == 0) return error.CorruptPage;
    return offset;
}

inline fn updateRepeatOffset(repeat_offsets: *[3]u32, offset: u32) void {
    repeat_offsets[2] = repeat_offsets[1];
    repeat_offsets[1] = repeat_offsets[0];
    repeat_offsets[0] = offset;
}

inline fn useRepeatOffset(repeat_offsets: *[3]u32, index: u32) u32 {
    if (index == 1) {
        std.mem.swap(u32, &repeat_offsets[0], &repeat_offsets[1]);
    } else if (index == 2) {
        std.mem.swap(u32, &repeat_offsets[0], &repeat_offsets[2]);
        std.mem.swap(u32, &repeat_offsets[1], &repeat_offsets[2]);
    }
    return repeat_offsets[0];
}

fn readLiteralsHeader(block: []const u8) !LiteralsHeader {
    if (block.len == 0) return error.CorruptPage;
    var pos: usize = 1;
    const byte0 = block[0];
    const block_type: LiteralBlockType = @enumFromInt(byte0 & 0x03);
    const size_format: u2 = @intCast((byte0 >> 2) & 0x03);
    var regenerated_size: usize = undefined;
    var compressed_size: ?usize = null;
    switch (block_type) {
        .raw, .rle => switch (size_format) {
            0, 2 => regenerated_size = byte0 >> 3,
            1 => {
                if (pos + 1 > block.len) return error.CorruptPage;
                regenerated_size = (byte0 >> 4) + (@as(usize, block[pos]) << 4);
                pos += 1;
            },
            3 => {
                if (pos + 2 > block.len) return error.CorruptPage;
                regenerated_size = (byte0 >> 4) +
                    (@as(usize, block[pos]) << 4) +
                    (@as(usize, block[pos + 1]) << 12);
                pos += 2;
            },
        },
        .compressed, .treeless => {
            if (pos + 2 > block.len) return error.CorruptPage;
            const byte1 = block[pos];
            const byte2 = block[pos + 1];
            pos += 2;
            switch (size_format) {
                0, 1 => {
                    regenerated_size = (byte0 >> 4) + ((@as(usize, byte1) & 0x3f) << 4);
                    compressed_size = ((@as(usize, byte1) & 0xc0) >> 6) + (@as(usize, byte2) << 2);
                },
                2 => {
                    if (pos + 1 > block.len) return error.CorruptPage;
                    const byte3 = block[pos];
                    pos += 1;
                    regenerated_size = (byte0 >> 4) + (@as(usize, byte1) << 4) + ((@as(usize, byte2) & 0x03) << 12);
                    compressed_size = ((@as(usize, byte2) & 0xfc) >> 2) + (@as(usize, byte3) << 6);
                },
                3 => {
                    if (pos + 2 > block.len) return error.CorruptPage;
                    const byte3 = block[pos];
                    const byte4 = block[pos + 1];
                    pos += 2;
                    regenerated_size = (byte0 >> 4) + (@as(usize, byte1) << 4) + ((@as(usize, byte2) & 0x3f) << 12);
                    compressed_size = ((@as(usize, byte2) & 0xc0) >> 6) + (@as(usize, byte3) << 2) + (@as(usize, byte4) << 10);
                },
            }
        },
    }
    if (regenerated_size > block_size_max) return error.CorruptPage;
    return .{
        .block_type = block_type,
        .size_format = size_format,
        .regenerated_size = regenerated_size,
        .compressed_size = compressed_size,
        .consumed = pos,
    };
}

fn decodeCompressedLiteralsFast(header: LiteralsHeader, bytes: []const u8, output: []u8, huffman_state: *FastHuffmanState, cache: ?*DecodeCache) !void {
    var pos: usize = 0;
    const tree = switch (header.block_type) {
        .compressed => blk: {
            const tree_len = try encodedHuffmanTreeLen(bytes[pos..]);
            const tree_bytes = bytes[pos..][0..tree_len];
            pos += tree_len;
            if (cache) |c| {
                if (c.find(tree_bytes)) |cached| break :blk huffman_state.setCurrent(cached);
                var decode_pos: usize = 0;
                const decoded = try decodeFastHuffmanTree(tree_bytes, &decode_pos);
                if (decode_pos != tree_bytes.len) return error.CorruptPage;
                const cached = c.put(tree_bytes, decoded);
                break :blk huffman_state.setCurrent(cached);
            }
            var decode_pos: usize = 0;
            const decoded = try decodeFastHuffmanTree(tree_bytes, &decode_pos);
            if (decode_pos != tree_bytes.len) return error.CorruptPage;
            break :blk huffman_state.setLocal(decoded);
        },
        .treeless => try huffman_state.requireCurrent(),
        else => return error.CorruptPage,
    };
    if (pos > bytes.len) return error.CorruptPage;
    const streams = try literalStreams(header.size_format, bytes[pos..]);
    try tree.decode(streams[0..], output);
}

fn prepareCompressedLiteralSourceFast(header: LiteralsHeader, bytes: []const u8, huffman_state: *FastHuffmanState, cache: ?*DecodeCache) !FastHuffmanLiteralSource {
    var pos: usize = 0;
    const tree = switch (header.block_type) {
        .compressed => blk: {
            const tree_len = try encodedHuffmanTreeLen(bytes[pos..]);
            const tree_bytes = bytes[pos..][0..tree_len];
            pos += tree_len;
            if (cache) |c| {
                if (c.find(tree_bytes)) |cached| break :blk huffman_state.setCurrent(cached);
                var decode_pos: usize = 0;
                const decoded = try decodeFastHuffmanTree(tree_bytes, &decode_pos);
                if (decode_pos != tree_bytes.len) return error.CorruptPage;
                const cached = c.put(tree_bytes, decoded);
                break :blk huffman_state.setCurrent(cached);
            }
            var decode_pos: usize = 0;
            const decoded = try decodeFastHuffmanTree(tree_bytes, &decode_pos);
            if (decode_pos != tree_bytes.len) return error.CorruptPage;
            break :blk huffman_state.setLocal(decoded);
        },
        .treeless => try huffman_state.requireCurrent(),
        else => return error.CorruptPage,
    };
    if (pos > bytes.len) return error.CorruptPage;
    return try FastHuffmanLiteralSource.init(header, bytes[pos..], tree);
}

fn encodedHuffmanTreeLen(bytes: []const u8) !usize {
    if (bytes.len == 0) return error.CorruptPage;
    const header = bytes[0];
    const len: usize = if (header < 128)
        1 + @as(usize, header)
    else blk: {
        const encoded_symbol_count: usize = header - 127;
        break :blk 1 + (encoded_symbol_count + 1) / 2;
    };
    if (len > bytes.len or len > DecodeCache.max_key_len) return error.CorruptPage;
    return len;
}

fn literalStreams(size_format: u2, data: []const u8) ![4][]const u8 {
    if (size_format == 0) return .{ data, &.{}, &.{}, &.{} };
    if (data.len < 6) return error.CorruptPage;
    const len1 = std.mem.readInt(u16, data[0..2], .little);
    const len2 = std.mem.readInt(u16, data[2..4], .little);
    const len3 = std.mem.readInt(u16, data[4..6], .little);
    const start1: usize = 6;
    const start2 = start1 + @as(usize, len1);
    const start3 = start2 + @as(usize, len2);
    const start4 = start3 + @as(usize, len3);
    if (start4 > data.len) return error.CorruptPage;
    return .{
        data[start1..start2],
        data[start2..start3],
        data[start3..start4],
        data[start4..],
    };
}

const FastHuffmanEntry = u16;

fn fastHuffmanEntry(symbol: u8, bit_count: u4) FastHuffmanEntry {
    return @as(u16, symbol) | (@as(u16, bit_count) << 8);
}

fn fastHuffmanSymbol(entry: FastHuffmanEntry) u8 {
    return @intCast(entry & 0xff);
}

fn fastHuffmanBitCount(entry: FastHuffmanEntry) u4 {
    return @intCast((entry >> 8) & 0x0f);
}

const FastHuffmanState = struct {
    local: ?FastHuffmanTree = null,
    current: ?*const FastHuffmanTree = null,

    fn setLocal(self: *FastHuffmanState, tree: FastHuffmanTree) *const FastHuffmanTree {
        self.local = tree;
        self.current = &self.local.?;
        return self.current.?;
    }

    fn setCurrent(self: *FastHuffmanState, tree: *const FastHuffmanTree) *const FastHuffmanTree {
        self.current = tree;
        return tree;
    }

    fn requireCurrent(self: *const FastHuffmanState) !*const FastHuffmanTree {
        return self.current orelse error.UnsupportedFastPath;
    }
};

pub const DecodeCache = struct {
    const max_entries = 8;
    const max_key_len = 128;

    const Entry = struct {
        key: [max_key_len]u8 = undefined,
        key_len: u8 = 0,
        tree: FastHuffmanTree = .{},
    };

    entries: [max_entries]Entry = [_]Entry{.{}} ** max_entries,
    next: usize = 0,

    fn find(self: *const DecodeCache, key: []const u8) ?*const FastHuffmanTree {
        if (key.len > max_key_len) return null;
        for (&self.entries) |*entry| {
            if (entry.key_len == key.len and std.mem.eql(u8, entry.key[0..entry.key_len], key)) return &entry.tree;
        }
        return null;
    }

    fn put(self: *DecodeCache, key: []const u8, tree: FastHuffmanTree) *const FastHuffmanTree {
        const idx = self.next;
        self.next = (self.next + 1) % self.entries.len;
        var entry = &self.entries[idx];
        entry.key_len = @intCast(key.len);
        @memcpy(entry.key[0..key.len], key);
        entry.tree = tree;
        return &entry.tree;
    }
};

const FastHuffmanTree = struct {
    lookup: [1 << 11]FastHuffmanEntry = [_]FastHuffmanEntry{0} ** (1 << 11),
    max_bits: u4 = 0,

    fn build(input_weights: *[256]u4, symbol_count: usize) !FastHuffmanTree {
        if (symbol_count == 0 or symbol_count > input_weights.len) return error.CorruptPage;
        var weights = input_weights.*;
        var weight_power_sum_big: u32 = 0;
        for (weights[0 .. symbol_count - 1]) |value| {
            weight_power_sum_big += (@as(u16, 1) << value) >> 1;
        }
        if (weight_power_sum_big >= 1 << 11) return error.CorruptPage;
        const weight_power_sum: u16 = @intCast(weight_power_sum_big);
        const max_bit_count: u4 = @intCast(if (weight_power_sum == 0) 1 else std.math.log2_int(u16, weight_power_sum) + 1);
        const next_power = @as(u16, 1) << max_bit_count;
        weights[symbol_count - 1] = @intCast(std.math.log2_int(u16, next_power - weight_power_sum) + 1);

        var counts: [16]usize = [_]usize{0} ** 16;
        for (weights[0..symbol_count]) |weight| {
            if (weight != 0) counts[weight] += 1;
        }

        var offsets: [17]usize = [_]usize{0} ** 17;
        var weight_index: usize = 1;
        while (weight_index < counts.len) : (weight_index += 1) {
            offsets[weight_index + 1] = offsets[weight_index] + counts[weight_index];
        }
        if (offsets[counts.len] > symbol_count) return error.CorruptPage;

        var cursors = offsets;
        var bucketed: [256]u8 = undefined;
        for (weights[0..symbol_count], 0..) |weight, symbol| {
            if (weight == 0) continue;
            const idx = cursors[weight];
            bucketed[idx] = @intCast(symbol);
            cursors[weight] += 1;
        }

        var tree: FastHuffmanTree = .{ .max_bits = max_bit_count };
        var prefix: u16 = 0;
        var previous_weight: u4 = 0;
        var weight: u4 = 1;
        while (weight <= max_bit_count) : (weight += 1) {
            const count = counts[weight];
            if (count == 0) continue;
            if (previous_weight != 0) {
                prefix = ((prefix - 1) >> @intCast(weight - previous_weight)) + 1;
            }
            previous_weight = weight;
            const bit_count: u4 = (max_bit_count + 1) - weight;
            const start = offsets[weight];
            for (bucketed[start..][0..count]) |symbol| {
                try tree.fill(prefix, bit_count, symbol);
                prefix += 1;
            }
        }
        return tree;
    }

    fn fill(self: *FastHuffmanTree, prefix: u16, bit_count: u4, symbol: u8) !void {
        if (bit_count == 0 or bit_count > self.max_bits or self.max_bits > 11) return error.CorruptPage;
        const suffix_bits: u4 = self.max_bits - bit_count;
        const start = @as(usize, prefix) << suffix_bits;
        const count = @as(usize, 1) << suffix_bits;
        const entry = fastHuffmanEntry(symbol, bit_count);
        for (start..start + count) |idx| {
            self.lookup[idx] = entry;
        }
    }

    fn decode(self: *const FastHuffmanTree, streams: []const []const u8, output: []u8) !void {
        const stream_count: usize = if (streams.len > 1 and streams[1].len != 0) 4 else 1;
        if (stream_count == 1) {
            try self.decodeStream(streams[0], output);
            return;
        }

        const segment_len = (output.len + 3) / 4;
        if (segment_len * 3 > output.len) return error.CorruptPage;
        try self.decodeStreams4(
            streams,
            output[0..segment_len],
            output[segment_len..][0..segment_len],
            output[2 * segment_len ..][0..segment_len],
            output[3 * segment_len ..],
        );
    }

    fn decodeStream(self: *const FastHuffmanTree, stream: []const u8, output: []u8) !void {
        var reader = try FastReverseBitReader.init(stream);
        for (output) |*byte| byte.* = try self.readSymbol(&reader);
        if (!reader.isEmpty()) return error.CorruptPage;
    }

    fn decodeStreams4(self: *const FastHuffmanTree, streams: []const []const u8, out0: []u8, out1: []u8, out2: []u8, out3: []u8) !void {
        var reader0 = try FastReverseBitReader.init(streams[0]);
        var reader1 = try FastReverseBitReader.init(streams[1]);
        var reader2 = try FastReverseBitReader.init(streams[2]);
        var reader3 = try FastReverseBitReader.init(streams[3]);

        var idx: usize = 0;
        while (idx < out3.len) : (idx += 1) {
            out0[idx] = try self.readSymbol(&reader0);
            out1[idx] = try self.readSymbol(&reader1);
            out2[idx] = try self.readSymbol(&reader2);
            out3[idx] = try self.readSymbol(&reader3);
        }
        while (idx < out0.len) : (idx += 1) {
            out0[idx] = try self.readSymbol(&reader0);
            out1[idx] = try self.readSymbol(&reader1);
            out2[idx] = try self.readSymbol(&reader2);
        }

        if (!reader0.isEmpty() or !reader1.isEmpty() or !reader2.isEmpty() or !reader3.isEmpty()) return error.CorruptPage;
    }

    inline fn readSymbol(self: *const FastHuffmanTree, reader: *FastReverseBitReader) !u8 {
        if (!reader.ensure(self.max_bits)) {
            return try self.readSymbolSlow(reader);
        }
        const prefix = reader.peek(self.max_bits);
        const entry = self.lookup[prefix];
        const bit_count = fastHuffmanBitCount(entry);
        if (bit_count == 0) return error.CorruptPage;
        reader.drop(bit_count);
        return fastHuffmanSymbol(entry);
    }

    fn readSymbolSlow(self: *const FastHuffmanTree, reader: *FastReverseBitReader) !u8 {
        var prefix: usize = 0;
        var bit_count: u4 = 0;
        while (bit_count < self.max_bits) {
            prefix = (prefix << 1) | @as(usize, try reader.readBit());
            bit_count += 1;
            const idx = prefix << @intCast(self.max_bits - bit_count);
            const entry = self.lookup[idx];
            if (fastHuffmanBitCount(entry) == bit_count) return fastHuffmanSymbol(entry);
        }
        return error.CorruptPage;
    }
};

const FastReverseBitReader = struct {
    bytes: []const u8,
    remaining: usize,
    bits: u64 = 0,
    count: u6 = 0,

    fn init(bytes: []const u8) !FastReverseBitReader {
        var result: FastReverseBitReader = .{
            .bytes = bytes,
            .remaining = bytes.len,
        };
        if (bytes.len == 0) return result;
        var i: usize = 0;
        while (i < 8) : (i += 1) {
            if (try result.readBit() != 0) return result;
        }
        return error.CorruptPage;
    }

    inline fn ensure(self: *FastReverseBitReader, needed: u16) bool {
        while (self.count < needed) {
            if (self.remaining == 0) return false;
            self.remaining -= 1;
            if (self.count > 56) return false;
            self.bits |= @as(u64, self.bytes[self.remaining]) << @intCast(56 - self.count);
            self.count += 8;
        }
        return true;
    }

    inline fn peek(self: *const FastReverseBitReader, needed: u16) u64 {
        return self.bits >> @intCast(64 - needed);
    }

    inline fn drop(self: *FastReverseBitReader, used: u16) void {
        self.bits <<= @intCast(used);
        self.count -= @intCast(used);
        if (self.count == 0) self.bits = 0;
    }

    fn readBit(self: *FastReverseBitReader) !u1 {
        if (!self.ensure(1)) return error.EndOfStream;
        const bit: u1 = @intCast(self.peek(1));
        self.drop(1);
        return bit;
    }

    fn readBits(self: *FastReverseBitReader, comptime T: type, count: u16, out_bits: *u16) !T {
        var out: T = 0;
        var read: u16 = 0;
        while (read < count) {
            if (!self.ensure(1)) break;
            const take = @min(count - read, @as(u16, self.count));
            out = (out << @intCast(take)) | @as(T, @intCast(self.peek(take)));
            self.drop(take);
            read += take;
        }
        out_bits.* = read;
        return out;
    }

    inline fn readBitsNoEof(self: *FastReverseBitReader, comptime T: type, count: u16) !T {
        if (count == 0) return 0;
        if (!self.ensure(count)) return error.EndOfStream;
        const value = self.peek(count);
        self.drop(count);
        return @intCast(value);
    }

    fn isEmpty(self: *const FastReverseBitReader) bool {
        return self.remaining == 0 and self.count == 0;
    }
};

fn decodeFastHuffmanTree(bytes: []const u8, pos: *usize) !FastHuffmanTree {
    if (pos.* >= bytes.len) return error.CorruptPage;
    const header = bytes[pos.*];
    pos.* += 1;
    var weights: [256]u4 = undefined;
    var symbol_count: usize = undefined;
    if (header < 128) {
        if (pos.* + header > bytes.len) return error.CorruptPage;
        symbol_count = try decodeFseWeights(bytes[pos.*..][0..header], &weights);
        pos.* += header;
    } else {
        const encoded_symbol_count: usize = header - 127;
        const weight_bytes = (encoded_symbol_count + 1) / 2;
        if (pos.* + weight_bytes > bytes.len) return error.CorruptPage;
        for (0..weight_bytes) |idx| {
            const byte = bytes[pos.* + idx];
            weights[2 * idx] = @intCast(byte >> 4);
            weights[2 * idx + 1] = @intCast(byte & 0x0f);
        }
        pos.* += weight_bytes;
        symbol_count = encoded_symbol_count + 1;
    }
    return try FastHuffmanTree.build(&weights, symbol_count);
}

const FastFseEntry = struct {
    symbol: u8,
    baseline: u16,
    bits: u8,
};

const predefined_literal_sequence_table = buildDefaultSequenceTable(.literal);
const predefined_offset_sequence_table = buildDefaultSequenceTable(.offset);
const predefined_match_sequence_table = buildDefaultSequenceTable(.match);

fn buildDefaultSequenceTable(comptime kind: FastSequenceKind) [defaultSequenceTableSize(kind)]FastFseEntry {
    @setEvalBranchQuota(20_000);
    var entries: [defaultSequenceTableSize(kind)]FastFseEntry = undefined;
    buildFseTable(defaultSequenceValues(kind), &entries) catch @compileError("invalid zstd default sequence table");
    return entries;
}

fn predefinedSequenceTable(comptime kind: FastSequenceKind) []const FastFseEntry {
    return switch (kind) {
        .literal => &predefined_literal_sequence_table,
        .offset => &predefined_offset_sequence_table,
        .match => &predefined_match_sequence_table,
    };
}

fn decodeFseWeights(bytes: []const u8, weights: *[256]u4) !usize {
    var bit_reader: FastBitReader = .{ .bytes = bytes };
    var entries: [1 << 6]FastFseEntry = undefined;
    const table_size = try decodeFseTable(&bit_reader, 256, 6, &entries);
    const accuracy_log = std.math.log2_int(usize, table_size);
    const remaining = bit_reader.bytes[bit_reader.index..];
    var huff_bits = try FastReverseBitReader.init(remaining);

    var idx: usize = 0;
    var even_state: u32 = try huff_bits.readBitsNoEof(u32, @intCast(accuracy_log));
    var odd_state: u32 = try huff_bits.readBitsNoEof(u32, @intCast(accuracy_log));
    while (idx < 254) {
        const even_data = entries[even_state];
        var read_bits: u16 = 0;
        const even_bits = huff_bits.readBits(u32, even_data.bits, &read_bits) catch unreachable;
        weights[idx] = std.math.cast(u4, even_data.symbol) orelse return error.CorruptPage;
        idx += 1;
        if (read_bits < even_data.bits) {
            weights[idx] = std.math.cast(u4, entries[odd_state].symbol) orelse return error.CorruptPage;
            idx += 1;
            break;
        }
        even_state = even_data.baseline + even_bits;

        read_bits = 0;
        const odd_data = entries[odd_state];
        const odd_bits = huff_bits.readBits(u32, odd_data.bits, &read_bits) catch unreachable;
        weights[idx] = std.math.cast(u4, odd_data.symbol) orelse return error.CorruptPage;
        idx += 1;
        if (read_bits < odd_data.bits) {
            if (idx == 255) return error.CorruptPage;
            weights[idx] = std.math.cast(u4, entries[even_state].symbol) orelse return error.CorruptPage;
            idx += 1;
            break;
        }
        odd_state = odd_data.baseline + odd_bits;
    } else {
        return error.CorruptPage;
    }
    if (!huff_bits.isEmpty()) return error.CorruptPage;
    return idx + 1;
}

fn decodeFseTable(bit_reader: *FastBitReader, expected_symbol_count: usize, max_accuracy_log: u4, entries: []FastFseEntry) !usize {
    const accuracy_log_biased = try bit_reader.readBitsNoEof(u4, 4);
    if (accuracy_log_biased > max_accuracy_log -| 5) return error.CorruptPage;
    const accuracy_log = accuracy_log_biased + 5;

    var values: [256]u16 = undefined;
    var value_count: usize = 0;
    const total_probability = @as(u16, 1) << accuracy_log;
    var accumulated_probability: u16 = 0;
    while (accumulated_probability < total_probability) {
        const max_bits = std.math.log2_int(u16, total_probability - accumulated_probability + 1) + 1;
        const small = try bit_reader.readBitsNoEof(u16, max_bits - 1);
        const cutoff = (@as(u16, 1) << max_bits) - 1 - (total_probability - accumulated_probability + 1);
        const value = if (small < cutoff)
            small
        else value: {
            const value_read = small + (try bit_reader.readBitsNoEof(u16, 1) << @intCast(max_bits - 1));
            break :value if (value_read < @as(u16, 1) << @intCast(max_bits - 1))
                value_read
            else
                value_read - cutoff;
        };

        accumulated_probability += if (value != 0) value - 1 else 1;
        values[value_count] = value;
        value_count += 1;
        if (value == 1) {
            while (true) {
                const repeat_flag = try bit_reader.readBitsNoEof(u2, 2);
                if (repeat_flag + value_count > values.len) return error.CorruptPage;
                for (0..repeat_flag) |_| {
                    values[value_count] = 1;
                    value_count += 1;
                }
                if (repeat_flag < 3) break;
            }
        }
        if (value_count == values.len) break;
    }
    bit_reader.alignToByte();
    if (value_count < 2 or accumulated_probability != total_probability or value_count > expected_symbol_count) return error.CorruptPage;
    try buildFseTable(values[0..value_count], entries[0..total_probability]);
    return total_probability;
}

fn buildFseTable(values: []const u16, entries: []FastFseEntry) !void {
    const total_probability: u16 = @intCast(entries.len);
    const accuracy_log = std.math.log2_int(u16, total_probability);
    var less_than_one_count: usize = 0;
    for (values, 0..) |value, symbol| {
        if (value == 0) {
            entries[entries.len - 1 - less_than_one_count] = .{
                .symbol = @intCast(symbol),
                .baseline = 0,
                .bits = accuracy_log,
            };
            less_than_one_count += 1;
        }
    }

    var position: usize = 0;
    var temp_states: [1 << 9]u16 = undefined;
    for (values, 0..) |value, symbol| {
        if (value == 0 or value == 1) continue;
        const probability = value - 1;
        const state_share_dividend = std.math.ceilPowerOfTwo(u16, probability) catch return error.CorruptPage;
        const share_size = @divExact(total_probability, state_share_dividend);
        const double_state_count = state_share_dividend - probability;
        const single_state_count = probability - double_state_count;
        const share_size_log = std.math.log2_int(u16, share_size);

        for (0..probability) |idx| {
            temp_states[idx] = @intCast(position);
            position += (entries.len >> 1) + (entries.len >> 3) + 3;
            position &= entries.len - 1;
            while (position >= entries.len - less_than_one_count) {
                position += (entries.len >> 1) + (entries.len >> 3) + 3;
                position &= entries.len - 1;
            }
        }
        std.mem.sort(u16, temp_states[0..probability], {}, std.sort.asc(u16));
        for (0..probability) |idx| {
            entries[temp_states[idx]] = if (idx < double_state_count) .{
                .symbol = @intCast(symbol),
                .bits = share_size_log + 1,
                .baseline = single_state_count * share_size + @as(u16, @intCast(idx)) * 2 * share_size,
            } else .{
                .symbol = @intCast(symbol),
                .bits = share_size_log,
                .baseline = (@as(u16, @intCast(idx)) - double_state_count) * share_size,
            };
        }
    }
}

const FastBitReader = struct {
    bytes: []const u8,
    index: usize = 0,
    bits: u64 = 0,
    count: u6 = 0,

    fn ensure(self: *FastBitReader, needed: u16) bool {
        while (self.count < needed) {
            if (self.index >= self.bytes.len) return false;
            self.bits |= @as(u64, self.bytes[self.index]) << @intCast(self.count);
            self.index += 1;
            self.count += 8;
        }
        return true;
    }

    fn readBitsNoEof(self: *FastBitReader, comptime T: type, needed: u16) !T {
        if (!self.ensure(needed)) return error.CorruptPage;
        const mask = if (needed == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(needed)) - 1;
        const value = self.bits & mask;
        self.bits >>= @intCast(needed);
        self.count -= @intCast(needed);
        return @intCast(value);
    }

    fn alignToByte(self: *FastBitReader) void {
        self.bits = 0;
        self.count = 0;
    }
};

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
    const scan_end = @min(plain.len, pos + repeat_scan_window);
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

test "raw zstd frame round-trips through page fast path" {
    const testing = std.testing;
    const input = "zig parquet zstd fast raw frame smoke";
    const compressed = try compressFrame(testing.allocator, input);
    defer testing.allocator.free(compressed);

    var output: [input.len]u8 = undefined;
    var scratch: [input.len + std_zstd.block_size_max]u8 = undefined;
    try decompressWithScratch(compressed, &output, &scratch, input.len);
    try testing.expectEqualStrings(input, &output);
}

test "empty raw zstd frame round-trips" {
    const testing = std.testing;
    const compressed = try compressFrame(testing.allocator, "");
    defer testing.allocator.free(compressed);
    try decompress(testing.allocator, compressed, &.{});
}

test "non-single-segment frame uses advertised window for scratch" {
    const testing = std.testing;
    const input = "hello";
    const compressed = "\x28\xb5\x2f\xfd" ++ "\x00" ++ "\x58" ++ "\x29\x00\x00" ++ input;

    const window_len = try decoderWindowLenBounded(compressed, input.len, 256 * 1024 * 1024);
    try testing.expectEqual(@as(usize, 2 * 1024 * 1024), window_len);
    try testing.expectError(error.CorruptPage, decoderWindowLenBounded(compressed, input.len, 1024));

    var output: [input.len]u8 = undefined;
    const scratch = try testing.allocator.alloc(u8, window_len + std_zstd.block_size_max);
    defer testing.allocator.free(scratch);
    try decompressWithScratch(compressed, &output, scratch, window_len);
    try testing.expectEqualStrings(input, &output);
}

test "rle zstd frame round-trips through page fast path" {
    const testing = std.testing;
    var input: [4096]u8 = undefined;
    @memset(&input, 0xab);

    var frame: std.Io.Writer.Allocating = .init(testing.allocator);
    defer frame.deinit();
    try frame.writer.writeAll("\x28\xb5\x2f\xfd");
    try frame.writer.writeByte(0xe0);
    try frame.writer.writeInt(u64, input.len, .little);
    try writeBlockHeader(&frame.writer, .rle, input.len, true);
    try frame.writer.writeByte(0xab);

    const compressed = try frame.toOwnedSlice();
    defer testing.allocator.free(compressed);

    const header = std.mem.readInt(u24, compressed[13..][0..3], .little);
    try testing.expectEqual(@as(u2, @intFromEnum(BlockType.rle)), @as(u2, @intCast((header >> 1) & 0x3)));

    var output: [input.len]u8 = undefined;
    var scratch: [input.len + std_zstd.block_size_max]u8 = undefined;
    try decompressWithScratch(compressed, &output, &scratch, input.len);
    try testing.expectEqualSlices(u8, &input, &output);
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

    const page_output = try testing.allocator.alloc(u8, input.len);
    defer testing.allocator.free(page_output);
    const scratch = try testing.allocator.alloc(u8, input.len + std_zstd.block_size_max);
    defer testing.allocator.free(scratch);
    try decompressWithScratch(compressed, page_output, scratch, input.len);
    try testing.expectEqualSlices(u8, input, page_output);
}
