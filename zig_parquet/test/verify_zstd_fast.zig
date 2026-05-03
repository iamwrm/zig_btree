const std = @import("std");
const parquet = @import("parquet");

const max_page_size = 256 * 1024 * 1024;

const PageRole = enum {
    dictionary,
    data,
};

const Stats = struct {
    pages: usize = 0,
    fast_pages: usize = 0,
    unsupported_pages: usize = 0,
    dictionary_pages: usize = 0,
    dictionary_fast_pages: usize = 0,
    dictionary_unsupported_pages: usize = 0,
    data_pages: usize = 0,
    data_fast_pages: usize = 0,
    data_unsupported_pages: usize = 0,
    unsupported_frame: usize = 0,
    unsupported_compressed_literals: usize = 0,
    unsupported_treeless_literals: usize = 0,
    unsupported_reserved_modes: usize = 0,
    unsupported_repeat_first: usize = 0,
    unsupported_other: usize = 0,
    compressed_literal_sequence_blocks: usize = 0,
    compressed_literal_regenerated_bytes: usize = 0,
    compressed_literal_compressed_bytes: usize = 0,
    compressed_literal_sequences: usize = 0,
    compressed_literal_max_regenerated: usize = 0,
    compressed_literal_max_compressed: usize = 0,
    compressed_literal_max_sequences: usize = 0,
    compressed_literal_length_modes: [4]usize = [_]usize{0} ** 4,
    compressed_literal_offset_modes: [4]usize = [_]usize{0} ** 4,
    compressed_literal_match_modes: [4]usize = [_]usize{0} ** 4,

    fn recordPage(self: *Stats, role: PageRole) void {
        self.pages += 1;
        switch (role) {
            .dictionary => self.dictionary_pages += 1,
            .data => self.data_pages += 1,
        }
    }

    fn recordFast(self: *Stats, role: PageRole) void {
        self.fast_pages += 1;
        switch (role) {
            .dictionary => self.dictionary_fast_pages += 1,
            .data => self.data_fast_pages += 1,
        }
    }

    fn recordUnsupported(self: *Stats, role: PageRole, compressed: []const u8) void {
        self.unsupported_pages += 1;
        switch (role) {
            .dictionary => self.dictionary_unsupported_pages += 1,
            .data => self.data_unsupported_pages += 1,
        }
        switch (classifyUnsupported(compressed, self) catch .other) {
            .frame => self.unsupported_frame += 1,
            .compressed_literals => self.unsupported_compressed_literals += 1,
            .treeless_literals => self.unsupported_treeless_literals += 1,
            .reserved_modes => self.unsupported_reserved_modes += 1,
            .repeat_first => self.unsupported_repeat_first += 1,
            .other => self.unsupported_other += 1,
        }
    }

    fn recordCompressedLiteralSequences(
        self: *Stats,
        regenerated_size: usize,
        compressed_size: usize,
        sequence_count: usize,
        literal_mode: u2,
        offset_mode: u2,
        match_mode: u2,
    ) void {
        self.compressed_literal_sequence_blocks += 1;
        self.compressed_literal_regenerated_bytes += regenerated_size;
        self.compressed_literal_compressed_bytes += compressed_size;
        self.compressed_literal_sequences += sequence_count;
        self.compressed_literal_max_regenerated = @max(self.compressed_literal_max_regenerated, regenerated_size);
        self.compressed_literal_max_compressed = @max(self.compressed_literal_max_compressed, compressed_size);
        self.compressed_literal_max_sequences = @max(self.compressed_literal_max_sequences, sequence_count);
        self.compressed_literal_length_modes[literal_mode] += 1;
        self.compressed_literal_offset_modes[offset_mode] += 1;
        self.compressed_literal_match_modes[match_mode] += 1;
    }
};

const UnsupportedReason = enum {
    frame,
    compressed_literals,
    treeless_literals,
    reserved_modes,
    repeat_first,
    other,
};

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingInputPath;
    const verbose = if (args.next()) |arg|
        std.mem.eql(u8, arg, "--verbose")
    else
        false;

    var file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);

    var reader_buffer: [64 * 1024]u8 = undefined;
    var io_reader = file.reader(init.io, &reader_buffer);
    var parsed = try parquet.reader.StreamFileReader.init(init.gpa, &io_reader);
    defer parsed.deinit();

    var stats: Stats = .{};
    for (parsed.metadata.row_groups, 0..) |row_group, rg_idx| {
        _ = rg_idx;
        for (row_group.columns, 0..) |column, col_idx| {
            _ = col_idx;
            if (column.codec != .zstd) continue;

            try verifyDictionaryPage(init.gpa, &parsed, column, &stats);
            const row_count = std.math.cast(usize, row_group.num_rows) orelse return error.CorruptMetadata;
            try verifyDataPages(init.gpa, &parsed, column, row_count, &stats);
        }
    }

    if (verbose) {
        var stdout_buffer: [256]u8 = undefined;
        var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
        const stdout = &stdout_writer.interface;
        try stdout.print(
            "zstd_pages={d}\nfast_pages={d}\nunsupported_pages={d}\n",
            .{ stats.pages, stats.fast_pages, stats.unsupported_pages },
        );
        try stdout.print(
            "dictionary_pages={d}\ndictionary_fast_pages={d}\ndictionary_unsupported_pages={d}\ndata_pages={d}\ndata_fast_pages={d}\ndata_unsupported_pages={d}\n",
            .{
                stats.dictionary_pages,
                stats.dictionary_fast_pages,
                stats.dictionary_unsupported_pages,
                stats.data_pages,
                stats.data_fast_pages,
                stats.data_unsupported_pages,
            },
        );
        try stdout.print(
            "unsupported_frame={d}\nunsupported_compressed_literals={d}\nunsupported_treeless_literals={d}\nunsupported_reserved_modes={d}\nunsupported_repeat_first={d}\nunsupported_other={d}\n",
            .{
                stats.unsupported_frame,
                stats.unsupported_compressed_literals,
                stats.unsupported_treeless_literals,
                stats.unsupported_reserved_modes,
                stats.unsupported_repeat_first,
                stats.unsupported_other,
            },
        );
        try stdout.print(
            "compressed_literal_sequence_blocks={d}\ncompressed_literal_regenerated_bytes={d}\ncompressed_literal_compressed_bytes={d}\ncompressed_literal_sequences={d}\n",
            .{
                stats.compressed_literal_sequence_blocks,
                stats.compressed_literal_regenerated_bytes,
                stats.compressed_literal_compressed_bytes,
                stats.compressed_literal_sequences,
            },
        );
        try stdout.print(
            "compressed_literal_max_regenerated={d}\ncompressed_literal_max_compressed={d}\ncompressed_literal_max_sequences={d}\n",
            .{
                stats.compressed_literal_max_regenerated,
                stats.compressed_literal_max_compressed,
                stats.compressed_literal_max_sequences,
            },
        );
        try stdout.print(
            "compressed_literal_length_modes=predefined:{d} rle:{d} fse:{d} repeat:{d}\n",
            .{
                stats.compressed_literal_length_modes[0],
                stats.compressed_literal_length_modes[1],
                stats.compressed_literal_length_modes[2],
                stats.compressed_literal_length_modes[3],
            },
        );
        try stdout.print(
            "compressed_literal_offset_modes=predefined:{d} rle:{d} fse:{d} repeat:{d}\n",
            .{
                stats.compressed_literal_offset_modes[0],
                stats.compressed_literal_offset_modes[1],
                stats.compressed_literal_offset_modes[2],
                stats.compressed_literal_offset_modes[3],
            },
        );
        try stdout.print(
            "compressed_literal_match_modes=predefined:{d} rle:{d} fse:{d} repeat:{d}\n",
            .{
                stats.compressed_literal_match_modes[0],
                stats.compressed_literal_match_modes[1],
                stats.compressed_literal_match_modes[2],
                stats.compressed_literal_match_modes[3],
            },
        );
        try stdout.flush();
    }
}

fn verifyDictionaryPage(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, column: parquet.types.ColumnChunkMeta, stats: *Stats) !void {
    const offset_i64 = column.dictionary_page_offset orelse return;
    const offset = std.math.cast(u64, offset_i64) orelse return error.CorruptMetadata;
    try parsed.file_reader.seekTo(offset);
    const header = try parquet.thrift.readPageHeader(&parsed.file_reader.interface);
    if (header.page_type != .dictionary_page) return error.CorruptPage;
    const compressed_size = try checkedPageSize(header.compressed_page_size);
    const uncompressed_size = try checkedPageSize(header.uncompressed_page_size);
    const data_offset = parsed.file_reader.logicalPos();
    try verifyZstdFrame(allocator, parsed, data_offset, compressed_size, uncompressed_size, .dictionary, stats);
}

fn verifyDataPages(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, column: parquet.types.ColumnChunkMeta, row_count: usize, stats: *Stats) !void {
    var rows_seen: usize = 0;
    var offset = std.math.cast(u64, column.data_page_offset) orelse return error.CorruptMetadata;
    while (rows_seen < row_count) {
        try parsed.file_reader.seekTo(offset);
        const header = try parquet.thrift.readPageHeader(&parsed.file_reader.interface);
        const page_rows = try pageRowCount(header);
        if (rows_seen + page_rows > row_count) return error.CorruptPage;

        const compressed_size = try checkedPageSize(header.compressed_page_size);
        const data_offset = parsed.file_reader.logicalPos();
        try verifyPagePayload(allocator, parsed, header, data_offset, compressed_size, stats);

        offset = std.math.add(u64, data_offset, compressed_size) catch return error.CorruptPage;
        rows_seen += page_rows;
    }
}

fn verifyPagePayload(
    allocator: std.mem.Allocator,
    parsed: *parquet.reader.StreamFileReader,
    header: parquet.types.PageHeader,
    data_offset: u64,
    compressed_size: usize,
    stats: *Stats,
) !void {
    const uncompressed_size = try checkedPageSize(header.uncompressed_page_size);
    switch (header.page_type) {
        .data_page => try verifyZstdFrame(allocator, parsed, data_offset, compressed_size, uncompressed_size, .data, stats),
        .data_page_v2 => {
            const data_header = header.data_page_header_v2 orelse return error.CorruptPage;
            if (!data_header.is_compressed) return;
            const repetition_len = try checkedPageSize(data_header.repetition_levels_byte_length);
            const definition_len = try checkedPageSize(data_header.definition_levels_byte_length);
            const levels_len = std.math.add(usize, repetition_len, definition_len) catch return error.CorruptPage;
            if (levels_len > compressed_size or levels_len > uncompressed_size) return error.CorruptPage;
            try verifyZstdFrame(
                allocator,
                parsed,
                std.math.add(u64, data_offset, @intCast(levels_len)) catch return error.CorruptPage,
                compressed_size - levels_len,
                uncompressed_size - levels_len,
                .data,
                stats,
            );
        },
        else => return error.UnsupportedPageType,
    }
}

fn verifyZstdFrame(
    allocator: std.mem.Allocator,
    parsed: *parquet.reader.StreamFileReader,
    data_offset: u64,
    compressed_size: usize,
    uncompressed_size: usize,
    role: PageRole,
    stats: *Stats,
) !void {
    const compressed = try allocator.alloc(u8, compressed_size);
    defer allocator.free(compressed);
    try parsed.file_reader.seekTo(data_offset);
    try parsed.file_reader.interface.readSliceAll(compressed);

    const std_out = try allocator.alloc(u8, uncompressed_size);
    defer allocator.free(std_out);
    const fast_out = try allocator.alloc(u8, uncompressed_size);
    defer allocator.free(fast_out);

    const scratch_len = std.math.add(usize, @max(uncompressed_size, 1), std.compress.zstd.block_size_max) catch return error.CorruptPage;
    const scratch = try allocator.alloc(u8, scratch_len);
    defer allocator.free(scratch);

    try decompressStd(compressed, std_out, scratch, @max(uncompressed_size, 1));

    stats.recordPage(role);
    var cache: parquet.zstd.DecodeCache = .{};
    parquet.zstd.tryDecompressFastWithScratchForTesting(compressed, fast_out, &cache, scratch) catch |err| switch (err) {
        error.UnsupportedFastPath => {
            stats.recordUnsupported(role, compressed);
            return;
        },
        else => |e| return e,
    };

    stats.recordFast(role);
    if (!std.mem.eql(u8, std_out, fast_out)) return error.FastZstdMismatch;
}

fn decompressStd(compressed: []const u8, output: []u8, scratch: []u8, window_len: usize) !void {
    const window_len_u32 = std.math.cast(u32, window_len) orelse return error.CorruptPage;
    var in: std.Io.Reader = .fixed(compressed);
    var stream: std.compress.zstd.Decompress = .init(&in, scratch, .{ .window_len = window_len_u32 });
    stream.reader.readSliceAll(output) catch return error.CorruptPage;
    const extra = stream.reader.discardRemaining() catch return error.CorruptPage;
    if (extra != 0) return error.CorruptPage;
}

fn checkedPageSize(size: i32) !usize {
    const value = std.math.cast(usize, size) orelse return error.CorruptPage;
    if (value > max_page_size) return error.CorruptPage;
    return value;
}

fn classifyUnsupported(compressed: []const u8, stats: ?*Stats) !UnsupportedReason {
    if (compressed.len < 4 + 1 + 3) return .frame;
    if (!std.mem.eql(u8, compressed[0..4], "\x28\xb5\x2f\xfd")) return .frame;

    var pos: usize = 4;
    const descriptor = compressed[pos];
    pos += 1;
    if ((descriptor & 0x20) == 0 or (descriptor & 0x04) != 0 or (descriptor & 0x03) != 0) return .frame;

    pos += switch (descriptor >> 6) {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        else => unreachable,
    };
    if (pos > compressed.len) return .frame;

    var saw_fse_table = false;
    while (true) {
        if (pos + 3 > compressed.len) return .frame;
        const header = std.mem.readInt(u24, compressed[pos..][0..3], .little);
        pos += 3;

        const last = (header & 1) != 0;
        const block_type = (header >> 1) & 0x3;
        const block_size: usize = @intCast(header >> 3);
        if (pos + block_size > compressed.len) return .frame;

        if (block_type == 2) {
            const reason = try classifyCompressedBlock(compressed[pos..][0..block_size], &saw_fse_table, stats);
            if (reason != .other) return reason;
        }
        pos += block_size;
        if (last) break;
    }

    return .other;
}

fn classifyCompressedBlock(block: []const u8, saw_fse_table: *bool, stats: ?*Stats) !UnsupportedReason {
    if (block.len == 0) return .other;
    var pos: usize = 1;
    const byte0 = block[0];
    const literal_type = byte0 & 0x03;
    const size_format = (byte0 >> 2) & 0x03;
    var regenerated_size: usize = 0;
    var compressed_size: ?usize = null;

    switch (literal_type) {
        0, 1 => switch (size_format) {
            0, 2 => regenerated_size = byte0 >> 3,
            1 => {
                if (pos + 1 > block.len) return .other;
                regenerated_size = (byte0 >> 4) + (@as(usize, block[pos]) << 4);
                pos += 1;
            },
            3 => {
                if (pos + 2 > block.len) return .other;
                regenerated_size = (byte0 >> 4) +
                    (@as(usize, block[pos]) << 4) +
                    (@as(usize, block[pos + 1]) << 12);
                pos += 2;
            },
            else => unreachable,
        },
        2, 3 => {
            if (pos + 2 > block.len) return .other;
            const byte1 = block[pos];
            const byte2 = block[pos + 1];
            pos += 2;
            switch (size_format) {
                0, 1 => {
                    regenerated_size = (byte0 >> 4) + ((@as(usize, byte1) & 0x3f) << 4);
                    compressed_size = ((@as(usize, byte1) & 0xc0) >> 6) + (@as(usize, byte2) << 2);
                },
                2 => {
                    if (pos + 1 > block.len) return .other;
                    const byte3 = block[pos];
                    pos += 1;
                    regenerated_size = (byte0 >> 4) + (@as(usize, byte1) << 4) + ((@as(usize, byte2) & 0x03) << 12);
                    compressed_size = ((@as(usize, byte2) & 0xfc) >> 2) + (@as(usize, byte3) << 6);
                },
                3 => {
                    if (pos + 2 > block.len) return .other;
                    const byte3 = block[pos];
                    const byte4 = block[pos + 1];
                    pos += 2;
                    regenerated_size = (byte0 >> 4) + (@as(usize, byte1) << 4) + ((@as(usize, byte2) & 0x3f) << 12);
                    compressed_size = ((@as(usize, byte2) & 0xc0) >> 6) + (@as(usize, byte3) << 2) + (@as(usize, byte4) << 10);
                },
                else => unreachable,
            }
        },
        else => unreachable,
    }

    switch (literal_type) {
        0 => pos += regenerated_size,
        1 => pos += 1,
        2, 3 => pos += compressed_size orelse return .other,
        else => unreachable,
    }
    if (pos >= block.len) return .other;

    const byte0_seq = block[pos];
    pos += 1;
    if (byte0_seq == 0) return .other;
    const sequence_count: usize = if (byte0_seq < 128) byte0_seq else if (byte0_seq < 255) blk: {
        if (pos >= block.len) return .other;
        const low = block[pos];
        pos += 1;
        break :blk (@as(usize, byte0_seq - 128) << 8) + low;
    } else blk: {
        if (pos + 2 > block.len) return .other;
        const value = @as(usize, block[pos]) + (@as(usize, block[pos + 1]) << 8) + 0x7f00;
        pos += 2;
        break :blk value;
    };
    if (pos >= block.len) return .other;
    const modes = block[pos];
    if ((modes & 0x03) != 0) return .reserved_modes;
    const literal_mode = (modes >> 6) & 0x03;
    const offset_mode = (modes >> 4) & 0x03;
    const match_mode = (modes >> 2) & 0x03;
    if ((literal_mode == 3 or offset_mode == 3 or match_mode == 3) and !saw_fse_table.*) return .repeat_first;
    if (literal_mode == 2 or offset_mode == 2 or match_mode == 2) saw_fse_table.* = true;

    if (literal_type == 2) {
        if (stats) |s| {
            s.recordCompressedLiteralSequences(
                regenerated_size,
                compressed_size orelse return .other,
                sequence_count,
                @intCast(literal_mode),
                @intCast(offset_mode),
                @intCast(match_mode),
            );
        }
        return .compressed_literals;
    }
    if (literal_type == 3) return .treeless_literals;
    return .other;
}

fn pageRowCount(page_header: parquet.types.PageHeader) !usize {
    return switch (page_header.page_type) {
        .data_page => blk: {
            const data_header = page_header.data_page_header orelse return error.CorruptPage;
            break :blk std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage;
        },
        .data_page_v2 => blk: {
            const data_header = page_header.data_page_header_v2 orelse return error.CorruptPage;
            break :blk std.math.cast(usize, data_header.num_values) orelse return error.CorruptPage;
        },
        else => error.UnsupportedPageType,
    };
}
