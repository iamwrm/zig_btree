const std = @import("std");
const parquet = @import("parquet");

const labels = [_][]const u8{
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
};

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingOutputPath;
    const rows_arg = args.next() orelse return error.MissingExpectedRows;
    const codec_arg = args.next() orelse "uncompressed";
    const page_version_arg = args.next() orelse "v1";
    const value_encoding_arg = args.next() orelse "plain";
    const row_group_arg = args.next() orelse "257";
    const max_page_arg = args.next() orelse "64";
    const dictionary_arg = args.next() orelse "dict";
    const checksum_arg = args.next() orelse "nocrc";

    const rows = try std.fmt.parseInt(usize, rows_arg, 10);
    const row_group_rows = try std.fmt.parseInt(usize, row_group_arg, 10);
    const max_page_rows = try std.fmt.parseInt(usize, max_page_arg, 10);
    if (row_group_rows == 0 or max_page_rows == 0) return error.InvalidColumnData;

    const codec: parquet.CompressionCodec = if (std.mem.eql(u8, codec_arg, "uncompressed"))
        .uncompressed
    else if (std.mem.eql(u8, codec_arg, "snappy"))
        .snappy
    else if (std.mem.eql(u8, codec_arg, "gzip"))
        .gzip
    else if (std.mem.eql(u8, codec_arg, "lz4") or std.mem.eql(u8, codec_arg, "lz4_raw"))
        .lz4_raw
    else if (std.mem.eql(u8, codec_arg, "zstd"))
        .zstd
    else
        return error.UnsupportedCompression;

    const data_page_version: parquet.DataPageVersion = if (std.mem.eql(u8, page_version_arg, "v1"))
        .v1
    else if (std.mem.eql(u8, page_version_arg, "v2"))
        .v2
    else
        return error.InvalidColumnData;

    const use_byte_stream_split = std.mem.eql(u8, value_encoding_arg, "byte_stream_split") or
        std.mem.eql(u8, value_encoding_arg, "delta_binary_packed+byte_stream_split");
    const use_delta_binary_packed = std.mem.eql(u8, value_encoding_arg, "delta_binary_packed") or
        std.mem.eql(u8, value_encoding_arg, "delta_binary_packed+byte_stream_split");
    const use_delta_length_byte_array = std.mem.eql(u8, value_encoding_arg, "delta_length_byte_array");
    const use_delta_byte_array = std.mem.eql(u8, value_encoding_arg, "delta_byte_array");
    if (!std.mem.eql(u8, value_encoding_arg, "plain") and !use_byte_stream_split and !use_delta_binary_packed and !use_delta_length_byte_array and !use_delta_byte_array) {
        return error.InvalidColumnData;
    }

    const use_dictionary = if (std.mem.eql(u8, dictionary_arg, "dict"))
        true
    else if (std.mem.eql(u8, dictionary_arg, "nodict"))
        false
    else
        return error.InvalidColumnData;

    const page_checksum = if (std.mem.eql(u8, checksum_arg, "nocrc"))
        false
    else if (std.mem.eql(u8, checksum_arg, "crc"))
        true
    else
        return error.InvalidColumnData;

    var file = try std.Io.Dir.cwd().createFile(init.io, path, .{ .truncate = true });
    defer file.close(init.io);

    var file_buffer: [64 * 1024]u8 = undefined;
    var file_writer = file.writer(init.io, &file_buffer);

    const schema_cols = [_]parquet.Column{
        .{ .name = "flag", .column_type = .{ .physical = .boolean }, .repetition = .required },
        .{ .name = "i32", .column_type = .{ .physical = .int32 }, .repetition = .optional },
        .{ .name = "i64", .column_type = .{ .physical = .int64 }, .repetition = .required },
        .{ .name = "f32", .column_type = .{ .physical = .float }, .repetition = .required },
        .{ .name = "f64", .column_type = .{ .physical = .double }, .repetition = .optional },
        .{ .name = "name", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
        .{ .name = "payload", .column_type = .{ .physical = .byte_array }, .repetition = .required },
        .{ .name = "fixed", .column_type = .{ .physical = .fixed_len_byte_array, .type_length = 4 }, .repetition = .required },
    };
    const schema = parquet.Schema.init("schema", &schema_cols);

    var writer = parquet.writer.StreamWriter.initOptions(init.gpa, &file_writer.interface, schema, .{
        .compression = codec,
        .max_page_rows = max_page_rows,
        .use_dictionary = use_dictionary,
        .data_page_version = data_page_version,
        .page_checksum = page_checksum,
        .use_byte_stream_split = use_byte_stream_split,
        .use_delta_binary_packed = use_delta_binary_packed,
        .use_delta_length_byte_array = use_delta_length_byte_array,
        .use_delta_byte_array = use_delta_byte_array,
    });
    defer writer.deinit();
    try writer.start();

    var produced: usize = 0;
    while (produced < rows or (rows == 0 and produced == 0)) {
        const batch_rows = if (rows == 0) 0 else @min(row_group_rows, rows - produced);
        try writeBatch(init.gpa, &writer, produced, batch_rows);
        if (rows == 0) break;
        produced += batch_rows;
    }

    try writer.finish();
    try file_writer.end();
}

fn writeBatch(allocator: std.mem.Allocator, writer: *parquet.writer.StreamWriter, start_row: usize, batch_rows: usize) !void {
    const flags = try allocator.alloc(bool, batch_rows);
    defer allocator.free(flags);
    const i32_validity = try allocator.alloc(bool, batch_rows);
    defer allocator.free(i32_validity);
    const i32_values = try allocator.alloc(i32, batch_rows);
    defer allocator.free(i32_values);
    const i64_values = try allocator.alloc(i64, batch_rows);
    defer allocator.free(i64_values);
    const f32_values = try allocator.alloc(f32, batch_rows);
    defer allocator.free(f32_values);
    const f64_validity = try allocator.alloc(bool, batch_rows);
    defer allocator.free(f64_validity);
    const f64_values = try allocator.alloc(f64, batch_rows);
    defer allocator.free(f64_values);
    const name_validity = try allocator.alloc(bool, batch_rows);
    defer allocator.free(name_validity);
    const names = try allocator.alloc([]const u8, batch_rows);
    defer allocator.free(names);
    const payloads = try allocator.alloc([]const u8, batch_rows);
    defer allocator.free(payloads);
    const fixed_values = try allocator.alloc([]const u8, batch_rows);
    defer allocator.free(fixed_values);
    const fixed_bytes = try allocator.alloc(u8, batch_rows * 4);
    defer allocator.free(fixed_bytes);

    var i32_count: usize = 0;
    var f64_count: usize = 0;
    var name_count: usize = 0;
    var payload_count: usize = 0;

    for (0..batch_rows) |i| {
        const row = start_row + i;
        flags[i] = ((row + (row % 3)) % 3) == 0;
        const i32_valid = row % 5 != 0;
        i32_validity[i] = i32_valid;
        if (i32_valid) {
            i32_values[i32_count] = @as(i32, @intCast(row % 97)) - 48;
            i32_count += 1;
        }
        i64_values[i] = @as(i64, @intCast(row)) * 17 - 12345;
        f32_values[i] = @as(f32, @floatFromInt(row % 101)) * 0.5 - 17.25;

        const f64_valid = row % 7 != 0;
        f64_validity[i] = f64_valid;
        if (f64_valid) {
            f64_values[f64_count] = @as(f64, @floatFromInt(row)) * 0.125 - 99.5;
            f64_count += 1;
        }

        const name_valid = row % 11 != 0;
        name_validity[i] = name_valid;
        if (name_valid) {
            names[name_count] = try std.fmt.allocPrint(allocator, "prefix-{d:0>6}-{s}", .{ row / 3, labels[row & (labels.len - 1)] });
            name_count += 1;
        }

        const payload_len = row % 12;
        const payload = try allocator.alloc(u8, payload_len);
        for (payload, 0..) |*byte, j| byte.* = @intCast((row + j * 13 + 7) & 0xff);
        payloads[payload_count] = payload;
        payload_count += 1;

        const fixed = fixed_bytes[i * 4 ..][0..4];
        for (fixed, 0..) |*byte, j| byte.* = @intCast(((row >> @intCast(j * 2)) + j * 41) & 0xff);
        fixed_values[i] = fixed;
    }
    defer {
        for (names[0..name_count]) |name| allocator.free(name);
        for (payloads[0..payload_count]) |payload| allocator.free(payload);
    }

    const batch = [_]parquet.ColumnData{
        .{ .boolean = .{ .values = flags } },
        .{ .int32 = .{ .values = i32_values[0..i32_count], .validity = i32_validity } },
        .{ .int64 = .{ .values = i64_values } },
        .{ .float = .{ .values = f32_values } },
        .{ .double = .{ .values = f64_values[0..f64_count], .validity = f64_validity } },
        .{ .byte_array = .{ .values = names[0..name_count], .validity = name_validity } },
        .{ .byte_array = .{ .values = payloads[0..payload_count] } },
        .{ .fixed_len_byte_array = .{ .values = fixed_values } },
    };
    try writer.writeRowGroup(batch_rows, batch[0..]);
}
