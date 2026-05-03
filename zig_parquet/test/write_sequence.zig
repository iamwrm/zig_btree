const std = @import("std");
const parquet = @import("parquet");

const labels = [_][]const u8{
    "alpha", "bravo",    "charlie", "delta",
    "echo",  "foxtrot",  "golf",    "hotel",
    "india", "juliet",   "kilo",    "lima",
    "mike",  "november", "oscar",   "papa",
};

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingOutputPath;
    const rows_arg = args.next() orelse return error.MissingExpectedRows;
    const codec_arg = args.next() orelse "uncompressed";
    const row_group_arg = args.next() orelse "65536";
    const max_page_arg = args.next() orelse "65536";
    const page_version_arg = args.next() orelse "v1";
    const value_encoding_arg = args.next() orelse "plain";

    const rows = try std.fmt.parseInt(usize, rows_arg, 10);
    const row_group_rows = try std.fmt.parseInt(usize, row_group_arg, 10);
    const max_page_rows = try std.fmt.parseInt(usize, max_page_arg, 10);
    if (row_group_rows == 0 or max_page_rows == 0) return error.InvalidColumnData;

    const codec: parquet.CompressionCodec = if (std.mem.eql(u8, codec_arg, "uncompressed"))
        .uncompressed
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
    const use_byte_stream_split = if (std.mem.eql(u8, value_encoding_arg, "plain"))
        false
    else if (std.mem.eql(u8, value_encoding_arg, "byte_stream_split"))
        true
    else
        return error.InvalidColumnData;

    var file = try std.Io.Dir.cwd().createFile(init.io, path, .{ .truncate = true });
    defer file.close(init.io);

    var file_buffer: [64 * 1024]u8 = undefined;
    var file_writer = file.writer(init.io, &file_buffer);

    const schema_cols = [_]parquet.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 }, .repetition = .required },
        .{ .name = "score", .column_type = .{ .physical = .double }, .repetition = .required },
        .{ .name = "name", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = parquet.Schema.init("schema", &schema_cols);

    var writer = parquet.writer.StreamWriter.initOptions(init.gpa, &file_writer.interface, schema, .{
        .compression = codec,
        .max_page_rows = max_page_rows,
        .data_page_version = data_page_version,
        .use_byte_stream_split = use_byte_stream_split,
    });
    defer writer.deinit();
    try writer.start();

    var produced: usize = 0;
    while (produced < rows) {
        const batch_rows = @min(row_group_rows, rows - produced);
        const ids = try init.gpa.alloc(i64, batch_rows);
        defer init.gpa.free(ids);
        const scores = try init.gpa.alloc(f64, batch_rows);
        defer init.gpa.free(scores);
        const validity = try init.gpa.alloc(bool, batch_rows);
        defer init.gpa.free(validity);
        const names = try init.gpa.alloc([]const u8, batch_rows);
        defer init.gpa.free(names);

        var name_count: usize = 0;
        for (0..batch_rows) |i| {
            const row = produced + i;
            ids[i] = @intCast(row);
            scores[i] = @as(f64, @floatFromInt(row)) * 0.25;
            const valid = row % 7 != 0;
            validity[i] = valid;
            if (valid) {
                names[name_count] = labels[row & (labels.len - 1)];
                name_count += 1;
            }
        }

        const batch = [_]parquet.ColumnData{
            .{ .int64 = .{ .values = ids } },
            .{ .double = .{ .values = scores } },
            .{ .byte_array = .{ .values = names[0..name_count], .validity = validity } },
        };
        try writer.writeRowGroup(batch_rows, batch[0..]);
        produced += batch_rows;
    }

    try writer.finish();
    try file_writer.end();
}
