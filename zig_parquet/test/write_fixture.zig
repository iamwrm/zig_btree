const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingOutputPath;
    const codec_arg = args.next() orelse "uncompressed";
    const page_version_arg = args.next() orelse "v1";
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

    var file = try std.Io.Dir.cwd().createFile(init.io, path, .{ .truncate = true });
    defer file.close(init.io);

    var file_buffer: [64 * 1024]u8 = undefined;
    var file_writer = file.writer(init.io, &file_buffer);

    const schema_cols = [_]parquet.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 }, .repetition = .required },
        .{ .name = "score", .column_type = .{ .physical = .double }, .repetition = .required },
        .{ .name = "name", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
        .{ .name = "blob", .column_type = .{ .physical = .fixed_len_byte_array, .type_length = 4 }, .repetition = .required },
        .{ .name = "amount", .column_type = .{ .physical = .int64, .logical = .decimal, .decimal_precision = 9, .decimal_scale = 2 }, .repetition = .required },
    };
    const schema = parquet.Schema.init("schema", &schema_cols);

    var writer = parquet.writer.StreamWriter.initOptions(init.gpa, &file_writer.interface, schema, .{
        .compression = codec,
        .data_page_version = data_page_version,
    });
    defer writer.deinit();
    try writer.start();

    const ids_a = [_]i64{ 1, 2, 3 };
    const scores_a = [_]f64{ 10.5, 20.25, 30.75 };
    const names_a = [_][]const u8{ "ann", "cat" };
    const valid_a = [_]bool{ true, false, true };
    const blobs_a = [_][]const u8{ "aaaa", "bbbb", "cccc" };
    const amounts_a = [_]i64{ 12345, -678, 0 };
    const batch_a = [_]parquet.ColumnData{
        .{ .int64 = .{ .values = ids_a[0..] } },
        .{ .double = .{ .values = scores_a[0..] } },
        .{ .byte_array = .{ .values = names_a[0..], .validity = valid_a[0..] } },
        .{ .fixed_len_byte_array = .{ .values = blobs_a[0..] } },
        .{ .int64 = .{ .values = amounts_a[0..] } },
    };
    try writer.writeRowGroup(ids_a.len, batch_a[0..]);

    const ids_b = [_]i64{ 4, 5 };
    const scores_b = [_]f64{ 40.0, 50.5 };
    const names_b = [_][]const u8{ "dan", "eve" };
    const valid_b = [_]bool{ true, true };
    const blobs_b = [_][]const u8{ "dddd", "eeee" };
    const amounts_b = [_]i64{ 999999, -100 };
    const batch_b = [_]parquet.ColumnData{
        .{ .int64 = .{ .values = ids_b[0..] } },
        .{ .double = .{ .values = scores_b[0..] } },
        .{ .byte_array = .{ .values = names_b[0..], .validity = valid_b[0..] } },
        .{ .fixed_len_byte_array = .{ .values = blobs_b[0..] } },
        .{ .int64 = .{ .values = amounts_b[0..] } },
    };
    try writer.writeRowGroup(ids_b.len, batch_b[0..]);

    try writer.finish();
    try file_writer.end();
}
