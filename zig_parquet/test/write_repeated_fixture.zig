const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingOutputPath;
    const codec_arg = args.next() orelse "uncompressed";
    const page_version_arg = args.next() orelse "v1";

    const compression = if (std.mem.eql(u8, codec_arg, "uncompressed"))
        parquet.CompressionCodec.uncompressed
    else if (std.mem.eql(u8, codec_arg, "zstd"))
        parquet.CompressionCodec.zstd
    else
        return error.UnsupportedCompression;

    const data_page_version = if (std.mem.eql(u8, page_version_arg, "v1"))
        parquet.DataPageVersion.v1
    else if (std.mem.eql(u8, page_version_arg, "v2"))
        parquet.DataPageVersion.v2
    else
        return error.InvalidColumnData;

    var file = try std.Io.Dir.cwd().createFile(init.io, path, .{ .truncate = true });
    defer file.close(init.io);

    var writer_buffer: [64 * 1024]u8 = undefined;
    var file_writer = file.writer(init.io, &writer_buffer);

    const cols = [_]parquet.Column{
        .{ .name = "items", .column_type = .{ .physical = .int32 }, .repetition = .repeated },
    };
    const schema = parquet.Schema.init("schema", cols[0..]);

    var writer = parquet.writer.StreamWriter.initOptions(init.gpa, &file_writer.interface, schema, .{
        .compression = compression,
        .data_page_version = data_page_version,
        .max_page_rows = 2,
    });
    defer writer.deinit();
    try writer.start();

    const values = [_]i32{ 10, 11, 20 };
    const definition_levels = [_]u16{ 1, 1, 0, 1, 0 };
    const repetition_levels = [_]u16{ 0, 1, 0, 0, 0 };
    const batch = [_]parquet.ColumnTripletData{
        .{
            .values = .{ .int32 = .{ .values = values[0..] } },
            .definition_levels = definition_levels[0..],
            .repetition_levels = repetition_levels[0..],
        },
    };
    try writer.writeRowGroupTriplets(4, batch[0..]);
    try writer.finish();
}
