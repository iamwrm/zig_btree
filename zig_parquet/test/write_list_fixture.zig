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

    const list_path = [_][]const u8{ "items", "list", "element" };
    const cols = [_]parquet.Column{
        .{
            .name = "element",
            .path = list_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 3,
            .max_repetition_level = 1,
            .list_info = .{ .list_definition_level = 1 },
        },
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
    const offsets = [_]usize{ 0, 0, 0, 3, 4 };
    const list_validity = [_]bool{ false, true, true, true };
    const element_validity = [_]bool{ true, false, true, true };
    const batch = [_]parquet.ColumnListData{
        .{
            .values = .{ .int32 = .{ .values = values[0..], .validity = element_validity[0..] } },
            .offsets = offsets[0..],
            .validity = list_validity[0..],
        },
    };
    try writer.writeRowGroupLists(4, batch[0..]);
    try writer.finish();
}
