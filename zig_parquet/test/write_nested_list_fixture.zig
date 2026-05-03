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

    const leaf_path = [_][]const u8{ "a", "list", "element", "list", "element" };
    const outer_path = [_][]const u8{"a"};
    const inner_path = [_][]const u8{ "a", "list", "element" };
    const logical_infos = [_]parquet.NestedLogicalInfo{
        .{
            .kind = .list,
            .definition_level = 1,
            .repetition_level = 0,
            .path = outer_path[0..],
            .optional = true,
        },
        .{
            .kind = .list,
            .definition_level = 3,
            .repetition_level = 1,
            .path = inner_path[0..],
            .optional = true,
        },
    };
    const cols = [_]parquet.Column{
        .{
            .name = "element",
            .path = leaf_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 5,
            .max_repetition_level = 2,
            .nested_logical_info = logical_infos[0..],
            .list_info = .{ .list_definition_level = 3 },
        },
    };
    const schema = parquet.Schema.init("schema", cols[0..]);

    var writer = parquet.writer.StreamWriter.initOptions(init.gpa, &file_writer.interface, schema, .{
        .compression = compression,
        .data_page_version = data_page_version,
        .max_page_rows = 3,
    });
    defer writer.deinit();
    try writer.start();

    const outer_offsets = [_]usize{ 0, 0, 0, 3, 4 };
    const outer_validity = [_]bool{ false, true, true, true };
    const inner_offsets = [_]usize{ 0, 0, 0, 3, 4 };
    const inner_validity = [_]bool{ false, true, true, true };
    const leaf_values = [_]i32{ 1, 2, 3 };
    const leaf_validity = [_]bool{ true, false, true, true };
    const levels = [_]parquet.ColumnNestedListLevelData{
        .{ .offsets = outer_offsets[0..], .validity = outer_validity[0..] },
        .{ .offsets = inner_offsets[0..], .validity = inner_validity[0..] },
    };
    const batch = [_]parquet.ColumnNestedListData{
        .{
            .values = .{ .int32 = .{ .values = leaf_values[0..], .validity = leaf_validity[0..] } },
            .levels = levels[0..],
        },
    };
    try writer.writeRowGroupNestedLists(4, batch[0..]);
    try writer.finish();
}
