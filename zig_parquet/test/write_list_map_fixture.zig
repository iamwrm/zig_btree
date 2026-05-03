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

    const key_path = [_][]const u8{ "a", "list", "element", "key_value", "key" };
    const value_path = [_][]const u8{ "a", "list", "element", "key_value", "value" };
    const list_path = [_][]const u8{"a"};
    const map_path = [_][]const u8{ "a", "list", "element" };
    const nested_logical_infos = [_]parquet.NestedLogicalInfo{
        .{
            .kind = .list,
            .definition_level = 1,
            .repetition_level = 0,
            .path = list_path[0..],
            .optional = true,
        },
        .{
            .kind = .map,
            .definition_level = 3,
            .repetition_level = 1,
            .path = map_path[0..],
            .optional = true,
        },
    };
    const cols = [_]parquet.Column{
        .{
            .name = "key",
            .path = key_path[0..],
            .column_type = .{ .physical = .byte_array, .logical = .string },
            .repetition = .repeated,
            .max_definition_level = 4,
            .max_repetition_level = 2,
            .nested_logical_info = nested_logical_infos[0..],
            .map_info = .{ .map_definition_level = 3 },
        },
        .{
            .name = "value",
            .path = value_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 5,
            .max_repetition_level = 2,
            .nested_logical_info = nested_logical_infos[0..],
            .map_info = .{ .map_definition_level = 3 },
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

    const list_offsets = [_]usize{ 0, 0, 0, 3, 4 };
    const list_validity = [_]bool{ false, true, true, true };
    const map_offsets = [_]usize{ 0, 0, 0, 2, 3 };
    const map_validity = [_]bool{ false, true, true, true };
    const key_values = [_][]const u8{ "aa", "b", "c" };
    const value_values = [_]i32{ 1, 3 };
    const value_validity = [_]bool{ true, false, true };
    const batch = [_]parquet.ColumnListMapData{
        .{
            .list = .{
                .offsets = list_offsets[0..],
                .validity = list_validity[0..],
            },
            .map = .{
                .keys = .{ .byte_array = .{ .values = key_values[0..] } },
                .offsets = map_offsets[0..],
                .validity = map_validity[0..],
            },
            .values = .{ .int32 = .{ .values = value_values[0..], .validity = value_validity[0..] } },
        },
    };
    try writer.writeRowGroupListMaps(4, batch[0..]);
    try writer.finish();
}
