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

    const outer_key_path = [_][]const u8{ "a", "key_value", "key" };
    const inner_key_path = [_][]const u8{ "a", "key_value", "value", "key_value", "key" };
    const inner_value_path = [_][]const u8{ "a", "key_value", "value", "key_value", "value" };
    const outer_map_path = [_][]const u8{"a"};
    const inner_map_path = [_][]const u8{ "a", "key_value", "value" };
    const outer_logical = parquet.NestedLogicalInfo{
        .kind = .map,
        .definition_level = 1,
        .repetition_level = 0,
        .path = outer_map_path[0..],
        .optional = true,
    };
    const nested_logical_infos = [_]parquet.NestedLogicalInfo{
        outer_logical,
        .{
            .kind = .map,
            .definition_level = 3,
            .repetition_level = 1,
            .path = inner_map_path[0..],
            .optional = true,
        },
    };
    const outer_logical_infos = [_]parquet.NestedLogicalInfo{outer_logical};
    const cols = [_]parquet.Column{
        .{
            .name = "key",
            .path = outer_key_path[0..],
            .column_type = .{ .physical = .byte_array, .logical = .string },
            .repetition = .repeated,
            .max_definition_level = 2,
            .max_repetition_level = 1,
            .nested_logical_info = outer_logical_infos[0..],
            .map_info = .{ .map_definition_level = 1 },
        },
        .{
            .name = "key",
            .path = inner_key_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 4,
            .max_repetition_level = 2,
            .nested_logical_info = nested_logical_infos[0..],
            .map_info = .{ .map_definition_level = 3 },
        },
        .{
            .name = "value",
            .path = inner_value_path[0..],
            .column_type = .{ .physical = .boolean },
            .repetition = .repeated,
            .max_definition_level = 4,
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

    const outer_offsets = [_]usize{ 0, 0, 0, 3, 4 };
    const outer_validity = [_]bool{ false, true, true, true };
    const outer_keys = [_][]const u8{ "aa", "b", "ccc", "d" };
    const inner_offsets = [_]usize{ 0, 0, 0, 2, 3 };
    const inner_validity = [_]bool{ false, true, true, true };
    const inner_keys = [_]i32{ 1, 2, 3 };
    const inner_values = [_]bool{ true, false, true };
    const levels = [_]parquet.ColumnNestedMapLevelData{
        .{
            .keys = .{ .byte_array = .{ .values = outer_keys[0..] } },
            .offsets = outer_offsets[0..],
            .validity = outer_validity[0..],
        },
        .{
            .keys = .{ .int32 = .{ .values = inner_keys[0..] } },
            .offsets = inner_offsets[0..],
            .validity = inner_validity[0..],
        },
    };
    const batch = [_]parquet.ColumnNestedMapData{
        .{
            .levels = levels[0..],
            .values = .{ .boolean = .{ .values = inner_values[0..] } },
        },
    };
    try writer.writeRowGroupNestedMaps(4, batch[0..]);
    try writer.finish();
}
