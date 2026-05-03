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
    const key_path = [_][]const u8{ "attrs", "key_value", "key" };
    const value_path = [_][]const u8{ "attrs", "key_value", "value" };
    const nested_outer_key_path = [_][]const u8{ "nested_attrs", "key_value", "key" };
    const nested_inner_key_path = [_][]const u8{ "nested_attrs", "key_value", "value", "key_value", "key" };
    const nested_inner_value_path = [_][]const u8{ "nested_attrs", "key_value", "value", "key_value", "value" };
    const nested_outer_map_path = [_][]const u8{"nested_attrs"};
    const nested_inner_map_path = [_][]const u8{ "nested_attrs", "key_value", "value" };
    const list_map_key_path = [_][]const u8{ "list_attrs", "list", "element", "key_value", "key" };
    const list_map_value_path = [_][]const u8{ "list_attrs", "list", "element", "key_value", "value" };
    const list_map_list_path = [_][]const u8{"list_attrs"};
    const list_map_map_path = [_][]const u8{ "list_attrs", "list", "element" };
    const list_map_logical_infos = [_]parquet.NestedLogicalInfo{
        .{
            .kind = .list,
            .definition_level = 1,
            .repetition_level = 0,
            .path = list_map_list_path[0..],
            .optional = true,
        },
        .{
            .kind = .map,
            .definition_level = 3,
            .repetition_level = 1,
            .path = list_map_map_path[0..],
            .optional = true,
        },
    };
    const nested_outer_logical = parquet.NestedLogicalInfo{
        .kind = .map,
        .definition_level = 1,
        .repetition_level = 0,
        .path = nested_outer_map_path[0..],
        .optional = true,
    };
    const nested_map_logical_infos = [_]parquet.NestedLogicalInfo{
        nested_outer_logical,
        .{
            .kind = .map,
            .definition_level = 3,
            .repetition_level = 1,
            .path = nested_inner_map_path[0..],
            .optional = true,
        },
    };
    const nested_outer_logical_infos = [_]parquet.NestedLogicalInfo{nested_outer_logical};
    const cols = [_]parquet.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 } },
        .{
            .name = "element",
            .path = list_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 3,
            .max_repetition_level = 1,
            .list_info = .{ .list_definition_level = 1 },
        },
        .{
            .name = "key",
            .path = key_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 2,
            .max_repetition_level = 1,
            .map_info = .{ .map_definition_level = 1 },
        },
        .{
            .name = "value",
            .path = value_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 3,
            .max_repetition_level = 1,
            .map_info = .{ .map_definition_level = 1 },
        },
        .{
            .name = "key",
            .path = nested_outer_key_path[0..],
            .column_type = .{ .physical = .byte_array, .logical = .string },
            .repetition = .repeated,
            .max_definition_level = 2,
            .max_repetition_level = 1,
            .nested_logical_info = nested_outer_logical_infos[0..],
            .map_info = .{ .map_definition_level = 1 },
        },
        .{
            .name = "key",
            .path = nested_inner_key_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 4,
            .max_repetition_level = 2,
            .nested_logical_info = nested_map_logical_infos[0..],
            .map_info = .{ .map_definition_level = 3 },
        },
        .{
            .name = "value",
            .path = nested_inner_value_path[0..],
            .column_type = .{ .physical = .boolean },
            .repetition = .repeated,
            .max_definition_level = 4,
            .max_repetition_level = 2,
            .nested_logical_info = nested_map_logical_infos[0..],
            .map_info = .{ .map_definition_level = 3 },
        },
        .{
            .name = "key",
            .path = list_map_key_path[0..],
            .column_type = .{ .physical = .byte_array, .logical = .string },
            .repetition = .repeated,
            .max_definition_level = 4,
            .max_repetition_level = 2,
            .nested_logical_info = list_map_logical_infos[0..],
            .map_info = .{ .map_definition_level = 3 },
        },
        .{
            .name = "value",
            .path = list_map_value_path[0..],
            .column_type = .{ .physical = .int32 },
            .repetition = .repeated,
            .max_definition_level = 5,
            .max_repetition_level = 2,
            .nested_logical_info = list_map_logical_infos[0..],
            .map_info = .{ .map_definition_level = 3 },
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

    const ids = [_]i64{ 100, 101, 102, 103 };
    const list_values = [_]i32{ 10, 11, 20 };
    const list_offsets = [_]usize{ 0, 0, 0, 3, 4 };
    const list_validity = [_]bool{ false, true, true, true };
    const element_validity = [_]bool{ true, false, true, true };
    const map_keys = [_]i32{ 1, 2, 3 };
    const map_values = [_]i32{ 10, 20 };
    const map_offsets = [_]usize{ 0, 0, 0, 2, 3 };
    const map_validity = [_]bool{ false, true, true, true };
    const value_validity = [_]bool{ true, false, true };
    const nested_outer_offsets = [_]usize{ 0, 0, 0, 3, 4 };
    const nested_outer_validity = [_]bool{ false, true, true, true };
    const nested_outer_keys = [_][]const u8{ "aa", "b", "ccc", "d" };
    const nested_inner_offsets = [_]usize{ 0, 0, 0, 2, 3 };
    const nested_inner_validity = [_]bool{ false, true, true, true };
    const nested_inner_keys = [_]i32{ 1, 2, 3 };
    const nested_inner_values = [_]bool{ true, false, true };
    const list_map_offsets = [_]usize{ 0, 0, 0, 3, 4 };
    const list_map_validity = [_]bool{ false, true, true, true };
    const list_map_entry_offsets = [_]usize{ 0, 0, 0, 2, 3 };
    const list_map_entry_validity = [_]bool{ false, true, true, true };
    const list_map_keys = [_][]const u8{ "aa", "b", "c" };
    const list_map_values = [_]i32{ 1, 3 };
    const list_map_value_validity = [_]bool{ true, false, true };
    const nested_map_levels = [_]parquet.ColumnNestedMapLevelData{
        .{
            .keys = .{ .byte_array = .{ .values = nested_outer_keys[0..] } },
            .offsets = nested_outer_offsets[0..],
            .validity = nested_outer_validity[0..],
        },
        .{
            .keys = .{ .int32 = .{ .values = nested_inner_keys[0..] } },
            .offsets = nested_inner_offsets[0..],
            .validity = nested_inner_validity[0..],
        },
    };
    const batch = [_]parquet.ColumnWriteData{
        .{ .flat = .{ .int64 = .{ .values = ids[0..] } } },
        .{ .list = .{
            .values = .{ .int32 = .{ .values = list_values[0..], .validity = element_validity[0..] } },
            .offsets = list_offsets[0..],
            .validity = list_validity[0..],
        } },
        .{ .map = .{
            .keys = .{ .int32 = .{ .values = map_keys[0..] } },
            .values = .{ .int32 = .{ .values = map_values[0..], .validity = value_validity[0..] } },
            .offsets = map_offsets[0..],
            .validity = map_validity[0..],
        } },
        .{ .nested_map = .{
            .levels = nested_map_levels[0..],
            .values = .{ .boolean = .{ .values = nested_inner_values[0..] } },
        } },
        .{ .list_map = .{
            .list = .{
                .offsets = list_map_offsets[0..],
                .validity = list_map_validity[0..],
            },
            .map = .{
                .keys = .{ .byte_array = .{ .values = list_map_keys[0..] } },
                .offsets = list_map_entry_offsets[0..],
                .validity = list_map_entry_validity[0..],
            },
            .values = .{ .int32 = .{ .values = list_map_values[0..], .validity = list_map_value_validity[0..] } },
        } },
    };
    try writer.writeRowGroupMixed(4, batch[0..]);
    try writer.finish();
}
