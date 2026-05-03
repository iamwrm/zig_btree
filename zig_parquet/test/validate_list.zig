const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingInputPath;
    const column_arg = args.next() orelse return error.MissingColumnIndex;
    const expected_rows_arg = args.next() orelse return error.MissingExpectedRows;
    const expected_elements_arg = args.next() orelse return error.MissingExpectedElements;
    const expected_values_arg = args.next() orelse return error.MissingExpectedValues;
    const expected_null_lists_arg = args.next() orelse return error.MissingExpectedNullLists;
    const expected_null_elements_arg = args.next() orelse return error.MissingExpectedNullElements;
    const expected_payload_sum_arg = args.next() orelse return error.MissingExpectedPayloadSum;

    const expected_rows = try std.fmt.parseInt(usize, expected_rows_arg, 10);
    const expected_elements = try std.fmt.parseInt(usize, expected_elements_arg, 10);
    const expected_values = try std.fmt.parseInt(usize, expected_values_arg, 10);
    const expected_null_lists = try std.fmt.parseInt(usize, expected_null_lists_arg, 10);
    const expected_null_elements = try std.fmt.parseInt(usize, expected_null_elements_arg, 10);
    const expected_payload_sum = try std.fmt.parseInt(i64, expected_payload_sum_arg, 10);

    var file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);

    var reader_buffer: [64 * 1024]u8 = undefined;
    var io_reader = file.reader(init.io, &reader_buffer);
    var parsed = try parquet.reader.StreamFileReader.init(init.gpa, &io_reader);
    defer parsed.deinit();
    if (parsed.metadata.num_rows != expected_rows) return error.BadRowCount;

    var total_rows: usize = 0;
    var total_elements: usize = 0;
    var total_values: usize = 0;
    var total_null_lists: usize = 0;
    var total_null_elements: usize = 0;
    var total_payload_sum: i64 = 0;

    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        var list = if (std.fmt.parseInt(usize, column_arg, 10)) |column_index|
            try parsed.readColumnList(init.gpa, rg_idx, column_index)
        else |_|
            try parsed.readColumnListByPath(init.gpa, rg_idx, column_arg);
        defer list.deinit(init.gpa);

        const row_group_rows = std.math.cast(usize, parsed.metadata.row_groups[rg_idx].num_rows) orelse return error.BadRowCount;
        if (list.offsets.len != row_group_rows + 1) return error.BadOffsetCount;
        if (list.offsets[0] != 0) return error.BadOffsetCount;
        var row: usize = 0;
        while (row < row_group_rows) : (row += 1) {
            if (list.offsets[row] > list.offsets[row + 1]) return error.BadOffsetCount;
        }

        total_rows += row_group_rows;
        total_elements += list.offsets[row_group_rows];
        if (list.validity) |validity| {
            if (validity.len != row_group_rows) return error.BadValidity;
            for (validity) |valid| {
                if (!valid) total_null_lists += 1;
            }
        }

        switch (list.values) {
            .int32 => |values| {
                total_values += values.values.len;
                if (values.validity) |validity| {
                    if (validity.len != list.offsets[row_group_rows]) return error.BadValidity;
                    for (validity) |valid| {
                        if (!valid) total_null_elements += 1;
                    }
                }
                for (values.values) |value| total_payload_sum += value;
            },
            .int64 => |values| {
                total_values += values.values.len;
                if (values.validity) |validity| {
                    if (validity.len != list.offsets[row_group_rows]) return error.BadValidity;
                    for (validity) |valid| {
                        if (!valid) total_null_elements += 1;
                    }
                }
                for (values.values) |value| total_payload_sum += value;
            },
            .byte_array => |values| {
                total_values += values.values.len;
                if (values.validity) |validity| {
                    if (validity.len != list.offsets[row_group_rows]) return error.BadValidity;
                    for (validity) |valid| {
                        if (!valid) total_null_elements += 1;
                    }
                }
                for (values.values) |value| total_payload_sum += @intCast(value.len);
            },
            else => return error.UnsupportedListValidationType,
        }
    }

    if (total_rows != expected_rows) return error.BadRowCount;
    if (total_elements != expected_elements) return error.BadElementCount;
    if (total_values != expected_values) return error.BadValueCount;
    if (total_null_lists != expected_null_lists) return error.BadListNullCount;
    if (total_null_elements != expected_null_elements) return error.BadElementNullCount;
    if (total_payload_sum != expected_payload_sum) return error.BadPayloadSum;
}
