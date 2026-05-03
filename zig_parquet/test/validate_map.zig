const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingInputPath;
    const key_column_arg = args.next() orelse return error.MissingKeyColumnIndex;
    const value_column_arg = args.next() orelse return error.MissingValueColumnIndex;
    const expected_rows_arg = args.next() orelse return error.MissingExpectedRows;
    const expected_entries_arg = args.next() orelse return error.MissingExpectedEntries;
    const expected_key_values_arg = args.next() orelse return error.MissingExpectedKeyValues;
    const expected_value_values_arg = args.next() orelse return error.MissingExpectedValueValues;
    const expected_null_maps_arg = args.next() orelse return error.MissingExpectedNullMaps;
    const expected_null_values_arg = args.next() orelse return error.MissingExpectedNullValues;
    const expected_key_payload_sum_arg = args.next() orelse return error.MissingExpectedKeyPayloadSum;
    const expected_value_payload_sum_arg = args.next() orelse return error.MissingExpectedValuePayloadSum;

    const expected_rows = try std.fmt.parseInt(usize, expected_rows_arg, 10);
    const expected_entries = try std.fmt.parseInt(usize, expected_entries_arg, 10);
    const expected_key_values = try std.fmt.parseInt(usize, expected_key_values_arg, 10);
    const expected_value_values = try std.fmt.parseInt(usize, expected_value_values_arg, 10);
    const expected_null_maps = try std.fmt.parseInt(usize, expected_null_maps_arg, 10);
    const expected_null_values = try std.fmt.parseInt(usize, expected_null_values_arg, 10);
    const expected_key_payload_sum = try std.fmt.parseInt(i64, expected_key_payload_sum_arg, 10);
    const expected_value_payload_sum = try std.fmt.parseInt(i64, expected_value_payload_sum_arg, 10);

    var file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);

    var reader_buffer: [64 * 1024]u8 = undefined;
    var io_reader = file.reader(init.io, &reader_buffer);
    var parsed = try parquet.reader.StreamFileReader.init(init.gpa, &io_reader);
    defer parsed.deinit();
    if (parsed.metadata.num_rows != expected_rows) return error.BadRowCount;

    var total_rows: usize = 0;
    var total_entries: usize = 0;
    var total_key_values: usize = 0;
    var total_value_values: usize = 0;
    var total_null_maps: usize = 0;
    var total_null_values: usize = 0;
    var total_key_payload_sum: i64 = 0;
    var total_value_payload_sum: i64 = 0;

    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        const key_by_index: ?usize = std.fmt.parseInt(usize, key_column_arg, 10) catch null;
        const value_is_none = std.mem.eql(u8, value_column_arg, "none");
        const value_by_index: ?usize = if (value_is_none)
            null
        else
            std.fmt.parseInt(usize, value_column_arg, 10) catch null;
        var map = if (key_by_index) |key_column_index| blk: {
            if (!value_is_none and value_by_index == null) return error.InvalidColumnData;
            break :blk try parsed.readColumnMap(init.gpa, rg_idx, key_column_index, value_by_index);
        } else try parsed.readColumnMapByPath(init.gpa, rg_idx, key_column_arg, if (value_is_none) null else value_column_arg);
        defer map.deinit(init.gpa);

        const row_group_rows = std.math.cast(usize, parsed.metadata.row_groups[rg_idx].num_rows) orelse return error.BadRowCount;
        if (map.offsets.len != row_group_rows + 1) return error.BadOffsetCount;
        if (map.offsets[0] != 0) return error.BadOffsetCount;
        var row: usize = 0;
        while (row < row_group_rows) : (row += 1) {
            if (map.offsets[row] > map.offsets[row + 1]) return error.BadOffsetCount;
        }

        total_rows += row_group_rows;
        total_entries += map.offsets[row_group_rows];
        total_key_values += try columnValueCount(map.keys, false, map.offsets[row_group_rows], &total_null_values);
        total_key_payload_sum += try columnPayloadSum(map.keys);

        if (map.validity) |validity| {
            if (validity.len != row_group_rows) return error.BadValidity;
            for (validity) |valid| {
                if (!valid) total_null_maps += 1;
            }
        }

        if (map.values) |values| {
            var null_values: usize = 0;
            total_value_values += try columnValueCount(values, true, map.offsets[row_group_rows], &null_values);
            total_null_values += null_values;
            total_value_payload_sum += try columnPayloadSum(values);
        } else if (!value_is_none) {
            return error.BadValueCount;
        }
    }

    if (total_rows != expected_rows) return error.BadRowCount;
    if (total_entries != expected_entries) return error.BadEntryCount;
    if (total_key_values != expected_key_values) return error.BadKeyValueCount;
    if (total_value_values != expected_value_values) return error.BadValueCount;
    if (total_null_maps != expected_null_maps) return error.BadMapNullCount;
    if (total_null_values != expected_null_values) return error.BadValueNullCount;
    if (total_key_payload_sum != expected_key_payload_sum) return error.BadKeyPayloadSum;
    if (total_value_payload_sum != expected_value_payload_sum) return error.BadValuePayloadSum;
}

fn columnValueCount(column: parquet.OwnedColumn, allow_validity: bool, expected_slots: usize, null_count: *usize) !usize {
    return switch (column) {
        inline else => |data| blk: {
            if (data.validity) |validity| {
                if (!allow_validity) return error.BadValidity;
                if (validity.len != expected_slots) return error.BadValidity;
                for (validity) |valid| {
                    if (!valid) null_count.* += 1;
                }
            }
            break :blk data.values.len;
        },
    };
}

fn columnPayloadSum(column: parquet.OwnedColumn) !i64 {
    return switch (column) {
        .boolean => |data| blk: {
            var sum: i64 = 0;
            for (data.values) |value| sum += if (value) 1 else 0;
            break :blk sum;
        },
        .int32 => |data| blk: {
            var sum: i64 = 0;
            for (data.values) |value| sum += value;
            break :blk sum;
        },
        .int64 => |data| blk: {
            var sum: i64 = 0;
            for (data.values) |value| sum += value;
            break :blk sum;
        },
        .byte_array => |data| blk: {
            var sum: i64 = 0;
            for (data.values) |value| sum += @intCast(value.len);
            break :blk sum;
        },
        .fixed_len_byte_array => |data| blk: {
            var sum: i64 = 0;
            for (data.values) |value| sum += @intCast(value.len);
            break :blk sum;
        },
        else => return error.UnsupportedMapValidationType,
    };
}
