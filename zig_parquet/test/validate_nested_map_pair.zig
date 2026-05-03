const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingInputPath;
    const key_column_arg = args.next() orelse return error.MissingKeyColumnIndex;
    const value_column_arg = args.next() orelse return error.MissingValueColumnIndex;
    const expected_rows_arg = args.next() orelse return error.MissingExpectedRows;
    const expected_parent_count_arg = args.next() orelse return error.MissingExpectedParentCount;
    const expected_entries_arg = args.next() orelse return error.MissingExpectedEntries;
    const expected_key_values_arg = args.next() orelse return error.MissingExpectedKeyValues;
    const expected_value_slots_arg = args.next() orelse return error.MissingExpectedValueSlots;
    const expected_value_values_arg = args.next() orelse return error.MissingExpectedValueValues;
    const expected_null_maps_arg = args.next() orelse return error.MissingExpectedNullMaps;
    const expected_null_values_arg = args.next() orelse return error.MissingExpectedNullValues;
    const expected_key_payload_sum_arg = args.next() orelse return error.MissingExpectedKeyPayloadSum;
    const expected_value_payload_sum_arg = args.next() orelse return error.MissingExpectedValuePayloadSum;
    if (args.next() != null) return error.TooManyArguments;

    const expected_rows = try std.fmt.parseInt(usize, expected_rows_arg, 10);
    const expected_parent_count = try std.fmt.parseInt(usize, expected_parent_count_arg, 10);
    const expected_entries = try std.fmt.parseInt(usize, expected_entries_arg, 10);
    const expected_key_values = try std.fmt.parseInt(usize, expected_key_values_arg, 10);
    const expected_value_slots = try std.fmt.parseInt(usize, expected_value_slots_arg, 10);
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
    var total_parent_count: usize = 0;
    var total_entries: usize = 0;
    var total_key_values: usize = 0;
    var total_value_slots: usize = 0;
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
        var pair = if (key_by_index) |key_column_index| blk: {
            if (!value_is_none and value_by_index == null) return error.InvalidColumnData;
            break :blk try parsed.readColumnNestedMapPair(init.gpa, rg_idx, key_column_index, value_by_index);
        } else try parsed.readColumnNestedMapPairByPath(init.gpa, rg_idx, key_column_arg, if (value_is_none) null else value_column_arg);
        defer pair.deinit(init.gpa);

        const row_group_rows = std.math.cast(usize, parsed.metadata.row_groups[rg_idx].num_rows) orelse return error.BadRowCount;
        total_rows += row_group_rows;

        const key_map = pair.keys.levels[pair.key_map_level_index];
        const parent_count = key_map.offsets.len - 1;
        total_parent_count += parent_count;
        if (key_map.offsets[0] != 0) return error.BadMapOffsetCount;
        for (0..parent_count) |parent_index| {
            if (key_map.offsets[parent_index] > key_map.offsets[parent_index + 1]) return error.BadMapOffsetCount;
        }
        if (key_map.validity) |validity| {
            if (validity.len != parent_count) return error.BadMapValidity;
            for (validity) |valid| {
                if (!valid) total_null_maps += 1;
            }
        }

        const entry_count = key_map.offsets[parent_count];
        total_entries += entry_count;
        total_key_values += try columnValueCount(pair.keys.values, false, entry_count, &total_null_values);
        total_key_payload_sum += try columnPayloadSum(pair.keys.values);

        if (pair.values) |values| {
            const value_slots = nestedLogicalLeafSlotCount(values);
            total_value_slots += value_slots;
            total_value_values += try columnValueCount(values.values, true, value_slots, &total_null_values);
            total_value_payload_sum += try columnPayloadSum(values.values);
            const value_map_index = pair.value_map_level_index orelse return error.BadMapOffsetCount;
            if (!sameMapLevel(key_map, values.levels[value_map_index])) return error.BadMapOffsetCount;
        } else if (!value_is_none) {
            return error.BadValueCount;
        }
    }

    if (total_rows != expected_rows) return error.BadRowCount;
    if (total_parent_count != expected_parent_count) return error.BadParentCount;
    if (total_entries != expected_entries) return error.BadEntryCount;
    if (total_key_values != expected_key_values) return error.BadKeyValueCount;
    if (total_value_slots != expected_value_slots) return error.BadValueSlotCount;
    if (total_value_values != expected_value_values) return error.BadValueCount;
    if (total_null_maps != expected_null_maps) return error.BadMapNullCount;
    if (total_null_values != expected_null_values) return error.BadValueNullCount;
    if (total_key_payload_sum != expected_key_payload_sum) return error.BadKeyPayloadSum;
    if (total_value_payload_sum != expected_value_payload_sum) return error.BadValuePayloadSum;
}

fn nestedLogicalLeafSlotCount(column: parquet.OwnedNestedLogicalColumn) usize {
    if (column.levels.len == 0) return 0;
    const last_level = column.levels[column.levels.len - 1];
    if (last_level.offsets.len == 0) return 0;
    return last_level.offsets[last_level.offsets.len - 1];
}

fn sameMapLevel(a: parquet.NestedLogicalColumnLevel, b: parquet.NestedLogicalColumnLevel) bool {
    if (a.kind != .map or b.kind != .map) return false;
    if (a.definition_level != b.definition_level or a.repetition_level != b.repetition_level) return false;
    if (!std.mem.eql(usize, a.offsets, b.offsets)) return false;
    if (a.validity == null or b.validity == null) return a.validity == null and b.validity == null;
    return std.mem.eql(bool, a.validity.?, b.validity.?);
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
            } else if (data.values.len != expected_slots) {
                return error.BadValueSlotCount;
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
        .byte_array, .int96, .fixed_len_byte_array => |data| blk: {
            var sum: i64 = 0;
            for (data.values) |value| sum += @intCast(value.len);
            break :blk sum;
        },
        else => return error.UnsupportedMapPairValidationType,
    };
}
