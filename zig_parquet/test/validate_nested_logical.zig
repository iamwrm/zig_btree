const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingInputPath;
    const column_arg = args.next() orelse return error.MissingColumnIndex;
    const expected_rows_arg = args.next() orelse return error.MissingExpectedRows;
    const expected_level_totals_arg = args.next() orelse return error.MissingExpectedLevelTotals;
    const expected_level_nulls_arg = args.next() orelse return error.MissingExpectedLevelNulls;
    const expected_leaf_slots_arg = args.next() orelse return error.MissingExpectedLeafSlots;
    const expected_values_arg = args.next() orelse return error.MissingExpectedValues;
    const expected_leaf_nulls_arg = args.next() orelse return error.MissingExpectedLeafNulls;
    const expected_payload_sum_arg = args.next() orelse return error.MissingExpectedPayloadSum;
    if (args.next() != null) return error.TooManyArguments;

    const expected_rows = try std.fmt.parseInt(usize, expected_rows_arg, 10);
    const expected_level_totals = try parseExpectedCounts(init.gpa, expected_level_totals_arg);
    defer init.gpa.free(expected_level_totals);
    const expected_level_nulls = try parseExpectedCounts(init.gpa, expected_level_nulls_arg);
    defer init.gpa.free(expected_level_nulls);
    if (expected_level_totals.len != expected_level_nulls.len) return error.BadNestedLogicalLevelCount;
    const expected_leaf_slots = try std.fmt.parseInt(usize, expected_leaf_slots_arg, 10);
    const expected_values = try std.fmt.parseInt(usize, expected_values_arg, 10);
    const expected_leaf_nulls = try std.fmt.parseInt(usize, expected_leaf_nulls_arg, 10);
    const expected_payload_sum = try std.fmt.parseInt(i64, expected_payload_sum_arg, 10);

    const total_level_totals = try init.gpa.alloc(usize, expected_level_totals.len);
    defer init.gpa.free(total_level_totals);
    @memset(total_level_totals, 0);
    const total_level_nulls = try init.gpa.alloc(usize, expected_level_nulls.len);
    defer init.gpa.free(total_level_nulls);
    @memset(total_level_nulls, 0);

    var file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);

    var reader_buffer: [64 * 1024]u8 = undefined;
    var io_reader = file.reader(init.io, &reader_buffer);
    var parsed = try parquet.reader.StreamFileReader.init(init.gpa, &io_reader);
    defer parsed.deinit();
    if (parsed.metadata.num_rows != expected_rows) return error.BadRowCount;

    const parsed_column_index: ?usize = std.fmt.parseInt(usize, column_arg, 10) catch null;
    const path_addressed = parsed_column_index == null;
    const column_index = parsed_column_index orelse parsed.columnIndexByPath(column_arg) orelse return error.BadSchemaPath;
    if (column_index >= parsed.metadata.schema.columns.len) return error.BadColumnCount;
    const schema_column = parsed.metadata.schema.columns[column_index];
    if (schema_column.nested_logical_info.len != expected_level_totals.len) return error.BadNestedLogicalLevelCount;

    var total_rows: usize = 0;
    var total_leaf_slots: usize = 0;
    var total_values: usize = 0;
    var total_leaf_nulls: usize = 0;
    var total_payload_sum: i64 = 0;

    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        var logical = if (path_addressed)
            try parsed.readColumnNestedLogicalByPath(init.gpa, rg_idx, column_arg)
        else
            try parsed.readColumnNestedLogical(init.gpa, rg_idx, column_index);
        defer logical.deinit(init.gpa);

        if (logical.levels.len != expected_level_totals.len) return error.BadNestedLogicalLevelCount;
        const row_group_rows = std.math.cast(usize, parsed.metadata.row_groups[rg_idx].num_rows) orelse return error.BadRowCount;
        total_rows += row_group_rows;

        var parent_count = row_group_rows;
        for (logical.levels, 0..) |level, level_index| {
            if (level.kind != schema_column.nested_logical_info[level_index].kind) return error.BadNestedLogicalLevelPath;
            if (level.definition_level != schema_column.nested_logical_info[level_index].definition_level) return error.BadNestedLogicalLevelPath;
            if (level.repetition_level != schema_column.nested_logical_info[level_index].repetition_level) return error.BadNestedLogicalLevelPath;
            if (level.path.len != schema_column.nested_logical_info[level_index].path.len) return error.BadNestedLogicalLevelPath;
            for (level.path, schema_column.nested_logical_info[level_index].path) |actual_part, expected_part| {
                if (!std.mem.eql(u8, actual_part, expected_part)) return error.BadNestedLogicalLevelPath;
            }
            if (level.offsets.len != parent_count + 1) return error.BadNestedLogicalOffsetCount;
            if (level.offsets[0] != 0) return error.BadNestedLogicalOffsetCount;
            var parent_index: usize = 0;
            while (parent_index < parent_count) : (parent_index += 1) {
                if (level.offsets[parent_index] > level.offsets[parent_index + 1]) return error.BadNestedLogicalOffsetCount;
            }
            if (level.validity) |validity| {
                if (validity.len != parent_count) return error.BadNestedLogicalValidity;
                for (validity) |valid| {
                    if (!valid) total_level_nulls[level_index] += 1;
                }
            }
            const child_count = level.offsets[parent_count];
            total_level_totals[level_index] += child_count;
            parent_count = child_count;
        }

        const leaf_slots = parent_count;
        total_leaf_slots += leaf_slots;
        total_values += try ownedColumnValueCount(logical.values, leaf_slots, &total_leaf_nulls);
        total_payload_sum += try ownedColumnPayloadSum(logical.values);
    }

    if (total_rows != expected_rows) return error.BadRowCount;
    for (total_level_totals, expected_level_totals) |actual, expected| {
        if (actual != expected) return error.BadNestedLogicalOffsetCount;
    }
    for (total_level_nulls, expected_level_nulls) |actual, expected| {
        if (actual != expected) return error.BadNestedLogicalValidity;
    }
    if (total_leaf_slots != expected_leaf_slots) return error.BadLeafSlotCount;
    if (total_values != expected_values) return error.BadValueCount;
    if (total_leaf_nulls != expected_leaf_nulls) return error.BadLeafNullCount;
    if (total_payload_sum != expected_payload_sum) return error.BadPayloadSum;
}

fn parseExpectedCounts(allocator: std.mem.Allocator, arg: []const u8) ![]usize {
    if (arg.len == 0 or std.mem.eql(u8, arg, "-")) return try allocator.alloc(usize, 0);
    var out: std.ArrayList(usize) = .empty;
    errdefer out.deinit(allocator);

    var parts = std.mem.splitScalar(u8, arg, ',');
    while (parts.next()) |part| {
        if (part.len == 0) return error.BadNestedLogicalLevelCount;
        try out.append(allocator, try std.fmt.parseInt(usize, part, 10));
    }
    return out.toOwnedSlice(allocator);
}

fn ownedColumnValueCount(column: parquet.OwnedColumn, expected_slots: usize, null_count: *usize) !usize {
    return switch (column) {
        inline else => |data| blk: {
            if (data.validity) |validity| {
                if (validity.len != expected_slots) return error.BadNestedLogicalValidity;
                for (validity) |valid| {
                    if (!valid) null_count.* += 1;
                }
            } else if (data.values.len != expected_slots) {
                return error.BadLeafSlotCount;
            }
            break :blk data.values.len;
        },
    };
}

fn ownedColumnPayloadSum(column: parquet.OwnedColumn) !i64 {
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
        else => return error.UnsupportedNestedLogicalValidationType,
    };
}
