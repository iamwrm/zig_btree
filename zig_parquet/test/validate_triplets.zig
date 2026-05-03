const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingInputPath;
    const column_arg = args.next() orelse return error.MissingColumnIndex;
    const expected_rows_arg = args.next() orelse return error.MissingExpectedRows;
    const expected_levels_arg = args.next() orelse return error.MissingExpectedLevels;
    const expected_values_arg = args.next() orelse return error.MissingExpectedValues;
    const expected_rep_zeroes_arg = args.next() orelse return error.MissingExpectedRepZeroes;
    const expected_payload_sum_arg = args.next() orelse return error.MissingExpectedSum;
    const expected_repeated_level_counts_arg = args.next();
    if (args.next() != null) return error.TooManyArguments;

    const expected_rows = try std.fmt.parseInt(usize, expected_rows_arg, 10);
    const expected_levels = try std.fmt.parseInt(usize, expected_levels_arg, 10);
    const expected_values = try std.fmt.parseInt(usize, expected_values_arg, 10);
    const expected_rep_zeroes = try std.fmt.parseInt(usize, expected_rep_zeroes_arg, 10);
    const expected_payload_sum = try std.fmt.parseInt(i64, expected_payload_sum_arg, 10);
    const expected_repeated_level_counts = if (expected_repeated_level_counts_arg) |arg|
        try parseExpectedRepeatedLevelCounts(init.gpa, arg)
    else
        null;
    defer if (expected_repeated_level_counts) |counts| init.gpa.free(counts);

    const total_repeated_level_counts = if (expected_repeated_level_counts) |counts| blk: {
        const totals = try init.gpa.alloc(usize, counts.len);
        @memset(totals, 0);
        break :blk totals;
    } else null;
    defer if (total_repeated_level_counts) |counts| init.gpa.free(counts);

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

    var total_levels: usize = 0;
    var total_values: usize = 0;
    var total_rep_zeroes: usize = 0;
    var total_payload_sum: i64 = 0;

    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        var nested_triplets = if (path_addressed)
            try parsed.readColumnNestedTripletsByPath(init.gpa, rg_idx, column_arg)
        else
            try parsed.readColumnNestedTriplets(init.gpa, rg_idx, column_index);
        defer nested_triplets.deinit(init.gpa);
        const triplets = nested_triplets.triplets;
        if (triplets.definition_levels.len != triplets.repetition_levels.len) return error.BadLevelCount;
        if (triplets.repetition_levels.len == 0 and parsed.metadata.row_groups[rg_idx].num_rows != 0) return error.BadLevelCount;
        if (triplets.repetition_levels.len > 0 and triplets.repetition_levels[0] != 0) return error.BadRepetitionLevel;
        if (schema_column.max_definition_level != triplets.max_definition_level) return error.BadDefinitionLevel;
        if (schema_column.max_repetition_level != triplets.max_repetition_level) return error.BadRepetitionLevel;
        if (schema_column.repeated_level_info.len != triplets.repeated_level_offsets.len) return error.BadRepeatedLevelOffsetCount;
        if (nested_triplets.repeated_levels.len != triplets.repeated_level_offsets.len) return error.BadRepeatedLevelOffsetCount;
        if (nested_triplets.logical_levels.len != schema_column.nested_logical_info.len) return error.BadNestedLogicalPath;
        for (nested_triplets.logical_levels, schema_column.nested_logical_info) |actual, expected| {
            if (actual.kind != expected.kind) return error.BadNestedLogicalPath;
            if (actual.definition_level != expected.definition_level) return error.BadNestedLogicalPath;
            if (actual.repetition_level != expected.repetition_level) return error.BadNestedLogicalPath;
            if (actual.optional != expected.optional) return error.BadNestedLogicalPath;
            if (actual.path.len != expected.path.len) return error.BadNestedLogicalPath;
            for (actual.path, expected.path) |actual_part, expected_part| {
                if (!std.mem.eql(u8, actual_part, expected_part)) return error.BadNestedLogicalPath;
            }
        }
        const row_group_rows = std.math.cast(usize, parsed.metadata.row_groups[rg_idx].num_rows) orelse return error.BadRowCount;
        if (triplets.row_offsets.len != row_group_rows + 1) return error.BadRowOffsetCount;
        if (triplets.row_offsets[0] != 0) return error.BadRowOffsetCount;
        if (triplets.row_offsets[row_group_rows] != triplets.repetition_levels.len) return error.BadRowOffsetCount;
        if (triplets.value_offsets.len != row_group_rows + 1) return error.BadValueOffsetCount;
        if (triplets.value_offsets[0] != 0) return error.BadValueOffsetCount;
        if (triplets.repeated_level_offsets.len != triplets.max_repetition_level) return error.BadRepeatedLevelOffsetCount;
        if (total_repeated_level_counts) |counts| {
            if (counts.len != triplets.repeated_level_offsets.len) return error.BadRepeatedLevelOffsetCount;
        }
        for (triplets.repeated_level_offsets, 1..) |level_offsets, expected_level| {
            const level_info = nested_triplets.repeated_levels[expected_level - 1];
            if (level_info.repetition_level != expected_level) return error.BadRepeatedLevelOffsetCount;
            if (level_info.path.len == 0 or level_info.path.len > schema_column.path.len) return error.BadRepeatedLevelOffsetCount;
            for (level_info.path, 0..) |part, part_index| {
                if (!std.mem.eql(u8, part, schema_column.path[part_index])) return error.BadRepeatedLevelOffsetCount;
            }
            if (level_offsets.repetition_level != expected_level) return error.BadRepeatedLevelOffsetCount;
            if (level_offsets.offsets.ptr != level_info.offsets.ptr or level_offsets.offsets.len != level_info.offsets.len) return error.BadRepeatedLevelOffsetCount;
            if (level_offsets.offsets.len == 0) return error.BadRepeatedLevelOffsetCount;
            if (level_offsets.offsets[0] != 0) return error.BadRepeatedLevelOffsetCount;
            if (level_offsets.offsets[level_offsets.offsets.len - 1] != triplets.repetition_levels.len) return error.BadRepeatedLevelOffsetCount;
            if (total_repeated_level_counts) |counts| counts[expected_level - 1] += level_offsets.offsets.len;

            var previous: usize = 0;
            var offset_index: usize = 1;
            while (offset_index < level_offsets.offsets.len) : (offset_index += 1) {
                const offset = level_offsets.offsets[offset_index];
                if (offset <= previous and offset != triplets.repetition_levels.len) return error.BadRepeatedLevelOffsetCount;
                if (offset < triplets.repetition_levels.len and triplets.repetition_levels[offset] > expected_level) return error.BadRepeatedLevelOffsetCount;
                previous = offset;
            }

            var expected_boundary_count: usize = 1;
            for (triplets.repetition_levels[1..]) |repetition_level| {
                if (repetition_level <= expected_level) expected_boundary_count += 1;
            }
            expected_boundary_count += 1;
            if (level_offsets.offsets.len != expected_boundary_count) return error.BadRepeatedLevelOffsetCount;
        }
        var row: usize = 0;
        while (row < row_group_rows) : (row += 1) {
            if (triplets.row_offsets[row] >= triplets.row_offsets[row + 1]) return error.BadRowOffsetCount;
            if (triplets.repetition_levels[triplets.row_offsets[row]] != 0) return error.BadRowOffsetCount;
            if (triplets.value_offsets[row] > triplets.value_offsets[row + 1]) return error.BadValueOffsetCount;
            var row_value_count: usize = 0;
            for (triplets.definition_levels[triplets.row_offsets[row]..triplets.row_offsets[row + 1]]) |level| {
                if (level == triplets.max_definition_level) row_value_count += 1;
            }
            if (triplets.value_offsets[row + 1] - triplets.value_offsets[row] != row_value_count) return error.BadValueOffsetCount;
        }
        const row_group_value_count = ownedColumnValueCount(triplets.values);
        if (triplets.value_offsets[row_group_rows] != row_group_value_count) return error.BadValueOffsetCount;

        total_levels += triplets.repetition_levels.len;
        for (triplets.repetition_levels) |level| {
            if (level == 0) total_rep_zeroes += 1;
            if (level > triplets.max_repetition_level) return error.BadRepetitionLevel;
        }
        for (triplets.definition_levels) |level| {
            if (level > triplets.max_definition_level) return error.BadDefinitionLevel;
        }

        switch (triplets.values) {
            .boolean => |values| {
                total_values += row_group_value_count;
                for (values.values) |value| {
                    if (value) total_payload_sum += 1;
                }
            },
            .int32 => |values| {
                total_values += row_group_value_count;
                for (values.values) |value| total_payload_sum += value;
            },
            .int64 => |values| {
                total_values += row_group_value_count;
                for (values.values) |value| total_payload_sum += value;
            },
            .byte_array => |values| {
                total_values += row_group_value_count;
                for (values.values) |value| total_payload_sum += @intCast(value.len);
            },
            else => return error.UnsupportedTripletValidationType,
        }
    }

    if (total_levels != expected_levels) return error.BadLevelCount;
    if (total_values != expected_values) return error.BadValueCount;
    if (total_rep_zeroes != expected_rep_zeroes) return error.BadRepetitionLevel;
    if (total_payload_sum != expected_payload_sum) return error.BadValueSum;
    if (expected_repeated_level_counts) |expected_counts| {
        const total_counts = total_repeated_level_counts orelse return error.BadRepeatedLevelOffsetCount;
        if (total_counts.len != expected_counts.len) return error.BadRepeatedLevelOffsetCount;
        for (total_counts, expected_counts, 1..) |actual, expected, level| {
            if (actual != expected) {
                std.debug.print("bad repeated level boundary count level={d} actual={d} expected={d}\n", .{ level, actual, expected });
                return error.BadRepeatedLevelOffsetCount;
            }
        }
    }
}

fn parseExpectedRepeatedLevelCounts(allocator: std.mem.Allocator, arg: []const u8) ![]usize {
    if (arg.len == 0 or std.mem.eql(u8, arg, "-")) return try allocator.alloc(usize, 0);
    var out: std.ArrayList(usize) = .empty;
    errdefer out.deinit(allocator);

    var parts = std.mem.splitScalar(u8, arg, ',');
    while (parts.next()) |part| {
        if (part.len == 0) return error.BadRepeatedLevelOffsetCount;
        try out.append(allocator, try std.fmt.parseInt(usize, part, 10));
    }
    return out.toOwnedSlice(allocator);
}

fn ownedColumnValueCount(column: parquet.types.OwnedColumn) usize {
    return switch (column) {
        inline else => |values| values.values.len,
    };
}
