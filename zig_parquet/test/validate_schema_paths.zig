const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingInputPath;

    var file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);

    var reader_buffer: [64 * 1024]u8 = undefined;
    var io_reader = file.reader(init.io, &reader_buffer);
    var parsed = try parquet.reader.StreamFileReader.init(init.gpa, &io_reader);
    defer parsed.deinit();

    var flat_paths: std.ArrayList([]const u8) = .empty;
    defer flat_paths.deinit(init.gpa);

    for (parsed.metadata.schema.columns, 0..) |column, column_index| {
        const expected = args.next() orelse return error.MissingExpectedPath;
        const found_index = parsed.columnIndexByPath(expected) orelse return error.BadSchemaPath;
        if (found_index != column_index) return error.BadSchemaPath;
        if (!dottedPathEquals(column.path, expected)) return error.BadSchemaPath;
        if (column.repeated_level_info.len != column.max_repetition_level) return error.BadRepeatedLevelPath;
        for (column.repeated_level_info, 1..) |level_info, expected_level| {
            if (level_info.repetition_level != expected_level) return error.BadRepeatedLevelPath;
            if (level_info.path.len == 0 or level_info.path.len > column.path.len) return error.BadRepeatedLevelPath;
            for (level_info.path, 0..) |part, part_index| {
                if (!std.mem.eql(u8, part, column.path[part_index])) return error.BadRepeatedLevelPath;
            }
        }
        var has_list_logical = false;
        var has_map_logical = false;
        for (column.nested_logical_info) |logical_info| {
            if (logical_info.definition_level > column.max_definition_level) return error.BadNestedLogicalPath;
            if (logical_info.repetition_level > column.max_repetition_level) return error.BadNestedLogicalPath;
            if (logical_info.path.len == 0 or logical_info.path.len > column.path.len) return error.BadNestedLogicalPath;
            for (logical_info.path, 0..) |part, part_index| {
                if (!std.mem.eql(u8, part, column.path[part_index])) return error.BadNestedLogicalPath;
            }
            switch (logical_info.kind) {
                .list => has_list_logical = true,
                .map => has_map_logical = true,
            }
        }
        if (column.list_info != null and !has_list_logical) return error.BadNestedLogicalPath;
        if (column.map_info != null and !has_map_logical) return error.BadNestedLogicalPath;
        for (parsed.metadata.row_groups) |row_group| {
            if (column_index >= row_group.columns.len) return error.BadColumnCount;
            if (!std.mem.eql(u8, row_group.columns[column_index].path, expected)) return error.BadColumnChunkPath;
        }
        if (column.max_repetition_level == 0 and column.repetition != .repeated) {
            try flat_paths.append(init.gpa, expected);
        }
    }
    if (args.next() != null) return error.BadColumnCount;

    if (parsed.metadata.row_groups.len != 0) {
        const row_group_indexes = try init.gpa.alloc(usize, parsed.metadata.row_groups.len);
        defer init.gpa.free(row_group_indexes);
        for (row_group_indexes, 0..) |*row_group_index, index| row_group_index.* = index;

        const columns = try parsed.readRowGroupSelectedColumnsByPath(init.gpa, 0, flat_paths.items);
        defer {
            for (columns) |*column| column.deinit(init.gpa);
            init.gpa.free(columns);
        }
        if (columns.len != flat_paths.items.len) return error.BadColumnCount;

        const batches = try parsed.readRowGroupsSelectedColumnsParallelByPath(init.gpa, row_group_indexes, flat_paths.items, .{ .max_threads = 2 });
        defer {
            for (batches) |*batch| batch.deinit(init.gpa);
            init.gpa.free(batches);
        }
        if (batches.len != parsed.metadata.row_groups.len) return error.BadRowCount;
        for (batches) |batch| {
            if (batch.columns.len != flat_paths.items.len) return error.BadColumnCount;
        }
    }
}

fn dottedPathEquals(path: []const []const u8, expected: []const u8) bool {
    var parts = std.mem.splitScalar(u8, expected, '.');
    var index: usize = 0;
    while (parts.next()) |part| : (index += 1) {
        if (index >= path.len) return false;
        if (!std.mem.eql(u8, path[index], part)) return false;
    }
    return index == path.len;
}
