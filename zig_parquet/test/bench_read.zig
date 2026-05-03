const std = @import("std");
const builtin = @import("builtin");
const parquet = @import("parquet");

const Mode = enum {
    all,
    ids,
    score,
    name,
};

const ReaderState = enum {
    reuse,
    fresh,
};

const OsCacheMode = enum {
    keep,
    evict,
};

const ColumnExecution = enum {
    serial,
    parallel,
};

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingInputPath;
    const expected_rows_arg = args.next() orelse return error.MissingExpectedRows;
    const expected_rows = try std.fmt.parseInt(i64, expected_rows_arg, 10);
    const mode_arg = args.next() orelse "all";
    const iterations_arg = args.next() orelse "5";
    const cache_arg = args.next() orelse "no-cache";
    const reader_state_arg = args.next() orelse "reuse-reader";
    const os_cache_arg = args.next() orelse "keep-os-cache";
    const column_execution_arg = args.next() orelse "serial-columns";
    const mode: Mode = if (std.mem.eql(u8, mode_arg, "all"))
        .all
    else if (std.mem.eql(u8, mode_arg, "ids"))
        .ids
    else if (std.mem.eql(u8, mode_arg, "score"))
        .score
    else if (std.mem.eql(u8, mode_arg, "name"))
        .name
    else
        return error.InvalidColumnData;
    const iterations = try std.fmt.parseInt(usize, iterations_arg, 10);
    if (iterations == 0) return error.InvalidColumnData;
    const cache_dictionaries = if (std.mem.eql(u8, cache_arg, "cache-dictionaries"))
        true
    else if (std.mem.eql(u8, cache_arg, "no-cache"))
        false
    else
        return error.InvalidColumnData;
    const reader_state: ReaderState = if (std.mem.eql(u8, reader_state_arg, "reuse-reader"))
        .reuse
    else if (std.mem.eql(u8, reader_state_arg, "fresh-reader"))
        .fresh
    else
        return error.InvalidColumnData;
    const os_cache: OsCacheMode = if (std.mem.eql(u8, os_cache_arg, "keep-os-cache") or std.mem.eql(u8, os_cache_arg, "keep-cache"))
        .keep
    else if (std.mem.eql(u8, os_cache_arg, "evict-os-cache") or std.mem.eql(u8, os_cache_arg, "evict-cache"))
        .evict
    else
        return error.InvalidColumnData;
    const column_execution: ColumnExecution = if (std.mem.eql(u8, column_execution_arg, "serial-columns"))
        .serial
    else if (std.mem.eql(u8, column_execution_arg, "parallel-columns"))
        .parallel
    else
        return error.InvalidColumnData;

    const result = switch (reader_state) {
        .reuse => try benchReuseReader(init, path, expected_rows, mode, iterations, cache_dictionaries, os_cache, column_execution),
        .fresh => try benchFreshReaders(init, path, expected_rows, mode, iterations, cache_dictionaries, os_cache, column_execution),
    };

    var stdout_buffer: [256]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print("rows={d}\n", .{expected_rows});
    try stdout.print("iterations={d}\n", .{iterations});
    try stdout.print("elapsed_ns={d}\n", .{result.elapsed_ns});
    try stdout.print("checksum={d}\n", .{result.checksum});
    try stdout.flush();
}

const BenchResult = struct {
    elapsed_ns: i128,
    checksum: u64,
};

fn benchReuseReader(init: std.process.Init, path: []const u8, expected_rows: i64, mode: Mode, iterations: usize, cache_dictionaries: bool, os_cache: OsCacheMode, column_execution: ColumnExecution) !BenchResult {
    const allocator = std.heap.smp_allocator;
    var file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);

    var reader_buffer: [1024]u8 = undefined;
    var io_reader = file.reader(init.io, &reader_buffer);
    var parsed = try parquet.reader.StreamFileReader.init(allocator, &io_reader);
    defer parsed.deinit();
    parsed.setDictionaryCacheEnabled(cache_dictionaries);
    if (parsed.metadata.num_rows != expected_rows) return error.BadRowCount;

    var checksum = try readOnce(allocator, &parsed, mode, expected_rows, true, cache_dictionaries, column_execution);
    var elapsed_ns: i128 = 0;
    if (os_cache == .keep) {
        const start = std.Io.Timestamp.now(init.io, .awake);
        for (0..iterations) |_| {
            checksum +%= try readOnce(allocator, &parsed, mode, expected_rows, false, cache_dictionaries, column_execution);
        }
        const end = std.Io.Timestamp.now(init.io, .awake);
        elapsed_ns = start.durationTo(end).toNanoseconds();
    } else {
        for (0..iterations) |_| {
            try evictFileCache(init, path);
            const start = std.Io.Timestamp.now(init.io, .awake);
            checksum +%= try readOnce(allocator, &parsed, mode, expected_rows, false, cache_dictionaries, column_execution);
            const end = std.Io.Timestamp.now(init.io, .awake);
            elapsed_ns += start.durationTo(end).toNanoseconds();
        }
    }
    return .{
        .elapsed_ns = elapsed_ns,
        .checksum = checksum,
    };
}

fn benchFreshReaders(init: std.process.Init, path: []const u8, expected_rows: i64, mode: Mode, iterations: usize, cache_dictionaries: bool, os_cache: OsCacheMode, column_execution: ColumnExecution) !BenchResult {
    var checksum = try readFreshOnce(init, path, expected_rows, mode, cache_dictionaries, true, column_execution);
    var elapsed_ns: i128 = 0;
    if (os_cache == .keep) {
        const start = std.Io.Timestamp.now(init.io, .awake);
        for (0..iterations) |_| {
            checksum +%= try readFreshOnce(init, path, expected_rows, mode, cache_dictionaries, false, column_execution);
        }
        const end = std.Io.Timestamp.now(init.io, .awake);
        elapsed_ns = start.durationTo(end).toNanoseconds();
    } else {
        for (0..iterations) |_| {
            try evictFileCache(init, path);
            const start = std.Io.Timestamp.now(init.io, .awake);
            checksum +%= try readFreshOnce(init, path, expected_rows, mode, cache_dictionaries, false, column_execution);
            const end = std.Io.Timestamp.now(init.io, .awake);
            elapsed_ns += start.durationTo(end).toNanoseconds();
        }
    }
    return .{
        .elapsed_ns = elapsed_ns,
        .checksum = checksum,
    };
}

fn evictFileCache(init: std.process.Init, path: []const u8) !void {
    if (builtin.os.tag != .linux) return error.UnsupportedOsCacheEviction;

    var file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);

    const rc = std.os.linux.fadvise(file.handle, 0, 0, std.os.linux.POSIX_FADV.DONTNEED);
    switch (std.os.linux.errno(rc)) {
        .SUCCESS => {},
        .INVAL, .NOSYS => return error.UnsupportedOsCacheEviction,
        else => return error.OsCacheEvictionFailed,
    }
}

fn readFreshOnce(init: std.process.Init, path: []const u8, expected_rows: i64, mode: Mode, cache_dictionaries: bool, validate_all: bool, column_execution: ColumnExecution) !u64 {
    const allocator = std.heap.smp_allocator;
    var file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);

    var reader_buffer: [1024]u8 = undefined;
    var io_reader = file.reader(init.io, &reader_buffer);
    var parsed = try parquet.reader.StreamFileReader.init(allocator, &io_reader);
    defer parsed.deinit();
    parsed.setDictionaryCacheEnabled(cache_dictionaries);
    if (parsed.metadata.num_rows != expected_rows) return error.BadRowCount;
    return try readOnce(allocator, &parsed, mode, expected_rows, validate_all, cache_dictionaries, column_execution);
}

fn readOnce(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, mode: Mode, expected_rows: i64, validate_all: bool, cache_dictionaries: bool, column_execution: ColumnExecution) !u64 {
    return switch (mode) {
        .all => switch (column_execution) {
            .serial => readAllColumns(allocator, parsed, expected_rows, validate_all),
            .parallel => readAllColumnsParallel(allocator, parsed, expected_rows, validate_all, cache_dictionaries),
        },
        .ids => switch (column_execution) {
            .serial => readIdColumn(allocator, parsed, expected_rows, validate_all),
            .parallel => readIdColumnParallel(allocator, parsed, expected_rows, validate_all, cache_dictionaries),
        },
        .score => switch (column_execution) {
            .serial => readScoreColumn(allocator, parsed, expected_rows, validate_all),
            .parallel => readScoreColumnParallel(allocator, parsed, expected_rows, validate_all, cache_dictionaries),
        },
        .name => switch (column_execution) {
            .serial => readNameColumn(allocator, parsed, expected_rows, validate_all),
            .parallel => readNameColumnParallel(allocator, parsed, expected_rows, validate_all, cache_dictionaries),
        },
    };
}

fn readAllColumns(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, expected_rows: i64, validate_all: bool) !u64 {
    if (parsed.metadata.schema.columns.len == 0) return error.BadColumnCount;
    var expected: i64 = 0;
    var checksum: u64 = 0;
    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        const columns = try parsed.readRowGroupColumns(allocator, rg_idx);
        defer {
            for (columns) |*column| column.deinit(allocator);
            allocator.free(columns);
        }
        if (columns.len != parsed.metadata.schema.columns.len) return error.BadColumnCount;
        checksum +%= try checkIds(columns[0], &expected, validate_all);
    }
    if (expected != expected_rows) return error.BadRowCount;
    return checksum;
}

fn readAllColumnsParallel(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, expected_rows: i64, validate_all: bool, cache_dictionaries: bool) !u64 {
    if (parsed.metadata.schema.columns.len == 0) return error.BadColumnCount;
    var expected: i64 = 0;
    var checksum: u64 = 0;
    const row_group_indexes = try allocator.alloc(usize, parsed.metadata.row_groups.len);
    defer allocator.free(row_group_indexes);
    for (row_group_indexes, 0..) |*row_group_index, idx| row_group_index.* = idx;

    const batches = try parsed.readRowGroupsColumnsParallel(allocator, row_group_indexes, .{
        .cache_dictionaries = cache_dictionaries,
    });
    defer {
        for (batches) |*batch| batch.deinit(allocator);
        allocator.free(batches);
    }
    for (batches, 0..) |batch, expected_rg_idx| {
        if (batch.row_group_index != expected_rg_idx) return error.BadRowCount;
        if (batch.columns.len != parsed.metadata.schema.columns.len) return error.BadColumnCount;
        checksum +%= try checkIds(batch.columns[0], &expected, validate_all);
    }
    if (expected != expected_rows) return error.BadRowCount;
    return checksum;
}

fn readIdColumn(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, expected_rows: i64, validate_all: bool) !u64 {
    if (parsed.metadata.schema.columns.len == 0) return error.BadColumnCount;
    var expected: i64 = 0;
    var checksum: u64 = 0;
    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        var ids_col = try parsed.readColumn(allocator, rg_idx, 0);
        defer ids_col.deinit(allocator);
        checksum +%= try checkIds(ids_col, &expected, validate_all);
    }
    if (expected != expected_rows) return error.BadRowCount;
    return checksum;
}

fn readIdColumnParallel(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, expected_rows: i64, validate_all: bool, cache_dictionaries: bool) !u64 {
    if (parsed.metadata.schema.columns.len == 0) return error.BadColumnCount;
    const batches = try readSingleColumnRowGroupsParallel(allocator, parsed, 0, cache_dictionaries);
    defer {
        for (batches) |*batch| batch.deinit(allocator);
        allocator.free(batches);
    }
    var expected: i64 = 0;
    var checksum: u64 = 0;
    for (batches, 0..) |batch, expected_rg_idx| {
        if (batch.row_group_index != expected_rg_idx or batch.columns.len != 1) return error.BadRowCount;
        checksum +%= try checkIds(batch.columns[0], &expected, validate_all);
    }
    if (expected != expected_rows) return error.BadRowCount;
    return checksum;
}

fn readScoreColumn(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, expected_rows: i64, validate_all: bool) !u64 {
    if (parsed.metadata.schema.columns.len < 2) return error.BadColumnCount;
    var expected_start: usize = 0;
    var checksum: u64 = 0;
    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        var score_col = try parsed.readColumn(allocator, rg_idx, 1);
        defer score_col.deinit(allocator);
        checksum +%= try checkScores(score_col, &expected_start, validate_all);
    }
    if (expected_start != @as(usize, @intCast(expected_rows))) return error.BadRowCount;
    return checksum;
}

fn readScoreColumnParallel(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, expected_rows: i64, validate_all: bool, cache_dictionaries: bool) !u64 {
    if (parsed.metadata.schema.columns.len < 2) return error.BadColumnCount;
    const batches = try readSingleColumnRowGroupsParallel(allocator, parsed, 1, cache_dictionaries);
    defer {
        for (batches) |*batch| batch.deinit(allocator);
        allocator.free(batches);
    }
    var expected_start: usize = 0;
    var checksum: u64 = 0;
    for (batches, 0..) |batch, expected_rg_idx| {
        if (batch.row_group_index != expected_rg_idx or batch.columns.len != 1) return error.BadRowCount;
        checksum +%= try checkScores(batch.columns[0], &expected_start, validate_all);
    }
    if (expected_start != @as(usize, @intCast(expected_rows))) return error.BadRowCount;
    return checksum;
}

fn readNameColumn(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, expected_rows: i64, validate_all: bool) !u64 {
    if (parsed.metadata.schema.columns.len < 3) return error.BadColumnCount;
    var row_start: usize = 0;
    var checksum: u64 = 0;
    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        const rows_before = row_start;
        const row_group_rows = std.math.cast(usize, parsed.metadata.row_groups[rg_idx].num_rows) orelse return error.BadRowCount;
        var name_col = try parsed.readColumn(allocator, rg_idx, 2);
        defer name_col.deinit(allocator);
        checksum +%= try checkNames(name_col, rows_before, row_group_rows, validate_all);
        row_start += row_group_rows;
    }
    if (row_start != @as(usize, @intCast(expected_rows))) return error.BadRowCount;
    return checksum;
}

fn readNameColumnParallel(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, expected_rows: i64, validate_all: bool, cache_dictionaries: bool) !u64 {
    if (parsed.metadata.schema.columns.len < 3) return error.BadColumnCount;
    const batches = try readSingleColumnRowGroupsParallel(allocator, parsed, 2, cache_dictionaries);
    defer {
        for (batches) |*batch| batch.deinit(allocator);
        allocator.free(batches);
    }
    var row_start: usize = 0;
    var checksum: u64 = 0;
    for (batches, 0..) |batch, expected_rg_idx| {
        if (batch.row_group_index != expected_rg_idx or batch.columns.len != 1) return error.BadRowCount;
        const row_group_rows = std.math.cast(usize, parsed.metadata.row_groups[batch.row_group_index].num_rows) orelse return error.BadRowCount;
        checksum +%= try checkNames(batch.columns[0], row_start, row_group_rows, validate_all);
        row_start += row_group_rows;
    }
    if (row_start != @as(usize, @intCast(expected_rows))) return error.BadRowCount;
    return checksum;
}

fn readSingleColumnRowGroupsParallel(allocator: std.mem.Allocator, parsed: *parquet.reader.StreamFileReader, column_index: usize, cache_dictionaries: bool) ![]parquet.reader.RowGroupColumns {
    const row_group_indexes = try allocator.alloc(usize, parsed.metadata.row_groups.len);
    defer allocator.free(row_group_indexes);
    for (row_group_indexes, 0..) |*row_group_index, idx| row_group_index.* = idx;
    const column_indexes = [_]usize{column_index};
    return try parsed.readRowGroupsSelectedColumnsParallel(allocator, row_group_indexes, column_indexes[0..], .{
        .cache_dictionaries = cache_dictionaries,
    });
}

fn checkIds(column: parquet.OwnedColumn, expected: *i64, validate_all: bool) !u64 {
    switch (column) {
        .int64 => |ids| {
            if (!validate_all) {
                if (ids.values.len == 0) return 0;
                const first_expected = expected.*;
                const last_expected = first_expected + @as(i64, @intCast(ids.values.len - 1));
                if (ids.values[0] != first_expected or ids.values[ids.values.len - 1] != last_expected) return error.BadIdSequence;
                expected.* = last_expected + 1;
                return @as(u64, @bitCast(ids.values[0])) +% @as(u64, @bitCast(ids.values[ids.values.len - 1])) +% ids.values.len;
            }

            var checksum: u64 = 0;
            for (ids.values) |id| {
                if (id != expected.*) return error.BadIdSequence;
                checksum +%= @bitCast(id);
                expected.* += 1;
            }
            return checksum;
        },
        else => return error.BadColumnType,
    }
}

fn checkScores(column: parquet.OwnedColumn, expected_start: *usize, validate_all: bool) !u64 {
    switch (column) {
        .double => |scores| {
            if (!validate_all) {
                if (scores.values.len == 0) return 0;
                const first_expected = @as(f64, @floatFromInt(expected_start.*)) * 0.25;
                const last_index = expected_start.* + scores.values.len - 1;
                const last_expected = @as(f64, @floatFromInt(last_index)) * 0.25;
                if (scores.values[0] != first_expected or scores.values[scores.values.len - 1] != last_expected) return error.BadColumnType;
                expected_start.* = last_index + 1;
                return @as(u64, @bitCast(scores.values[0])) +% @as(u64, @bitCast(scores.values[scores.values.len - 1])) +% scores.values.len;
            }

            var checksum: u64 = 0;
            for (scores.values) |score| {
                const expected = @as(f64, @floatFromInt(expected_start.*)) * 0.25;
                if (score != expected) return error.BadColumnType;
                checksum +%= @bitCast(score);
                expected_start.* += 1;
            }
            return checksum;
        },
        else => return error.BadColumnType,
    }
}

fn checkNames(column: parquet.OwnedColumn, row_start: usize, row_count: usize, validate_all: bool) !u64 {
    switch (column) {
        .byte_array => |names| {
            const validity = names.validity orelse return error.BadColumnType;
            if (validity.len != row_count) return error.BadRowCount;
            var non_null_count: usize = 0;
            var checksum: u64 = 0;
            for (0..row_count) |i| {
                const row = row_start + i;
                const valid = row % 7 != 0;
                if (validity[i] != valid) return error.BadColumnType;
                if (!valid) continue;
                if (non_null_count >= names.values.len) return error.BadRowCount;
                if (validate_all) {
                    var expected_buf: [16]u8 = undefined;
                    const expected = try std.fmt.bufPrint(&expected_buf, "name-{d}", .{row & 15});
                    if (!std.mem.eql(u8, names.values[non_null_count], expected)) return error.BadColumnType;
                }
                checksum +%= names.values[non_null_count].len + row;
                non_null_count += 1;
            }
            if (non_null_count != names.values.len) return error.BadRowCount;
            return checksum;
        },
        else => return error.BadColumnType,
    }
}
