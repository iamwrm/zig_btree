const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingInputPath;
    const expected_rows_arg = args.next() orelse return error.MissingExpectedRows;
    const expected_rows = try std.fmt.parseInt(i64, expected_rows_arg, 10);

    var file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);

    var reader_buffer: [64 * 1024]u8 = undefined;
    var io_reader = file.reader(init.io, &reader_buffer);
    var parsed = try parquet.reader.StreamFileReader.init(init.gpa, &io_reader);
    defer parsed.deinit();
    if (parsed.metadata.num_rows != expected_rows) return error.BadRowCount;
    if (parsed.metadata.schema.columns.len == 0) return error.BadColumnCount;

    var expected: i64 = 0;
    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        var info_iter = try parsed.columnPageInfoIterator(init.gpa, rg_idx, 0);
        defer info_iter.deinit();
        var info_expected: i64 = expected;
        while (try info_iter.next()) |page_info_value| {
            var page_info = page_info_value;
            defer page_info.deinit(init.gpa);
            if (page_info.row_start != @as(usize, @intCast(info_expected - expected))) return error.BadPageInfo;
            if (page_info.row_count == 0) return error.BadPageInfo;
            if (page_info.compressed_page_size == 0 or page_info.uncompressed_page_size == 0) return error.BadPageInfo;
            if (try page_info.statistics.minPhysical(.{ .physical = .int64 })) |min_value| {
                if (min_value.int64 != info_expected) return error.BadPageInfo;
            }
            info_expected += @intCast(page_info.row_count);
        }
        if (info_expected != expected + parsed.metadata.row_groups[rg_idx].num_rows) return error.BadPageInfo;

        var pages = try parsed.columnPageIterator(init.gpa, rg_idx, 0);
        defer pages.deinit();
        while (try pages.next()) |page| {
            var ids_col = page;
            defer ids_col.deinit(init.gpa);
            switch (ids_col) {
                .int64 => |ids| {
                    for (ids.values) |id| {
                        if (id != expected) return error.BadIdSequence;
                        expected += 1;
                    }
                },
                else => return error.BadColumnType,
            }
        }
    }
    if (expected != expected_rows) return error.BadRowCount;
}
