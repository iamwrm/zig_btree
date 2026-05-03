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
        const columns = try parsed.readRowGroupColumns(init.gpa, rg_idx);
        defer {
            for (columns) |*column| column.deinit(init.gpa);
            init.gpa.free(columns);
        }
        switch (columns[0]) {
            .int64 => |ids| {
                for (ids.values) |id| {
                    if (id != expected) return error.BadIdSequence;
                    expected += 1;
                }
            },
            else => return error.BadColumnType,
        }
    }
    if (expected != expected_rows) return error.BadRowCount;
}
