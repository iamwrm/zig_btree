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
    if (parsed.metadata.num_rows != 5) return error.BadRowCount;
    if (parsed.metadata.row_groups.len != 2) return error.BadRowGroupCount;

    var ids = try parsed.readColumn(init.gpa, 0, 0);
    defer ids.deinit(init.gpa);
    const expected_ids = [_]i64{ 1, 2, 3 };
    if (!std.mem.eql(i64, ids.int64.values, expected_ids[0..])) return error.BadIds;

    var names = try parsed.readColumn(init.gpa, 0, 2);
    defer names.deinit(init.gpa);
    const expected_valid = [_]bool{ true, false, true };
    if (!std.mem.eql(bool, names.byte_array.validity.?, expected_valid[0..])) return error.BadValidity;
    if (!std.mem.eql(u8, names.byte_array.values[0], "ann")) return error.BadNames;
    if (!std.mem.eql(u8, names.byte_array.values[1], "cat")) return error.BadNames;

    var blobs = try parsed.readColumn(init.gpa, 0, 3);
    defer blobs.deinit(init.gpa);
    if (!std.mem.eql(u8, blobs.fixed_len_byte_array.values[0], "aaaa")) return error.BadFixedBinary;
    if (!std.mem.eql(u8, blobs.fixed_len_byte_array.values[1], "bbbb")) return error.BadFixedBinary;
    if (!std.mem.eql(u8, blobs.fixed_len_byte_array.values[2], "cccc")) return error.BadFixedBinary;

    if (parsed.metadata.schema.columns[4].column_type.logical != .decimal) return error.BadDecimal;
    if (parsed.metadata.schema.columns[4].column_type.decimal_precision != 9) return error.BadDecimal;
    if (parsed.metadata.schema.columns[4].column_type.decimal_scale != 2) return error.BadDecimal;
    var amounts = try parsed.readColumn(init.gpa, 0, 4);
    defer amounts.deinit(init.gpa);
    switch (amounts) {
        .int64 => |d| {
            const expected_amounts = [_]i64{ 12345, -678, 0 };
            if (!std.mem.eql(i64, d.values, expected_amounts[0..])) return error.BadDecimal;
        },
        .fixed_len_byte_array => |d| {
            if (d.values.len != 3) return error.BadDecimal;
        },
        .byte_array => |d| {
            if (d.values.len != 3) return error.BadDecimal;
        },
        else => return error.BadDecimal,
    }
}
