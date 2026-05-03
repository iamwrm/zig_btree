const std = @import("std");
const parquet = @import("parquet");

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    const schema_cols = [_]parquet.Column{
        .{ .name = "id", .column_type = .{ .physical = .int64 }, .repetition = .required },
        .{ .name = "name", .column_type = .{ .physical = .byte_array, .logical = .string }, .repetition = .optional },
    };
    const schema = parquet.Schema.init("schema", &schema_cols);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    var file_writer = parquet.writer.StreamWriter.init(allocator, &out.writer, schema);
    defer file_writer.deinit();
    try file_writer.start();

    const ids = [_]i64{ 1, 2, 3 };
    const names = [_][]const u8{ "ann", "bob" };
    const validity = [_]bool{ true, false, true };
    const batch = [_]parquet.ColumnData{
        .{ .int64 = .{ .values = ids[0..] } },
        .{ .byte_array = .{ .values = names[0..], .validity = validity[0..] } },
    };
    try file_writer.writeRowGroup(3, &batch);
    try file_writer.finish();

    var parsed = try parquet.reader.readFileFromMemory(allocator, out.written());
    defer parsed.deinit();

    if (parsed.metadata.num_rows != 3) return error.BadRowCount;
    if (parsed.metadata.schema.columns.len != 2) return error.BadColumnCount;
}
