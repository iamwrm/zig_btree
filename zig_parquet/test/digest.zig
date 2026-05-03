const std = @import("std");
const parquet = @import("parquet");

const Sha256 = std.crypto.hash.sha2.Sha256;

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

    var hasher = Sha256.init(.{});
    hasher.update("zig-parquet-digest-v1\x00");
    updateU64(&hasher, @intCast(parsed.metadata.num_rows));
    updateU64(&hasher, parsed.metadata.schema.columns.len);
    for (parsed.metadata.schema.columns) |column| {
        updateBytes(&hasher, column.name);
        updateI32(&hasher, @intFromEnum(column.column_type.physical));
        updateU8(&hasher, @intFromEnum(column.column_type.logical));
        updateI32(&hasher, @intFromEnum(column.repetition));
        updateI32(&hasher, column.column_type.type_length orelse -1);
        updateI32(&hasher, column.column_type.decimal_precision orelse -1);
        updateI32(&hasher, column.column_type.decimal_scale orelse -1);
    }

    var rows_seen: i64 = 0;
    for (0..parsed.metadata.row_groups.len) |rg_idx| {
        const row_group = parsed.metadata.row_groups[rg_idx];
        const row_count = std.math.cast(usize, row_group.num_rows) orelse return error.BadRowCount;
        const columns = try parsed.readRowGroupColumns(init.gpa, rg_idx);
        defer {
            for (columns) |*column| column.deinit(init.gpa);
            init.gpa.free(columns);
        }
        if (columns.len != parsed.metadata.schema.columns.len) return error.BadColumnCount;

        const value_offsets = try init.gpa.alloc(usize, columns.len);
        defer init.gpa.free(value_offsets);
        @memset(value_offsets, 0);

        var row: usize = 0;
        while (row < row_count) : (row += 1) {
            for (columns, 0..) |*column, column_index| {
                try hashCell(&hasher, column, row, &value_offsets[column_index]);
            }
        }

        for (columns, value_offsets) |column, offset| {
            try checkConsumed(column, row_count, offset);
        }
        rows_seen += row_group.num_rows;
    }
    if (rows_seen != parsed.metadata.num_rows) return error.BadRowCount;

    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);

    var stdout_buffer: [256]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print("rows={d}\n", .{parsed.metadata.num_rows});
    try stdout.print("digest={s}\n", .{&hex});
    try stdout.flush();
}

fn hashCell(hasher: *Sha256, column: *const parquet.OwnedColumn, row: usize, value_offset: *usize) !void {
    switch (column.*) {
        .boolean => |data| {
            const index = try valueIndex(hasher, data.validity, row, value_offset);
            if (index) |i| updateU8(hasher, if (data.values[i]) 1 else 0);
        },
        .int32 => |data| {
            const index = try valueIndex(hasher, data.validity, row, value_offset);
            if (index) |i| updateI32(hasher, data.values[i]);
        },
        .int64 => |data| {
            const index = try valueIndex(hasher, data.validity, row, value_offset);
            if (index) |i| updateI64(hasher, data.values[i]);
        },
        .int96 => |data| {
            const index = try valueIndex(hasher, data.validity, row, value_offset);
            if (index) |i| updateBytes(hasher, data.values[i]);
        },
        .float => |data| {
            const index = try valueIndex(hasher, data.validity, row, value_offset);
            if (index) |i| updateU32(hasher, @bitCast(data.values[i]));
        },
        .double => |data| {
            const index = try valueIndex(hasher, data.validity, row, value_offset);
            if (index) |i| updateU64(hasher, @bitCast(data.values[i]));
        },
        .byte_array => |data| {
            const index = try valueIndex(hasher, data.validity, row, value_offset);
            if (index) |i| updateBytes(hasher, data.values[i]);
        },
        .fixed_len_byte_array => |data| {
            const index = try valueIndex(hasher, data.validity, row, value_offset);
            if (index) |i| updateBytes(hasher, data.values[i]);
        },
    }
}

fn valueIndex(hasher: *Sha256, validity: ?[]const bool, row: usize, value_offset: *usize) !?usize {
    if (validity) |valid| {
        if (row >= valid.len) return error.BadRowCount;
        if (!valid[row]) {
            updateU8(hasher, 0);
            return null;
        }
        updateU8(hasher, 1);
        const index = value_offset.*;
        value_offset.* += 1;
        return index;
    }
    updateU8(hasher, 1);
    return row;
}

fn checkConsumed(column: parquet.OwnedColumn, row_count: usize, offset: usize) !void {
    switch (column) {
        inline else => |data| {
            if (data.validity) |_| {
                if (offset != data.values.len) return error.BadRowCount;
            } else if (data.values.len != row_count) {
                return error.BadRowCount;
            }
        },
    }
}

fn updateBytes(hasher: *Sha256, bytes: []const u8) void {
    updateU64(hasher, bytes.len);
    hasher.update(bytes);
}

fn updateU8(hasher: *Sha256, value: u8) void {
    hasher.update(&[_]u8{value});
}

fn updateI32(hasher: *Sha256, value: i32) void {
    updateU32(hasher, @bitCast(value));
}

fn updateU32(hasher: *Sha256, value: u32) void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    hasher.update(&bytes);
}

fn updateI64(hasher: *Sha256, value: i64) void {
    updateU64(hasher, @bitCast(value));
}

fn updateU64(hasher: *Sha256, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hasher.update(&bytes);
}
