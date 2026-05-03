const std = @import("std");

pub fn decompress(compressed: []const u8, output: []u8) !void {
    var pos: usize = 0;
    const expected_len = try readVarUint(compressed, &pos);
    if (expected_len != output.len) return error.CorruptPage;

    var out_pos: usize = 0;
    while (pos < compressed.len) {
        const tag = compressed[pos];
        pos += 1;
        switch (tag & 0x03) {
            0 => {
                const len_code: usize = tag >> 2;
                var len: usize = undefined;
                if (len_code < 60) {
                    len = len_code + 1;
                } else {
                    const bytes = len_code - 59;
                    if (bytes > 4 or pos + bytes > compressed.len) return error.CorruptPage;
                    len = 0;
                    var i: usize = 0;
                    while (i < bytes) : (i += 1) len |= @as(usize, compressed[pos + i]) << @intCast(8 * i);
                    pos += bytes;
                    len += 1;
                }
                if (pos + len > compressed.len or out_pos + len > output.len) return error.CorruptPage;
                @memcpy(output[out_pos..][0..len], compressed[pos..][0..len]);
                pos += len;
                out_pos += len;
            },
            1 => {
                if (pos >= compressed.len) return error.CorruptPage;
                const len = ((tag >> 2) & 0x7) + 4;
                const offset = (@as(usize, tag & 0xe0) << 3) | compressed[pos];
                pos += 1;
                try copyFromSelf(output, &out_pos, offset, len);
            },
            2 => {
                if (pos + 2 > compressed.len) return error.CorruptPage;
                const len = (tag >> 2) + 1;
                const offset = std.mem.readInt(u16, compressed[pos..][0..2], .little);
                pos += 2;
                try copyFromSelf(output, &out_pos, offset, len);
            },
            3 => {
                if (pos + 4 > compressed.len) return error.CorruptPage;
                const len = (tag >> 2) + 1;
                const offset = std.mem.readInt(u32, compressed[pos..][0..4], .little);
                pos += 4;
                try copyFromSelf(output, &out_pos, offset, len);
            },
            else => unreachable,
        }
    }
    if (out_pos != output.len) return error.CorruptPage;
}

fn copyFromSelf(output: []u8, out_pos: *usize, offset: usize, len: usize) !void {
    if (offset == 0 or offset > out_pos.* or out_pos.* + len > output.len) return error.CorruptPage;
    var i: usize = 0;
    while (i < len) : (i += 1) {
        output[out_pos.* + i] = output[out_pos.* - offset + i];
    }
    out_pos.* += len;
}

fn readVarUint(data: []const u8, pos: *usize) !usize {
    var result: usize = 0;
    var shift: u6 = 0;
    while (true) {
        if (pos.* >= data.len) return error.CorruptPage;
        const byte = data[pos.*];
        pos.* += 1;
        result |= (@as(usize, byte & 0x7f) << @intCast(shift));
        if ((byte & 0x80) == 0) return result;
        if (shift >= @bitSizeOf(usize) - 1) return error.CorruptPage;
        shift += 7;
    }
}

test "snappy literal-only block" {
    const compressed = [_]u8{ 5, 16, 'h', 'e', 'l', 'l', 'o' };
    var out: [5]u8 = undefined;
    try decompress(compressed[0..], out[0..]);
    try std.testing.expectEqualStrings("hello", out[0..]);
}
