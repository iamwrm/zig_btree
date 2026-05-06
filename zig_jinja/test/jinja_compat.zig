const std = @import("std");
const jinja = @import("jinja");

test "compatibility smoke covers common jinja2 rendering patterns" {
    const allocator = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "title": "Orders",
        \\  "orders": [
        \\    {"id": 7, "customer": "Ada", "total": 42.5},
        \\    {"id": 8, "customer": "Grace", "total": 13}
        \\  ],
        \\  "raw": "<b>trusted</b>"
        \\}
    , .{});
    defer parsed.deinit();

    var env = jinja.Environment.init(allocator);
    defer env.deinit();
    try env.addTemplate("row", "{{ loop.index }}. #{{ order.id }} {{ order.customer|upper }}={{ order.total }};");

    const out = try env.renderString(
        \\{{ title|lower|capitalize }}
        \\{% for order in orders -%}
        \\{% include "row" %}
        \\{%- endfor %}
        \\{{ raw }}|{{ raw|safe }}
    , parsed.value);
    defer allocator.free(out);

    try std.testing.expect(std.mem.indexOf(u8, out, "Orders") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "1. #7 ADA=42.5;") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "2. #8 GRACE=13;") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "&lt;b&gt;trusted&lt;/b&gt;|<b>trusted</b>") != null);
}

test "operators tests and loop else" {
    const allocator = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"names":["ada","grace"],"empty":[],"count":2}
    , .{});
    defer parsed.deinit();

    var env = jinja.Environment.init(allocator);
    defer env.deinit();

    const out = try env.renderString(
        \\{% if count >= 2 %}ok{% endif %}
        \\{% if "ada" in names|join(",") %} in{% endif %}
        \\{% if missing is undefined %} missing{% endif %}
        \\{% for x in empty %}bad{% else %} empty{% endfor %}
    , parsed.value);
    defer allocator.free(out);

    try std.testing.expect(std.mem.indexOf(u8, out, "ok") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "in") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "missing") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "empty") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "bad") == null);
}
