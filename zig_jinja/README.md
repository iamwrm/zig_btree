# zig_jinja

`zig_jinja` is a small, dependency-free Jinja2-style renderer for Zig.  It is
designed around predictable production behavior rather than string replacement:
templates are lexed, parsed into an AST, and rendered against `std.json.Value`
data with explicit error reporting and optional HTML autoescaping.

Implemented Jinja2-compatible features:

- `{{ expression }}` interpolation with dot/index lookup.
- `{% if %}`, `{% elif %}`, `{% else %}`, `{% endif %}`.
- `{% for item in items %}`, `{% else %}`, `{% endfor %}` with `loop.index`,
  `loop.index0`, `loop.first`, `loop.last`, and `loop.length`.
- `{% set name = expression %}`.
- `{% include "name" %}` through an in-memory template registry.
- `{# comments #}`.
- Jinja whitespace controls using `{%-`, `-%}`, `{{-`, `-}}`, `{#-`, `-#}`.
- Boolean/comparison operators: `and`, `or`, `not`, `==`, `!=`, `<`, `<=`,
  `>`, `>=`, `in`, `not in`.
- Tests: `is defined`, `is undefined`, `is none`, `is string`, `is number`,
  `is boolean`, `is iterable`.
- Filters: `default`, `escape`/`e`, `safe`, `lower`, `upper`, `capitalize`,
  `title`, `trim`, `length`, `join`, `replace`, `string`, `int`, and `float`.

Unsupported advanced Jinja2 features include inheritance, macros, call blocks,
custom extensions, async rendering, and Python-specific object introspection.

## Example

```zig
const std = @import("std");
const jinja = @import("jinja");

var parsed = try std.json.parseFromSlice(std.json.Value, allocator,
    \\{"user":{"name":"Ada"},"items":["zig","jinja"]}
, .{});
defer parsed.deinit();

var env = jinja.Environment.init(allocator);
defer env.deinit();

const out = try env.renderString("Hello {{ user.name }}: {{ items|join(\", \") }}", parsed.value);
defer allocator.free(out);
```
