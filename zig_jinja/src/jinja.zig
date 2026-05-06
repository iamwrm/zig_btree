//! Jinja2-style template renderer backed by `std.json.Value`.
//!
//! The implementation intentionally keeps the public surface small:
//! create an `Environment`, optionally register named templates for includes,
//! then render a template string against a JSON value.  Templates are parsed
//! into nodes before rendering, so control-flow and expression failures report
//! structured errors instead of silently producing malformed output.

const std = @import("std");

pub const Error = error{
    BadSyntax,
    BadExpression,
    UnknownTag,
    UnexpectedEnd,
    UnmatchedEndTag,
    IncludeNotFound,
    TypeMismatch,
    DivisionByZero,
    OutOfMemory,
};

pub const Options = struct {
    autoescape: bool = true,
    trim_blocks: bool = false,
    lstrip_blocks: bool = false,
    missing_is_error: bool = false,
    max_include_depth: usize = 64,
};

pub const Environment = struct {
    allocator: std.mem.Allocator,
    options: Options,
    templates: std.StringHashMap([]const u8),

    pub fn init(allocator: std.mem.Allocator) Environment {
        return initOptions(allocator, .{});
    }

    pub fn initOptions(allocator: std.mem.Allocator, options: Options) Environment {
        return .{
            .allocator = allocator,
            .options = options,
            .templates = std.StringHashMap([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *Environment) void {
        var it = self.templates.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.templates.deinit();
        self.* = undefined;
    }

    pub fn addTemplate(self: *Environment, name: []const u8, source: []const u8) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const value = try self.allocator.dupe(u8, source);
        errdefer self.allocator.free(value);

        if (try self.templates.fetchPut(key, value)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value);
        }
    }

    pub fn renderString(self: *Environment, source: []const u8, data: std.json.Value) ![]u8 {
        var parser = Parser.init(self.allocator, source, self.options);
        var template = try parser.parse();
        defer template.deinit(self.allocator);

        var renderer = Renderer.init(self, data);
        defer renderer.deinit();
        try renderer.renderNodes(template.nodes);
        return renderer.finish();
    }
};

const Node = union(enum) {
    text: []const u8,
    expr: []const u8,
    set: SetNode,
    include: []const u8,
    if_node: *IfNode,
    for_node: *ForNode,

    fn deinit(self: Node, allocator: std.mem.Allocator) void {
        switch (self) {
            .text => |s| allocator.free(s),
            .expr => |s| allocator.free(s),
            .include => |s| allocator.free(s),
            .set => |s| {
                allocator.free(s.name);
                allocator.free(s.expr);
            },
            .if_node => |n| {
                n.deinit(allocator);
                allocator.destroy(n);
            },
            .for_node => |n| {
                n.deinit(allocator);
                allocator.destroy(n);
            },
        }
    }
};

const SetNode = struct {
    name: []const u8,
    expr: []const u8,
};

const IfBranch = struct {
    condition: ?[]const u8,
    body: []Node,

    fn deinit(self: IfBranch, allocator: std.mem.Allocator) void {
        if (self.condition) |condition| allocator.free(condition);
        freeNodes(allocator, self.body);
    }
};

const IfNode = struct {
    branches: []IfBranch,

    fn deinit(self: *IfNode, allocator: std.mem.Allocator) void {
        for (self.branches) |branch| branch.deinit(allocator);
        allocator.free(self.branches);
    }
};

const ForNode = struct {
    item_name: []const u8,
    iterable_expr: []const u8,
    body: []Node,
    else_body: []Node,

    fn deinit(self: *ForNode, allocator: std.mem.Allocator) void {
        allocator.free(self.item_name);
        allocator.free(self.iterable_expr);
        freeNodes(allocator, self.body);
        freeNodes(allocator, self.else_body);
    }
};

const Template = struct {
    nodes: []Node,

    fn deinit(self: *Template, allocator: std.mem.Allocator) void {
        freeNodes(allocator, self.nodes);
    }
};

fn freeNodes(allocator: std.mem.Allocator, nodes: []Node) void {
    for (nodes) |node| node.deinit(allocator);
    allocator.free(nodes);
}

const TagKind = enum { variable, block, comment };

const Tag = struct {
    kind: TagKind,
    start: usize,
    content_start: usize,
    content_end: usize,
    end: usize,
    trim_left: bool,
    trim_right: bool,
};

const Stop = struct {
    tag: []const u8,
    rest: []const u8,
};

const ParseResult = struct {
    nodes: []Node,
    stop: ?Stop,
};

const Parser = struct {
    allocator: std.mem.Allocator,
    source: []const u8,
    options: Options,
    pos: usize,

    fn init(allocator: std.mem.Allocator, source: []const u8, options: Options) Parser {
        return .{ .allocator = allocator, .source = source, .options = options, .pos = 0 };
    }

    fn parse(self: *Parser) anyerror!Template {
        const result = try self.parseUntil(&.{});
        if (result.stop != null) {
            freeNodes(self.allocator, result.nodes);
            return Error.UnmatchedEndTag;
        }
        return .{ .nodes = result.nodes };
    }

    fn parseUntil(self: *Parser, comptime stops: []const []const u8) anyerror!ParseResult {
        var nodes: std.ArrayList(Node) = .empty;
        errdefer {
            for (nodes.items) |node| node.deinit(self.allocator);
            nodes.deinit(self.allocator);
        }

        while (self.pos < self.source.len) {
            const tag = self.nextTag(self.pos) orelse {
                if (self.pos < self.source.len) try self.appendText(&nodes, self.source[self.pos..]);
                self.pos = self.source.len;
                break;
            };

            var text_end = tag.start;
            if (tag.trim_left) text_end = trimRightAscii(self.source[self.pos..text_end]) + self.pos;
            if (text_end > self.pos) try self.appendText(&nodes, self.source[self.pos..text_end]);

            self.pos = tag.end;
            if (tag.trim_right) self.pos = trimLeftFrom(self.source, self.pos);

            const raw = std.mem.trim(u8, self.source[tag.content_start..tag.content_end], ascii_ws);
            switch (tag.kind) {
                .comment => continue,
                .variable => try nodes.append(self.allocator, .{ .expr = try self.allocator.dupe(u8, raw) }),
                .block => {
                    const head = firstWord(raw);
                    inline for (stops) |stop_name| {
                        if (std.mem.eql(u8, head, stop_name)) {
                            return .{ .nodes = try nodes.toOwnedSlice(self.allocator), .stop = .{ .tag = stop_name, .rest = std.mem.trim(u8, raw[head.len..], ascii_ws) } };
                        }
                    }

                    if (std.mem.eql(u8, head, "if")) {
                        try nodes.append(self.allocator, .{ .if_node = try self.parseIf(std.mem.trim(u8, raw[2..], ascii_ws)) });
                    } else if (std.mem.eql(u8, head, "for")) {
                        try nodes.append(self.allocator, .{ .for_node = try self.parseFor(std.mem.trim(u8, raw[3..], ascii_ws)) });
                    } else if (std.mem.eql(u8, head, "set")) {
                        try nodes.append(self.allocator, .{ .set = try self.parseSet(std.mem.trim(u8, raw[3..], ascii_ws)) });
                    } else if (std.mem.eql(u8, head, "include")) {
                        try nodes.append(self.allocator, .{ .include = try self.parseInclude(std.mem.trim(u8, raw[7..], ascii_ws)) });
                    } else {
                        return Error.UnknownTag;
                    }
                },
            }
        }

        return .{ .nodes = try nodes.toOwnedSlice(self.allocator), .stop = null };
    }

    fn appendText(self: *Parser, nodes: *std.ArrayList(Node), raw: []const u8) !void {
        if (raw.len == 0) return;
        try nodes.append(self.allocator, .{ .text = try self.allocator.dupe(u8, raw) });
    }

    fn parseIf(self: *Parser, first_condition: []const u8) anyerror!*IfNode {
        var branches: std.ArrayList(IfBranch) = .empty;
        errdefer {
            for (branches.items) |branch| branch.deinit(self.allocator);
            branches.deinit(self.allocator);
        }

        var condition: ?[]const u8 = try self.allocator.dupe(u8, first_condition);
        while (true) {
            const body = try self.parseUntil(&.{ "elif", "else", "endif" });
            errdefer freeNodes(self.allocator, body.nodes);
            try branches.append(self.allocator, .{ .condition = condition, .body = body.nodes });
            condition = null;

            const stop = body.stop orelse return Error.UnexpectedEnd;
            if (std.mem.eql(u8, stop.tag, "endif")) break;
            if (std.mem.eql(u8, stop.tag, "else")) {
                const else_body = try self.parseUntil(&.{"endif"});
                errdefer freeNodes(self.allocator, else_body.nodes);
                if (else_body.stop == null) return Error.UnexpectedEnd;
                try branches.append(self.allocator, .{ .condition = null, .body = else_body.nodes });
                break;
            }
            condition = try self.allocator.dupe(u8, stop.rest);
        }

        const node = try self.allocator.create(IfNode);
        node.* = .{ .branches = try branches.toOwnedSlice(self.allocator) };
        return node;
    }

    fn parseFor(self: *Parser, spec: []const u8) anyerror!*ForNode {
        const in_pos = findWord(spec, "in") orelse return Error.BadSyntax;
        const item = std.mem.trim(u8, spec[0..in_pos], ascii_ws);
        const expr = std.mem.trim(u8, spec[in_pos + 2 ..], ascii_ws);
        if (!isIdentifier(item) or expr.len == 0) return Error.BadSyntax;

        const body = try self.parseUntil(&.{ "else", "endfor" });
        errdefer freeNodes(self.allocator, body.nodes);
        var else_body: []Node = &.{};
        errdefer if (else_body.len != 0) freeNodes(self.allocator, else_body);

        const stop = body.stop orelse return Error.UnexpectedEnd;
        if (std.mem.eql(u8, stop.tag, "else")) {
            const parsed_else = try self.parseUntil(&.{"endfor"});
            if (parsed_else.stop == null) {
                freeNodes(self.allocator, parsed_else.nodes);
                return Error.UnexpectedEnd;
            }
            else_body = parsed_else.nodes;
        }

        const node = try self.allocator.create(ForNode);
        node.* = .{
            .item_name = try self.allocator.dupe(u8, item),
            .iterable_expr = try self.allocator.dupe(u8, expr),
            .body = body.nodes,
            .else_body = else_body,
        };
        return node;
    }

    fn parseSet(self: *Parser, spec: []const u8) !SetNode {
        const eq = std.mem.indexOfScalar(u8, spec, '=') orelse return Error.BadSyntax;
        const name = std.mem.trim(u8, spec[0..eq], ascii_ws);
        const expr = std.mem.trim(u8, spec[eq + 1 ..], ascii_ws);
        if (!isIdentifier(name) or expr.len == 0) return Error.BadSyntax;
        return .{
            .name = try self.allocator.dupe(u8, name),
            .expr = try self.allocator.dupe(u8, expr),
        };
    }

    fn parseInclude(self: *Parser, spec: []const u8) ![]const u8 {
        const name = try parseStringLiteral(spec);
        return self.allocator.dupe(u8, name);
    }

    fn nextTag(self: *Parser, from: usize) ?Tag {
        var i = from;
        while (i + 1 < self.source.len) : (i += 1) {
            const kind: ?TagKind = if (std.mem.eql(u8, self.source[i .. i + 2], "{{"))
                .variable
            else if (std.mem.eql(u8, self.source[i .. i + 2], "{%"))
                .block
            else if (std.mem.eql(u8, self.source[i .. i + 2], "{#"))
                .comment
            else
                null;
            const actual = kind orelse continue;
            const close = switch (actual) {
                .variable => "}}",
                .block => "%}",
                .comment => "#}",
            };
            var content_start = i + 2;
            const trim_left = content_start < self.source.len and self.source[content_start] == '-';
            if (trim_left) content_start += 1;

            const search = content_start;
            while (std.mem.indexOf(u8, self.source[search..], close)) |rel| {
                const close_start = search + rel;
                var content_end = close_start;
                const trim_right = content_end > content_start and self.source[content_end - 1] == '-';
                if (trim_right) content_end -= 1;
                return .{
                    .kind = actual,
                    .start = i,
                    .content_start = content_start,
                    .content_end = content_end,
                    .end = close_start + 2,
                    .trim_left = trim_left,
                    .trim_right = trim_right,
                };
            }
            return null;
        }
        return null;
    }
};

const RuntimeValue = union(enum) {
    missing,
    null,
    bool: bool,
    int: i64,
    float: f64,
    string: []const u8,
    safe_string: []const u8,
    json: *const std.json.Value,
    array: []const std.json.Value,

    fn isTruthy(self: RuntimeValue) bool {
        return switch (self) {
            .missing, .null => false,
            .bool => |v| v,
            .int => |v| v != 0,
            .float => |v| v != 0,
            .string, .safe_string => |s| s.len != 0,
            .array => |a| a.len != 0,
            .json => |v| jsonTruthy(v),
        };
    }

    fn isSafe(self: RuntimeValue) bool {
        return switch (self) {
            .safe_string => true,
            else => false,
        };
    }
};

const Scope = struct {
    values: std.StringHashMap(RuntimeValue),

    fn init(allocator: std.mem.Allocator) Scope {
        return .{ .values = std.StringHashMap(RuntimeValue).init(allocator) };
    }

    fn deinit(self: *Scope) void {
        self.values.deinit();
    }
};

const Renderer = struct {
    env: *Environment,
    root: std.json.Value,
    out: std.ArrayList(u8),
    scratch: std.ArrayList([]u8),
    scopes: std.ArrayList(Scope),
    include_depth: usize = 0,

    fn init(env: *Environment, root: std.json.Value) Renderer {
        return .{
            .env = env,
            .root = root,
            .out = .empty,
            .scratch = .empty,
            .scopes = .empty,
        };
    }

    fn deinit(self: *Renderer) void {
        for (self.scratch.items) |owned| self.env.allocator.free(owned);
        self.scratch.deinit(self.env.allocator);
        for (self.scopes.items) |*scope| scope.deinit();
        self.scopes.deinit(self.env.allocator);
        self.out.deinit(self.env.allocator);
    }

    fn finish(self: *Renderer) ![]u8 {
        return self.out.toOwnedSlice(self.env.allocator);
    }

    fn renderNodes(self: *Renderer, nodes: []const Node) anyerror!void {
        for (nodes) |node| {
            switch (node) {
                .text => |text| try self.out.appendSlice(self.env.allocator, text),
                .expr => |expr| {
                    const value = try self.eval(expr);
                    try self.writeValue(value);
                },
                .set => |set| {
                    if (self.scopes.items.len == 0) try self.pushScope();
                    const value = try self.eval(set.expr);
                    try self.scopes.items[self.scopes.items.len - 1].values.put(set.name, value);
                },
                .include => |name| try self.renderInclude(name),
                .if_node => |if_node| try self.renderIf(if_node),
                .for_node => |for_node| try self.renderFor(for_node),
            }
        }
    }

    fn renderIf(self: *Renderer, node: *const IfNode) anyerror!void {
        for (node.branches) |branch| {
            if (branch.condition == null or (try self.eval(branch.condition.?)).isTruthy()) {
                try self.renderNodes(branch.body);
                return;
            }
        }
    }

    fn renderFor(self: *Renderer, node: *const ForNode) anyerror!void {
        const iterable = try self.eval(node.iterable_expr);
        const items = switch (iterable) {
            .array => |a| a,
            .json => |v| switch (v.*) {
                .array => |a| a.items,
                else => return Error.TypeMismatch,
            },
            else => return Error.TypeMismatch,
        };

        if (items.len == 0) {
            try self.renderNodes(node.else_body);
            return;
        }

        try self.pushScope();
        defer self.popScope();
        var i: usize = 0;
        while (i < items.len) : (i += 1) {
            const scope = &self.scopes.items[self.scopes.items.len - 1];
            try scope.values.put(node.item_name, .{ .json = &items[i] });
            try scope.values.put("loop.index0", .{ .int = @intCast(i) });
            try scope.values.put("loop.index", .{ .int = @intCast(i + 1) });
            try scope.values.put("loop.length", .{ .int = @intCast(items.len) });
            try scope.values.put("loop.first", .{ .bool = i == 0 });
            try scope.values.put("loop.last", .{ .bool = i + 1 == items.len });
            try self.renderNodes(node.body);
        }
    }

    fn renderInclude(self: *Renderer, name: []const u8) anyerror!void {
        if (self.include_depth >= self.env.options.max_include_depth) return Error.BadSyntax;
        const source = self.env.templates.get(name) orelse return Error.IncludeNotFound;
        var parser = Parser.init(self.env.allocator, source, self.env.options);
        var template = try parser.parse();
        defer template.deinit(self.env.allocator);

        self.include_depth += 1;
        defer self.include_depth -= 1;
        try self.renderNodes(template.nodes);
    }

    fn pushScope(self: *Renderer) !void {
        try self.scopes.append(self.env.allocator, Scope.init(self.env.allocator));
    }

    fn popScope(self: *Renderer) void {
        var scope = self.scopes.pop().?;
        scope.deinit();
    }

    fn eval(self: *Renderer, expr: []const u8) anyerror!RuntimeValue {
        var evaluator = ExprParser{ .renderer = self, .input = expr, .pos = 0 };
        const value = try evaluator.parseExpression();
        evaluator.skipWs();
        if (evaluator.pos != evaluator.input.len) return Error.BadExpression;
        return value;
    }

    fn lookup(self: *Renderer, name: []const u8) RuntimeValue {
        var i = self.scopes.items.len;
        while (i > 0) {
            i -= 1;
            if (self.scopes.items[i].values.get(name)) |value| return value;
            if (firstPathSegment(name)) |segment| {
                if (segment.len < name.len) {
                    if (self.scopes.items[i].values.get(segment)) |base| {
                        if (lookupPath(base, name[segment.len + 1 ..])) |value| return value;
                    }
                }
            }
        }
        if (lookupPath(.{ .json = &self.root }, name)) |value| return value;
        if (self.env.options.missing_is_error) return .missing;
        return .missing;
    }

    fn writeValue(self: *Renderer, value: RuntimeValue) !void {
        if (self.env.options.missing_is_error and value == .missing) return Error.BadExpression;
        const s = try self.stringify(value);
        if (self.env.options.autoescape and !value.isSafe()) {
            try appendEscaped(self.env.allocator, &self.out, s);
        } else {
            try self.out.appendSlice(self.env.allocator, s);
        }
    }

    fn stringify(self: *Renderer, value: RuntimeValue) anyerror![]const u8 {
        return switch (value) {
            .missing, .null => "",
            .bool => |v| if (v) "true" else "false",
            .int => |v| try self.allocPrint("{}", .{v}),
            .float => |v| try self.allocPrint("{d}", .{v}),
            .string, .safe_string => |s| s,
            .array => |a| try self.jsonStringifyArray(a),
            .json => |v| try self.jsonStringify(v),
        };
    }

    fn allocPrint(self: *Renderer, comptime fmt: []const u8, args: anytype) ![]const u8 {
        const text = try std.fmt.allocPrint(self.env.allocator, fmt, args);
        try self.scratch.append(self.env.allocator, text);
        return text;
    }

    fn own(self: *Renderer, text: []const u8) ![]const u8 {
        const owned = try self.env.allocator.dupe(u8, text);
        try self.scratch.append(self.env.allocator, owned);
        return owned;
    }

    fn jsonStringify(self: *Renderer, value: *const std.json.Value) anyerror![]const u8 {
        switch (value.*) {
            .null => return "",
            .bool => |v| return if (v) "true" else "false",
            .integer => |v| return self.allocPrint("{}", .{v}),
            .float => |v| return self.allocPrint("{d}", .{v}),
            .number_string, .string => |s| return s,
            .array => |a| return self.jsonStringifyArray(a.items),
            .object => {},
        }
        const owned = try std.json.Stringify.valueAlloc(self.env.allocator, value.*, .{});
        try self.scratch.append(self.env.allocator, owned);
        return owned;
    }

    fn jsonStringifyArray(self: *Renderer, items: []const std.json.Value) ![]const u8 {
        const owned = try std.json.Stringify.valueAlloc(self.env.allocator, items, .{});
        try self.scratch.append(self.env.allocator, owned);
        return owned;
    }
};

const ExprParser = struct {
    renderer: *Renderer,
    input: []const u8,
    pos: usize,

    fn parseExpression(self: *ExprParser) anyerror!RuntimeValue {
        return self.parseFilter();
    }

    fn parseFilter(self: *ExprParser) anyerror!RuntimeValue {
        const value = try self.parseOr();
        return self.parseFilterTail(value);
    }

    fn parseFilterTail(self: *ExprParser, initial: RuntimeValue) anyerror!RuntimeValue {
        var value = initial;
        while (true) {
            self.skipWs();
            if (!self.consume("|")) break;
            self.skipWs();
            const name = try self.parseIdentifier();
            var args: std.ArrayList(RuntimeValue) = .empty;
            defer args.deinit(self.renderer.env.allocator);
            self.skipWs();
            if (self.consume("(")) {
                self.skipWs();
                if (!self.consume(")")) {
                    while (true) {
                        try args.append(self.renderer.env.allocator, try self.parseExpression());
                        self.skipWs();
                        if (self.consume(")")) break;
                        if (!self.consume(",")) return Error.BadExpression;
                    }
                }
            }
            value = try self.applyFilter(name, value, args.items);
        }
        return value;
    }

    fn parseOr(self: *ExprParser) anyerror!RuntimeValue {
        var left = try self.parseAnd();
        while (true) {
            self.skipWs();
            if (!self.consumeWord("or")) break;
            const right = try self.parseAnd();
            left = .{ .bool = left.isTruthy() or right.isTruthy() };
        }
        return left;
    }

    fn parseAnd(self: *ExprParser) anyerror!RuntimeValue {
        var left = try self.parseNot();
        while (true) {
            self.skipWs();
            if (!self.consumeWord("and")) break;
            const right = try self.parseNot();
            left = .{ .bool = left.isTruthy() and right.isTruthy() };
        }
        return left;
    }

    fn parseNot(self: *ExprParser) anyerror!RuntimeValue {
        self.skipWs();
        if (self.consumeWord("not")) {
            if (self.peekWord("in")) {
                self.pos -= 3;
                return self.parseCompare();
            }
            return .{ .bool = !(try self.parseNot()).isTruthy() };
        }
        return self.parseCompare();
    }

    fn parseCompare(self: *ExprParser) anyerror!RuntimeValue {
        var left = try self.parseTest();
        left = try self.parseFilterTail(left);
        while (true) {
            self.skipWs();
            const op = if (self.consume("==")) "==" else if (self.consume("!=")) "!=" else if (self.consume("<=")) "<=" else if (self.consume(">=")) ">=" else if (self.consume("<")) "<" else if (self.consume(">")) ">" else if (self.consumeWord("not")) blk: {
                if (!self.consumeWord("in")) {
                    self.pos -= 3;
                    break;
                }
                break :blk "not in";
            } else if (self.consumeWord("in")) "in" else break;
            var right = try self.parseTest();
            right = try self.parseFilterTail(right);
            const cmp = try compareValues(self.renderer, op, left, right);
            left = .{ .bool = cmp };
        }
        return left;
    }

    fn parseTest(self: *ExprParser) anyerror!RuntimeValue {
        var value = try self.parsePrimary();
        self.skipWs();
        if (self.consumeWord("is")) {
            self.skipWs();
            var negated = false;
            if (self.consumeWord("not")) {
                negated = true;
                self.skipWs();
            }
            const name = try self.parseIdentifier();
            var ok = testValue(name, value);
            if (negated) ok = !ok;
            value = .{ .bool = ok };
        }
        return value;
    }

    fn parsePrimary(self: *ExprParser) anyerror!RuntimeValue {
        self.skipWs();
        if (self.consume("(")) {
            const value = try self.parseExpression();
            self.skipWs();
            if (!self.consume(")")) return Error.BadExpression;
            return value;
        }
        if (self.peek() == '"' or self.peek() == '\'') return .{ .string = try self.parseString() };
        if (self.peekIsDigit() or self.peek() == '-') return self.parseNumber();

        const ident = try self.parseIdentifier();
        if (std.mem.eql(u8, ident, "true") or std.mem.eql(u8, ident, "True")) return .{ .bool = true };
        if (std.mem.eql(u8, ident, "false") or std.mem.eql(u8, ident, "False")) return .{ .bool = false };
        if (std.mem.eql(u8, ident, "none") or std.mem.eql(u8, ident, "None") or std.mem.eql(u8, ident, "null")) return .null;

        var end = self.pos;
        while (end < self.input.len) {
            if (self.input[end] == '.') {
                end += 1;
                while (end < self.input.len and isIdentChar(self.input[end])) end += 1;
            } else if (self.input[end] == '[') {
                end += 1;
                var quote: u8 = 0;
                while (end < self.input.len) : (end += 1) {
                    const c = self.input[end];
                    if (quote != 0) {
                        if (c == quote) quote = 0;
                    } else if (c == '"' or c == '\'') quote = c else if (c == ']') {
                        end += 1;
                        break;
                    }
                }
            } else break;
        }
        const path = self.input[self.pos - ident.len .. end];
        self.pos = end;
        return self.renderer.lookup(path);
    }

    fn applyFilter(self: *ExprParser, name: []const u8, value: RuntimeValue, args: []RuntimeValue) anyerror!RuntimeValue {
        const r = self.renderer;
        if (std.mem.eql(u8, name, "safe")) return .{ .safe_string = try r.stringify(value) };
        if (std.mem.eql(u8, name, "escape") or std.mem.eql(u8, name, "e")) {
            var buf: std.ArrayList(u8) = .empty;
            errdefer buf.deinit(r.env.allocator);
            try appendEscaped(r.env.allocator, &buf, try r.stringify(value));
            const owned = try buf.toOwnedSlice(r.env.allocator);
            try r.scratch.append(r.env.allocator, owned);
            return .{ .safe_string = owned };
        }
        if (std.mem.eql(u8, name, "default")) {
            if (args.len == 0) return Error.BadExpression;
            if (value == .missing or value == .null or (args.len > 1 and args[1].isTruthy() and !value.isTruthy())) return args[0];
            return value;
        }
        if (std.mem.eql(u8, name, "string")) return .{ .string = try r.stringify(value) };
        if (std.mem.eql(u8, name, "lower")) return .{ .string = try asciiAlloc(r, try r.stringify(value), std.ascii.toLower) };
        if (std.mem.eql(u8, name, "upper")) return .{ .string = try asciiAlloc(r, try r.stringify(value), std.ascii.toUpper) };
        if (std.mem.eql(u8, name, "capitalize")) return .{ .string = try capitalizeAlloc(r, try r.stringify(value)) };
        if (std.mem.eql(u8, name, "title")) return .{ .string = try titleAlloc(r, try r.stringify(value)) };
        if (std.mem.eql(u8, name, "trim")) return .{ .string = std.mem.trim(u8, try r.stringify(value), ascii_ws) };
        if (std.mem.eql(u8, name, "length")) return .{ .int = @intCast(lengthOf(value)) };
        if (std.mem.eql(u8, name, "join")) return .{ .string = try joinFilter(r, value, if (args.len > 0) try r.stringify(args[0]) else "") };
        if (std.mem.eql(u8, name, "replace")) {
            if (args.len < 2) return Error.BadExpression;
            return .{ .string = try replaceAll(r, try r.stringify(value), try r.stringify(args[0]), try r.stringify(args[1])) };
        }
        if (std.mem.eql(u8, name, "int")) return .{ .int = toInt(value) orelse 0 };
        if (std.mem.eql(u8, name, "float")) return .{ .float = toFloat(value) orelse 0 };
        return Error.BadExpression;
    }

    fn parseNumber(self: *ExprParser) !RuntimeValue {
        const start = self.pos;
        if (self.peek() == '-') self.pos += 1;
        while (self.peekIsDigit()) self.pos += 1;
        var is_float = false;
        if (self.peek() == '.') {
            is_float = true;
            self.pos += 1;
            while (self.peekIsDigit()) self.pos += 1;
        }
        const raw = self.input[start..self.pos];
        if (is_float) return .{ .float = try std.fmt.parseFloat(f64, raw) };
        return .{ .int = try std.fmt.parseInt(i64, raw, 10) };
    }

    fn parseString(self: *ExprParser) ![]const u8 {
        const quote = self.peek();
        if (quote != '"' and quote != '\'') return Error.BadExpression;
        self.pos += 1;
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(self.renderer.env.allocator);
        while (self.pos < self.input.len) {
            const c = self.input[self.pos];
            self.pos += 1;
            if (c == quote) {
                const owned = try buf.toOwnedSlice(self.renderer.env.allocator);
                try self.renderer.scratch.append(self.renderer.env.allocator, owned);
                return owned;
            }
            if (c == '\\' and self.pos < self.input.len) {
                const escaped = self.input[self.pos];
                self.pos += 1;
                try buf.append(self.renderer.env.allocator, switch (escaped) {
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    else => escaped,
                });
            } else {
                try buf.append(self.renderer.env.allocator, c);
            }
        }
        return Error.BadExpression;
    }

    fn parseIdentifier(self: *ExprParser) ![]const u8 {
        self.skipWs();
        const start = self.pos;
        if (start >= self.input.len or !isIdentStart(self.input[start])) return Error.BadExpression;
        self.pos += 1;
        while (self.pos < self.input.len and isIdentChar(self.input[self.pos])) self.pos += 1;
        return self.input[start..self.pos];
    }

    fn skipWs(self: *ExprParser) void {
        while (self.pos < self.input.len and std.mem.indexOfScalar(u8, ascii_ws, self.input[self.pos]) != null) self.pos += 1;
    }

    fn consume(self: *ExprParser, lit: []const u8) bool {
        if (!std.mem.startsWith(u8, self.input[self.pos..], lit)) return false;
        self.pos += lit.len;
        return true;
    }

    fn consumeWord(self: *ExprParser, word: []const u8) bool {
        self.skipWs();
        if (!std.mem.startsWith(u8, self.input[self.pos..], word)) return false;
        const end = self.pos + word.len;
        if (end < self.input.len and isIdentChar(self.input[end])) return false;
        self.pos = end;
        return true;
    }

    fn peekWord(self: *ExprParser, word: []const u8) bool {
        const saved = self.pos;
        var copy = self.*;
        copy.pos = saved;
        return copy.consumeWord(word);
    }

    fn peek(self: *const ExprParser) u8 {
        return if (self.pos < self.input.len) self.input[self.pos] else 0;
    }

    fn peekIsDigit(self: *const ExprParser) bool {
        const c = self.peek();
        return c >= '0' and c <= '9';
    }
};

fn lookupPath(root: RuntimeValue, path: []const u8) ?RuntimeValue {
    var cur = root;
    var i: usize = 0;
    while (i < path.len) {
        const start = i;
        if (!isIdentStart(path[i])) return null;
        i += 1;
        while (i < path.len and isIdentChar(path[i])) i += 1;
        const key = path[start..i];
        cur = lookupKey(cur, key) orelse return null;

        while (i < path.len and path[i] == '[') {
            i += 1;
            const close = std.mem.indexOfScalarPos(u8, path, i, ']') orelse return null;
            const raw = std.mem.trim(u8, path[i..close], ascii_ws);
            if (raw.len == 0) return null;
            if (raw[0] == '"' or raw[0] == '\'') {
                const parsed = parseStringLiteral(raw) catch return null;
                cur = lookupKey(cur, parsed) orelse return null;
            } else {
                const index = std.fmt.parseUnsigned(usize, raw, 10) catch return null;
                cur = lookupIndex(cur, index) orelse return null;
            }
            i = close + 1;
        }
        if (i < path.len) {
            if (path[i] != '.') return null;
            i += 1;
        }
    }
    return cur;
}

fn lookupKey(value: RuntimeValue, key: []const u8) ?RuntimeValue {
    if (std.mem.eql(u8, key, "")) return null;
    if (value == .json) {
        switch (value.json.*) {
            .object => |*obj| if (obj.getPtr(key)) |v| return jsonToRuntime(v),
            else => {},
        }
    }
    return null;
}

fn jsonToRuntime(value: *const std.json.Value) RuntimeValue {
    return switch (value.*) {
        .null => .null,
        .bool => |v| .{ .bool = v },
        .integer => |v| .{ .int = v },
        .float => |v| .{ .float = v },
        .number_string, .string => |s| .{ .string = s },
        .array => |a| .{ .array = a.items },
        .object => .{ .json = value },
    };
}

fn lookupIndex(value: RuntimeValue, index: usize) ?RuntimeValue {
    return switch (value) {
        .json => |v| switch (v.*) {
            .array => |a| if (index < a.items.len) .{ .json = &a.items[index] } else null,
            else => null,
        },
        .array => |a| if (index < a.len) .{ .json = &a[index] } else null,
        else => null,
    };
}

fn jsonTruthy(value: *const std.json.Value) bool {
    return switch (value.*) {
        .null => false,
        .bool => |v| v,
        .integer => |v| v != 0,
        .float => |v| v != 0,
        .number_string, .string => |s| s.len != 0,
        .array => |a| a.items.len != 0,
        .object => |o| o.count() != 0,
    };
}

fn appendEscaped(allocator: std.mem.Allocator, out: *std.ArrayList(u8), text: []const u8) !void {
    for (text) |c| {
        switch (c) {
            '&' => try out.appendSlice(allocator, "&amp;"),
            '<' => try out.appendSlice(allocator, "&lt;"),
            '>' => try out.appendSlice(allocator, "&gt;"),
            '"' => try out.appendSlice(allocator, "&#34;"),
            '\'' => try out.appendSlice(allocator, "&#39;"),
            else => try out.append(allocator, c),
        }
    }
}

fn compareValues(renderer: *Renderer, op: []const u8, left: RuntimeValue, right: RuntimeValue) !bool {
    if (std.mem.eql(u8, op, "in") or std.mem.eql(u8, op, "not in")) {
        const needle = try renderer.stringify(left);
        var found = false;
        switch (right) {
            .string, .safe_string => |s| found = std.mem.indexOf(u8, s, needle) != null,
            .array => |a| for (a) |*item| {
                if (std.mem.eql(u8, needle, try renderer.jsonStringify(item))) {
                    found = true;
                    break;
                }
            },
            .json => |v| switch (v.*) {
                .array => |a| for (a.items) |*item| {
                    if (std.mem.eql(u8, needle, try renderer.jsonStringify(item))) {
                        found = true;
                        break;
                    }
                },
                .string => |s| found = std.mem.indexOf(u8, s, needle) != null,
                else => {},
            },
            else => {},
        }
        return if (std.mem.eql(u8, op, "not in")) !found else found;
    }

    if (toFloat(left)) |lf| {
        if (toFloat(right)) |rf| {
            if (std.mem.eql(u8, op, "==")) return lf == rf;
            if (std.mem.eql(u8, op, "!=")) return lf != rf;
            if (std.mem.eql(u8, op, "<")) return lf < rf;
            if (std.mem.eql(u8, op, "<=")) return lf <= rf;
            if (std.mem.eql(u8, op, ">")) return lf > rf;
            if (std.mem.eql(u8, op, ">=")) return lf >= rf;
        }
    }

    const ls = try renderer.stringify(left);
    const rs = try renderer.stringify(right);
    const ord = std.mem.order(u8, ls, rs);
    if (std.mem.eql(u8, op, "==")) return ord == .eq;
    if (std.mem.eql(u8, op, "!=")) return ord != .eq;
    if (std.mem.eql(u8, op, "<")) return ord == .lt;
    if (std.mem.eql(u8, op, "<=")) return ord != .gt;
    if (std.mem.eql(u8, op, ">")) return ord == .gt;
    if (std.mem.eql(u8, op, ">=")) return ord != .lt;
    return Error.BadExpression;
}

fn testValue(name: []const u8, value: RuntimeValue) bool {
    if (std.mem.eql(u8, name, "defined")) return value != .missing;
    if (std.mem.eql(u8, name, "undefined")) return value == .missing;
    if (std.mem.eql(u8, name, "none") or std.mem.eql(u8, name, "null")) return value == .null or (value == .json and value.json.* == .null);
    if (std.mem.eql(u8, name, "string")) return value == .string or value == .safe_string or (value == .json and (value.json.* == .string or value.json.* == .number_string));
    if (std.mem.eql(u8, name, "number")) return value == .int or value == .float or (value == .json and (value.json.* == .integer or value.json.* == .float));
    if (std.mem.eql(u8, name, "boolean")) return value == .bool or (value == .json and value.json.* == .bool);
    if (std.mem.eql(u8, name, "iterable")) return value == .array or (value == .json and value.json.* == .array);
    return false;
}

fn toInt(value: RuntimeValue) ?i64 {
    return switch (value) {
        .int => |v| v,
        .float => |v| @intFromFloat(v),
        .bool => |v| if (v) 1 else 0,
        .string, .safe_string => |s| std.fmt.parseInt(i64, std.mem.trim(u8, s, ascii_ws), 10) catch null,
        .json => |v| switch (v.*) {
            .integer => |i| i,
            .float => |f| @intFromFloat(f),
            .string, .number_string => |s| std.fmt.parseInt(i64, std.mem.trim(u8, s, ascii_ws), 10) catch null,
            .bool => |b| if (b) 1 else 0,
            else => null,
        },
        else => null,
    };
}

fn toFloat(value: RuntimeValue) ?f64 {
    return switch (value) {
        .int => |v| @floatFromInt(v),
        .float => |v| v,
        .bool => |v| if (v) 1 else 0,
        .string, .safe_string => |s| std.fmt.parseFloat(f64, std.mem.trim(u8, s, ascii_ws)) catch null,
        .json => |v| switch (v.*) {
            .integer => |i| @floatFromInt(i),
            .float => |f| f,
            .string, .number_string => |s| std.fmt.parseFloat(f64, std.mem.trim(u8, s, ascii_ws)) catch null,
            .bool => |b| if (b) 1 else 0,
            else => null,
        },
        else => null,
    };
}

fn lengthOf(value: RuntimeValue) usize {
    return switch (value) {
        .missing, .null => 0,
        .bool, .int, .float => 1,
        .string, .safe_string => |s| s.len,
        .array => |a| a.len,
        .json => |v| switch (v.*) {
            .null => 0,
            .bool, .integer, .float => 1,
            .string, .number_string => |s| s.len,
            .array => |a| a.items.len,
            .object => |o| o.count(),
        },
    };
}

fn joinFilter(renderer: *Renderer, value: RuntimeValue, sep: []const u8) ![]const u8 {
    const items = switch (value) {
        .array => |a| a,
        .json => |v| switch (v.*) {
            .array => |a| a.items,
            else => return Error.TypeMismatch,
        },
        else => return Error.TypeMismatch,
    };
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(renderer.env.allocator);
    for (items, 0..) |*item, i| {
        if (i != 0) try buf.appendSlice(renderer.env.allocator, sep);
        try buf.appendSlice(renderer.env.allocator, try renderer.jsonStringify(item));
    }
    const owned = try buf.toOwnedSlice(renderer.env.allocator);
    try renderer.scratch.append(renderer.env.allocator, owned);
    return owned;
}

fn replaceAll(renderer: *Renderer, haystack: []const u8, needle: []const u8, replacement: []const u8) ![]const u8 {
    if (needle.len == 0) return renderer.own(haystack);
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(renderer.env.allocator);
    var start: usize = 0;
    while (std.mem.indexOf(u8, haystack[start..], needle)) |rel| {
        const pos = start + rel;
        try buf.appendSlice(renderer.env.allocator, haystack[start..pos]);
        try buf.appendSlice(renderer.env.allocator, replacement);
        start = pos + needle.len;
    }
    try buf.appendSlice(renderer.env.allocator, haystack[start..]);
    const owned = try buf.toOwnedSlice(renderer.env.allocator);
    try renderer.scratch.append(renderer.env.allocator, owned);
    return owned;
}

fn asciiAlloc(renderer: *Renderer, text: []const u8, comptime op: fn (u8) u8) ![]const u8 {
    const owned = try renderer.env.allocator.dupe(u8, text);
    for (owned) |*c| c.* = op(c.*);
    try renderer.scratch.append(renderer.env.allocator, owned);
    return owned;
}

fn capitalizeAlloc(renderer: *Renderer, text: []const u8) ![]const u8 {
    const owned = try renderer.env.allocator.dupe(u8, text);
    if (owned.len > 0) {
        owned[0] = std.ascii.toUpper(owned[0]);
        for (owned[1..]) |*c| c.* = std.ascii.toLower(c.*);
    }
    try renderer.scratch.append(renderer.env.allocator, owned);
    return owned;
}

fn titleAlloc(renderer: *Renderer, text: []const u8) ![]const u8 {
    const owned = try renderer.env.allocator.dupe(u8, text);
    var word = true;
    for (owned) |*c| {
        if (std.ascii.isAlphabetic(c.*)) {
            c.* = if (word) std.ascii.toUpper(c.*) else std.ascii.toLower(c.*);
            word = false;
        } else {
            word = true;
        }
    }
    try renderer.scratch.append(renderer.env.allocator, owned);
    return owned;
}

fn parseStringLiteral(raw: []const u8) ![]const u8 {
    const s = std.mem.trim(u8, raw, ascii_ws);
    if (s.len < 2) return Error.BadSyntax;
    const quote = s[0];
    if ((quote != '"' and quote != '\'') or s[s.len - 1] != quote) return Error.BadSyntax;
    return s[1 .. s.len - 1];
}

fn trimRightAscii(text: []const u8) usize {
    var end = text.len;
    while (end > 0 and std.mem.indexOfScalar(u8, ascii_ws, text[end - 1]) != null) end -= 1;
    return end;
}

fn trimLeftFrom(source: []const u8, start: usize) usize {
    var pos = start;
    while (pos < source.len and std.mem.indexOfScalar(u8, ascii_ws, source[pos]) != null) pos += 1;
    return pos;
}

fn firstWord(text: []const u8) []const u8 {
    var i: usize = 0;
    while (i < text.len and !std.ascii.isWhitespace(text[i])) i += 1;
    return text[0..i];
}

fn findWord(text: []const u8, word: []const u8) ?usize {
    var i: usize = 0;
    while (i + word.len <= text.len) : (i += 1) {
        if (!std.mem.eql(u8, text[i .. i + word.len], word)) continue;
        const before = i == 0 or !isIdentChar(text[i - 1]);
        const after = i + word.len == text.len or !isIdentChar(text[i + word.len]);
        if (before and after) return i;
    }
    return null;
}

fn firstPathSegment(path: []const u8) ?[]const u8 {
    if (path.len == 0 or !isIdentStart(path[0])) return null;
    var i: usize = 1;
    while (i < path.len and isIdentChar(path[i])) i += 1;
    return path[0..i];
}

fn isIdentifier(text: []const u8) bool {
    if (text.len == 0 or !isIdentStart(text[0])) return false;
    for (text[1..]) |c| if (!isIdentChar(c)) return false;
    return true;
}

fn isIdentStart(c: u8) bool {
    return std.ascii.isAlphabetic(c) or c == '_';
}

fn isIdentChar(c: u8) bool {
    return isIdentStart(c) or std.ascii.isDigit(c);
}

const ascii_ws = " \t\r\n";

test "renders interpolation, escaping, conditionals, loops, and filters" {
    const allocator = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"user":{"name":"Ada <Lovelace>","admin":true},"items":["zig","jinja"],"empty":[]}
    , .{});
    defer parsed.deinit();

    var env = Environment.init(allocator);
    defer env.deinit();

    const out = try env.renderString(
        \\Hello {{ user.name }}.
        \\{% if user.admin %}admin{% else %}user{% endif %}
        \\{% for item in items %}[{{ loop.index }}:{{ item|upper }}]{% else %}empty{% endfor %}
        \\{{ missing|default("fallback") }}
    , parsed.value);
    defer allocator.free(out);

    try std.testing.expect(std.mem.indexOf(u8, out, "Ada &lt;Lovelace&gt;") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "admin") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "[1:ZIG][2:JINJA]") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "fallback") != null);
}

test "supports includes and whitespace trim markers" {
    const allocator = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, "{\"name\":\"Zig\"}", .{});
    defer parsed.deinit();

    var env = Environment.init(allocator);
    defer env.deinit();
    try env.addTemplate("partial", "{{ name|lower }}");

    const out = try env.renderString("A {%- include \"partial\" -%} B", parsed.value);
    defer allocator.free(out);
    try std.testing.expectEqualStrings("AzigB", out);
}
