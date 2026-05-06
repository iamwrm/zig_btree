const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("jinja", .{
        .root_source_file = b.path("src/jinja.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jinja.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    const compat_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/jinja_compat.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "jinja", .module = mod }},
        }),
    });
    const run_compat_tests = b.addRunArtifact(compat_tests);

    const test_step = b.step("test", "Run zig_jinja tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_compat_tests.step);
}
