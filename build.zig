const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const phmap_mod = b.addModule("phmap", .{
        .root_source_file = b.path("zig_phmap/src/phmap.zig"),
        .target = target,
        .optimize = optimize,
    });

    const phmap_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_phmap/src/phmap.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_phmap_unit_tests = b.addRunArtifact(phmap_unit_tests);

    const phmap_stress_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_phmap/test/phmap_stress.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "phmap", .module = phmap_mod }},
        }),
    });
    const run_phmap_stress_tests = b.addRunArtifact(phmap_stress_tests);

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_phmap_unit_tests.step);
    test_step.dependOn(&run_phmap_stress_tests.step);

    const phmap_bench = b.addExecutable(.{
        .name = "phmap_bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_phmap/bench/phmap_bench.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "phmap", .module = phmap_mod }},
        }),
    });
    const run_phmap_bench = b.addRunArtifact(phmap_bench);
    if (b.args) |args| run_phmap_bench.addArgs(args);

    const bench_step = b.step("bench", "Run phmap benchmark");
    bench_step.dependOn(&run_phmap_bench.step);
}
