const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("phmap", .{
        .root_source_file = b.path("src/phmap.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/phmap.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    const basic_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/phmap_basic.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "phmap", .module = mod }},
        }),
    });
    const run_basic_tests = b.addRunArtifact(basic_tests);

    const stress_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/phmap_stress.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "phmap", .module = mod }},
        }),
    });
    const run_stress_tests = b.addRunArtifact(stress_tests);

    const test_step = b.step("test", "Run phmap tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_basic_tests.step);
    test_step.dependOn(&run_stress_tests.step);

    const bench_exe = b.addExecutable(.{
        .name = "phmap_bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/phmap_bench.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "phmap", .module = mod }},
        }),
    });
    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |args| run_bench.addArgs(args);

    const bench_step = b.step("bench", "Run the phmap microbenchmark");
    bench_step.dependOn(&run_bench.step);
}
