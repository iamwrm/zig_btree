const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = addBtreeModule(b, target, optimize);

    const unit_tests = addTestArtifact(b, "src/btree.zig", target, optimize, &.{});
    const run_unit_tests = b.addRunArtifact(unit_tests);

    const stress_tests = addTestArtifact(
        b,
        "test/btree_stress.zig",
        target,
        optimize,
        &.{.{ .name = "btree", .module = mod }},
    );
    const run_stress_tests = b.addRunArtifact(stress_tests);

    const unit_test_step = b.step("unit-test", "Run btree unit tests");
    unit_test_step.dependOn(&run_unit_tests.step);

    const stress_test_step = b.step("stress-test", "Run btree stress tests");
    stress_test_step.dependOn(&run_stress_tests.step);

    const test_step = b.step("test", "Run btree tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_stress_tests.step);

    const bench_exe = addBenchmarkArtifact(b, mod, target, optimize);
    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |args| run_bench.addArgs(args);

    const bench_step = b.step("bench", "Run the B-tree microbenchmark");
    bench_step.dependOn(&run_bench.step);
}

fn addBtreeModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Module {
    return b.addModule("btree", .{
        .root_source_file = b.path("src/btree.zig"),
        .target = target,
        .optimize = optimize,
    });
}

fn addTestArtifact(
    b: *std.Build,
    root_source_file: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    imports: []const std.Build.Module.Import,
) *std.Build.Step.Compile {
    return b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(root_source_file),
            .target = target,
            .optimize = optimize,
            .imports = imports,
        }),
    });
}

fn addBenchmarkArtifact(
    b: *std.Build,
    mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step.Compile {
    return b.addExecutable(.{
        .name = "btree_bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/btree_bench.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "btree", .module = mod }},
        }),
    });
}
