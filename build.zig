const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const phmap_mod = b.addModule("phmap", .{
        .root_source_file = b.path("zig_phmap/src/phmap.zig"),
        .target = target,
        .optimize = optimize,
    });

    const parquet_mod = b.addModule("parquet", .{
        .root_source_file = b.path("zig_parquet/src/parquet.zig"),
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

    const parquet_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_parquet/src/parquet.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_parquet_unit_tests = b.addRunArtifact(parquet_unit_tests);

    const parquet_smoke = b.addExecutable(.{
        .name = "parquet_smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_parquet/test/parquet_smoke.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = parquet_mod }},
        }),
    });
    const run_parquet_smoke = b.addRunArtifact(parquet_smoke);

    const parquet_write_fixture = b.addExecutable(.{
        .name = "parquet_write_fixture",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_parquet/test/write_fixture.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = parquet_mod }},
        }),
    });
    b.installArtifact(parquet_write_fixture);

    const parquet_read_fixture = b.addExecutable(.{
        .name = "parquet_read_fixture",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_parquet/test/read_fixture.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = parquet_mod }},
        }),
    });
    b.installArtifact(parquet_read_fixture);

    const parquet_validate_sequence = b.addExecutable(.{
        .name = "parquet_validate_sequence",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_parquet/test/validate_sequence.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = parquet_mod }},
        }),
    });
    b.installArtifact(parquet_validate_sequence);

    const parquet_validate_ids = b.addExecutable(.{
        .name = "parquet_validate_ids",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_parquet/test/validate_ids.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = parquet_mod }},
        }),
    });
    b.installArtifact(parquet_validate_ids);

    const parquet_digest = b.addExecutable(.{
        .name = "parquet_digest",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_parquet/test/digest.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = parquet_mod }},
        }),
    });
    b.installArtifact(parquet_digest);

    const parquet_write_sequence = b.addExecutable(.{
        .name = "parquet_write_sequence",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig_parquet/test/write_sequence.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = parquet_mod }},
        }),
    });
    b.installArtifact(parquet_write_sequence);

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_phmap_unit_tests.step);
    test_step.dependOn(&run_phmap_stress_tests.step);
    test_step.dependOn(&run_parquet_unit_tests.step);
    test_step.dependOn(&run_parquet_smoke.step);

    const parquet_test_step = b.step("parquet-test", "Run parquet tests");
    parquet_test_step.dependOn(&run_parquet_unit_tests.step);
    parquet_test_step.dependOn(&run_parquet_smoke.step);

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
