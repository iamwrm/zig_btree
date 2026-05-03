const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("parquet", .{
        .root_source_file = b.path("src/parquet.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/parquet.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    const smoke = b.addExecutable(.{
        .name = "parquet_smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/parquet_smoke.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = mod }},
        }),
    });
    const run_smoke = b.addRunArtifact(smoke);
    if (b.args) |args| run_smoke.addArgs(args);

    const write_fixture = b.addExecutable(.{
        .name = "parquet_write_fixture",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/write_fixture.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = mod }},
        }),
    });
    b.installArtifact(write_fixture);

    const read_fixture = b.addExecutable(.{
        .name = "parquet_read_fixture",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/read_fixture.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = mod }},
        }),
    });
    b.installArtifact(read_fixture);

    const validate_sequence = b.addExecutable(.{
        .name = "parquet_validate_sequence",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/validate_sequence.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = mod }},
        }),
    });
    b.installArtifact(validate_sequence);

    const validate_ids = b.addExecutable(.{
        .name = "parquet_validate_ids",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/validate_ids.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = mod }},
        }),
    });
    b.installArtifact(validate_ids);

    const digest = b.addExecutable(.{
        .name = "parquet_digest",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/digest.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = mod }},
        }),
    });
    b.installArtifact(digest);

    const write_sequence = b.addExecutable(.{
        .name = "parquet_write_sequence",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/write_sequence.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = mod }},
        }),
    });
    b.installArtifact(write_sequence);

    const write_matrix = b.addExecutable(.{
        .name = "parquet_write_matrix",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/write_matrix.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = mod }},
        }),
    });
    b.installArtifact(write_matrix);

    const bench_read = b.addExecutable(.{
        .name = "parquet_bench_read",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/bench_read.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "parquet", .module = mod }},
        }),
    });
    b.installArtifact(bench_read);

    const test_step = b.step("test", "Run parquet tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_smoke.step);
}
