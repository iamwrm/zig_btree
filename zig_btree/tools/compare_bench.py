#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import tempfile
import unittest
from pathlib import Path
from statistics import median


WORKLOADS = ("insert", "lookup", "iterate", "remove")


def parse_samples(path: Path) -> dict[str, list[float]]:
    samples: dict[str, list[float]] = {}
    for line in path.read_text().splitlines():
        match = re.match(r"(insert|lookup|iterate|remove):\s+([0-9.]+) ns/(?:op|item)", line)
        if match:
            samples.setdefault(match.group(1), []).append(float(match.group(2)))
    return samples


def build_summary(zig_path: Path, abseil_path: Path, runs: str) -> str:
    zig = parse_samples(zig_path)
    abseil = parse_samples(abseil_path)

    lines = [
        "# zig_btree vs C++ Abseil btree medians",
        "",
        f"Each implementation ran {runs} benchmark samples.",
        "",
        "| workload | Zig median | C++ Abseil btree median | Zig gap | Zig samples | C++ samples |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for workload in WORKLOADS:
        zig_samples = zig.get(workload, [])
        abseil_samples = abseil.get(workload, [])
        if not zig_samples or not abseil_samples:
            lines.append(
                f"| `{workload}` | missing | missing | n/a | {len(zig_samples)} | {len(abseil_samples)} |"
            )
            continue
        zig_median = median(zig_samples)
        abseil_median = median(abseil_samples)
        gap = ((zig_median / abseil_median) - 1.0) * 100.0
        lines.append(
            f"| `{workload}` | {zig_median:.3f} ns | {abseil_median:.3f} ns | "
            f"{gap:+.1f}% | {len(zig_samples)} | {len(abseil_samples)} |"
        )

    return "\n".join(lines) + "\n"


def missing_workloads(zig_path: Path, abseil_path: Path) -> list[str]:
    zig = parse_samples(zig_path)
    abseil = parse_samples(abseil_path)
    missing: list[str] = []
    for workload in WORKLOADS:
        if not zig.get(workload):
            missing.append(f"zig:{workload}")
        if not abseil.get(workload):
            missing.append(f"abseil:{workload}")
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare zig_btree and Abseil benchmark logs.")
    parser.add_argument("--zig", type=Path, required=True, help="Path to zig_btree benchmark log.")
    parser.add_argument("--abseil", type=Path, required=True, help="Path to C++ Abseil benchmark log.")
    parser.add_argument("--runs", default=os.environ.get("BENCH_RUNS", "?"), help="Benchmark sample count.")
    parser.add_argument("--output", type=Path, default=Path("benchmark-summary.md"), help="Markdown output path.")
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Write a summary even if a workload is missing from one of the logs.",
    )
    args = parser.parse_args()

    summary = build_summary(args.zig, args.abseil, args.runs)
    print(summary)
    args.output.write_text(summary)

    github_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if github_summary:
        with Path(github_summary).open("a") as handle:
            handle.write(summary)

    missing = missing_workloads(args.zig, args.abseil)
    if missing and not args.allow_missing:
        raise SystemExit("missing benchmark samples: " + ", ".join(missing))


class CompareBenchTests(unittest.TestCase):
    def test_parse_samples_accepts_ops_and_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bench.txt"
            path.write_text(
                "insert:  10.000 ns/op\n"
                "lookup:  20.000 ns/op\n"
                "iterate: 3.000 ns/item\n"
                "remove:  30.000 ns/op\n"
            )

            self.assertEqual(
                parse_samples(path),
                {
                    "insert": [10.0],
                    "lookup": [20.0],
                    "iterate": [3.0],
                    "remove": [30.0],
                },
            )

    def test_build_summary_reports_medians_and_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zig_path = Path(tmp) / "zig.txt"
            abseil_path = Path(tmp) / "abseil.txt"
            zig_path.write_text("insert:  10.000 ns/op\ninsert:  20.000 ns/op\ninsert:  30.000 ns/op\n")
            abseil_path.write_text("insert:  5.000 ns/op\ninsert:  10.000 ns/op\ninsert:  15.000 ns/op\n")

            summary = build_summary(zig_path, abseil_path, "3")

            self.assertIn("Each implementation ran 3 benchmark samples.", summary)
            self.assertIn("| `insert` | 20.000 ns | 10.000 ns | +100.0% | 3 | 3 |", summary)
            self.assertIn("| `lookup` | missing | missing | n/a | 0 | 0 |", summary)

    def test_missing_workloads_reports_each_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zig_path = Path(tmp) / "zig.txt"
            abseil_path = Path(tmp) / "abseil.txt"
            zig_path.write_text("insert:  10.000 ns/op\n")
            abseil_path.write_text("lookup:  20.000 ns/op\n")

            self.assertIn("abseil:insert", missing_workloads(zig_path, abseil_path))
            self.assertIn("zig:lookup", missing_workloads(zig_path, abseil_path))


if __name__ == "__main__":
    main()
