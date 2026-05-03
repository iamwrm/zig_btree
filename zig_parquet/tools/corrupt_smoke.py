#!/usr/bin/env python3
import pathlib
import random
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]
TMP = ROOT / "zig_parquet" / "tmp"
ZIG_WRITER = ROOT / "zig-out" / "bin" / "parquet_write_fixture"
ZIG_READER = ROOT / "zig-out" / "bin" / "parquet_read_fixture"


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=check)


def assert_clean_failure(proc: subprocess.CompletedProcess[str], label: str) -> None:
    text = f"{proc.stdout}\n{proc.stderr}".lower()
    bad_markers = (
        "panic",
        "reached unreachable",
        "segmentation fault",
        "bus error",
        "illegal instruction",
    )
    for marker in bad_markers:
        if marker in text:
            raise AssertionError(f"{label}: saw unsafe failure marker {marker!r}\n{text}")


def main() -> int:
    TMP.mkdir(parents=True, exist_ok=True)
    base = TMP / "corrupt_base.parquet"
    run([str(ZIG_WRITER), str(base)])
    data = bytearray(base.read_bytes())
    rng = random.Random(0x5EED)

    cases: list[bytes] = []
    for size in (0, 1, 2, 3, 4, 7, 8, 11, 12, 16, 32, max(0, len(data) - 1)):
        cases.append(bytes(data[:size]))

    for _ in range(96):
        mutated = bytearray(data)
        for _ in range(rng.randint(1, 8)):
            idx = rng.randrange(len(mutated))
            mutated[idx] ^= rng.randrange(1, 256)
        if rng.random() < 0.25:
            del mutated[rng.randrange(len(mutated)) :]
        cases.append(bytes(mutated))

    for idx, payload in enumerate(cases):
        path = TMP / f"corrupt_{idx:03d}.parquet"
        path.write_bytes(payload)
        proc = run([str(ZIG_READER), str(path)], check=False)
        assert_clean_failure(proc, path.name)

    print(f"corrupt-smoke-ok cases={len(cases)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
