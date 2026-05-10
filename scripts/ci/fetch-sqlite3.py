#!/usr/bin/env python3
"""Fetch the SQLite amalgamation used for local relayd builds."""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path


def env_default(name: str, fallback: str) -> str:
    return os.environ.get(name, fallback)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", default=env_default("SQLITE_AMALGAMATION_YEAR", "2026"))
    parser.add_argument("--version", default=env_default("SQLITE_AMALGAMATION_VERSION", "3530100"))
    parser.add_argument(
        "--sha3",
        default=env_default(
            "SQLITE_AMALGAMATION_SHA3",
            "3c07136e4f6b5dd0c395be86455014039597bc65b6851f7111e88f71b6e06114",
        ),
    )
    parser.add_argument("--output-dir", default="lib")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    url = f"https://sqlite.org/{args.year}/sqlite-amalgamation-{args.version}.zip"
    output_dir = Path(args.output_dir)

    with tempfile.TemporaryDirectory(prefix="sqlite-amalgamation-") as tmp:
        tmp_dir = Path(tmp)
        archive = tmp_dir / "sqlite-amalgamation.zip"
        urllib.request.urlretrieve(url, archive)

        actual = hashlib.sha3_256(archive.read_bytes()).hexdigest()
        if actual != args.sha3:
            print(
                f"SQLite amalgamation checksum mismatch: {actual} != {args.sha3}",
                file=sys.stderr,
            )
            return 1

        with zipfile.ZipFile(archive) as zf:
            zf.extractall(tmp_dir)

        source_dir = tmp_dir / f"sqlite-amalgamation-{args.version}"
        sqlite3_c = source_dir / "sqlite3.c"
        sqlite3_h = source_dir / "sqlite3.h"
        if not sqlite3_c.is_file() or not sqlite3_h.is_file():
            print(f"SQLite amalgamation missing expected files under {source_dir}", file=sys.stderr)
            return 1

        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)
        shutil.copy2(sqlite3_c, output_dir / "sqlite3.c")
        shutil.copy2(sqlite3_h, output_dir / "sqlite3.h")

    print(f"Fetched SQLite amalgamation {args.version} into {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
