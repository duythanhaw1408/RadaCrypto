from __future__ import annotations

import argparse
from pathlib import Path

def doctor() -> int:
    required = [
        Path("sql/sqlite/001_state.sql"),
        Path("sql/sqlite/002_indexes.sql"),
        Path("configs/profiles/swing_perp.yaml"),
        Path("src/cfte/books/local_book.py"),
        Path("src/cfte/features/tape.py"),
        Path("src/cfte/thesis/engines.py"),
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        print("Missing required files:")
        for item in missing:
            print(f" - {item}")
        return 1
    print("Doctor OK: core files are present.")
    return 0

def main() -> int:
    parser = argparse.ArgumentParser(prog="cfte")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("doctor")
    args = parser.parse_args()

    if args.cmd == "doctor":
        return doctor()

    parser.print_help()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
