"""
pull_fresh.py
-------------
Start a fresh download while keeping the configured data folder current:

    1. Move the current data folder (e.g. ~/monarch-data/data) to the next
       free numbered sibling (data1, data2, ...) as an archive.
    2. Create an empty data folder and copy the archived *.txt files into it
       (filter terms, optimizable groups/categories, patterns).
    3. Log in once, then run every pull script into the new folder.

Usage:
    python pull/pull_fresh.py
    python pull/pull_fresh.py --dry-run      # show what would happen
    python pull/pull_fresh.py --no-pull      # archive + new folder only
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from common.config import data_dir

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent


def pull_commands(target: Path) -> list[list[str]]:
    return [
        [str(REPO_DIR / "auth" / "login.py")],
        [str(SCRIPT_DIR / "pull_transactions_persist_batches.py"), "--data-dir", str(target)],
        [str(SCRIPT_DIR / "pull_cats_tags.py"), "--data-dir", str(target)],
        [str(SCRIPT_DIR / "pull_category_groups.py"), "--output", str(target / "category_groups.csv")],
        [str(SCRIPT_DIR / "pull_account_groups.py"), "--output", str(target / "account_groups.csv")],
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Archive the current data folder to dataN and pull fresh data."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned archive and pull steps without changing anything.",
    )
    parser.add_argument(
        "--no-pull",
        action="store_true",
        help="Archive and create the new folder, but skip the pull scripts.",
    )
    return parser.parse_args()


def next_archive_dir(current: Path) -> Path:
    n = 1
    while (current.parent / f"{current.name}{n}").exists():
        n += 1
    return current.parent / f"{current.name}{n}"


def main() -> int:
    args = parse_args()
    current = data_dir()
    has_data = any(current.iterdir())
    archive = next_archive_dir(current) if has_data else None
    carried = sorted(current.glob("*.txt")) if has_data else []

    if archive:
        print(f"Archive: {current} -> {archive}")
        for path in carried:
            print(f"  carry over: {path.name}")
    else:
        print(f"{current} is empty; nothing to archive.")
    commands = [] if args.no_pull else pull_commands(current)
    for command in commands:
        print("Run:", subprocess.list2cmdline([sys.executable, *command]))

    if args.dry_run:
        print("Dry run; nothing changed.")
        return 0

    if archive:
        current.rename(archive)
        current.mkdir()
        for path in carried:
            shutil.copy2(archive / path.name, current / path.name)
        print(f"Archived previous data to {archive}")

    for command in commands:
        result = subprocess.run([sys.executable, *command], check=False)
        if result.returncode != 0:
            print(
                f"\n{Path(command[0]).name} failed (exit {result.returncode}). "
                f"{current} may be incomplete; the previous data is safe in "
                f"{archive or current}. Fix the problem and rerun the failed "
                "pull script directly (not pull_fresh.py, which would archive again).",
                file=sys.stderr,
            )
            return result.returncode

    if commands:
        print(f"\nFresh data is in {current}")
    else:
        print(f"\nReady for a fresh pull into {current}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
