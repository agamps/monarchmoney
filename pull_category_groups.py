"""
pull_category_groups.py
-----------------------
Pulls all Monarch Money categories with their parent group info and writes:

    category_groups.csv
        Category Name | Category ID | Group Name | Group ID

Usage:
    python pull_category_groups.py
    python pull_category_groups.py --output my_groups.csv
"""

import asyncio
import argparse
import csv
import subprocess
import sys
from pathlib import Path

from gql.transport.exceptions import TransportServerError
from monarchmoney import MonarchMoney

SESSION_FILE = Path(".mm/mm_session.pickle")
LOGIN_SCRIPT = Path("login.py")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Monarch Money category → group mappings to CSV."
    )
    parser.add_argument(
        "--output",
        default="category_groups.csv",
        help="Output CSV file path (default: category_groups.csv)",
    )
    return parser.parse_args()


async def get_mm() -> MonarchMoney:
    if not SESSION_FILE.exists():
        print(f"Session file not found: {SESSION_FILE}")
        print(f"Running {LOGIN_SCRIPT}...")
        result = subprocess.run([sys.executable, str(LOGIN_SCRIPT)], check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"Unable to create a Monarch session. {LOGIN_SCRIPT} exited with code {result.returncode}."
            )

        if not SESSION_FILE.exists():
            raise RuntimeError(
                f"{LOGIN_SCRIPT} completed but did not create {SESSION_FILE}."
            )

    mm = MonarchMoney()
    mm.load_session(str(SESSION_FILE))

    try:
        await mm.get_accounts()
        return mm
    except TransportServerError as e:
        if "401" not in str(e):
            raise

        print("Saved session expired. Re-running login.py...")
        result = subprocess.run([sys.executable, str(LOGIN_SCRIPT)], check=False)
        if result.returncode != 0:
            raise RuntimeError(
                "Monarch session expired and automatic re-login failed. "
                f"Please run `py .\\{LOGIN_SCRIPT}` and try again."
            ) from e

        if not SESSION_FILE.exists():
            raise RuntimeError(
                "Monarch re-login completed but no session file was saved. "
                f"Expected: {SESSION_FILE}"
            ) from e

        mm = MonarchMoney()
        mm.load_session(str(SESSION_FILE))
        await mm.get_accounts()
        return mm


async def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    mm = await get_mm()

    print("\n⬇️  Fetching categories...")
    cat_result = await mm.get_transaction_categories()
    categories = cat_result.get("categories", [])
    print(f"   Found {len(categories)} categories.")

    rows = []
    for cat in categories:
        group = cat.get("group") or {}
        rows.append(
            {
                "Category Name": cat.get("name", ""),
                "Category ID":   cat.get("id", ""),
                "Group Name":    group.get("name", ""),
                "Group ID":      group.get("id", ""),
            }
        )

    # Sort: Group Name first, then Category Name within each group
    rows.sort(key=lambda r: (r["Group Name"].lower(), r["Category Name"].lower()))

    fieldnames = ["Category Name", "Category ID", "Group Name", "Group ID"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✅  Saved {len(rows)} rows → {output_path}")

    # Print a quick preview grouped by group
    current_group = None
    for r in rows:
        if r["Group Name"] != current_group:
            current_group = r["Group Name"]
            print(f"\n  📁 {current_group}  ({r['Group ID']})")
        print(f"      {r['Category Name']}  ({r['Category ID']})")


if __name__ == "__main__":
    asyncio.run(main())
