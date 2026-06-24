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
from pathlib import Path

from gql.transport.exceptions import TransportServerError

from monarch_api import configure_monarch_api
from monarch_auth import get_monarch_client

configure_monarch_api()


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


# Authentication handled by `monarch_auth.get_monarch_client()`


async def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    mm = await get_monarch_client()

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
