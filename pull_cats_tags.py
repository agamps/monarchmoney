import asyncio
import json
import csv
import os
import subprocess
import sys
from pathlib import Path
import argparse

from gql.transport.exceptions import TransportServerError
from monarchmoney import MonarchMoney

SESSION_FILE = Path(".mm/mm_session.pickle")
LOGIN_SCRIPT = Path("login.py")
DEFAULT_DATA_DIR = Path(os.environ.get("MONARCH_DATA_DIR", "data"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory where categories/tags files will be written.",
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


async def main():
    args = parse_args()
    data_dir = args.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    categories_json = data_dir / "categories.json"
    categories_csv = data_dir / "categories.csv"
    tags_json = data_dir / "tags.json"
    tags_csv = data_dir / "tags.csv"

    mm = await get_mm()

    print("Fetching categories...")
    cats = await mm.get_transaction_categories()
    cat_list = cats.get("categories", [])

    # JSON - name to ID mapping
    cat_map = {c["name"]: c["id"] for c in cat_list}
    with open(categories_json, "w", encoding="utf-8") as f:
        json.dump(cat_map, f, indent=2)
    print(f"Saved: {categories_json} ({len(cat_map)} categories)")

    # CSV
    with open(categories_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Category Name", "Category ID"])
        for name in sorted(cat_map.keys()):
            writer.writerow([name, cat_map[name]])
    print(f"Saved: {categories_csv} ({len(cat_map)} categories)")

    '''
    print("Fetching tags...")
    tags_data = await mm.get_transaction_tags()
    tag_list = tags_data.get("transactionTags", [])
    tag_map = {t["name"]: t["id"] for t in tag_list}
    with open("tags.json", "w") as f:
        json.dump(tag_map, f, indent=2)
    print(f"Saved: tags.json ({len(tag_map)} tags)")
    '''
    
    
    print("Fetching tags...")
    tags_data = await mm.get_transaction_tags()
    tag_list = tags_data.get("householdTransactionTags", [])
    tag_map = {t["name"]: t["id"] for t in tag_list}

    with open(tags_json, "w", encoding="utf-8") as f:
        json.dump(tag_map, f, indent=2)

    print(f"Saved: {tags_json} ({len(tag_map)} tags)")

    with open(tags_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Tag Name", "Tag ID"])
        for name in sorted(tag_map.keys()):
            writer.writerow([name, tag_map[name]])

    print(f"Saved: {tags_csv} ({len(tag_map)} tags)")

asyncio.run(main())
