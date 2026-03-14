import asyncio
import json
import csv
import os
from pathlib import Path
import argparse

from monarchmoney import MonarchMoney

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


async def main():
    args = parse_args()
    data_dir = args.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    categories_json = data_dir / "categories.json"
    categories_csv = data_dir / "categories.csv"
    tags_json = data_dir / "tags.json"
    tags_csv = data_dir / "tags.csv"

    mm = MonarchMoney()
    mm.load_session()

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
