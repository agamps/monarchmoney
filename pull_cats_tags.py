import asyncio
import json
import csv
from monarchmoney import MonarchMoney

async def main():
    mm = MonarchMoney()
    mm.load_session()

    print("Fetching categories...")
    cats = await mm.get_transaction_categories()
    cat_list = cats.get("categories", [])

    # JSON - name to ID mapping
    cat_map = {c["name"]: c["id"] for c in cat_list}
    with open("categories.json", "w") as f:
        json.dump(cat_map, f, indent=2)
    print(f"Saved: categories.json ({len(cat_map)} categories)")

    # CSV
    with open("categories.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Category Name", "Category ID"])
        for name in sorted(cat_map.keys()):
            writer.writerow([name, cat_map[name]])
    print(f"Saved: categories.csv ({len(cat_map)} categories)")

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

    with open("tags.json", "w") as f:
        json.dump(tag_map, f, indent=2)

    print(f"Saved: tags.json ({len(tag_map)} tags)")

asyncio.run(main())
