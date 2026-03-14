import asyncio
import csv
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from gql import gql
from gql.transport.exceptions import TransportServerError, TransportQueryError
from monarchmoney import MonarchMoney

# ----------------------------
# Config
# ----------------------------
SESSION_FILE = Path(".mm/mm_session.pickle")
LOGIN_SCRIPT = Path("login.py")

INPUT_FILE = Path("push.csv")
CATEGORIES_FILE = Path("categories.json")
TAGS_FILE = Path("tags.json")

DRY_RUN = False  # Set to False when ready to push for real


def clean_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text != "" else None


def normalize_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if text == "":
        return None
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False

    raise ValueError(f"Cannot convert to bool: {value!r}")


def normalize_date(value: Any) -> str | None:
    text = clean_str(value)
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    raise ValueError(f"Unsupported date format: {value!r}")


def split_tag_names(value: Any) -> list[str]:
    text = clean_str(value)
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def load_name_id_map(path: Path, label: str) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"{label} file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(f"{path} must be a JSON object of {{name: id}}")

    return {
        str(k).strip(): str(v).strip()
        for k, v in data.items()
        if str(k).strip() and str(v).strip()
    }


def load_rows(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() == ".csv":
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))

    if path.suffix.lower() == ".json":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("JSON input must be a list of objects")
        return data

    raise ValueError("INPUT_FILE must be a .csv or .json file")


async def get_mm() -> MonarchMoney:
    mm = MonarchMoney()
    result = subprocess.run([sys.executable, str(LOGIN_SCRIPT)], check=False)
    if result.returncode != 0:
        raise RuntimeError(f"{LOGIN_SCRIPT} failed with exit code {result.returncode}")
    mm.load_session(str(SESSION_FILE))
    return mm


async def set_reviewed(mm: MonarchMoney, transaction_id: str, reviewed: bool = True):
    query = gql("""
    mutation Web_TransactionDrawerUpdateTransaction($input: UpdateTransactionMutationInput!) {
      updateTransaction(input: $input) {
        transaction {
          id
          reviewStatus
          needsReview
          __typename
        }
        errors {
          message
          __typename
        }
        __typename
      }
    }
    """)

    variables = {
        "input": {
            "id": transaction_id,
            "reviewed": reviewed,
        }
    }

    return await mm.gql_call(
        operation="Web_TransactionDrawerUpdateTransaction",
        graphql_query=query,
        variables=variables,
    )


async def update_transaction_with_reauth(mm: MonarchMoney, **kwargs) -> MonarchMoney:
    try:
        await mm.update_transaction(**kwargs)
        return mm
    except TransportServerError as e:
        if "401" not in str(e):
            raise

        print("Session expired. Re-running login.py and retrying update...")
        mm = await get_mm()
        await mm.update_transaction(**kwargs)
        return mm


async def set_reviewed_with_reauth(
    mm: MonarchMoney, transaction_id: str, reviewed: bool
) -> MonarchMoney:
    try:
        await set_reviewed(mm, transaction_id, reviewed=reviewed)
        return mm
    except TransportServerError as e:
        if "401" not in str(e):
            raise

        print("Session expired. Re-running login.py and retrying review update...")
        mm = await get_mm()
        await set_reviewed(mm, transaction_id, reviewed=reviewed)
        return mm


async def set_transaction_tags_with_reauth(
    mm: MonarchMoney, transaction_id: str, tag_ids: list[str]
) -> MonarchMoney:
    try:
        await mm.set_transaction_tags(transaction_id=transaction_id, tag_ids=tag_ids)
        return mm
    except TransportServerError as e:
        if "401" not in str(e):
            raise

        print("Session expired. Re-running login.py and retrying tag update...")
        mm = await get_mm()
        await mm.set_transaction_tags(transaction_id=transaction_id, tag_ids=tag_ids)
        return mm


def build_update_payload(
    row: dict,
    category_map: dict[str, str],
    tag_map: dict[str, str],
) -> tuple[dict, bool | None, list[str] | None]:
    transaction_id = clean_str(row.get("Transaction ID") or row.get("id"))
    if not transaction_id:
        raise ValueError("Missing Transaction ID")

    payload = {
        "transaction_id": transaction_id,
    }

    category_name = clean_str(row.get("Category"))
    if category_name:
        category_id = category_map.get(category_name)
        if not category_id:
            raise ValueError(f"Category not found in categories.json: {category_name!r}")
        payload["category_id"] = category_id

    merchant_name = clean_str(row.get("Merchant"))
    if merchant_name:
        payload["merchant_name"] = merchant_name

    amount = clean_str(row.get("Amount"))
    if amount:
        payload["amount"] = float(amount.replace(",", ""))

    date = normalize_date(row.get("Date"))
    if date:
        payload["date"] = date

    hide_from_reports = normalize_bool(row.get("Hide From Reports"))
    if hide_from_reports is not None:
        payload["hide_from_reports"] = hide_from_reports

    notes = clean_str(row.get("Notes"))
    if notes:
        payload["notes"] = notes

    needs_review = normalize_bool(row.get("Needs Review"))
    reviewed = None if needs_review is None else (not needs_review)

    tags_value_present = "Tags" in row
    tag_ids: list[str] | None = None

    if tags_value_present:
        tag_names = split_tag_names(row.get("Tags"))
        if tag_names:
            tag_ids = []
            for tag_name in tag_names:
                tag_id = tag_map.get(tag_name)
                if not tag_id:
                    raise ValueError(f"Tag not found in tags.json: {tag_name!r}")
                tag_ids.append(tag_id)

    return payload, reviewed, tag_ids


async def main():
    rows = load_rows(INPUT_FILE)
    category_map = load_name_id_map(CATEGORIES_FILE, "Categories")
    tag_map = load_name_id_map(TAGS_FILE, "Tags")

    print(f"Loaded {len(rows)} rows from {INPUT_FILE}")
    print(f"Loaded {len(category_map)} categories from {CATEGORIES_FILE}")
    print(f"Loaded {len(tag_map)} tags from {TAGS_FILE}")

    mm = await get_mm()

    updated = 0
    skipped = 0
    failed = 0

    for i, row in enumerate(rows, start=1):
        try:
            payload, reviewed, tag_ids = build_update_payload(row, category_map, tag_map)
        except Exception as e:
            skipped += 1
            print(f"[{i}] Skipping row: {e}")
            continue

        print(f"[{i}] Transaction ID: {payload['transaction_id']}")
        print(f"    Update payload: {payload}")
        if reviewed is not None:
            print(f"    Reviewed mutation: reviewed={reviewed}")
        if tag_ids is not None:
            print(f"    Tag IDs: {tag_ids}")

        if DRY_RUN:
            updated += 1
            continue

        try:
            mm = await update_transaction_with_reauth(mm, **payload)

            if reviewed is not None:
                mm = await set_reviewed_with_reauth(
                    mm,
                    transaction_id=payload["transaction_id"],
                    reviewed=reviewed,
                )

            if tag_ids is not None:
                mm = await set_transaction_tags_with_reauth(
                    mm,
                    transaction_id=payload["transaction_id"],
                    tag_ids=tag_ids,
                )

            print(f"[{i}] Updated transaction {payload['transaction_id']}")
            updated += 1

        except TransportQueryError as e:
            failed += 1
            print(f"[{i}] Update failed for {payload['transaction_id']}: {e}")
        except Exception as e:
            failed += 1
            print(f"[{i}] Unexpected failure for {payload['transaction_id']}: {e}")

    mode = "DRY RUN" if DRY_RUN else "PUSHED"
    print(f"\nDone. {mode}: {updated}, Skipped: {skipped}, Failed: {failed}")

    if DRY_RUN and updated > 0:
        print("Set DRY_RUN = False and run again to push for real.")


if __name__ == "__main__":
    asyncio.run(main())
