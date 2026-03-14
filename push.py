import asyncio
import argparse
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from gql import gql
from gql.transport.exceptions import TransportQueryError
from monarchmoney import MonarchMoney

# ----------------------------
# Config
# ----------------------------
SESSION_FILE = Path(".mm/mm_session.pickle")
DEFAULT_DATA_DIR = Path(os.environ.get("MONARCH_DATA_DIR", "data"))
DEFAULT_INPUT_FILE = Path(os.environ.get("MONARCH_PUSH_FILE", "push.csv"))
DEFAULT_DRY_RUN = os.environ.get("MONARCH_DRY_RUN", "true").strip().lower() in {
    "true",
    "1",
    "yes",
    "y",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory containing categories.json and tags.json.",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=DEFAULT_INPUT_FILE,
        help="CSV or JSON file containing transaction updates to push.",
    )
    parser.add_argument(
        "--dry-run",
        type=normalize_bool,
        default=DEFAULT_DRY_RUN,
        help="Whether to simulate updates without pushing them.",
    )
    return parser.parse_args()


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


def resolve_input_file(input_file: Path, data_dir: Path) -> Path:
    if input_file.is_absolute():
        return input_file

    if len(input_file.parts) == 1:
        data_dir_candidate = data_dir / input_file
        if data_dir_candidate.exists():
            return data_dir_candidate

    return input_file


async def get_mm() -> MonarchMoney:
    mm = MonarchMoney()

    if not SESSION_FILE.exists():
        raise RuntimeError(
            f"Session file not found: {SESSION_FILE}\n"
            f"Run: py .\\login.py"
        )

    mm.load_session(str(SESSION_FILE))
    await mm.get_accounts()  # validate session
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


async def update_transaction_safe(mm: MonarchMoney, **kwargs) -> MonarchMoney:
    await mm.update_transaction(**kwargs)
    return mm


async def set_reviewed_safe(
    mm: MonarchMoney, transaction_id: str, reviewed: bool
) -> MonarchMoney:
    await set_reviewed(mm, transaction_id, reviewed=reviewed)
    return mm


async def set_transaction_tags_safe(
    mm: MonarchMoney, transaction_id: str, tag_ids: list[str]
) -> MonarchMoney:
    await mm.set_transaction_tags(transaction_id=transaction_id, tag_ids=tag_ids)
    return mm


def build_update_payload(
    row: dict,
    category_map: dict[str, str],
    tag_map: dict[str, str],
    categories_file: Path,
    tags_file: Path,
) -> tuple[dict, bool | None, list[str] | None]:
    transaction_id = clean_str(row.get("Transaction ID") or row.get("id"))
    if not transaction_id:
        raise ValueError("Missing Transaction ID")

    payload = {
        "transaction_id": transaction_id,
    }

    category_name_raw = row.get("Category")
    if "Category" in row:
        category_name = clean_str(category_name_raw)
        if category_name:
            category_id = category_map.get(category_name)
            if not category_id:
                raise ValueError(f"Category not found in {categories_file}: {category_name!r}")
            payload["category_id"] = category_id
        else:
            # Best-effort clear. Whether Monarch accepts None here depends on the API.
            payload["category_id"] = None

    merchant_raw = row.get("Merchant")
    if "Merchant" in row:
        merchant_name = clean_str(merchant_raw)
        payload["merchant_name"] = merchant_name

    amount_raw = row.get("Amount")
    if "Amount" in row:
        amount = clean_str(amount_raw)
        payload["amount"] = None if amount is None else float(amount.replace(",", ""))

    date_raw = row.get("Date")
    if "Date" in row:
        payload["date"] = normalize_date(date_raw)

    hide_from_reports = normalize_bool(row.get("Hide From Reports"))
    if hide_from_reports is not None:
        payload["hide_from_reports"] = hide_from_reports

    # Notes are the one exception: blank means "leave unchanged"
    notes = clean_str(row.get("Notes"))
    if notes:
        payload["notes"] = notes

    needs_review = normalize_bool(row.get("Needs Review"))
    if needs_review is not None:
        payload["needs_review"] = needs_review
        reviewed = not needs_review
    else:
        reviewed = None

    # IMPORTANT:
    # If Tags column exists and is blank, send [] to clear tags in Monarch.
    tag_ids: list[str] | None = None
    if "Tags" in row:
        tag_ids = []
        tag_names = split_tag_names(row.get("Tags"))
        for tag_name in tag_names:
            tag_id = tag_map.get(tag_name)
            if not tag_id:
                raise ValueError(f"Tag not found in {tags_file}: {tag_name!r}")
            tag_ids.append(tag_id)

    return payload, reviewed, tag_ids


async def main():
    args = parse_args()
    data_dir = args.data_dir
    input_file = resolve_input_file(args.input_file, data_dir)
    dry_run = bool(args.dry_run)

    categories_file = data_dir / "categories.json"
    tags_file = data_dir / "tags.json"

    rows = load_rows(input_file)
    category_map = load_name_id_map(categories_file, "Categories")
    tag_map = load_name_id_map(tags_file, "Tags")

    print(f"Loaded {len(rows)} rows from {input_file}")
    print(f"Loaded {len(category_map)} categories from {categories_file}")
    print(f"Loaded {len(tag_map)} tags from {tags_file}")

    mm = await get_mm()

    updated = 0
    skipped = 0
    failed = 0

    for i, row in enumerate(rows, start=1):
        try:
            payload, reviewed, tag_ids = build_update_payload(
                row,
                category_map,
                tag_map,
                categories_file,
                tags_file,
            )
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

        if dry_run:
            updated += 1
            continue

        try:
            mm = await update_transaction_safe(mm, **payload)

            if reviewed is not None:
                mm = await set_reviewed_safe(
                    mm,
                    transaction_id=payload["transaction_id"],
                    reviewed=reviewed,
                )

            if tag_ids is not None:
                mm = await set_transaction_tags_safe(
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

    mode = "DRY RUN" if dry_run else "PUSHED"
    print(f"\nDone. {mode}: {updated}, Skipped: {skipped}, Failed: {failed}")

    if dry_run and updated > 0:
        print("Set DRY_RUN = False and run again to push for real.")


if __name__ == "__main__":
    asyncio.run(main())
