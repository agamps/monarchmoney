import asyncio
import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from gql import gql
from gql.transport.exceptions import TransportServerError
from gql.transport.exceptions import TransportQueryError
from monarchmoney import MonarchMoney

from monarch_api import configure_monarch_api
from monarch_auth import get_monarch_client

# ----------------------------
# Config
# ----------------------------
DEFAULT_DATA_DIR = Path(os.environ.get("MONARCH_DATA_DIR", "data"))
DEFAULT_INPUT_FILE = Path(os.environ.get("MONARCH_PUSH_FILE", "push.csv"))
DEFAULT_DRY_RUN = os.environ.get("MONARCH_DRY_RUN", "true").strip().lower() in {
    "true",
    "1",
    "yes",
    "y",
}
CSV_ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")

configure_monarch_api()


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
    parser.add_argument(
        "--update-local",
        type=normalize_bool,
        default=False,
        help=(
            "After a live push, patch local CSV files to reflect changes. "
            "Updates all_transactions.csv and removes reviewed rows from the "
            "unreviewed transactions file. Ignored in dry-run mode."
        ),
    )
    parser.add_argument(
        "--local-only",
        type=normalize_bool,
        default=False,
        help=(
            "Skip Monarch API calls and only patch local CSV files from --input-file. "
            "Use after a live push succeeds but local CSV files were locked."
        ),
    )
    parser.add_argument(
        "--all-transactions",
        type=Path,
        default=None,
        help=(
            "Path to all_transactions.csv to patch when --update-local is set. "
            "Defaults to <data-dir>/all_transactions.csv."
        ),
    )
    parser.add_argument(
        "--unreviewed-file",
        type=Path,
        default=None,
        help=(
            "Path to the unreviewed transactions CSV to patch when --update-local is set. "
            "Defaults to <data-dir>/unreviewed_transactions.csv."
        ),
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


def parse_amount(value: Any) -> float | None:
    text = clean_str(value)
    if text is None:
        return None

    is_parenthesized = text.startswith("(") and text.endswith(")")
    if is_parenthesized:
        text = text[1:-1].strip()

    text = text.replace(",", "").replace("$", "")
    amount = float(text)
    return -abs(amount) if is_parenthesized else amount


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
        last_error: UnicodeDecodeError | None = None
        for encoding in CSV_ENCODINGS:
            try:
                with open(path, "r", encoding=encoding, newline="") as f:
                    return list(csv.DictReader(f))
            except UnicodeDecodeError as e:
                last_error = e

        assert last_error is not None
        raise ValueError(
            f"Could not decode CSV file {path} using supported encodings: {', '.join(CSV_ENCODINGS)}"
        ) from last_error

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


# Authentication handled by `monarch_auth.get_monarch_client()`


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
            payload["category_id"] = None

    merchant_raw = row.get("Merchant")
    if "Merchant" in row:
        merchant_name = clean_str(merchant_raw)
        payload["merchant_name"] = merchant_name

    amount_raw = row.get("Amount")
    if "Amount" in row:
        payload["amount"] = parse_amount(amount_raw)

    date_raw = row.get("Date")
    if "Date" in row:
        payload["date"] = normalize_date(date_raw)

    hide_from_reports = normalize_bool(row.get("Hide From Reports"))
    if hide_from_reports is not None:
        payload["hide_from_reports"] = hide_from_reports

    notes = clean_str(row.get("Notes"))
    if notes:
        payload["notes"] = notes

    needs_review = normalize_bool(row.get("Needs Review"))
    if needs_review is not None:
        payload["needs_review"] = needs_review
        reviewed = not needs_review
    else:
        reviewed = None

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


# ── Local file patching ──────────────────────────────────────────────────────

UPDATABLE_COLS = [
    "Date",
    "Merchant",
    "Amount",
    "Category",
    "Notes",
    "Hide From Reports",
    "Needs Review",
    "Tags",
]


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame to CSV atomically via a temp file."""
    tmp = Path(tempfile.mktemp(dir=path.parent, suffix=".tmp"))
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        shutil.move(str(tmp), str(path))
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def load_csv_df(path: Path) -> pd.DataFrame:
    for enc in CSV_ENCODINGS:
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {path}")


def is_locked_file_error(error: OSError) -> bool:
    return isinstance(error, PermissionError) or getattr(error, "winerror", None) in {
        32,
        33,
    }


def load_csv_df_or_warn(path: Path) -> pd.DataFrame | None:
    try:
        return load_csv_df(path)
    except OSError as e:
        if not is_locked_file_error(e):
            raise
        print(f"  WARNING: Could not read {path}.")
        print("           It may be open in Excel. Close it and run local update again.")
        return None


def atomic_write_csv_or_warn(df: pd.DataFrame, path: Path) -> bool:
    try:
        atomic_write_csv(df, path)
        return True
    except OSError as e:
        if not is_locked_file_error(e):
            raise
        print(f"  WARNING: Could not update {path}.")
        print("           It may be open in Excel. Close it and run local update again.")
        return False


def local_recovery_command(
    data_dir: Path,
    input_file: Path,
    all_transactions_path: Path,
    unreviewed_path: Path,
) -> str:
    command = [
        sys.executable,
        str(Path(__file__)),
        "--data-dir",
        str(data_dir),
        "--input-file",
        str(input_file),
        "--local-only",
        "true",
        "--all-transactions",
        str(all_transactions_path),
        "--unreviewed-file",
        str(unreviewed_path),
    ]
    return subprocess.list2cmdline(command)


def update_local_files(
    pushed_rows: list[dict],
    _category_map: dict[str, str],
    all_transactions_path: Path,
    unreviewed_path: Path,
) -> bool:
    # Build a lookup of all successfully pushed rows by transaction ID.
    pushed_by_id = {
        str(clean_str(r.get("Transaction ID") or r.get("id"))): r
        for r in pushed_rows
    }
    completed = True

    def apply_row_updates(df: pd.DataFrame) -> pd.DataFrame:
        id_col = next((c for c in df.columns if c.strip().lower() in ("transaction id", "id")), None)
        if id_col is None:
            return df
        for idx, df_row in df.iterrows():
            txn_id = str(df_row[id_col]).strip()
            pushed = pushed_by_id.get(txn_id)
            if pushed is None:
                continue
            for col in UPDATABLE_COLS:
                if col in pushed and col in df.columns:
                    df.at[idx, col] = pushed[col] if pushed[col] is not None else ""
        return df

    all_df_for_unreviewed = None

    # Patch all_transactions.csv.
    if all_transactions_path.exists():
        df = load_csv_df_or_warn(all_transactions_path)
        if df is None:
            completed = False
        else:
            all_df_for_unreviewed = apply_row_updates(df)
            if atomic_write_csv_or_warn(all_df_for_unreviewed, all_transactions_path):
                print(f"  Updated {all_transactions_path.name}: {len(pushed_by_id)} row(s)")
            else:
                completed = False
    else:
        print(f"  WARNING: {all_transactions_path} not found; skipped.")
        completed = False

    # Patch unreviewed_transactions.csv.
    if unreviewed_path.exists():
        df = load_csv_df_or_warn(unreviewed_path)
        if df is None:
            return False

        df = apply_row_updates(df)

        # Find the Needs Review column (handle either naming convention)
        nr_col = next(
            (c for c in df.columns if c.strip().lower() in ("needs review", "needs_review")),
            None,
        )

        def is_unreviewed(val: str) -> bool:
            return str(val).strip().lower() not in ("false", "0", "no")

        before = len(df)
        if nr_col:
            df = df[df[nr_col].apply(is_unreviewed)]
        dropped = before - len(df)

        # Add back any rows flipped back to Needs Review = True.
        # Source the full row from all_transactions.csv so we have all columns.
        added = 0
        if all_df_for_unreviewed is not None and nr_col:
            unrev_id_col = next(
                (c for c in df.columns if c.strip().lower() in ("transaction id", "id")),
                None,
            )
            ids_already_in_unreviewed = (
                set(df[unrev_id_col].astype(str).str.strip())
                if unrev_id_col else set()
            )
            all_df = all_df_for_unreviewed
            all_id_col = next(
                (c for c in all_df.columns if c.strip().lower() in ("transaction id", "id")),
                None,
            )
            for txn_id, pushed in pushed_by_id.items():
                nr_val = pushed.get("Needs Review") or pushed.get("needs_review") or ""
                if not is_unreviewed(nr_val):
                    continue  # marked reviewed — handled by drop above
                if txn_id in ids_already_in_unreviewed:
                    continue  # already in the unreviewed file
                if all_id_col is None:
                    continue
                match = all_df[all_df[all_id_col].astype(str).str.strip() == txn_id]
                if match.empty:
                    continue
                new_row = match.iloc[[0]].reindex(columns=df.columns, fill_value="")
                df = pd.concat([df, new_row], ignore_index=True)
                added += 1

        if atomic_write_csv_or_warn(df, unreviewed_path):
            print(
                f"  Updated {unreviewed_path.name}: "
                f"{dropped} row(s) removed (reviewed), "
                f"{added} row(s) added back (unreviewed), "
                f"{len(df)} remaining"
            )
        else:
            completed = False
    else:
        print(f"  WARNING: {unreviewed_path} not found; skipped.")
        completed = False

    return completed


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    args = parse_args()
    data_dir = args.data_dir
    input_file = resolve_input_file(args.input_file, data_dir)
    dry_run = bool(args.dry_run)
    update_local = bool(args.update_local)
    local_only = bool(args.local_only)

    # Resolve local file paths for --update-local
    all_transactions_path = args.all_transactions or (data_dir / "all_transactions.csv")
    unreviewed_path = args.unreviewed_file or (data_dir / "unreviewed_transactions.csv")

    categories_file = data_dir / "categories.json"
    tags_file = data_dir / "tags.json"

    rows = load_rows(input_file)
    category_map = load_name_id_map(categories_file, "Categories")
    tag_map = load_name_id_map(tags_file, "Tags")

    print(f"Loaded {len(rows)} rows from {input_file}")
    print(f"Loaded {len(category_map)} categories from {categories_file}")
    print(f"Loaded {len(tag_map)} tags from {tags_file}")
    if local_only:
        print("Local-only mode: skipping Monarch API calls.")
        print(f"Will update : {all_transactions_path}")
        print(f"             {unreviewed_path}")
        print("\nUpdating local CSV files...")
        local_update_completed = update_local_files(
            rows,
            category_map,
            all_transactions_path,
            unreviewed_path,
        )
        if local_update_completed:
            print("  Local files updated.")
        else:
            print("  Local file update incomplete.")
            print("  Close any open CSVs and run the same command again.")
        return

    if not dry_run and update_local:
        print(f"Will update : {all_transactions_path}")
        print(f"             {unreviewed_path}")

    mm = None
    if not dry_run:
        mm = await get_monarch_client()

    updated = 0
    skipped = 0
    failed = 0
    successfully_pushed_rows: list[dict] = []

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
            assert mm is not None
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
            successfully_pushed_rows.append(row)

        except TransportQueryError as e:
            failed += 1
            print(f"[{i}] Update failed for {payload['transaction_id']}: {e}")
        except Exception as e:
            failed += 1
            print(f"[{i}] Unexpected failure for {payload['transaction_id']}: {e}")

    mode = "DRY RUN" if dry_run else "PUSHED"
    print(f"\nDone. {mode}: {updated}, Skipped: {skipped}, Failed: {failed}")

    if dry_run and updated > 0:
        print("Set --dry-run false and run again to push for real.")
        return

    # ── Update local files ───────────────────────────────────────────────────
    if update_local and successfully_pushed_rows:
        print("\nUpdating local CSV files...")
        local_update_completed = update_local_files(
            successfully_pushed_rows,
            category_map,
            all_transactions_path,
            unreviewed_path,
        )
        if local_update_completed:
            print("  Local files updated.")
        else:
            print("  Local file update incomplete.")
            print("  Close any open CSVs and rerun this local-only recovery command:")
            print(
                "  "
                + local_recovery_command(
                    data_dir,
                    input_file,
                    all_transactions_path,
                    unreviewed_path,
                )
            )
    elif update_local and not successfully_pushed_rows:
        print("\nNo successful pushes; local files not updated.")


if __name__ == "__main__":
    asyncio.run(main())
