"""
push_changes.py
---------------
Read a reviewed transaction CSV and push changes back to Monarch Money.
Marks transactions as reviewed and applies edits to merchant_name,
category_id, notes, and hide_from_reports.

Self-contained — no monarch_utils dependency. Session handling mirrors push.py.

Usage:
    python push_changes.py                              # dry run (safe default)
    python push_changes.py --dry-run false              # LIVE push
    python push_changes.py --dry-run false --update-local true

Arguments:
    --input             Source CSV path                       (default: push.csv)
    --data-dir          Directory used to resolve bare CSV names (default: data)
    --dry-run           true/false — simulate without pushing  (default: true)
    --update-local      true/false — after a live push, patch all_transactions.csv
                        and the input unreviewed CSV in place  (default: false)
    --all-transactions  Path to all_transactions CSV to patch  (default: all_transactions.csv)
    --unreviewed-file   Path to unreviewed CSV to patch        (default: same as --input)

IMPORTANT:
  • The CSV must have an 'id' or 'Transaction ID' column.
  • Set needs_review / Needs Review to False for rows you have reviewed.
  • category_id must be a valid Monarch category ID (use pull_categories_tags.py).
"""

import argparse
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pandas as pd
from gql import gql
from monarchmoney import MonarchMoney

# ── Config (mirrors push.py) ────────────────────────────────────────────────

SESSION_FILE = Path(".mm/mm_session.pickle")
DEFAULT_DATA_DIR = Path(os.environ.get("MONARCH_DATA_DIR", "data"))
DEFAULT_INPUT_FILE = Path(os.environ.get("MONARCH_PUSH_FILE", "push.csv"))
DEFAULT_ALL_TRANSACTIONS_FILE = Path(
    os.environ.get("MONARCH_ALL_TRANSACTIONS_FILE", "all_transactions.csv")
)

ID_COLS = ("id", "Transaction ID", "transaction_id")
NEEDS_REVIEW_COLS = ("needs_review", "Needs Review")
MERCHANT_COLS = ("merchant_name", "Merchant")
CATEGORY_ID_COLS = ("category_id", "Category ID")
NOTES_COLS = ("notes", "Notes")
HIDE_FROM_REPORTS_COLS = ("hide_from_reports", "Hide From Reports")
UPDATABLE_COL_GROUPS = (
    MERCHANT_COLS,
    CATEGORY_ID_COLS,
    NOTES_COLS,
    HIDE_FROM_REPORTS_COLS,
    NEEDS_REVIEW_COLS,
)

# ── GraphQL mutation — same as the Monarch UI "mark reviewed" action ────────

REVIEW_MUTATION = gql("""
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


# ── Auth ────────────────────────────────────────────────────────────────────

async def get_client() -> MonarchMoney:
    """Load saved session or prompt for interactive login."""
    mm = MonarchMoney()
    if SESSION_FILE.exists():
        try:
            mm.load_session(str(SESSION_FILE))
            print(f"  Session loaded from {SESSION_FILE}")
            return mm
        except Exception:
            print("  Warning: session file found but failed to load; prompting login")

    print("  No valid session found. Logging in interactively...")
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    await mm.interactive_login()
    mm.save_session(str(SESSION_FILE))
    print(f"  Session saved to {SESSION_FILE}")
    return mm


# ── Helpers ─────────────────────────────────────────────────────────────────

def coerce_bool(val) -> bool | None:
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        v = val.strip().lower()
        if v in ("true", "1", "yes"):
            return True
        if v in ("false", "0", "no"):
            return False
    return None


def resolve_data_file(path: Path, data_dir: Path) -> Path:
    if path.is_absolute():
        return path

    if len(path.parts) == 1:
        data_dir_candidate = data_dir / path
        if data_dir_candidate.exists():
            return data_dir_candidate

    return path


def find_col(columns, candidates: tuple[str, ...]) -> str | None:
    normalized = {str(col).strip().casefold(): str(col) for col in columns}
    for candidate in candidates:
        match = normalized.get(candidate.casefold())
        if match is not None:
            return match
    return None


def row_value(row: pd.Series, candidates: tuple[str, ...]):
    column = find_col(row.index, candidates)
    if column is None:
        return None
    value = row.get(column)
    if pd.isna(value):
        return None
    return value


def clean_value(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def load_csv(path: Path) -> pd.DataFrame:
    """Load a CSV trying common encodings."""
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {path} with any supported encoding")


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame to CSV atomically via a temp file."""
    tmp = Path(tempfile.mktemp(dir=path.parent, suffix=".tmp"))
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        shutil.move(str(tmp), str(path))
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


# ── Local file patching ─────────────────────────────────────────────────────

def patch_df(target_df: pd.DataFrame, pushed_rows: pd.DataFrame) -> pd.DataFrame:
    """Apply pushed row values onto target_df, matched by id."""
    target_id_col = find_col(target_df.columns, ID_COLS)
    pushed_id_col = find_col(pushed_rows.columns, ID_COLS)
    if target_id_col is None or pushed_id_col is None:
        return target_df

    for _, row in pushed_rows.iterrows():
        mask = target_df[target_id_col].astype(str) == str(row[pushed_id_col])
        if not mask.any():
            continue
        for candidates in UPDATABLE_COL_GROUPS:
            source_col = find_col(row.index, candidates)
            target_col = find_col(target_df.columns, candidates)
            if source_col is not None and target_col is not None:
                val = row[source_col]
                if not (isinstance(val, float) and pd.isna(val)):
                    target_df.loc[mask, target_col] = str(val)
    return target_df


def update_local_files(
    pushed_rows: pd.DataFrame,
    all_transactions_path: Path,
    unreviewed_path: Path,
) -> None:
    # ── Patch all_transactions.csv ──────────────────────────────────────────
    if all_transactions_path.exists():
        df = load_csv(all_transactions_path)
        pushed_id_col = find_col(pushed_rows.columns, ID_COLS)
        target_id_col = find_col(df.columns, ID_COLS)
        if pushed_id_col is not None and target_id_col is not None:
            n = (
                pushed_rows[pushed_id_col]
                .astype(str)
                .isin(df[target_id_col].astype(str))
                .sum()
            )
        else:
            n = 0
        df = patch_df(df, pushed_rows)
        atomic_write_csv(df, all_transactions_path)
        print(f"  {all_transactions_path.name}: {n} row(s) updated")
    else:
        print(f"  Warning: {all_transactions_path} not found; skipped")

    # ── Patch unreviewed_transactions.csv ───────────────────────────────────
    if unreviewed_path.exists():
        df = load_csv(unreviewed_path)
        df = patch_df(df, pushed_rows)
        needs_review_col = find_col(df.columns, NEEDS_REVIEW_COLS)
        if needs_review_col is None:
            print(f"  Warning: {unreviewed_path} has no needs_review column; skipped pruning")
            atomic_write_csv(df, unreviewed_path)
            return

        def still_unreviewed(val: str) -> bool:
            return str(val).strip().lower() not in ("false", "0", "no")

        before = len(df)
        df = df[df[needs_review_col].apply(still_unreviewed)]
        dropped = before - len(df)

        atomic_write_csv(df, unreviewed_path)
        print(
            f"  {unreviewed_path.name}: "
            f"{dropped} row(s) removed (reviewed), {len(df)} remaining"
        )
    else:
        print(f"  Warning: {unreviewed_path} not found; skipped")


# ── Argument parsing ────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Push reviewed transaction changes back to Monarch Money."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory used to resolve bare CSV paths (default: data)",
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_FILE,
        type=Path,
        help="Path to the reviewed transactions CSV (default: push.csv)",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        default="true",
        choices=["true", "false", "True", "False", "1", "0"],
        help="true = simulate only, false = push to Monarch (default: true)",
    )
    parser.add_argument(
        "--update-local",
        dest="update_local",
        default="false",
        choices=["true", "false", "True", "False", "1", "0"],
        help="After a live push, patch local CSV files in place (default: false)",
    )
    parser.add_argument(
        "--all-transactions",
        dest="all_transactions_file",
        default=DEFAULT_ALL_TRANSACTIONS_FILE,
        type=Path,
        help="Path to all_transactions CSV to patch (default: all_transactions.csv)",
    )
    parser.add_argument(
        "--unreviewed-file",
        dest="unreviewed_file",
        default=None,
        type=Path,
        help="Path to unreviewed CSV to patch (default: same as --input)",
    )
    return parser.parse_args()


# ── Main ────────────────────────────────────────────────────────────────────

async def main() -> None:
    args = parse_args()
    dry_run = coerce_bool(args.dry_run)
    update_local = coerce_bool(args.update_local)

    data_dir: Path = args.data_dir
    input_path: Path = resolve_data_file(args.input, data_dir)
    all_transactions_path: Path = resolve_data_file(args.all_transactions_file, data_dir)
    unreviewed_path: Path = (
        resolve_data_file(args.unreviewed_file, data_dir)
        if args.unreviewed_file
        else input_path
    )

    print(f"{'DRY RUN' if dry_run else 'LIVE'} - push_changes.py")
    print(f"  Input : {input_path}")
    if not dry_run and update_local:
        print(f"  all_transactions : {all_transactions_path}")
        print(f"  unreviewed file  : {unreviewed_path}")
    print()

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        sys.exit(1)

    df = load_csv(input_path)

    id_col = find_col(df.columns, ID_COLS)
    needs_review_col = find_col(df.columns, NEEDS_REVIEW_COLS)

    if id_col is None:
        print("CSV must have an 'id' or 'Transaction ID' column.")
        sys.exit(1)
    if needs_review_col is None:
        print("CSV must have a 'needs_review' or 'Needs Review' column.")
        sys.exit(1)

    # Only push rows explicitly marked as reviewed
    to_push = df[df[needs_review_col].apply(
        lambda v: coerce_bool(str(v)) is False
    )].copy()

    if to_push.empty:
        print("Nothing to push: no rows with needs_review=False found.")
        return

    print(f"{len(to_push)} transaction(s) to push\n")

    mm = None
    if not dry_run:
        mm = await get_client()
        print()

    success_count = 0
    error_count = 0
    errors = []
    pushed_ids = []

    for _, row in to_push.iterrows():
        txn_id = str(row[id_col]).strip()

        # Collect field updates
        kwargs = {}

        v = clean_value(row_value(row, MERCHANT_COLS))
        if v:
            kwargs["merchant_name"] = v

        v = clean_value(row_value(row, CATEGORY_ID_COLS))
        if v:
            kwargs["category_id"] = v

        notes = row_value(row, NOTES_COLS)
        if notes is not None:
            kwargs["notes"] = str(notes)

        hide_from_reports = row_value(row, HIDE_FROM_REPORTS_COLS)
        hfr = coerce_bool(str(hide_from_reports)) if hide_from_reports is not None else None
        if hfr is not None:
            kwargs["hide_from_reports"] = hfr

        if dry_run:
            print(f"  [DRY RUN] {txn_id}")
            if kwargs:
                print(f"            Fields  : {kwargs}")
            print("            Reviewed: -> True")
            success_count += 1
            continue

        try:
            if kwargs:
                await mm.update_transaction(transaction_id=txn_id, **kwargs)

            # Use the proper GraphQL mutation to set reviewStatus = "reviewed"
            await mm.gql_call(
                operation="Web_TransactionDrawerUpdateTransaction",
                graphql_query=REVIEW_MUTATION,
                variables={"input": {"id": txn_id, "reviewed": True}},
            )

            print(f"  OK {txn_id}")
            success_count += 1
            pushed_ids.append(txn_id)

        except Exception as e:
            print(f"  ERROR {txn_id}: {e}")
            errors.append({"id": txn_id, "error": str(e)})
            error_count += 1

    # ── Summary ─────────────────────────────────────────────────────────────
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Results:")
    print(f"  Success : {success_count}")
    if error_count:
        print(f"  Errors  : {error_count}")
        for e in errors:
            print(f"     - {e['id']}: {e['error']}")

    if dry_run:
        print("\nRun with --dry-run false to push these changes to Monarch.")
        return

    print("\nChanges pushed to Monarch Money.")

    # ── Update local files ───────────────────────────────────────────────────
    if update_local and pushed_ids:
        print("\nUpdating local CSV files...")
        pushed_df = to_push[
            to_push[id_col].astype(str).isin(set(str(i) for i in pushed_ids))
        ].copy()
        update_local_files(pushed_df, all_transactions_path, unreviewed_path)
        print("  Local files updated.")
    elif update_local and not pushed_ids:
        print("\nNo successful pushes; local files not updated.")


if __name__ == "__main__":
    asyncio.run(main())
