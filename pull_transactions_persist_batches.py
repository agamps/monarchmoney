import asyncio
import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

from monarchmoney import MonarchMoney

# ----------------------------
# Config
# ----------------------------
SESSION_FILE = Path(".mm/mm_session.pickle")
LOGIN_SCRIPT = Path("login.py")
DEFAULT_DATA_DIR = Path(os.environ.get("MONARCH_DATA_DIR", "data"))
BATCH_SIZE = 100  # configurable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory where transaction export files will be written.",
    )
    return parser.parse_args()


async def get_mm() -> MonarchMoney:
    mm = MonarchMoney()

    if not SESSION_FILE.exists():
        print(f"Session file not found: {SESSION_FILE}")
        print(f"Running {LOGIN_SCRIPT}...")
        result = subprocess.run([sys.executable, str(LOGIN_SCRIPT)], check=False)
        if result.returncode != 0:
            raise RuntimeError(f"{LOGIN_SCRIPT} failed with exit code {result.returncode}")

        if not SESSION_FILE.exists():
            raise FileNotFoundError(
                f"{LOGIN_SCRIPT} ran, but session file still does not exist: {SESSION_FILE}"
            )

    mm.load_session(str(SESSION_FILE))
    return mm


def flatten_transaction(txn: dict) -> dict:
    account = txn.get("account") or {}
    merchant = txn.get("merchant") or {}
    category = txn.get("category") or {}
    tags = txn.get("tags") or []

    return {
        "Transaction ID": txn.get("id"),
        "Account": account.get("displayName"),
        "Date": txn.get("date"),
        "Merchant": merchant.get("name") or txn.get("merchantName"),
        "Plaid Name": txn.get("plaidName") or txn.get("originalName"),
        "Amount": txn.get("amount"),
        "Category": category.get("name"),
        "Tags": ",".join(str(t.get("name")) for t in tags if t.get("name")),
        "Notes": txn.get("notes"),
        "Hide From Reports": txn.get("hideFromReports"),
        "Needs Review": txn.get("needsReview"),
    }


def write_json(path: Path, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, default=str)


def write_csv(path: Path, rows: list[dict]) -> None:
    flat_rows = [flatten_transaction(r) for r in rows]

    headers = [
        "Transaction ID",
        "Account",
        "Date",
        "Merchant",
        "Plaid Name",
        "Amount",
        "Category",
        "Tags",
        "Notes",
        "Hide From Reports",
        "Needs Review",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        if flat_rows:
            writer.writerows(flat_rows)


def persist_outputs(
    all_json: Path,
    all_csv: Path,
    unreviewed_json: Path,
    unreviewed_csv: Path,
    all_transactions: list[dict],
    unreviewed_transactions: list[dict],
) -> None:
    write_json(all_json, all_transactions)
    write_csv(all_csv, all_transactions)
    write_json(unreviewed_json, unreviewed_transactions)
    write_csv(unreviewed_csv, unreviewed_transactions)


async def main():
    args = parse_args()
    data_dir = args.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    all_json = data_dir / "all_transactions.json"
    all_csv = data_dir / "all_transactions.csv"
    unreviewed_json = data_dir / "unreviewed_transactions.json"
    unreviewed_csv = data_dir / "unreviewed_transactions.csv"

    mm = await get_mm()

    all_transactions: list[dict] = []
    unreviewed_transactions: list[dict] = []

    offset = 0
    batch_num = 0

    while True:
        batch_num += 1
        print(f"Fetching batch {batch_num} with offset={offset}, limit={BATCH_SIZE}...")

        data = await mm.get_transactions(limit=BATCH_SIZE, offset=offset)
        transactions = data.get("allTransactions", {}).get("results", [])

        if not transactions:
            print("No more transactions returned.")
            break

        all_transactions.extend(transactions)

        batch_unreviewed = [t for t in transactions if t.get("needsReview")]
        unreviewed_transactions.extend(batch_unreviewed)

        persist_outputs(
            all_json,
            all_csv,
            unreviewed_json,
            unreviewed_csv,
            all_transactions,
            unreviewed_transactions,
        )

        print(
            f"Saved after batch {batch_num}: "
            f"{len(all_transactions)} total, "
            f"{len(unreviewed_transactions)} unreviewed"
        )

        if len(transactions) < BATCH_SIZE:
            print("Last partial batch received. Finished.")
            break

        offset += BATCH_SIZE

    print("Done.")
    print(f"All transactions JSON: {all_json.resolve()}")
    print(f"All transactions CSV:  {all_csv.resolve()}")
    print(f"Unreviewed JSON:       {unreviewed_json.resolve()}")
    print(f"Unreviewed CSV:        {unreviewed_csv.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
