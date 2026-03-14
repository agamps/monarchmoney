import asyncio
import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

from gql import gql
from gql.transport.exceptions import TransportServerError
from monarchmoney import MonarchMoney

# ----------------------------
# Config
# ----------------------------
SESSION_FILE = Path(".mm/mm_session.pickle")
LOGIN_SCRIPT = Path("login.py")
DEFAULT_DATA_DIR = Path(os.environ.get("MONARCH_DATA_DIR", "data"))
DEFAULT_OUTPUT_BASENAME = "unreviewed_only_transactions"
BATCH_SIZE = 400  # configurable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory where unreviewed transaction files will be written.",
    )
    parser.add_argument(
        "--filename",
        default=DEFAULT_OUTPUT_BASENAME,
        help="Base filename for the exported unreviewed transaction files, without extension.",
    )
    return parser.parse_args()


async def get_mm() -> MonarchMoney:
    mm = MonarchMoney()
    result = subprocess.run([sys.executable, str(LOGIN_SCRIPT)], check=False)
    if result.returncode != 0:
        raise RuntimeError(f"{LOGIN_SCRIPT} failed with exit code {result.returncode}")
    mm.load_session(str(SESSION_FILE))
    return mm


async def fetch_unreviewed_with_reauth(
    mm: MonarchMoney, limit: int, offset: int
) -> tuple[MonarchMoney, list[dict]]:
    try:
        transactions = await get_unreviewed_batch(mm, limit, offset)
        return mm, transactions
    except TransportServerError as e:
        if "401" not in str(e):
            raise

        print("Session expired. Re-running login.py and retrying batch...")
        mm = await get_mm()
        transactions = await get_unreviewed_batch(mm, limit, offset)
        return mm, transactions


async def get_unreviewed_batch(mm: MonarchMoney, limit: int, offset: int) -> list[dict]:
    query = gql("""
    query GetUnreviewedTransactions($offset: Int, $limit: Int, $filters: TransactionFilterInput, $orderBy: TransactionOrdering) {
      allTransactions(filters: $filters) {
        totalCount
        results(offset: $offset, limit: $limit, orderBy: $orderBy) {
          id
          amount
          date
          plaidName
          notes
          hideFromReports
          needsReview
          category {
            id
            name
          }
          merchant {
            id
            name
          }
          account {
            id
            displayName
          }
          tags {
            id
            name
          }
        }
      }
    }
    """)

    variables = {
        "offset": offset,
        "limit": limit,
        "orderBy": "date",
        "filters": {
            "needsReview": True,
        },
    }

    result = await mm.gql_call(
        operation="GetUnreviewedTransactions",
        graphql_query=query,
        variables=variables,
    )
    return result.get("allTransactions", {}).get("results", [])


def flatten_transaction(txn: dict) -> dict:
    account = txn.get("account") or {}
    merchant = txn.get("merchant") or {}
    category = txn.get("category") or {}
    tags = txn.get("tags") or []

    return {
        "Transaction ID": txn.get("id"),
        "Account": account.get("displayName"),
        "Date": txn.get("date"),
        "Merchant": merchant.get("name"),
        "Plaid Name": txn.get("plaidName"),
        "Amount": txn.get("amount"),
        "Category": category.get("name"),
        "Tags": ",".join(str(t.get("name")) for t in tags if t.get("name")),
        "Notes": txn.get("notes"),
        "Hide From Reports": txn.get("hideFromReports"),
        "Needs Review": txn.get("needsReview"),
    }


def write_json(path: Path, rows: list[dict]) -> None:
    flattened = [flatten_transaction(row) for row in rows]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(flattened, f, indent=2, default=str)


def write_csv(path: Path, rows: list[dict]) -> None:
    flat_rows = [flatten_transaction(row) for row in rows]

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


async def main():
    args = parse_args()
    data_dir = args.data_dir
    output_basename = Path(args.filename).stem
    data_dir.mkdir(parents=True, exist_ok=True)

    unreviewed_json = data_dir / f"{output_basename}.json"
    unreviewed_csv = data_dir / f"{output_basename}.csv"

    mm = await get_mm()

    unreviewed_transactions: list[dict] = []
    offset = 0
    batch_num = 0

    while True:
        batch_num += 1
        print(f"Fetching unreviewed batch {batch_num} with offset={offset}, limit={BATCH_SIZE} ...")

        mm, transactions = await fetch_unreviewed_with_reauth(mm, BATCH_SIZE, offset)

        if not transactions:
            print("No more unreviewed transactions returned.")
            break

        unreviewed_transactions.extend(transactions)

        write_json(unreviewed_json, unreviewed_transactions)
        write_csv(unreviewed_csv, unreviewed_transactions)

        print(f"Saved after batch {batch_num}: {len(unreviewed_transactions)} unreviewed")

        if len(transactions) < BATCH_SIZE:
            print("Last partial batch received. Finished.")
            break

        offset += BATCH_SIZE

    print("Done.")
    print(f"Unreviewed JSON: {unreviewed_json.resolve()}")
    print(f"Unreviewed CSV:  {unreviewed_csv.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
