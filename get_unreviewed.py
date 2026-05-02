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
DEFAULT_OUTPUT_BASENAME = "unreviewed_transactions"
BATCH_SIZE = 400  # configurable
CSV_HEADERS = [
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
    parser.add_argument(
        "--all-transactions",
        type=Path,
        default=None,
        help=(
            "Path to all_transactions.csv to upsert with fetched unreviewed rows. "
            "Defaults to <data-dir>/all_transactions.csv."
        ),
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
        "Merchant": merchant.get("name") or txn.get("merchantName"),
        "Plaid Name": txn.get("plaidName") or txn.get("originalName"),
        "Amount": txn.get("amount"),
        "Category": category.get("name"),
        "Tags": ",".join(str(t.get("name")) for t in tags if t.get("name")),
        "Notes": txn.get("notes"),
        "Hide From Reports": txn.get("hideFromReports"),
        "Needs Review": txn.get("needsReview"),
    }


def flatten_transactions(rows: list[dict]) -> list[dict]:
    return [flatten_transaction(row) for row in rows]


def write_json(path: Path, rows: list[dict]) -> None:
    flattened = flatten_transactions(rows)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(flattened, f, indent=2, default=str)


def write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_csv(path: Path, rows: list[dict]) -> None:
    write_csv_rows(path, CSV_HEADERS, flatten_transactions(rows))


def read_csv_rows(path: Path) -> tuple[list[str], list[dict]]:
    if not path.exists():
        return list(CSV_HEADERS), []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return list(CSV_HEADERS), []
        return list(reader.fieldnames), list(reader)


def upsert_all_transactions_csv(path: Path, transactions: list[dict]) -> tuple[int, int]:
    incoming_rows = flatten_transactions(transactions)
    fieldnames, existing_rows = read_csv_rows(path)

    for header in CSV_HEADERS:
        if header not in fieldnames:
            fieldnames.append(header)

    rows_by_id = {
        str(row.get("Transaction ID") or row.get("id") or "").strip(): row
        for row in existing_rows
        if str(row.get("Transaction ID") or row.get("id") or "").strip()
    }

    updated = 0
    appended = 0
    for incoming in incoming_rows:
        transaction_id = str(incoming.get("Transaction ID") or "").strip()
        if not transaction_id:
            continue

        existing = rows_by_id.get(transaction_id)
        if existing is None:
            new_row = {field: "" for field in fieldnames}
            for field in CSV_HEADERS:
                new_row[field] = incoming.get(field, "")
            existing_rows.append(new_row)
            rows_by_id[transaction_id] = new_row
            appended += 1
            continue

        for field in CSV_HEADERS:
            existing[field] = incoming.get(field, "")
        updated += 1

    write_csv_rows(path, fieldnames, existing_rows)
    return updated, appended


async def main():
    args = parse_args()
    data_dir = args.data_dir
    output_basename = Path(args.filename).stem
    data_dir.mkdir(parents=True, exist_ok=True)

    unreviewed_json = data_dir / f"{output_basename}.json"
    unreviewed_csv = data_dir / f"{output_basename}.csv"
    all_transactions_csv = args.all_transactions or (data_dir / "all_transactions.csv")

    mm = await get_mm()

    unreviewed_transactions: list[dict] = []
    write_json(unreviewed_json, unreviewed_transactions)
    write_csv(unreviewed_csv, unreviewed_transactions)
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

        updated, appended = upsert_all_transactions_csv(all_transactions_csv, transactions)
        write_json(unreviewed_json, unreviewed_transactions)
        write_csv(unreviewed_csv, unreviewed_transactions)

        print(
            f"Saved after batch {batch_num}: "
            f"{len(unreviewed_transactions)} unreviewed, "
            f"{updated} updated in all_transactions.csv, "
            f"{appended} appended"
        )

        if len(transactions) < BATCH_SIZE:
            print("Last partial batch received. Finished.")
            break

        offset += BATCH_SIZE

    print("Done.")
    print(f"All transactions CSV: {all_transactions_csv.resolve()}")
    print(f"Unreviewed JSON: {unreviewed_json.resolve()}")
    print(f"Unreviewed CSV:  {unreviewed_csv.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
