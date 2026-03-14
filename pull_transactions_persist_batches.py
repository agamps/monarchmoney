import asyncio
import csv
import json
import subprocess
import sys
from pathlib import Path

from monarchmoney import MonarchMoney

# ----------------------------
# Config
# ----------------------------
SESSION_FILE = Path(".mm/mm_session.pickle")
LOGIN_SCRIPT = Path("login.py")
SAVE_FOLDER = Path(".")
BATCH_SIZE = 100  # configurable

ALL_JSON = SAVE_FOLDER / "all_transactions.json"
ALL_CSV = SAVE_FOLDER / "all_transactions.csv"
UNREVIEWED_JSON = SAVE_FOLDER / "unreviewed_transactions.json"
UNREVIEWED_CSV = SAVE_FOLDER / "unreviewed_transactions.csv"


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
        "id": txn.get("id"),
        "date": txn.get("date"),
        "amount": txn.get("amount"),
        "signedAmount": txn.get("signedAmount"),
        "isDebit": txn.get("isDebit"),
        "merchantName": txn.get("merchantName"),
        "originalName": txn.get("originalName"),
        "notes": txn.get("notes"),
        "needsReview": txn.get("needsReview"),
        "isRecurring": txn.get("isRecurring"),
        "reviewStatus": txn.get("reviewStatus"),
        "accountId": account.get("id"),
        "accountDisplayName": account.get("displayName"),
        "accountType": account.get("accountType"),
        "merchantId": merchant.get("id"),
        "merchantNameFromObject": merchant.get("name"),
        "categoryId": category.get("id"),
        "categoryName": category.get("name"),
        "categoryGroup": category.get("group"),
        "tagIds": ",".join(str(t.get("id")) for t in tags if t.get("id") is not None),
        "tagNames": ",".join(str(t.get("name")) for t in tags if t.get("name")),
    }


def write_json(path: Path, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, default=str)


def write_csv(path: Path, rows: list[dict]) -> None:
    flat_rows = [flatten_transaction(r) for r in rows]

    headers = [
        "id",
        "date",
        "amount",
        "signedAmount",
        "isDebit",
        "merchantName",
        "originalName",
        "notes",
        "needsReview",
        "isRecurring",
        "reviewStatus",
        "accountId",
        "accountDisplayName",
        "accountType",
        "merchantId",
        "merchantNameFromObject",
        "categoryId",
        "categoryName",
        "categoryGroup",
        "tagIds",
        "tagNames",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        if flat_rows:
            writer.writerows(flat_rows)


def persist_outputs(all_transactions: list[dict], unreviewed_transactions: list[dict]) -> None:
    write_json(ALL_JSON, all_transactions)
    write_csv(ALL_CSV, all_transactions)
    write_json(UNREVIEWED_JSON, unreviewed_transactions)
    write_csv(UNREVIEWED_CSV, unreviewed_transactions)


async def main():
    SAVE_FOLDER.mkdir(parents=True, exist_ok=True)
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

        persist_outputs(all_transactions, unreviewed_transactions)

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
    print(f"All transactions JSON: {ALL_JSON.resolve()}")
    print(f"All transactions CSV:  {ALL_CSV.resolve()}")
    print(f"Unreviewed JSON:       {UNREVIEWED_JSON.resolve()}")
    print(f"Unreviewed CSV:        {UNREVIEWED_CSV.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
