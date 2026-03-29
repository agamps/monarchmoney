import argparse
import csv
import re
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy selected transactions from all_transactions.csv into push.csv."
    )
    parser.add_argument(
        "transaction_ids",
        nargs="*",
        help="One or more transaction IDs. Comma-separated values are also accepted.",
    )
    parser.add_argument(
        "--transaction-ids",
        dest="transaction_ids_text",
        help="Transaction IDs as a single string separated by spaces, commas, semicolons, or newlines.",
    )
    parser.add_argument(
        "--transactions",
        type=Path,
        default=Path("data/all_transactions.csv"),
        help="Source transactions CSV.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/push.csv"),
        help="Destination CSV to write.",
    )
    return parser.parse_args()


def normalize_transaction_ids(raw_ids: list[str]) -> list[str]:
    transaction_ids: list[str] = []
    seen: set[str] = set()

    for raw in raw_ids:
        parts = [part.strip() for part in raw.split(",")]
        for part in parts:
            if not part or part in seen:
                continue
            seen.add(part)
            transaction_ids.append(part)

    return transaction_ids


def collect_transaction_ids(args: argparse.Namespace) -> list[str]:
    raw_ids = list(args.transaction_ids or [])
    if args.transaction_ids_text:
        raw_ids.extend(
            part
            for part in re.split(r"[\s,;]+", args.transaction_ids_text.strip())
            if part
        )

    transaction_ids = normalize_transaction_ids(raw_ids)
    if not transaction_ids:
        raise ValueError("Provide at least one transaction ID.")

    return transaction_ids


def load_transactions(path: Path) -> tuple[list[str], dict[str, dict[str, str]]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"No header row found in {path}")

        rows_by_id: dict[str, dict[str, str]] = {}
        for row in reader:
            transaction_id = (row.get("Transaction ID") or "").strip()
            if transaction_id:
                rows_by_id[transaction_id] = row

    return reader.fieldnames, rows_by_id


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    transaction_ids = collect_transaction_ids(args)
    fieldnames, rows_by_id = load_transactions(args.transactions)

    selected_rows: list[dict[str, str]] = []
    missing_ids: list[str] = []

    for transaction_id in transaction_ids:
        row = rows_by_id.get(transaction_id)
        if row is None:
            missing_ids.append(transaction_id)
            continue
        selected_rows.append(row)

    write_rows(args.output, fieldnames, selected_rows)

    print(f"Requested transaction IDs: {len(transaction_ids)}")
    print(f"Copied rows: {len(selected_rows)}")
    print(f"Wrote {args.output}")

    if missing_ids:
        print("Missing transaction IDs:")
        for transaction_id in missing_ids:
            print(f"  {transaction_id}")


if __name__ == "__main__":
    main()
