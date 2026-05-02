"""
Create push.csv from unreviewed transactions matching merchant/account filters.

Default inputs:
    data/unreviewed_transactions.csv
    data/filter-unrev-merchants.txt
    data/filter-unrev-accounts.txt

Each filter file is one search term per line. Blank lines and lines starting
with # are ignored. Matching is case-insensitive substring matching by default.
"""

import argparse
import csv
from pathlib import Path

CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
DEFAULT_UNREVIEWED = Path("data/unreviewed_transactions.csv")
DEFAULT_OUTPUT = Path("data/push.csv")
DEFAULT_MERCHANT_FILTER = Path("data/filter-unrev-merchants.txt")
DEFAULT_ACCOUNT_FILTER = Path("data/filter-unrev-accounts.txt")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter unreviewed transactions by merchant/account lists and write "
            "matching rows to push.csv."
        )
    )
    parser.add_argument(
        "--filter-type",
        choices=("accounts", "merchants", "both"),
        default="both",
        help="Which default filter file to use.",
    )
    parser.add_argument(
        "--unreviewed",
        type=Path,
        default=DEFAULT_UNREVIEWED,
        help="Source unreviewed transactions CSV.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Destination push CSV to create.",
    )
    parser.add_argument(
        "--merchant-filter",
        type=Path,
        default=None,
        help="Merchant filter file. Defaults to data/filter-unrev-merchants.txt if present.",
    )
    parser.add_argument(
        "--account-filter",
        type=Path,
        default=None,
        help="Account filter file. Defaults to data/filter-unrev-accounts.txt if present.",
    )
    parser.add_argument(
        "--exact",
        action="store_true",
        help="Use exact matches instead of substring matches.",
    )
    parser.add_argument(
        "--case-sensitive",
        action="store_true",
        help="Use case-sensitive matching.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the write summary without writing output.",
    )
    return parser.parse_args()


def normalize(value: str, *, case_sensitive: bool) -> str:
    text = value.strip()
    return text if case_sensitive else text.casefold()


def resolve_filter_file(
    explicit_path: Path | None,
    default_path: Path,
    label: str,
) -> Path | None:
    if explicit_path is not None:
        if not explicit_path.exists():
            raise FileNotFoundError(f"{label} filter file not found: {explicit_path}")
        return explicit_path

    return default_path if default_path.exists() else None


def load_terms(path: Path | None, *, case_sensitive: bool) -> list[str]:
    if path is None:
        return []

    terms: list[str] = []
    seen: set[str] = set()

    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            term = line.strip()
            if not term or term.startswith("#"):
                continue

            key = normalize(term, case_sensitive=case_sensitive)
            if key in seen:
                continue

            seen.add(key)
            terms.append(term)

    return terms


def load_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    last_error: UnicodeDecodeError | None = None

    for encoding in CSV_ENCODINGS:
        try:
            with open(path, "r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    raise ValueError(f"No header row found in {path}")
                return reader.fieldnames, list(reader)
        except UnicodeDecodeError as e:
            last_error = e

    assert last_error is not None
    raise ValueError(
        f"Could not decode {path} using supported encodings: {', '.join(CSV_ENCODINGS)}"
    ) from last_error


def find_column(fieldnames: list[str], candidates: tuple[str, ...]) -> str | None:
    columns = {field.strip().casefold(): field for field in fieldnames}
    for candidate in candidates:
        match = columns.get(candidate.casefold())
        if match is not None:
            return match
    return None


def matches(value: str, terms: list[str], *, exact: bool, case_sensitive: bool) -> bool:
    haystack = normalize(value, case_sensitive=case_sensitive)
    for term in terms:
        needle = normalize(term, case_sensitive=case_sensitive)
        if exact and haystack == needle:
            return True
        if not exact and needle in haystack:
            return True
    return False


def sort_rows(
    rows: list[dict[str, str]],
    *,
    filter_type: str,
    merchant_column: str | None,
    account_column: str | None,
    id_column: str,
    case_sensitive: bool,
) -> list[dict[str, str]]:
    if filter_type == "accounts":
        primary_column = account_column
        secondary_column = merchant_column
    else:
        primary_column = merchant_column
        secondary_column = account_column

    def sort_value(row: dict[str, str], column: str | None) -> str:
        if column is None:
            return ""
        return normalize(row.get(column, ""), case_sensitive=case_sensitive)

    return sorted(
        rows,
        key=lambda row: (
            sort_value(row, primary_column),
            sort_value(row, secondary_column),
            normalize(row.get(id_column, ""), case_sensitive=case_sensitive),
        ),
    )


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()

    merchant_filter_file = resolve_filter_file(
        args.merchant_filter, DEFAULT_MERCHANT_FILTER, "Merchant"
    )
    account_filter_file = resolve_filter_file(
        args.account_filter, DEFAULT_ACCOUNT_FILTER, "Account"
    )
    if args.filter_type == "accounts":
        merchant_filter_file = None
    elif args.filter_type == "merchants":
        account_filter_file = None

    merchant_terms = load_terms(
        merchant_filter_file, case_sensitive=args.case_sensitive
    )
    account_terms = load_terms(account_filter_file, case_sensitive=args.case_sensitive)

    if not merchant_terms and not account_terms:
        if args.filter_type == "accounts":
            expected = str(DEFAULT_ACCOUNT_FILTER)
        elif args.filter_type == "merchants":
            expected = str(DEFAULT_MERCHANT_FILTER)
        else:
            expected = f"{DEFAULT_MERCHANT_FILTER} or {DEFAULT_ACCOUNT_FILTER}"
        raise ValueError(f"No filter terms found. Create {expected}.")

    source_fieldnames, unreviewed_rows = load_csv(args.unreviewed)
    id_column = find_column(source_fieldnames, ("Transaction ID", "id", "transaction_id"))
    merchant_column = find_column(source_fieldnames, ("Merchant", "merchant_name"))
    account_column = find_column(source_fieldnames, ("Account", "account_name"))

    if id_column is None:
        raise ValueError(f"{args.unreviewed} does not have a transaction ID column.")
    if merchant_terms and merchant_column is None:
        raise ValueError(f"{args.unreviewed} does not have a Merchant column.")
    if account_terms and account_column is None:
        raise ValueError(f"{args.unreviewed} does not have an Account column.")

    matched_rows: list[dict[str, str]] = []
    selected_ids: set[str] = set()

    for row in unreviewed_rows:
        transaction_id = str(row.get(id_column, "")).strip()
        if not transaction_id or transaction_id in selected_ids:
            continue

        merchant_hit = bool(
            merchant_terms
            and merchant_column
            and matches(
                row.get(merchant_column, ""),
                merchant_terms,
                exact=args.exact,
                case_sensitive=args.case_sensitive,
            )
        )
        account_hit = bool(
            account_terms
            and account_column
            and matches(
                row.get(account_column, ""),
                account_terms,
                exact=args.exact,
                case_sensitive=args.case_sensitive,
            )
        )

        if merchant_hit or account_hit:
            selected_ids.add(transaction_id)
            matched_rows.append(row)

    print(f"Unreviewed rows scanned: {len(unreviewed_rows)}")
    print(f"Merchant filters: {len(merchant_terms)}")
    print(f"Account filters: {len(account_terms)}")
    print(f"Rows to write: {len(matched_rows)}")

    if args.dry_run:
        print("Dry run: no changes written.")
        return

    matched_rows = sort_rows(
        matched_rows,
        filter_type=args.filter_type,
        merchant_column=merchant_column,
        account_column=account_column,
        id_column=id_column,
        case_sensitive=args.case_sensitive,
    )
    write_rows(args.output, source_fieldnames, matched_rows)

    print(f"Done: {args.output}")


if __name__ == "__main__":
    main()
