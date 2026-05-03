"""
Create push.csv from unreviewed transactions matching merchant/account/category/group filters.

Default inputs:
    data/unreviewed_transactions.csv
    data/filter-unrev-merchants.txt
    data/filter-unrev-accounts.txt
    data/filter-unrev-categories.txt
    data/filter-unrev-groups.txt

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
DEFAULT_CATEGORY_FILTER = Path("data/filter-unrev-categories.txt")
DEFAULT_GROUP_FILTER = Path("data/filter-unrev-groups.txt")
DEFAULT_GROUPS = Path("data/category_groups.csv")
GROUP_SORT_COLUMN = "__group__"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter unreviewed transactions by merchant/account/category/group lists and write "
            "matching rows to push.csv."
        )
    )
    parser.add_argument(
        "--filter-type",
        choices=("accounts", "merchants", "categories", "groups", "all", "both"),
        default="all",
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
        help="Destination push CSV to create. Filenames like push3 become data/push3.csv.",
    )
    parser.add_argument(
        "--write-mode",
        choices=("overwrite", "append"),
        default="overwrite",
        help="Whether to overwrite the output CSV or append new matching rows.",
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
        "--category-filter",
        type=Path,
        default=None,
        help="Category filter file. Defaults to data/filter-unrev-categories.txt if present.",
    )
    parser.add_argument(
        "--group-filter",
        type=Path,
        default=None,
        help="Group filter file. Defaults to data/filter-unrev-groups.txt if present.",
    )
    parser.add_argument(
        "--groups",
        type=Path,
        default=DEFAULT_GROUPS,
        help="Category groups CSV used to map transaction categories to groups.",
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


def resolve_output_path(path: Path) -> Path:
    if path.is_absolute():
        return path

    if len(path.parts) == 1:
        filename = path.name if path.suffix else f"{path.name}.csv"
        return Path("data") / filename

    return path


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


def load_category_group_map(path: Path) -> dict[str, str]:
    fieldnames, rows = load_csv(path)
    category_column = find_column(fieldnames, ("Category Name", "Category"))
    group_column = find_column(fieldnames, ("Group Name", "Group"))
    if category_column is None or group_column is None:
        raise ValueError(f"{path} must have Category Name and Group Name columns.")

    return {
        row[category_column].strip(): row[group_column].strip()
        for row in rows
        if row.get(category_column, "").strip() and row.get(group_column, "").strip()
    }


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


def row_group(
    row: dict[str, str],
    *,
    group_column: str | None,
    category_column: str | None,
    category_to_group: dict[str, str],
) -> str:
    if group_column:
        return row.get(group_column, "")
    if category_column:
        return category_to_group.get(row.get(category_column, "").strip(), "")
    return ""


def sort_rows(
    rows: list[dict[str, str]],
    *,
    filter_type: str,
    merchant_column: str | None,
    account_column: str | None,
    category_column: str | None,
    group_column: str | None,
    category_to_group: dict[str, str],
    id_column: str,
    case_sensitive: bool,
) -> list[dict[str, str]]:
    if filter_type == "accounts":
        primary_column = account_column
        secondary_column = merchant_column
        tertiary_column = category_column
    elif filter_type == "categories":
        primary_column = category_column
        secondary_column = merchant_column
        tertiary_column = account_column
    elif filter_type == "groups":
        primary_column = GROUP_SORT_COLUMN
        secondary_column = category_column
        tertiary_column = merchant_column
    else:
        primary_column = merchant_column
        secondary_column = account_column
        tertiary_column = category_column

    def sort_value(row: dict[str, str], column: str | None) -> str:
        if column is None:
            return ""
        if column == GROUP_SORT_COLUMN:
            return normalize(
                row_group(
                    row,
                    group_column=group_column,
                    category_column=category_column,
                    category_to_group=category_to_group,
                ),
                case_sensitive=case_sensitive,
            )
        return normalize(row.get(column, ""), case_sensitive=case_sensitive)

    primary_counts: dict[str, int] = {}
    for row in rows:
        value = sort_value(row, primary_column)
        primary_counts[value] = primary_counts.get(value, 0) + 1

    return sorted(
        rows,
        key=lambda row: (
            -primary_counts[sort_value(row, primary_column)],
            sort_value(row, primary_column),
            sort_value(row, secondary_column),
            sort_value(row, tertiary_column),
            normalize(row.get(id_column, ""), case_sensitive=case_sensitive),
        ),
    )


def is_locked_file_error(error: OSError) -> bool:
    return isinstance(error, PermissionError) or getattr(error, "winerror", None) in {
        32,
        33,
    }


def warn_locked_output(path: Path) -> None:
    print(f"WARNING: Could not write {path}.")
    print("         It may be open in Excel. Close it and run the filter again.")


def existing_output_ids(path: Path) -> set[str] | None:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return set()

        output_fieldnames, output_rows = load_csv(path)
    except OSError as e:
        if not is_locked_file_error(e):
            raise
        warn_locked_output(path)
        return None

    output_id_column = find_column(output_fieldnames, ("Transaction ID", "id", "transaction_id"))
    if output_id_column is None:
        raise ValueError(f"{path} does not have a transaction ID column.")

    return {
        str(row.get(output_id_column, "")).strip()
        for row in output_rows
        if str(row.get(output_id_column, "")).strip()
    }


def write_rows(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, str]],
    *,
    write_mode: str,
) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        should_append = (
            write_mode == "append" and path.exists() and path.stat().st_size > 0
        )
        mode = "a" if should_append else "w"
        encoding = "utf-8" if should_append else "utf-8-sig"

        with open(path, mode, encoding=encoding, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if not should_append:
                writer.writeheader()
            writer.writerows(rows)
    except OSError as e:
        if not is_locked_file_error(e):
            raise
        warn_locked_output(path)
        return False

    return True


def main() -> None:
    args = parse_args()
    output_path = resolve_output_path(args.output)

    merchant_filter_file = resolve_filter_file(
        args.merchant_filter, DEFAULT_MERCHANT_FILTER, "Merchant"
    )
    account_filter_file = resolve_filter_file(
        args.account_filter, DEFAULT_ACCOUNT_FILTER, "Account"
    )
    category_filter_file = resolve_filter_file(
        args.category_filter, DEFAULT_CATEGORY_FILTER, "Category"
    )
    group_filter_file = resolve_filter_file(
        args.group_filter, DEFAULT_GROUP_FILTER, "Group"
    )
    if args.filter_type == "accounts":
        merchant_filter_file = None
        category_filter_file = None
        group_filter_file = None
    elif args.filter_type == "merchants":
        account_filter_file = None
        category_filter_file = None
        group_filter_file = None
    elif args.filter_type == "categories":
        merchant_filter_file = None
        account_filter_file = None
        group_filter_file = None
    elif args.filter_type == "groups":
        merchant_filter_file = None
        account_filter_file = None
        category_filter_file = None

    merchant_terms = load_terms(
        merchant_filter_file, case_sensitive=args.case_sensitive
    )
    account_terms = load_terms(account_filter_file, case_sensitive=args.case_sensitive)
    category_terms = load_terms(category_filter_file, case_sensitive=args.case_sensitive)
    group_terms = load_terms(group_filter_file, case_sensitive=args.case_sensitive)

    if not merchant_terms and not account_terms and not category_terms and not group_terms:
        if args.filter_type == "accounts":
            expected = str(DEFAULT_ACCOUNT_FILTER)
        elif args.filter_type == "merchants":
            expected = str(DEFAULT_MERCHANT_FILTER)
        elif args.filter_type == "categories":
            expected = str(DEFAULT_CATEGORY_FILTER)
        elif args.filter_type == "groups":
            expected = str(DEFAULT_GROUP_FILTER)
        else:
            expected = (
                f"{DEFAULT_MERCHANT_FILTER}, {DEFAULT_ACCOUNT_FILTER}, "
                f"{DEFAULT_CATEGORY_FILTER}, or {DEFAULT_GROUP_FILTER}"
            )
        raise ValueError(f"No filter terms found. Create {expected}.")

    source_fieldnames, unreviewed_rows = load_csv(args.unreviewed)
    id_column = find_column(source_fieldnames, ("Transaction ID", "id", "transaction_id"))
    merchant_column = find_column(source_fieldnames, ("Merchant", "merchant_name"))
    account_column = find_column(source_fieldnames, ("Account", "account_name"))
    category_column = find_column(source_fieldnames, ("Category", "category_name"))
    group_column = find_column(source_fieldnames, ("Group", "Group Name", "category_group"))
    category_to_group = (
        load_category_group_map(args.groups)
        if group_terms and group_column is None
        else {}
    )

    if id_column is None:
        raise ValueError(f"{args.unreviewed} does not have a transaction ID column.")
    if merchant_terms and merchant_column is None:
        raise ValueError(f"{args.unreviewed} does not have a Merchant column.")
    if account_terms and account_column is None:
        raise ValueError(f"{args.unreviewed} does not have an Account column.")
    if category_terms and category_column is None:
        raise ValueError(f"{args.unreviewed} does not have a Category column.")
    if group_terms and group_column is None and category_column is None:
        raise ValueError(
            f"{args.unreviewed} must have a Group column or a Category column for group filtering."
        )

    matched_rows: list[dict[str, str]] = []
    selected_ids: set[str] = set()
    if args.write_mode == "append":
        output_ids = existing_output_ids(output_path)
        if output_ids is None:
            return
        selected_ids.update(output_ids)

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
        category_hit = bool(
            category_terms
            and category_column
            and matches(
                row.get(category_column, ""),
                category_terms,
                exact=args.exact,
                case_sensitive=args.case_sensitive,
            )
        )
        group_hit = bool(
            group_terms
            and matches(
                row_group(
                    row,
                    group_column=group_column,
                    category_column=category_column,
                    category_to_group=category_to_group,
                ),
                group_terms,
                exact=args.exact,
                case_sensitive=args.case_sensitive,
            )
        )

        if merchant_hit or account_hit or category_hit or group_hit:
            selected_ids.add(transaction_id)
            matched_rows.append(row)

    print(f"Unreviewed rows scanned: {len(unreviewed_rows)}")
    print(f"Merchant filters: {len(merchant_terms)}")
    print(f"Account filters: {len(account_terms)}")
    print(f"Category filters: {len(category_terms)}")
    print(f"Group filters: {len(group_terms)}")
    print(f"Write mode: {args.write_mode}")
    print(f"Rows to write: {len(matched_rows)}")

    if args.dry_run:
        print("Dry run: no changes written.")
        return

    matched_rows = sort_rows(
        matched_rows,
        filter_type=args.filter_type,
        merchant_column=merchant_column,
        account_column=account_column,
        category_column=category_column,
        group_column=group_column,
        category_to_group=category_to_group,
        id_column=id_column,
        case_sensitive=args.case_sensitive,
    )
    if not write_rows(
        output_path,
        source_fieldnames,
        matched_rows,
        write_mode=args.write_mode,
    ):
        return

    print(f"Done: {output_path}")


if __name__ == "__main__":
    main()
