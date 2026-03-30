import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find rows in all_transactions.csv that match patterns from patterns.txt "
            "and write them to a new CSV."
        )
    )
    parser.add_argument(
        "--patterns",
        type=Path,
        default=Path("data/patterns.txt"),
        help="Text file containing one pattern per line.",
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
        default=Path("data/patterns_from_all.csv"),
        help="Destination CSV for matching rows.",
    )
    parser.add_argument(
        "--case-sensitive",
        action="store_true",
        help="Match patterns with case sensitivity.",
    )
    return parser.parse_args()


def load_patterns(path: Path) -> list[str]:
    with open(path, "r", encoding="utf-8-sig") as f:
        patterns = [line.strip() for line in f if line.strip()]

    if not patterns:
        raise ValueError(f"No patterns found in {path}")

    return patterns


def load_transactions(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"No header row found in {path}")

        rows = list(reader)

    return reader.fieldnames, rows


def find_matching_patterns(
    row: dict[str, str], patterns: list[str], case_sensitive: bool
) -> list[str]:
    haystack_parts = [value.strip() for value in row.values() if value and value.strip()]
    haystack = " | ".join(haystack_parts)

    if not case_sensitive:
        haystack = haystack.casefold()

    matched_patterns: list[str] = []
    for pattern in patterns:
        needle = pattern if case_sensitive else pattern.casefold()
        if needle in haystack:
            matched_patterns.append(pattern)

    return matched_patterns


def write_rows(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    patterns = load_patterns(args.patterns)
    fieldnames, rows = load_transactions(args.transactions)

    output_rows: list[dict[str, str]] = []
    for row in rows:
        matched_patterns = find_matching_patterns(
            row=row,
            patterns=patterns,
            case_sensitive=args.case_sensitive,
        )
        if not matched_patterns:
            continue

        output_row = dict(row)
        output_row["Matched Patterns"] = " | ".join(matched_patterns)
        output_rows.append(output_row)

    write_rows(
        path=args.output,
        fieldnames=["Matched Patterns", *fieldnames],
        rows=output_rows,
    )

    print(f"Patterns loaded: {len(patterns)}")
    print(f"Matching rows: {len(output_rows)}")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
