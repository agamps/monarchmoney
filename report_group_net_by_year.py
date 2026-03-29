import argparse
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize transaction net totals by category group and year."
    )
    parser.add_argument(
        "--transactions",
        type=Path,
        default=Path("data/all_transactions.csv"),
        help="Path to the all transactions CSV.",
    )
    parser.add_argument(
        "--groups",
        type=Path,
        default=Path("data/category_groups.csv"),
        help="Path to the category groups CSV.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/group_net_by_year_report.csv"),
        help="Path for the generated report CSV.",
    )
    parser.add_argument(
        "--include-unmapped",
        action="store_true",
        help="Include categories that are missing from the groups file under 'Unmapped'.",
    )
    return parser.parse_args()


def parse_date(value: str) -> datetime:
    text = value.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unsupported date format: {value!r}")


def parse_amount(value: str) -> float:
    return float(value.replace(",", "").strip())


def load_category_group_map(path: Path) -> dict[str, str]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return {
            row["Category Name"].strip(): row["Group Name"].strip()
            for row in reader
            if row.get("Category Name") and row.get("Group Name")
        }


def build_totals(
    transactions_path: Path,
    category_to_group: dict[str, str],
    include_unmapped: bool,
) -> tuple[dict[int, dict[str, float]], int]:
    totals: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    row_count = 0

    with open(transactions_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            category = (row.get("Category") or "").strip()
            group_name = category_to_group.get(category)
            if not group_name:
                if not include_unmapped:
                    continue
                group_name = "Unmapped"

            date_value = row.get("Date")
            amount_value = row.get("Amount")
            if not date_value or not amount_value:
                continue

            year = parse_date(date_value).year
            amount = parse_amount(amount_value)
            totals[year][group_name] += amount

    return totals, row_count


def write_report(path: Path, totals: dict[int, dict[str, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    current_year = datetime.now().year
    years = sorted(totals.keys(), reverse=True)
    years = sorted(years, key=lambda year: (year != current_year, -year))

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)

        for index, year in enumerate(years):
            writer.writerow([str(year)])
            writer.writerow(["Group Name", "Net Income/Expense"])
            for group_name, total in sorted(
                totals[year].items(), key=lambda item: item[0].lower()
            ):
                writer.writerow([group_name, f"{total:.2f}"])

            if index < len(years) - 1:
                writer.writerow([])


def main() -> None:
    args = parse_args()
    category_to_group = load_category_group_map(args.groups)
    totals, row_count = build_totals(
        args.transactions, category_to_group, args.include_unmapped
    )
    write_report(args.output, totals)

    print(f"Read {row_count} transaction rows from {args.transactions}")
    print(f"Loaded {len(category_to_group)} category-to-group mappings from {args.groups}")
    print(f"Saved year-by-group report to {args.output}")


if __name__ == "__main__":
    main()
