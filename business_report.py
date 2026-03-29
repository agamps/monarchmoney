import argparse
from pathlib import Path

import pandas as pd
from openpyxl.styles import Font
from openpyxl.styles import PatternFill


BUSINESS_GROUPS = {
    "immanent": [
        "Immanent Holdings Expenses",
        "Immanent Holdings Income",
        "Immanent to Personal and Back",
    ],
    "virtus": [
        "Virtus Medicus Expenses",
        "Virtus Medicus Income",
        "Virtus Medicus to Personal and Back",
    ],
}

HEADER_FILL = PatternFill(fill_type="solid", fgColor="1F4E78")
HEADER_FONT = Font(name="Consolas", bold=True, color="FFFFFF")
BODY_FONT = Font(name="Consolas")
AMOUNT_FORMAT = "#,##0.00"
DATE_FORMAT = "mm/dd/yyyy"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a 3-tab Excel business report from Monarch transactions."
    )
    parser.add_argument(
        "--fiscal-year",
        type=int,
        help="Fiscal year to report on, using the transaction date year.",
    )
    parser.add_argument(
        "--business",
        choices=sorted(BUSINESS_GROUPS.keys()),
        help="Business to report on: immanent or virtus.",
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
        help="Output Excel file path. Defaults to data/<business>_business_report_<year>.xlsx.",
    )
    return parser.parse_args()


def prompt_for_year(value: int | None) -> int:
    if value is not None:
        return value

    while True:
        raw = input("Fiscal year: ").strip()
        if raw.isdigit() and len(raw) == 4:
            return int(raw)
        print("Enter a 4-digit year like 2026.")


def prompt_for_business(value: str | None) -> str:
    if value is not None:
        return value

    while True:
        raw = input("Business (virtus or immanent): ").strip().lower()
        if raw in BUSINESS_GROUPS:
            return raw
        print("Enter either 'virtus' or 'immanent'.")


def default_output_path(business: str, fiscal_year: int) -> Path:
    return Path("data") / f"{business}_business_report_{fiscal_year}.xlsx"


def load_data(transactions_path: Path, groups_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    transactions_df = pd.read_csv(transactions_path, dtype={"Transaction ID": str}, encoding="utf-8-sig")
    groups_df = pd.read_csv(groups_path, dtype=str, encoding="utf-8-sig")
    return transactions_df, groups_df


def prepare_transactions(
    transactions_df: pd.DataFrame,
    groups_df: pd.DataFrame,
    fiscal_year: int,
    business: str,
) -> pd.DataFrame:
    tx = transactions_df.copy()
    tx["Date"] = pd.to_datetime(tx["Date"], errors="coerce")
    tx["Amount"] = pd.to_numeric(tx["Amount"], errors="coerce")
    tx = tx.dropna(subset=["Date", "Amount", "Category"])
    tx = tx[tx["Date"].dt.year == fiscal_year]

    group_lookup = groups_df[["Category Name", "Group Name"]].dropna().copy()
    group_lookup["Category Name"] = group_lookup["Category Name"].str.strip()
    group_lookup["Group Name"] = group_lookup["Group Name"].str.strip()

    tx["Category"] = tx["Category"].astype(str).str.strip()
    merged = tx.merge(
        group_lookup,
        how="left",
        left_on="Category",
        right_on="Category Name",
    )

    selected_groups = BUSINESS_GROUPS[business]
    filtered = merged[merged["Group Name"].isin(selected_groups)].copy()

    filtered["Group Name"] = pd.Categorical(
        filtered["Group Name"],
        categories=selected_groups,
        ordered=True,
    )
    return filtered


def build_group_summary(filtered_df: pd.DataFrame, business: str) -> pd.DataFrame:
    selected_groups = BUSINESS_GROUPS[business]
    summary = (
        filtered_df.groupby("Group Name", observed=False)["Amount"]
        .sum()
        .reindex(selected_groups, fill_value=0.0)
        .reset_index()
    )
    summary = summary.rename(columns={"Amount": "Net Amount"})
    summary = summary.sort_values(["Net Amount", "Group Name"], kind="stable")
    return summary


def build_category_totals(filtered_df: pd.DataFrame) -> pd.DataFrame:
    category_totals = (
        filtered_df.groupby(["Group Name", "Category"], observed=True)["Amount"]
        .sum()
        .reset_index()
        .rename(columns={"Amount": "Net Amount"})
        .sort_values(["Group Name", "Net Amount", "Category"], kind="stable")
    )
    return category_totals


def build_transactions_tab(filtered_df: pd.DataFrame) -> pd.DataFrame:
    transactions_tab = filtered_df.copy()
    transactions_tab = transactions_tab.drop(
        columns=["Category Name", "Hide From Reports", "Needs Review"],
        errors="ignore",
    )
    transactions_tab = transactions_tab.rename(columns={"Category": "Category Name"})
    transactions_tab = transactions_tab.sort_values(
        ["Group Name", "Category Name", "Amount"],
        ascending=[True, True, False],
        kind="stable",
    )

    first_columns = ["Transaction ID", "Group Name", "Category Name"]
    remaining_columns = [
        col for col in transactions_tab.columns if col not in first_columns
    ]
    transactions_tab = transactions_tab[first_columns + remaining_columns]

    return transactions_tab


def write_excel_report(
    output_path: Path,
    summary_df: pd.DataFrame,
    category_totals_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="Group Summary")
        category_totals_df.to_excel(writer, index=False, sheet_name="Category Totals")
        transactions_df.to_excel(writer, index=False, sheet_name="Transactions")

        for sheet_name in ["Group Summary", "Category Totals", "Transactions"]:
            ws = writer.sheets[sheet_name]

            headers = {
                cell.column: str(cell.value or "").strip()
                for cell in ws[1]
            }

            for cell in ws[1]:
                cell.font = HEADER_FONT
                cell.fill = HEADER_FILL

            for row in ws.iter_rows(min_row=2):
                for cell in row:
                    cell.font = BODY_FONT
                    header_name = headers.get(cell.column, "")
                    if header_name in {"Amount", "Net Amount"}:
                        cell.number_format = AMOUNT_FORMAT
                    elif header_name == "Date":
                        cell.number_format = DATE_FORMAT

            for column_cells in ws.columns:
                max_length = max(len(str(cell.value or "")) for cell in column_cells)
                ws.column_dimensions[column_cells[0].column_letter].width = min(max_length + 2, 50)


def main() -> None:
    args = parse_args()
    fiscal_year = prompt_for_year(args.fiscal_year)
    business = prompt_for_business(args.business)
    output_path = args.output or default_output_path(business, fiscal_year)

    transactions_df, groups_df = load_data(args.transactions, args.groups)
    filtered_df = prepare_transactions(transactions_df, groups_df, fiscal_year, business)

    summary_df = build_group_summary(filtered_df, business)
    category_totals_df = build_category_totals(filtered_df)
    transactions_tab_df = build_transactions_tab(filtered_df)

    write_excel_report(
        output_path,
        summary_df,
        category_totals_df,
        transactions_tab_df,
    )

    print(f"Fiscal year: {fiscal_year}")
    print(f"Business: {business}")
    print(f"Transactions included: {len(filtered_df)}")
    print(f"Saved Excel report to {output_path}")


if __name__ == "__main__":
    main()
