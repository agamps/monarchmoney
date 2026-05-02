import argparse
from pathlib import Path

import pandas as pd
from openpyxl.styles import Font
from openpyxl.styles import PatternFill


CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
DEFAULT_TRANSACTIONS = Path("data/unreviewed_transactions.csv")
DEFAULT_OUTPUT = Path("data/unreviewed_pivots.xlsx")

HEADER_FILL = PatternFill(fill_type="solid", fgColor="1F4E78")
HEADER_FONT = Font(name="Consolas", bold=True, color="FFFFFF")
BODY_FONT = Font(name="Consolas")
AMOUNT_FORMAT = "#,##0.00"
DATE_FORMAT = "mm/dd/yyyy"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create an Excel pivot-style workbook from unreviewed transactions."
    )
    parser.add_argument(
        "--transactions",
        type=Path,
        default=DEFAULT_TRANSACTIONS,
        help="Path to unreviewed_transactions.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path for the generated Excel workbook.",
    )
    return parser.parse_args()


def read_csv(path: Path) -> pd.DataFrame:
    last_error: UnicodeDecodeError | None = None

    for encoding in CSV_ENCODINGS:
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding)
        except UnicodeDecodeError as e:
            last_error = e

    assert last_error is not None
    raise ValueError(
        f"Could not decode {path} using supported encodings: {', '.join(CSV_ENCODINGS)}"
    ) from last_error


def require_columns(df: pd.DataFrame, path: Path, columns: list[str]) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{path} is missing required columns: {', '.join(missing)}")


def prepare_transactions(df: pd.DataFrame, path: Path) -> pd.DataFrame:
    require_columns(
        df,
        path,
        ["Transaction ID", "Account", "Merchant", "Category", "Amount"],
    )

    prepared = df.copy()
    prepared["Amount"] = pd.to_numeric(
        prepared["Amount"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(0.0)
    prepared["Total Abs Amount"] = prepared["Amount"].abs()

    if "Date" in prepared.columns:
        prepared["Date"] = pd.to_datetime(prepared["Date"], errors="coerce")

    for column in ["Account", "Merchant", "Category"]:
        prepared[column] = (
            prepared[column]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", "(blank)")
        )

    return prepared


def summary_by(df: pd.DataFrame, dimension: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[dimension, "Transaction Count", "Net Amount", "Total Abs Amount"]
        )

    summary = (
        df.groupby(dimension, dropna=False)
        .agg(
            **{
                "Transaction Count": ("Transaction ID", "count"),
                "Net Amount": ("Amount", "sum"),
                "Total Abs Amount": ("Total Abs Amount", "sum"),
            }
        )
        .reset_index()
    )
    return summary.sort_values(
        ["Transaction Count", "Total Abs Amount", dimension],
        ascending=[False, False, True],
        kind="stable",
    )


def summary_by_pair(df: pd.DataFrame, first: str, second: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[first, second, "Transaction Count", "Net Amount", "Total Abs Amount"]
        )

    summary = (
        df.groupby([first, second], dropna=False)
        .agg(
            **{
                "Transaction Count": ("Transaction ID", "count"),
                "Net Amount": ("Amount", "sum"),
                "Total Abs Amount": ("Total Abs Amount", "sum"),
            }
        )
        .reset_index()
    )
    return summary.sort_values(
        ["Transaction Count", "Total Abs Amount", first, second],
        ascending=[False, False, True, True],
        kind="stable",
    )


def count_matrix(df: pd.DataFrame, index: str, columns: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[index, "Total"])

    matrix = pd.pivot_table(
        df,
        index=index,
        columns=columns,
        values="Transaction ID",
        aggfunc="count",
        fill_value=0,
        margins=True,
        margins_name="Total",
    )

    if "Total" in matrix.index:
        total_row = matrix.loc[["Total"]]
        body = matrix.drop(index="Total").sort_values(
            ["Total"],
            ascending=[False],
            kind="stable",
        )
        matrix = pd.concat([body, total_row])
    else:
        matrix = matrix.sort_index(kind="stable")

    matrix = matrix.reset_index()
    matrix.columns = [str(column) for column in matrix.columns]
    return matrix


def raw_unreviewed(df: pd.DataFrame) -> pd.DataFrame:
    preferred_columns = [
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
    ordered_columns = [
        column for column in preferred_columns if column in df.columns
    ] + [column for column in df.columns if column not in preferred_columns]

    sort_columns = [column for column in ["Account", "Merchant", "Date"] if column in df.columns]
    if sort_columns:
        return df[ordered_columns].sort_values(sort_columns, kind="stable")
    return df[ordered_columns]


def format_workbook(writer: pd.ExcelWriter) -> None:
    for ws in writer.sheets.values():
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        headers = {cell.column: str(cell.value or "").strip() for cell in ws[1]}
        for cell in ws[1]:
            cell.font = HEADER_FONT
            cell.fill = HEADER_FILL

        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.font = BODY_FONT
                header_name = headers.get(cell.column, "")
                if "Amount" in header_name:
                    cell.number_format = AMOUNT_FORMAT
                elif header_name == "Date":
                    cell.number_format = DATE_FORMAT

        for column_cells in ws.columns:
            max_length = max(len(str(cell.value or "")) for cell in column_cells)
            ws.column_dimensions[column_cells[0].column_letter].width = min(
                max(max_length + 2, 10),
                50,
            )


def write_report(output_path: Path, df: pd.DataFrame) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sheets = {
        "Merchant Summary": summary_by(df, "Merchant"),
        "Account Summary": summary_by(df, "Account"),
        "Category Summary": summary_by(df, "Category"),
        "Merchant Account": summary_by_pair(df, "Merchant", "Account"),
        "Account Category": summary_by_pair(df, "Account", "Category"),
        "Merchant Acct Pivot": count_matrix(df, "Merchant", "Account"),
        "Account Cat Pivot": count_matrix(df, "Account", "Category"),
        "Raw Unreviewed": raw_unreviewed(df),
    }

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, sheet_df in sheets.items():
            sheet_df.to_excel(writer, index=False, sheet_name=sheet_name)
        format_workbook(writer)


def main() -> None:
    args = parse_args()
    transactions_df = read_csv(args.transactions)
    prepared_df = prepare_transactions(transactions_df, args.transactions)
    write_report(args.output, prepared_df)

    print(f"Read {len(prepared_df)} unreviewed transaction rows from {args.transactions}")
    print(f"Saved unreviewed pivot workbook to {args.output}")


if __name__ == "__main__":
    main()
