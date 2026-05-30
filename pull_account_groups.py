"""
pull_account_groups.py
----------------------
Pulls Monarch Money accounts with their account type/group metadata and writes:

    data/account_groups.csv
        Account | Account ID | Account Group | Account Type | Account Subtype | ...

Usage:
    python pull_account_groups.py
    python pull_account_groups.py --output data/account_groups.csv
"""

import argparse
import asyncio
import csv
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from gql.transport.exceptions import TransportServerError
from monarchmoney import MonarchMoney

SESSION_FILE = Path(".mm/mm_session.pickle")
LOGIN_SCRIPT = Path("login.py")
DEFAULT_DATA_DIR = Path(os.environ.get("MONARCH_DATA_DIR", "data"))
DEFAULT_OUTPUT = DEFAULT_DATA_DIR / "account_groups.csv"


FIELDNAMES = [
    "Account",
    "Account ID",
    "Account Group",
    "Account Group Key",
    "Account Type",
    "Account Type Key",
    "Account Subtype",
    "Account Subtype Key",
    "Institution",
    "Current Balance",
    "Display Balance",
    "Is Asset",
    "Include In Net Worth",
    "Include Balance In Net Worth",
    "Hide From List",
    "Hide Transactions From Reports",
    "Is Manual",
    "Sync Disabled",
    "Deactivated At",
    "Transactions Count",
    "Holdings Count",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Monarch Money account type/group mappings to CSV."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV file path (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args()


async def get_mm() -> MonarchMoney:
    if not SESSION_FILE.exists():
        print(f"Session file not found: {SESSION_FILE}")
        print(f"Running {LOGIN_SCRIPT}...")
        result = subprocess.run([sys.executable, str(LOGIN_SCRIPT)], check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"Unable to create a Monarch session. {LOGIN_SCRIPT} exited with code {result.returncode}."
            )

        if not SESSION_FILE.exists():
            raise RuntimeError(
                f"{LOGIN_SCRIPT} completed but did not create {SESSION_FILE}."
            )

    mm = MonarchMoney()
    mm.load_session(str(SESSION_FILE))

    try:
        await mm.get_accounts()
        return mm
    except TransportServerError as e:
        if "401" not in str(e):
            raise

        print("Saved session expired. Re-running login.py...")
        result = subprocess.run([sys.executable, str(LOGIN_SCRIPT)], check=False)
        if result.returncode != 0:
            raise RuntimeError(
                "Monarch session expired and automatic re-login failed. "
                f"Please run `py .\\{LOGIN_SCRIPT}` and try again."
            ) from e

        if not SESSION_FILE.exists():
            raise RuntimeError(
                "Monarch re-login completed but no session file was saved. "
                f"Expected: {SESSION_FILE}"
            ) from e

        mm = MonarchMoney()
        mm.load_session(str(SESSION_FILE))
        await mm.get_accounts()
        return mm


def label_from_key(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.replace("_", " ").title()


def build_type_lookup(type_options: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    for option in type_options:
        type_info = option.get("type") or {}
        type_key = str(type_info.get("name") or "").strip()
        if not type_key:
            continue

        lookup[type_key] = {
            "display": str(type_info.get("display") or "").strip(),
            "group": str(type_info.get("group") or "").strip(),
        }
    return lookup


def flatten_account(
    account: dict[str, Any],
    type_lookup: dict[str, dict[str, str]],
) -> dict[str, object]:
    type_info = account.get("type") or {}
    subtype_info = account.get("subtype") or {}
    institution = account.get("institution") or {}

    type_key = str(type_info.get("name") or "").strip()
    type_display = str(type_info.get("display") or "").strip()
    type_metadata = type_lookup.get(type_key, {})
    group_key = str(type_info.get("group") or type_metadata.get("group") or "").strip()

    return {
        "Account": account.get("displayName", ""),
        "Account ID": account.get("id", ""),
        "Account Group": label_from_key(group_key),
        "Account Group Key": group_key,
        "Account Type": type_display or type_metadata.get("display", ""),
        "Account Type Key": type_key,
        "Account Subtype": subtype_info.get("display", ""),
        "Account Subtype Key": subtype_info.get("name", ""),
        "Institution": institution.get("name", ""),
        "Current Balance": account.get("currentBalance", ""),
        "Display Balance": account.get("displayBalance", ""),
        "Is Asset": account.get("isAsset", ""),
        "Include In Net Worth": account.get("includeInNetWorth", ""),
        "Include Balance In Net Worth": account.get("includeBalanceInNetWorth", ""),
        "Hide From List": account.get("hideFromList", ""),
        "Hide Transactions From Reports": account.get("hideTransactionsFromReports", ""),
        "Is Manual": account.get("isManual", ""),
        "Sync Disabled": account.get("syncDisabled", ""),
        "Deactivated At": account.get("deactivatedAt", ""),
        "Transactions Count": account.get("transactionsCount", ""),
        "Holdings Count": account.get("holdingsCount", ""),
    }


def sort_key(row: dict[str, object]) -> tuple[str, str, str, str]:
    return (
        str(row["Account Group"]).lower(),
        str(row["Account Type"]).lower(),
        str(row["Institution"]).lower(),
        str(row["Account"]).lower(),
    )


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


async def main() -> None:
    args = parse_args()
    mm = await get_mm()

    print("Fetching accounts...")
    accounts_result = await mm.get_accounts()
    accounts = accounts_result.get("accounts", [])
    print(f"Found {len(accounts)} accounts.")

    print("Fetching account type options...")
    type_options_result = await mm.get_account_type_options()
    type_options = type_options_result.get("accountTypeOptions", [])
    type_lookup = build_type_lookup(type_options)

    rows = [flatten_account(account, type_lookup) for account in accounts]
    rows.sort(key=sort_key)
    write_csv(args.output, rows)

    print(f"Saved {len(rows)} rows to {args.output}")

    current_group = None
    for row in rows:
        group = str(row["Account Group"] or "(blank)")
        if group != current_group:
            current_group = group
            print(f"\n{current_group}")
        print(f"  {row['Account']} - {row['Account Type']} / {row['Account Subtype']}")


if __name__ == "__main__":
    asyncio.run(main())
