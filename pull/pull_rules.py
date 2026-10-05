"""
pull_rules.py
-------------
Export the transaction rules from Monarch's Settings -> Rules.

Writes to the data folder:
    rules.json   the rules as Monarch returns them, in rule order (read by monarch-categorize)
    rules.csv    one row per rule, readable in a spreadsheet

The monarchmoney library has no call for rules, so this sends the same
read-only GraphQL query the Monarch web app uses.

Usage:
    python pull/pull_rules.py
    python pull/pull_rules.py --data-dir ~/monarch-data/data3
"""
import argparse
import asyncio
import csv
import json
from pathlib import Path

from gql import gql

from common.client import get_monarch_client
from common.config import data_dir
from common.monarch_api import configure_monarch_api

configure_monarch_api()

RULES_QUERY = gql("""
query GetTransactionRules {
  transactionRules {
    id
    order
    merchantCriteriaUseOriginalStatement
    merchantCriteria { operator value }
    merchantNameCriteria { operator value }
    originalStatementCriteria { operator value }
    amountCriteria { operator isExpense value valueRange { lower upper } }
    categoryIds
    accountIds
    categories { id name }
    accounts { id displayName }
    setMerchantAction { id name }
    setCategoryAction { id name }
    addTagsAction { id name }
    setHideFromReportsAction
    reviewStatusAction
    needsReviewByUserAction { id }
    sendNotificationAction
    recentApplicationCount
    lastAppliedAt
  }
}
""")

CSV_FIELDS = [
    "Order", "Rule ID", "Merchant Criteria", "Matches", "Amount", "Accounts",
    "Only If Category", "Set Category", "Set Merchant", "Add Tags",
    "Hide From Reports", "Recent Applications", "Last Applied",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Monarch transaction rules.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Directory where rules.json and rules.csv will be written.",
    )
    return parser.parse_args()


def describe_criteria(rule: dict) -> tuple[str, str]:
    """Readable merchant criteria, and which field they are checked against."""
    parts = []
    field = "merchant"
    for key in ("merchantCriteria", "merchantNameCriteria", "originalStatementCriteria"):
        for criterion in rule.get(key) or []:
            parts.append(f"{criterion['operator']} {criterion['value']!r}")
        if rule.get(key) and (key == "originalStatementCriteria" or (
            key == "merchantCriteria" and rule.get("merchantCriteriaUseOriginalStatement")
        )):
            field = "original statement"
    return " OR ".join(parts), field if parts else ""


def describe_amount(criteria: dict | None) -> str:
    if not criteria:
        return ""
    kind = {True: "expense ", False: "income "}.get(criteria.get("isExpense"), "")
    if criteria["operator"] == "between":
        bounds = criteria["valueRange"]
        return f"{kind}between {bounds['lower']} and {bounds['upper']}"
    return f"{kind}{criteria['operator']} {criteria['value']}"


def csv_row(rule: dict) -> dict:
    criteria, field = describe_criteria(rule)
    return {
        "Order": rule["order"],
        "Rule ID": rule["id"],
        "Merchant Criteria": criteria,
        "Matches": field,
        "Amount": describe_amount(rule.get("amountCriteria")),
        "Accounts": "; ".join(a["displayName"] for a in rule.get("accounts") or []),
        "Only If Category": "; ".join(c["name"] for c in rule.get("categories") or []),
        "Set Category": (rule.get("setCategoryAction") or {}).get("name", ""),
        "Set Merchant": (rule.get("setMerchantAction") or {}).get("name", ""),
        "Add Tags": ", ".join(t["name"] for t in rule.get("addTagsAction") or []),
        "Hide From Reports": rule.get("setHideFromReportsAction") or False,
        "Recent Applications": rule.get("recentApplicationCount") or 0,
        "Last Applied": (rule.get("lastAppliedAt") or "")[:10],
    }


async def main():
    args = parse_args()
    target = args.data_dir.expanduser() if args.data_dir else data_dir()
    target.mkdir(parents=True, exist_ok=True)

    mm = await get_monarch_client()
    print("Fetching transaction rules...")
    result = await mm.gql_call(operation="GetTransactionRules", graphql_query=RULES_QUERY)
    rules = sorted(result.get("transactionRules", []), key=lambda r: r["order"])

    rules_json = target / "rules.json"
    with open(rules_json, "w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2)
    print(f"Saved: {rules_json} ({len(rules)} rules)")

    rules_csv = target / "rules.csv"
    with open(rules_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for rule in rules:
            writer.writerow(csv_row(rule))
    print(f"Saved: {rules_csv} ({len(rules)} rules)")


asyncio.run(main())
