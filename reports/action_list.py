"""
action_list.py
--------------
Quick list of follow-ups tagged in Monarch, plus a watch list of recurring costs.

Follow-ups are transactions carrying a tag that starts with "!Act_" (read live
from Monarch, so tags added in the app show up right away):

    !Act_High / !Act_Low   priority (exactly one per open item)
    !Act_Due               has a deadline: note starts "Due YYYY-MM-DD:"
    !Act_Waiting           someone else has to act next
    !Act_Recurring         a recurring cost; tag only its latest charge

Sections:
    1. Overdue / due soon     any item with a Due date within --due-days
    2. High                   your move, largest yearly $ first
    3. Waiting                oldest first; flagged after --nudge-days
    4. Low                    your move, largest yearly $ first (never expires)
    5. Watch list             recurring costs from all_transactions.csv: large
                              (>= --threshold a year) and small, change vs the
                              year before, and price increases
    6. Housekeeping           tags and notes that disagree, Recurring tags on an
                              old charge, refunds that look like they close an item

Usage:
    python reports/action_list.py
    python reports/action_list.py --due-days 30 --threshold 300 --output ~/action_list.txt
"""

import argparse
import asyncio
import re
from datetime import date
from pathlib import Path

import pandas as pd
from gql import gql

from common.client import get_monarch_client
from common.config import data_path

PREFIX = "!Act_"
HIGH, LOW, DUE, WAITING, RECURRING = (PREFIX + s for s in ("High", "Low", "Due", "Waiting", "Recurring"))
DUE_RE = re.compile(r"^Due (\d{4}-\d{2}-\d{2}):")
WAITING_RE = re.compile(r"Waiting since (\d{4}-\d{2}-\d{2})")
# Frequent shopping is not a recurring bill, even when it happens about monthly.
SHOPPING = re.compile(r"Repair|Remodel|Groceries|Restaurants|Clothing|Replenishables|Gifts|Trip \d")
NOT_COSTS = re.compile(r"^(Transfer|Credit Card Payment|Investment Transaction|Transfer to investments|Transfer from)")
COST_CATEGORIES = {  # label -> category-name pattern
    "High interest": "High Interest",
    "Insurance (categories with 'Insurance', incl. Utilities, Insurance)": "Insurance",
    "Insurance only (Vehicle, Life, College)": r"^(Vehicle: Insurance|Life Insurance|College Insurance)",
}

QUERY = gql("""
query ActionItems($filters: TransactionFilterInput) {
  allTransactions(filters: $filters) {
    results(offset: 0, limit: 2000) {
      id date amount notes needsReview
      merchant { name } account { displayName } category { name } tags { name }
    }
  }
}
""")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Quick list of !Act_ follow-ups and recurring costs.")
    p.add_argument("--transactions", default=str(data_path("all_transactions.csv")),
                   help="History used for recurring costs (default: <data dir>/all_transactions.csv).")
    p.add_argument("--output", default=str(data_path("action_list.txt")), help="Also save the list here.")
    p.add_argument("--due-days", type=int, default=14, help="'Due soon' window in days (default 14).")
    p.add_argument("--nudge-days", type=int, default=30, help="Flag Waiting items older than this (default 30).")
    p.add_argument("--threshold", type=float, default=300, help="Large vs small recurring, $/yr (default 300).")
    p.add_argument("--top", type=int, default=15, help="Rows in the large-recurring list (default 15).")
    return p.parse_args()


# ── Data ─────────────────────────────────────────────────────────────────────

async def fetch_action_items() -> pd.DataFrame:
    mm = await get_monarch_client()
    tags = (await mm.get_transaction_tags())["householdTransactionTags"]
    ids = [t["id"] for t in tags if t["name"].startswith(PREFIX)]
    if not ids:
        return pd.DataFrame()
    res = await mm.gql_call(operation="ActionItems", graphql_query=QUERY, variables={"filters": {"tags": ids}})
    rows = []
    for t in res["allTransactions"]["results"]:
        rows.append({
            "id": t["id"],
            "date": pd.Timestamp(t["date"]),
            "amount": float(t["amount"]),
            "notes": (t["notes"] or "").strip(),
            "merchant": (t["merchant"] or {}).get("name", "").strip(),
            "account": (t["account"] or {}).get("displayName", ""),
            "category": (t["category"] or {}).get("name", ""),
            "tags": [x["name"] for x in t["tags"]],
        })
    return pd.DataFrame(rows)


def load_history(path: str) -> pd.DataFrame:
    h = pd.read_csv(path, dtype=str, keep_default_na=False)  # IDs stay strings
    h["amt"] = pd.to_numeric(h["Amount"].str.replace(",", ""), errors="coerce").fillna(0.0)
    h["d"] = pd.to_datetime(h["Date"])
    h["Merchant"] = h["Merchant"].str.strip()
    return h


# ── Recurring costs ──────────────────────────────────────────────────────────

def recurring_costs(hist: pd.DataFrame, today: pd.Timestamp) -> pd.DataFrame:
    """One row per (account, merchant, category) that charges monthly or yearly.
    Category keeps apart policies one payee drafts from the same account (car vs house)."""
    costs = hist[(hist.amt < 0) & ~hist.Category.str.match(NOT_COSTS) & ~hist.Category.str.contains(SHOPPING)]
    out = []
    for (account, merchant, category), g in costs.groupby(["Account", "Merchant", "Category"]):
        g = g.sort_values("d")
        if len(g) < 2 or g.d.max() < today - pd.Timedelta(days=400):
            continue
        gaps = g.d.diff().dt.days.dropna().tail(8)
        if len(gaps) >= 5 and 25 <= gaps.median() <= 35 and len(g[g.d > today - pd.Timedelta(days=365)]) >= 6:  # monthly, tolerating a skipped or catch-up month
            period = "monthly"
        elif (gaps.iloc[-1] in range(340, 395) and abs(g.amt.iloc[-2] / g.amt.iloc[-1] - 1) <= 0.02
              and len(g[g.d > g.d.max() - pd.Timedelta(days=330)]) == 1 and -g.amt.iloc[-1] >= 10):
            period = "yearly"  # same price a year apart, nothing in between: a renewal, not a repeat purchase
        else:
            continue
        year = -g[g.d > today - pd.Timedelta(days=365)].amt.sum()
        prior = -g[(g.d <= today - pd.Timedelta(days=365)) & (g.d > today - pd.Timedelta(days=730))].amt.sum()
        if period == "monthly" and g.d.max() < today - pd.Timedelta(days=60):
            continue  # stopped
        recent = -g.amt.tail(4)
        out.append({
            "account": account, "merchant": merchant, "category": category, "period": period,
            "yearly": year if period == "monthly" else -g.amt.iloc[-1],
            "prior": prior, "latest": -g.amt.iloc[-1], "latest_date": g.d.max(),
            "increase": len(recent) >= 4 and recent.iloc[-1] > recent.iloc[:-1].median() * 1.10
            and recent.iloc[:-1].std() <= recent.iloc[:-1].median() * 0.05,
        })
    return pd.DataFrame(out)


def item_yearly(item: pd.Series, recurring: pd.DataFrame) -> float:
    if RECURRING in item.tags and not recurring.empty:
        r = recurring[(recurring.account == item.account) & (recurring.merchant == item.merchant)]
        if len(r):
            return float(r.yearly.iloc[0])
    return abs(item.amount)


# ── Formatting ───────────────────────────────────────────────────────────────

def money(x: float) -> str:
    return f"${x:,.0f}"


def describe(item: pd.Series, today: pd.Timestamp) -> str:
    note = item.notes.splitlines()[0] if item.notes else "(no note)"
    note = DUE_RE.sub("", note).strip() or "(no note)"
    flags = [t[len(PREFIX):] for t in item.tags if t in (DUE, WAITING, RECURRING)]
    yearly = f"{money(item.yearly)}/yr" if RECURRING in item.tags else money(item.yearly)
    return (f"{item.date:%Y-%m-%d}  {item.merchant[:24]:<24} {yearly:>12}  "
            f"{note[:60]:<60}  {(item.today_age)}d" + (f"  [{', '.join(flags)}]" if flags else ""))


def section(title: str, lines: list[str], out: list[str]) -> None:
    out.append(f"\n{title} ({len(lines)})")
    out.append("-" * len(out[-1].strip()))
    out.extend("  " + line for line in lines) if lines else out.append("  (none)")


# ── Main ─────────────────────────────────────────────────────────────────────

def build(items: pd.DataFrame, hist: pd.DataFrame, args: argparse.Namespace) -> list[str]:
    today = pd.Timestamp(date.today())
    recurring = recurring_costs(hist, today)
    out = [f"Action list — {today:%Y-%m-%d}"]

    if items.empty:
        out.append("\nNo transactions carry an !Act_ tag.")
        items = pd.DataFrame(columns=["id", "date", "amount", "notes", "merchant", "account", "category", "tags"])
    items = items.copy()
    items["due"] = items.notes.map(lambda n: pd.Timestamp(m.group(1)) if (m := DUE_RE.match(n)) else pd.NaT)
    items["since"] = [pd.Timestamp(m.group(1)) if (m := WAITING_RE.search(n)) else d
                      for n, d in zip(items.notes, items.date)]
    items["yearly"] = [item_yearly(r, recurring) for _, r in items.iterrows()]
    items["today_age"] = (today - items.date).dt.days
    is_open = items.tags.map(lambda t: HIGH in t or LOW in t)
    waiting = items.tags.map(lambda t: WAITING in t)

    # 1. Due
    due = items[items.due.notna() & (items.due <= today + pd.Timedelta(days=args.due_days))].sort_values("due")
    section(f"1. OVERDUE / DUE WITHIN {args.due_days} DAYS (any priority)", [
        f"{'OVERDUE ' if r.due < today else ''}due {r.due:%b %d}  " + describe(r, today) for _, r in due.iterrows()
    ], out)
    later = items[items.due > today + pd.Timedelta(days=args.due_days)].sort_values("due")
    if len(later):
        out.append("  later: " + "; ".join(f"{r.merchant} {r.due:%b %d %Y}" for _, r in later.iterrows()))

    # 2-4. High / Waiting / Low
    for title, mask, key, asc in (
        ("2. HIGH — your move", items.tags.map(lambda t: HIGH in t) & ~waiting, "yearly", False),
        ("3. WAITING on someone else", waiting, "since", True),
        ("4. LOW — your move", items.tags.map(lambda t: LOW in t) & ~waiting, "yearly", False),
    ):
        rows = items[mask].sort_values(key, ascending=asc)
        lines = []
        for _, r in rows.iterrows():
            line = describe(r, today)
            if WAITING in r.tags:
                days = (today - r.since).days
                not_yet_due = pd.notna(r.due) and r.due > today
                line += f"  waiting {days}d" + ("  ⚠ nudge" if days > args.nudge_days and not not_yet_due else "")
            lines.append(line)
        section(title, lines, out)

    # 5. Watch list
    tagged = {(a, m) for a, m in zip(items.account, items.merchant)}
    if not recurring.empty:
        recurring["tagged"] = [(a, m) in tagged for a, m in zip(recurring.account, recurring.merchant)]

        def cost_line(r) -> str:
            change = f"{(r.yearly / r.prior - 1):+.0%} vs prior yr" if r.prior > 0 and r.period == "monthly" else ""
            return (f"{money(r.yearly):>9}/yr  {r.merchant[:22]:<22} {r.category[:30]:<30} {r.account[:30]:<30} {r.period:<7} "
                    f"{change:<18}{'★ tagged' if r.tagged else ''}")

        large = recurring[recurring.yearly >= args.threshold].sort_values("yearly", ascending=False)
        small = recurring[recurring.yearly < args.threshold].sort_values("yearly", ascending=False)
        section(f"5a. WATCH — LARGE RECURRING (>= {money(args.threshold)}/yr, top {args.top})",
                [cost_line(r) for _, r in large.head(args.top).iterrows()], out)
        section(f"5b. WATCH — SMALL RECURRING (< {money(args.threshold)}/yr; together {money(small.yearly.sum())}/yr)",
                [cost_line(r) for _, r in small.iterrows()], out)
        up = recurring[recurring.increase]
        section("5c. WATCH — PRICE INCREASES (latest charge >10% above the usual)",
                [f"{r.merchant[:26]:<26} {r.account[:34]:<34} now {money(r.latest)}" for _, r in up.iterrows()], out)

    lines = []
    year = hist[hist.d > today - pd.Timedelta(days=365)]
    prior = hist[(hist.d <= today - pd.Timedelta(days=365)) & (hist.d > today - pd.Timedelta(days=730))]
    for label, pattern in COST_CATEGORIES.items():
        now_total = -year[year.Category.str.contains(pattern) & (year.amt < 0)].amt.sum()
        before = -prior[prior.Category.str.contains(pattern) & (prior.amt < 0)].amt.sum()
        change = f"  ({now_total / before - 1:+.0%} vs prior yr)" if before else ""
        lines.append(f"{money(now_total):>9} last 12 months{change:<24} {label}")
    section("5d. WATCH — COST CATEGORIES", lines, out)

    # 6. Housekeeping
    hk = []
    for _, r in items.iterrows():
        name = f"{r.date:%Y-%m-%d} {r.merchant} {money(abs(r.amount))}"
        priorities = sum(t in r.tags for t in (HIGH, LOW))
        if priorities != 1 and (priorities or any(t in r.tags for t in (DUE, RECURRING))):
            hk.append(f"{name}: needs exactly one of High/Low")
        if (DUE in r.tags) != pd.notna(r.due):
            hk.append(f"{name}: !Act_Due tag and 'Due YYYY-MM-DD:' note disagree")
        if HIGH in r.tags and not r.notes:
            hk.append(f"{name}: High with no note — what is the action?")
        if RECURRING in r.tags:
            newer = hist[(hist.Account == r.account) & (hist.Merchant == r.merchant) & (hist.amt < 0) & (hist.d > r.date)]
            if len(newer):
                hk.append(f"{name}: Recurring tag is on an old charge; latest is {newer.d.max():%Y-%m-%d} (move it)")
        if r.amount < 0 and is_open[_]:
            back = hist[(hist.Account == r.account) & (hist.Merchant == r.merchant) & (hist.d >= r.date)
                        & (hist.amt > 0) & ((hist.amt - abs(r.amount)).abs() <= max(0.01, abs(r.amount) * 0.01))]
            if len(back):
                hk.append(f"{name}: a matching credit arrived {back.d.max():%Y-%m-%d} — done?")
    section("6. HOUSEKEEPING", hk, out)
    return out


def main() -> None:
    args = parse_args()
    items = asyncio.run(fetch_action_items())
    hist = load_history(args.transactions)
    text = "\n".join(build(items, hist, args))
    print(text)
    Path(args.output).expanduser().write_text(text + "\n")
    print(f"\nSaved: {args.output}")


if __name__ == "__main__":
    main()
