import argparse
import sqlite3
from rich.table import Table
from rich.console import Console


def search(db: str, query: str, limit: int = 50):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    sql = (
        "SELECT id, sent_at, sender_email, subject, account_tag FROM messages "
        "WHERE (subject LIKE ? OR body LIKE ?) "
        "ORDER BY sent_at DESC LIMIT ?"
    )
    like = f"%{query}%"
    rows = con.execute(sql, (like, like, limit)).fetchall()
    tbl = Table(title=f"Search: {query}")
    for col in ["id", "sent_at", "sender_email", "account_tag", "subject"]:
        tbl.add_column(col)
    for r in rows:
        tbl.add_row(str(r["id"] or ""), r["sent_at"] or "", r["sender_email"] or "", r["account_tag"] or "", (r["subject"] or "")[:80])
    Console().print(tbl)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--q", required=True)
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()
    search(args.db, args.q, args.limit)


if __name__ == "__main__":
    main()
