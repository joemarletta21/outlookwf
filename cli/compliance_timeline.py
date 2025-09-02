import argparse
import os
import sqlite3
from jinja2 import Environment, FileSystemLoader, select_autoescape


def build_timeline(db: str, account: str):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT id, subject, sender_email, sent_at FROM messages WHERE account_tag=? ORDER BY sent_at",
        (account,),
    ).fetchall()
    return rows


def render_timeline(db: str, account: str, out_path: str):
    rows = build_timeline(db, account)
    env = Environment(
        loader=FileSystemLoader("reports/templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tpl = env.get_template("compliance_timeline.html")
    html = tpl.render(account=account, items=rows)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--account", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    out = render_timeline(args.db, args.account, args.out)
    print(f"Wrote compliance timeline to {out}")


if __name__ == "__main__":
    main()
