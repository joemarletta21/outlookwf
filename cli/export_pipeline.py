import argparse
import csv
import os
import sqlite3


def export_tables(db: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    for tbl in ["messages", "attachments", "events", "entities", "account_tags"]:
        rows = con.execute(f"SELECT * FROM {tbl}").fetchall()
        out = os.path.join(out_dir, f"{tbl}.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            if rows:
                w = csv.DictWriter(f, fieldnames=rows[0].keys())
                w.writeheader()
                for r in rows:
                    w.writerow(dict(r))
            else:
                f.write("")
        print(f"Exported {tbl} -> {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    export_tables(args.db, args.out)


if __name__ == "__main__":
    main()
