import argparse
import sqlite3
from rich.table import Table
from rich.console import Console
from nlp.embeddings import EmbeddingIndexer
import yaml


def load_cfg(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def semantic_search(db: str, cfg_path: str, query: str, k: int = 10):
    cfg = load_cfg(cfg_path)
    sem = cfg.get("semantic", {})
    if not sem.get("enabled"):
        print("Semantic layer disabled in config.")
        return
    indexer = EmbeddingIndexer(sem.get("model_name"), sem.get("faiss_index"))
    results = indexer.search(query, k)
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    table = Table(title=f"Semantic results: '{query}'")
    for col in ["score", "sent_at", "sender_email", "subject", "account_tag"]:
        table.add_column(col)
    for mid, score in results:
        r = con.execute("SELECT sent_at, sender_email, subject, account_tag FROM messages WHERE id=?", (mid,)).fetchone()
        if r:
            table.add_row(f"{score:.3f}", r["sent_at"] or "", r["sender_email"] or "", (r["subject"] or "")[:80], r["account_tag"] or "")
    Console().print(table)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--config", default="config/accounts.yml")
    ap.add_argument("--q", required=True)
    ap.add_argument("--k", type=int, default=10)
    args = ap.parse_args()
    semantic_search(args.db, args.config, args.q, args.k)


if __name__ == "__main__":
    main()
