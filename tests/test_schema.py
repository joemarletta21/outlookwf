import os
import sqlite3
from db.util import init_db


def test_init_schema(tmp_path):
    db = tmp_path / "test.db"
    init_db(str(db))
    con = sqlite3.connect(db)
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    names = {r[0] for r in cur.fetchall()}
    assert {"messages", "attachments", "events", "entities", "account_tags"}.issubset(names)
