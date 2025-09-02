Outlook PST Local Pipeline
==========================

This repository provides a fully local pipeline for extracting, normalizing, tagging, and analyzing Outlook `.pst` archives for account planning and compliance workflows.

- Local-only processing. No external uploads.
- Streamed PST parsing with resume checkpoints.
- Dual ingest paths: `pypff` (Windows) and `readpst` (any OS).
- SQLite by default (DuckDB optional) with indexes.
- CLI tools for fast search, compliance timeline, and account dossiers.
- Optional semantic layer (FAISS embeddings) via config toggle.

Requirements
------------
- Python 3.10+
- Disk space to materialize `readpst` EML output (cross-platform path)
- Optional: `wkhtmltopdf` on PATH for PDF export

Setup – macOS
-------------
1) Install `libpst` (`readpst`):
   brew install libpst
2) Create a venv and install deps:
   python -m venv .venv
   . .venv/bin/activate
   pip install -r requirements.txt
3) Initialize the database:
   python -m db.util --init --db data/pst.db
4) Place your PST locally, e.g., `Outlook for Mac Archive.pst`.
5) Ingest (auto-selects `readpst` path on macOS):
   python -m ingest.pst_extract --pst "Outlook for Mac Archive.pst" --db data/pst.db --checkpoint data/checkpoints/state.json

Setup – Windows
---------------
1) Install Python 3.10+ and ensure `pip` works.
2) Install `pypff` package (prebuilt wheel preferred):
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   # If `pypff` fails to install, install prerequisites per its docs.
3) Initialize the database:
   python -m db.util --init --db data\pst.db
4) Ingest (auto-selects `pypff` on Windows if available):
   python -m ingest.pst_extract --pst "Outlook for Mac Archive.pst" --db data\pst.db --checkpoint data\checkpoints\state.json

Setup – Linux
-------------
1) Install `libpst`:
   sudo apt-get update && sudo apt-get install -y pst-utils
2) Create venv and install deps:
   python -m venv .venv
   . .venv/bin/activate
   pip install -r requirements.txt
3) Initialize DB and ingest using `readpst` path (same as macOS).

Configuration
-------------
- Edit `config/accounts.yml` to define accounts, partners, keyword/alias heuristics, and manual overrides. The tagging engine prefers overrides and domains > keywords.
- The optional semantic layer is disabled by default. Set `semantic.enabled: true` to enable. It uses local embeddings + FAISS. No external calls.

Usage
-----
Initialize DB:
  python -m db.util --init --db data/pst.db

Ingest PST or Outlook-exported Zip:
  # PST file path (Windows export)
  python -m ingest.pst_extract --pst "Outlook for Mac Archive.pst" --db data/pst.db --checkpoint data/checkpoints/state.json

  # Zip containing EML/mbox (common on macOS exports)
  python -m ingest.pst_extract --pst "Export.zip" --db data/pst.db --checkpoint data/checkpoints/state.json

Apple Mail Export (mbox/emlx) on macOS
--------------------------------------
- In Apple Mail, select a mailbox (folder) and choose Mailbox > Export Mailbox… to create an .mbox. Repeat per folder if needed.
- Alternatively, export messages as .eml/.emlx if available.
- Zip the exported folder(s) if large, or point ingest directly at the Zip.
- The pipeline parses .mbox, .eml, and .emlx recursively from a folder or a Zip.

Calendar .ics Parsing
---------------------
- If your export contains .ics files, the pipeline now parses VEVENTs and stores them in the `events` table (no message_id linkage).
- Fields captured: `SUMMARY` (title), `LOCATION`, `DTSTART`, `DTEND`.

Search:
  python -m cli.search --db data/pst.db --q "invoice AND (Acme OR Contoso)"

Account Dossier:
  python -m cli.make_dossier --db data/pst.db --account "Acme Corp" --out reports/acme_dossier.html

Compliance Timeline:
  python -m cli.compliance_timeline --db data/pst.db --account "Acme Corp" --out reports/acme_compliance.html

CSV Exports:
  python -m cli.export_pipeline --db data/pst.db --out exports/

Semantic Layer (Optional)
-------------------------
Enable in `config/accounts.yml`:

  semantic:
    enabled: true
    model_name: sentence-transformers/all-MiniLM-L6-v2
    faiss_index: data/semantic/faiss.index

Notes:
- Ensure the model is available locally (first run may download; keep network off if you already have it cached). No message content leaves your machine.
- Embeddings are computed on 2k-char excerpts (subject + body head) during ingest and added to a FAISS inner-product index with normalized vectors (cosine similarity).

Semantic search:
  python -m cli.semantic_search --db data/pst.db --config config/accounts.yml --q "contract renewal with Acme"

Notes on Scale and Performance
------------------------------
- Ingest streams one message at a time to avoid loading the entire PST into memory.
- Checkpointing allows safe resume if the process is interrupted.
- SQLite uses WAL + covering indexes to keep queries responsive.
- Batch writes are applied via transactions; attachments can be stored without blob content to limit DB size.

Testing
-------
Run unit tests:
  pytest -q

Security and PII
----------------
- All processing is local. Keep CSV/PDF exports in a secure location.
- Do not upload raw data or outputs.
- Troubleshooting: Outlook for Mac typically exports .olm files or a .zip of EML/mbox, not .pst. The `readpst` tool only accepts true PST files. If you see errors like "Opening PST file and indexes... Error opening File", verify the file type:
  - file "Outlook for Mac Archive.pst"
  - If it reports Zip, re-run ingest with that Zip path; the pipeline will unzip and parse EML recursively.
  - If it reports OLM, export a PST from Outlook on Windows (Import OLM, then Export to PST), or use any local OLM→PST conversion tool. Keep everything local.
