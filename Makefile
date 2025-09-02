PY=python
PIP=pip
DB?=data/pst.db
PST?=Outlook for Mac Archive.pst

.PHONY: venv install init ingest search dossier compliance export test

venv:
	$(PY) -m venv .venv

install:
	. .venv/bin/activate && $(PIP) install -r requirements.txt

init:
	$(PY) -m db.util --init --db $(DB)

ingest:
	$(PY) -m ingest.pst_extract --pst "$(PST)" --db $(DB) --checkpoint data/checkpoints/state.json

search:
	$(PY) -m cli.search --db $(DB) --q "invoice"

dossier:
	$(PY) -m cli.make_dossier --db $(DB) --account "Acme" --out reports/acme_dossier.html

compliance:
	$(PY) -m cli.compliance_timeline --db $(DB) --account "Acme" --out reports/acme_compliance.html

export:
	$(PY) -m cli.export_pipeline --db $(DB) --out exports/

test:
	pytest -q
