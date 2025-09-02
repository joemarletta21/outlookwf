import argparse
import email
import email.policy
import hashlib
import json
import os
import platform
import re
import zipfile
import mailbox
import shutil
import subprocess
import sys
from datetime import datetime
from email.header import decode_header, make_header
from email.utils import parsedate_to_datetime
from typing import Optional, Dict, Any, Iterable, Tuple, List

from db.util import connect, upsert_message, insert_attachment, tag_message, add_entities
from nlp.entities import extract_entities
from nlp.embeddings import EmbeddingIndexer
from dateutil import parser as dateparse
from ingest.omx_convert import ingest_outlook_mac_dir


def safe_decode(s: Optional[str]) -> str:
    if not s:
        return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:
        return s


def iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    try:
        if dt.tzinfo:
            return dt.astimezone(datetime.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def compute_external_id(msg: email.message.Message) -> str:
    mid = msg.get("Message-ID") or msg.get("Message-Id") or ""
    if mid:
        return mid.strip()
    # Fallback hash
    base = (msg.get("From", "") + msg.get("Date", "") + msg.get("Subject", "")).encode("utf-8", "ignore")
    return hashlib.sha1(base).hexdigest()


def parse_eml_stream(root: str) -> Iterable[Tuple[email.message.Message, str]]:
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if not fn.lower().endswith((".eml", ".txt")):
                continue
            path = os.path.join(dirpath, fn)
            try:
                with open(path, "rb") as f:
                    msg = email.message_from_binary_file(f, policy=email.policy.default)
                yield msg, path
            except Exception:
                continue


def parse_mbox_stream(root: str) -> Iterable[Tuple[email.message.Message, str]]:
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if not fn.lower().endswith((".mbox",)):
                continue
            path = os.path.join(dirpath, fn)
            try:
                mbox = mailbox.mbox(path)
                for i, msg in enumerate(mbox):
                    yield msg, f"{path}::msg:{i}"
            except Exception:
                continue


def parse_emlx_stream(root: str) -> Iterable[Tuple[email.message.Message, str]]:
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if not fn.lower().endswith((".emlx",)):
                continue
            path = os.path.join(dirpath, fn)
            try:
                with open(path, "rb") as f:
                    raw = f.read()
                try:
                    msg = email.message_from_bytes(raw, policy=email.policy.default)
                except Exception:
                    # Some emlx have a length prefix before the RFC822 payload
                    msg = email.message_from_bytes(raw.split(b"\n", 1)[-1], policy=email.policy.default)
                yield msg, path
            except Exception:
                continue


def unzip_archive(zip_path: str, out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(out_dir)
    return out_dir


def is_zip_archive(path: str) -> bool:
    try:
        return zipfile.is_zipfile(path)
    except Exception:
        return False


def parse_ics_stream(root: str) -> Iterable[Tuple[dict, str]]:
    # Minimal ICS parser (VEVENT only) using line scanning, tolerant of folded lines
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if not fn.lower().endswith((".ics",)):
                continue
            path = os.path.join(dirpath, fn)
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                lines = []
                # Unfold folded lines (RFC 5545): lines starting with space or tab continue previous
                for raw in content.splitlines():
                    if raw.startswith(" ") or raw.startswith("\t"):
                        if lines:
                            lines[-1] += raw.strip()
                    else:
                        lines.append(raw)
                events = []
                cur = None
                for ln in lines:
                    if ln.strip() == "BEGIN:VEVENT":
                        cur = {}
                    elif ln.strip() == "END:VEVENT":
                        if cur is not None:
                            events.append(cur)
                            cur = None
                    elif cur is not None and ":" in ln:
                        k, v = ln.split(":", 1)
                        k = k.split(";", 1)[0].upper()
                        cur[k] = v
                for ev in events:
                    yield ev, path
            except Exception:
                continue


def extract_body(msg: email.message.Message) -> Tuple[str, bool]:
    if msg.is_multipart():
        parts = []
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get_content_disposition())
            if disp == "attachment":
                continue
            if ctype == "text/plain":
                try:
                    parts.append(part.get_content())
                except Exception:
                    pass
        return ("\n\n".join(parts), True)
    else:
        try:
            return (msg.get_content(), False)
        except Exception:
            return ("", False)


def readpst_available() -> bool:
    return shutil.which("readpst") is not None


def run_readpst(pst_path: str, out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    abspath = os.path.abspath(pst_path)
    if not os.path.exists(abspath):
        raise FileNotFoundError(f"PST not found: {abspath}")
    cmd = [
        "readpst",
        "-D",   # preserve directory structure
        "-r",   # recursive
        "-e",   # EML output
        "-o", out_dir,
        abspath,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        msg = (
            "readpst failed. stdout:\n" + (e.stdout or "") +
            "\nstderr:\n" + (e.stderr or "") +
            "\nHints: Ensure the file is a valid .pst (not .olm), not password-protected, and not in use. "
            "If possible, try the pypff path (install pypff) or export a fresh PST."
        )
        raise RuntimeError(msg)
    return out_dir


def load_config_accounts(cfg_path: str = "config/accounts.yml") -> Dict[str, Any]:
    import yaml
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def preflight_source(path: str) -> None:
    abspath = os.path.abspath(path)
    if not os.path.exists(abspath):
        raise FileNotFoundError(f"File not found: {abspath}")
    # Best-effort type hint using 'file' if available
    tool = shutil.which("file")
    if tool:
        try:
            out = subprocess.check_output([tool, "-b", abspath], text=True).strip()
            low = out.lower()
            if "olm" in low:
                raise RuntimeError(
                    f"The provided file appears to be an Outlook for Mac OLM export, not a PST. 'file' says: {out}. "
                    "Re-export as a PST from Outlook on Windows, or convert OLM->PST locally, then retry."
                )
            if "composite document file" in low and "microsoft outlook" not in low:
                # Still likely PST, but let readpst try.
                pass
        except subprocess.CalledProcessError:
            pass


def tag_from_config(cfg: Dict[str, Any], sender_email: str, recipients: List[str], subject: str, body: str) -> Tuple[Optional[str], List[str], List[Tuple[str, str]]]:
    overrides = cfg.get("overrides", {})
    addresses = overrides.get("addresses", {})
    subject_patterns = overrides.get("subject_patterns", [])
    if sender_email in addresses:
        t = addresses[sender_email]
        return t, [], [(t, "account")]

    for sp in subject_patterns:
        pat = sp.get("pattern")
        acc = sp.get("account")
        if pat and acc and re.search(pat, subject or ""):
            return acc, [], [(acc, "account")]

    accounts = cfg.get("accounts", [])
    primary = None
    partners: List[str] = []
    tags: List[Tuple[str, str]] = []
    text = f"{subject}\n{body}".lower()
    recips_lower = [r.lower() for r in recipients]

    for acc in accounts:
        name = acc.get("name")
        aliases = [a.lower() for a in acc.get("aliases", [])]
        domains = [d.lower() for d in acc.get("domains", [])]
        keywords = [k.lower() for k in acc.get("keywords", [])]
        partners_cfg = acc.get("partners", [])

        score = 0
        if any(a in text for a in aliases):
            score += 1
        if any(k in text for k in keywords):
            score += 1
        if any(('@' + d) in sender_email.lower() for d in domains) or any(('@' + d) in r for d in domains for r in recips_lower):
            score += 2
        if score >= 2 and not primary:
            primary = name
            tags.append((name, "account"))
        for p in partners_cfg:
            if p.lower() in text:
                partners.append(p)
                tags.append((p, "partner"))

    return primary, partners, tags


def iter_pypff_messages(pst_path: str) -> Iterable[Tuple[Dict[str, Any], List[Tuple[str, bytes]]]]:
    import pypff

    def walk(folder):
        for i in range(folder.number_of_sub_messages):
            yield folder.get_sub_message(i)
        for j in range(folder.number_of_sub_folders):
            yield from walk(folder.get_sub_folder(j))

    pf = pypff.file()
    pf.open(pst_path)
    root = pf.get_root_folder()

    for item in walk(root):
        try:
            headers = item.get_transport_headers() or ""
            raw = (headers + "\n\n" + (item.plain_text_body or "")).encode("utf-8", "ignore")
            msg = email.message_from_bytes(raw, policy=email.policy.default)
            # Attachments
            atts: List[Tuple[str, bytes]] = []
            for a in range(item.number_of_attachments):
                att = item.get_attachment(a)
                try:
                    atts.append((att.get_filename() or "attachment", att.read_buffer(att.get_size())))
                except Exception:
                    continue
            yield {"msg": msg, "folder": item.get_parent_folder().name if item.get_parent_folder() else None}, atts
        except Exception:
            continue


def process_message(conn, cfg, msg: email.message.Message, folder: Optional[str]):
    body, _ = extract_body(msg)
    sender = email.utils.parseaddr(msg.get("From", ""))[1]
    tos = ";".join([email.utils.parseaddr(x)[1] for x in msg.get_all("To", [])])
    ccs = ";".join([email.utils.parseaddr(x)[1] for x in msg.get_all("Cc", [])])
    bccs = ";".join([email.utils.parseaddr(x)[1] for x in msg.get_all("Bcc", [])])
    recipients = []
    if tos:
        recipients.extend(tos.split(";"))
    if ccs:
        recipients.extend(ccs.split(";"))
    if bccs:
        recipients.extend(bccs.split(";"))

    subject = safe_decode(msg.get("Subject", ""))
    date_hdr = msg.get("Date")
    sent = iso(parsedate_to_datetime(date_hdr)) if date_hdr else None
    recvd = sent
    external_id = compute_external_id(msg)

    account_tag, partner_tags, tags = tag_from_config(cfg, sender, recipients, subject or "", body or "")

    rec = {
        "external_id": external_id,
        "thread_id": msg.get("Thread-Index") or msg.get("Thread-Topic"),
        "folder": folder,
        "sender_name": safe_decode(msg.get("From")),
        "sender_email": sender,
        "recipients_to": tos,
        "recipients_cc": ccs,
        "recipients_bcc": bccs,
        "subject": subject,
        "body": body,
        "sent_at": sent,
        "received_at": recvd,
        "is_read": 0,
        "has_attachments": 0,
        "account_tag": account_tag,
        "partner_tags": ";".join(partner_tags) if partner_tags else None,
        "raw_headers": None,
    }
    mid = upsert_message(conn, rec)

    ents = extract_entities(body or "")
    if ents:
        add_entities(conn, mid, ents)

    if tags:
        tag_message(conn, mid, tags)

    return mid


def ingest_with_readpst(pst_path: str, conn, cfg: Dict[str, Any], checkpoint: str) -> None:
    out_dir = os.path.join("data", "readpst_out")
    run_readpst(pst_path, out_dir)
    state = _load_state(checkpoint)
    done = state.get("processed", {})
    embedder = _maybe_embedder(cfg)
    count = 0
    for msg, path in parse_eml_stream(out_dir):
        if done.get(path):
            continue
        mid = process_message(conn, cfg, msg, folder=None)
        if embedder:
            try:
                embedder.add(mid, msg.get("Subject", ""), extract_body(msg)[0])
            except Exception:
                pass
        _save_progress(checkpoint, path, mid)
        count += 1
        if count % 500 == 0:
            print(f"Processed {count} messages...")
    if embedder:
        embedder.flush()
    # Finalize
    try:
        conn.commit()
    except Exception:
        pass
    # ICS files -> events
    created = 0
    for ev, path in parse_ics_stream(root_dir):
        if done.get(path):
            continue
        title = ev.get("SUMMARY")
        loc = ev.get("LOCATION")
        starts_at = None
        ends_at = None
        try:
            if ev.get("DTSTART"):
                starts_at = iso(dateparse.parse(ev.get("DTSTART")))
            if ev.get("DTEND"):
                ends_at = iso(dateparse.parse(ev.get("DTEND")))
        except Exception:
            pass
        insert_event(conn, None, "calendar", title, starts_at, ends_at, loc, None)
        _save_progress(checkpoint, path, -1)
        created += 1
        if created % 1000 == 0:
            print(f"Parsed {created} ICS events...")
    if created:
        print(f"Created {created} events from ICS files.")


def ingest_from_eml_dir(root_dir: str, conn, cfg: Dict[str, Any], checkpoint: str) -> None:
    state = _load_state(checkpoint)
    done = state.get("processed", {})
    embedder = _maybe_embedder(cfg)
    count = 0
    # EML files
    for msg, path in parse_eml_stream(root_dir):
        if done.get(path):
            continue
        mid = process_message(conn, cfg, msg, folder=None)
        if embedder:
            try:
                embedder.add(mid, msg.get("Subject", ""), extract_body(msg)[0])
            except Exception:
                pass
        _save_progress(checkpoint, path, mid)
        count += 1
        if count % 500 == 0:
            print(f"Processed {count} messages...")
    if embedder:
        embedder.flush()
    try:
        conn.commit()
    except Exception:
        pass
    # MBOX files
    for msg, path in parse_mbox_stream(root_dir):
        if done.get(path):
            continue
        mid = process_message(conn, cfg, msg, folder=None)
        if embedder:
            try:
                embedder.add(mid, msg.get("Subject", ""), extract_body(msg)[0])
            except Exception:
                pass
        _save_progress(checkpoint, path, mid)
    if embedder:
        embedder.flush()
    try:
        conn.commit()
    except Exception:
        pass
    # EMLX files (Apple Mail)
    for msg, path in parse_emlx_stream(root_dir):
        if done.get(path):
            continue
        mid = process_message(conn, cfg, msg, folder=None)
        if embedder:
            try:
                embedder.add(mid, msg.get("Subject", ""), extract_body(msg)[0])
            except Exception:
                pass
        _save_progress(checkpoint, path, mid)
    if embedder:
        embedder.flush()


def ingest_with_pypff(pst_path: str, conn, cfg: Dict[str, Any], checkpoint: str) -> None:
    state = _load_state(checkpoint)
    seen_ids = set(state.get("external_ids", []))
    embedder = _maybe_embedder(cfg)
    count = 0
    for payload, atts in iter_pypff_messages(pst_path):
        msg = payload["msg"]
        folder = payload.get("folder")
        external_id = compute_external_id(msg)
        if external_id in seen_ids:
            continue
        mid = process_message(conn, cfg, msg, folder)
        # attachments
        for fn, data in atts:
            insert_attachment(conn, mid, fn, None, len(data) if data else 0, None)
        if embedder:
            try:
                embedder.add(mid, msg.get("Subject", ""), extract_body(msg)[0])
            except Exception:
                pass
        seen_ids.add(external_id)
        _save_state(checkpoint, {"external_ids": list(seen_ids)})
        count += 1
        if count % 500 == 0:
            print(f"Processed {count} messages...")
    if embedder:
        embedder.flush()


def _load_state(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_progress(path: str, item_key: str, message_id: int) -> None:
    st = _load_state(path)
    st.setdefault("processed", {})[item_key] = message_id
    _save_state(path, st)


def _save_state(path: str, st: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser(description="Stream PST into SQLite with tagging and entities")
    ap.add_argument("--pst", required=True, help="Path to PST file")
    ap.add_argument("--db", required=True, help="Path to SQLite DB")
    ap.add_argument("--checkpoint", required=True, help="Path to checkpoint state JSON")
    ap.add_argument("--config", default="config/accounts.yml", help="Accounts config")
    args = ap.parse_args()

    cfg = load_config_accounts(args.config)
    conn = connect(args.db)
    try:
        preflight_source(args.pst)
    except Exception as e:
        print(f"Preflight check failed: {e}", file=sys.stderr)
        sys.exit(2)

    # Try pypff first if importable on any OS; fallback to readpst
    use_pypff = False
    try:
        import pypff  # type: ignore  # noqa: F401
        use_pypff = True
    except Exception:
        use_pypff = False

    # If the provided file is a Zip archive (Outlook export often), unzip and parse EML
    if is_zip_archive(args.pst):
        print("Detected Zip archive. Unzipping and parsing EML...")
        eml_root = unzip_archive(args.pst, os.path.join('data', 'eml_unzip'))
        # First, try EML/MBOX/EMLX
        ingest_from_eml_dir(eml_root, conn, cfg, args.checkpoint)
        # If no messages were created, attempt Outlook Mac XML conversion
        try:
            cur = conn.execute("SELECT COUNT(*) FROM messages")
            n = int(cur.fetchone()[0])
        except Exception:
            n = 0
        if n == 0:
            print("No EML/MBOX found. Attempting Outlook Mac XML conversion...")
            created = ingest_outlook_mac_dir(eml_root, conn, cfg, args.checkpoint)
            print(f"Converted {created} messages from Outlook Mac XML.")
    elif use_pypff:
        print("Using pypff path.")
        ingest_with_pypff(args.pst, conn, cfg, args.checkpoint)
    else:
        if not readpst_available():
            print("readpst not found. Please install libpst (readpst) or provide a Zip of EML/mbox exports.", file=sys.stderr)
            sys.exit(2)
        print("Using readpst path (cross-platform).")
        ingest_with_readpst(args.pst, conn, cfg, args.checkpoint)

    print("Ingest completed.")


def _maybe_embedder(cfg: Dict[str, Any]) -> Optional[EmbeddingIndexer]:
    sem = (cfg or {}).get("semantic", {})
    if not sem or not sem.get("enabled"):
        return None
    model = sem.get("model_name", "sentence-transformers/all-MiniLM-L6-v2")
    index_path = sem.get("faiss_index", "data/semantic/faiss.index")
    os.makedirs(os.path.dirname(index_path), exist_ok=True)
    try:
        return EmbeddingIndexer(model, index_path)
    except Exception as e:
        print(f"Semantic layer disabled due to error: {e}")
        return None


if __name__ == "__main__":
    main()
