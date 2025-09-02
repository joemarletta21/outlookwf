import os
import re
import json
import hashlib
from typing import Dict, Any, Iterable, Tuple, Optional, List
from xml.etree import ElementTree as ET

from db.util import upsert_message, insert_attachment

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _text(n: Optional[ET.Element]) -> str:
    if n is None:
        return ""
    return (n.text or "").strip()


def _flatten_xml(root: ET.Element) -> Dict[str, List[str]]:
    flat: Dict[str, List[str]] = {}
    for elem in root.iter():
        key = elem.tag.split('}')[-1].lower()
        val = (elem.text or '').strip()
        if not val:
            continue
        flat.setdefault(key, []).append(val)
    return flat


def _pick(flat: Dict[str, List[str]], keys: List[str]) -> Optional[str]:
    for k in keys:
        vals = flat.get(k)
        if vals:
            return vals[0]
    return None


def _collect_emails(flat: Dict[str, List[str]], keys: List[str]) -> str:
    out: List[str] = []
    for k in keys:
        for v in flat.get(k, []):
            out.extend(EMAIL_RE.findall(v))
    # de-dupe preserving order
    seen = set()
    uniq = []
    for e in out:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return ";".join(uniq)


def parse_message_xml(path: str) -> Optional[Dict[str, Any]]:
    try:
        tree = ET.parse(path)
        root = tree.getroot()
    except Exception:
        return None
    flat = _flatten_xml(root)

    subject = _pick(flat, ["subject", "mssubject", "itemsubject", "title", "opfmessagecopysubject"]) or None
    body = _pick(flat, ["body", "textbody", "plaintext", "preview", "bodypreview", "content", "opfmessagecopybody"]) or None
    sent = _pick(flat, ["datesent", "datetimesent", "sent", "date", "receivedtime", "opfmessagecopyreceivedtime", "opfmessagecopysenttime"]) or None
    sender_block = _pick(flat, ["from", "sender", "fromname", "fromemailaddress", "opfmessagecopysenderaddress"]) or ""
    sender_email = None
    m = EMAIL_RE.search(sender_block)
    if m:
        sender_email = m.group(0)
    if not sender_email:
        # try attributes like <emailAddress OPFContactEmailAddressAddress="...">
        try:
            tree = ET.parse(path)
            for el in tree.iter():
                if el.tag.split('}')[-1].lower() == 'emailaddress':
                    for attr_val in el.attrib.values():
                        m = EMAIL_RE.search(attr_val)
                        if m:
                            sender_email = m.group(0)
                            break
                if sender_email:
                    break
        except Exception:
            pass
    if not sender_email:
        # final fallback: any email-like text in doc
        for k, vals in flat.items():
            for v in vals:
                m = EMAIL_RE.search(v)
                if m:
                    sender_email = m.group(0)
                    break
            if sender_email:
                break

    tos = _collect_emails(flat, ["to", "torecipients", "recipient", "toaddresses", "toemailaddress"])
    ccs = _collect_emails(flat, ["cc", "ccrecipients", "ccaddresses", "ccemailaddress"])
    bccs = _collect_emails(flat, ["bcc", "bccrecipients", "bccaddresses", "bccemailaddress"])

    if not subject and not body:
        return None

    rec = {
        "external_id": hashlib.sha1((path + (subject or "")).encode("utf-8", "ignore")).hexdigest(),
        "thread_id": None,
        "folder": None,
        "sender_name": sender_email,
        "sender_email": sender_email,
        "recipients_to": tos or None,
        "recipients_cc": ccs or None,
        "recipients_bcc": bccs or None,
        "subject": subject,
        "body": body,
        "sent_at": sent,
        "received_at": sent,
        "is_read": 0,
        "has_attachments": 0,
        "account_tag": None,
        "partner_tags": None,
        "raw_headers": None,
    }
    return rec


def _decode_bytes(b: bytes) -> str:
    if not b:
        return ""
    try:
        return b.decode("utf-8")
    except Exception:
        try:
            import chardet  # type: ignore
            enc = chardet.detect(b).get("encoding") or "latin-1"
            return b.decode(enc, "ignore")
        except Exception:
            return b.decode("latin-1", "ignore")


def _collect_body_from_parts(xml_path: str, max_chars: int = 10000) -> str:
    # Heuristic: concatenate readable text from sibling part files (.com_000*, .html, .htm, .rtf)
    body_parts: List[str] = []
    d = os.path.dirname(xml_path)
    try:
        for fn in sorted(os.listdir(d)):
            fl = fn.lower()
            if fl.endswith(('.com_0000', '.com_0001', '.com_0002', '.com_0003', '.com_0004', '.com_0005', '.com_0006', '.com_0007', '.com_0008', '.com_0009', '.com_0010')) or fl.endswith(('.html', '.htm', '.rtf', '.txt')):
                p = os.path.join(d, fn)
                if not os.path.isfile(p):
                    continue
                try:
                    with open(p, 'rb') as f:
                        raw = f.read(200000)  # 200KB per part cap
                    text = _decode_bytes(raw)
                    # If looks like HTML, strip tags to text
                    if '<html' in text.lower() or '</p>' in text.lower():
                        try:
                            from bs4 import BeautifulSoup  # type: ignore
                            text = BeautifulSoup(text, 'html.parser').get_text('\n')
                        except Exception:
                            pass
                    body_parts.append(text)
                    if sum(len(x) for x in body_parts) >= max_chars:
                        break
                except Exception:
                    continue
    except Exception:
        return ""
    body = "\n\n".join([t.strip() for t in body_parts if t and t.strip()])
    return body[:max_chars]


def _find_attachment_candidates(xml_path: str, limit: int = 10) -> List[Tuple[str, int]]:
    # Heuristic: look for a sibling directory named 'com.microsoft.__Attachments' and collect files
    out: List[Tuple[str, int]] = []
    base = os.path.dirname(xml_path)
    att_dir = os.path.join(base, 'com.microsoft.__Attachments')
    if os.path.isdir(att_dir):
        try:
            for fn in sorted(os.listdir(att_dir))[:limit]:
                p = os.path.join(att_dir, fn)
                if os.path.isfile(p) and not fn.lower().endswith('.xml'):
                    try:
                        sz = os.path.getsize(p)
                    except Exception:
                        sz = 0
                    out.append((fn, sz))
        except Exception:
            pass
    return out


def ingest_outlook_mac_dir(root_dir: str, conn, cfg: Dict[str, Any], checkpoint_path: Optional[str]) -> int:
    state = {}
    processed = set()
    if checkpoint_path and os.path.exists(checkpoint_path):
        try:
            state = json.load(open(checkpoint_path, 'r', encoding='utf-8'))
            processed = set((state.get('omx_processed') or {}).keys())
        except Exception:
            state = {}

    count = 0
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if not fn.lower().endswith('.xml'):
                continue
            if fn.lower() in ("categories.xml",):
                continue
            path = os.path.join(dirpath, fn)
            if path in processed:
                continue
            rec = parse_message_xml(path)
            if not rec:
                continue
            # If body is empty, try to reconstruct from sibling parts
            if not rec.get('body'):
                reconstructed = _collect_body_from_parts(path)
                if reconstructed:
                    rec['body'] = reconstructed
            mid = upsert_message(conn, rec)
            # Attachments (names/sizes only)
            for fn, sz in _find_attachment_candidates(path):
                try:
                    insert_attachment(conn, mid, fn, None, sz, None)
                except Exception:
                    pass
            # minimal tagging/partners can be applied later; for now rely on post-pass or search
            if checkpoint_path:
                state.setdefault('omx_processed', {})[path] = mid
                tmp = checkpoint_path + '.tmp'
                os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(state, f)
                os.replace(tmp, checkpoint_path)
            count += 1
            if count % 1000 == 0:
                try:
                    conn.commit()
                except Exception:
                    pass
    try:
        conn.commit()
    except Exception:
        pass
    return count
