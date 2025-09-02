import re
from typing import List, Dict

_RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_RE_DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4})\b")
_RE_MONEY = re.compile(r"\$\s?\d{1,3}(,\d{3})*(\.\d{2})?\b")


def extract_entities(text: str) -> List[Dict]:
    ents: List[Dict] = []
    if not text:
        return ents

    try:
        import spacy  # type: ignore
        try:
            nlp = spacy.load("en_core_web_sm")
        except Exception:
            nlp = None
        if nlp is not None:
            doc = nlp(text)
            for e in doc.ents:
                ents.append({
                    "label": e.label_,
                    "text": e.text,
                    "start_char": int(e.start_char),
                    "end_char": int(e.end_char),
                })
    except Exception:
        pass

    # Fallback regexes
    for m in _RE_EMAIL.finditer(text):
        ents.append({"label": "EMAIL", "text": m.group(0), "start_char": m.start(), "end_char": m.end()})
    for m in _RE_DATE.finditer(text):
        ents.append({"label": "DATE", "text": m.group(0), "start_char": m.start(), "end_char": m.end()})
    for m in _RE_MONEY.finditer(text):
        ents.append({"label": "MONEY", "text": m.group(0), "start_char": m.start(), "end_char": m.end()})

    return ents
