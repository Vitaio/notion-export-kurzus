import os
import io
import re
import csv
import unicodedata
import zipfile
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

import streamlit as st
from notion_client import Client

# ────────────────────────────────────────────────────────────────────────────────
# Alapbeállítások
# ────────────────────────────────────────────────────────────────────────────────
APP_TITLE = "Notion → CSV Export – Kurzus"
DEFAULT_GROUP_PROP = os.getenv("NOTION_PROPERTY_NAME", st.secrets.get("NOTION_PROPERTY_NAME", "Kurzus"))
MAX_CONTENT_CHARS = int(os.getenv("MAX_CONTENT_CHARS", st.secrets.get("MAX_CONTENT_CHARS", 40000)))

NOTION_API_KEY = os.getenv("NOTION_API_KEY", st.secrets.get("NOTION_API_KEY", ""))
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", st.secrets.get("NOTION_DATABASE_ID", ""))
APP_PASSWORD = os.getenv("APP_PASSWORD", st.secrets.get("APP_PASSWORD", ""))

CSV_FIELDNAMES = ["kurzus", "sorszám", "név", "típus", "tartalom"]

# Csak EZEKET a H2-ket figyeljük (pontos egyezés, ékezetekkel!)
VIDEO_HEADING = "Videó szöveg"
LESSON_HEADING = "Lecke szöveg"

# ────────────────────────────────────────────────────────────────────────────────
# Unicode normalizálás / mojibake-javítás
# ────────────────────────────────────────────────────────────────────────────────
def _normalize_unicode(s: str) -> str:
    if s is None:
        return ""
    try:
        return unicodedata.normalize("NFC", str(s))
    except Exception:
        return str(s)

def _maybe_fix_mojibake(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    if ("Ã" in s) or ("Â" in s) or ("�" in s):
        try:
            fixed = s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
            def score(txt: str) -> int:
                bad = txt.count("�")
                accents = sum(txt.count(ch) for ch in "áéíóöőúüűÁÉÍÓÖŐÚÜŰ")
                return accents - bad
            if score(fixed) >= score(s):
                s = fixed
        except Exception:
            pass
    return _normalize_unicode(s)

def _norm_csv_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        kk = _maybe_fix_mojibake(k) if isinstance(k, str) else k
        out[kk] = _maybe_fix_mojibake(v) if isinstance(v, str) else v
    return out

# ────────────────────────────────────────────────────────────────────────────────
# Segédfüggvények
# ────────────────────────────────────────────────────────────────────────────────
def _slug(s: str) -> str:
    s = _maybe_fix_mojibake(s).strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s-]+", "_", s)
    return s[:80] if len(s) > 80 else s

def _zip_utf8(files: List[Tuple[str, bytes]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for arcname, data in files:
            arcname = _maybe_fix_mojibake(arcname).replace("\\", "/")
            zi = zipfile.ZipInfo(arcname)
            zi.flag_bits |= 0x800  # UTF-8 filename flag
            zf.writestr(zi, data, compress_type=zipfile.ZIP_DEFLATED)
    buf.seek(0)
    return buf.read()

def _split_content_for_csv(text: str, max_len: int) -> Dict[str, str]:
    text = _maybe_fix_mojibake(text)
    if not text:
        return {"tartalom": ""}
    if len(text) <= max_len:
        return {"tartalom": text}

    parts: List[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_len)
        window = text[start:end]
        cut = window.rfind("\n\n")
        if cut >= int(max_len * 0.6):
            end = start + cut
        part = text[start:end].rstrip()
        if part:
            parts.append(part)
        start = end

    out: Dict[str, str] = {}
    for i, p in enumerate(parts):
        out["tartalom" if i == 0 else f"tartalom_cont_{i}"] = p
    return out

def _fix_numbered_lists(md: str) -> str:
    lines = md.splitlines()
    n = 1
    for i, line in enumerate(lines):
        if re.match(r"^\s*\d+\.\s", line):
            lines[i] = re.sub(r"^\s*\d+\.\s", f"{n}. ", line)
            n += 1
        elif line.strip() == "":
            n = 1
    return "\n".join(lines)

# ────────────────────────────────────────────────────────────────────────────────
# Notion → Markdown
# ────────────────────────────────────────────────────────────────────────────────
def blocks_to_md(client: Client, block_id: str) -> str:
    """
    Bejárja a teljes blokkfát (rekurzívan), és egyszerű Markdownná alakítja.
    Kezeli a beágyazott (oszlopok, toggle, callout, synced_block stb.) struktúrákat is,
    így a belső H2/H3 címsorok is bekerülnek a kimenetbe.
    """
    def rt(payload: Dict[str, Any]) -> str:
        return "".join([r.get("plain_text", "") for r in payload.get("rich_text", [])])

    def walk(bid: str, acc: List[str]):
        cursor = None
        while True:
            resp = client.blocks.children.list(block_id=bid, start_cursor=cursor) if cursor else client.blocks.children.list(block_id=bid)
            for blk in resp.get("results", []):
                t = blk.get("type")
                payload = blk.get(t, {}) if t else {}
                has_children = blk.get("has_children", False)

                line = None
                text = ""
                if isinstance(payload, dict) and "rich_text" in payload:
                    text = _maybe_fix_mojibake(rt(payload))

                if t == "paragraph":
                    line = text
                elif t == "heading_1":
                    line = "# " + text
                elif t == "heading_2":
                    line = "## " + text
                elif t == "heading_3":
                    line = "### " + text
                elif t == "bulleted_list_item":
                    line = "- " + text
                elif t == "numbered_list_item":
                    line = "1. " + text  # később normalizáljuk
                elif t == "quote":
                    line = "> " + text
                elif t == "to_do":
                    checked = payload.get("checked", False)
                    line = f"- [{'x' if checked else ' '}] " + text
                elif t == "code":
                    code_text = "".join([r.get("plain_text", "") for r in payload.get("rich_text", [])])
                    lang = payload.get("language", "")
                    line = f"```{lang}\n{_maybe_fix_mojibake(code_text)}\n```"
                elif t == "divider":
                    line = "---"
                elif t in ("callout", "toggle"):
                    # a toggle/callout címe jelenjen meg idézetként
                    if text:
                        line = "> " + text
                elif t in ("column_list", "column", "synced_block", "synced_block_reference", "table", "table_row"):
                    # Ezeknél csak a gyerekek érdekesek
                    line = None

                if line is not None and str(line).strip() != "":
                    acc.append(line)

                # Rekurzív bejárás minden gyereken
                if has_children:
                    walk(blk["id"], acc)

            if resp.get("has_more"):
                cursor = resp.get("next_cursor")
            else:
                break

    out_lines: List[str] = []
    walk(block_id, out_lines)
    md = "\n".join(out_lines).strip()
    return _fix_numbered_lists(md)

def _extract_section_exact(md: str, heading: str) -> str:
    """
    Rugalmasabb egyezés a megadott címsorra:
    - H2 vagy H3 (## / ###)
    - kis/nagybetű független egyezés
    - opcionális kettőspont a végén
    A kijelölt H2/H3-tól a KÖVETKEZŐ H2/H3-ig vágunk.
    """
    md = (_normalize_unicode(md) or "").replace("\u00A0", " ")
    pat = re.compile(rf"^##{{1,2}}\s*{re.escape(heading)}\s*:?\s*$", flags=re.MULTILINE | re.IGNORECASE)
    m = pat.search(md)
    if not m:
        return ""
    start = m.end()
    # Következő H2/H3
    m2 = re.search(r"^##{1,2}\s+.+$", md[start:], flags=re.MULTILINE)
    raw = md[start:start + (m2.start() if m2 else len(md))].strip()
    return raw if raw.strip() else ""