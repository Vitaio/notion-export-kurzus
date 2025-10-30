
import os
import io
import re
import csv
import unicodedata
import zipfile
from datetime import datetime
from typing import List, Dict, Any, Tuple

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

CSV_FIELDNAMES_BASE = ["kurzus", "sorszám", "név", "típus", "tartalom"]

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
        cut = window.rfind("\\n\\n")
        if cut >= int(max_len * 0.6):
            end = start + cut
        part = text[start:end].rstrip()
        if part:
            parts.append(part)
        start = end if end > start else len(text)

    out: Dict[str, str] = {}
    for i, p in enumerate(parts):
        out["tartalom" if i == 0 else f"tartalom_cont_{i}"] = p
    return out

def _fix_numbered_lists(md: str) -> str:
    lines = md.splitlines()
    n = 1
    for i, line in enumerate(lines):
        if re.match(r"^\\s*\\d+\\.\\s", line):
            lines[i] = re.sub(r"^\\s*\\d+\\.\\s", f"{n}. ", line)
            n += 1
        elif line.strip() == "":
            n = 1
    return "\\n".join(lines)

# ────────────────────────────────────────────────────────────────────────────────
# Notion → Markdown (rekurzív bejárás, kezeli toggle/column/callout)
# ────────────────────────────────────────────────────────────────────────────────
def blocks_to_md(client: Client, block_id: str) -> str:
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
                    line = "1. " + text
                elif t == "quote":
                    line = "> " + text
                elif t == "to_do":
                    checked = payload.get("checked", False)
                    line = f"- [{'x' if checked else ' '}] " + text
                elif t == "code":
                    code_text = "".join([r.get("plain_text", "") for r in payload.get("rich_text", [])])
                    lang = payload.get("language", "")
                    line = f"```{lang}\\n{_maybe_fix_mojibake(code_text)}\\n```"
                elif t == "divider":
                    line = "---"
                elif t in ("callout", "toggle"):
                    if text:
                        line = "> " + text
                elif t in ("column_list", "column", "synced_block", "synced_block_reference", "table", "table_row"):
                    line = None

                if line is not None and str(line).strip() != "":
                    acc.append(line)

                if has_children:
                    walk(blk["id"], acc)

            if resp.get("has_more"):
                cursor = resp.get("next_cursor")
            else:
                break

    out_lines: List[str] = []
    walk(block_id, out_lines)
    md = "\\n".join(out_lines).strip()
    return _fix_numbered_lists(md)

def _extract_section_exact(md: str, heading: str) -> str:
    """
    Rugalmas egyezés a megadott címsorra:
    - H2 vagy H3 (## / ###)
    - kis/nagybetű független
    - opcionális kettőspont a végén
    A kijelölt H2/H3-tól a KÖVETKEZŐ H2/H3-ig vágunk.
    """
    md = (unicodedata.normalize("NFC", md or "")).replace("\\u00A0", " ")
    pat = re.compile(rf"^##{{1,2}}\\s*{re.escape(heading)}\\s*:?\\s*$", flags=re.MULTILINE | re.IGNORECASE)
    m = pat.search(md)
    if not m:
        return ""
    start = m.end()
    m2 = re.search(r"^##{1,2}\\s+.+$", md[start:], flags=re.MULTILINE)
    raw = md[start:start + (m2.start() if m2 else len(md))].strip()
    return raw if raw.strip() else ""

# ────────────────────────────────────────────────────────────────────────────────
# Notion segédek
# ────────────────────────────────────────────────────────────────────────────────
def _get_title_from_page(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})
    for _, prop in props.items():
        if prop.get("type") == "title":
            return "".join([t.get("plain_text", "") for t in prop.get("title", [])]).strip()
    return page.get("id", "")

def _get_select_or_text(page: Dict[str, Any], prop_name: str) -> str:
    prop = page.get("properties", {}).get(prop_name)
    if not prop:
        return ""
    t = prop.get("type")
    if t == "select":
        sel = prop.get("select")
        return (sel or {}).get("name", "") if sel else ""
    if t == "multi_select":
        return ", ".join([x.get("name", "") for x in prop.get("multi_select", [])])
    if t == "rich_text":
        return "".join([rt.get("plain_text", "") for rt in prop.get("rich_text", [])]).strip()
    if t == "number":
        num = prop.get("number", None)
        return "" if num is None else str(num)
    if t == "status":
        stt = prop.get("status")
        return (stt or {}).get("name", "") if stt else ""
    return ""

# ────────────────────────────────────────────────────────────────────────────────
# I/O + CSV építés
# ────────────────────────────────────────────────────────────────────────────────
def _query_all_pages_with_status(client: Client, database_id: str) -> List[Dict[str, Any]]:
    """Oldalak beolvasása státusz kijelzéssel."""
    status = st.empty()
    pages: List[Dict[str, Any]] = []
    cursor = None
    batch = 0
    while True:
        batch += 1
        status.info(f"Notion lekérdezés… (batch {batch}, eddig {len(pages)} oldal)")
        resp = client.databases.query(database_id=database_id, start_cursor=cursor) if cursor else client.databases.query(database_id=database_id)
        pages.extend(resp.get("results", []))
        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break
    status.success(f"Kész: {len(pages)} oldal beolvasva.")
    return pages

def _rows_from_pages_with_progress(client: Client, pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sorok építése előrehaladás jelzéssel."""
    rows: List[Dict[str, Any]] = []
    n = len(pages)
    prog = st.progress(0.0)
    note = st.empty()
    for i, page in enumerate(pages):
        title = _get_title_from_page(page)
        group = _get_select_or_text(page, DEFAULT_GROUP_PROP) or "Ismeretlen"
        sorszam = _get_select_or_text(page, "Sorszám")
        szakasz = _get_select_or_text(page, "Szakasz")

        md = blocks_to_md(client, page["id"])
        chosen_type = None
        content = _extract_section_exact(md, VIDEO_HEADING)
        if content:
            chosen_type = VIDEO_HEADING
        else:
            content = _extract_section_exact(md, LESSON_HEADING)
            if content:
                chosen_type = LESSON_HEADING

        if not content:
            content = "Ehhez a leckéhez nem készült leírás."

        base = {"kurzus": group or "", "sorszám": sorszam or "", "név": title or "", "típus": chosen_type or (szakasz or "")}
        pieces = _split_content_for_csv(content, MAX_CONTENT_CHARS)
        row = {**base, **pieces}
        rows.append(_norm_csv_row(row))

        if n:
            prog.progress((i + 1) / n)
            note.text(f"Feldolgozás: {i+1}/{n} oldal")
    note.text(f"Kész: {len(rows)} sor")
    return rows

def _group_by(rows: List[Dict[str, Any]], key: str) -> Dict[str, List[Dict[str, Any]]]:
    g: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        g.setdefault(r.get(key, "Ismeretlen") or "Ismeretlen", []).append(r)
    return g

def _csv_bytes(rows: List[Dict[str, Any]]) -> bytes:
    header_set = set()
    for r in rows:
        header_set.update(r.keys())
    base = [c for c in CSV_FIELDNAMES_BASE if c in header_set]
    rest = [c for c in sorted(header_set) if c not in base]
    headers = base + rest

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return buf.getvalue().encode("utf-8-sig")  # BOM

def _zip_by_group(rows: List[Dict[str, Any]], group_key: str = "kurzus") -> bytes:
    groups = _group_by(rows, group_key)
    files = []
    for group, items in groups.items():
        csv_bytes = _csv_bytes(items)
        fname = f"{_slug(group) or 'ismeretlen'}.csv"
        files.append((fname, csv_bytes))
    return _zip_utf8(files)

# ────────────────────────────────────────────────────────────────────────────────
# Streamlit UI (gombnyomásra indul, haladás kijelzés, fő gomb kiemelve)
# ────────────────────────────────────────────────────────────────────────────────
def _check_secrets():
    missing = []
    if not NOTION_API_KEY:
        missing.append("NOTION_API_KEY")
    if not NOTION_DATABASE_ID:
        missing.append("NOTION_DATABASE_ID")
    if missing:
        st.error("Hiányzó beállítások: " + ", ".join(missing))
        st.info("Állítsd be környezeti változóként vagy a Streamlit secrets-ben.")
        st.stop()

def _password_gate():
    if not APP_PASSWORD:
        return True
    st.session_state.setdefault("_auth_ok", False)
    if st.session_state["_auth_ok"]:
        return True

    with st.form("auth_form", clear_on_submit=True):
        st.markdown("### Belépés")
        pw = st.text_input("Jelszó", type="password")
        ok = st.form_submit_button("Belépés", use_container_width=True)
        if ok:
            if pw == APP_PASSWORD:
                st.session_state["_auth_ok"] = True
                st.success("Sikeres belépés.")
                st.rerun()
            else:
                st.error("Hibás jelszó.")
    st.stop()

def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📄", layout="wide")
    st.title(APP_TITLE)
    st.caption("Notion adatbázis → CSV export. A 'Videó szöveg' / 'Lecke szöveg' szakaszok kinyerése, toggle/oszlop alatt is.")

    # Fő gomb hangsúly + finom UI
    st.markdown("""
    <style>
    .primary-big button {font-size:1.08rem; padding:0.9rem 1rem; font-weight:700;}
    .secondary button {opacity:0.95;}
    </style>
    """, unsafe_allow_html=True)

    _check_secrets()
    _password_gate()

    client = Client(auth=NOTION_API_KEY)

    st.markdown("### Export mód")
    colA, colB = st.columns([1.6, 1])
    run_all = colA.button("⬇️ Minden egy CSV-ben", type="primary", use_container_width=True, key="run_all")
    run_zip = colB.button("ZIP – kurzusonként külön CSV", type="secondary", use_container_width=True, key="run_zip")

    if not (run_all or run_zip):
        st.info("Válassz export módot a fenti gombokkal. A feldolgozás csak gombnyomásra indul.")
        st.stop()

    # 1) Beolvasás
    st.markdown("#### 1) Oldalak beolvasása")
    pages = _query_all_pages_with_status(client, NOTION_DATABASE_ID)
    if not pages:
        st.warning("Nem érkezett oldal a Notionből.")
        st.stop()

    # 2) Feldolgozás
    st.markdown("#### 2) Tartalom feldolgozása")
    rows = _rows_from_pages_with_progress(client, pages)

    # 3) Exportálás
    st.markdown("#### 3) Exportálás")
    if run_all:
        csv_bytes = _csv_bytes(rows)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.success(f"Kész: {len(rows)} sor egy CSV-ben.")
        st.download_button(
            label="CSV letöltése",
            data=csv_bytes,
            file_name=f"export_minden_{ts}.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True,
            key="dl_all",
        )
    else:
        zip_bytes = _zip_by_group(rows, group_key="kurzus")
        ts2 = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.success(f"Kész: {len(rows)} sor, kurzusonként csoportosítva (ZIP).")
        st.download_button(
            label="ZIP letöltése",
            data=zip_bytes,
            file_name=f"export_kurzusonként_{ts2}.zip",
            mime="application/zip",
            type="secondary",
            use_container_width=True,
            key="dl_zip",
        )

if __name__ == "__main__":
    main()
