import os
import io
import re
import csv
import time
import unicodedata
import zipfile
from datetime import datetime
from typing import List, Dict, Any, Tuple, Callable

import streamlit as st
from notion_client import Client
from notion_client.errors import APIResponseError

# ────────────────────────────────────────────────────────────────────────────────
# Alapbeállítások
# ────────────────────────────────────────────────────────────────────────────────
APP_TITLE = "Notion → CSV Export – Kurzus"
DEFAULT_GROUP_PROP = os.getenv("NOTION_PROPERTY_NAME", st.secrets.get("NOTION_PROPERTY_NAME", "Kurzus"))
MAX_CONTENT_CHARS = int(os.getenv("MAX_CONTENT_CHARS", st.secrets.get("MAX_CONTENT_CHARS", 40000)))

RAW_NOTION_API_KEY = os.getenv("NOTION_API_KEY", st.secrets.get("NOTION_API_KEY", ""))
RAW_NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", st.secrets.get("NOTION_DATABASE_ID", ""))

APP_PASSWORD = os.getenv("APP_PASSWORD", st.secrets.get("APP_PASSWORD", ""))

# Rate limit / teljesítmény
NOTION_PAGE_SIZE = int(os.getenv("NOTION_PAGE_SIZE", st.secrets.get("NOTION_PAGE_SIZE", 50)))  # 1..100
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", st.secrets.get("RETRY_MAX_ATTEMPTS", 6)))
RETRY_BACKOFF_START = float(os.getenv("RETRY_BACKOFF_START", st.secrets.get("RETRY_BACKOFF_START", 1.0)))  # sec
RETRY_BACKOFF_MAX = float(os.getenv("RETRY_BACKOFF_MAX", st.secrets.get("RETRY_BACKOFF_MAX", 30.0)))       # sec
POLITE_DELAY_SEC = float(os.getenv("NOTION_POLITE_DELAY_SEC", st.secrets.get("NOTION_POLITE_DELAY_SEC", 0.35)))  # sec

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
# ID / beállítás segédek
# ────────────────────────────────────────────────────────────────────────────────
def _mask(s: str, show: int = 6) -> str:
    s = s or ""
    return s if len(s) <= show else s[:show] + "…"

def _extract_uuid_like(s: str) -> str:
    """Teljes Notion URL-ből is kinyeri a DB azonosítót."""
    if not s:
        return ""
    s = s.strip()
    m = re.search(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", s)
    if m:
        return m.group(1)
    m = re.search(r"([0-9a-fA-F]{32})", s)
    if m:
        u = m.group(1)
        return f"{u[0:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:32]}"
    return s

def _sanitize_db_id(raw: str) -> str:
    return _extract_uuid_like(raw)

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
        start = end if end > start else len(text)

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
# Notion hívások – retry + backoff + Retry-After
# ────────────────────────────────────────────────────────────────────────────────
def _with_retry(call: Callable[[], Any], where: str, warn_placeholder=None):
    delay = RETRY_BACKOFF_START
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            return call()
        except APIResponseError as e:
            code = getattr(e, "code", "")
            transient = code in ("rate_limited", "service_unavailable", "internal_server_error", "conflict_error")
            if not transient or attempt == RETRY_MAX_ATTEMPTS:
                if warn_placeholder:
                    warn_placeholder.error(f"Notion API hiba ({where}): **{code or 'ismeretlen'}** – leállok.")
                raise
            retry_after = 0.0
            resp = getattr(e, "response", None)
            if resp is not None:
                ra = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
                if ra:
                    try:
                        retry_after = float(ra)
                    except Exception:
                        retry_after = 0.0
            wait_for = max(retry_after, delay)
            if warn_placeholder:
                warn_placeholder.warning(f"{where}: **{code}** – újrapróbálkozás {attempt}/{RETRY_MAX_ATTEMPTS}, várakozás {wait_for:.1f}s")
            time.sleep(wait_for)
            delay = min(delay * 2.0, RETRY_BACKOFF_MAX)

# ────────────────────────────────────────────────────────────────────────────────
# Notion → Markdown (rekurzív bejárás, kezeli toggle/column/callout)
# ────────────────────────────────────────────────────────────────────────────────
def blocks_to_md(client: Client, block_id: str) -> str:
    def rt(payload: Dict[str, Any]) -> str:
        return "".join([r.get("plain_text", "") for r in payload.get("rich_text", [])])

    def walk(bid: str, acc: List[str]):
        cursor = None
        while True:
            resp = _with_retry(
                lambda: client.blocks.children.list(block_id=bid, start_cursor=cursor) if cursor else
                        client.blocks.children.list(block_id=bid),
                where="Blocks.children.list"
            )
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
                    line = f"```{lang}\n{_maybe_fix_mojibake(code_text)}\n```"
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
                time.sleep(POLITE_DELAY_SEC)
            else:
                break

    out_lines: List[str] = []
    walk(block_id, out_lines)
    md = "\n".join(out_lines).strip()
    return _fix_numbered_lists(md)

def _extract_section_exact(md: str, heading: str) -> str:
    """H2/H3 címsor: kis/nagybetű független, opcionális ':'."""
    md = (_normalize_unicode(md) or "").replace("\u00A0", " ")
    pat = re.compile(rf"^#{2,3}\s*{re.escape(heading)}\s*:?\s*$", flags=re.MULTILINE | re.IGNORECASE)
    m = pat.search(md)
    if not m:
        return ""
    start = m.end()
    m2 = re.search(r"^#{2,3}\s+.+$", md[start:], flags=re.MULTILINE)
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
    """Oldalak beolvasása státusz kijelzéssel + retry/backoff."""
    status = st.empty()
    warn = st.empty()
    pages: List[Dict[str, Any]] = []
    cursor = None
    batch = 0
    while True:
        batch += 1
        status.info(f"Notion lekérdezés… (batch {batch}, eddig {len(pages)} oldal)")
        resp = _with_retry(
            lambda: client.databases.query(database_id=database_id, start_cursor=cursor, page_size=NOTION_PAGE_SIZE)
                    if cursor else
                    client.databases.query(database_id=database_id, page_size=NOTION_PAGE_SIZE),
            where="Databases.query",
            warn_placeholder=warn
        )
        pages.extend(resp.get("results", []))
        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
            time.sleep(POLITE_DELAY_SEC)
        else:
            break
    status.success(f"Kész: {len(pages)} oldal beolvasva.")
    warn.empty()
    return pages

def _rows_from_pages_with_progress(client: Client, pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sorok építése előrehaladás jelzéssel + hibatűrés blokkolvasáskor."""
    rows: List[Dict[str, Any]] = []
    n = len(pages)
    prog = st.progress(0.0)
    note = st.empty()
    warn = st.empty()
    for i, page in enumerate(pages):
        title = _get_title_from_page(page)
        group = _get_select_or_text(page, DEFAULT_GROUP_PROP) or "Ismeretlen"
        sorszam = _get_select_or_text(page, "Sorszám")
        szakasz = _get_select_or_text(page, "Szakasz")

        try:
            md = blocks_to_md(client, page["id"])
        except APIResponseError as e:
            warn.warning(f"Blokkolvasás kihagyva: '{title}' (hiba: {getattr(e, 'code', 'ismeretlen')})")
            md = ""

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
        time.sleep(POLITE_DELAY_SEC * 0.5)

    note.text(f"Kész: {len(rows)} sor")
    warn.empty()
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

def _filter_rows_by_courses(rows: List[Dict[str, Any]], selected_courses: List[str]) -> List[Dict[str, Any]]:
    if not selected_courses:
        return []
    sel = set(selected_courses)
    out = []
    for r in rows:
        g = r.get("kurzus") or "Ismeretlen"
        if g in sel:
            out.append(r)
    return out

# ────────────────────────────────────────────────────────────────────────────────
# Streamlit UI (gombnyomásra indul, progress, kurzus-választó multiselect)
# ────────────────────────────────────────────────────────────────────────────────
def _check_secrets():
    missing = []
    if not RAW_NOTION_API_KEY:
        missing.append("NOTION_API_KEY")
    if not RAW_NOTION_DATABASE_ID:
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

    # UI finomhangolás: primary gomb erősebb
    st.markdown("""
    <style>
    .secondary button {opacity:0.95;}
    </style>
    """, unsafe_allow_html=True)

    _check_secrets()
    _password_gate()

    api_key = RAW_NOTION_API_KEY.strip()
    db_id = _sanitize_db_id(RAW_NOTION_DATABASE_ID)
    client = Client(auth=api_key)

    with st.expander("Csatlakozás ellenőrzése (opcionális)", expanded=False):
        st.write("**NOTION_API_KEY**:", _mask(api_key))
        st.write("**NOTION_DATABASE_ID (szabványosítva)**:", _mask(db_id))
        if st.button("🔍 Teszt kapcsolat", use_container_width=True):
            info = st.empty()
            try:
                meta = _with_retry(lambda: client.databases.retrieve(database_id=db_id), "Databases.retrieve", info)
                title = "".join([t.get("plain_text", "") for t in meta.get("title", [])]) if meta.get("title") else ""
                info.success(f"Siker! Adatbázis neve: **{title or '(nincs cím)'}**")
            except APIResponseError as e:
                st.error(f"Hiba: {getattr(e,'code','ismeretlen')}")
                return

    # Indító gomb a feldolgozáshoz
    st.markdown("### Indítás")
    start = st.button("🔄 Kurzusok beolvasása és feldolgozása", type="primary", use_container_width=True)
    if not start:
        st.info("Kattints a fenti gombra a feldolgozás indításához, majd válaszd ki a kurzusokat az exporthoz.")
        return

    # 1) Beolvasás
    st.markdown("#### 1) Oldalak beolvasása")
    try:
        pages = _query_all_pages_with_status(client, db_id)
    except APIResponseError as e:
        st.error(f"Notion API hiba (Databases.query): {getattr(e,'code','ismeretlen')}")
        return
    if not pages:
        st.warning("Nem érkezett oldal a Notionből.")
        return

    # 2) Feldolgozás
    st.markdown("#### 2) Tartalom feldolgozása")
    rows = _rows_from_pages_with_progress(client, pages)

    # 3) Kurzusválasztás és export
    st.markdown("#### 3) Kurzusválasztás és export")
    # opcionlista felépítése
    groups = sorted({(r.get("kurzus") or "Ismeretlen") for r in rows}, key=lambda s: s.lower())
    default_sel = groups  # alapértelmezés: mind
    selected = st.multiselect("Válaszd ki a kurzusokat", options=groups, default=default_sel, help="A kiválasztott kurzusok kerülnek a letöltésbe.")

    sel_rows = _filter_rows_by_courses(rows, selected)
    st.caption(f"Kiválasztott kurzusok: **{len(selected)}** • sorok: **{len(sel_rows)}** / összes: {len(rows)}")

    colA, colB = st.columns([1.6, 1])
    with colA:
        csv_bytes = _csv_bytes(sel_rows)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="⬇️ CSV – csak a kiválasztott kurzusok",
            data=csv_bytes,
            file_name=f"export_kiválasztott_{ts}.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True,
            key="dl_selected_csv",
            disabled=not sel_rows,
            help="Az összes kijelölt kurzus egyetlen CSV fájlban (UTF-8 BOM).",
        )
    with colB:
        zip_bytes = _zip_by_group(sel_rows, group_key="kurzus")
        ts2 = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="ZIP – kurzusonként (kiválasztottak)",
            data=zip_bytes,
            file_name=f"export_kiválasztott_kurzusonként_{ts2}.zip",
            mime="application/zip",
            type="secondary",
            use_container_width=True,
            key="dl_selected_zip",
            disabled=not sel_rows,
            help="A kijelölt kurzusok külön CSV-ként, ZIP-be csomagolva.",
        )

if __name__ == "__main__":
    main()
