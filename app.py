import os
import io
import re
import csv
import unicodedata
import zipfile
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Set

import streamlit as st
from notion_client import Client

# ────────────────────────────────────────────────────────────────────────────────
# Beállítások (ENV / secrets)
# ────────────────────────────────────────────────────────────────────────────────
APP_TITLE = "Notion → CSV Export – Kurzus"
DEFAULT_GROUP_PROP = os.getenv("NOTION_PROPERTY_NAME", st.secrets.get("NOTION_PROPERTY_NAME", "Kurzus"))
MAX_CONTENT_CHARS = int(os.getenv("MAX_CONTENT_CHARS", st.secrets.get("MAX_CONTENT_CHARS", 40000)))

NOTION_API_KEY = os.getenv("NOTION_API_KEY", st.secrets.get("NOTION_API_KEY", ""))
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", st.secrets.get("NOTION_DATABASE_ID", ""))
APP_PASSWORD = os.getenv("APP_PASSWORD", st.secrets.get("APP_PASSWORD", ""))

CSV_FIELDNAMES = ["kurzus", "sorszám", "név", "típus", "tartalom"]

# Csak EZEKET a H2-ket figyeljük (fix, nem szerkeszthető):
VIDEO_HEADING = "Videó szöveg"
LESSON_HEADING = "Lecke szöveg"

# ────────────────────────────────────────────────────────────────────────────────
# Unicode normalizálás & mojibake javítás
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
# Segédek
# ────────────────────────────────────────────────────────────────────────────────
def _slug(s: str) -> str:
    s = _maybe_fix_mojibake(s)
    s = s.strip().lower()
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
    md_lines: List[str] = []
    cursor = None
    while True:
        resp = client.blocks.children.list(block_id=block_id, start_cursor=cursor) if cursor else client.blocks.children.list(block_id=block_id)
        for blk in resp.get("results", []):
            t = blk.get("type")
            def rt(x): return "".join([r.get("plain_text","") for r in x.get("rich_text", [])])
            if t == "paragraph":
                md_lines.append(_maybe_fix_mojibake(rt(blk[t])))
            elif t == "heading_1":
                md_lines.append("# " + _maybe_fix_mojibake(rt(blk[t])))
            elif t == "heading_2":
                md_lines.append("## " + _maybe_fix_mojibake(rt(blk[t])))
            elif t == "heading_3":
                md_lines.append("### " + _maybe_fix_mojibake(rt(blk[t])))
            elif t == "bulleted_list_item":
                md_lines.append(f"- {_maybe_fix_mojibake(rt(blk[t]))}")
            elif t == "numbered_list_item":
                md_lines.append(f"1. {_maybe_fix_mojibake(rt(blk[t]))}")
            elif t == "to_do":
                checked = blk[t].get("checked", False)
                md_lines.append(f"- [{'x' if checked else ' '}] {_maybe_fix_mojibake(rt(blk[t]))}")
            elif t == "quote":
                md_lines.append("> " + _maybe_fix_mojibake(rt(blk[t])))
            elif t == "code":
                lang = blk[t].get("language", "")
                md_lines.append(f"```{lang}\n{_maybe_fix_mojibake(rt(blk[t]))}\n```")
            elif t == "divider":
                md_lines.append("---")
        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break
    md = "\n".join(md_lines).strip()
    return _fix_numbered_lists(md)

# ────────────────────────────────────────────────────────────────────────────────
# Notion DB lekérés & sor-építés
# ────────────────────────────────────────────────────────────────────────────────
def _get_client() -> Client:
    return Client(auth=NOTION_API_KEY)

def _get_all_pages(client: Client, db_id: str) -> List[Dict[str, Any]]:
    out = []
    cursor = None
    while True:
        resp = client.databases.query(database_id=db_id, start_cursor=cursor) if cursor else client.databases.query(database_id=db_id)
        out.extend(resp.get("results", []))
        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break
    return out

def _get_prop(page: Dict[str, Any], name: str) -> Any:
    return page.get("properties", {}).get(name)

def _title_of(page: Dict[str, Any]) -> str:
    for key, prop in page.get("properties", {}).items():
        if prop.get("type") == "title":
            return _maybe_fix_mojibake("".join([t.get("plain_text","") for t in prop["title"]]))
    return ""

def _select_value(prop: Dict[str, Any]) -> str:
    if not prop: return ""
    t = prop.get("type")
    if t == "select" and prop.get("select"):
        return _maybe_fix_mojibake(prop["select"]["name"])
    if t == "multi_select":
        return _maybe_fix_mojibake(", ".join([x["name"] for x in prop.get("multi_select", [])]))
    if t == "rich_text":
        return _maybe_fix_mojibake("".join([t.get("plain_text","") for t in prop.get("rich_text", [])]))
    if t == "number":
        return str(prop.get("number", "") or "")
    return ""

def _extract_section(md: str, heading: str) -> str:
    # PONTOS egyezés a két H2-re
    pat = re.compile(rf"^##\s*{re.escape(heading)}\s*$", flags=re.MULTILINE)
    m = pat.search(md or "")
    if not m:
        return ""
    start = m.end()
    m2 = re.search(r"^##\s+.+$", (md or "")[start:], flags=re.MULTILINE)
    raw = (md or "")[start:start + (m2.start() if m2 else len(md or ""))].strip()
    return raw if raw.strip() else ""

def _extract_video_or_lesson(md: str) -> str:
    video = _extract_section(md, VIDEO_HEADING)
    lesson = _extract_section(md, LESSON_HEADING)
    if video:
        return video
    if lesson:
        return lesson
    return "Ehhez a leckéhez nem készült leírás."

def _row_from_page(client: Client, page: Dict[str, Any]) -> Dict[str, str]:
    title = _title_of(page)
    order = _select_value(_get_prop(page, "Sorszám"))
    section = _select_value(_get_prop(page, "Szakasz"))
    group = _select_value(_get_prop(page, DEFAULT_GROUP_PROP))
    page_id = page["id"].replace("-", "")
    md_full = blocks_to_md(client, page_id)
    content = _extract_video_or_lesson(md_full)
    row = {"kurzus": group or "", "sorszám": order or "", "név": title or "", "típus": section or "", "tartalom": content}
    return _norm_csv_row(row)

# ────────────────────────────────────────────────────────────────────────────────
# CSV építés
# ────────────────────────────────────────────────────────────────────────────────
def export_group_csv_bytes(client: Client, pages: List[Dict[str, Any]]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDNAMES)
    writer.writeheader()
    if not pages:
        return output.getvalue().encode("utf-8-sig")

    rows_base = [_row_from_page(client, pg) for pg in pages]
    split_rows: List[Dict[str, str]] = []
    max_extra = 0
    for r in rows_base:
        chunks = _split_content_for_csv(r.get("tartalom",""), MAX_CONTENT_CHARS)
        out = dict(r); out.pop("tartalom", None); out.update(chunks)
        max_extra = max(max_extra, len(out) - len(CSV_FIELDNAMES))
        split_rows.append(_norm_csv_row(out))

    fieldnames = CSV_FIELDNAMES + [f"tartalom_cont_{i}" for i in range(1, max_extra+1)]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in split_rows:
        for i in range(1, max_extra+1):
            r.setdefault(f"tartalom_cont_{i}", "")
        writer.writerow(r)

    return output.getvalue().encode("utf-8-sig")

def export_unified_csv_bytes(client: Client, all_pages: List[Dict[str, Any]]) -> bytes:
    rows = [_row_from_page(client, pg) for pg in all_pages]
    split_rows: List[Dict[str, str]] = []
    max_extra = 0
    for r in rows:
        chunks = _split_content_for_csv(r.get("tartalom",""), MAX_CONTENT_CHARS)
        out = dict(r); out.pop("tartalom", None); out.update(chunks)
        max_extra = max(max_extra, len(out) - len(CSV_FIELDNAMES))
        split_rows.append(_norm_csv_row(out))

    fieldnames = CSV_FIELDNAMES + [f"tartalom_cont_{i}" for i in range(1, max_extra+1)]
    out = io.StringIO(newline="")
    w = csv.DictWriter(out, fieldnames=fieldnames)
    w.writeheader()
    for r in split_rows:
        for i in range(1, max_extra+1):
            r.setdefault(f"tartalom_cont_{i}", "")
        w.writerow(r)

    return out.getvalue().encode("utf-8-sig")

# ────────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ────────────────────────────────────────────────────────────────────────────────
def _auth():
    if not APP_PASSWORD:
        return True
    pwd = st.text_input("Jelszó", type="password")
    if pwd == APP_PASSWORD:
        st.session_state["authed"] = True
    if st.session_state.get("authed"):
        return True
    st.stop()

def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📤", layout="wide")
    st.title(APP_TITLE)
    st.caption("Csak a **Videó szöveg** és **Lecke szöveg** H2-k figyelése • UTF-8 BOM CSV • UTF-8 ZIP-fájlnevek • ékezetjavítás ✅")

    if not NOTION_API_KEY or not NOTION_DATABASE_ID:
        st.error("Állítsd be a NOTION_API_KEY és NOTION_DATABASE_ID értékeket (ENV vagy st.secrets).")
        st.stop()

    _auth()

    with st.expander("Beállítások (információ)", expanded=True):
        st.markdown(
            "- **Figyelt szakaszok:**\n"
            f"  - `## {VIDEO_HEADING}`\n"
            f"  - `## {LESSON_HEADING}`\n\n"
            "Más H2-ket a rendszer **nem** vesz figyelembe. Ha mindkettőben van tartalom, a **Videó szöveg** élvez elsőbbséget. "
            "Ha egyik sincs, ezt írjuk ki: _Ehhez a leckéhez nem készült leírás._"
        )

    client = _get_client()
    with st.spinner("Adatok lekérése a Notionból…"):
        pages = _get_all_pages(client, NOTION_DATABASE_ID)

    groups: Dict[str, List[Dict[str, Any]]] = {}
    for pg in pages:
        g = _select_value(_get_prop(pg, DEFAULT_GROUP_PROP)) or "(Ismeretlen)"
        groups.setdefault(g, []).append(pg)

    st.subheader("Összefoglaló")
    st.write(f"Oldalak száma: **{len(pages)}**")
    st.write(f"Csoport tulajdonság: **{DEFAULT_GROUP_PROP}**")
    st.write(f"Csoportok száma: **{len(groups)}**")

    tab1, tab2 = st.tabs(["Kurzusonként külön CSV (ZIP)", "Minden egy CSV-ben"])

    with tab1:
        st.write("Minden kurzushoz külön CSV-t készítünk, és UTF-8 fájlnévvel ZIP-be csomagoljuk.")
        if st.button("Export indítása (ZIP)", type="primary", use_container_width=True):
            files: List[Tuple[str, bytes]] = []
            prog = st.progress(0.0)
            for i, (gname, gpages) in enumerate(sorted(groups.items(), key=lambda x: x[0].lower())):
                data = export_group_csv_bytes(client, gpages)
                arc = f"export_{_slug(gname)}.csv"
                files.append((arc, data))
                prog.progress((i+1)/max(1,len(groups)))
            zip_bytes = _zip_utf8(files)
            st.success("Export kész.")
            st.download_button("ZIP letöltése", data=zip_bytes,
                               file_name=f"notion_kurzus_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                               mime="application/zip")

    with tab2:
        st.write("Összes oldal egyetlen CSV-ben (Excel-barát UTF-8 BOM-mal).")
        if st.button("Export indítása (1 CSV)", type="primary", use_container_width=True):
            data = export_unified_csv_bytes(client, pages)
            st.success("Export kész.")
            st.download_button("CSV letöltése",
                               data=data,
                               file_name=f"Content_egylap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                               mime="text/csv")

if __name__ == "__main__":
    main()
