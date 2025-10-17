import os
import io
import csv
import time
import re
import json
import unicodedata
from typing import Dict, List, Optional, Set, Tuple
from collections import Counter, defaultdict

import streamlit as st
from notion_client import Client
from notion_client.errors import APIResponseError

# ────────────────────────────────────────────────────────────────────────────────
# Secrets → env bridge (Streamlit Cloud esetén hasznos)
# ────────────────────────────────────────────────────────────────────────────────
try:
    for k in ("NOTION_API_KEY", "NOTION_DATABASE_ID", "APP_PASSWORD", "NOTION_PROPERTY_NAME",
              "GOOGLE_SHEETS_SPREADSHEET_ID", "GOOGLE_SERVICE_ACCOUNT"):
        if k in st.secrets and not os.getenv(k):
            os.environ[k] = str(st.secrets[k])
except Exception:
    pass

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────────────────────
NOTION_API_KEY = os.getenv("NOTION_API_KEY", "").strip()
DATABASE_ID    = os.getenv("NOTION_DATABASE_ID", "").strip()
APP_PASSWORD   = os.getenv("APP_PASSWORD", "").strip()

# A csoportosításhoz használt property a Notion adatbázisban:
PROPERTY_NAME  = os.getenv("NOTION_PROPERTY_NAME", "Kurzus").strip()

# Google Sheets
GS_SHEET_ID    = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
GS_SA_JSON     = os.getenv("GOOGLE_SERVICE_ACCOUNT", "").strip()

# Megjelenítési átnevezések: {VALÓDI_NÉV -> MIT MUTASSON A LISTÁBAN}
DISPLAY_RENAMES: Dict[str, str] = {
    "Üzleti Modellek": "Milyen vállalkozást indíts",
    "Marketing rendszerek": "Ügyfélszerző marketing rendszerek",
}

# CSV oszlopok – egységes snake_case
CSV_FIELDNAMES = ["oldal_cime", "szakasz", "sorszam", "tartalom"]

# ────────────────────────────────────────────────────────────────────────────────
# Oldalbeállítás
# ────────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Notion export – Kurzus", page_icon="📦", layout="centered")

# ────────────────────────────────────────────────────────────────────────────────
# Autentikáció
# ────────────────────────────────────────────────────────────────────────────────
def need_auth() -> bool:
    if "authed" not in st.session_state:
        st.session_state.authed = False
    return not st.session_state.authed

def login_form() -> None:
    st.subheader("Belépés")
    with st.form("login", clear_on_submit=False):
        pwd = st.text_input("Jelszó", type="password")
        ok = st.form_submit_button("Belépés")
        if ok:
            if APP_PASSWORD and pwd == APP_PASSWORD:
                st.session_state.authed = True
                st.success("Beléptél ✅")
                st.rerun()
            else:
                st.error("Hibás jelszó.")

# ────────────────────────────────────────────────────────────────────────────────
# Notion kliens és sémainformáció
# ────────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_client() -> Client:
    if not NOTION_API_KEY:
        raise RuntimeError("A NOTION_API_KEY nincs beállítva (környezeti változó vagy Streamlit Secrets).")
    return Client(auth=NOTION_API_KEY)

@st.cache_data(ttl=120)
def get_database_schema() -> Dict:
    if not DATABASE_ID:
        raise RuntimeError("A NOTION_DATABASE_ID nincs beállítva.")
    return get_client().databases.retrieve(database_id=DATABASE_ID)

@st.cache_data(ttl=120)
def get_property_type() -> Optional[str]:
    """A csoportosító property (PROPERTY_NAME) típusa: select / multi_select / status."""
    db = get_database_schema()
    p = (db.get("properties", {}) or {}).get(PROPERTY_NAME)
    return p.get("type") if p else None

@st.cache_data(ttl=120)
def schema_id_to_current_name() -> Dict[str, str]:
    """A PROPERTY_NAME opciók (id → jelenlegi név) táblája."""
    db = get_database_schema()
    props = db.get("properties", {}) or {}
    p = props.get(PROPERTY_NAME)
    id2name: Dict[str, str] = {}
    if p:
        ptype = p.get("type")
        if ptype in ("select", "multi_select", "status"):
            for opt in (p.get(ptype, {}) or {}).get("options", []) or []:
                if opt.get("id") and opt.get("name"):
                    id2name[opt["id"]] = opt["name"]
    return id2name

def with_backoff(fn, *args, retries: int = 5, **kwargs):
    """Egyszerű backoff réteg 429/5xx hibákra."""
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except APIResponseError as e:
            status = getattr(e, "status", None)
            if status in (429, 500, 502, 503):
                time.sleep((2 ** i) + 0.1)
                continue
            raise

def query_all_pages() -> List[Dict]:
    """Az adatbázis minden oldalát lekéri lapozással (általános – NINCS szűrés/rendezés)."""
    client = get_client()
    results: List[Dict] = []
    cursor = None
    while True:
        resp = with_backoff(client.databases.query, database_id=DATABASE_ID, start_cursor=cursor, page_size=100)
        results.extend(resp.get("results", []) or [])
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return results

def query_filtered_pages(filter_: Dict, sorts: Optional[List[Dict]] = None) -> List[Dict]:
    """Szűrt lekérdezés lapozással, opcionális rendezéssel (sorts)."""
    client = get_client()
    results: List[Dict] = []
    cursor = None
    while True:
        kwargs = {
            "database_id": DATABASE_ID,
            "filter": filter_,
            "start_cursor": cursor,
            "page_size": 100
        }
        if sorts:
            kwargs["sorts"] = sorts
        resp = with_backoff(client.databases.query, **kwargs)
        results.extend(resp.get("results", []) or [])
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return results

@st.cache_data(ttl=120)
def collect_used_ids_and_names() -> Tuple[Counter, Dict[str, Set[str]]]:
    """
    A PROPERTY_NAME property-hez:
      - megszámoljuk az opciókat (id szerint),
      - és összegyűjtjük, hogy az oldalakban milyen 'név' változatok fordultak elő.
    """
    pages = query_all_pages()
    ptype = get_property_type()
    used_by_id: Counter = Counter()
    names_seen_by_id: Dict[str, Set[str]] = defaultdict(set)

    for page in pages:
        prop = (page.get("properties", {}) or {}).get(PROPERTY_NAME)
        if not prop:
            continue
        if ptype == "select":
            node = prop.get("select") or {}
            oid, name = node.get("id"), (node.get("name") or "").strip()
            if oid:
                used_by_id[oid] += 1
                if name:
                    names_seen_by_id[oid].add(name)
        elif ptype == "multi_select":
            for node in prop.get("multi_select") or []:
                oid, name = node.get("id"), (node.get("name") or "").strip()
                if oid:
                    used_by_id[oid] += 1
                    if name:
                        names_seen_by_id[oid].add(name)
        elif ptype == "status":
            node = prop.get("status") or {}
            oid, name = node.get("id"), (node.get("name") or "").strip()
            if oid:
                used_by_id[oid] += 1
                if name:
                    names_seen_by_id[oid].add(name)

    return used_by_id, names_seen_by_id

def build_display_list() -> List[Tuple[str, int, Set[str]]]:
    """
    Visszaadja a megjelenítési listát:
      [(display_name, count, canonical_names), ...]
      - display_name: amit a listában mutatunk (DISPLAY_RENAMES alkalmazva)
      - canonical_names: ezzel próbálunk szűrni (aktuális név + esetleges régi variánsok + reverse aliasok)
    """
    used_by_id, names_seen = collect_used_ids_and_names()
    id2current = schema_id_to_current_name()

    # reverse alias tábla: {megjelenített_név → {régi_nevek}}
    reverse_alias: Dict[str, Set[str]] = defaultdict(set)
    for old, new in DISPLAY_RENAMES.items():
        reverse_alias[new].add(old)

    display_items: Dict[str, Dict[str, object]] = {}
    for oid, cnt in used_by_id.items():
        current_candidates = names_seen.get(oid, set())
        current_name = id2current.get(oid) or (sorted(current_candidates)[0] if current_candidates else f"(árva {oid[:6]}...)")
        display_name = DISPLAY_RENAMES.get(current_name, current_name)
        canon: Set[str] = set([current_name]) | current_candidates | reverse_alias.get(display_name, set())

        entry = display_items.setdefault(display_name, {"count": 0, "canon": set()})
        entry["count"] = int(entry["count"]) + cnt  # type: ignore
        entry["canon"] = set(entry["canon"]) | canon  # type: ignore

    items: List[Tuple[str, int, Set[str]]] = [
        (disp, int(meta["count"]), set(meta["canon"]))  # type: ignore
        for disp, meta in display_items.items()
    ]
    # Alapból csökkenő rendezés a listához (UI): nagyobb elől
    items.sort(key=lambda x: (-x[1], x[0].lower()))
    return items

def build_filter(ptype: Optional[str], name: str) -> Dict:
    if ptype == "select":
        return {"property": PROPERTY_NAME, "select": {"equals": name}}
    if ptype == "multi_select":
        return {"property": PROPERTY_NAME, "multi_select": {"contains": name}}
    if ptype == "status":
        return {"property": PROPERTY_NAME, "status": {"equals": name}}
    return {"property": PROPERTY_NAME, "select": {"equals": name}}

def extract_title(page: Dict) -> str:
    """Az oldal címének kinyerése."""
    props = page.get("properties", {}) or {}
    for _, val in props.items():
        if val.get("type") == "title":
            arr = val.get("title", []) or []
            if arr:
                return " ".join(x.get("plain_text", "") for x in arr).strip() or "Névtelen oldal"
    lekce = props.get("Lecke címe", {})
    if lekce.get("type") == "title" and lekce.get("title"):
        return " ".join((x.get("plain_text") or "") for x in lekce["title"]).strip() or "Névtelen oldal"
    return "Névtelen oldal"

def resolve_title_prop_name() -> str:
    db = get_database_schema()
    for pname, meta in (db.get("properties", {}) or {}).items():
        if meta.get("type") == "title":
            return pname
    return ""

def format_rich_text(rt_list: List[Dict]) -> str:
    out = ""
    for r in rt_list or []:
        t = r.get("plain_text", "") or ""
        href = r.get("href")
        out += f"[{t}]({href})" if href else t
    return out

def blocks_to_md(block_id: str, depth: int = 0) -> str:
    client = get_client()
    lines: List[str] = []
    cursor = None
    indent = "  " * depth

    while True:
        resp = with_backoff(client.blocks.children.list, block_id=block_id, start_cursor=cursor)
        for block in resp.get("results", []) or []:
            btype = block.get("type")
            data = block.get(btype, {}) or {}
            line = ""

            if btype in (
                "paragraph", "heading_1", "heading_2", "heading_3",
                "bulleted_list_item", "numbered_list_item",
                "quote", "to_do", "callout", "toggle"
            ):
                txt = format_rich_text(data.get("rich_text", []))
                prefix = ""
                if   btype == "heading_1":          prefix = "# "
                elif btype == "heading_2":          prefix = "## "
                elif btype == "heading_3":          prefix = "### "
                elif btype == "bulleted_list_item": prefix = "- "
                elif btype == "numbered_list_item": prefix = "1. "
                elif btype == "quote":              prefix = "> "
                elif btype == "to_do":              prefix = "- [x] " if data.get("checked") else "- [ ] "
                elif btype == "callout":            prefix = "💡 "
                elif btype == "toggle":             prefix = "▶ "
                if txt or prefix:
                    line = f"{indent}{prefix}{txt}"

            elif btype == "code":
                lang = data.get("language", "") or ""
                inner = format_rich_text(data.get("rich_text", []))
                line = f"{indent}```{lang}\n{inner}\n```"

            elif btype == "equation":
                expr = data.get("expression", "") or ""
                line = f"{indent}$$ {expr } $$"

            elif btype == "divider":
                line = f"{indent}---"

            elif btype in ("image", "video", "file", "pdf"):
                cap = format_rich_text(data.get("caption", []))
                line = f"{indent}*[{btype.upper()}]* {cap}".rstrip()

            if line:
                lines.append(line)

            if block.get("has_children"):
                child = blocks_to_md(block["id"], depth + 1)
                if child.strip():
                    lines.append(child)

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return "\n".join(lines)

# ────────────────────────────────────────────────────────────────────────────────
# Property felderítés „Szakasz” / „Sorszám”
# ────────────────────────────────────────────────────────────────────────────────
def _norm_key(s: str) -> str:
    if not isinstance(s, str):
        s = str(s or "")
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower()
    for ch in (" ", "_", "-", ".", ":"):
        s = s.replace(ch, "")
    return s

SECTION_TARGETS = ["szakasz", "szekcio", "section", "modul", "fejezet", "rész", "resz"]
ORDER_TARGETS   = ["sorszám", "sorszam", "sorrend", "order", "index", "pozicio", "pozíció", "rank"]

@st.cache_data(ttl=300)
def resolve_section_and_order_props() -> Tuple[str, str]:
    db = get_database_schema()
    props: Dict[str, Dict] = db.get("properties", {}) or {}

    lookup = { _norm_key(k): k for k in props.keys() }
    sec_key = ""
    for cand in SECTION_TARGETS + ["szakasz"]:
        key = lookup.get(_norm_key(cand))
        if key:
            sec_key = key
            break
    if not sec_key:
        for k, v in props.items():
            if v.get("type") in ("select", "multi_select", "status"):
                sec_key = k
                break

    ord_key = ""
    for cand in ORDER_TARGETS + ["sorszám", "sorszam"]:
        key = lookup.get(_norm_key(cand))
        if key:
            ord_key = key
            break
    if not ord_key:
        for k, v in props.items():
            if v.get("type") == "number":
                ord_key = k
                break

    return (sec_key or ""), (ord_key or "")

def format_property_for_csv(page: Dict, prop_name: str) -> str:
    if not prop_name:
        return ""
    props = page.get("properties", {}) or {}
    p = props.get(prop_name)
    if not p:
        return ""
    ptype = p.get("type")
    try:
        if ptype == "number":
            val = p.get("number", None)
            return "" if val is None else str(val)
        if ptype == "select":
            node = p.get("select") or {}
            return (node.get("name") or "").strip()
        if ptype == "multi_select":
            arr = p.get("multi_select") or []
            return ", ".join((x.get("name") or "").strip() for x in arr if x.get("name"))
        if ptype == "status":
            node = p.get("status") or {}
            return (node.get("name") or "").strip()
        if ptype == "rich_text":
            arr = p.get("rich_text") or []
            return " ".join((x.get("plain_text") or "") for x in arr).strip()
        if ptype == "title":
            arr = p.get("title") or []
            return " ".join((x.get("plain_text") or "") for x in arr).strip()
        if ptype == "date":
            node = p.get("date") or {}
            start = node.get("start") or ""
            end   = node.get("end") or ""
            return f"{start}..{end}" if end else start
        if ptype == "url":
            return p.get("url") or ""
        if ptype == "email":
            return p.get("email") or ""
        if ptype == "people":
            arr = p.get("people") or []
            names = []
            for person in arr:
                name = (person.get("name") or "").strip()
                if not name:
                    name = (person.get("person", {}) or {}).get("email", "") or ""
                if name:
                    names.append(name)
            return ", ".join(names)
        return ""
    except Exception:
        return ""

# ────────────────────────────────────────────────────────────────────────────────
# Rendezés kiválasztása
# ────────────────────────────────────────────────────────────────────────────────
def resolve_sorts(order_prop: Optional[str]) -> Tuple[List[Dict], str]:
    if order_prop:
        return [{"property": order_prop, "direction": "ascending"}], f"property: {order_prop} ↑"
    title_prop = resolve_title_prop_name()
    if title_prop:
        return [{"property": title_prop, "direction": "ascending"}], f"title: {title_prop} ↑"
    return [], "unspecified (API default)"

# ────────────────────────────────────────────────────────────────────────────────
# Markdown szűrés + számozott listák ÚJRASZÁMOZÁSA + H2-szakasz kivágás
# ────────────────────────────────────────────────────────────────────────────────
def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.strip().lower()

def _norm_heading_key(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower()
    return re.sub(r"[^a-z0-9]+", "", s)

def _find_h2_positions(md: str) -> List[Tuple[int, str, str]]:
    lines = (md or "").splitlines()
    out = []
    for i, line in enumerate(lines):
        m = re.match(r"^\s*##\s+(.*)\s*$", line)
        if m:
            title = m.group(1).strip()
            out.append((i, title, _norm_heading_key(title)))
    return out

def fix_numbered_lists(md: str) -> str:
    lines = (md or "").splitlines()
    out: List[str] = []
    in_code = False
    fence_re = re.compile(r'^\s*```')
    num_re = re.compile(r'^(\s*)(\d+)\.\s(.*)$')

    active_list_indent: Optional[int] = None
    counter_for_indent: Dict[int, int] = {}

    for line in lines:
        if fence_re.match(line):
            in_code = not in_code
            out.append(line)
            continue

        if in_code:
            out.append(line)
            continue

        m = num_re.match(line)
        if m:
            indent_str = m.group(1)
            indent_len = len(indent_str)
            content = m.group(3)

            if active_list_indent is None or indent_len != active_list_indent:
                for k in list(counter_for_indent.keys()):
                    if k >= indent_len:
                        del counter_for_indent[k]
                active_list_indent = indent_len
                counter_for_indent[indent_len] = 1
            else:
                counter_for_indent[indent_len] = counter_for_indent.get(indent_len, 0) + 1

            n = counter_for_indent[indent_len]
            out.append(f"{indent_str}{n}. {content}")
        else:
            if active_list_indent is not None:
                leading_spaces = len(line) - len(line.lstrip(" "))
                if line.strip() == "":
                    out.append(line)
                    continue
                if leading_spaces > active_list_indent:
                    out.append(line)
                    continue
                active_list_indent = None
                counter_for_indent.clear()
            out.append(line)

    return "\n".join(out)

def _extract_section_by_h2(md: str, target_keys: List[str], stop_keys: List[str]) -> str:
    lines = (md or "").splitlines()
    h2s = _find_h2_positions(md)
    if not h2s:
        return ""
    target_keys_n = set(_norm_heading_key(k) for k in target_keys)
    stop_keys_n   = set(_norm_heading_key(k) for k in stop_keys)

    start_idx = None
    for (i, title, key) in h2s:
        if key in target_keys_n:
            start_idx = i
            break
    if start_idx is None:
        return ""

    stop_idx = None
    for (i, title, key) in h2s:
        if i > start_idx and key in stop_keys_n:
            stop_idx = i
            break

    from_line = start_idx + 1
    to_line = stop_idx if stop_idx is not None else len(lines)
    chunk = "\n".join(lines[from_line:to_line]).strip()
    return chunk

def select_video_or_lesson(md: str) -> str:
    video = _extract_section_by_h2(
        md,
        target_keys=["Videó szöveg", "Video szoveg", "Videó szöveg:", "Videó szöveg –"],
        stop_keys=["Lecke szöveg", "Lecke szöveg:", "Megjegyzés", "Megjegyzes", "Videó szöveg", "Video szoveg"]
    )
    if re.search(r"\S", video):
        return fix_numbered_lists(video)

    lesson = _extract_section_by_h2(
        md,
        target_keys=["Lecke szöveg", "Lecke szoveg", "Lecke szöveg:", "Lecke szöveg –"],
        stop_keys=["Videó szöveg", "Video szoveg", "Videó szöveg:", "Megjegyzés", "Megjegyzes", "Lecke szöveg", "Lecke szoveg"]
    )
    if re.search(r"\S", lesson):
        return fix_numbered_lists(lesson)

    return ""

# ────────────────────────────────────────────────────────────────────────────────
# Export / közös sor-gyűjtő
# ────────────────────────────────────────────────────────────────────────────────
def resolve_sorts(order_prop: Optional[str]) -> Tuple[List[Dict], str]:
    if order_prop:
        return [{"property": order_prop, "direction": "ascending"}], f"property: {order_prop} ↑"
    title_prop = resolve_title_prop_name()
    if title_prop:
        return [{"property": title_prop, "direction": "ascending"}], f"title: {title_prop} ↑"
    return [], "unspecified (API default)"

def collect_rows_for_display_group(display_name: str, canonical_names: Set[str]) -> List[Dict[str, str]]:
    """Ugyanaz a logika, mint a CSV exportban – csak listát ad vissza."""
    ptype = get_property_type()
    section_prop, order_prop = resolve_section_and_order_props()
    sorts, _sort_desc = resolve_sorts(order_prop)

    pages: List[Dict] = []
    for nm in sorted(canonical_names, key=lambda s: (0 if s == display_name else 1, s)):
        try:
            subset = query_filtered_pages(filter_=build_filter(ptype, nm), sorts=sorts)
        except APIResponseError:
            subset = []
        if subset:
            pages = subset
            break

    rows: List[Dict[str, str]] = []
    for page in pages:
        pid   = page.get("id")
        title = extract_title(page)
        try:
            raw_md = blocks_to_md(pid).strip()
            content = select_video_or_lesson(raw_md)
        except Exception as e:
            content = f"[HIBA: {e}]"

        rows.append({
            "oldal_cime": title,
            "szakasz": format_property_for_csv(page, section_prop),
            "sorszam": format_property_for_csv(page, order_prop) if order_prop else "",
            "tartalom": content,
        })
        time.sleep(0.01)
    return rows

def export_one(display_name: str, canonical_names: Set[str]) -> bytes:
    """CSV export – változatlanul a korábbihoz képest."""
    rows = collect_rows_for_display_group(display_name, canonical_names)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDNAMES)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return buf.getvalue().encode("utf-8")

# ────────────────────────────────────────────────────────────────────────────────
# Google Sheets segédfüggvények
# ────────────────────────────────────────────────────────────────────────────────
def sheets_enabled() -> bool:
    return bool(GS_SHEET_ID and GS_SA_JSON)

def _gs_client():
    try:
        import gspread
    except Exception as e:
        st.error("A Google Sheets-hez szükséges a 'gspread' csomag. Add hozzá a requirements.txt-hez.")
        raise
    try:
        creds = json.loads(GS_SA_JSON)
    except Exception as e:
        st.error(f"GOOGLE_SERVICE_ACCOUNT JSON nem olvasható: {e}")
        raise
    try:
        return gspread.service_account_from_dict(creds)
    except Exception as e:
        st.error(f"Google service account hitelesítés sikertelen: {e}")
        raise

def _gs_open_spreadsheet():
    gc = _gs_client()
    try:
        return gc.open_by_key(GS_SHEET_ID)
    except Exception as e:
        st.error(f"Spreadsheet megnyitása sikertelen: {e}")
        raise

def _sanitize_sheet_title(name: str) -> str:
    name = re.sub(r'[:\\/?*\\[\\]]', '_', name)
    return name[:31] if len(name) > 31 else name or "lap"

def _gs_get_or_create_ws(sh, title: str):
    safe = _sanitize_sheet_title(title)
    try:
        return sh.worksheet(safe)
    except Exception:
        try:
            return sh.add_worksheet(title=safe, rows=100, cols=10)
        except Exception as e:
            st.error(f"Worksheet létrehozása sikertelen ({safe}): {e}")
            raise

def _ws_clear_and_header(ws, header: List[str]):
    try:
        ws.clear()
        ws.update("A1", [header])
    except Exception as e:
        st.error(f"Worksheet tisztítás/fejléc hiba ({ws.title}): {e}")
        raise

def _ws_append_rows(ws, rows: List[List[str]]):
    CHUNK = 200
    for i in range(0, len(rows), CHUNK):
        batch = rows[i:i+CHUNK]
        for attempt in range(1, 5):
            try:
                ws.append_rows(batch, value_input_option="RAW", table_range="A1")
                break
            except Exception:
                time.sleep(0.6 * attempt)
                if attempt == 4:
                    raise

def sync_all_groups_to_sheets(items: List[Tuple[str, int, Set[str]]], canon_by_name: Dict[str, Set[str]]) -> None:
    """
    Google Sheets szinkron – a LEGKISEBBTŐL a legnagyobbig.
    Minden csoport külön munkalapra kerül, a CSV-vel AZONOS oszlopokkal és tartalommal.
    """
    if not sheets_enabled():
        st.error("Google Sheets nincs bekapcsolva (GOOGLE_SHEETS_SPREADSHEET_ID / GOOGLE_SERVICE_ACCOUNT).")
        return

    sh = _gs_open_spreadsheet()
    items_asc = sorted(items, key=lambda t: (t[1], t[0].lower()))  # (count ↑, név ABC)

    bar = st.progress(0, text="Google Sheets szinkron indul…")
    log = st.empty()

    total = len(items_asc)
    for idx, (display_name, count, _canon) in enumerate(items_asc, start=1):
        log.info(f"„{display_name}” – adatok gyűjtése és feltöltése… (kb. {count} oldal)")
        rows = collect_rows_for_display_group(display_name, canon_by_name[display_name])

        ws = _gs_get_or_create_ws(sh, display_name)
        _ws_clear_and_header(ws, CSV_FIELDNAMES)
        values = [[r.get("oldal_cime",""), r.get("szakasz",""), r.get("sorszam",""), r.get("tartalom","")] for r in rows]
        if values:
            _ws_append_rows(ws, values)

        pct = int(idx * 100 / total)
        bar.progress(pct, text=f"Google Sheets szinkron: {idx}/{total} kész – utolsó: {ws.title} ({len(values)} sor)")

    st.success("Google Sheets szinkron kész ✅")

# ────────────────────────────────────────────────────────────────────────────────
# UI
# ────────────────────────────────────────────────────────────────────────────────
st.title("📦 Notion export – Kurzus")
st.caption("Rendezés: Sorszám ↑, különben ABC cím ↑. A „tartalom” a teljes Videó szöveg (ha üres: Lecke szöveg) – belső H2-ket is tartalmaz, a számozott listák automatikusan újraszámozva.")

# Jelszó
if need_auth():
    if not APP_PASSWORD:
        st.warning("Admin: állítsd be az APP_PASSWORD változót / Secrets-et a jelszóhoz.")
    login_form()
    st.stop()

# Fő felület
try:
    items = build_display_list()  # [(display_name, count, canon_set)]
except Exception as e:
    st.error(f"Hiba a Notion lekérésnél: {e}")
    st.stop()

if not items:
    st.info("Nem találtam „Kurzus” értékeket.")
    st.stop()

# Választó
labels = [f"{name} ({count})" for name, count, _ in items]
name_by_label = {labels[i]: items[i][0] for i in range(len(items))}
canon_by_name = {items[i][0]: items[i][2] for i in range(len(items))}

# Tájékoztatás: mely property-t/sortot használunk
sec_prop, ord_prop = resolve_section_and_order_props()
sorts, sorts_desc = resolve_sorts(ord_prop)
with st.expander("Részletek (felismert mezők és rendezés)"):
    st.write(f"**Szakasz mező**: `{sec_prop or '— (nem találtam; üres lesz a CSV-ben)'}`")
    st.write(f"**Sorszám mező**: `{ord_prop or '— (nincs; ABC cím szerint rendezünk)'}`")
    st.write(f"**Rendezés**: {sorts_desc}")

pick = st.multiselect("Válaszd ki, mit exportáljunk:", labels, max_selections=None)

col1, col2 = st.columns(2)

with col1:
    if st.button("Exportálás (CSV)"):
        if not pick:
            st.warning("Válassz legalább egy elemet.")
        else:
            for lbl in pick:
                name = name_by_label[lbl]
                data = export_one(name, canon_by_name[name])
                fname_safe = re.sub(r"[^\w\-. ]", "_", name).strip().replace(" ", "_")
                st.download_button(
                    label=f"Letöltés: {name}.csv",
                    data=data,
                    file_name=f"export_Kurzus_{fname_safe}.csv",
                    mime="text/csv",
                    key=f"dl-{fname_safe}",
                )

with col2:
    st.markdown("**Google Sheets szinkron** – minden csoport külön lapra, **legkisebbtől a legnagyobbig**.")
    if sheets_enabled():
        if st.button("Google Sheet szinkron indítása"):
            try:
                sync_all_groups_to_sheets(items, canon_by_name)
            except Exception as e:
                st.error(f"Szinkron hiba: {e}")
    else:
        st.info("A Google Sheets szinkronhoz add meg a `GOOGLE_SHEETS_SPREADSHEET_ID` és `GOOGLE_SERVICE_ACCOUNT` secreteket.")
