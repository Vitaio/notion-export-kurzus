import os
import io
import csv
import time
import re
import json
import zipfile
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from collections import Counter, defaultdict

import streamlit as st
from notion_client import Client
from notion_client.errors import APIResponseError

# ────────────────────────────────────────────────────────────────────────────────
# Page config
# ────────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Notion export – Kurzus", page_icon="📦", layout="wide")

# ────────────────────────────────────────────────────────────────────────────────
# Secrets → env bridge
# ────────────────────────────────────────────────────────────────────────────────
try:
    for k in ("NOTION_API_KEY", "NOTION_DATABASE_ID", "APP_PASSWORD", "NOTION_PROPERTY_NAME"):
        if k in st.secrets and not os.getenv(k):
            os.environ[k] = str(st.secrets[k])
except Exception:
    pass

# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────
NOTION_API_KEY = os.getenv("NOTION_API_KEY", "").strip()
DATABASE_ID    = os.getenv("NOTION_DATABASE_ID", "").strip()
APP_PASSWORD   = os.getenv("APP_PASSWORD", "").strip()
PROPERTY_NAME  = os.getenv("NOTION_PROPERTY_NAME", "Kurzus").strip()

# max cella-hossz CSV-ben; felette tartalom_cont_X oszlopokba daraboljuk
MAX_CONTENT_CHARS = int(os.getenv("MAX_CONTENT_CHARS", "40000"))

DISPLAY_RENAMES: Dict[str, str] = {
    "Üzleti Modellek": "Milyen vállalkozást indíts",
    "Marketing rendszerek": "Ügyfélszerző marketing rendszerek",
}

CSV_FIELDNAMES = ["oldal_cime", "szakasz", "sorszam", "tartalom"]  # per-kurzus CSV alap
EXPORTS_ROOT = "exports"

# ────────────────────────────────────────────────────────────────────────────────
# Auth
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
# Notion client + schema
# ────────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_client() -> Client:
    if not NOTION_API_KEY:
        raise RuntimeError("Hiányzó NOTION_API_KEY")
    return Client(auth=NOTION_API_KEY)

@st.cache_data(ttl=120)
def get_database_schema() -> Dict:
    client = get_client()
    return client.databases.retrieve(database_id=DATABASE_ID)

@st.cache_data(ttl=300)
def get_property_type() -> str:
    db = get_database_schema()
    props: Dict[str, Dict] = db.get("properties", {}) or {}
    if PROPERTY_NAME not in props:
        raise RuntimeError(f"A(z) '{PROPERTY_NAME}' property nem található az adatbázisban.")
    return props[PROPERTY_NAME]["type"]

@st.cache_data(ttl=300)
def schema_id_to_current_name() -> Dict[str, str]:
    db = get_database_schema()
    prop = db.get("properties", {}).get(PROPERTY_NAME, {})
    ptype = prop.get("type")
    res: Dict[str, str] = {}
    if ptype in ("select", "status"):
        opts = (prop.get(ptype, {}) or {}).get("options", []) or []
        for o in opts:
            res[o["id"]] = (o.get("name") or "").strip()
    elif ptype == "multi_select":
        opts = (prop.get("multi_select", {}) or {}).get("options", []) or []
        for o in opts:
            res[o["id"]] = (o.get("name") or "").strip()
    return res

def with_backoff(fn, *args, **kwargs):
    for i in range(6):
        try:
            return fn(*args, **kwargs)
        except APIResponseError as e:
            status = getattr(e, "status", None)
            if status in (429, 500, 502, 503):
                time.sleep((2 ** i) + 0.1)
                continue
            raise

def query_all_pages() -> List[Dict]:
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
    client = get_client()
    results: List[Dict] = []
    cursor = None
    while True:
        kwargs = {
            "database_id": DATABASE_ID,
            "filter": filter_,
            "page_size": 100,
            "start_cursor": cursor
        }
        if sorts:
            kwargs["sorts"] = sorts
        resp = with_backoff(client.databases.query, **kwargs)
        results.extend(resp.get("results", []) or [])
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return results

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _norm_key(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", "", s or "").strip().lower()

def format_rich_text(rt_array: List[Dict]) -> str:
    out = []
    for r in rt_array or []:
        t = r.get("plain_text", "")
        ann = (r.get("annotations") or {})
        if ann.get("code"):      t = f"`{t}`"
        if ann.get("bold"):      t = f"**{t}**"
        if ann.get("italic"):    t = f"*{t}*"
        if ann.get("strikethrough"): t = f"~~{t}~~"
        out.append(t)
    return "".join(out)

@st.cache_data(ttl=300)
def collect_used_ids_and_names() -> Tuple[Dict[str, int], Dict[str, Set[str]]]:
    ptype = get_property_type()
    pages = query_all_pages()
    used_by_id: Dict[str, int] = Counter()
    names_seen: Dict[str, Set[str]] = defaultdict(set)

    for page in pages:
        props = page.get("properties", {}) or {}
        prop = props.get(PROPERTY_NAME, {}) or {}
        if ptype == "select":
            sl = prop.get("select") or {}
            oid = sl.get("id")
            if oid:
                used_by_id[oid] += 1
                name = (sl.get("name") or "").strip()
                if name:
                    names_seen[oid].add(name)
        elif ptype == "multi_select":
            arr = prop.get("multi_select") or []
            for sl in arr:
                oid = sl.get("id")
                if oid:
                    used_by_id[oid] += 1
                    name = (sl.get("name") or "").strip()
                    if name:
                        names_seen[oid].add(name)
        elif ptype == "status":
            stv = prop.get("status") or {}
            oid = stv.get("id")
            if oid:
                used_by_id[oid] += 1
                name = (stv.get("name") or "").strip()
                if name:
                    names_seen[oid].add(name)
    return dict(used_by_id), {k: set(v) for k, v in names_seen.items()}

@st.cache_data(ttl=300)
def build_display_list() -> List[Tuple[str, int, Set[str]]]:
    used_by_id, names_seen = collect_used_ids_and_names()
    id2current = schema_id_to_current_name()

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
    # UI-ban nem kommunikáljuk a belső sorrendet
    items.sort(key=lambda x: (x[0].lower()))
    return items

def build_filter(ptype: str, name: str) -> Dict:
    if ptype == "select":
        return {"property": PROPERTY_NAME, "select": {"equals": name}}
    if ptype == "multi_select":
        return {"property": PROPERTY_NAME, "multi_select": {"contains": name}}
    if ptype == "status":
        return {"property": PROPERTY_NAME, "status": {"equals": name}}
    raise RuntimeError(f"Nem támogatott property típus: {ptype}")

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
                elif btype == "to_do":
                    checked = "x" if data.get("checked") else " "
                    prefix = f"- [{checked}] "
                elif btype == "callout":            prefix = "> "
                elif btype == "toggle":             prefix = "▸ "

                if prefix:
                    line = indent + prefix + txt
                else:
                    line = indent + txt

            elif btype == "code":
                lang = (data.get("language") or "").strip()
                code = data.get("rich_text", [])
                content = "".join([t.get("plain_text", "") for t in code])
                line = f"```{lang}\n{content}\n```"

            elif btype == "divider":
                line = "---"

            if line:
                lines.append(line)

            if block.get("has_children"):
                lines.append(blocks_to_md(block["id"], depth=depth + 1))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return "\n".join(lines).strip()

def _split_h2_sections(md: str) -> Dict[str, List[str]]:
    sections: Dict[str, List[str]] = {}
    current = None
    lines = md.splitlines()
    for ln in lines:
        if ln.startswith("## "):
            current = ln[3:].strip()
            sections[current] = []
        else:
            if current is not None:
                sections[current].append(ln)
    return sections

def _join(lines: List[str]) -> str:
    return "\n".join(lines).strip()

def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower()
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
        key = lookup.get(_norm_key(cand)); 
        if key: sec_key = key; break
    if not sec_key:
        for k, v in props.items():
            if v.get("type") in ("select", "multi_select", "status"):
                sec_key = k; break

    ord_key = ""
    for cand in ORDER_TARGETS + ["sorszám"]:
        key = lookup.get(_norm_key(cand));
        if key: ord_key = key; break
    if not ord_key:
        for k, v in props.items():
            if v.get("type") == "number":
                ord_key = k; break

    return sec_key, ord_key

def extract_title(page: Dict) -> str:
    props = page.get("properties", {}) or {}
    title_key = None
    for k, v in props.items():
        if v.get("type") == "title":
            title_key = k; break
    if not title_key:
        for k in ("Lecke címe", "Lecke cime", "Cím", "Cim", "Name", "Név"):
            if k in props and props[k].get("type") in ("title", "rich_text"):
                title_key = k; break
    if not title_key:
        return "(cím nélkül)"
    title_arr = props[title_key].get("title") or props[title_key].get("rich_text") or []
    title = "".join([t.get("plain_text", "") for t in title_arr]).strip()
    return title or "(cím nélkül)"

def format_property_for_csv(page: Dict, prop_name: str) -> str:
    props = page.get("properties", {}) or {}
    p = props.get(prop_name, {}) or {}
    ptype = p.get("type")
    try:
        if ptype == "number":
            v = p.get("number"); return "" if v is None else str(v)
        if ptype == "select":
            s = p.get("select") or {}; return (s.get("name") or "").strip()
        if ptype == "multi_select":
            arr = p.get("multi_select") or []; return ", ".join([(x.get("name") or "").strip() for x in arr if x.get("name")])
        if ptype == "status":
            s = p.get("status") or {}; return (s.get("name") or "").strip()
        if ptype == "rich_text":
            arr = p.get("rich_text") or []; return "".join([t.get("plain_text", "") for t in arr]).strip()
        if ptype == "date":
            d = p.get("date") or {}; return (d.get("start") or "") + ((" – " + d.get("end")) if d.get("end") else "")
        if ptype == "url": return p.get("url") or ""
        if ptype == "email": return p.get("email") or ""
        if ptype == "people":
            arr = p.get("people") or []; names = []
            for person in arr:
                name = (person.get("name") or "").strip()
                if not name: name = (person.get("person", {}) or {}).get("email", "") or ""
                if name: names.append(name)
            return ", ".join(names)
        return ""
    except Exception:
        return ""

def resolve_sorts(order_prop: Optional[str]) -> Tuple[List[Dict], str]:
    if order_prop:
        return [{"property": order_prop, "direction": "ascending"}], f"Sorszám (`{order_prop}`) szerint növekvő"
    db = get_database_schema()
    title_key = None
    for k, v in (db.get("properties", {}) or {}).items():
        if v.get("type") == "title":
            title_key = k; break
    if not title_key: title_key = "Name"
    return [{"property": title_key, "direction": "ascending"}], f"Cím (`{title_key}`) szerint ABC növekvő"

# ────────────────────────────────────────────────────────────────────────────────
# Markdown tisztítás + kivágás
# ────────────────────────────────────────────────────────────────────────────────
def clean_markdown(md: str) -> str:
    """Kíméletes tisztítás: címsorok, idézetek, whitespace normalizálás, kódblokkok érintetlenek."""
    if not md:
        return ""
    # ##Miért → ## Miért
    md = re.sub(r"^(#+)([^\s#])", r"\1 \2", md, flags=re.M)
    # headingek/idézetek előtt üres sor
    md = re.sub(r"(\n#+\s)", r"\n\n\1", md)
    md = re.sub(r"(\n>\s)",  r"\n\n\1", md)
    # 3+ üres sor → 1 üres sor
    md = re.sub(r"\n{3,}", "\n\n", md)
    # idézet-lista kisimítás (ha Notionból így jön)
    md = re.sub(r"^>\s-\s", "- ", md, flags=re.M)
    # felesleges formázások takarítása
    md = md.replace("****", "")
    return md.strip()

def select_video_or_lesson_with_type(md: str) -> Tuple[str, Optional[str]]:
    """Visszaadja a kivágott szöveget és a típust: 'video_szoveg' / 'lecke_szoveg' / None."""
    sections = _split_h2_sections(md)

    def pick(variants: List[str]) -> Optional[str]:
        targets = set(_normalize(v) for v in variants)
        for k in sections.keys():
            if _normalize(k) in targets:
                body = _join(sections[k]).strip()
                if body:
                    return body
        return None

    video = pick(["Videó szöveg", "Video szoveg", "Video szöveg", "Videó szoveg", "Videó", "Video"])
    if video:
        return video, "video_szoveg"
    lesson = pick(["Lecke szöveg", "Lecke szoveg", "Lecke", "Lecke anyag"])
    if lesson:
        return lesson, "lecke_szoveg"
    return "", None

# ────────────────────────────────────────────────────────────────────────────────
# Hosszú cella darabolás (CSV-hez)
# ────────────────────────────────────────────────────────────────────────────────
def _split_content_for_csv(text: str, max_len: int) -> Dict[str, str]:
    """
    A 'tartalom' mezőt több oszlopra vágja: tartalom, tartalom_cont_1, ...
    Soft-split: lehetőleg \n\n határon vágunk; fallback a kemény vágás.
    """
    text = text or ""
    if len(text) <= max_len:
        return {"tartalom": text}

    parts: List[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + max_len, n)
        # soft cut: keressük az utolsó dupla sortörést az ablak végéhez közel
        window = text[start:end]
        cut = window.rfind("\n\n")
        if cut >= int(max_len * 0.6):  # ne vágjunk túl korán
            end = start + cut
        part = text[start:end].rstrip()
        if part:
            parts.append(part)
        start = end

    out: Dict[str, str] = {}
    for i, p in enumerate(parts):
        if i == 0:
            out["tartalom"] = p
        else:
            out[f"tartalom_cont_{i}"] = p
    return out

def _max_cont_cols(rows: List[Dict[str, str]]) -> int:
    """Visszaadja, hány tartalom_cont_X oszlopra van szükség maximum (X max értéke)."""
    mx = 0
    for r in rows:
        for k in r.keys():
            m = re.match(r"tartalom_cont_(\d+)$", k)
            if m:
                mx = max(mx, int(m.group(1)))
    return mx

# ────────────────────────────────────────────────────────────────────────────────
# Sorépítés – közös logika (dupla kód elkerülésére)
# ────────────────────────────────────────────────────────────────────────────────
def _pages_for_group(display_name: str, canonical_names: Set[str]) -> List[Dict]:
    """Lekéri az adott csoporthoz tartozó oldalakat a rendezési szabállyal."""
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
    return pages

def _row_from_page(page: Dict) -> Tuple[Dict[str, str], Optional[str]]:
    """Felépít egy alap sort egy Notion oldalból + visszaadja a section_type-ot (ha van)."""
    section_prop, order_prop = resolve_section_and_order_props()
    title = extract_title(page)
    section_val = format_property_for_csv(page, section_prop) if section_prop else ""
    order_val   = format_property_for_csv(page, order_prop) if order_prop else ""

    md = blocks_to_md(page["id"])
    chosen, section_type = select_video_or_lesson_with_type(md)
    if chosen:
        chosen = fix_numbered_lists(chosen)
        chosen = clean_markdown(chosen)

    base = {
        "oldal_cime": title,
        "szakasz": section_val,
        "sorszam": order_val,
        "tartalom": chosen or "",
    }
    return base, section_type

# ────────────────────────────────────────────────────────────────────────────────
# Eredeti per-kurzus export (MEGMARAD) – kiegészítve clean+split-tel
# ────────────────────────────────────────────────────────────────────────────────
def export_one(display_name: str, canonical_names: Set[str]) -> bytes:
    pages = _pages_for_group(display_name, canonical_names)

    # nincs oldal → üres táblázat az eredeti 4 fejléccel
    if not pages:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        return output.getvalue().encode("utf-8")

    # felépítjük a sorokat, clean + hosszú tartalom darabolása
    rows_base: List[Dict[str, str]] = []
    for pg in pages:
        base, _stype = _row_from_page(pg)
        rows_base.append(base)

    # darabolás oszlopokba (csak ha kell)
    split_rows: List[Dict[str, str]] = []
    for r in rows_base:
        chunks = _split_content_for_csv(r.get("tartalom", ""), MAX_CONTENT_CHARS)
        out = dict(r)
        out.pop("tartalom", None)
        out.update(chunks)
        split_rows.append(out)

    # fejléc: alap + annyi tartalom_cont_X, amennyit a legnagyobb sor igényel
    max_extra = _max_cont_cols(split_rows)
    fieldnames = ["oldal_cime", "szakasz", "sorszam", "tartalom"] + [f"tartalom_cont_{i}" for i in range(1, max_extra + 1)]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in split_rows:
        # töltsük fel a hiányzó cont oszlopokat üressel
        for i in range(1, max_extra + 1):
            r.setdefault(f"tartalom_cont_{i}", "")
        writer.writerow(r)
    return output.getvalue().encode("utf-8")

# ────────────────────────────────────────────────────────────────────────────────
# ÚJ: Összes – egy munkalap (CSV, Google Sheets-hez)
# ────────────────────────────────────────────────────────────────────────────────
def export_unified_sheet(groups_display: List[Tuple[str,int,Set[str]]]) -> bytes:
    """
    Minden kurzus/csoport egyetlen táblába kerül.
    Fejléc: course, oldal_cime, szakasz, sorszam, section_type, tartalom, tartalom_cont_X...
    """
    all_rows: List[Dict[str, str]] = []

    for display_name, _count, canon in groups_display:
        pages = _pages_for_group(display_name, canon)
        if not pages:
            continue
        for pg in pages:
            base, section_type = _row_from_page(pg)
            # unified: tegyük be a 'course' és 'section_type' mezőt
            row = {
                "course": display_name,
                "oldal_cime": base["oldal_cime"],
                "szakasz": base["szakasz"],
                "sorszam": base["sorszam"],
                "section_type": section_type or "",
                "tartalom": base["tartalom"],
            }
            # tartalom darabolása (ha hosszú)
            chunks = _split_content_for_csv(row.get("tartalom", ""), MAX_CONTENT_CHARS)
            row.pop("tartalom", None)
            row.update(chunks)
            all_rows.append(row)

    # nincs adat → üres tábla alap fejléccel
    if not all_rows:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["course", "oldal_cime", "szakasz", "sorszam", "section_type", "tartalom"])
        writer.writeheader()
        return output.getvalue().encode("utf-8")

    # mennyi tartalom_cont_X kell?
    max_extra = _max_cont_cols(all_rows)
    fieldnames = ["course", "oldal_cime", "szakasz", "sorszam", "section_type", "tartalom"] + [f"tartalom_cont_{i}" for i in range(1, max_extra + 1)]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in all_rows:
        for i in range(1, max_extra + 1):
            r.setdefault(f"tartalom_cont_{i}", "")
        writer.writerow(r)
    return output.getvalue().encode("utf-8")

# ────────────────────────────────────────────────────────────────────────────────
# Bulk engine with progress cards + checkpoints
# ────────────────────────────────────────────────────────────────────────────────
def _slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s-]+", "_", s)
    return s[:80] if len(s) > 80 else s

def _ensure_dir(p: str): os.makedirs(p, exist_ok=True)
def _run_dir(run_id: str) -> str: return os.path.join(EXPORTS_ROOT, f"run_{run_id}")
def _checkpoint_path(run_id: str) -> str: return os.path.join(_run_dir(run_id), "checkpoint.json")

def _append_log(run_id: str, msg: str):
    rd = _run_dir(run_id); _ensure_dir(rd)
    try:
        with open(os.path.join(rd, "run_log.txt"), "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}\n")
    except Exception:
        pass

def _save_checkpoint(run_id: str, state: dict):
    rd = _run_dir(run_id); _ensure_dir(rd)
    with open(_checkpoint_path(run_id), "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def _load_checkpoint(run_id: str) -> Optional[dict]:
    try:
        with open(_checkpoint_path(run_id), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _write_csv_file(run_id: str, display_name: str, data: bytes) -> str:
    rd = _run_dir(run_id); _ensure_dir(rd)
    fn = f"export_{_slug(display_name)}.csv"
    fp = os.path.join(rd, fn)
    with open(fp, "wb") as f:
        f.write(data)
    return fp

def _zip_folder(folder: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(folder):
            for name in sorted(files):
                if name.endswith(".json") or name.endswith(".txt"):
                    continue
                fp = os.path.join(root, name)
                arcname = os.path.relpath(fp, folder)
                zf.write(fp, arcname)
    buf.seek(0)
    return buf.read()

def _retry_export_one(display_name: str, canon_set: Set[str], export_one_fn, run_id: str, max_tries: int = 3):
    last_exc = None
    for attempt in range(1, max_tries + 1):
        try:
            _append_log(run_id, f"START {display_name} (attempt {attempt}/{max_tries})")
            data = export_one_fn(display_name, canon_set)
            _append_log(run_id, f"SUCCESS {display_name}")
            return data, attempt-1  # hány újrapróba kellett
        except APIResponseError as e:
            last_exc = e
            _append_log(run_id, f"API ERROR {display_name}: {getattr(e, 'status', '?')} – {e!r}")
            if getattr(e, "status", None) not in (429, 500, 502, 503):
                break
        except Exception as e:
            last_exc = e
            _append_log(run_id, f"ERROR {display_name}: {e!r}")
        time.sleep(2 ** (attempt - 1))
    _append_log(run_id, f"FINAL FAIL {display_name}: {last_exc!r}")
    return None, max_tries-1

def _init_run(groups_display: List[Tuple[str,int,Set[str]]]):
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    rd = _run_dir(run_id); _ensure_dir(rd)
    ordered = sorted(groups_display, key=lambda t: (t[1], t[0]))
    state = {
        "run_id": run_id,
        "created_at": datetime.now().isoformat(),
        "completed": [],
        "pending": [d for d, _, _ in ordered],
        "failed": [],
        "retries": 0,
        "total": len(ordered),
        "eta_sec_per_item": None,
        "durations": [],
    }
    _save_checkpoint(run_id, state)
    _append_log(run_id, "=== ÚJ FUTÁS INDULT ===")
    return run_id, state

def _resume_or_new_run(groups_display: List[Tuple[str,int,Set[str]]]):
    run_id = st.session_state.get("current_run_id")
    if run_id:
        cp = _load_checkpoint(run_id)
        if cp: 
            return run_id, cp
    run_id, cp = _init_run(groups_display)
    st.session_state["current_run_id"] = run_id
    return run_id, cp

# ────────────────────────────────────────────────────────────────────────────────
# UI helpers: progress cards
# ────────────────────────────────────────────────────────────────────────────────
class ProgressUI:
    def __init__(self, run_id: str, ordered: List[Tuple[str,int,Set[str]]]):
        self.run_id = run_id
        self.total = len(ordered)
        self.rows = {}
        self.global_progress_placeholder = st.empty()
        self.eta_placeholder = st.empty()
        self.grid = st.container()

        cp = _load_checkpoint(run_id) or {}
        completed = set(cp.get("completed", []))
        failed = set(cp.get("failed", []))

        with self.grid:
            for name, count, _ in ordered:
                ph = st.container(border=True)
                with ph:
                    col1, col2 = st.columns([0.6, 0.4])
                    with col1:
                        st.markdown(f"**{name}**  \n{int(count)} oldal")
                    with col2:
                        self.rows[name] = {
                            "status": st.empty(),
                            "pbar": st.progress(0.0),
                            "note": st.empty(),
                        }
                if name in completed:
                    self.set_status(name, "done")
                elif name in failed:
                    self.set_status(name, "error")
                else:
                    self.set_status(name, "pending")

        self.update_global(cp)

    def set_status(self, name: str, status: str, note: str = ""):
        icons = {"done": "🟢 Kész", "running": "🟡 Folyamatban…", "error": "🔴 Hiba", "pending": "⚪ Várakozik"}
        row = self.rows[name]
        row["status"].markdown(f"**Állapot:** {icons.get(status, status)}")
        if status == "running":
            row["pbar"].progress(0.3)
        elif status == "done":
            row["pbar"].progress(1.0)
        elif status == "error":
            row["pbar"].progress(0.0)
        else:
            row["pbar"].progress(0.0)
        if note:
            row["note"].write(note)

    def update_global(self, cp: dict):
        total = cp.get("total", self.total)
        done = len(cp.get("completed", []))
        failed = len(cp.get("failed", []))
        retries = int(cp.get("retries", 0))
        pct = 0.0 if total == 0 else done / total
        self.global_progress_placeholder.progress(pct, text=f"Össz-progressz: {done}/{total} kész ({int(pct*100)}%)")
        eta_per = cp.get("eta_sec_per_item")
        if eta_per:
            remaining = total - done
            eta_sec = max(0, int(remaining * eta_per))
            mins = eta_sec // 60
            secs = eta_sec % 60
            self.eta_placeholder.info(f"**Előrehaladás összefoglaló:** {done}/{total} csoport exportálva ({int(pct*100)}%) — várható hátralévő idő: {mins} perc {secs:02d} mp | 🔁 újrapróbálások: {retries}")
        else:
            self.eta_placeholder.info(f"**Előrehaladás összefoglaló:** {done}/{total} csoport exportálva ({int(pct*100)}%) | 🔁 újrapróbálások: {retries}")

    def summary_box(self, cp: dict):
        total = cp.get("total", self.total)
        done = len(cp.get("completed", []))
        failed = len(cp.get("failed", []))
        retries = int(cp.get("retries", 0))
        st.success(f"Összegzés: ✅ {done} sikeres, ⚠️ {failed} hiba, 🔁 {retries} újrapróbálás (összesen: {total})")

# ────────────────────────────────────────────────────────────────────────────────
# Bulk export core
# ────────────────────────────────────────────────────────────────────────────────
def export_engine(run_id: str, groups_display: List[Tuple[str,int,Set[str]]]):
    ordered = sorted(groups_display, key=lambda t: (t[1], t[0]))
    cp = _load_checkpoint(run_id)
    if not cp:
        cp = {
            "run_id": run_id,
            "created_at": datetime.now().isoformat(),
            "completed": [],
            "pending": [d for d,_,_ in ordered],
            "failed": [],
            "retries": 0,
            "total": len(ordered),
            "eta_sec_per_item": None,
            "durations": []
        }
        _save_checkpoint(run_id, cp)

    ui = ProgressUI(run_id, ordered)

    for name, count, canon in ordered:
        if name in cp["completed"]:
            ui.set_status(name, "done")
            continue

        ui.set_status(name, "running")
        t0 = time.time()
        data, retry_count = _retry_export_one(name, canon, export_one, run_id, max_tries=3)
        cp["retries"] = int(cp.get("retries", 0)) + retry_count

        if data is None:
            cp["failed"] = sorted(set(cp.get("failed", [])) | {name})
            _save_checkpoint(run_id, cp)
            ui.set_status(name, "error", note="Hibás export. Automatikus folytatás során újrapróbáljuk.")
            continue

        _write_csv_file(run_id, name, data)
        elapsed = max(0.1, time.time() - t0)
        durs = cp.get("durations", [])
        durs.append(elapsed)
        if len(durs) > 10:
            durs = durs[-10:]
        cp["durations"] = durs
        cp["eta_sec_per_item"] = sum(durs) / len(durs)

        cp["completed"] = sorted(set(cp.get("completed", [])) | {name})
        if name in cp.get("failed", []):
            cp["failed"] = [x for x in cp["failed"] if x != name]
        _save_checkpoint(run_id, cp)

        ui.set_status(name, "done")
        ui.update_global(cp)

    done = len(cp.get("completed", []))
    total = cp.get("total", len(ordered))
    if done == total:
        zip_bytes = _zip_folder(_run_dir(run_id))
        st.success("Export kész!")
        ui.summary_box(cp)
        st.download_button("ZIP letöltése", data=zip_bytes, file_name=f"notion_kurzus_export_{run_id}.zip", mime="application/zip")
        _append_log(run_id, "=== KÉSZ ===")

# ────────────────────────────────────────────────────────────────────────────────
# UI – main
# ────────────────────────────────────────────────────────────────────────────────
st.title("📦 Notion export – Kurzus")
st.caption("Rendezés: Sorszám ↑, különben ABC cím ↑. A „tartalom” a „Videó szöveg”/„Lecke szöveg” H2 alatti rész; a számozott listák automatikusan 1., 2., 3.… formára újraszámozva. Hosszú cellák 40 000+ karakternél további oszlopokba törve.")

if need_auth():
    if not APP_PASSWORD:
        st.warning("Admin: állítsd be az APP_PASSWORD változót / Secrets-et a jelszóhoz.")
    login_form()
    st.stop()

try:
    items = build_display_list()
except Exception as e:
    st.error(f"Hiba a Notion lekérésnél: {e}")
    st.stop()

if not items:
    st.info("Nem találtam csoportokat a megadott PROPERTY_NAME alapján.")
    st.stop()

labels = [f"{name} ({cnt})" for name, cnt, _ in items]
name_by_label = {f"{name} ({cnt})": name for name, cnt, _ in items}
canon_by_name = {name: canon for name, _, canon in items}

with st.expander("Csoportok listája"):
    st.write(", ".join(labels))

sec_prop, ord_prop = resolve_section_and_order_props()
_, sorts_desc = resolve_sorts(ord_prop)
with st.expander("Részletek (felismert mezők és rendezés)"):
    st.write(f"**Szakasz mező**: `{sec_prop or '— (nem találtam; üres lesz a CSV-ben)'}`")
    st.write(f"**Sorszám mező**: `{ord_prop or '— (nincs; ABC cím szerint rendezünk)'}`")
    st.write(f"**Rendezés**: {sorts_desc}")

# Egyenkénti export (változatlan viselkedés, tisztítás + hosszú cella vágás hozzáadva)
pick = st.multiselect("Válaszd ki, mit exportáljunk (egyenkénti letöltés):", labels, max_selections=None)
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

st.markdown("---")
# Összes (ZIP) – megmarad
st.subheader("Összes letöltése (ZIP)")
start_run = st.button("Exportálás – Összes", type="primary", use_container_width=True)

def _resume_or_render(run_id: Optional[str]):
    if run_id and _load_checkpoint(run_id):
        export_engine(run_id, items)

# Auto-resume render (progress megőrzése újratöltéskor)
_resume_or_render(st.session_state.get("current_run_id"))

if start_run:
    run_id, cp = _resume_or_new_run(items)
    st.info(f"Futás azonosító: `{run_id}`")
    export_engine(run_id, items)

# ÚJ: Összes – egy munkalap (CSV)
st.markdown("---")
st.subheader("Összes – egy munkalap (CSV)")
if st.button("Exportálás – Egy munkalap (minden kurzus együtt)", use_container_width=True):
    unified = export_unified_sheet(items)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        "Letöltés: Content_egylap_%s.csv" % ts,
        data=unified,
        file_name=f"Content_egylap_{ts}.csv",
        mime="text/csv",
    )
