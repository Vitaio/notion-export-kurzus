import os
import io
import csv
import time
import re
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
    for k in ("NOTION_API_KEY", "NOTION_DATABASE_ID", "APP_PASSWORD", "NOTION_PROPERTY_NAME"):
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
        # jelenlegi sémanév vagy oldalakon látott egyik név (árva fallback)
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
    # fallback: ha a DB-ben konkrétan "Lecke címe" a title mező neve
    lekce = props.get("Lecke címe", {})
    if lekce.get("type") == "title" and lekce.get("title"):
        return " ".join((x.get("plain_text") or "") for x in lekce["title"]).strip() or "Névtelen oldal"
    return "Névtelen oldal"

def resolve_title_prop_name() -> str:
    """A DB-ben lévő cím (title) típusú property NEVE (az API a property-névvel várja a sortot)."""
    db = get_database_schema()
    for pname, meta in (db.get("properties", {}) or {}).items():
        if meta.get("type") == "title":
            return pname
    return ""  # extrém esetben üres (nem reális egy DB-nél)

def format_rich_text(rt_list: List[Dict]) -> str:
    out = ""
    for r in rt_list or []:
        t = r.get("plain_text", "") or ""
        href = r.get("href")
        out += f"[{t}]({href})" if href else t
    return out


def blocks_to_md(block_id: str, depth: int = 0) -> str:
    """Az oldal/blokk gyerekeit markdownná alakítja rekurzívan."""
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
                line = f"{indent}$$ {expr} $$"

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
# Property felderítés „Szakasz” / „Sorszám” részére
# ────────────────────────────────────────────────────────────────────────────────
def _norm_key(s: str) -> str:
    # ékezetek eltávolítása, lower, szóköz/alsóvonás/dísz jelek törlése
    if not isinstance(s, str):
        s = str(s or "")
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower()
    for ch in (" ", "_", "-", ".", ":"):
        s = s.replace(ch, "")
    return s

SECTION_TARGETS = [
    "szakasz", "szekcio", "section", "modul", "fejezet", "rész", "resz"
]
ORDER_TARGETS = [
    "sorszám", "sorszam", "sorrend", "order", "index", "pozicio", "pozíció", "rank"
]

@st.cache_data(ttl=300)
def resolve_section_and_order_props() -> Tuple[str, str]:
    """
    Visszaadja a Notion property kulcsnevét (pontosan), amit 'Szakasz' és 'Sorszám' alatt értsünk.
    - Név szerinti (ékezet/kis-nagybetű/stb.) keresés szinonimákkal.
    - Végül best-effort: 'select/multi_select/status' → szakasz; 'number' → sorszám.
    """
    db = get_database_schema()
    props: Dict[str, Dict] = db.get("properties", {}) or {}

    # Szakasz
    lookup = { _norm_key(k): k for k in props.keys() }
    sec_key = ""
    for cand in SECTION_TARGETS + ["szakasz"]:
        key = lookup.get(_norm_key(cand))
        if key:
            sec_key = key
            break
    if not sec_key:
        # típus szerinti tipp: kategorizáló property
        for k, v in props.items():
            if v.get("type") in ("select", "multi_select", "status"):
                sec_key = k
                break

    # Sorszám
    ord_key = ""
    for cand in ORDER_TARGETS + ["sorszám", "sorszam"]:
        key = lookup.get(_norm_key(cand))
        if key:
            ord_key = key
            break
    if not ord_key:
        # típus szerinti tipp: number property
        for k, v in props.items():
            if v.get("type") == "number":
                ord_key = k
                break

    return (sec_key or ""), (ord_key or "")


def format_property_for_csv(page: Dict, prop_name: str) -> str:
    """
    Általános property-kivonat CSV-hez.
    Lefedi: number, select, multi_select, status, rich_text, date, url, email, people, title.
    """
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
# Rendezés kiválasztása: 1) Sorszám property ↑  2) ABC cím szerint ↑
# ────────────────────────────────────────────────────────────────────────────────
def resolve_sorts(order_prop: Optional[str]) -> Tuple[List[Dict], str]:
    """
    Visszaadja a Notion API "sorts" listát és egy emberi leírást.
    Követelmény:
      1) Ha van 'Sorszám' property → aszerint növekvő
      2) Ha nincs → cím (title property) szerint ABC (növekvő)
    """
    if order_prop:
        return [{"property": order_prop, "direction": "ascending"}], f"property: {order_prop} ↑"

    title_prop = resolve_title_prop_name()
    if title_prop:
        return [{"property": title_prop, "direction": "ascending"}], f"title: {title_prop} ↑"

    # legvégső fallback – nem valószínű, hogy kell
    return [], "unspecified (API default)"


# ────────────────────────────────────────────────────────────────────────────────
# Markdown szűrés: csak „Videó szöveg” vagy – ha az üres – „Lecke szöveg”
# ────────────────────────────────────────────────────────────────────────────────
def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.strip().lower()

def _split_h2_sections(md: str) -> Dict[str, List[str]]:
    """
    H2 (##) címek mentén darabol, a kulcs a H2 címsor szövege (heading nélkül).
    A tartalom NEM tartalmazza a H2 sort, csak az utána jövő sorokat a következő H2-ig.
    """
    sections: Dict[str, List[str]] = {}
    current: Optional[str] = None
    for line in (md or "").splitlines():
        m = re.match(r"^\s*##\s+(.*)\s*$", line)
        if m:
            title = m.group(1).strip()
            current = title
            sections.setdefault(title, [])
            continue
        if current is not None:
            sections[current].append(line)
    return sections

def _join(lines: List[str]) -> str:
    return "\n".join(lines).strip()

def select_video_or_lesson(md: str) -> str:
    """
    Logika:
      - Ha a „Videó szöveg” rész tartalma NEM üres → csak azt adja vissza.
      - Egyébként, ha a „Lecke szöveg” NEM üres → csak azt adja vissza.
      - Különben üres string.
    A „Megjegyzés” és más H2 részek figyelmen kívül maradnak.
    """
    sections = _split_h2_sections(md)

    # kulcsok normalizálása a biztos egyezéshez
    norm_map = { _normalize(k): k for k in sections.keys() }

    video_key  = norm_map.get(_normalize("Videó szöveg"))
    lesson_key = norm_map.get(_normalize("Lecke szöveg"))

    video_txt  = _join(sections.get(video_key, [])) if video_key else ""
    lesson_txt = _join(sections.get(lesson_key, [])) if lesson_key else ""

    if re.search(r"\S", video_txt or ""):
        return video_txt
    if re.search(r"\S", lesson_txt or ""):
        return lesson_txt
    return ""


# ────────────────────────────────────────────────────────────────────────────────
# Export
# ────────────────────────────────────────────────────────────────────────────────
def export_one(display_name: str, canonical_names: Set[str]) -> bytes:
    """
    Egy megjelenítési csoport (display_name) exportja CSV-be.
    Rendezés:
      - ha van dedikált 'Sorszám' property → annak értéke szerint növekvő
      - különben: cím (title) szerint ABC
    A CSV 'tartalom' mező:
      - csak a „Videó szöveg” H2 alatti rész, HA az nem üres;
      - különben a „Lecke szöveg” H2 alatti rész (ha nem üres);
      - különben üres.
    A CSV 'sorszam' mező:
      - ha van 'Sorszám' property → annak értéke,
      - különben üres (nincs explicit sorszám a DB-ben).
    """
    ptype = get_property_type()
    section_prop, order_prop = resolve_section_and_order_props()
    sorts, _sort_desc = resolve_sorts(order_prop)

    # próbáljunk végig több néven, első találat nyer
    pages: List[Dict] = []
    for nm in sorted(canonical_names, key=lambda s: (0 if s == display_name else 1, s)):
        try:
            subset = query_filtered_pages(filter_=build_filter(ptype, nm), sorts=sorts)
        except APIResponseError:
            subset = []
        if subset:
            pages = subset
            break

    # CSV összeállítása memóriában
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDNAMES)
    writer.writeheader()

    for page in pages:
        pid   = page.get("id")
        title = extract_title(page)
        try:
            raw_md = blocks_to_md(pid).strip()
            content = select_video_or_lesson(raw_md)  # <-- itt történik a feltételes kivágás
        except Exception as e:
            content = f"[HIBA: {e}]"

        sorszam_value = format_property_for_csv(page, order_prop) if order_prop else ""

        row = {
            "oldal_cime": title,
            "szakasz": format_property_for_csv(page, section_prop),
            "sorszam": sorszam_value,
            "tartalom": content,
        }
        writer.writerow(row)
        time.sleep(0.01)  # udvarias tempó

    return buf.getvalue().encode("utf-8")


# ────────────────────────────────────────────────────────────────────────────────
# UI
# ────────────────────────────────────────────────────────────────────────────────
st.title("📦 Notion export – Kurzus")
st.caption("Rendezés: Sorszám ↑, különben ABC cím ↑. A „tartalom” csak a Videó szöveg vagy – ha az üres – a Lecke szöveg H2 alatti része.")

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
