import os
import io
import csv
import time
import re
import unicodedata
from typing import Dict, List, Optional, Set, Tuple
from collections import Counter, defaultdict

import json
import zipfile
from datetime import datetime


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
    """
    Az oldal/blokk gyerekeit markdownná alakítja rekurzívan.

    FIGYELEM: a Notion API "numbered_list_item" blokkokat ad vissza, és a hagyományos markdownban
    gyakori, hogy minden elem "1."-ként kerül kiírva. Mi itt szándékosan MINDIG "1."-et írunk ki,
    majd a teljes szöveg összeállítása UTÁN, egy külön lépésben újraszámozzuk a listákat
    (fix_numbered_lists), így a beágyazott tartalom és a lapozás sem zavarja össze a számlálót.
    """
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
                elif btype == "numbered_list_item": prefix = "1. "  # ← mindig 1., később renumber
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
# Markdown szűrés + számozott listák ÚJRASZÁMOZÁSA
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

def fix_numbered_lists(md: str) -> str:
    """
    ÚJRASZÁMOZÁS:
      - csak azokat a sorokat módosítja, amelyek *szóközök után* közvetlenül „szám + . + szóköz” mintával kezdődnek.
      - figyeli a kódblokkokat (```), azokat érintetlenül hagyja.
      - kezeli a beágyazott tartalmat: a listához tartozó, de jobban behúzott sorok (pl. a listapont alatti bekezdés)
        nem szakítják meg a számozást.
    """
    lines = (md or "").splitlines()
    out: List[str] = []
    in_code = False
    fence_re = re.compile(r'^\s*```')
    num_re = re.compile(r'^(\s*)(\d+)\.\s(.*)$')

    active_list_indent: Optional[int] = None  # hány space a numerikus pontoknál
    counter_for_indent: Dict[int, int] = {}

    for line in lines:
        # kódblokk nyit/zár
        if fence_re.match(line):
            in_code = not in_code
            out.append(line)
            # kódblokk sorai ne befolyásolják a listaszámlálót
            continue

        if in_code:
            out.append(line)
            continue

        m = num_re.match(line)
        if m:
            indent_str = m.group(1)
            indent_len = len(indent_str)
            content = m.group(3)

            # új lista vagy új szint?
            if active_list_indent is None or indent_len != active_list_indent:
                # új lista ezen az indenten
                active_list_indent = indent_len
                # töröljük a mélyebb számlálókat
                for k in list(counter_for_indent.keys()):
                    if k >= indent_len:
                        del counter_for_indent[k]
                counter_for_indent[indent_len] = 1
            else:
                # folytatólagos elem ugyanazon az indenten
                counter_for_indent[indent_len] = counter_for_indent.get(indent_len, 0) + 1

            n = counter_for_indent[indent_len]
            out.append(f"{indent_str}{n}. {content}")
        else:
            # nem számozott sor: eldöntjük, hogy a listán belüli tartalom-e
            if active_list_indent is not None:
                leading_spaces = len(line) - len(line.lstrip(" "))
                if line.strip() == "":
                    # üres sor: listát nem szakítjuk meg
                    out.append(line)
                    continue
                if leading_spaces > active_list_indent:
                    # a jelenlegi listapont alatti „tartalom” → marad a lista aktív
                    out.append(line)
                    continue
                # ide érve vagy kisebb/egyenlő indent, vagy nincs indent → vége a listának
                active_list_indent = None
                counter_for_indent.clear()

            out.append(line)

    return "\n".join(out)

def select_video_or_lesson(md: str) -> str:
    """
    Logika:
      - Ha a „Videó szöveg” rész tartalma NEM üres → csak azt adja vissza (újraszámozva).
      - Egyébként, ha a „Lecke szöveg” NEM üres → csak azt adja vissza (újraszámozva).
      - Különben üres string.
    """
    sections = _split_h2_sections(md)
    norm_map = { _normalize(k): k for k in sections.keys() }

    video_key  = norm_map.get(_normalize("Videó szöveg"))
    lesson_key = norm_map.get(_normalize("Lecke szöveg"))

    video_txt  = _join(sections.get(video_key, [])) if video_key else ""
    lesson_txt = _join(sections.get(lesson_key, [])) if lesson_key else ""

    if re.search(r"\S", video_txt or ""):
        return fix_numbered_lists(video_txt)
    if re.search(r"\S", lesson_txt or ""):
        return fix_numbered_lists(lesson_txt)
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
      - a számozott listákat mindig 1., 2., 3. … formára újraszámozzuk (fix_numbered_lists).
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
            content = select_video_or_lesson(raw_md)  # feltételes kivágás + újraszámozás
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
st.caption("Rendezés: Sorszám ↑, különben ABC cím ↑. A „tartalom” a Videó szöveg (ha üres: Lecke szöveg) – a számozott listák automatikusan 1., 2., 3.… formára újraszámozva.")

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


# ────────────────────────────────────────────────────────────────────────────────
# Tömeges export segédfüggvények: checkpoint + ZIP + progress
# ────────────────────────────────────────────────────────────────────────────────
CHECKPOINT_FILE = ".export_checkpoint.json"
EXPORTS_ROOT = "exports"

def _slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s-]+", "_", s)
    return s[:80] if len(s) > 80 else s

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def _save_checkpoint(state: dict):
    try:
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _load_checkpoint() -> Optional[dict]:
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _clear_checkpoint():
    try:
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
    except Exception:
        pass

def _write_csv_file(folder: str, display_name: str, data: bytes) -> str:
    fn = f"export_{_slug(display_name)}.csv"
    fp = os.path.join(folder, fn)
    with open(fp, "wb") as f:
        f.write(data)
    return fp

def _zip_folder(folder: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(folder):
            for name in sorted(files):
                fp = os.path.join(root, name)
                arcname = os.path.relpath(fp, folder)
                zf.write(fp, arcname)
    buf.seek(0)
    return buf.read()

def export_all_groups_sorted_smallest_first(
    groups_display,   # [(display_name, count, canon_set)]
    export_one_fn,    # export_one(display_name, canon_set) -> bytes
):
    """
    - count szerint növekvőre rendezve kezdi az exportot
    - Minden csoport külön CSV-be kerül (export_{slug}.csv)
    - Folytonosan checkpointol; megszakadás után onnan folytatható
    - A végén egy ZIP-et készít a mappában összegyűlt CSV-kből
    Visszatér: (zip_bytes_or_None, run_dir)
    """
    ordered = sorted(groups_display, key=lambda t: (t[1], t[0]))
    total = len(ordered)

    cp = _load_checkpoint()
    now_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not cp:
        run_id = now_id
        run_dir = os.path.join(EXPORTS_ROOT, f"run_{run_id}")
        _ensure_dir(run_dir)
        cp = {
            "run_id": run_id,
            "run_dir": run_dir,
            "completed": [],
            "pending": [d for d, _, _ in ordered],
        }
        _save_checkpoint(cp)
    else:
        run_dir = cp.get("run_dir") or os.path.join(EXPORTS_ROOT, f"run_{now_id}")
        _ensure_dir(run_dir)
        cp["run_dir"] = run_dir
        _save_checkpoint(cp)

    success, failed = 0, 0
    completed_set = set(cp.get("completed", []))
    pending_names = [n for n in cp.get("pending", []) if n not in completed_set]
    num_done_initial = len(completed_set)

    with st.status("Exportálás folyamatban…", expanded=True) as status:
        prog = st.progress(0.0)
        for idx, (display_name, count, canon) in enumerate(ordered, start=1):
            if display_name in completed_set:
                prog.progress(min(1.0, (num_done_initial + success) / max(1,total)))
                st.write(f"✅ Már kész: **{display_name}** ({count} db)")
                continue
            if display_name not in pending_names:
                st.write(f"⏭ Kihagyva: **{display_name}** ({count} db)")
                continue

            st.write(f"→ Exportálás: **{display_name}** ({count} db)")
            try:
                data = export_one_fn(display_name, canon)
                _write_csv_file(run_dir, display_name, data)

                completed_set.add(display_name)
                success += 1
                cp["completed"] = sorted(list(completed_set))
                cp["pending"] = [n for n in cp["pending"] if n != display_name]
                _save_checkpoint(cp)

                prog.progress(min(1.0, (num_done_initial + success) / max(1,total)))
                st.write(f"✅ Kész: **{display_name}**")
            except Exception as e:
                failed += 1
                st.write(f"❌ Hiba: **{display_name}** — {e!r}")
                st.info("A folyamat folytatható a „Folytatás megszakadt exportból” gombbal.")
                # hagyjuk a pendingben

        if failed == 0 and len(completed_set) == total:
            zip_bytes = _zip_folder(run_dir)
            _clear_checkpoint()
            status.update(label="Export kész", state="complete")
            return zip_bytes, run_dir
        else:
            status.update(label="Export részben kész – folytatható", state="running")
            return None, run_dir



st.markdown("""---""")
st.subheader("Összes letöltése (ZIP) – legkisebb csoporttól kezdve")


# A display_list a build_display_list() eredménye fent
try_resume = _load_checkpoint() is not None
col1, col2 = st.columns([1,1])
with col1:
    run_all = st.button("Exportálás – Összes (kicsiktől nagyig)", type="primary", use_container_width=True)
with col2:
    resume = st.button("Folytatás megszakadt exportból", disabled=not try_resume, use_container_width=True)

if run_all:
    zip_bytes, run_dir = export_all_groups_sorted_smallest_first(
        groups_display=items,
        export_one_fn=export_one,
    )
    if zip_bytes:
        st.success("Sikeres export! Letölthető a ZIP:")
        st.download_button(
            label="ZIP letöltése",
            data=zip_bytes,
            file_name="notion_kurzus_export_all.zip",
            mime="application/zip",
            use_container_width=True
        )
        st.caption(f"Futás mappa: `{run_dir}` – A CSV-k külön fájlokban vannak, a ZIP ezeket tartalmazza.")
    else:
        st.warning("Az export nem készült el teljes egészében. A „Folytatás megszakadt exportból” gombbal onnan folytathatod, ahol abbahagyta.")

if resume:
    zip_bytes, run_dir = export_all_groups_sorted_smallest_first(
        groups_display=items,
        export_one_fn=export_one,
    )
    if zip_bytes:
        st.success("Sikeres export! Letölthető a ZIP:")
        st.download_button(
            label="ZIP letöltése",
            data=zip_bytes,
            file_name="notion_kurzus_export_all.zip",
            mime="application/zip",
            use_container_width=True
        )
        st.caption(f"Futás mappa: `{run_dir}`")
    else:
        st.info("Még vannak hátralévő csoportok vagy hiba történt közben. Újra megpróbálhatod a folytatást.")

