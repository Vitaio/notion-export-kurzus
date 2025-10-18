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
# Cache-elt Notion kliens és séma
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
    """
    A csoportosító mező opcióinak id → aktuális név mapja.
    """
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
# Utilityk a név/normalizálás/RT formázás
# ────────────────────────────────────────────────────────────────────────────────
def _norm_key(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", "", s or "").strip().lower()

def format_rich_text(rt_array: List[Dict]) -> str:
    out = []
    for r in rt_array or []:
        t = r.get("plain_text", "")
        # egyszerű félkövér/italic/code detekt – (Notion -> markdown)
        ann = (r.get("annotations") or {})
        if ann.get("code"):      t = f"`{t}`"
        if ann.get("bold"):      t = f"**{t}**"
        if ann.get("italic"):    t = f"*{t}*"
        if ann.get("strikethrough"): t = f"~~{t}~~"
        out.append(t)
    return "".join(out)

# ────────────────────────────────────────────────────────────────────────────────
# Csoportok és megjelenítés
# ────────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def collect_used_ids_and_names() -> Tuple[Dict[str, int], Dict[str, Set[str]]]:
    """
    Visszaad:
      - used_by_id: {option_id → count}
      - names_seen: {option_id → {névváltozatok}}
    """
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
    """
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

def build_filter(ptype: str, name: str) -> Dict:
    if ptype == "select":
        return {"property": PROPERTY_NAME, "select": {"equals": name}}
    if ptype == "multi_select":
        return {"property": PROPERTY_NAME, "multi_select": {"contains": name}}
    if ptype == "status":
        return {"property": PROPERTY_NAME, "status": {"equals": name}}
    raise RuntimeError(f"Nem támogatott property típus: {ptype}")

# ────────────────────────────────────────────────────────────────────────────────
# Tartalom kinyerése Notionból → Markdown
# ────────────────────────────────────────────────────────────────────────────────
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

            else:
                # egyéb típusok ignorálása vagy egyszerűsített reprezentáció
                pass

            if line:
                lines.append(line)

            # children rekurzió pl. toggle, listák, stb.
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
        # best-effort: az első select/multi/status
        for k, v in props.items():
            if v.get("type") in ("select", "multi_select", "status"):
                sec_key = k
                break

    # Sorszám
    ord_key = ""
    for cand in ORDER_TARGETS + ["sorszám"]:
        key = lookup.get(_norm_key(cand))
        if key:
            ord_key = key
            break
    if not ord_key:
        # best-effort: az első number
        for k, v in props.items():
            if v.get("type") == "number":
                ord_key = k
                break

    return sec_key, ord_key

def extract_title(page: Dict) -> str:
    props = page.get("properties", {}) or {}
    # Title property azonosítása
    title_key = None
    for k, v in props.items():
        if v.get("type") == "title":
            title_key = k
            break
    if not title_key:
        # fallback
        for k in ("Lecke címe", "Lecke cime", "Cím", "Cim", "Name", "Név"):
            if k in props and props[k].get("type") in ("title", "rich_text"):
                title_key = k
                break
    if not title_key:
        return "(cím nélkül)"

    title_arr = props[title_key].get("title") or props[title_key].get("rich_text") or []
    title = "".join([t.get("plain_text", "") for t in title_arr]).strip()
    return title or "(cím nélkül)"

def format_property_for_csv(page: Dict, prop_name: str) -> str:
    """Általános property → CSV-barátságos szöveg."""
    props = page.get("properties", {}) or {}
    p = props.get(prop_name, {}) or {}
    ptype = p.get("type")
    try:
        if ptype == "number":
            v = p.get("number")
            return "" if v is None else str(v)

        if ptype == "select":
            s = p.get("select") or {}
            return (s.get("name") or "").strip()

        if ptype == "multi_select":
            arr = p.get("multi_select") or []
            return ", ".join([(x.get("name") or "").strip() for x in arr if x.get("name")])

        if ptype == "status":
            s = p.get("status") or {}
            return (s.get("name") or "").strip()

        if ptype == "rich_text":
            arr = p.get("rich_text") or []
            return "".join([t.get("plain_text", "") for t in arr]).strip()

        if ptype == "date":
            d = p.get("date") or {}
            return (d.get("start") or "") + ((" – " + d.get("end")) if d.get("end") else "")

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
        desc = f"Sorszám (`{order_prop}`) szerint növekvő"
        return [{"property": order_prop, "direction": "ascending"}], desc

    # Cím property kulcsa
    db = get_database_schema()
    title_key = None
    for k, v in (db.get("properties", {}) or {}).items():
        if v.get("type") == "title":
            title_key = k
            break
    if not title_key:
        title_key = "Name"

    desc = f"Cím (`{title_key}`) szerint ABC növekvő"
    return [{"property": title_key, "direction": "ascending"}], desc


# ────────────────────────────────────────────────────────────────────────────────
# „Videó szöveg” / „Lecke szöveg” kivágás + listák újraszámozása
# ────────────────────────────────────────────────────────────────────────────────
def select_video_or_lesson(md: str) -> str:
    sections = _split_h2_sections(md)

    def pick(key_variants: List[str]) -> str:
        for k in sections.keys():
            if _normalize(k) in (_normalize(v) for v in key_variants):
                body = _join(sections[k])
                if body.strip():
                    return body
        return ""

    video = pick(["Videó szöveg", "Video szoveg", "Video szöveg", "Videó szoveg", "Video", "Videó"])
    if video.strip():
        return video

    lesson = pick(["Lecke szöveg", "Lecke szoveg", "Lecke", "Lecke anyag"])
    if lesson.strip():
        return lesson

    return ""

def fix_numbered_lists(md: str) -> str:
    """
    A számozott listák újraszámozása a teljes kivágott részben.
    - Kódblokkokat (``` … ```) nem érinti.
    - Beágyazott pontokat az indent (szóközök) alapján kezeli.
    """
    lines = md.splitlines()
    out: List[str] = []
    in_code = False
    fence_re = re.compile(r"^\s*```")
    num_re = re.compile(r"^(\s*)(\d+)\.\s+(.*)$")

    # számlálók indent szintenként
    counter_for_indent: Dict[int, int] = {}
    active_list_indent: Optional[int] = None

    for line in lines:
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
            # nem számozott sor → lezárjuk az aktív listát
            active_list_indent = None
            out.append(line)

    return "\n".join(out).strip()

# ────────────────────────────────────────────────────────────────────────────────
# Export – egy csoport CSV-je
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

    # ha nem találtunk semmit
    if not pages:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        return output.getvalue().encode("utf-8")

    rows: List[Dict[str, str]] = []
    for pg in pages:
        page_id = pg["id"]
        title = extract_title(pg)

        # propertyk CSV-re
        section_val = format_property_for_csv(pg, section_prop) if section_prop else ""
        order_val   = format_property_for_csv(pg, order_prop) if order_prop else ""

        # tartalom kivágás
        md = blocks_to_md(page_id)
        chosen = select_video_or_lesson(md)
        if chosen:
            chosen = fix_numbered_lists(chosen)

        rows.append({
            "oldal_cime": title,
            "szakasz": section_val,
            "sorszam": order_val,
            "tartalom": chosen or "",
        })

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDNAMES)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return output.getvalue().encode("utf-8")


# ────────────────────────────────────────────────────────────────────────────────
# UI – lista, részletek, egyesével exportálás
# ────────────────────────────────────────────────────────────────────────────────
st.title("📦 Notion export – Kurzus")
st.caption("Rendezés: Sorszám ↑, különben ABC cím ↑. A „tartalom” a „Videó szöveg”/„Lecke szöveg” H2 alatti rész; a számozott listák automatikusan 1., 2., 3.… formára újraszámozva.")

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
    st.info("Nem találtam csoportokat a megadott PROPERTY_NAME alapján.")
    st.stop()

# Kijelzés: Név (db)
labels = [f"{name} ({cnt})" for name, cnt, _ in items]
name_by_label = {f"{name} ({cnt})": name for name, cnt, _ in items}
canon_by_name = {name: canon for name, _, canon in items}

st.write("Választható csoportok (megjelenített név → darabszám):")
st.write(", ".join(labels))

# Részletek: felismert mezők és rendezés
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
st.subheader("Összes letöltése (ZIP) – legkisebbtől kezdve")


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
