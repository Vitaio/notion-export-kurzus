
import os, io, csv, time, re
from typing import Dict, List, Optional, Set, Tuple
from collections import Counter, defaultdict

import streamlit as st
from notion_client import Client
from notion_client.errors import APIResponseError

# --- Secrets -> env bridge (Streamlit Cloud-on fontos) ---
try:
    for k in ("NOTION_API_KEY", "NOTION_DATABASE_ID", "APP_PASSWORD"):
        if k in st.secrets and not os.getenv(k):
            os.environ[k] = str(st.secrets[k])
except Exception:
    pass
# ---------------------------------------------------------

# ===== CONFIG – ÁLLÍTSD BE ITT =====
NOTION_API_KEY = os.getenv("NOTION_API_KEY", "").strip()      # pl. secret_xxx (szerveren env var vagy Streamlit Secrets)
DATABASE_ID    = os.getenv("NOTION_DATABASE_ID", "623b8b80decd4f24a77a52c0d1dfc6ae").strip()
APP_PASSWORD   = os.getenv("APP_PASSWORD", "").strip()        # az app jelszava (env var / secrets)
PROPERTY_NAME  = "Kurzus"

# Megjelenítési átnevezések: {VALÓDI_NÉV -> MIT MUTASSON A LISTÁBAN}
DISPLAY_RENAMES = {
    "Üzleti Modellek": "Milyen vállalkozást indíts",
    "Marketing rendszerek": "Ügyfélszerző marketing rendszerek",
}
# ===================================

st.set_page_config(page_title="Notion export – Kurzus", page_icon="📦", layout="centered")

def need_auth() -> bool:
    if "authed" not in st.session_state:
        st.session_state.authed = False
    return not st.session_state.authed

def login_form():
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

@st.cache_data(ttl=60)  # 60 mp cache a Notion lekérésekre
def get_client() -> Client:
    if not NOTION_API_KEY:
        raise RuntimeError("A NOTION_API_KEY nincs beállítva (környezeti változó vagy Streamlit Secrets).")
    return Client(auth=NOTION_API_KEY)

@st.cache_data(ttl=60)
def get_property_type() -> Optional[str]:
    db = get_client().databases.retrieve(database_id=DATABASE_ID)
    p = (db.get("properties", {}) or {}).get(PROPERTY_NAME)
    return p.get("type") if p else None

@st.cache_data(ttl=60)
def schema_id_to_current_name() -> Dict[str, str]:
    db = get_client().databases.retrieve(database_id=DATABASE_ID)
    props = db.get("properties", {}) or {}
    p = props.get(PROPERTY_NAME)
    id2name = {}
    if p:
        ptype = p.get("type")
        if ptype in ("select", "multi_select", "status"):
            for opt in (p.get(ptype, {}) or {}).get("options", []) or []:
                if opt.get("id") and opt.get("name"):
                    id2name[opt["id"]] = opt["name"]
    return id2name

def query_all_pages() -> List[Dict]:
    client = get_client()
    results, cursor = [], None
    while True:
        resp = client.databases.query(database_id=DATABASE_ID, start_cursor=cursor)
        results.extend(resp.get("results", []))
        if not resp.get("has_more"): break
        cursor = resp.get("next_cursor")
    return results

@st.cache_data(ttl=60)
def collect_used_ids_and_names() -> Tuple[Counter, Dict[str, Set[str]]]:
    pages = query_all_pages()
    ptype = get_property_type()
    used_by_id = Counter()
    names_seen_by_id: Dict[str, Set[str]] = defaultdict(set)
    for page in pages:
        prop = page.get("properties", {}).get(PROPERTY_NAME)
        if not prop: continue
        if ptype == "select":
            node = prop.get("select") or {}
            oid, name = node.get("id"), (node.get("name") or "").strip()
            if oid:
                used_by_id[oid]+=1
                if name: names_seen_by_id[oid].add(name)
        elif ptype == "multi_select":
            for node in prop.get("multi_select") or []:
                oid, name = node.get("id"), (node.get("name") or "").strip()
                if oid:
                    used_by_id[oid]+=1
                    if name: names_seen_by_id[oid].add(name)
        elif ptype == "status":
            node = prop.get("status") or {}
            oid, name = node.get("id"), (node.get("name") or "").strip()
            if oid:
                used_by_id[oid]+=1
                if name: names_seen_by_id[oid].add(name)
    return used_by_id, names_seen_by_id

def build_display_list() -> List[Tuple[str, int, Set[str]]]:
    """
    Vissza: [(display_name, count, canonical_names), ...]
    - display_name: amit a listában mutatunk (DISPLAY_RENAMES alkalmazva)
    - canonical_names: ezzel próbálunk szűrni (aktuális név + esetleges régi variánsok + reverse alias)
    """
    used_by_id, names_seen = collect_used_ids_and_names()
    id2current = schema_id_to_current_name()
    reverse_alias = defaultdict(set)
    for old, new in DISPLAY_RENAMES.items():
        reverse_alias[new].add(old)

    display_items: Dict[str, Dict] = {}
    for oid, cnt in used_by_id.items():
        # jelenlegi sémanév vagy oldalakon látott egyik név (árva fallback)
        current_candidates = names_seen.get(oid, set())
        current_name = id2current.get(oid) or (sorted(current_candidates)[0] if current_candidates else f"(árva {oid[:6]}...)")
        display_name = DISPLAY_RENAMES.get(current_name, current_name)
        canon = set([current_name]) | current_candidates | reverse_alias.get(display_name, set())
        entry = display_items.setdefault(display_name, {"count": 0, "canon": set()})
        entry["count"] += cnt
        entry["canon"] |= canon

    items = [(disp, meta["count"], meta["canon"]) for disp, meta in display_items.items()]
    items.sort(key=lambda x: (-x[1], x[0].lower()))
    return items

def build_filter(ptype: Optional[str], name: str) -> Dict:
    if ptype == "select":       return {"property": PROPERTY_NAME, "select":      {"equals": name}}
    if ptype == "multi_select": return {"property": PROPERTY_NAME, "multi_select":{"contains": name}}
    if ptype == "status":       return {"property": PROPERTY_NAME, "status":      {"equals": name}}
    return {"property": PROPERTY_NAME, "select": {"equals": name}}

def extract_title(page: Dict) -> str:
    props = page.get("properties", {}) or {}
    for _, val in props.items():
        if val.get("type") == "title":
            arr = val.get("title", []) or []
            if arr: return " ".join(x.get("plain_text","") for x in arr).strip() or "Névtelen oldal"
    lekce = props.get("Lecke címe", {})
    if lekce.get("type") == "title" and lekce.get("title"):
        return " ".join(x.get("plain_text","") for x in lekce["title"]).strip() or "Névtelen oldal"
    return "Névtelen oldal"

def format_rich_text(rt_list: List[Dict]) -> str:
    out=""
    for r in rt_list or []:
        t = r.get("plain_text","") or ""
        href = r.get("href")
        out += f"[{t}]({href})" if href else t
    return out

def blocks_to_md(block_id: str, depth:int=0) -> str:
    client = get_client()
    lines, cursor = [], None
    indent = "  "*depth
    while True:
        resp = client.blocks.children.list(block_id=block_id, start_cursor=cursor)
        for block in resp.get("results", []):
            btype = block.get("type"); data = block.get(btype, {}) or {}
            line = ""
            if btype in ("paragraph","heading_1","heading_2","heading_3","bulleted_list_item","numbered_list_item","quote","to_do","callout","toggle"):
                txt = format_rich_text(data.get("rich_text", []))
                prefix = ""
                if   btype=="heading_1": prefix="# "
                elif btype=="heading_2": prefix="## "
                elif btype=="heading_3": prefix="### "
                elif btype=="bulleted_list_item": prefix="- "
                elif btype=="numbered_list_item": prefix="1. "
                elif btype=="quote": prefix="> "
                elif btype=="to_do": prefix="- [x] " if data.get("checked") else "- [ ] "
                elif btype=="callout": prefix="💡 "
                elif btype=="toggle": prefix="▶ "
                if txt or prefix: line=f"{indent}{prefix}{txt}"
            elif btype=="code":
                lang = data.get("language","") or ""
                inner= format_rich_text(data.get("rich_text", []))
                line = f"{indent}```{lang}\n{inner}\n```"
            elif btype=="equation":
                expr=data.get("expression","") or ""
                line=f"{indent}$$ {expr} $$"
            elif btype=="divider":
                line=f"{indent}---"
            elif btype in ("image","video","file","pdf"):
                cap = format_rich_text(data.get("caption", []))
                line=f"{indent}*[{btype.upper()}]* {cap}".rstrip()
            if line: lines.append(line)
            if block.get("has_children"):
                child = blocks_to_md(block["id"], depth+1)
                if child.strip(): lines.append(child)
        if not resp.get("has_more"): break
        cursor = resp.get("next_cursor")
    return "\n".join(lines)

def export_one(display_name: str, canonical_names: Set[str]) -> bytes:
    ptype = get_property_type()
    client = get_client()
    # próbáljunk végig több néven, első találat nyer
    pages: List[Dict] = []
    for nm in sorted(canonical_names, key=lambda s:(0 if s==display_name else 1, s)):
        try:
            subset = client.databases.query(database_id=DATABASE_ID, filter=build_filter(ptype, nm)).get("results",[])
        except APIResponseError:
            subset = []
        if subset:
            pages = subset; break

    # CSV összeállítása memóriában
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["oldal_cime", "tartalom"])
    writer.writeheader()
    for page in pages:
        pid = page.get("id")
        title = extract_title(page)
        try:
            content = blocks_to_md(pid).strip()
        except Exception as e:
            content = f"[HIBA: {e}]"
        writer.writerow({"oldal_cime": title, "tartalom": content})
        time.sleep(0.02)
    return buf.getvalue().encode("utf-8")

# ---------------- UI ----------------

st.title("📦 Notion export – Kurzus")
st.caption("Csak a jelenleg látható Kurzus-értékekből, jelszóval védve.")

# Jelszó ellenőrzés
if need_auth():
    if not APP_PASSWORD:
        st.warning("Admin: állítsd be az APP_PASSWORD környezeti változót vagy Streamlit Secrets-et a jelszóhoz.")
    login_form()
    st.stop()

# fő felület
try:
    items = build_display_list()  # [(display_name, count, canon_set)]
except Exception as e:
    st.error(f"Hiba a Notion lekérésnél: {e}")
    st.stop()

if not items:
    st.info("Nem találtam Kurzus értékeket.")
    st.stop()

# választó
labels = [f"{name} ({count})" for name, count, _ in items]
name_by_label = {labels[i]: items[i][0] for i in range(len(items))}
canon_by_name = {items[i][0]: items[i][2] for i in range(len(items))}

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
                file_name=f"export_Kurzus_{fname_safe}_.csv",
                mime="text/csv",
                key=f"dl-{fname_safe}",
            )
