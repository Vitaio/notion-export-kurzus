
# app.py
# Streamlit + Notion export (resumable, progress, counts, disappearing login)
# Env/Secrets: NOTION_API_KEY, NOTION_DATABASE_ID, APP_PASSWORD, NOTION_PROPERTY_NAME (default: "Kurzus")

import os, io, re, time, math, unicodedata, json
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional

import streamlit as st
import pandas as pd

try:
    from slugify import slugify
except Exception:
    def slugify(s: str) -> str:
        s = unicodedata.normalize("NFKD", s)
        s = "".join(c for c in s if not unicodedata.combining(c))
        s = re.sub(r"[^a-zA-Z0-9_-]+", "-", s).strip("-").lower()
        return s

from notion_client import Client
from notion_client.errors import APIResponseError

# ----------------------------
# UI helpers: progress + resumable
# ----------------------------
class ProgressReporter:
    def __init__(self, total_steps: int, title: str):
        self.total = max(1, int(total_steps))
        self.step = 0
        self.progress = st.progress(0)
        try:
            self.status = st.status(title, state="running", expanded=False)
            self._ctx = self.status.__enter__()
            self._uses_status = True
        except Exception:
            self._uses_status = False
            st.write(f"**{title}**")
        self._log = st.empty()

    def log(self, msg: str):
        if msg:
            self._log.write(msg)

    def tick(self, msg: str = ""):
        self.step = min(self.total, self.step + 1)
        pct = int(self.step * 100 / self.total)
        self.progress.progress(pct)
        if msg:
            self.log(f"{pct}% – {msg}")

    def finish(self, ok: bool = True, msg: str = ""):
        if msg:
            self.log(msg)
        if self._uses_status:
            try:
                if ok:
                    self.status.update(label="Kész", state="complete")
                else:
                    self.status.update(label="Hiba", state="error")
            finally:
                try:
                    self.status.__exit__(None, None, None)
                except Exception:
                    pass

# ----------------------------
# Config / constants
# ----------------------------
DEFAULT_PROPERTY_NAME = "Kurzus"
CSV_FIELDNAMES = ["oldal_cime", "szakasz", "sorszam", "tartalom"]

DISPLAY_RENAMES: Dict[str, str] = {
    "Üzleti Modellek": "Milyen vállalkozást indíts",
    "Marketing rendszerek": "Ügyfélszerző marketing rendszerek",
}

VIDEO_SECTION_KEYS = [
    "videó szöveg", "video szoveg", "video szöveg", "videó: szöveg", "videó - szöveg",
    "videó tartalom", "video tartalom"
]
LESSON_SECTION_KEYS = [
    "lecke szöveg", "lecke szoveg", "lecke: szöveg", "lecke - szöveg",
    "lecke tartalom", "lesson text"
]

# ----------------------------
# Secrets/env
# ----------------------------
def get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    if name in os.environ and os.environ.get(name):
        return os.environ.get(name)
    try:
        return st.secrets.get(name, default)
    except Exception:
        return default

# ----------------------------
# Caching
# ----------------------------
@st.cache_resource
def get_notion_client() -> Client:
    token = get_secret("NOTION_API_KEY")
    if not token:
        st.error("Hiányzik a NOTION_API_KEY.")
        st.stop()
    return Client(auth=token)

@st.cache_data(ttl=120)
def get_property_name() -> str:
    return get_secret("NOTION_PROPERTY_NAME", DEFAULT_PROPERTY_NAME) or DEFAULT_PROPERTY_NAME

@st.cache_data(ttl=120)
def get_database_id() -> str:
    dbid = get_secret("NOTION_DATABASE_ID")
    if not dbid:
        st.error("Hiányzik a NOTION_DATABASE_ID konfiguráció.")
        st.stop()
    return dbid

@st.cache_data(ttl=120)
def get_db_schema(dbid: str) -> Dict[str, Any]:
    client = get_notion_client()
    return client.databases.retrieve(database_id=dbid)

# ----------------------------
# Notion helpers
# ----------------------------
def backoff_retry(fn, max_tries=5, base=0.5, factor=2.0, **kwargs):
    attempt = 0
    while True:
        try:
            return fn(**kwargs)
        except APIResponseError:
            attempt += 1
            if attempt >= max_tries:
                raise
            sleep_s = base * (factor ** (attempt - 1)) + (0.01 * (attempt % 7))
            time.sleep(sleep_s)

def normalize(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def detect_title_prop(schema: Dict[str, Any]) -> Optional[str]:
    props = schema.get("properties", {})
    for name, meta in props.items():
        if meta.get("type") == "title":
            return name
    return None

def all_properties(schema: Dict[str, Any]) -> Dict[str, Any]:
    return schema.get("properties", {})

def best_effort_section_prop(schema: Dict[str, Any]) -> Optional[str]:
    candidates = {"szakasz","szekcio","section","modul","fejezet","resz","rész","chapter"}
    props = all_properties(schema)
    for name in props:
        if normalize(name) in candidates:
            return name
    for name, meta in props.items():
        if meta.get("type") in ("select","multi_select","status"):
            return name
    return None

def best_effort_order_prop(schema: Dict[str, Any]) -> Optional[str]:
    candidates = {"sorszám","sorszam","sorrend","order","index","pozicio","pozíció","rank"}
    props = all_properties(schema)
    for name in props:
        if normalize(name) in candidates:
            return name
    for name, meta in props.items():
        if meta.get("type") == "number":
            return name
    return None

def find_group_property(schema: Dict[str, Any], wanted: str) -> Tuple[str, str, Dict[str, Any]]:
    props = all_properties(schema)
    if wanted in props:
        p = props[wanted]
        return wanted, p.get("type"), p
    norm_wanted = normalize(wanted)
    for name, meta in props.items():
        if normalize(name) == norm_wanted:
            return name, meta.get("type"), meta
    st.error(f"Nem találom a csoportosító property-t: {wanted}")
    st.stop()

def property_options_map(prop_meta: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    ptype = prop_meta.get("type")
    m: Dict[str, Dict[str, Any]] = {}
    if ptype in ("select","multi_select"):
        for opt in prop_meta.get(ptype, {}).get("options", []):
            m[opt["id"]] = {"name": opt.get("name","")}
    elif ptype == "status":
        for opt in prop_meta.get("status", {}).get("options", []):
            m[opt["id"]] = {"name": opt.get("name","")}
    return m

def list_all_pages(client: Client, dbid: str, filter_obj=None, sorts=None, on_batch=None) -> List[Dict[str, Any]]:
    pages = []
    cursor = None
    batch = 0
    while True:
        payload = {}
        if filter_obj:
            payload["filter"] = filter_obj
        if sorts:
            payload["sorts"] = sorts
        if cursor:
            payload["start_cursor"] = cursor
        resp = client.databases.query(database_id=dbid, **payload)
        chunk = resp.get("results", [])
        pages.extend(chunk)
        batch += 1
        if callable(on_batch):
            try:
                on_batch(batch, len(pages))
            except Exception:
                pass
        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break
    return pages

def extract_title(page: Dict[str, Any], title_prop: str) -> str:
    props = page.get("properties", {})
    p = props.get(title_prop, {})
    if p.get("type") == "title":
        parts = p.get("title", [])
        return "".join([t.get("plain_text","") for t in parts]).strip()
    return ""

def extract_property_as_string(page: Dict[str, Any], prop_name: Optional[str]) -> str:
    if not prop_name:
        return ""
    props = page.get("properties", {})
    p = props.get(prop_name)
    if not p:
        return ""
    t = p.get("type")
    if t == "number":
        v = p.get("number")
        return "" if v is None else str(v)
    if t == "select":
        sel = p.get("select")
        return "" if not sel else sel.get("name","")
    if t == "multi_select":
        arr = p.get("multi_select", [])
        return ", ".join(x.get("name","") for x in arr if x)
    if t == "status":
        stt = p.get("status")
        return "" if not stt else stt.get("name","")
    if t == "rich_text":
        arr = p.get("rich_text", [])
        return "".join(rt.get("plain_text","") for rt in arr)
    if t == "date":
        d = p.get("date")
        if not d:
            return ""
        if d.get("end"):
            return f"{d.get('start','')}..{d.get('end','')}"
        return d.get("start","")
    if t == "url":
        return p.get("url","") or ""
    if t == "email":
        return p.get("email","") or ""
    if t == "people":
        arr = p.get("people", [])
        names = []
        for person in arr:
            nm = person.get("name")
            if nm:
                names.append(nm)
            else:
                em = person.get("person", {}).get("email")
                if em:
                    names.append(em)
        return ", ".join(names)
    if t == "title":
        arr = p.get("title", [])
        return "".join(rt.get("plain_text","") for rt in arr)
    return ""

# Blocks -> Markdown
def fetch_blocks(client: Client, block_id: str) -> List[Dict[str, Any]]:
    results = []
    cursor = None
    while True:
        resp = client.blocks.children.list(block_id=block_id, start_cursor=cursor) if cursor \
            else client.blocks.children.list(block_id=block_id)
        results.extend(resp.get("results", []))
        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break
    return results

def fetch_blocks_recursive(client: Client, page_id: str) -> List[Dict[str, Any]]:
    root = fetch_blocks(client, page_id)
    def walk(block):
        if block.get("has_children"):
            child = fetch_blocks(client, block["id"])
            block["_children"] = child
            for c in child:
                walk(c)
    for b in root:
        walk(b)
    return root

def rich_text_to_md(rt: List[Dict[str, Any]]) -> str:
    s = ""
    for t in rt or []:
        text = t.get("plain_text","")
        ann = t.get("annotations", {})
        if ann.get("code"): text = f"`{text}`"
        if ann.get("bold"): text = f"**{text}**"
        if ann.get("italic"): text = f"*{text}*"
        if ann.get("strikethrough"): text = f"~~{text}~~"
        if ann.get("underline"): text = f"<u>{text}</u>"
        s += text
    return s

def fix_numbered_lists(md: str) -> str:
    lines = md.splitlines()
    out = []
    in_code = False
    counters = {}

    def is_num_item(s: str) -> Optional[int]:
        m = re.match(r"^(\s*)(\d+)\.\s", s)
        if m:
            return len(m.group(1))
        return None

    for line in lines:
        if line.strip().startswith("```"):
            in_code = not in_code
            out.append(line)
            continue
        if in_code:
            out.append(line)
            continue

        indent = is_num_item(line)
        if indent is None:
            out.append(line)
            continue

        to_del = [k for k in list(counters.keys()) if k > indent]
        for k in to_del:
            counters.pop(k, None)
        if indent not in counters:
            counters[indent] = 1
        else:
            counters[indent] += 1

        new_idx = counters[indent]
        line = re.sub(r"^(\s*)\d+\.\s", r"\g<1>" + f"{new_idx}. ", line)
        out.append(line)
    return "\n".join(out)

def blocks_to_md(blocks: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    def emit(s: str=""):
        lines.append(s)

    def walk(block, indent=0):
        t = block.get("type")
        b = block.get(t, {})
        prefix = " " * indent
        if t in ("paragraph","quote","callout","to_do","toggle","bulleted_list_item","numbered_list_item"):
            content = rich_text_to_md(b.get("rich_text", []))
            if t == "paragraph":
                if content.strip():
                    emit(prefix + content)
                else:
                    emit("")
            elif t == "quote":
                emit(prefix + "> " + content)
            elif t == "callout":
                emoji = b.get("icon", {}).get("emoji") if isinstance(b.get("icon"), dict) else ""
                emit(prefix + f"> {emoji or '💡'} {content}")
            elif t == "to_do":
                ck = "[x]" if b.get("checked") else "[ ]"
                emit(prefix + f"- {ck} {content}")
            elif t == "bulleted_list_item":
                emit(prefix + f"- {content}")
            elif t == "numbered_list_item":
                emit(prefix + f"1. {content}")
            for c in block.get("_children", []):
                walk(c, indent=indent+2)
        elif t in ("heading_1","heading_2","heading_3"):
            level = {"heading_1": "#", "heading_2": "##", "heading_3": "###"}[t]
            content = rich_text_to_md(b.get("rich_text", []))
            emit(f"{level} {content}")
        elif t in ("divider",):
            emit("\n---\n")
        elif t in ("equation",):
            ex = b.get("expression","")
            if ex:
                emit(f"$$ {ex} $$")
        elif t in ("image","video","file","pdf"):
            cap = rich_text_to_md(b.get("caption", [])) if b.get("caption") else ""
            emit(f"*[{t.upper()}]* {cap}".rstrip())
        elif t in ("table","table_row"):
            emit(f"*[{t.upper()}]*")
        else:
            emit(f"*[{t.upper()}]*")

    for bl in blocks:
        walk(bl, indent=0)

    md = "\n".join(lines)
    md = re.sub(r"\n{3,}", "\n\n", md)
    md = fix_numbered_lists(md)
    return md

def _find_h2_positions(md: str) -> List[Tuple[int, str]]:
    pos = []
    for m in re.finditer(r"^##\s+(.+)$", md, flags=re.MULTILINE):
        pos.append((m.start(), m.group(1).strip()))
    return pos

def _extract_section_by_h2(md: str, target_keys: List[str], stop_keys: Optional[List[str]]=None) -> str:
    stop_keys = stop_keys or []
    h2s = _find_h2_positions(md)
    if not h2s:
        return ""
    norm_t = [normalize(x) for x in target_keys]
    norm_stop = [normalize(x) for x in stop_keys]
    target_idx = None
    for i, (pos, title) in enumerate(h2s):
        if normalize(title) in norm_t:
            target_idx = i
            break
    if target_idx is None:
        return ""
    start_pos = h2s[target_idx][0]
    end_pos = len(md)
    for j in range(target_idx+1, len(h2s)):
        if normalize(h2s[j][1]) in norm_stop:
            end_pos = h2s[j][0]
            break
    chunk = md[start_pos:end_pos]
    return chunk.strip()

def select_video_or_lesson(md: str) -> str:
    s = _extract_section_by_h2(md, VIDEO_SECTION_KEYS, stop_keys=[])
    if s.strip():
        return fix_numbered_lists(s)
    s = _extract_section_by_h2(md, LESSON_SECTION_KEYS, stop_keys=[])
    if s.strip():
        return fix_numbered_lists(s)
    return ""

def resolve_sorts(order_prop: Optional[str], title_prop: Optional[str]) -> List[Dict[str, Any]]:
    sorts = []
    if order_prop:
        sorts.append({"property": order_prop, "direction": "ascending"})
    elif title_prop:
        sorts.append({"property": title_prop, "direction": "ascending"})
    return sorts

def collect_group_index(client: Client, dbid: str, prop_name: str, prop_type: str, prop_meta: Dict[str, Any]) -> Tuple[List[Tuple[str,int,set]], Dict[str, Dict[str, Any]]]:
    id_to_opt = property_options_map(prop_meta)
    counts_by_id = {oid: 0 for oid in id_to_opt.keys()}
    seen_names_by_id = {oid: set() for oid in id_to_opt.keys()}
    pages = list_all_pages(client, dbid)
    for pg in pages:
        p = pg.get("properties", {}).get(prop_name)
        if not p:
            continue
        if prop_type == "select":
            sel = p.get("select")
            if sel:
                oid = sel.get("id")
                nm = sel.get("name","")
                if oid in counts_by_id:
                    counts_by_id[oid] += 1
                    if nm:
                        seen_names_by_id[oid].add(nm)
        elif prop_type == "multi_select":
            arr = p.get("multi_select", [])
            for sel in arr:
                oid = sel.get("id")
                nm = sel.get("name","")
                if oid in counts_by_id:
                    counts_by_id[oid] += 1
                    if nm:
                        seen_names_by_id[oid].add(nm)
        elif prop_type == "status":
            stt = p.get("status")
            if stt:
                oid = stt.get("id")
                nm = stt.get("name","")
                if oid in counts_by_id:
                    counts_by_id[oid] += 1
                    if nm:
                        seen_names_by_id[oid].add(nm)
    display_to_canon: Dict[str, Dict[str, Any]] = {}
    items = []
    reverse_alias = {}
    for src, dst in DISPLAY_RENAMES.items():
        reverse_alias.setdefault(dst, set()).add(src)
    for oid, meta in id_to_opt.items():
        current_name = meta.get("name","")
        display_name = DISPLAY_RENAMES.get(current_name, current_name)
        canon = set()
        canon.add(current_name)
        canon.update(seen_names_by_id.get(oid, set()))
        if display_name in reverse_alias:
            canon.update(reverse_alias[display_name])
        canon.add(display_name)
        count = counts_by_id.get(oid, 0)
        items.append((display_name, count, canon))
        display_to_canon[display_name] = {"canonical": canon}
    items.sort(key=lambda x: x[1], reverse=True)
    return items, display_to_canon

def build_filter(prop_name: str, prop_type: str, name: str) -> Dict[str, Any]:
    if prop_type == "select":
        return {"property": prop_name, "select": {"equals": name}}
    if prop_type == "multi_select":
        return {"property": prop_name, "multi_select": {"contains": name}}
    if prop_type == "status":
        return {"property": prop_name, "status": {"equals": name}}
    return {}

def collect_rows_for_group(client: Client, dbid: str, prop_name: str, prop_type: str,
                           canonical_name: str, title_prop: str, section_prop: Optional[str],
                           order_prop: Optional[str], sorts: List[Dict[str, Any]],
                           on_progress=None) -> List[Dict[str,str]]:
    f = build_filter(prop_name, prop_type, canonical_name)
    def _on_batch(batch_no, total_so_far):
        if callable(on_progress):
            try:
                on_progress(f"„{canonical_name}” – {total_so_far} oldal beolvasva (batch {batch_no}).")
            except Exception:
                pass
    pages = list_all_pages(client, dbid, filter_obj={"and": [f]}, sorts=sorts, on_batch=_on_batch)
    rows = []
    for pg in pages:
        oldal_cime = extract_title(pg, title_prop) if title_prop else ""
        szakasz = extract_property_as_string(pg, section_prop) if section_prop else ""
        sorszam = extract_property_as_string(pg, order_prop) if order_prop else ""
        blocks = fetch_blocks_recursive(client, pg["id"])
        md = blocks_to_md(blocks)
        tartalom = select_video_or_lesson(md)
        rows.append({
            "oldal_cime": oldal_cime or "",
            "szakasz": szakasz or "",
            "sorszam": sorszam or "",
            "tartalom": tartalom or ""
        })
    def _num(x):
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return math.inf
    if order_prop:
        rows.sort(key=lambda r: (_num(r["sorszam"]), r["oldal_cime"].lower()))
    else:
        rows.sort(key=lambda r: r["oldal_cime"].lower())
    return rows

# Excel writer fallback
def _excel_writer(output_io):
    try:
        import xlsxwriter  # noqa: F401
        return pd.ExcelWriter(output_io, engine="xlsxwriter")
    except Exception:
        try:
            import openpyxl  # noqa: F401
            return pd.ExcelWriter(output_io, engine="openpyxl")
        except Exception:
            st.error("Hiányzik XLSX író motor: telepítsd az 'XlsxWriter' vagy 'openpyxl' csomagot.")
            st.stop()

def sanitize_sheet_name(name: str) -> str:
    name = re.sub(r'[:\\/?*\\[\\]]', "_", name)
    return name[:31] if len(name) > 31 else name

# ----------------------------
# Resumable export engine
# ----------------------------
def init_resume(mode: str, groups_display: List[str], property_name: str, dbid: str):
    st.session_state["resume"] = {
        "version": 1,
        "mode": mode,
        "property_name": property_name,
        "dbid": dbid,
        "groups": groups_display,
        "done": [],
        "created_at": datetime.utcnow().isoformat()+"Z",
    }

def get_resume():
    return st.session_state.get("resume")

def checkpoint_download_button(label="💾 Checkpoint letöltése"):
    meta = get_resume()
    if not meta:
        return
    data = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
    st.download_button(label, data=data, file_name="notion_export_checkpoint.json", mime="application/json", use_container_width=True)

def checkpoint_uploader(label="🔁 Checkpoint betöltése"):
    up = st.file_uploader(label, type=["json"], accept_multiple_files=False)
    if up is not None:
        try:
            meta = json.load(up)
            if not isinstance(meta, dict) or "mode" not in meta or "groups" not in meta:
                st.error("Érvénytelen checkpoint fájl.")
            else:
                st.session_state["resume"] = meta
                st.success("Checkpoint betöltve. Nyomd meg a 'Folytatás' gombot.")
        except Exception as e:
            st.error(f"Nem sikerült betölteni: {e}")

def resumable_xlsx(client: Client, dbid: str, prop_name: str, prop_type: str,
                   display_to_canon: Dict[str, Dict[str, Any]], title_prop: str,
                   section_prop: Optional[str], order_prop: Optional[str],
                   sorts: List[Dict[str, Any]]) -> bytes:
    meta = get_resume()
    groups = meta["groups"]
    done = set(meta.get("done", []))
    remaining = [g for g in groups if g not in done]

    reporter = ProgressReporter(total_steps=len(remaining), title="XLSX export folyamatban… (folytatható)")
    output = io.BytesIO()
    with _excel_writer(output) as writer:
        all_for_final = []

        for display_name in groups:
            canon = display_to_canon.get(display_name, {}).get("canonical", set())
            rows: List[Dict[str,str]] = []
            for cname in canon:
                rows = collect_rows_for_group(client, dbid, prop_name, prop_type, cname, title_prop, section_prop, order_prop, sorts, on_progress=lambda m: reporter.log(m))
                if rows:
                    break
            all_for_final.append((display_name, rows))
            if display_name in remaining:
                meta["done"].append(display_name)
                reporter.tick(f"{display_name} – {len(rows)} sor kész.")

        for display_name, rows in all_for_final:
            df = pd.DataFrame(rows, columns=CSV_FIELDNAMES)
            sheet = sanitize_sheet_name(display_name) or "lap"
            base = sheet; i = 1
            while sheet in getattr(writer, "sheets", {}):
                i += 1
                sheet = sanitize_sheet_name(f"{base}_{i}")
            df.to_excel(writer, index=False, sheet_name=sheet)

    output.seek(0)
    reporter.finish(ok=True, msg="XLSX elkészült.")
    st.session_state.pop("resume", None)
    return output.read()

def resumable_csv(client: Client, dbid: str, prop_name: str, prop_type: str,
                  display_to_canon: Dict[str, Dict[str, Any]], title_prop: str,
                  section_prop: Optional[str], order_prop: Optional[str],
                  sorts: List[Dict[str, Any]]) -> bytes:
    meta = get_resume()
    groups = meta["groups"]
    done = set(meta.get("done", []))
    remaining = [g for g in groups if g not in done]

    reporter = ProgressReporter(total_steps=len(remaining), title="CSV (egybefűzött) export folyamatban… (folytatható)")
    all_rows: List[Dict[str,str]] = []

    for display_name in groups:
        canon = display_to_canon.get(display_name, {}).get("canonical", set())
        rows: List[Dict[str,str]] = []
        for cname in canon:
            rows = collect_rows_for_group(client, dbid, prop_name, prop_type, cname, title_prop, section_prop, order_prop, sorts, on_progress=lambda m: reporter.log(m))
            if rows:
                break
        for r in rows:
            r2 = dict(r); r2["csoport"] = display_name; all_rows.append(r2)
        if display_name in remaining:
            meta["done"].append(display_name)
            reporter.tick(f"{display_name} – {len(rows)} sor hozzáadva.")

    buf = io.StringIO()
    pd.DataFrame(all_rows, columns=["csoport"] + CSV_FIELDNAMES).to_csv(buf, index=False)
    reporter.finish(ok=True, msg="CSV elkészült.")
    st.session_state.pop("resume", None)
    return buf.getvalue().encode("utf-8-sig")

# ----------------------------
# UI + login
# ----------------------------
def require_login():
    app_pw = get_secret("APP_PASSWORD")
    if not app_pw:
        st.warning("Nincs beállítva APP_PASSWORD – belépés kikapcsolva (dev mód).")
        return True
    if st.session_state.get("authed", False):
        return True
    with st.form("login"):
        pw = st.text_input("Jelszó", type="password")
        ok = st.form_submit_button("Belépés")
    if ok:
        if pw == app_pw:
            st.session_state.authed = True
            st.success("Sikeres belépés.")
            st.rerun()
        else:
            st.error("Hibás jelszó.")
    st.stop()

def main():
    st.set_page_config(page_title="Notion → Export (Kurzus)", page_icon="📦", layout="wide")
    st.title("📦 Notion → Export (Kurzus)")
    st.caption("Notion adatbázisból exportálás csoportok szerint • Beállítás: `NOTION_PROPERTY_NAME` (alap: „Kurzus”).")

    require_login()

    client = get_notion_client()
    dbid = get_database_id()
    schema = get_db_schema(dbid)

    PROPERTY_NAME = get_property_name()
    group_prop_name, group_prop_type, group_prop_meta = find_group_property(schema, PROPERTY_NAME)

    title_prop = detect_title_prop(schema)
    section_prop = best_effort_section_prop(schema)
    order_prop = best_effort_order_prop(schema)
    sorts = resolve_sorts(order_prop, title_prop)

    with st.expander("ℹ️ Használt mezők és rendezés", expanded=False):
        st.markdown(
            f"""
**Csoportosítás**: `{group_prop_name}` (*{group_prop_type}*)  
**Cím property**: `{title_prop or '—'}`  
**Szakasz property**: `{section_prop or '—'}`  
**Sorszám property**: `{order_prop or '—'}`  
**Rendezés**: {"Sorszám ↑" if order_prop else "Cím ↑"}
            """
        )

    st.write("Adatok beolvasása…")
    groups_sorted, display_to_canon = collect_group_index(client, dbid, group_prop_name, group_prop_type, group_prop_meta)
    if not groups_sorted:
        st.info("Nem találtam csoportokat/értékeket.")
        st.stop()

    st.subheader("Csoportok (db szerint csökkenő)")
    labels = [f"{name} ({cnt})" for (name, cnt, _) in groups_sorted]
    label_to_name = {f"{name} ({cnt})": name for (name, cnt, _) in groups_sorted}

    selected_labels = st.multiselect(
        "Válaszd ki, melyeket szeretnéd külön CSV-ként is:",
        options=labels,
        default=[],
        placeholder="(nem kötelező)"
    )

    st.divider()
    col1, col2, col3 = st.columns([1,1,1])

    with col1:
        st.markdown("#### Összes egy fájlban – Excel (több munkalap)")
        if st.button("⬇️ Letöltés (XLSX)"):
            groups_display = [name for (name,_,_) in groups_sorted]
            init_resume("xlsx", groups_display, group_prop_name, dbid)
            data = resumable_xlsx(
                client, dbid, group_prop_name, group_prop_type,
                display_to_canon, title_prop, section_prop, order_prop, sorts
            )
            st.download_button(
                "📥 összes_kurzus.xlsx",
                data=data,
                file_name="osszes_kurzus.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            checkpoint_download_button("💾 Checkpoint letöltése (XLSX)")

    with col2:
        st.markdown("#### Összes egy fájlban – CSV (egybefűzve)")
        if st.button("⬇️ Letöltés (CSV)"):
            groups_display = [name for (name,_,_) in groups_sorted]
            init_resume("csv", groups_display, group_prop_name, dbid)
            data = resumable_csv(
                client, dbid, group_prop_name, group_prop_type,
                display_to_canon, title_prop, section_prop, order_prop, sorts
            )
            st.download_button(
                "📥 osszes_kurzus.csv",
                data=data,
                file_name="osszes_kurzus.csv",
                mime="text/csv",
                use_container_width=True
            )
            checkpoint_download_button("💾 Checkpoint letöltése (CSV)")

    with col3:
        st.markdown("#### Kiválasztott csoportok – külön CSV-k")
        if not selected_labels:
            st.caption("Tipp: válassz a listából, ha külön CSV-k is kellenek.")
        for label in selected_labels:
            display_name = label_to_name.get(label, label)
            canon = display_to_canon.get(display_name, {}).get("canonical", set())
            rows: List[Dict[str,str]] = []
            last_msg = ""
            for cname in canon:
                rows = collect_rows_for_group(
                    client, dbid, group_prop_name, group_prop_type, cname,
                    title_prop, section_prop, order_prop, sorts,
                    on_progress=lambda m: st.write(m)
                )
                if rows:
                    last_msg = f"{display_name} – {len(rows)} sor kész."
                    break
            csv_bytes = io.StringIO()
            pd.DataFrame(rows, columns=CSV_FIELDNAMES).to_csv(csv_bytes, index=False)
            safe = slugify(display_name) or "export"
            st.download_button(
                f"📥 {display_name}.csv",
                data=csv_bytes.getvalue().encode("utf-8-sig"),
                file_name=f"export_{safe}.csv",
                mime="text/csv",
                use_container_width=True
            )
            if last_msg:
                st.caption(last_msg)

    st.divider()
    st.markdown("### 🔁 Megszakadt export folytatása")
    resume_meta = get_resume()
    if resume_meta:
        total = len(resume_meta.get("groups", []))
        done = len(resume_meta.get("done", []))
        st.info(f"Mentett állapot: **{resume_meta.get('mode').upper()}**, kész: {done}/{total}")
        colr1, colr2, colr3 = st.columns([1,1,1])
        with colr1:
            if st.button("▶️ Folytatás most"):
                if resume_meta.get("mode") == "xlsx":
                    data = resumable_xlsx(client, dbid, group_prop_name, group_prop_type, display_to_canon, title_prop, section_prop, order_prop, sorts)
                    st.download_button("📥 összes_kurzus.xlsx", data=data, file_name="osszes_kurzus.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                else:
                    data = resumable_csv(client, dbid, group_prop_name, group_prop_type, display_to_canon, title_prop, section_prop, order_prop, sorts)
                    st.download_button("📥 osszes_kurzus.csv", data=data, file_name="osszes_kurzus.csv", mime="text/csv", use_container_width=True)
        with colr2:
            checkpoint_download_button("💾 Checkpoint letöltése (aktuális)")
        with colr3:
            if st.button("🗑️ Mentés törlése"):
                st.session_state.pop("resume", None)
                st.success("Mentés törölve.")
    else:
        checkpoint_uploader("🔁 Checkpoint betöltése (JSON)")

    st.caption("© Notion → Export • Streamlit app")

if __name__ == "__main__":
    main()
