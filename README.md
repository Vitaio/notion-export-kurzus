
# Notion export – v8 (Google Sheets realtime – opcionális)

## Újdonságok
- **Google Sheets** integráció (opcionális, gspread): futás közben **csoportonként azonnal** ment a megadott táblázatba.
- A `Resume` munkalapra JSON formában menti az állapotot (groups/done/cache-meta), és **automatikusan be is tölti**, ha nincs session állapot.
- A csoportok **külön munkalapokra** kerülnek (ugyanazzal a névvel, mint az XLSX-ben).
- Minden korábbi funkció megmaradt: multiselect *Név (db)*, login után `st.rerun()`, progress, XLSX több munkalap, egybefűzött CSV, XlsxWriter→openpyxl fallback, folytatható export, watchdog.

## Beállítás (Streamlit secrets)
- `NOTION_API_KEY` – kötelező
- `NOTION_DATABASE_ID` – kötelező
- `APP_PASSWORD` – kötelező
- `NOTION_PROPERTY_NAME` – opcionális (alap: „Kurzus”)

**Google Sheets (opcionális):**
- `GOOGLE_SHEETS_SPREADSHEET_ID` – a cél spreadsheet azonosítója (URL-ből a /d/<ID>/ részt másold ki)
- `GOOGLE_SERVICE_ACCOUNT` – a service account JSON tartalma **egészben**, stringként (idézőjelek, kulcsok, stb.)  
  → Ezt a service account e-mail címével **megosztani** kell a spreadsheetet *Editor* jogosultsággal.

**Watchdog:**
- `AUTO_RERUN_SECONDS` – pl. 480 (8 perc)
- `MAX_GROUPS_PER_RUN` – pl. 8
- `AUTO_RESUME` – `true`/`false`

## Használat
- Letöltés (XLSX/CSV) közben a csoport adatai a Google Sheet **ugyanígy** íródnak ki.
- Ha megszakad a futás: induláskor, ha a Sheets `Resume` lapon talál állapotot, **aut. betölti** és a „Folytatás most”-tal mehetsz tovább.
- Egy csoport mindig **egyben kerül** a saját lapjára (ha újra futtatod ugyanazt a csoportot, a lapját **törlöm és újraírom** – így nincs duplikáció).
