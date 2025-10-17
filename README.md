# Notion export – v6 (watchdog + auto-rerun)

- Multiselect: **Név (db)**
- Login után `st.rerun()`
- **Progress + státusz log**
- **XLSX több munkalap** + **egybefűzött CSV**
- **XlsxWriter → openpyxl fallback**
- **Folytatható export** (kész csoportok cache-ből)
- **Robusztus Notion hívások** (retry 429/5xx, 403/404 skip)
- **Watchdog**: hosszú futásokkor automatikus részfutam + `st.rerun()`

## Konfiguráció (Secrets)
- `AUTO_RERUN_SECONDS` (alap: 540 = 9 perc)
- `MAX_GROUPS_PER_RUN` (alap: 0 = nincs limit)
- `AUTO_RESUME` (true/false; ha true, a rerun után automatikusan folytat)
