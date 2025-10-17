# Notion export – v7-stable
- Multiselect: **Név (db)**
- Login után `st.rerun()`
- Progress + státusz log
- XLSX több munkalap + egybefűzött CSV
- XlsxWriter → openpyxl fallback
- Folytatható export (checkpoint: cache + done)
- Notion retry (409/429/5xx) + 403/404 skip
- Watchdog: `AUTO_RERUN_SECONDS`, `MAX_GROUPS_PER_RUN`, `AUTO_RESUME`
