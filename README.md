# Notion export – v6.1 (hotfix)
- Név (db) multiselect, login után `st.rerun()`
- Progress + státusz log
- XLSX (több munkalap) + összefűzött CSV
- XlsxWriter → openpyxl fallback
- Folytatható export (cache + done lista)
- Notion retry (429/5xx) + 403/404 skip
- Watchdog (AUTO_RERUN_SECONDS / MAX_GROUPS_PER_RUN / AUTO_RESUME)
- **Hotfix**: `collect_group_index` bekerült, így a belépés utáni NameError megszűnik.
