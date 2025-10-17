# 📦 Notion export – Kurzus (Streamlit, jelszóval)

- Multiselect: **Név (db)** formátum
- Login panel sikeres jelszó után **eltűnik**
- **Progress bar + státusz log** export közben
- Exportok: **XLSX (több munkalap)** és **CSV (egybefűzött)**
- **Excel-writer fallback**: XlsxWriter → openpyxl
- **Folytatható export**: checkpoint letöltés/betöltés + Folytatás gomb

## Secrets
`NOTION_API_KEY`, `NOTION_DATABASE_ID`, `APP_PASSWORD`, `NOTION_PROPERTY_NAME` (alap: `Kurzus`)
