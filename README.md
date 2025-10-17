# 📦 Notion export – Kurzus (Streamlit, jelszóval)

- Multiselect opciók: **Név (db)** formátumban.
- Sikeres login után a belépő panel azonnal **eltűnik**.
- **Progress bar + státusz log** export közben.
- Exportok: **XLSX (több munkalap)** és **CSV (egybefűzött)**.
- **Excel-writer fallback**: XlsxWriter → openpyxl.
- **Folytatható** export: checkpoint **letöltés/betöltés**, és **Folytatás** gomb.

## Secrets / környezeti változók
- `NOTION_API_KEY`
- `NOTION_DATABASE_ID`
- `APP_PASSWORD`
- `NOTION_PROPERTY_NAME` (alap: `Kurzus`)

## Helyi futtatás
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export NOTION_API_KEY="secret_xxx"
export NOTION_DATABASE_ID="xxxxxxxxxxxxxxxxxxxxxxxx"
export APP_PASSWORD="erős_jelszó"
streamlit run app.py
```

