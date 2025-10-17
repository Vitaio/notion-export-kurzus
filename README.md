
# 📦 Notion export – Kurzus (Streamlit, jelszóval)

Ez egy egyszerű, jelszóval védett webapp a Notion adatbázisod (Database ID: `623b8b80decd4f24a77a52c0d1dfc6ae`) "Kurzus" oszlopa alapján történő exporthoz.

## Fájlok
- `app.py` – Streamlit alkalmazás
- `requirements.txt` – Python függőségek

## Helyi futtatás
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export NOTION_API_KEY="secret_xxx"
export NOTION_DATABASE_ID="623b8b80decd4f24a77a52c0d1dfc6ae"
export APP_PASSWORD="valamiErősJelszó"

streamlit run app.py
```

## Deploy – Streamlit Community Cloud
1. Pushold a repo-t GitHubra (legalább: `app.py`, `requirements.txt`).
2. Lépj be: https://share.streamlit.io → **Deploy a public app from GitHub**.
3. App Settings → **Secrets**:
   ```
   NOTION_API_KEY = "secret_xxx"
   NOTION_DATABASE_ID = "623b8b80decd4f24a77a52c0d1dfc6ae"
   APP_PASSWORD = "valamiErősJelszó"
   ```
4. Deploy. A kapott URL nyilvános, de az alkalmazás **saját jelszóval** védett.

> Ha privát hozzáférést akarsz az URL szintjén is, tedd Cloudflare Access / Google IAP mögé, vagy válaszd a „Deploy a private app in Snowflake” opciót (enterprise).

## Megjelenítési átnevezések
Az app a listában átnevezi a valós Notion-neveket:
- `Üzleti Modellek` → **Milyen vállalkozást indíts**
- `Marketing rendszerek` → **Ügyfélszerző marketing rendszerek**

Szerkeszthető az `app.py` tetején a `DISPLAY_RENAMES` szótárban.
