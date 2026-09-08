# Clay Middleware API - Scaled Version

Middleware between Clay and your data. Clay pushes rows in over HTTP; the rows
land in Postgres (**append-only** — nothing is de-duplicated); you read them back
as CSV/JSON, or mirror them into a Google Spreadsheet **one tab per export**.

## Flow

1. Clay HTTP action → `POST /bulk-ingest` (batches of 200–1000 rows). Send a
   `batch_name` with every row — that's what groups an export.
2. Every row is stored as a new record. Repeats are kept — multiple contacts per
   domain, re-runs of the same Clay export, etc. `external_id` is just an
   optional passthrough label, not a key. De-dupe downstream when you want to.
3. If the Google Sheets mirror is configured, each ingest rebuilds the tab(s)
   for the `batch_name`(s) it touched. One `batch_name` = one tab in the
   spreadsheet. Rows with no `batch_name` go to the `ungrouped` tab.
4. Also readable via `GET /export/csv` / `GET /export/json` and `POST /push`.

## Endpoints

- `POST /ingest` — one row
- `POST /bulk-ingest` — batch (max `MAX_BATCH_SIZE`, default 1000)
- `POST /ingest/raw` — arbitrary JSON object
- `GET /records?limit=&status=` — inspect stored rows
- `GET /batches` — list distinct `batch_name`s, row counts, and their tab names
- `GET /export/json?limit=&status=`
- `GET /export/csv?limit=&status=`
- `POST /sync-sheets?batch=<name>` — rebuild one batch's tab. Omit `batch` to
  rebuild **every** batch's tab (backfill). `batch=` empty or `__ungrouped__`
  targets the rows with no `batch_name`.
- `POST /push?limit=&only_status=` — forward to a destination API

If `INGEST_API_KEY` is set, send `x-api-key: <key>` on every `POST`.

## Env vars

See `.env.example`. Key ones:

| var | purpose |
|---|---|
| `DATABASE_URL` | Postgres connection string (required) |
| `INGEST_API_KEY` | optional; gates all POST endpoints |
| `EXPORT_MAX_ROWS` | cap + default for `/export/*` and per-tab sync (default 50000) |
| `GOOGLE_SA_JSON` | full service-account JSON key, as a string |
| `SHEET_ID` | the id in the spreadsheet URL |
| `UNGROUPED_TAB` | tab name for rows with no `batch_name` (default `ungrouped`) |
| `SYNC_ON_INGEST` | rebuild affected tabs after each ingest (default `true`) |
| `SHEET_SYNC_SETTLE_SECONDS` | wait before each rebuild so chunked exports collapse into one write (default 3) |

## Google Sheets mirror — one-time setup

1. Google Cloud Console → new project (or reuse one).
2. **APIs & Services → Enable APIs** → enable **Google Sheets API**.
3. **Credentials → Create credentials → Service account.** Name it, create.
4. On the service account → **Keys → Add key → JSON** → download.
5. Create the spreadsheet that will hold every export's tab. **Share** it with
   the service account's `client_email`
   (`name@project.iam.gserviceaccount.com`) as **Editor**.
6. Copy the spreadsheet ID from the URL:
   `https://docs.google.com/spreadsheets/d/`**`THIS_PART`**`/edit`
7. In Railway → Variables:
   - `GOOGLE_SA_JSON` = the entire contents of the downloaded JSON file
   - `SHEET_ID` = the id from step 6
8. Redeploy. Backfill tabs for the data already in Postgres:
   `curl -X POST https://YOUR-APP.up.railway.app/sync-sheets`
   (add `-H "x-api-key: ..."` if `INGEST_API_KEY` is set).

From then on, each ingest with a `batch_name` creates/refreshes that tab
automatically.

## De-dupe

Not done here, by design. Every row Clay sends is kept.

## Limits

- **Per tab:** `EXPORT_MAX_ROWS` rows (default 50000). Each export is normally a
  few thousand rows, so this is not a concern; a single export bigger than
  ~50k would need chunked tab writes.
- **Spreadsheet:** Google caps a spreadsheet at 10,000,000 cells total across
  all tabs. At ~20 columns and a few thousand rows per export that's hundreds
  of exports before it matters.
- **Sheets API:** ~60 write requests/min per user. Each tab rebuild is ~2
  writes and they're serialised, so bursts queue rather than fail.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill it in
export $(grep -v '^#' .env | xargs)
uvicorn main:app --reload
```

## Example bulk request

```json
{
  "rows": [
    { "external_id": "acme.com", "data": { "batch_name": "CC 12th July", "company": "Acme", "email": "john@acme.com" } },
    { "external_id": "beta.io",  "data": { "batch_name": "CC 12th July", "company": "Beta", "email": "sarah@beta.io" } }
  ]
}
```
