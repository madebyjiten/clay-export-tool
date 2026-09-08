# Clay Middleware API - Scaled Version

Middleware between Clay and your data. Clay pushes rows in over HTTP; the rows
land in Postgres (**append-only** — nothing is de-duplicated); you read them back
as CSV/JSON or mirror the whole set into a Google Sheet.

## Flow

1. Clay HTTP action → `POST /bulk-ingest` (batches of 200–1000 rows).
2. Every row is stored as a new record. Repeats are kept — multiple contacts per
   domain, re-runs of the same Clay export, etc. `external_id` is just an
   optional passthrough label, not a key. De-dupe downstream when you want to.
3. Read the data:
   - `GET /export/csv` / `GET /export/json` — up to `EXPORT_MAX_ROWS` (default 50000).
   - `POST /sync-sheets` — overwrite a Google Sheet tab with the full dataset.
   - `POST /push` — forward rows to `DESTINATION_API_URL`.

## Endpoints

- `POST /ingest` — one row
- `POST /bulk-ingest` — batch (max `MAX_BATCH_SIZE`, default 1000)
- `POST /ingest/raw` — arbitrary JSON object
- `GET /records?limit=&status=` — inspect stored rows
- `GET /export/json?limit=&status=`
- `GET /export/csv?limit=&status=`
- `POST /sync-sheets?limit=` — rebuild the Google Sheet mirror from Postgres
- `POST /push?limit=&only_status=` — forward to a destination API

If `INGEST_API_KEY` is set, send `x-api-key: <key>` on every `POST`.

## Env vars

See `.env.example`. Key ones:

| var | purpose |
|---|---|
| `DATABASE_URL` | Postgres connection string (required) |
| `INGEST_API_KEY` | optional; gates all POST endpoints |
| `EXPORT_MAX_ROWS` | cap + default for CSV/JSON/sheet export (default 50000) |
| `GOOGLE_SA_JSON` | full service-account JSON key, as a string |
| `SHEET_ID` | the id in the Google Sheet URL |
| `SHEET_TAB` | tab name to write (default `Leads`, created if missing) |
| `SYNC_ON_INGEST` | `true` to refresh the sheet after each ingest (debounced) |
| `SHEET_SYNC_MIN_INTERVAL` | min seconds between auto-syncs (default 15) |

## Google Sheets mirror — one-time setup

1. Google Cloud Console → new project (or reuse one).
2. **APIs & Services → Enable APIs** → enable **Google Sheets API**.
3. **Credentials → Create credentials → Service account.** Name it, create.
4. On the service account → **Keys → Add key → JSON** → download.
5. Open your Google Sheet → **Share** → paste the service account's
   `client_email` (looks like `name@project.iam.gserviceaccount.com`) →
   give it **Editor** → send.
6. Copy the Sheet ID from the URL:
   `https://docs.google.com/spreadsheets/d/`**`THIS_PART`**`/edit`
7. In Railway → Variables:
   - `GOOGLE_SA_JSON` = the entire contents of the downloaded JSON file
   - `SHEET_ID` = the id from step 6
   - `SHEET_TAB` = `Leads` (or whatever tab you want)
8. Redeploy. Test: `curl -X POST https://YOUR-APP.up.railway.app/sync-sheets`
   (add `-H "x-api-key: ..."` if you set `INGEST_API_KEY`).

### Keep the sheet fresh automatically

Recommended: a Railway **cron** service that runs every 15 min:

```bash
curl -fsS -X POST "https://YOUR-APP.up.railway.app/sync-sheets" \
  -H "x-api-key: $INGEST_API_KEY"
```

Or set `SYNC_ON_INGEST=true` to refresh right after each ingest instead.

## De-dupe

Not done here, by design. If you later want the **Google Sheet** de-duplicated
(e.g. one row per email, keeping all contacts per domain), that can be added as
an opt-in flag on `/sync-sheets` without touching what Postgres stores.

## Limits

- **Export / sheet sync:** `EXPORT_MAX_ROWS` (default 50000). Raise the env var
  if you need more, but past ~50k the in-memory CSV build and a single-shot
  sheet overwrite get slow — that's the point to add cursor-based paging.
- **Google Sheets:** hard cap 10,000,000 cells (~500k rows at 20 columns), and
  sluggish well before that. Fine under 50k.
- **Sheets API:** ~60 write requests/min per user. `/sync-sheets` uses one write,
  so cron every 15 min is nowhere near the limit.

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
    { "external_id": "acme.com", "data": { "company": "Acme", "email": "john@acme.com" } },
    { "external_id": "beta.io",  "data": { "company": "Beta", "email": "sarah@beta.io" } }
  ]
}
```
