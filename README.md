# Clay Middleware API - Scaled Version

This version is meant for higher-volume use than the SQLite MVP.

## What changed

- PostgreSQL instead of SQLite
- bulk ingest endpoint for batching rows
- idempotency via `external_id`
- indexed status and created timestamps
- better fit for 5k-10k row pushes

## Recommended flow

Instead of sending one row at a time from Clay, send data in chunks:

- 200 rows
- 500 rows
- 1000 rows max per request

Use:

- `POST /bulk-ingest` for batch inserts
- `GET /export/json`
- `GET /export/csv`
- `POST /push`

## Endpoints

- `POST /ingest`
- `POST /bulk-ingest`
- `POST /ingest/raw`
- `GET /records`
- `GET /export/json`
- `GET /export/csv`
- `POST /push`

## Important notes

### 1. Use `external_id`
Set a stable ID from Clay so repeat syncs update existing rows instead of duplicating them.

Good examples:
- company domain
- Clay row ID
- email
- your own generated unique key

### 2. Batch sizes
For 5k-10k rows, do not send everything in one request.
Send in batches of 200-1000 rows.

### 3. Export
CSV export works fine for 10k-ish rows. For much larger datasets, move export jobs to background workers later.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
uvicorn main:app --reload
```

## Example bulk request

```json
{
  "rows": [
    {
      "external_id": "acme.com",
      "data": {
        "company": "Acme",
        "email": "john@acme.com"
      }
    },
    {
      "external_id": "beta.com",
      "data": {
        "company": "Beta",
        "email": "sarah@beta.com"
      }
    }
  ]
}
```

Header if using API key:

`x-api-key: YOUR_INGEST_API_KEY`

## What to do in Clay

If you can only send row-by-row from Clay, this still works.
But for real scale, use a batching layer or export rows in chunks before calling `/bulk-ingest`.

## Recommended production path after this

- add Redis queue + worker for background pushes
- add dead-letter handling for failed pushes
- add cursor-based export for 50k+ rows
- add S3/R2 export for big CSV files
