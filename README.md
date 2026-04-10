# Clay Middleware API - Scaled v2

This version adds real batch support.

## New features

- `batch_name` stored as its own database column
- export CSV/JSON by batch
- list all batches with `/batches`
- works with existing deployments by adding `batch_name` automatically on startup

## Request format

```json
{
  "batch_name": "apr09_india_ai_companies",
  "external_id": "acme.com",
  "data": {
    "first_name": "John",
    "last_name": "Doe",
    "full_name": "John Doe",
    "email": "john@acme.com"
  }
}
```

## Useful endpoints

- `POST /ingest`
- `POST /bulk-ingest`
- `GET /batches`
- `GET /records?batch_name=apr09_india_ai_companies`
- `GET /export/json?batch_name=apr09_india_ai_companies`
- `GET /export/csv?batch_name=apr09_india_ai_companies`
