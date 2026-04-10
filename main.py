import csv
import io
import os
import time
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

APP_NAME = "Clay Middleware API - Scaled v2"
DATABASE_URL = os.getenv("DATABASE_URL", "")
INGEST_API_KEY = os.getenv("INGEST_API_KEY", "")
DESTINATION_API_URL = os.getenv("DESTINATION_API_URL", "")
DESTINATION_API_KEY = os.getenv("DESTINATION_API_KEY", "")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "1000"))

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=10,
    kwargs={"row_factory": dict_row},
)

app = FastAPI(title=APP_NAME)

def now_ts() -> int:
    return int(time.time())

def require_api_key(x_api_key: Optional[str]) -> None:
    if INGEST_API_KEY and x_api_key != INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

def init_db() -> None:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                CREATE TABLE IF NOT EXISTS records (
                    id BIGSERIAL PRIMARY KEY,
                    batch_name TEXT NULL,
                    external_id TEXT NULL,
                    payload JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'stored',
                    last_error TEXT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL
                )
                '''
            )
            cur.execute("ALTER TABLE records ADD COLUMN IF NOT EXISTS batch_name TEXT NULL")
            cur.execute(
                '''
                CREATE UNIQUE INDEX IF NOT EXISTS uq_records_external_id_batch
                ON records (external_id, COALESCE(batch_name, ''))
                WHERE external_id IS NOT NULL
                '''
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_status_id ON records(status, id DESC)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_created_at ON records(created_at DESC)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_batch_name ON records(batch_name)"
            )
        conn.commit()

@app.on_event("startup")
def startup_event() -> None:
    init_db()

class IngestBody(BaseModel):
    batch_name: Optional[str] = None
    data: Dict[str, Any]
    external_id: Optional[str] = None

class BulkIngestBody(BaseModel):
    rows: List[IngestBody] = Field(default_factory=list)

@app.get("/")
def root() -> Dict[str, str]:
    return {"name": APP_NAME, "status": "ok", "docs": "/docs"}

@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "healthy"}

def upsert_rows(rows: List[IngestBody]) -> Dict[str, int]:
    if not rows:
        return {"received": 0, "inserted_or_updated": 0}

    if len(rows) > MAX_BATCH_SIZE:
        raise HTTPException(status_code=400, detail=f"Batch too large. Max allowed is {MAX_BATCH_SIZE}")

    ts = now_ts()
    payload_rows = []
    for row in rows:
        payload_rows.append(
            (
                row.batch_name,
                row.external_id,
                row.data,
                "stored",
                None,
                ts,
                ts,
            )
        )

    sql = '''
        INSERT INTO records (batch_name, external_id, payload, status, last_error, created_at, updated_at)
        VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s)
        ON CONFLICT (external_id, COALESCE(batch_name, '')) WHERE external_id IS NOT NULL
        DO UPDATE SET
            payload = EXCLUDED.payload,
            status = 'stored',
            last_error = NULL,
            updated_at = EXCLUDED.updated_at,
            batch_name = EXCLUDED.batch_name
    '''

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, payload_rows)
        conn.commit()

    return {"received": len(rows), "inserted_or_updated": len(rows)}

@app.post("/ingest")
def ingest(body: IngestBody, x_api_key: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    require_api_key(x_api_key)
    result = upsert_rows([body])
    return {"success": True, **result}

@app.post("/bulk-ingest")
def bulk_ingest(body: BulkIngestBody, x_api_key: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    require_api_key(x_api_key)
    result = upsert_rows(body.rows)
    return {"success": True, **result}

@app.post("/ingest/raw")
async def ingest_raw(request: Request, x_api_key: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    require_api_key(x_api_key)
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Raw payload must be a JSON object")

    body = IngestBody(
        batch_name=payload.get("batch_name"),
        external_id=payload.get("external_id") or payload.get("id"),
        data=payload.get("data", payload),
    )
    result = upsert_rows([body])
    return {"success": True, **result}

@app.get("/records")
def list_records(
    limit: int = Query(default=100, ge=1, le=5000),
    status: Optional[str] = Query(default=None),
    batch_name: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    params: List[Any] = []
    sql = "SELECT id, batch_name, external_id, payload, status, last_error, created_at, updated_at FROM records"

    conditions = []
    if status:
        conditions.append("status = %s")
        params.append(status)
    if batch_name:
        conditions.append("batch_name = %s")
        params.append(batch_name)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return {"count": len(rows), "records": rows}

@app.get("/batches")
def list_batches() -> Dict[str, Any]:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                SELECT
                    batch_name,
                    COUNT(*) AS row_count,
                    MIN(created_at) AS first_created_at,
                    MAX(created_at) AS last_created_at
                FROM records
                WHERE batch_name IS NOT NULL
                GROUP BY batch_name
                ORDER BY last_created_at DESC
                '''
            )
            rows = cur.fetchall()

    return {"count": len(rows), "batches": rows}

@app.get("/export/json")
def export_json(
    limit: int = Query(default=1000, ge=1, le=50000),
    status: Optional[str] = Query(default=None),
    batch_name: Optional[str] = Query(default=None),
) -> JSONResponse:
    params: List[Any] = []
    sql = "SELECT id, batch_name, external_id, payload AS data, status, last_error, created_at, updated_at FROM records"

    conditions = []
    if status:
        conditions.append("status = %s")
        params.append(status)
    if batch_name:
        conditions.append("batch_name = %s")
        params.append(batch_name)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return JSONResponse(content={"count": len(rows), "rows": rows})

@app.get("/export/csv")
def export_csv(
    limit: int = Query(default=10000, ge=1, le=50000),
    status: Optional[str] = Query(default=None),
    batch_name: Optional[str] = Query(default=None),
) -> StreamingResponse:
    params: List[Any] = []
    sql = "SELECT id, batch_name, external_id, payload, status, last_error, created_at, updated_at FROM records"

    conditions = []
    if status:
        conditions.append("status = %s")
        params.append(status)
    if batch_name:
        conditions.append("batch_name = %s")
        params.append(batch_name)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    flattened_rows: List[Dict[str, Any]] = []
    for row in rows:
        item = {
            "id": row["id"],
            "batch_name": row["batch_name"],
            "external_id": row["external_id"],
            "status": row["status"],
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        payload = row["payload"] or {}
        if isinstance(payload, dict):
            item.update(payload)
        flattened_rows.append(item)

    if not flattened_rows:
        flattened_rows = [{"message": "no_data"}]

    output = io.StringIO()
    fieldnames = sorted({key for row in flattened_rows for key in row.keys()})
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(flattened_rows)
    output.seek(0)

    filename = "clay_export.csv" if not batch_name else f"{batch_name}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

@app.post("/push")
def push_records(
    limit: int = Query(default=500, ge=1, le=5000),
    only_status: str = Query(default="stored"),
    batch_name: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    if not DESTINATION_API_URL:
        raise HTTPException(status_code=400, detail="DESTINATION_API_URL is not configured")

    params: List[Any] = [only_status]
    sql = '''
        SELECT id, batch_name, payload
        FROM records
        WHERE status = %s
    '''
    if batch_name:
        sql += " AND batch_name = %s"
        params.append(batch_name)

    sql += " ORDER BY id ASC LIMIT %s"
    params.append(limit)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    headers = {"Content-Type": "application/json"}
    if DESTINATION_API_KEY:
        headers["Authorization"] = f"Bearer {DESTINATION_API_KEY}"

    results = []
    for row in rows:
        record_id = row["id"]
        payload = row["payload"]

        try:
            response = requests.post(
                DESTINATION_API_URL,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if 200 <= response.status_code < 300:
                status = "pushed"
                error_text = None
                ok = True
            else:
                status = "failed"
                error_text = response.text[:500]
                ok = False

            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        '''
                        UPDATE records
                        SET status = %s, last_error = %s, updated_at = %s
                        WHERE id = %s
                        ''',
                        (status, error_text, now_ts(), record_id),
                    )
                conn.commit()

            result = {
                "record_id": record_id,
                "batch_name": row["batch_name"],
                "ok": ok,
                "status_code": response.status_code,
            }
            if error_text:
                result["error"] = error_text
            results.append(result)

        except Exception as exc:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        '''
                        UPDATE records
                        SET status = %s, last_error = %s, updated_at = %s
                        WHERE id = %s
                        ''',
                        ("failed", str(exc), now_ts(), record_id),
                    )
                conn.commit()

            results.append({
                "record_id": record_id,
                "batch_name": row["batch_name"],
                "ok": False,
                "error": str(exc),
            })

    return {"count": len(results), "results": results}
