import csv
import io
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

APP_NAME = "Clay Middleware API - Scaled"
DATABASE_URL = os.getenv("DATABASE_URL", "")
INGEST_API_KEY = os.getenv("INGEST_API_KEY", "")
DESTINATION_API_URL = os.getenv("DESTINATION_API_URL", "")
DESTINATION_API_KEY = os.getenv("DESTINATION_API_KEY", "")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "1000"))
EXPORT_MAX_ROWS = int(os.getenv("EXPORT_MAX_ROWS", "50000"))

# --- Google Sheets mirror config ---
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON", "")
SHEET_ID = os.getenv("SHEET_ID", "")
SHEET_TAB = os.getenv("SHEET_TAB", "Leads")
SYNC_ON_INGEST = os.getenv("SYNC_ON_INGEST", "").lower() in {"1", "true", "yes", "on"}
SHEET_SYNC_MIN_INTERVAL = int(os.getenv("SHEET_SYNC_MIN_INTERVAL", "15"))
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("clay-middleware")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=10,
    kwargs={"row_factory": dict_row},
)

app = FastAPI(title=APP_NAME)

_sheets_lock = threading.Lock()
_last_sheet_sync = 0.0


def now_ts() -> int:
    return int(time.time())


def sheets_configured() -> bool:
    return bool(GOOGLE_SA_JSON and SHEET_ID)


def require_api_key(x_api_key: Optional[str]) -> None:
    if INGEST_API_KEY and x_api_key != INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


def normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        json.dumps(payload)
    except TypeError as exc:
        raise HTTPException(status_code=400, detail=f"Payload is not JSON serializable: {exc}")
    return payload


def init_db() -> None:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                CREATE TABLE IF NOT EXISTS records (
                    id BIGSERIAL PRIMARY KEY,
                    external_id TEXT NULL,
                    payload JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'stored',
                    last_error TEXT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL
                )
                '''
            )
            # Append-only: every ingested row is kept, even repeats of the same
            # domain / external_id (multiple contacts per company, re-runs of the
            # same Clay export, etc.). De-duplication is done downstream, not here.
            # Plain (non-unique) index just to speed up later lookups by external_id.
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_external_id ON records(external_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_status_id ON records(status, id DESC)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_created_at ON records(created_at DESC)"
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
def root() -> Dict[str, Any]:
    return {
        "name": APP_NAME,
        "status": "ok",
        "docs": "/docs",
        "sheets_mirror": "configured" if sheets_configured() else "not_configured",
        "sync_on_ingest": SYNC_ON_INGEST,
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "healthy"}


def insert_rows(rows: List[IngestBody]) -> Dict[str, int]:
    if not rows:
        return {"received": 0, "inserted": 0}

    if len(rows) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Batch too large. Max allowed is {MAX_BATCH_SIZE}"
        )

    ts = now_ts()
    payload = []
    for row in rows:
        normalized = normalize_payload(row.data)
        if row.batch_name:
            normalized["batch_name"] = row.batch_name
        payload.append(
            (
                row.external_id,
                json.dumps(normalized),
                "stored",
                None,
                ts,
                ts,
            )
        )

    # Append-only. Every row is inserted as a new record, including repeats.
    sql = '''
    INSERT INTO records (external_id, payload, status, last_error, created_at, updated_at)
    VALUES (%s, %s::jsonb, %s, %s, %s, %s)
'''

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, payload)
        conn.commit()

    return {"received": len(rows), "inserted": len(rows)}


@app.post("/ingest")
def ingest(
    body: IngestBody,
    background_tasks: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_api_key(x_api_key)
    result = insert_rows([body])
    if SYNC_ON_INGEST:
        background_tasks.add_task(maybe_sync_sheet)
    return {"success": True, **result}


@app.post("/bulk-ingest")
def bulk_ingest(
    body: BulkIngestBody,
    background_tasks: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_api_key(x_api_key)
    result = insert_rows(body.rows)
    if SYNC_ON_INGEST:
        background_tasks.add_task(maybe_sync_sheet)
    return {"success": True, **result}


@app.post("/ingest/raw")
async def ingest_raw(
    request: Request,
    background_tasks: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_api_key(x_api_key)
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Raw payload must be a JSON object")
    body = IngestBody(
        batch_name=payload.get("batch_name"),
        external_id=payload.get("external_id") or payload.get("id"),
        data=payload.get("data", payload),
    )
    result = insert_rows([body])
    if SYNC_ON_INGEST:
        background_tasks.add_task(maybe_sync_sheet)
    return {"success": True, **result}


@app.get("/records")
def list_records(
    limit: int = Query(default=100, ge=1, le=5000),
    status: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    params: List[Any] = []
    sql = "SELECT id, external_id, payload, status, last_error, created_at, updated_at FROM records"

    if status:
        sql += " WHERE status = %s"
        params.append(status)

    sql += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return {"count": len(rows), "records": rows}


def fetch_flattened(
    limit: int,
    status: Optional[str] = None,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Pull records and flatten each JSONB payload up to the top level.

    Returns (fieldnames, rows) where fieldnames is the sorted union of every
    key seen, so a Clay column that only appears on some rows still gets a
    column in the output.
    """
    params: List[Any] = []
    sql = "SELECT id, external_id, payload, status, last_error, created_at, updated_at FROM records"

    if status:
        sql += " WHERE status = %s"
        params.append(status)

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

    fieldnames = sorted({key for row in flattened_rows for key in row.keys()})
    return fieldnames, flattened_rows


@app.get("/export/json")
def export_json(
    limit: int = Query(default=EXPORT_MAX_ROWS, ge=1, le=EXPORT_MAX_ROWS),
    status: Optional[str] = Query(default=None),
) -> JSONResponse:
    params: List[Any] = []
    sql = "SELECT id, external_id, payload AS data, status, last_error, created_at, updated_at FROM records"

    if status:
        sql += " WHERE status = %s"
        params.append(status)

    sql += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return JSONResponse(content={"count": len(rows), "rows": rows})


@app.get("/export/csv")
def export_csv(
    limit: int = Query(default=EXPORT_MAX_ROWS, ge=1, le=EXPORT_MAX_ROWS),
    status: Optional[str] = Query(default=None),
) -> StreamingResponse:
    fieldnames, flattened_rows = fetch_flattened(limit=limit, status=status)

    if not flattened_rows:
        flattened_rows = [{"message": "no_data"}]
        fieldnames = ["message"]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(flattened_rows)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=clay_export.csv"},
    )


# ---------------------------------------------------------------------------
# Google Sheets mirror
# ---------------------------------------------------------------------------

def _cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _get_worksheet():
    import gspread
    from google.oauth2.service_account import Credentials

    info = json.loads(GOOGLE_SA_JSON)
    creds = Credentials.from_service_account_info(info, scopes=SHEETS_SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)
    try:
        return spreadsheet.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=SHEET_TAB, rows=1000, cols=26)


def sync_sheet(limit: Optional[int] = None) -> Dict[str, Any]:
    """Overwrite the target tab with the full dataset from Postgres (no de-dup)."""
    if not sheets_configured():
        return {"synced": False, "reason": "sheets_not_configured"}

    limit = limit or EXPORT_MAX_ROWS
    fieldnames, rows = fetch_flattened(limit=limit)
    values: List[List[Any]] = [fieldnames]
    for row in rows:
        values.append([_cell(row.get(name)) for name in fieldnames])

    with _sheets_lock:
        worksheet = _get_worksheet()
        worksheet.resize(rows=max(len(values), 1), cols=max(len(fieldnames), 1))
        worksheet.update(range_name="A1", values=values, value_input_option="RAW")

    logger.info("sheet sync ok: %d rows, %d columns", len(rows), len(fieldnames))
    return {"synced": True, "rows": len(rows), "columns": len(fieldnames)}


def maybe_sync_sheet() -> Dict[str, Any]:
    """Debounced sync used by the post-ingest background task."""
    global _last_sheet_sync
    if not sheets_configured():
        return {"synced": False, "reason": "sheets_not_configured"}
    now = time.time()
    if now - _last_sheet_sync < SHEET_SYNC_MIN_INTERVAL:
        return {"synced": False, "reason": "debounced"}
    _last_sheet_sync = now
    try:
        return sync_sheet()
    except Exception as exc:  # never let a Sheets hiccup break ingest
        logger.exception("sheet sync failed")
        return {"synced": False, "error": str(exc)[:500]}


@app.post("/sync-sheets")
def sync_sheets_endpoint(
    limit: int = Query(default=EXPORT_MAX_ROWS, ge=1, le=EXPORT_MAX_ROWS),
    x_api_key: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_api_key(x_api_key)
    if not sheets_configured():
        raise HTTPException(
            status_code=400,
            detail="Google Sheets mirror is not configured (set GOOGLE_SA_JSON and SHEET_ID)",
        )
    global _last_sheet_sync
    try:
        result = sync_sheet(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Sheet sync failed: {exc}")
    _last_sheet_sync = time.time()
    return result


@app.post("/push")
def push_records(
    limit: int = Query(default=500, ge=1, le=5000),
    only_status: str = Query(default="stored"),
) -> Dict[str, Any]:
    if not DESTINATION_API_URL:
        raise HTTPException(status_code=400, detail="DESTINATION_API_URL is not configured")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                SELECT id, payload
                FROM records
                WHERE status = %s
                ORDER BY id ASC
                LIMIT %s
                ''',
                (only_status, limit),
            )
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

            result = {"record_id": record_id, "ok": ok, "status_code": response.status_code}
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

            results.append({"record_id": record_id, "ok": False, "error": str(exc)})

    return {"count": len(results), "results": results}
