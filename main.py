import csv
import io
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from google.oauth2.service_account import Credentials
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
# One spreadsheet (SHEET_ID). Every distinct Clay `batch_name` becomes its own
# tab in that spreadsheet, rebuilt from Postgres. Rows with no batch_name land
# in the UNGROUPED_TAB tab.
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON", "")
SHEET_ID = os.getenv("SHEET_ID", "")
UNGROUPED_TAB = os.getenv("UNGROUPED_TAB", "ungrouped")
SYNC_ON_INGEST = os.getenv("SYNC_ON_INGEST", "true").lower() in {"1", "true", "yes", "on"}
# Seconds a batch's sync worker waits before each rebuild, so a burst of chunks
# for the same export collapses into one write instead of one write per chunk.
SHEET_SYNC_SETTLE_SECONDS = float(os.getenv("SHEET_SYNC_SETTLE_SECONDS", "3"))
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

# All Google Sheets writes are serialised through this lock.
_sheets_lock = threading.Lock()
# Coalescing state for the post-ingest background sync (see _schedule_batch_syncs).
_batch_sync_lock = threading.Lock()
_batch_pending: set = set()
_batch_active: set = set()
# Internal key standing in for "rows with no batch_name".
_UNGROUPED = "\x00__ungrouped__\x00"

_gspread_client = None
_TAB_BAD_CHARS = re.compile(r"[\[\]:\\/?*\x00-\x1f]")


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
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_external_id ON records(external_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_status_id ON records(status, id DESC)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_created_at ON records(created_at DESC)"
            )
            # Group-by-batch lookups for the per-tab sheet sync.
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_batch_name "
                "ON records((payload->>'batch_name'))"
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
        "spreadsheet_id": SHEET_ID or None,
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "healthy"}


def effective_batch_name(row: IngestBody) -> Optional[str]:
    """The batch_name a row will be grouped by: explicit field, else data.batch_name."""
    if row.batch_name:
        return row.batch_name
    if isinstance(row.data, dict):
        value = row.data.get("batch_name")
        if value:
            return str(value)
    return None


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


def _batch_keys_from_rows(rows: List[IngestBody]) -> List[str]:
    keys = set()
    for row in rows:
        name = effective_batch_name(row)
        keys.add(name if name else _UNGROUPED)
    return list(keys)


def _after_ingest(rows: List[IngestBody], background_tasks: BackgroundTasks) -> None:
    if SYNC_ON_INGEST and sheets_configured():
        background_tasks.add_task(_schedule_batch_syncs, _batch_keys_from_rows(rows))


@app.post("/ingest")
def ingest(
    body: IngestBody,
    background_tasks: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_api_key(x_api_key)
    result = insert_rows([body])
    _after_ingest([body], background_tasks)
    return {"success": True, **result}


@app.post("/bulk-ingest")
def bulk_ingest(
    body: BulkIngestBody,
    background_tasks: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_api_key(x_api_key)
    result = insert_rows(body.rows)
    _after_ingest(body.rows, background_tasks)
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
    _after_ingest([body], background_tasks)
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
    limit: Optional[int] = None,
    status: Optional[str] = None,
    batch_name: Optional[str] = None,
    ungrouped: bool = False,
    order: str = "desc",
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Pull records and flatten each JSONB payload up to the top level.

    Returns (fieldnames, rows) where fieldnames is the sorted union of every
    key seen, so a Clay column that only appears on some rows still gets a
    column in the output. Optionally scoped to one batch_name (or the rows
    with no batch_name when `ungrouped` is True).
    """
    limit = limit or EXPORT_MAX_ROWS
    where: List[str] = []
    params: List[Any] = []

    if status:
        where.append("status = %s")
        params.append(status)
    if ungrouped:
        where.append("(payload->>'batch_name') IS NULL")
    elif batch_name is not None:
        where.append("payload->>'batch_name' = %s")
        params.append(batch_name)

    sql = "SELECT id, external_id, payload, status, last_error, created_at, updated_at FROM records"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY id {'ASC' if order == 'asc' else 'DESC'} LIMIT %s"
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
# Google Sheets mirror — one tab per Clay batch_name
# ---------------------------------------------------------------------------

def _cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def tab_name_for(batch_name: Optional[str]) -> str:
    """Sheet-tab-safe name for a batch. None -> the ungrouped tab."""
    if batch_name is None:
        return UNGROUPED_TAB
    name = _TAB_BAD_CHARS.sub(" ", str(batch_name)).strip()
    name = re.sub(r"\s+", " ", name)
    return name[:90] or "export"


def _get_spreadsheet():
    global _gspread_client
    if _gspread_client is None:
        info = json.loads(GOOGLE_SA_JSON)
        creds = Credentials.from_service_account_info(info, scopes=SHEETS_SCOPES)
        _gspread_client = gspread.authorize(creds)
    return _gspread_client.open_by_key(SHEET_ID)


def distinct_batches() -> List[Tuple[Optional[str], int]]:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload->>'batch_name' AS batch_name, count(*) AS n "
                "FROM records GROUP BY 1 ORDER BY n DESC"
            )
            return [(r["batch_name"], r["n"]) for r in cur.fetchall()]


def sync_one_tab(key: str) -> Dict[str, Any]:
    """Rebuild a single tab from Postgres. `key` is a batch_name or _UNGROUPED."""
    if key == _UNGROUPED:
        title = UNGROUPED_TAB
        fieldnames, rows = fetch_flattened(ungrouped=True, order="asc")
    else:
        title = tab_name_for(key)
        fieldnames, rows = fetch_flattened(batch_name=key, order="asc")

    values: List[List[Any]] = [fieldnames or ["message"]]
    for row in rows:
        values.append([_cell(row.get(name)) for name in fieldnames])
    if not rows:
        values.append(["no_data"])

    n_rows = max(len(values), 1)
    n_cols = max(len(fieldnames), 1)

    with _sheets_lock:
        spreadsheet = _get_spreadsheet()
        try:
            worksheet = spreadsheet.worksheet(title)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=title, rows=n_rows, cols=n_cols)
        worksheet.resize(rows=n_rows, cols=n_cols)
        worksheet.update(values=values, range_name="A1", value_input_option="RAW")

    logger.info("synced tab %r: %d rows x %d cols", title, len(rows), len(fieldnames))
    return {"tab": title, "rows": len(rows), "columns": len(fieldnames)}


def _schedule_batch_syncs(keys: List[str]) -> None:
    """Queue tab rebuilds, coalescing repeats for the same batch into one run."""
    if not sheets_configured():
        return
    to_start: List[str] = []
    with _batch_sync_lock:
        for key in keys:
            _batch_pending.add(key)
            if key not in _batch_active:
                _batch_active.add(key)
                to_start.append(key)
    for key in to_start:
        threading.Thread(target=_batch_sync_worker, args=(key,), daemon=True).start()


def _batch_sync_worker(key: str) -> None:
    while True:
        if SHEET_SYNC_SETTLE_SECONDS > 0:
            time.sleep(SHEET_SYNC_SETTLE_SECONDS)
        with _batch_sync_lock:
            if key not in _batch_pending:
                _batch_active.discard(key)
                return
            _batch_pending.discard(key)
        try:
            sync_one_tab(key)
        except Exception:
            logger.exception("sheet sync failed for batch=%r", key)
            time.sleep(3)  # brief backoff; a later ingest re-queues this batch


@app.get("/batches")
def batches() -> Dict[str, Any]:
    return {
        "batches": [
            {
                "batch_name": name,
                "rows": count,
                "tab": tab_name_for(name),
            }
            for name, count in distinct_batches()
        ]
    }


@app.post("/sync-sheets")
def sync_sheets_endpoint(
    batch: Optional[str] = Query(
        default=None,
        description="Batch name to sync. Omit to (re)build every batch's tab. "
        "Use an empty value or '__ungrouped__' for rows with no batch_name.",
    ),
    x_api_key: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_api_key(x_api_key)
    if not sheets_configured():
        raise HTTPException(
            status_code=400,
            detail="Google Sheets mirror is not configured (set GOOGLE_SA_JSON and SHEET_ID)",
        )

    try:
        if batch is not None:
            key = _UNGROUPED if batch in ("", "__ungrouped__") else batch
            return {"synced": [sync_one_tab(key)]}

        results = []
        for name, _count in distinct_batches():
            results.append(sync_one_tab(_UNGROUPED if name is None else name))
        return {"tabs": len(results), "synced": results}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Sheet sync failed: {exc}")


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
