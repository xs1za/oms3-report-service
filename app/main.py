import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread
from typing import Annotated
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from uuid import uuid4
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

from fastapi import FastAPI, Header, HTTPException, Query, Response, status
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.healthcheck.router import router as healthcheck_router
from app.kafka import consume_events, publish_event
from app.logging_config import configure_logging
from app.problem_events import (
    EventProcessingError,
    enqueue_manual_reprocess,
    get_problem_event,
    list_problem_events,
    mark_manual_status,
    register_problem_event,
)
from app.settings import settings

configure_logging()

app = FastAPI(title="OMS3 Report Service", version="0.1.0", root_path=settings.root_path)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8088", "http://127.0.0.1:8088"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(healthcheck_router)

logger = logging.getLogger(__name__)

report_tasks: dict[str, dict] = {}
report_cache_stale_by_shift: dict[str, dict] = {}
processed_shift_status_events: set[str] = set()

SHIFT_REPORT_COLUMNS = [
    ("shift_id", "id"),
    ("client_id", "client_id"),
    ("starts_at", "starts_at"),
    ("ends_at", "ends_at"),
    ("status", "status"),
    ("location", "location"),
    ("assigned_performer_id", "assigned_performer_id"),
    ("close_reason", "close_reason"),
    ("failure_reason", "failure_reason"),
    ("created_at", "created_at"),
    ("updated_at", "updated_at"),
]
XLSX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class ReportTaskCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    report_type: Annotated[str, Field(alias="reportType", examples=["orders"])]
    filter: dict = Field(default_factory=dict)
    format: str = Field(default="xlsx", examples=["xlsx"])

    @field_validator("format")
    @classmethod
    def validate_format(cls, value: str) -> str:
        allowed = {"xlsx", "csv", "pdf"}
        if value not in allowed:
            raise ValueError(f"format must be one of: {', '.join(sorted(allowed))}")
        return value


class ManualProblemEventAction(BaseModel):
    comment: str


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def utc_iso(value: datetime) -> str:
    return as_aware(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_datetime(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"{field_name} must be a valid ISO 8601 datetime") from exc
    return as_aware(parsed).astimezone(timezone.utc)


def validate_shifts_report_request(payload: ReportTaskCreate) -> tuple[datetime | None, datetime | None]:
    if payload.report_type != "shifts":
        return None, None
    if payload.format != "xlsx":
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="shifts report supports only xlsx format")
    starts_at_from_raw = payload.filter.get("startsAtFrom")
    starts_at_to_raw = payload.filter.get("startsAtTo")
    if not starts_at_from_raw or not starts_at_to_raw:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="startsAtFrom and startsAtTo are required for shifts report")
    starts_at_from = parse_datetime(starts_at_from_raw, "startsAtFrom")
    starts_at_to = parse_datetime(starts_at_to_raw, "startsAtTo")
    if starts_at_to < starts_at_from:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="startsAtTo must be greater than or equal to startsAtFrom")
    if starts_at_to - starts_at_from > timedelta(days=366):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Period must not exceed 366 calendar days")
    return starts_at_from, starts_at_to


def report_storage_dir() -> Path:
    path = Path(settings.report_storage_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def fetch_shifts_from_oms5(starts_at_from: datetime, starts_at_to: datetime) -> list[dict]:
    query = urlencode({"startsAtFrom": utc_iso(starts_at_from), "startsAtTo": utc_iso(starts_at_to)})
    url = f"{settings.oms5_internal_base_url.rstrip('/')}/internal/shifts?{query}"
    try:
        with urlopen(url, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except (OSError, URLError, json.JSONDecodeError) as exc:
        raise RuntimeError("Failed to fetch shifts from OMS5") from exc


def xlsx_cell(column_index: int, row_index: int, value: object) -> str:
    column_name = chr(ord("A") + column_index)
    cell_ref = f"{column_name}{row_index}"
    if value is None:
        text = ""
    else:
        text = str(value)
    return f'<c r="{cell_ref}" t="inlineStr"><is><t>{escape(text)}</t></is></c>'


def build_xlsx(rows: list[list[object]], output_path: Path) -> None:
    sheet_rows = []
    for row_index, row in enumerate(rows, start=1):
        cells = "".join(xlsx_cell(column_index, row_index, value) for column_index, value in enumerate(row))
        sheet_rows.append(f'<row r="{row_index}">{cells}</row>')
    sheet_xml = "".join(sheet_rows)
    with ZipFile(output_path, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
        archive.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        archive.writestr("xl/workbook.xml", '<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Shifts" sheetId="1" r:id="rId1"/></sheets></workbook>')
        archive.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
        archive.writestr("xl/worksheets/sheet1.xml", f'<?xml version="1.0" encoding="UTF-8"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>{sheet_xml}</sheetData></worksheet>')


def shifts_report_file_name(task_id: str, starts_at_from: datetime, starts_at_to: datetime) -> str:
    return f"shifts-{task_id}-{starts_at_from:%Y%m%d}-{starts_at_to:%Y%m%d}.xlsx"


def build_shifts_report(task: dict) -> None:
    task["status"] = "running"
    task["progress"] = 25
    task["startedAt"] = utcnow()
    task["updatedAt"] = task["startedAt"]
    starts_at_from = parse_datetime(task["parameters"]["filter"]["startsAtFrom"], "startsAtFrom")
    starts_at_to = parse_datetime(task["parameters"]["filter"]["startsAtTo"], "startsAtTo")
    logger.info("Started shifts XLSX report", extra={"task_id": task["id"], "startsAtFrom": utc_iso(starts_at_from), "startsAtTo": utc_iso(starts_at_to)})
    shifts = fetch_shifts_from_oms5(starts_at_from, starts_at_to)
    rows = [[header for header, _ in SHIFT_REPORT_COLUMNS]]
    rows.extend([[shift.get(field) for _, field in SHIFT_REPORT_COLUMNS] for shift in shifts])
    file_name = shifts_report_file_name(task["id"], starts_at_from, starts_at_to)
    file_path = report_storage_dir() / file_name
    build_xlsx(rows, file_path)
    completed_at = utcnow()
    task.update(
        {
            "status": "completed",
            "progress": 100,
            "reportId": task["reportId"] or f"rpt_{uuid4().hex[:12]}",
            "completedAt": completed_at,
            "updatedAt": completed_at,
            "result": {
                "fileName": file_name,
                "downloadUrl": f"/api/v1/report-tasks/{task['id']}/download",
                "expiresAt": completed_at + timedelta(days=30),
                "filePath": str(file_path),
                "sizeBytes": file_path.stat().st_size,
            },
        }
    )
    logger.info("Completed shifts XLSX report", extra={"task_id": task["id"], "fileName": file_name, "row_count": len(shifts)})


def process_report_task(task_id: str) -> None:
    task = report_tasks.get(task_id)
    if task is None or task["parameters"].get("reportType") != "shifts":
        return
    try:
        build_shifts_report(task)
    except Exception as exc:
        failed_at = utcnow()
        task.update({"status": "failed", "progress": 100, "updatedAt": failed_at, "error": {"message": str(exc)}})
        logger.exception("Failed shifts XLSX report", extra={"task_id": task_id})


def cleanup_expired_reports() -> None:
    now = utcnow()
    for task in report_tasks.values():
        result = task.get("result") or {}
        expires_at = result.get("expiresAt")
        if not expires_at or as_aware(expires_at) > now:
            continue
        file_path = result.get("filePath")
        if file_path:
            path = Path(file_path)
            if path.exists():
                path.unlink()
                logger.info("Deleted expired report file", extra={"task_id": task["id"], "fileName": result.get("fileName"), "expiresAt": utc_iso(as_aware(expires_at))})
        if task["status"] == "completed":
            task["status"] = "expired"
            task["updatedAt"] = now


def require_operations_role(x_operational_role: str | None) -> None:
    if x_operational_role != "operations":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Operations role is required")


def process_shift_status_changed_event(event: dict) -> None:
    if event.get("event_type") != "operations.shift.status_changed":
        raise EventProcessingError("contract", "unsupported_event_type", "Unsupported event_type")
    if event.get("schema_version") != 1:
        raise EventProcessingError("contract", "unsupported_schema_version", "Unsupported schema_version")
    event_id = event.get("event_id")
    shift_id = event.get("shift_id")
    if not event_id or not shift_id:
        raise EventProcessingError("contract", "required_field_missing", "event_id and shift_id are required")
    if event.get("external_store_id") == "missing":
        raise EventProcessingError("business", "store_not_found", "Store is not registered in platform")
    if event.get("reason") == "force_technical_error":
        raise EventProcessingError("technical", "temporary_dependency_error", "Temporary dependency error")
    if event_id in processed_shift_status_events:
        logger.info(
            "Skipping duplicate shift status event",
            extra={"event_id": event_id, "correlation_id": event.get("correlation_id"), "shift_id": shift_id},
        )
        return
    report_cache_stale_by_shift[shift_id] = {
        "shift_id": shift_id,
        "stale": True,
        "event_id": event_id,
        "correlation_id": event.get("correlation_id"),
        "marked_stale_at": utcnow(),
    }
    processed_shift_status_events.add(event_id)
    logger.info(
        "Marked report cache as stale",
        extra={"event_id": event_id, "correlation_id": event.get("correlation_id"), "shift_id": shift_id},
    )


def handle_shift_status_changed(event: dict, metadata: dict | None = None) -> None:
    try:
        process_shift_status_changed_event(event)
    except EventProcessingError as exc:
        register_problem_event(event, exc, settings.service_name, metadata)


def start_shift_status_consumer() -> None:
    consume_events("operations.shift.status_changed", settings.kafka_shift_status_group_id, handle_shift_status_changed)


@app.on_event("startup")
def start_consumers() -> None:
    Thread(target=start_shift_status_consumer, daemon=True).start()


@app.get("/admin/problem-events")
def get_problem_events(
    x_operational_role: str | None = Header(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    error_code: str | None = None,
    error_type: str | None = None,
    shift_id: str | None = None,
    event_type: str | None = None,
) -> list[dict]:
    require_operations_role(x_operational_role)
    items = list_problem_events()
    if status_filter:
        items = [item for item in items if item.get("status") == status_filter]
    if error_code:
        items = [item for item in items if item.get("error_code") == error_code]
    if error_type:
        items = [item for item in items if item.get("error_type") == error_type]
    if shift_id:
        items = [item for item in items if item.get("shift_id") == shift_id]
    if event_type:
        items = [item for item in items if item.get("event_type") == event_type]
    return items


@app.get("/admin/problem-events/{problem_event_id}")
def get_problem_event_detail(problem_event_id: str, x_operational_role: str | None = Header(default=None) ) -> dict:
    require_operations_role(x_operational_role)
    item = get_problem_event(problem_event_id)
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return item


@app.post("/admin/problem-events/{problem_event_id}/reprocess")
def reprocess_problem_event_api(problem_event_id: str, payload: ManualProblemEventAction, x_operational_role: str | None = Header(default=None)) -> dict:
    require_operations_role(x_operational_role)
    if get_problem_event(problem_event_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return enqueue_manual_reprocess(problem_event_id, "operations", payload.comment)


@app.post("/admin/problem-events/{problem_event_id}/ignore")
def ignore_problem_event(problem_event_id: str, payload: ManualProblemEventAction, x_operational_role: str | None = Header(default=None)) -> dict:
    require_operations_role(x_operational_role)
    if get_problem_event(problem_event_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return mark_manual_status(problem_event_id, "ignored", "operations", payload.comment)


@app.post("/admin/problem-events/{problem_event_id}/manual-review")
def manual_review_problem_event(problem_event_id: str, payload: ManualProblemEventAction, x_operational_role: str | None = Header(default=None)) -> dict:
    require_operations_role(x_operational_role)
    if get_problem_event(problem_event_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return mark_manual_status(problem_event_id, "manual_review", "operations", payload.comment)


@app.post("/admin/problem-events/{problem_event_id}/dlq")
def dlq_problem_event(problem_event_id: str, payload: ManualProblemEventAction, x_operational_role: str | None = Header(default=None)) -> dict:
    require_operations_role(x_operational_role)
    if get_problem_event(problem_event_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return mark_manual_status(problem_event_id, "dlq", "operations", payload.comment)


def serialize_task(task: dict) -> dict:
    return {key: value for key, value in task.items() if key != "parameters"} | {"parameters": task["parameters"]}


def refresh_task_status(task: dict) -> dict:
    cleanup_expired_reports()
    if task["parameters"].get("reportType") == "shifts":
        return task
    if task["status"] in {"completed", "failed", "cancelled", "expired"}:
        return task

    elapsed = (utcnow() - task["createdAt"]).total_seconds()
    if elapsed < 5:
        task["status"] = "queued"
        task["progress"] = 0
    elif elapsed < 15:
        task["status"] = "running"
        task["progress"] = 65
        task["startedAt"] = task["startedAt"] or utcnow()
    else:
        report_id = task["reportId"] or f"rpt_{uuid4().hex[:12]}"
        task.update(
            {
                "status": "completed",
                "progress": 100,
                "reportId": report_id,
                "completedAt": utcnow(),
                "result": {
                    "fileName": f"{task['parameters']['reportType']}-{task['id']}.{task['parameters']['format']}",
                    "downloadUrl": f"/api/v1/report-tasks/{task['id']}/download",
                    "expiresAt": utcnow() + timedelta(hours=1),
                },
            }
        )
    task["updatedAt"] = utcnow()
    return task


@app.post("/api/v1/report-tasks", status_code=status.HTTP_202_ACCEPTED)
def start_report_generation(payload: ReportTaskCreate, response: Response) -> dict:
    starts_at_from, starts_at_to = validate_shifts_report_request(payload)
    task_id = f"tsk_{uuid4().hex[:12]}"
    now = utcnow()
    parameters = payload.model_dump(by_alias=True)
    if payload.report_type == "shifts":
        parameters["filter"]["startsAtFrom"] = utc_iso(starts_at_from)
        parameters["filter"]["startsAtTo"] = utc_iso(starts_at_to)
    task = {
        "id": task_id,
        "taskId": task_id,
        "status": "queued",
        "progress": 0,
        "statusUrl": f"/api/v1/report-tasks/{task_id}",
        "createdAt": now,
        "updatedAt": now,
        "startedAt": None,
        "completedAt": None,
        "expiresAt": now + timedelta(hours=24),
        "reportId": None,
        "parameters": parameters,
        "result": None,
        "error": None,
    }
    report_tasks[task_id] = task
    response.headers["Location"] = task["statusUrl"]
    response.headers["Retry-After"] = "5"
    event = {"task_id": task_id, "status": "queued", "request": task["parameters"], "created_at": now}
    publish_event("report.requested", event)
    logger.info("Created report task", extra={"task_id": task_id, "reportType": task["parameters"].get("reportType")})
    if payload.report_type == "shifts":
        Thread(target=process_report_task, args=(task_id,), daemon=True).start()
    return {"taskId": task_id, "status": "queued", "statusUrl": task["statusUrl"], "createdAt": now}


@app.get("/api/v1/report-tasks/{task_id}")
def get_report_task_status(task_id: str, response: Response) -> dict:
    task = report_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report task not found")
    refresh_task_status(task)
    if task["status"] in {"queued", "running"}:
        response.headers["Retry-After"] = "5"
    return serialize_task(task)


@app.get("/api/v1/report-tasks/{task_id}/download")
def download_report(task_id: str):
    task = report_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report task not found")
    refresh_task_status(task)
    if task["status"] == "expired":
        raise HTTPException(status_code=status.HTTP_410_GONE, detail="Report file expired")
    if task["status"] != "completed":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Report is not ready")
    if task["parameters"].get("reportType") == "shifts":
        file_path = Path(task["result"].get("filePath", ""))
        if not file_path.exists():
            task["status"] = "expired"
            task["updatedAt"] = utcnow()
            raise HTTPException(status_code=status.HTTP_410_GONE, detail="Report file expired")
        return FileResponse(file_path, media_type=XLSX_CONTENT_TYPE, filename=task["result"]["fileName"])
    return {"downloadUrl": f"https://storage.example.local/reports/{task['result']['fileName']}", "expiresAt": task["result"]["expiresAt"]}


@app.delete("/api/v1/report-tasks/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
def cancel_report_task(task_id: str) -> Response:
    task = report_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report task not found")
    refresh_task_status(task)
    if task["status"] == "completed":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Completed report task cannot be cancelled")
    task["status"] = "cancelled"
    task["updatedAt"] = utcnow()
    publish_event("report.cancelled", {"task_id": task_id, "cancelled_at": task["updatedAt"]})
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/reports", status_code=status.HTTP_202_ACCEPTED)
def request_report_legacy(payload: ReportTaskCreate, response: Response) -> dict:
    return start_report_generation(payload, response)


@app.get("/reports/{task_id}")
def get_report_legacy(task_id: str, response: Response) -> dict:
    return get_report_task_status(task_id, response)
