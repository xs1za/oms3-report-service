import logging
from datetime import datetime, timedelta, timezone
from threading import Thread
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

from app.healthcheck.router import router as healthcheck_router
from app.kafka import consume_events, publish_event
from app.problem_events import (
    EventProcessingError,
    enqueue_manual_reprocess,
    get_problem_event,
    list_problem_events,
    mark_manual_status,
    register_problem_event,
)
from app.settings import settings

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


class ReportTaskCreate(BaseModel):
    report_type: str = Field(alias="reportType", examples=["orders"])
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
        logger.info("Skipping duplicate shift status event", extra={"event_id": event_id, "shift_id": shift_id})
        return
    report_cache_stale_by_shift[shift_id] = {
        "shift_id": shift_id,
        "stale": True,
        "event_id": event_id,
        "correlation_id": event.get("correlation_id"),
        "marked_stale_at": utcnow(),
    }
    processed_shift_status_events.add(event_id)
    logger.info("Marked report cache as stale", extra={"event_id": event_id, "shift_id": shift_id})


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
    task_id = f"tsk_{uuid4().hex[:12]}"
    now = utcnow()
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
        "parameters": payload.model_dump(by_alias=True),
        "result": None,
        "error": None,
    }
    report_tasks[task_id] = task
    response.headers["Location"] = task["statusUrl"]
    response.headers["Retry-After"] = "5"
    event = {"task_id": task_id, "status": "queued", "request": task["parameters"], "created_at": now}
    publish_event("report.requested", event)
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
def download_report(task_id: str) -> dict:
    task = report_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report task not found")
    refresh_task_status(task)
    if task["status"] != "completed":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Report is not ready")
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
