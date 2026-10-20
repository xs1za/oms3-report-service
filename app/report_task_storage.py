import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.settings import settings


MIGRATION_PATH = Path(__file__).with_name("migrations") / "001_create_report_tasks.sql"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: Any) -> Any:
    if value is None:
        return None
    return Jsonb(json.loads(json.dumps(value, default=lambda item: item.astimezone(timezone.utc).isoformat() if isinstance(item, datetime) else str(item))))


def _task(row: dict[str, Any]) -> dict[str, Any]:
    result = row["result"]
    if result and result.get("expiresAt"):
        result = {**result, "expiresAt": datetime.fromisoformat(result["expiresAt"].replace("Z", "+00:00"))}
    return {
        "id": row["task_id"],
        "taskId": row["task_id"],
        "idempotencyKey": row["idempotency_key"],
        "status": row["status"],
        "progress": row["progress"],
        "parameters": row["parameters"],
        "reportId": row["report_id"],
        "result": result,
        "error": row["error"],
        "statusUrl": row["status_url"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "startedAt": row["started_at"],
        "completedAt": row["completed_at"],
        "expiresAt": row["expires_at"],
        "workerId": row["worker_id"],
        "leaseUntil": row["lease_until"],
    }


class ReportTaskStorage:
    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(settings.database_url, row_factory=dict_row) as connection:
            yield connection

    def migrate(self) -> None:
        with self.connection() as connection:
            connection.execute(MIGRATION_PATH.read_text(encoding="utf-8"))

    def check(self) -> None:
        with self.connection() as connection:
            connection.execute("SELECT 1")

    def create_or_get(
        self,
        task: dict[str, Any],
        idempotency_key: str | None,
    ) -> tuple[dict[str, Any], bool]:
        with self.connection() as connection:
            if idempotency_key:
                existing = connection.execute(
                    "SELECT * FROM report_tasks WHERE idempotency_key = %s",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    return _task(existing), False
            try:
                row = connection.execute(
                    """
                    INSERT INTO report_tasks (
                        task_id, idempotency_key, status, progress, parameters,
                        status_url, created_at, updated_at, expires_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        task["id"],
                        idempotency_key,
                        task["status"],
                        task["progress"],
                        _json(task["parameters"]),
                        task["statusUrl"],
                        task["createdAt"],
                        task["updatedAt"],
                        task["expiresAt"],
                    ),
                ).fetchone()
            except psycopg.errors.UniqueViolation:
                connection.rollback()
                row = connection.execute(
                    "SELECT * FROM report_tasks WHERE idempotency_key = %s",
                    (idempotency_key,),
                ).fetchone()
                if row is None:
                    raise
                return _task(row), False
            return _task(row), True

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute("SELECT * FROM report_tasks WHERE task_id = %s", (task_id,)).fetchone()
            return _task(row) if row else None

    def claim(self, task_id: str, worker_id: str, lease_until: datetime) -> dict[str, Any] | None:
        now = utcnow()
        with self.connection() as connection:
            row = connection.execute(
                """
                UPDATE report_tasks
                SET status = 'running',
                    worker_id = %s,
                    started_at = COALESCE(started_at, %s),
                    lease_until = %s,
                    updated_at = %s
                WHERE task_id = %s
                  AND (status = 'queued' OR (status = 'running' AND lease_until < %s))
                RETURNING *
                """,
                (worker_id, now, lease_until, now, task_id, now),
            ).fetchone()
            return _task(row) if row else None

    def update_running(self, task_id: str, worker_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        assignments = ["updated_at = %s"]
        parameters: list[Any] = [utcnow()]
        for field, value in values.items():
            column = {
                "status": "status",
                "progress": "progress",
                "reportId": "report_id",
                "result": "result",
                "error": "error",
                "completedAt": "completed_at",
                "leaseUntil": "lease_until",
            }.get(field)
            if column is None:
                continue
            assignments.append(f"{column} = %s")
            parameters.append(_json(value) if field in {"result", "error"} else value)
        parameters.extend([task_id, worker_id])
        with self.connection() as connection:
            row = connection.execute(
                f"UPDATE report_tasks SET {', '.join(assignments)} WHERE task_id = %s AND worker_id = %s AND status = 'running' AND (lease_until IS NULL OR lease_until >= %s) RETURNING *",
                [*parameters, utcnow()],
            ).fetchone()
            return _task(row) if row else None

    def cancel(self, task_id: str) -> dict[str, Any] | None:
        now = utcnow()
        with self.connection() as connection:
            row = connection.execute(
                """
                UPDATE report_tasks
                SET status = 'cancelled', updated_at = %s
                WHERE task_id = %s AND status IN ('queued', 'running')
                RETURNING *
                """,
                (now, task_id),
            ).fetchone()
            return _task(row) if row else None

    def update(self, task_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        assignments = ["updated_at = %s"]
        parameters: list[Any] = [utcnow()]
        columns = {
            "status": "status",
            "progress": "progress",
            "reportId": "report_id",
            "result": "result",
            "error": "error",
            "startedAt": "started_at",
            "completedAt": "completed_at",
            "expiresAt": "expires_at",
        }
        for field, value in values.items():
            column = columns.get(field)
            if column is None:
                continue
            assignments.append(f"{column} = %s")
            parameters.append(_json(value) if field in {"result", "error"} else value)
        parameters.append(task_id)
        with self.connection() as connection:
            row = connection.execute(
                f"UPDATE report_tasks SET {', '.join(assignments)} WHERE task_id = %s RETURNING *",
                parameters,
            ).fetchone()
            return _task(row) if row else None

    def expire(self, task_id: str, result: dict[str, Any] | None = None) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute(
                """
                UPDATE report_tasks
                SET status = 'expired', updated_at = %s, result = COALESCE(%s, result)
                WHERE task_id = %s AND status = 'completed'
                RETURNING *
                """,
                (utcnow(), _json(result), task_id),
            ).fetchone()
            return _task(row) if row else None

    def expired_results(self, now: datetime) -> list[dict[str, Any]]:
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM report_tasks WHERE status = 'completed' AND (result->>'expiresAt')::timestamptz <= %s",
                (now,),
            ).fetchall()
            return [_task(row) for row in rows]


report_task_storage = ReportTaskStorage()
