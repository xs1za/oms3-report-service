import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pika

from app.settings import settings

logger = logging.getLogger(__name__)

RETRY_INTERVALS = [
    ("5m", timedelta(minutes=5), settings.problem_events_retry_5m_queue),
    ("15m", timedelta(minutes=15), settings.problem_events_retry_15m_queue),
    ("1h", timedelta(hours=1), settings.problem_events_retry_1h_queue),
]

problem_events: dict[str, dict[str, Any]] = {}
problem_events_by_event_id: dict[str, str] = {}


class EventProcessingError(Exception):
    def __init__(self, error_type: str, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.error_code = error_code
        self.message = message


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def create_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.URLParameters(settings.rabbitmq_url))


def declare_problem_events_topology(channel: pika.channel.Channel) -> None:
    channel.exchange_declare(exchange=settings.problem_events_retry_exchange, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=settings.problem_events_reprocess_exchange, exchange_type="direct", durable=True)
    channel.queue_declare(queue=settings.problem_events_reprocess_queue, durable=True)
    channel.queue_bind(
        queue=settings.problem_events_reprocess_queue,
        exchange=settings.problem_events_reprocess_exchange,
        routing_key="problem-events.reprocess",
    )
    channel.queue_declare(queue=settings.problem_events_manual_reprocess_queue, durable=True)
    channel.queue_bind(
        queue=settings.problem_events_manual_reprocess_queue,
        exchange=settings.problem_events_reprocess_exchange,
        routing_key="problem-events.reprocess.manual",
    )
    for interval, delay, queue_name in RETRY_INTERVALS:
        channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={
                "x-message-ttl": int(delay.total_seconds() * 1000),
                "x-dead-letter-exchange": settings.problem_events_reprocess_exchange,
                "x-dead-letter-routing-key": "problem-events.reprocess",
            },
        )
        channel.queue_bind(queue=queue_name, exchange=settings.problem_events_retry_exchange, routing_key=f"problem-events.retry.{interval}")


def publish_retry(problem_event: dict[str, Any]) -> None:
    if problem_event["attempt_count"] > len(RETRY_INTERVALS):
        return
    interval, _, _queue_name = RETRY_INTERVALS[problem_event["attempt_count"] - 1]
    with create_connection() as connection:
        channel = connection.channel()
        declare_problem_events_topology(channel)
        channel.basic_publish(
            exchange=settings.problem_events_retry_exchange,
            routing_key=f"problem-events.retry.{interval}",
            body=json.dumps({"problem_event_id": problem_event["id"]}).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )


def publish_manual_reprocess(problem_event_id: str) -> None:
    with create_connection() as connection:
        channel = connection.channel()
        declare_problem_events_topology(channel)
        channel.basic_publish(
            exchange=settings.problem_events_reprocess_exchange,
            routing_key="problem-events.reprocess.manual",
            body=json.dumps({"problem_event_id": problem_event_id}).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )


def _source_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    metadata = metadata or {}
    return {
        "source_topic": metadata.get("source_topic"),
        "source_partition": metadata.get("source_partition"),
        "source_offset": metadata.get("source_offset"),
        "kafka_key": metadata.get("kafka_key"),
    }


def register_problem_event(event: dict[str, Any], error: EventProcessingError, consumer_service: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    now = utcnow()
    event_id = event.get("event_id") or str(uuid4())
    problem_event_id = problem_events_by_event_id.get(event_id)
    if problem_event_id:
        item = problem_events[problem_event_id]
    else:
        problem_event_id = str(uuid4())
        item = {
            "id": problem_event_id,
            "event_id": event_id,
            "event_type": event.get("event_type"),
            "schema_version": event.get("schema_version"),
            "correlation_id": event.get("correlation_id"),
            "producer": event.get("producer"),
            **_source_metadata(metadata),
            "shift_id": event.get("shift_id"),
            "external_store_id": event.get("external_store_id"),
            "external_client_id": event.get("external_client_id"),
            "payload": event,
            "attempt_count": 0,
            "max_attempts": len(RETRY_INTERVALS),
            "first_failed_at": now,
            "resolved_at": None,
            "resolved_by": None,
            "last_manual_action_by": None,
            "last_manual_action_at": None,
            "last_manual_comment": None,
            "consumer_service": consumer_service,
        }
        problem_events[problem_event_id] = item
        problem_events_by_event_id[event_id] = problem_event_id

    item.update(
        {
            "error_type": error.error_type,
            "error_code": error.error_code,
            "error_message": error.message,
            "last_failed_at": now,
            "consumer_service": consumer_service,
        }
    )
    if error.error_type == "technical":
        item["attempt_count"] += 1
        if item["attempt_count"] <= len(RETRY_INTERVALS):
            interval, delay, _queue_name = RETRY_INTERVALS[item["attempt_count"] - 1]
            item.update({"status": "retry_scheduled", "last_retry_interval": interval, "next_retry_at": now + delay})
            publish_retry(item)
        else:
            item.update({"status": "manual_review", "next_retry_at": None, "last_retry_interval": RETRY_INTERVALS[-1][0]})
    elif error.error_type == "business":
        item.update({"status": "pending", "next_retry_at": None})
    else:
        item.update({"status": "dlq", "next_retry_at": None})
    logger.error(
        "Registered problem event",
        extra={
            "event_id": event_id,
            "problem_event_id": problem_event_id,
            "error_code": error.error_code,
            "consumer_service": consumer_service,
        },
    )
    return item


def list_problem_events() -> list[dict[str, Any]]:
    return list(problem_events.values())


def get_problem_event(problem_event_id: str) -> dict[str, Any] | None:
    return problem_events.get(problem_event_id)


def mark_manual_status(problem_event_id: str, status: str, user: str, comment: str) -> dict[str, Any]:
    item = problem_events[problem_event_id]
    item.update(
        {
            "status": status,
            "last_manual_action_by": user,
            "last_manual_action_at": utcnow(),
            "last_manual_comment": comment,
            "next_retry_at": None,
        }
    )
    return item


def enqueue_manual_reprocess(problem_event_id: str, user: str, comment: str | None = None) -> dict[str, Any]:
    item = problem_events[problem_event_id]
    item.update(
        {
            "status": "retry_scheduled",
            "last_manual_action_by": user,
            "last_manual_action_at": utcnow(),
            "last_manual_comment": comment,
        }
    )
    publish_manual_reprocess(problem_event_id)
    return item


def reprocess_problem_event(problem_event_id: str, handler) -> dict[str, Any]:
    item = problem_events[problem_event_id]
    if item["status"] in {"resolved", "ignored", "dlq"}:
        return item
    for other in problem_events.values():
        if other["id"] == item["id"] or other.get("shift_id") != item.get("shift_id"):
            continue
        if other.get("source_partition") != item.get("source_partition"):
            continue
        if other.get("source_offset") is None or item.get("source_offset") is None:
            continue
        if other["source_offset"] < item["source_offset"] and other.get("status") not in {"resolved", "ignored", "dlq"}:
            item.update({"status": "pending", "error_code": "earlier_shift_event_unresolved", "error_message": "Earlier shift event is not resolved"})
            return item
    item["status"] = "retrying"
    try:
        handler(item["payload"])
    except EventProcessingError as exc:
        return register_problem_event(item["payload"], exc, item["consumer_service"])
    item.update({"status": "resolved", "resolved_at": utcnow(), "resolved_by": "reprocessor", "next_retry_at": None})
    return item


def process_reprocess_message(body: bytes, handler) -> dict[str, Any]:
    message = json.loads(body.decode("utf-8"))
    return reprocess_problem_event(message["problem_event_id"], handler)
