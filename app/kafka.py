import json
import logging
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from app.settings import settings

logger = logging.getLogger(__name__)


def publish_event(topic: str, payload: dict[str, Any]) -> None:
    try:
        producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        producer.produce(topic, json.dumps(payload, default=str).encode("utf-8"))
        producer.flush(2)
    except Exception:
        logger.exception("Failed to publish Kafka event to %s", topic)


def consume_events(topic: str, group_id: str, handler: Callable[[dict[str, Any]], None]) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    logger.info("Started Kafka consumer for %s", topic)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka consumer error: %s", message.error())
                continue
            try:
                payload = json.loads(message.value().decode("utf-8"))
                handler(payload)
                consumer.commit(message=message, asynchronous=False)
            except Exception:
                logger.exception("Rejected Kafka event from %s", topic)
                consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
