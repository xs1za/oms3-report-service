import logging
import time

import pika

from app.main import process_shift_status_changed_event
from app.problem_events import create_connection, declare_problem_events_topology, process_reprocess_message
from app.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_message(channel: pika.channel.Channel, method, _properties: pika.BasicProperties, body: bytes) -> None:
    try:
        process_reprocess_message(body, process_shift_status_changed_event)
    except Exception:
        logger.exception("Failed to reprocess problem event")
    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    while True:
        try:
            with create_connection() as connection:
                channel = connection.channel()
                declare_problem_events_topology(channel)
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=settings.problem_events_reprocess_queue, on_message_callback=process_message)
                channel.basic_consume(queue=settings.problem_events_manual_reprocess_queue, on_message_callback=process_message)
                logger.info("OMS3 problem event worker consuming reprocess queues")
                channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logger.exception("RabbitMQ is unavailable; retrying problem event worker connection")
            time.sleep(5)


if __name__ == "__main__":
    main()
