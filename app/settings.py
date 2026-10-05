from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    service_name: str = "OMS3"
    root_path: str = ""
    kafka_bootstrap_servers: str = "kafka.oms.svc.cluster.local:9092"
    kafka_shift_status_group_id: str = "oms3.shift-status-cache"
    rabbitmq_url: str = "amqp://oms:oms@rabbitmq.oms.svc.cluster.local:5672/%2F"
    problem_events_retry_exchange: str = "problem-events.retry.exchange"
    problem_events_reprocess_exchange: str = "problem-events.reprocess.exchange"
    problem_events_retry_5m_queue: str = "problem-events.retry.5m"
    problem_events_retry_15m_queue: str = "problem-events.retry.15m"
    problem_events_retry_1h_queue: str = "problem-events.retry.1h"
    problem_events_reprocess_queue: str = "problem-events.reprocess"
    problem_events_manual_reprocess_queue: str = "problem-events.reprocess.manual"


settings = Settings()
