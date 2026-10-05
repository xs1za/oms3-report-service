from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    service_name: str = "OMS3"
    root_path: str = ""
    kafka_bootstrap_servers: str = "kafka.oms.svc.cluster.local:9092"
    kafka_shift_status_group_id: str = "oms3.shift-status-cache"


settings = Settings()
