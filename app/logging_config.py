import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
EXTRA_FIELDS = (
    "event_id",
    "correlation_id",
    "shift_id",
    "problem_event_id",
    "error_code",
    "consumer_service",
)


class ExtraFieldsFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        fields = [f"{field}={getattr(record, field)}" for field in EXTRA_FIELDS if hasattr(record, field)]
        if fields:
            return f"{message} {' '.join(fields)}"
        return message


def configure_logging(level: int = logging.INFO) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    formatter = ExtraFieldsFormatter(LOG_FORMAT)
    managed_handlers = [handler for handler in root_logger.handlers if getattr(handler, "_oms3_configured", False)]
    if managed_handlers:
        handler = managed_handlers[0]
        for duplicate in managed_handlers[1:]:
            root_logger.removeHandler(duplicate)
    else:
        handler = logging.StreamHandler(sys.stdout)
        handler._oms3_configured = True
        root_logger.addHandler(handler)

    handler.setLevel(level)
    handler.setFormatter(formatter)

    for logger_name in ("app", "uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(logger_name).setLevel(level)
