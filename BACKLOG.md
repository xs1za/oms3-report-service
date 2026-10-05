# OMS3 Backlog

- Заменить in-memory статусы `ReportTask` на постоянное хранилище с миграциями.
- Добавить RabbitMQ-backed lightweight worker для команды `report.build` с очередями `oms3.report.build`, `oms3.report.build.retry`, `oms3.report.build.dlq`; Kafka оставить для событий жизненного цикла отчета.
- Сохранять результаты отчетов во внешнем файловом/object storage с TTL и контролем доступа к скачиванию.
- Реализовать идемпотентность запуска отчетов по `Idempotency-Key`.
- Добавить авторизацию через `OMS1`.
- Публиковать итоговые события `report.completed` и `report.failed` после выполнения worker job.
- Реализовать фактическое построение XLSX, CSV и PDF вместо прототипной ссылки на результат.
