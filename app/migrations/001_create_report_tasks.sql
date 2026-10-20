CREATE TABLE IF NOT EXISTS report_tasks (
    task_id varchar(64) PRIMARY KEY,
    idempotency_key varchar(255),
    status varchar(16) NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled', 'expired')),
    progress smallint NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
    parameters jsonb NOT NULL,
    report_id varchar(64),
    result jsonb,
    error jsonb,
    status_url text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    started_at timestamptz,
    completed_at timestamptz,
    expires_at timestamptz,
    worker_id varchar(255),
    lease_until timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS report_tasks_idempotency_key_idx
    ON report_tasks (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS report_tasks_status_idx ON report_tasks (status);
CREATE INDEX IF NOT EXISTS report_tasks_expires_at_idx ON report_tasks (expires_at);
CREATE INDEX IF NOT EXISTS report_tasks_lease_until_idx ON report_tasks (lease_until);
