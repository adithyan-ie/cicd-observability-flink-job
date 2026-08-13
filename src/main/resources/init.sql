-- ── PostgreSQL schema for CI/CD Observability Flink results ─────────────────
-- Run this once before deploying the Flink job.

-- DORA metrics + health scores + late event metrics
CREATE TABLE IF NOT EXISTS cicd_metrics (
    id               BIGSERIAL        PRIMARY KEY,
    metric_type      VARCHAR(60)      NOT NULL,
    pipeline_id      VARCHAR(200)     NOT NULL,
    service_name     VARCHAR(200),
    window_start_ms  BIGINT,
    window_end_ms    BIGINT,
    value            DOUBLE PRECISION,
    performance_band VARCHAR(20),
    sample_count     BIGINT,
    detail           TEXT,
    computed_at_ms   BIGINT,
    -- Wall-clock time Flink deserialised the source event(s) for this row
    -- (CicdEvent.flinkReceivedAtMs) — NULL when unknown. Same type as
    -- inserted_at so latency is a direct interval subtraction:
    -- inserted_at - flink_received_at gives Flink's own processing
    -- latency (excludes Kafka consumer lag/backlog catch-up); see
    -- PostgresMetricSink.
    flink_received_at TIMESTAMPTZ,
    inserted_at      TIMESTAMPTZ      DEFAULT NOW()
);

-- Existing deployments created before this column existed (or with earlier
-- versions of it: BIGINT-epoch-ms, then the Kafka-CreateTime-based
-- source_generated_at).
ALTER TABLE cicd_metrics DROP COLUMN IF EXISTS source_generated_at_ms;
ALTER TABLE cicd_metrics DROP COLUMN IF EXISTS source_generated_at;
ALTER TABLE cicd_metrics ADD COLUMN IF NOT EXISTS flink_received_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_cicd_metrics_type_pipeline
    ON cicd_metrics (metric_type, pipeline_id);
CREATE INDEX IF NOT EXISTS idx_cicd_metrics_window
    ON cicd_metrics (window_start_ms, window_end_ms);
CREATE INDEX IF NOT EXISTS idx_cicd_metrics_service
    ON cicd_metrics (service_name);

-- One row per computed window: replays / job restarts recompute the same
-- (metric_type, pipeline_id, window) and should overwrite, not duplicate.
-- Late-event audit rows use their own metric_type per source metric (e.g.
-- DEPLOYMENT_FREQUENCY_LATE_EVENTS), so this key still holds without any
-- extra column — see LateEventOperator.
CREATE UNIQUE INDEX IF NOT EXISTS ux_cicd_metrics_window
    ON cicd_metrics (metric_type, pipeline_id, window_start_ms, window_end_ms);

-- CEP alerts + pattern timeouts + late event audit records
CREATE TABLE IF NOT EXISTS cicd_alerts (
    id          BIGSERIAL    PRIMARY KEY,
    alert_type  VARCHAR(60)  NOT NULL,
    payload     JSONB        NOT NULL,
    inserted_at TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cicd_alerts_type
    ON cicd_alerts (alert_type);
CREATE INDEX IF NOT EXISTS idx_cicd_alerts_payload
    ON cicd_alerts USING gin(payload);
CREATE INDEX IF NOT EXISTS idx_cicd_alerts_inserted
    ON cicd_alerts (inserted_at);

-- Grafana dashboard user (read-only)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_readonly';
    END IF;
END $$;
GRANT SELECT ON cicd_metrics TO grafana_reader;
GRANT SELECT ON cicd_alerts  TO grafana_reader;

-- Flink writer user (also used by Grafana's data source — see grafana-datasource.yaml)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'flink') THEN
        CREATE ROLE flink LOGIN PASSWORD 'admin';
    END IF;
END $$;
GRANT INSERT, UPDATE, SELECT ON cicd_metrics TO flink;
GRANT INSERT, SELECT ON cicd_alerts  TO flink;
GRANT USAGE, SELECT ON SEQUENCE cicd_metrics_id_seq TO flink;
GRANT USAGE, SELECT ON SEQUENCE cicd_alerts_id_seq  TO flink;
