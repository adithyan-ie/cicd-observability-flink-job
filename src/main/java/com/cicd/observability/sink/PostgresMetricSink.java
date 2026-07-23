package com.cicd.observability.sink;

import com.cicd.observability.config.FlinkConfig;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * PostgreSQL sink for all four use case results.
 *
 * One RichSinkFunction handles every MetricType — it opens a single
 * JDBC connection per TaskManager slot (in open()) and closes it
 * in close().  Flink guarantees open()/close() lifecycle per slot.
 *
 * Table schema (run once):
 * ──────────────────────────────────────────────────────────────────
 *  CREATE TABLE cicd_metrics (
 *      id               BIGSERIAL PRIMARY KEY,
 *      metric_type      VARCHAR(60)   NOT NULL,
 *      pipeline_id      VARCHAR(200)  NOT NULL,
 *      service_name     VARCHAR(200),
 *      window_start_ms  BIGINT,
 *      window_end_ms    BIGINT,
 *      value            DOUBLE PRECISION,
 *      performance_band VARCHAR(20),
 *      sample_count     BIGINT,
 *      detail           TEXT,
 *      computed_at_ms   BIGINT,
 *      inserted_at      TIMESTAMPTZ   DEFAULT NOW()
 *  );
 *
 *  CREATE INDEX idx_cicd_metrics_type_pipeline
 *      ON cicd_metrics (metric_type, pipeline_id);
 *  CREATE INDEX idx_cicd_metrics_window
 *      ON cicd_metrics (window_start_ms, window_end_ms);
 *  CREATE UNIQUE INDEX ux_cicd_metrics_window
 *      ON cicd_metrics (metric_type, pipeline_id, window_start_ms, window_end_ms);
 *
 *  Late-event audit rows (see LateEventOperator) use their own specific
 *  metric_type per source metric (e.g. DEPLOYMENT_FREQUENCY_LATE_EVENTS),
 *  so metric_type alone keeps them from colliding with each other or with
 *  the real metric row on this index — no extra column needed.
 * ──────────────────────────────────────────────────────────────────
 * Recomputing the same window (e.g. after a job restart replays the Kafka
 * backlog) UPSERTs via ON CONFLICT instead of inserting a duplicate row.
 *
 * For the CEP / late-event string alerts, use PostgresStringSink below.
 */
public class PostgresMetricSink extends RichSinkFunction<MetricResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresMetricSink.class);

    private static final String INSERT_SQL =
            "INSERT INTO cicd_metrics " +
            "(metric_type, pipeline_id, service_name, window_start_ms, window_end_ms, " +
            " value, performance_band, sample_count, detail, computed_at_ms) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (metric_type, pipeline_id, window_start_ms, window_end_ms) DO UPDATE SET " +
            "  service_name     = EXCLUDED.service_name, " +
            "  value            = EXCLUDED.value, " +
            "  performance_band = EXCLUDED.performance_band, " +
            "  sample_count     = EXCLUDED.sample_count, " +
            "  detail           = EXCLUDED.detail, " +
            "  computed_at_ms   = EXCLUDED.computed_at_ms, " +
            "  inserted_at      = NOW()";

    private final String url;
    private final String user;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement statement;

    // ── Constructors ───────────────────────────────────────────────────

    /** Uses values from FlinkConfig (default). */
    public PostgresMetricSink() {
        this(FlinkConfig.PG_URL, FlinkConfig.PG_USER, FlinkConfig.PG_PASSWORD);
    }

    /** Explicit connection params (for testing with a different DB). */
    public PostgresMetricSink(String url, String user, String password) {
        this.url      = url;
        this.user     = user;
        this.password = password;
    }

    // ── Lifecycle ──────────────────────────────────────────────────────

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(url, user, password);
        connection.setAutoCommit(true);
        statement  = connection.prepareStatement(INSERT_SQL);
        LOG.info("PostgresMetricSink: connection opened to {}", url);
    }

    @Override
    public void invoke(MetricResult metric, Context ctx) throws Exception {
        if (metric == null) return;
        try {
            statement.setString(1,  metric.getMetricType() != null
                    ? metric.getMetricType().name() : "UNKNOWN");
            statement.setString(2,  nullSafe(metric.getPipelineId()));
            statement.setString(3,  nullSafe(metric.getServiceName()));
            statement.setObject(4,  toEpochMs(metric.getWindowStartMs()));
            statement.setObject(5,  toEpochMs(metric.getWindowEndMs()));
            statement.setDouble(6,  metric.getValue());
            statement.setString(7,  nullSafe(metric.getPerformanceBand()));
            statement.setLong(8,    metric.getSampleCount());
            statement.setString(9,  metric.getDetail());
            statement.setLong(10,   metric.getComputedAtMs());
            statement.executeUpdate();

            LOG.debug("Persisted metric: type={} pipeline={} value={}",
                    metric.getMetricType(), metric.getPipelineId(), metric.getValue());

        } catch (SQLException e) {
            LOG.error("Failed to insert metric: {}", metric, e);
            // Re-throw so Flink retries from last checkpoint
            throw new RuntimeException("PostgreSQL insert failed", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (statement  != null) statement.close();
        if (connection != null) connection.close();
        LOG.info("PostgresMetricSink: connection closed");
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }

    /** Parses the model's ISO LocalDateTime string (UTC, no zone suffix) back to epoch millis. */
    private static Long toEpochMs(String isoLocalDateTime) {
        if (isoLocalDateTime == null || isoLocalDateTime.isEmpty()) return null;
        try {
            return LocalDateTime.parse(isoLocalDateTime).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            return null;
        }
    }
}
