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
 * ──────────────────────────────────────────────────────────────────
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
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
//            statement.setLong(4,    metric.getWindowStartMs());
//            statement.setLong(5,    metric.getWindowEndMs());
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
}
