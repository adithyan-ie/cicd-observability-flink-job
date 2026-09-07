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
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class PostgresMetricSink extends RichSinkFunction<MetricResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresMetricSink.class);

    private static final String INSERT_SQL =
            "INSERT INTO cicd_metrics " +
            "(metric_type, pipeline_id, service_name, window_start_ms, window_end_ms, " +
            " value, performance_band, sample_count, detail, computed_at_ms, flink_received_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (metric_type, pipeline_id, window_start_ms, window_end_ms) DO UPDATE SET " +
            "  service_name         = EXCLUDED.service_name, " +
            "  value                = EXCLUDED.value, " +
            "  performance_band     = EXCLUDED.performance_band, " +
            "  sample_count         = EXCLUDED.sample_count, " +
            "  detail               = EXCLUDED.detail, " +
            "  computed_at_ms       = EXCLUDED.computed_at_ms, " +
            "  flink_received_at    = EXCLUDED.flink_received_at, " +
            "  inserted_at          = NOW()";

    private final String url;
    private final String user;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement statement;

    public PostgresMetricSink() {
        this(FlinkConfig.PG_URL, FlinkConfig.PG_USER, FlinkConfig.PG_PASSWORD);
    }

    public PostgresMetricSink(String url, String user, String password) {
        this.url      = url;
        this.user     = user;
        this.password = password;
    }

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

            if (metric.getFlinkReceivedAtMs() == 0) {
                statement.setNull(11, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                statement.setTimestamp(11, new Timestamp(metric.getFlinkReceivedAtMs()));
            }
            statement.executeUpdate();

            LOG.debug("Persisted metric: type={} pipeline={} value={}",
                    metric.getMetricType(), metric.getPipelineId(), metric.getValue());

        } catch (SQLException e) {
            LOG.error("Failed to insert metric: {}", metric, e);

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

    private static Long toEpochMs(String isoLocalDateTime) {
        if (isoLocalDateTime == null || isoLocalDateTime.isEmpty()) return null;
        try {
            return LocalDateTime.parse(isoLocalDateTime).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            return null;
        }
    }
}
