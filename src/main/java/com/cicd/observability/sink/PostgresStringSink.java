package com.cicd.observability.sink;

import com.cicd.observability.config.FlinkConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresStringSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresStringSink.class);

    private static final String INSERT_SQL =
            "INSERT INTO cicd_alerts (alert_type, payload) " +
            "VALUES (?::VARCHAR, ?::JSONB)";

    private final String url;
    private final String user;
    private final String password;
    private final String alertType;

    private transient Connection       connection;
    private transient PreparedStatement statement;

    public PostgresStringSink(String alertType) {
        this(alertType, FlinkConfig.PG_URL, FlinkConfig.PG_USER, FlinkConfig.PG_PASSWORD);
    }

    public PostgresStringSink(String alertType, String url, String user, String password) {
        this.alertType = alertType;
        this.url       = url;
        this.user      = user;
        this.password  = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(url, user, password);
        connection.setAutoCommit(true);
        statement  = connection.prepareStatement(INSERT_SQL);
        LOG.info("PostgresStringSink[{}]: connection opened", alertType);
    }

    @Override
    public void invoke(String payload, Context ctx) throws Exception {
        if (payload == null || payload.isEmpty()) return;
        try {
            statement.setString(1, alertType);
            statement.setString(2, payload);
            statement.executeUpdate();
            LOG.debug("Persisted alert: type={}", alertType);
        } catch (SQLException e) {
            LOG.error("Failed to insert alert [{}]: {}", alertType, payload, e);
            throw new RuntimeException("PostgreSQL insert failed", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (statement  != null) statement.close();
        if (connection != null) connection.close();
        LOG.info("PostgresStringSink[{}]: connection closed", alertType);
    }
}
