package com.cicd.observability.jobs;

import com.cicd.observability.config.FlinkConfig;
import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.cep.FailurePatternOperator;
import com.cicd.observability.operators.dora.DeploymentFrequencyOperator;
import com.cicd.observability.operators.dora.DoraOperators;
import com.cicd.observability.operators.health.PipelineHealthOperator;
import com.cicd.observability.operators.late.LateEventOperator;
import com.cicd.observability.router.EventRouter;
import com.cicd.observability.sink.GrafanaSink;
import com.cicd.observability.sink.PostgresMetricSink;
import com.cicd.observability.sink.PostgresStringSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Flink job — all four use cases with conditional routing.
 *
 * Each result stream is sunk to THREE destinations:
 *   1. Kafka       — for downstream consumers and replay
 *   2. PostgreSQL  — for long-term storage and Grafana data source
 *   3. Grafana     — real-time annotations for significant events
 *
 * RocksDB state backend is configured in FlinkConfig.createEnvironment().
 */
public class PipelineObservabilityJob {

    private static final Logger LOG =
            LoggerFactory.getLogger(PipelineObservabilityJob.class);

    public static void main(String[] args) throws Exception {

        // ── 1. Flink environment (RocksDB + checkpointing enabled) ───────────
        StreamExecutionEnvironment env = FlinkConfig.createEnvironment();

        // ── 2. Kafka source + watermark ───────────────────────────────────────
        KafkaSource<CicdEvent> source    = FlinkConfig.kafkaSource();
        WatermarkStrategy<CicdEvent> wms = FlinkConfig.watermarkStrategy();

        DataStream<CicdEvent> rawStream = env
                .fromSource(source, wms, "kafka-cicd-source")
                .filter(e -> e != null && e.getPipelineId() != null)
                .name("filter-nulls");

        // ── 3. EventRouter — conditional use-case routing ─────────────────────
        SingleOutputStreamOperator<CicdEvent> routedStream = rawStream
                .process(new EventRouter())
                .name("event-router");

        DataStream<CicdEvent> doraStream   = routedStream.getSideOutput(EventRouter.DORA_TAG);
        DataStream<CicdEvent> healthStream = routedStream.getSideOutput(EventRouter.HEALTH_TAG);
        DataStream<CicdEvent> lateStream   = routedStream.getSideOutput(EventRouter.LATE_TAG);
        DataStream<CicdEvent> cepStream    = routedStream.getSideOutput(EventRouter.CEP_TAG);

        // ════════════════════════════════════════════════════════════════
        // [A] DORA Metrics → Kafka + Postgres + Grafana
        // ════════════════════════════════════════════════════════════════

        DataStream<MetricResult> deployFreq =
                DeploymentFrequencyOperator.compute(doraStream, Time.days(1));
        sinkMetric(deployFreq, FlinkConfig.TOPIC_METRICS, "dora-deploy-freq");

        DataStream<MetricResult> leadTime = DoraOperators.leadTime(doraStream);
        sinkMetric(leadTime, FlinkConfig.TOPIC_METRICS, "dora-lead-time");

        DataStream<MetricResult> cfr =
                DoraOperators.changeFailureRate(doraStream, Time.days(7));
        sinkMetric(cfr, FlinkConfig.TOPIC_METRICS, "dora-cfr");

        DataStream<MetricResult> mttr = DoraOperators.mttr(doraStream);
        sinkMetric(mttr, FlinkConfig.TOPIC_METRICS, "dora-mttr");

        // ════════════════════════════════════════════════════════════════
        // [B] Pipeline Health Score → Kafka + Postgres + Grafana
        // ════════════════════════════════════════════════════════════════

        DataStream<MetricResult> health =
                PipelineHealthOperator.compute(healthStream);
        sinkMetric(health, FlinkConfig.TOPIC_HEALTH, "health-score");

        // ════════════════════════════════════════════════════════════════
        // [C] Late Event Processing → Kafka + Postgres + Grafana
        // ════════════════════════════════════════════════════════════════

        SingleOutputStreamOperator<MetricResult> lateOp =
                LateEventOperator.compute(lateStream);
        sinkMetric(lateOp, FlinkConfig.TOPIC_METRICS, "late-event-metrics");

        // Truly late events side output → Kafka + Postgres audit
        DataStream<CicdEvent> trulyLate =
                lateOp.getSideOutput(LateEventOperator.LATE_TAG);
        trulyLate
                .map(e -> String.format(
                        "{\"pipeline_id\":\"%s\",\"event_type\":\"%s\","
                        + "\"ts\":\"%s\",\"late\":true}",
                        e.getPipelineId(), e.getEventType(), e.getEventTimestamp()))
                .name("format-late-events")
                .sinkTo(FlinkConfig.stringSink(FlinkConfig.TOPIC_LATE_EVENTS))
                .name("kafka-sink-late-events");

        trulyLate
                .map(e -> String.format(
                        "{\"pipeline_id\":\"%s\",\"event_type\":\"%s\","
                        + "\"ts\":\"%s\",\"late\":true}",
                        e.getPipelineId(), e.getEventType(), e.getEventTimestamp()))
                .name("format-late-events-pg")
                .addSink(new PostgresStringSink("LATE_EVENT"))
                .name("postgres-sink-late-events");

        // ════════════════════════════════════════════════════════════════
        // [D] CEP Failure Pattern → Kafka + Postgres + Grafana
        // ════════════════════════════════════════════════════════════════

        DataStream<CicdEvent> keyedForCep =
                cepStream.keyBy(CicdEvent::getPipelineId);

        SingleOutputStreamOperator<String> cepAlerts =
                FailurePatternOperator.detect(keyedForCep);

        // Full match alerts
        cepAlerts.sinkTo(FlinkConfig.stringSink(FlinkConfig.TOPIC_FAILURE_ALERTS))
                 .name("kafka-sink-cep-alerts");
        cepAlerts.addSink(new PostgresStringSink("FAILURE_PATTERN"))
                 .name("postgres-sink-cep-alerts");
        // Grafana annotation for each pattern detection
        cepAlerts
                .map(json -> buildAlertMetric(json,
                        MetricResult.MetricType.FAILURE_PATTERN_DETECTED))
                .addSink(new GrafanaSink())
                .name("grafana-sink-cep-alerts");

        // Timeout alerts (partial patterns)
        DataStream<String> timeouts =
                cepAlerts.getSideOutput(FailurePatternOperator.TIMEOUT_TAG);
        timeouts.sinkTo(FlinkConfig.stringSink(FlinkConfig.TOPIC_PATTERN_TIMEOUT))
                .name("kafka-sink-cep-timeouts");
        timeouts.addSink(new PostgresStringSink("PATTERN_TIMEOUT"))
                .name("postgres-sink-cep-timeouts");

        // ── Execute ───────────────────────────────────────────────────────────
        env.execute("CI/CD Pipeline Observability — Kafka + Postgres + Grafana");
    }

    // ── Helper: wire one MetricResult stream to all three sinks ──────────────

    private static void sinkMetric(DataStream<MetricResult> stream,
                                   String kafkaTopic, String name) {
        // 1. Kafka
        stream.sinkTo(FlinkConfig.metricSink(kafkaTopic))
              .name("kafka-sink-" + name);

        // 2. Postgres
        stream.addSink(new PostgresMetricSink())
              .name("postgres-sink-" + name);

        // 3. Grafana annotations (only for significant events — filtered inside GrafanaSink)
        stream.addSink(new GrafanaSink())
              .name("grafana-sink-" + name);
    }

    /** Build a thin MetricResult wrapper around a CEP JSON string for GrafanaSink. */
    private static MetricResult buildAlertMetric(String json,
                                                  MetricResult.MetricType type) {
        MetricResult r = new MetricResult();
        r.setMetricType(type);
        r.setDetail(json);
        return r;
    }
}
