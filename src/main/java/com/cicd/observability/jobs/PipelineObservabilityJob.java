package com.cicd.observability.jobs;

import com.cicd.observability.config.FlinkConfig;
import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.cep.FailurePatternOperator;
import com.cicd.observability.operators.dora.DeploymentFrequencyOperator;
import com.cicd.observability.operators.dora.DoraOperators;
import com.cicd.observability.operators.health.PipelineHealthOperator;
// import com.cicd.observability.operators.late.LateEventOperator; // disabled — see [C] below
import com.cicd.observability.router.EventRouter;
import com.cicd.observability.sink.GrafanaSink;
import com.cicd.observability.sink.PostgresMetricSink;
import com.cicd.observability.sink.PostgresStringSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        DataStream<CicdEvent> cepStream    = routedStream.getSideOutput(EventRouter.CEP_TAG);

        // ════════════════════════════════════════════════════════════════
        // [A] DORA Metrics → Kafka + Postgres + Grafana
        // ════════════════════════════════════════════════════════════════

        SingleOutputStreamOperator<MetricResult> deployFreq =
                DeploymentFrequencyOperator.compute(doraStream, FlinkConfig.DEPLOYMENT_FREQUENCY_WINDOW);
        sinkMetric(deployFreq, FlinkConfig.TOPIC_METRICS, "dora-deploy-freq");

        // Deploy events beyond the allowed-lateness grace period → alert table only (no Kafka)
        DataStream<CicdEvent> deployTrulyLate =
                deployFreq.getSideOutput(DeploymentFrequencyOperator.TRULY_LATE_TAG);
        DeploymentFrequencyOperator.auditTrulyLate(deployTrulyLate)
                .name("format-truly-late-dora-deploy-freq")
                .addSink(new PostgresStringSink("LATE_EVENT"))
                .name("postgres-sink-truly-late-dora-deploy-freq");

        // Live counter for the real-time dashboard tile — separate from the
        // historical daily window above, so it updates instantly per deploy
        // without forcing the window to re-fire.
        DataStream<MetricResult> deployFreqLive =
                DeploymentFrequencyOperator.computeLive(doraStream, FlinkConfig.DEPLOYMENT_FREQUENCY_WINDOW);
        sinkMetric(deployFreqLive, FlinkConfig.TOPIC_METRICS, "dora-deploy-freq-live");

        DataStream<MetricResult> leadTime = DoraOperators.leadTime(doraStream);
        sinkMetric(leadTime, FlinkConfig.TOPIC_METRICS, "dora-lead-time");

        SingleOutputStreamOperator<MetricResult> cfr =
                DoraOperators.changeFailureRate(doraStream, FlinkConfig.CHANGE_FAILURE_RATE_WINDOW);
        sinkMetric(cfr, FlinkConfig.TOPIC_METRICS, "dora-cfr");

        DataStream<MetricResult> cfrLive =
                DoraOperators.changeFailureRateLive(doraStream, FlinkConfig.CHANGE_FAILURE_RATE_WINDOW);
        sinkMetric(cfrLive, FlinkConfig.TOPIC_METRICS, "dora-cfr-live");

        DataStream<MetricResult> mttr = DoraOperators.mttr(doraStream);
        sinkMetric(mttr, FlinkConfig.TOPIC_METRICS, "dora-mttr");

        // ════════════════════════════════════════════════════════════════
        // [B] Pipeline Health Score → Kafka + Postgres + Grafana
        // ════════════════════════════════════════════════════════════════

        SingleOutputStreamOperator<MetricResult> health =
                PipelineHealthOperator.compute(healthStream, FlinkConfig.PIPELINE_HEALTH_WINDOW);
        sinkMetric(health, FlinkConfig.TOPIC_HEALTH, "health-score");

        DataStream<MetricResult> healthLive =
                PipelineHealthOperator.computeLive(healthStream, FlinkConfig.PIPELINE_HEALTH_WINDOW);
        DataStream<MetricResult> healthLiveChanged =
                PipelineHealthOperator.filterChanged(healthLive);
        sinkMetric(healthLiveChanged, FlinkConfig.TOPIC_HEALTH, "health-score-live");

        // ════════════════════════════════════════════════════════════════
        // [C] Late Event Auditing — DISABLED for now.
        //
        // LateEventOperator re-bucketed deployTrulyLate into 10-minute
        // *processing-time* tumbling windows to feed the Grafana "Truly-Late
        // Events (count per window)" panel. Processing-time windows only
        // fire once 10 real wall-clock minutes elapse while the job keeps
        // running — they don't flush on stream completion the way
        // event-time windows do — so short-lived job runs/restarts left
        // this panel permanently on "No data" even though truly-late events
        // were happening. Keeping the per-event detail panel (fed directly
        // by DeploymentFrequencyOperator.auditTrulyLate() above, no
        // windowing involved) and the live/history metrics; re-enable this
        // once the processing-time window issue is addressed.
        //
        // DataStream<MetricResult> deployLateCounts =
        //         LateEventOperator.auditTrulyLate(
        //                 deployTrulyLate, MetricResult.MetricType.DEPLOYMENT_FREQUENCY_LATE_EVENTS);
        // sinkMetricNoKafka(deployLateCounts, "late-event-audit-count");

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

    // /** Same as sinkMetric() but skips Kafka — used for audit-only metric streams. */
    // private static void sinkMetricNoKafka(DataStream<MetricResult> stream, String name) {
    //     stream.addSink(new PostgresMetricSink())
    //           .name("postgres-sink-" + name);
    //     stream.addSink(new GrafanaSink())
    //           .name("grafana-sink-" + name);
    // }

    /** Build a thin MetricResult wrapper around a CEP JSON string for GrafanaSink. */
    private static MetricResult buildAlertMetric(String json,
                                                  MetricResult.MetricType type) {
        MetricResult r = new MetricResult();
        r.setMetricType(type);
        r.setDetail(json);
        return r;
    }
}
