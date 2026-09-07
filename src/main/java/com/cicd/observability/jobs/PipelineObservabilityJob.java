package com.cicd.observability.jobs;

import com.cicd.observability.config.FlinkConfig;
import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.cep.FailurePatternOperator;
import com.cicd.observability.operators.dora.DeploymentFrequencyOperator;
import com.cicd.observability.operators.dora.DoraOperators;
import com.cicd.observability.operators.health.PipelineHealthOperator;

import com.cicd.observability.operators.late.TrulyLateAuditOperator;
import com.cicd.observability.operators.watermark.WatermarkReporterOperator;
import com.cicd.observability.router.EventRouter;
import com.cicd.observability.sink.GrafanaSink;
import com.cicd.observability.sink.PostgresMetricSink;
import com.cicd.observability.sink.PostgresStringSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineObservabilityJob {

    private static final Logger LOG =
            LoggerFactory.getLogger(PipelineObservabilityJob.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkConfig.createEnvironment();

        KafkaSource<CicdEvent> source    = FlinkConfig.kafkaSource();
        WatermarkStrategy<CicdEvent> wms = FlinkConfig.watermarkStrategy();

        DataStream<CicdEvent> rawStream = env
                .fromSource(source, wms, "kafka-cicd-source")
                .filter(e -> e != null && e.getPipelineId() != null)
                .name("filter-nulls");

        SingleOutputStreamOperator<CicdEvent> routedStream = rawStream
                .process(new EventRouter())
                .name("event-router");

        DataStream<CicdEvent> doraStream   = routedStream.getSideOutput(EventRouter.DORA_TAG);
        DataStream<CicdEvent> healthStream = routedStream.getSideOutput(EventRouter.HEALTH_TAG);
        DataStream<CicdEvent> cepStream    = routedStream.getSideOutput(EventRouter.CEP_TAG);

        DataStream<MetricResult> watermarkReport = WatermarkReporterOperator.report(rawStream);
        sinkMetric(watermarkReport, "watermark");

        SingleOutputStreamOperator<MetricResult> deployFreq =
                DeploymentFrequencyOperator.compute(doraStream, FlinkConfig.DEPLOYMENT_FREQUENCY_WINDOW);
        sinkMetric(deployFreq, "dora-deploy-freq");

        DataStream<CicdEvent> deployTrulyLate =
                deployFreq.getSideOutput(DeploymentFrequencyOperator.TRULY_LATE_TAG);
        TrulyLateAuditOperator.auditTrulyLate(deployTrulyLate, "DEPLOYMENT_FREQUENCY")
                .addSink(new PostgresStringSink("LATE_EVENT"))
                .name("postgres-sink-truly-late-dora-deploy-freq")
                .setParallelism(4);

        DataStream<MetricResult> deployFreqLive =
                DeploymentFrequencyOperator.computeLive(doraStream, FlinkConfig.DEPLOYMENT_FREQUENCY_WINDOW);
        sinkMetric(deployFreqLive, "dora-deploy-freq-live");

        DataStream<MetricResult> leadTime = DoraOperators.leadTime(doraStream);
        sinkMetric(leadTime, "dora-lead-time");

        SingleOutputStreamOperator<MetricResult> cfr =
                DoraOperators.changeFailureRate(doraStream, FlinkConfig.CHANGE_FAILURE_RATE_WINDOW);
        sinkMetric(cfr, "dora-cfr");

        DataStream<CicdEvent> cfrTrulyLate =
                cfr.getSideOutput(DoraOperators.CFR_TRULY_LATE_TAG);
        TrulyLateAuditOperator.auditTrulyLate(cfrTrulyLate, "CHANGE_FAILURE_RATE")
                .addSink(new PostgresStringSink("LATE_EVENT"))
                .name("postgres-sink-truly-late-dora-cfr")
                .setParallelism(4);

        DataStream<MetricResult> cfrLive =
                DoraOperators.changeFailureRateLive(doraStream, FlinkConfig.CHANGE_FAILURE_RATE_WINDOW);
        sinkMetric(cfrLive, "dora-cfr-live");

        DataStream<MetricResult> mttr = DoraOperators.mttr(doraStream);
        sinkMetric(mttr, "dora-mttr");

        SingleOutputStreamOperator<MetricResult> health =
                PipelineHealthOperator.compute(healthStream, FlinkConfig.PIPELINE_HEALTH_WINDOW);
        sinkMetric(health, "health-score");

        DataStream<CicdEvent> healthTrulyLate =
                health.getSideOutput(PipelineHealthOperator.TRULY_LATE_TAG);
        TrulyLateAuditOperator.auditTrulyLate(healthTrulyLate, "PIPELINE_HEALTH_SCORE")
                .addSink(new PostgresStringSink("LATE_EVENT"))
                .name("postgres-sink-truly-late-health-score")
                .setParallelism(4);

        DataStream<MetricResult> healthLive =
                PipelineHealthOperator.computeLive(healthStream, FlinkConfig.PIPELINE_HEALTH_WINDOW);
        DataStream<MetricResult> healthLiveChanged =
                PipelineHealthOperator.filterChanged(healthLive);
        sinkMetric(healthLiveChanged, "health-score-live");

        DataStream<CicdEvent> keyedForCep =
                cepStream.keyBy(CicdEvent::getPipelineId);

        sinkCepPattern(
                FailurePatternOperator.detectRollbackCascade(keyedForCep),
                FailurePatternOperator.ROLLBACK_CASCADE_TIMEOUT_TAG,
                "DEPLOY_ROLLBACK_CASCADE");

        sinkCepPattern(
                FailurePatternOperator.detectDeploymentInstability(keyedForCep),
                FailurePatternOperator.DEPLOY_INSTABILITY_TIMEOUT_TAG,
                "DEPLOY_INSTABILITY");

        sinkCepPattern(
                FailurePatternOperator.detectBuildOkDeployBroken(keyedForCep),
                FailurePatternOperator.BUILD_OK_DEPLOY_BROKEN_TIMEOUT_TAG,
                "BUILD_OK_DEPLOY_BROKEN");

        env.execute("CI/CD Pipeline Observability — Postgres + Grafana");
    }

    private static void sinkMetric(DataStream<MetricResult> stream, String name) {

        stream.addSink(new PostgresMetricSink())
              .name("postgres-sink-" + name)
              .setParallelism(4);

        stream.addSink(new GrafanaSink())
              .name("grafana-sink-" + name)
              .setParallelism(4);
    }

    private static void sinkCepPattern(SingleOutputStreamOperator<String> alerts,
                                        OutputTag<String> timeoutTag,
                                        String alertTypeBase) {

        alerts.addSink(new PostgresStringSink(alertTypeBase))
              .name("postgres-sink-cep-" + alertTypeBase.toLowerCase())
              .setParallelism(4);
        alerts
              .map(json -> buildAlertMetric(json,
                      MetricResult.MetricType.FAILURE_PATTERN_DETECTED))
              .addSink(new GrafanaSink())
              .name("grafana-sink-cep-" + alertTypeBase.toLowerCase())
              .setParallelism(4);

        DataStream<String> timeouts = alerts.getSideOutput(timeoutTag);
        timeouts.addSink(new PostgresStringSink(alertTypeBase + "_TIMEOUT"))
                .name("postgres-sink-cep-" + alertTypeBase.toLowerCase() + "-timeout")
                .setParallelism(4);
        timeouts
              .map(json -> buildAlertMetric(json,
                      MetricResult.MetricType.PATTERN_TIMEOUT))
              .addSink(new GrafanaSink())
              .name("grafana-sink-cep-" + alertTypeBase.toLowerCase() + "-timeout")
              .setParallelism(4);
    }

    private static MetricResult buildAlertMetric(String json,
                                                  MetricResult.MetricType type) {
        MetricResult r = new MetricResult();
        r.setMetricType(type);
        r.setDetail(json);
        return r;
    }
}
