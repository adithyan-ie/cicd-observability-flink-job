package com.cicd.observability.dora;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.dora.DeploymentFrequencyOperator;
import com.cicd.observability.operators.dora.DoraOperators;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DoraOperatorsTest {

    private StreamExecutionEnvironment env;

    private LocalDateTime base;

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        base = LocalDateTime.parse("2026-07-08T23:07:00");
    }

    private CicdEvent event(String pipelineId, String serviceName,
                            String eventType, String status, LocalDateTime eventTime) {
        CicdEvent e = new CicdEvent();
        e.setPipelineId(pipelineId);
        e.setServiceName(serviceName);
        e.setEventType(eventType);
        e.setStatus(status);
        e.setCommitSha("sha-" + eventTime);
        e.setTimestampMs(eventTime.toInstant(ZoneOffset.UTC).toEpochMilli());
        e.setEventTimestamp(eventTime.toString());
        return e;
    }

    private long nowMs() { return System.currentTimeMillis(); }

    @Test
    public void testDeploymentFrequency_countsSuccessfulDeployments() throws Exception {

        List<CicdEvent> events = List.of(
                event("pipe-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(1)),
                event("pipe-1", "svc-a", "DEPLOY_FAILED",  "FAILURE", base.plusSeconds(2)),
                event("pipe-1", "svc-b", "DEPLOY_SUCCESS",  "SUCCESS", base.plusSeconds(3)),
                event("pipe-1", "svc-b", "DEPLOY_SUCCESS",  "SUCCESS", base.plusSeconds(10)),
                event("pipe-1", "svc-a", "BUILD_STARTED",  "SUCCESS", base.plusSeconds(3))
        );

        List<MetricResult> results = new ArrayList<>();

        DataStream<CicdEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event, ts) -> event.getTimestampMs())
        );
        DeploymentFrequencyOperator
                .compute(stream, Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse("Expected at least one deployment frequency metric",
                results.isEmpty());
        assertEquals("no of windows created",2, results.size());
        MetricResult firstWindow = results.get(0);
        assertEquals(MetricResult.MetricType.DEPLOYMENT_FREQUENCY, firstWindow.getMetricType());
        assertEquals("pipe-1", firstWindow.getPipelineId());

        assertTrue("Deploy frequency should be > 0", firstWindow.getValue() > 0);
        assertTrue("Expected deployment frequency is", firstWindow.getValue() == 3.0);
    }

    @Test
    public void testDeploymentFrequency_ignoresFailedDeployments() throws Exception {
         base = LocalDateTime.now();
        List<CicdEvent> events = List.of(
                event("pipe-2", "svc-b", "DEPLOY_FAILED", "FAILURE", base),
                event("pipe-2", "svc-b", "DEPLOY_FAILED", "FAILURE", base.plusSeconds(2))
        );

        List<MetricResult> results = new ArrayList<>();
        env.fromCollection(events);

        DataStream<CicdEvent> stream = env.fromCollection(events);
        DeploymentFrequencyOperator
                .compute(stream, Time.days(1))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertTrue("No DEPLOY_SUCCESS events should produce no metric",
                results.isEmpty());
    }

    @Test
    public void testDeploymentFrequency_historyShowsDistinctCountPerWindow() throws Exception {

        List<CicdEvent> events = List.of(
                event("pipe-hist-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-hist-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(1)),
                event("pipe-hist-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(2)),
                event("pipe-hist-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(15)),
                event("pipe-hist-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(16))
        );

        List<MetricResult> results = new ArrayList<>();
        DataStream<CicdEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));

        DeploymentFrequencyOperator
                .compute(stream, Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        results.sort(java.util.Comparator.comparing(MetricResult::getWindowStartMs));
        assertEquals("Expected one history row per distinct window", 2, results.size());
        assertEquals("First window should count only its own 3 deploys",
                3.0, results.get(0).getValue(), 0.0001);
        assertEquals("Second window should count only its own 2 deploys, not 5",
                2.0, results.get(1).getValue(), 0.0001);
    }

    @Test
    public void testLiveDeployCounter_incrementsPerEventWithinWindow() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-live-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-live-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(1)),
                event("pipe-live-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(2))
        );

        List<MetricResult> results = new ArrayList<>();
        DataStream<CicdEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));

        DeploymentFrequencyOperator.computeLive(stream, Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertEquals(4, results.size());
        assertEquals(MetricResult.MetricType.DEPLOYMENT_FREQUENCY_LIVE, results.get(0).getMetricType());
        assertEquals(1L, results.get(0).getSampleCount());
        assertEquals(2L, results.get(1).getSampleCount());
        assertEquals(3L, results.get(2).getSampleCount());
        assertEquals("Trailing reset from end-of-stream window close",
                0L, results.get(3).getSampleCount());
    }

    @Test
    public void testLiveDeployCounter_resetsWhenWindowCloses() throws Exception {

        List<CicdEvent> events = List.of(
                event("pipe-live-2", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-live-2", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(1)),
                event("pipe-live-2", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(15))
        );

        List<MetricResult> results = new ArrayList<>();
        DataStream<CicdEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));

        DeploymentFrequencyOperator.computeLive(stream, Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertEquals(4, results.size());
        assertEquals(1L, results.get(0).getSampleCount());
        assertEquals(2L, results.get(1).getSampleCount());
        assertEquals("Counter should reset to 1 once the window closes, not continue to 3",
                1L, results.get(2).getSampleCount());
        assertEquals("Window 1's timer must not fire a stale reset after window 2 already opened",
                0L, results.get(3).getSampleCount());
    }

    @Test
    public void testLiveDeployCounter_dropsLateEventsInsteadOfMiscountingIntoNewWindow() throws Exception {

        List<CicdEvent> events = List.of(
                event("pipe-live-3", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(15)),
                event("pipe-live-3", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(5)),
                event("pipe-live-3", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(16))
        );

        List<MetricResult> results = new ArrayList<>();
        DataStream<CicdEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));

        DeploymentFrequencyOperator.computeLive(stream, Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        List<MetricResult> increments = results.stream().filter(r -> r.getSampleCount() > 0).toList();
        assertEquals("Late event should be dropped, not emitted at all", 2, increments.size());
        assertEquals(1L, increments.get(0).getSampleCount());
        assertEquals("Late event must not be added to window 2's count",
                2L, increments.get(1).getSampleCount());
        assertEquals(3, results.size());
    }

    @Test
    public void testLiveCfr_updatesRatePerEventWithinWindow() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-cfr-live-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-cfr-live-1", "svc-a", "DEPLOY_FAILED",  "FAILURE", base.plusSeconds(1))
        );

        List<MetricResult> results = new ArrayList<>();
        DataStream<CicdEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));

        DoraOperators.changeFailureRateLive(stream, Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        List<MetricResult> increments = results.stream().filter(r -> r.getSampleCount() > 0).toList();
        assertEquals(2, increments.size());
        assertEquals(MetricResult.MetricType.CHANGE_FAILURE_RATE_LIVE, increments.get(0).getMetricType());
        assertEquals("1 success, 0 failures so far = 0%", 0.0, increments.get(0).getValue(), 0.0001);
        assertEquals("1 success, 1 failure so far = 50%", 50.0, increments.get(1).getValue(), 0.0001);
        assertEquals(1L, increments.get(0).getSampleCount());
        assertEquals(2L, increments.get(1).getSampleCount());
        assertEquals(3, results.size());
    }

    @Test
    public void testLiveCfr_resetsWhenWindowCloses() throws Exception {

        List<CicdEvent> events = List.of(
                event("pipe-cfr-live-2", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-cfr-live-2", "svc-a", "DEPLOY_FAILED",  "FAILURE", base.plusSeconds(1)),
                event("pipe-cfr-live-2", "svc-a", "DEPLOY_FAILED",  "FAILURE", base.plusSeconds(15))
        );

        List<MetricResult> results = new ArrayList<>();
        DataStream<CicdEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));

        DoraOperators.changeFailureRateLive(stream, Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertEquals(4, results.size());
        assertEquals(50.0, results.get(1).getValue(), 0.0001);
        assertEquals("New window should restart at 1 failure / 1 total = 100%, not 2/3",
                100.0, results.get(2).getValue(), 0.0001);
        assertEquals(1L, results.get(2).getSampleCount());
        assertEquals("Window 1's timer must not fire a stale reset after window 2 already opened",
                0L, results.get(3).getSampleCount());
    }

    @Test
    public void testLiveCfr_dropsLateEventsInsteadOfMiscountingIntoNewWindow() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-cfr-live-3", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(15)),
                event("pipe-cfr-live-3", "svc-a", "DEPLOY_FAILED",  "FAILURE", base.plusSeconds(5)),
                event("pipe-cfr-live-3", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(16))
        );

        List<MetricResult> results = new ArrayList<>();
        DataStream<CicdEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));

        DoraOperators.changeFailureRateLive(stream, Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        List<MetricResult> increments = results.stream().filter(r -> r.getSampleCount() > 0).toList();
        assertEquals("Late event should be dropped, not emitted at all", 2, increments.size());
        assertEquals("Late failure must not be folded into window 2's rate",
                0.0, increments.get(1).getValue(), 0.0001);
        assertEquals(2L, increments.get(1).getSampleCount());
        assertEquals(3, results.size());
    }

    @Test
    public void testLeadTime_computedCorrectly() throws Exception {
        base = LocalDateTime.now();
        var deployTime     = base.plusMinutes(20);

        CicdEvent build = event("pipe-3", "svc-c", "BUILD_STARTED", "SUCCESS", base);
        build.setCommitSha("abc123");

        CicdEvent deploy = event("pipe-3", "svc-c", "DEPLOY_SUCCESS", "SUCCESS", deployTime);
        deploy.setCommitSha("abc123");

        List<MetricResult> results = new ArrayList<>();
        env.fromCollection(List.of(build, deploy))
           .keyBy(CicdEvent::getPipelineId);

        DoraOperators.leadTime(env.fromCollection(List.of(build, deploy)))
                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertFalse("Expected lead time metric", results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES, r.getMetricType());

        assertEquals(20.0, r.getValue(), 1.0);
    }

    @Test
    public void testLeadTime_noMatchForDifferentCommitSha() throws Exception {
        CicdEvent build  = event("pipe-4", "svc-d", "BUILD_STARTED",  "SUCCESS", base);
        build.setCommitSha("sha-A");
        CicdEvent deploy = event("pipe-4", "svc-d", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(5));
        deploy.setCommitSha("sha-B");

        List<MetricResult> results = new ArrayList<>();
        DoraOperators.leadTime(env.fromCollection(List.of(build, deploy)))
                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertTrue("Different commit SHA should produce no lead time metric",
                results.isEmpty());
    }

    @Test
    public void testLeadTime_failedDeployDoesNotLeakStateOrMatchLaterCommit() throws Exception {

        CicdEvent buildA  = event("pipe-6", "svc-f", "BUILD_STARTED", "SUCCESS", base);
        buildA.setCommitSha("sha-A");
        CicdEvent deployFailA = event("pipe-6", "svc-f", "DEPLOY_FAILED", "FAILURE", base.plusMinutes(5));
        deployFailA.setCommitSha("sha-A");

        CicdEvent buildB  = event("pipe-6", "svc-f", "BUILD_STARTED", "SUCCESS", base.plusMinutes(10));
        buildB.setCommitSha("sha-B");
        CicdEvent deploySuccessB = event("pipe-6", "svc-f", "DEPLOY_SUCCESS", "SUCCESS", base.plusMinutes(25));
        deploySuccessB.setCommitSha("sha-B");

        List<MetricResult> results = new ArrayList<>();
        DoraOperators.leadTime(env.fromCollection(
                        List.of(buildA, deployFailA, buildB, deploySuccessB)))
                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertEquals("Only commit B's successful deploy should produce a sample",
                1, results.size());
        assertEquals(15.0, results.get(0).getValue(), 1.0);
    }

    @Test
    public void testCfr_calculatesCorrectPercentage() throws Exception {

        List<CicdEvent> events = List.of(
                event("pipe-5", "svc-e", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-5", "svc-e", "DEPLOY_FAILED",  "FAILURE", base.plusMinutes(5))
        );

        List<MetricResult> results = new ArrayList<>();
      var dataStream =   env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));
        DoraOperators.changeFailureRate(dataStream, Time.days(7))

                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertFalse("Expected CFR metric", results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(MetricResult.MetricType.CHANGE_FAILURE_RATE, r.getMetricType());
        assertEquals(50.0, r.getValue(), 0.01);
    }

    @Test
    public void testCfr_zeroFailures_givesZeroPercent() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-6", "svc-f", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-6", "svc-f", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(10))
        );

        List<MetricResult> results = new ArrayList<>();
        var dataStream =   env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));
        DoraOperators.changeFailureRate(dataStream, Time.days(7))
                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        assertEquals(0.0, results.get(0).getValue(), 0.001);
        assertEquals("Elite", results.get(0).getPerformanceBand());
    }

    @Test
    public void testMttr_computedInMinutes() throws Exception {
        var recoveryMs = base.plusMinutes(45);

        List<CicdEvent> events = List.of(
                event("pipe-7", "svc-g", "BUILD_FAILED",  "FAILURE", base),
                event("pipe-7", "svc-g", "BUILD_SUCCESS", "SUCCESS", recoveryMs)
        );

        List<MetricResult> results = new ArrayList<>();

        var dataStream =   env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));

        DoraOperators.mttr(dataStream)
                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertFalse("Expected MTTR metric", results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(MetricResult.MetricType.MEAN_TIME_TO_RECOVERY, r.getMetricType());
        assertEquals(45.0, r.getValue(), 1.0);
        assertEquals("Elite", r.getPerformanceBand());
    }

    @Test
    public void testMttr_noSuccessAfterFailure_emitsNoMetric() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-8", "svc-h", "BUILD_FAILED", "FAILURE", base)

        );

        List<MetricResult> results = new ArrayList<>();
        var dataStream =   env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTimestampMs()));
        DoraOperators.mttr(dataStream)
                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertTrue("Failure without recovery should emit no MTTR metric",
                results.isEmpty());
    }

    @Test
    public void testPerformanceBand_deployFreq() {
        MetricResult elite  = new MetricResult(MetricResult.MetricType.DEPLOYMENT_FREQUENCY,
                "p", "s", "0", "0", 2.0, 2);
        MetricResult low    = new MetricResult(MetricResult.MetricType.DEPLOYMENT_FREQUENCY,
                "p", "s", "0", "0", 0.01, 1);

        assertEquals("Elite", elite.getPerformanceBand());
        assertEquals("Low",   low.getPerformanceBand());
    }

    @Test
    public void testPerformanceBand_leadTime() {
        MetricResult elite = new MetricResult(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                "p", "s", "0", "0", 30.0, 1);
        MetricResult high  = new MetricResult(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                "p", "s", "0", "0", 200.0, 1);
        MetricResult low   = new MetricResult(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                "p", "s", "0", "0", 20000.0, 1);

        assertEquals("Elite", elite.getPerformanceBand());
        assertEquals("High",  high.getPerformanceBand());
        assertEquals("Low",   low.getPerformanceBand());
    }
}
