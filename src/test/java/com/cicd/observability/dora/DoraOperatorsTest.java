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

/**
 * JUnit tests for DORA metric operators.
 *
 * Uses Flink's local StreamExecutionEnvironment — no cluster needed.
 * Events are created as bounded collections so the job terminates.
 */
public class DoraOperatorsTest {

    private StreamExecutionEnvironment env;

    private LocalDateTime base;

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // single-threaded for deterministic test output
        base = LocalDateTime.parse("2026-07-08T23:07:00");
    }

    // ── Helpers ────────────────────────────────────────────────────────

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

    // ══════════════════════════════════════════════════════════════════
    // Deployment Frequency
    // ══════════════════════════════════════════════════════════════════

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
        // 2 successful deploys in 1-day window → deploysPerDay = 2.0
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
        // DeploymentFrequencyOperator filters for DEPLOY_SUCCESS only
        // With no DEPLOY_SUCCESS events, no metric should be emitted
        DataStream<CicdEvent> stream = env.fromCollection(events);
        DeploymentFrequencyOperator
                .compute(stream, Time.days(1))
                .executeAndCollect()
                .forEachRemaining(results::add);

        // No DEPLOY_SUCCESS events → no deployment frequency metric
        assertTrue("No DEPLOY_SUCCESS events should produce no metric",
                results.isEmpty());
    }

    @Test
    public void testDeploymentFrequency_historyShowsDistinctCountPerWindow() throws Exception {
        // Window size 10s. 3 deploys land in the first window, 2 in the
        // next one (base+15s/+16s are guaranteed to be in a later window
        // since the offset exceeds the 10s window size).
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

    // ══════════════════════════════════════════════════════════════════
    // Live deployment counter (separate from the historical window above)
    // ══════════════════════════════════════════════════════════════════

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

        assertEquals(3, results.size());
        assertEquals(MetricResult.MetricType.DEPLOYMENT_FREQUENCY_LIVE, results.get(0).getMetricType());
        assertEquals(1L, results.get(0).getSampleCount());
        assertEquals(2L, results.get(1).getSampleCount());
        assertEquals(3L, results.get(2).getSampleCount());
    }

    @Test
    public void testLiveDeployCounter_resetsWhenWindowCloses() throws Exception {
        // Window size 10s. First two events land in the same window; the
        // third is 15s later — past the boundary — so the counter must
        // restart at 1 instead of continuing to 3.
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

        assertEquals(3, results.size());
        assertEquals(1L, results.get(0).getSampleCount());
        assertEquals(2L, results.get(1).getSampleCount());
        assertEquals("Counter should reset to 1 once the window closes, not continue to 3",
                1L, results.get(2).getSampleCount());
    }

    @Test
    public void testLiveDeployCounter_dropsLateEventsInsteadOfMiscountingIntoNewWindow() throws Exception {
        // Window size 10s. Event at +15s opens window 2 (count=1). A late
        // event at +5s (belongs to window 1, already closed) arrives next —
        // it must be dropped, not added to window 2's count as a 2nd deploy.
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

        // The late event (+5s) should have been silently dropped — only
        // the two window-2 events (+15s, +16s) produce output.
        assertEquals("Late event should be dropped, not emitted at all", 2, results.size());
        assertEquals(1L, results.get(0).getSampleCount());
        assertEquals("Late event must not be added to window 2's count",
                2L, results.get(1).getSampleCount());
    }

    // ══════════════════════════════════════════════════════════════════
    // Live Change Failure Rate (same pattern as the live deploy counter)
    // ══════════════════════════════════════════════════════════════════

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

        assertEquals(2, results.size());
        assertEquals(MetricResult.MetricType.CHANGE_FAILURE_RATE_LIVE, results.get(0).getMetricType());
        assertEquals("1 success, 0 failures so far = 0%", 0.0, results.get(0).getValue(), 0.0001);
        assertEquals("1 success, 1 failure so far = 50%", 50.0, results.get(1).getValue(), 0.0001);
        assertEquals(1L, results.get(0).getSampleCount());
        assertEquals(2L, results.get(1).getSampleCount());
    }

    @Test
    public void testLiveCfr_resetsWhenWindowCloses() throws Exception {
        // Window size 10s. First window: 1 failure out of 2 = 50%. Third
        // event is 15s later — past the boundary — so the tally must
        // restart (1 failure out of 1 = 100%), not accumulate to 2/3.
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

        assertEquals(3, results.size());
        assertEquals(50.0, results.get(1).getValue(), 0.0001);
        assertEquals("New window should restart at 1 failure / 1 total = 100%, not 2/3",
                100.0, results.get(2).getValue(), 0.0001);
        assertEquals(1L, results.get(2).getSampleCount());
    }

    @Test
    public void testLiveCfr_dropsLateEventsInsteadOfMiscountingIntoNewWindow() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-cfr-live-3", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(15)),
                event("pipe-cfr-live-3", "svc-a", "DEPLOY_FAILED",  "FAILURE", base.plusSeconds(5)), // late
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

        assertEquals("Late event should be dropped, not emitted at all", 2, results.size());
        assertEquals("Late failure must not be folded into window 2's rate",
                0.0, results.get(1).getValue(), 0.0001);
        assertEquals(2L, results.get(1).getSampleCount());
    }

    // ══════════════════════════════════════════════════════════════════
    // Lead Time for Changes
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testLeadTime_computedCorrectly() throws Exception {
        base = LocalDateTime.now();
        var deployTime     = base.plusMinutes(20); // 20 minutes later

        CicdEvent build = event("pipe-3", "svc-c", "BUILD_STARTED", "SUCCESS", base);
        build.setCommitSha("abc123");

        CicdEvent deploy = event("pipe-3", "svc-c", "DEPLOY_SUCCESS", "SUCCESS", deployTime);
        deploy.setCommitSha("abc123"); // same commit — should be matched

        List<MetricResult> results = new ArrayList<>();
        env.fromCollection(List.of(build, deploy))
           .keyBy(CicdEvent::getPipelineId);

        DoraOperators.leadTime(env.fromCollection(List.of(build, deploy)))
                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertFalse("Expected lead time metric", results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES, r.getMetricType());
        // 90 minutes lead time
        assertEquals(20.0, r.getValue(), 1.0);  // ±1 minute tolerance
    }

    @Test
    public void testLeadTime_noMatchForDifferentCommitSha() throws Exception {
        CicdEvent build  = event("pipe-4", "svc-d", "BUILD_STARTED",  "SUCCESS", base);
        build.setCommitSha("sha-A");
        CicdEvent deploy = event("pipe-4", "svc-d", "DEPLOY_SUCCESS", "SUCCESS", base.plusSeconds(5));
        deploy.setCommitSha("sha-B"); // different commit — no match

        List<MetricResult> results = new ArrayList<>();
        DoraOperators.leadTime(env.fromCollection(List.of(build, deploy)))
                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertTrue("Different commit SHA should produce no lead time metric",
                results.isEmpty());
    }

    // ══════════════════════════════════════════════════════════════════
    // Change Failure Rate
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testCfr_calculatesCorrectPercentage() throws Exception {
        // 1 success + 1 failure = 50% CFR
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
        assertEquals(50.0, r.getValue(), 0.01); // exactly 50%
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

    // ══════════════════════════════════════════════════════════════════
    // MTTR
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testMttr_computedInMinutes() throws Exception {
        var recoveryMs = base.plusMinutes(45); // 45 minutes

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
        assertEquals(45.0, r.getValue(), 1.0); // ±1 minute
        assertEquals("Elite", r.getPerformanceBand()); // < 60 minutes = Elite
    }

    @Test
    public void testMttr_noSuccessAfterFailure_emitsNoMetric() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-8", "svc-h", "BUILD_FAILED", "FAILURE", base)
                // No BUILD_SUCCESS follows
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

    // ══════════════════════════════════════════════════════════════════
    // Performance band classification
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testPerformanceBand_deployFreq() {
        MetricResult elite  = new MetricResult(MetricResult.MetricType.DEPLOYMENT_FREQUENCY,
                "p", "s", "0", "0", 2.0, 2);  // 2/day = Elite
        MetricResult low    = new MetricResult(MetricResult.MetricType.DEPLOYMENT_FREQUENCY,
                "p", "s", "0", "0", 0.01, 1); // < monthly = Low

        assertEquals("Elite", elite.getPerformanceBand());
        assertEquals("Low",   low.getPerformanceBand());
    }

    @Test
    public void testPerformanceBand_leadTime() {
        MetricResult elite = new MetricResult(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                "p", "s", "0", "0", 30.0, 1);    // 30 min < 60 = Elite
        MetricResult high  = new MetricResult(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                "p", "s", "0", "0", 200.0, 1);   // 200 min < 1440 = High
        MetricResult low   = new MetricResult(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                "p", "s", "0", "0", 20000.0, 1); // > 1 week = Low

        assertEquals("Elite", elite.getPerformanceBand());
        assertEquals("High",  high.getPerformanceBand());
        assertEquals("Low",   low.getPerformanceBand());
    }
}
