package com.cicd.observability.dora;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.dora.DeploymentFrequencyOperator;
import com.cicd.observability.operators.dora.DoraOperators;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
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

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // single-threaded for deterministic test output
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private CicdEvent event(String pipelineId, String serviceName,
                            String eventType, String status, long tsMs) {
        CicdEvent e = new CicdEvent();
        e.setPipelineId(pipelineId);
        e.setServiceName(serviceName);
        e.setEventType(eventType);
        e.setStatus(status);
        e.setCommitSha("sha-" + tsMs);
        e.setTimestampMs(tsMs);
        e.setEventTimestamp(Instant.ofEpochMilli(tsMs).toString());
        return e;
    }

    private long nowMs() { return System.currentTimeMillis(); }

    // ══════════════════════════════════════════════════════════════════
    // Deployment Frequency
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testDeploymentFrequency_countsSuccessfulDeployments() throws Exception {
        long base = nowMs();

        List<CicdEvent> events = List.of(
                event("pipe-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-1", "svc-a", "DEPLOY_SUCCESS", "SUCCESS", base + 1000),
                event("pipe-1", "svc-a", "DEPLOY_FAILED",  "FAILURE", base + 2000),
                event("pipe-1", "svc-a", "BUILD_STARTED",  "SUCCESS", base + 3000)
        );

        List<MetricResult> results = new ArrayList<>();

        DataStream<CicdEvent> stream = env.fromCollection(events);
        DeploymentFrequencyOperator
                .compute(stream, Time.days(1))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse("Expected at least one deployment frequency metric",
                results.isEmpty());

        MetricResult r = results.get(0);
        assertEquals(MetricResult.MetricType.DEPLOYMENT_FREQUENCY, r.getMetricType());
        assertEquals("pipe-1", r.getPipelineId());
        // 2 successful deploys in 1-day window → deploysPerDay = 2.0
        assertTrue("Deploy frequency should be > 0", r.getValue() > 0);
    }

    @Test
    public void testDeploymentFrequency_ignoresFailedDeployments() throws Exception {
        long base = nowMs();
        List<CicdEvent> events = List.of(
                event("pipe-2", "svc-b", "DEPLOY_FAILED", "FAILURE", base),
                event("pipe-2", "svc-b", "DEPLOY_FAILED", "FAILURE", base + 1000)
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

    // ══════════════════════════════════════════════════════════════════
    // Lead Time for Changes
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testLeadTime_computedCorrectly() throws Exception {
        long buildStartMs = nowMs();
        long deployMs     = buildStartMs + (90L * 60 * 1000); // 90 minutes later

        CicdEvent build = event("pipe-3", "svc-c", "BUILD_STARTED", "SUCCESS", buildStartMs);
        build.setCommitSha("abc123");

        CicdEvent deploy = event("pipe-3", "svc-c", "DEPLOY_SUCCESS", "SUCCESS", deployMs);
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
        assertEquals(90.0, r.getValue(), 1.0);  // ±1 minute tolerance
    }

    @Test
    public void testLeadTime_noMatchForDifferentCommitSha() throws Exception {
        long base = nowMs();
        CicdEvent build  = event("pipe-4", "svc-d", "BUILD_STARTED",  "SUCCESS", base);
        build.setCommitSha("sha-A");
        CicdEvent deploy = event("pipe-4", "svc-d", "DEPLOY_SUCCESS", "SUCCESS", base + 5000);
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
        long base = nowMs();
        // 1 success + 1 failure = 50% CFR
        List<CicdEvent> events = List.of(
                event("pipe-5", "svc-e", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-5", "svc-e", "DEPLOY_FAILED",  "FAILURE", base + 1000)
        );

        List<MetricResult> results = new ArrayList<>();
        DoraOperators.changeFailureRate(env.fromCollection(events), Time.days(7))
                     .executeAndCollect()
                     .forEachRemaining(results::add);

        assertFalse("Expected CFR metric", results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(MetricResult.MetricType.CHANGE_FAILURE_RATE, r.getMetricType());
        assertEquals(50.0, r.getValue(), 0.01); // exactly 50%
    }

    @Test
    public void testCfr_zeroFailures_givesZeroPercent() throws Exception {
        long base = nowMs();
        List<CicdEvent> events = List.of(
                event("pipe-6", "svc-f", "DEPLOY_SUCCESS", "SUCCESS", base),
                event("pipe-6", "svc-f", "DEPLOY_SUCCESS", "SUCCESS", base + 1000)
        );

        List<MetricResult> results = new ArrayList<>();
        DoraOperators.changeFailureRate(env.fromCollection(events), Time.days(7))
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
        long failureMs  = nowMs();
        long recoveryMs = failureMs + (45L * 60 * 1000); // 45 minutes

        List<CicdEvent> events = List.of(
                event("pipe-7", "svc-g", "BUILD_FAILED",  "FAILURE", failureMs),
                event("pipe-7", "svc-g", "BUILD_SUCCESS", "SUCCESS", recoveryMs)
        );

        List<MetricResult> results = new ArrayList<>();
        DoraOperators.mttr(env.fromCollection(events))
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
                event("pipe-8", "svc-h", "BUILD_FAILED", "FAILURE", nowMs())
                // No BUILD_SUCCESS follows
        );

        List<MetricResult> results = new ArrayList<>();
        DoraOperators.mttr(env.fromCollection(events))
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
                "p", "s", 0, 0, 2.0, 2);  // 2/day = Elite
        MetricResult low    = new MetricResult(MetricResult.MetricType.DEPLOYMENT_FREQUENCY,
                "p", "s", 0, 0, 0.01, 1); // < monthly = Low

        assertEquals("Elite", elite.getPerformanceBand());
        assertEquals("Low",   low.getPerformanceBand());
    }

    @Test
    public void testPerformanceBand_leadTime() {
        MetricResult elite = new MetricResult(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                "p", "s", 0, 0, 30.0, 1);    // 30 min < 60 = Elite
        MetricResult high  = new MetricResult(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                "p", "s", 0, 0, 200.0, 1);   // 200 min < 1440 = High
        MetricResult low   = new MetricResult(MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                "p", "s", 0, 0, 20000.0, 1); // > 1 week = Low

        assertEquals("Elite", elite.getPerformanceBand());
        assertEquals("High",  high.getPerformanceBand());
        assertEquals("Low",   low.getPerformanceBand());
    }
}
