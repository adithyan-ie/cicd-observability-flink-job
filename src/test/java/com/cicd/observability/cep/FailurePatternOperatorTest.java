package com.cicd.observability.cep;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.operators.cep.FailurePatternOperator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * JUnit tests for FailurePatternOperator's three Flink CEP deployment-failure
 * patterns:
 *
 *  1. Rollback cascade      — DEPLOY_FAILED -> DEPLOY_FAILED -> ROLLBACK_STARTED, within 10 min
 *  2. Deployment instability — DEPLOY_FAILED -> DEPLOY_STARTED -> DEPLOY_FAILED, within 10 min
 *  3. Build OK, deploy broken — BUILD_SUCCESS -> DEPLOY_FAILED -> DEPLOY_FAILED, within 15 min
 *
 * Uses Flink's local StreamExecutionEnvironment with a bounded, watermarked
 * source (same approach as DoraOperatorsTest) so .within() is evaluated
 * against real event-time gaps, not wall-clock execution time — otherwise
 * the "outside window" tests would trivially pass regardless of the
 * timestamps used, since executeAndCollect() runs in milliseconds.
 */
public class FailurePatternOperatorTest {

    private StreamExecutionEnvironment env;
    private LocalDateTime base;

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        base = LocalDateTime.parse("2026-07-08T23:07:00");
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private CicdEvent event(String pipelineId, String eventType, String status, LocalDateTime eventTime) {
        CicdEvent e = new CicdEvent();
        e.setPipelineId(pipelineId);
        e.setServiceName("cep-test-svc");
        e.setEventType(eventType);
        e.setStatus(status);
        e.setBranch("main");
        e.setCommitSha("abc123");
        e.setTimestampMs(eventTime.toInstant(ZoneOffset.UTC).toEpochMilli());
        e.setEventTimestamp(eventTime.toString());
        return e;
    }

    /** Bounded, event-time-watermarked, pipeline-keyed stream — what detect*() expects. */
    private DataStream<CicdEvent> keyedStream(List<CicdEvent> events) {
        return env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((e, ts) -> e.getTimestampMs()))
                .keyBy(CicdEvent::getPipelineId);
    }

    // ══════════════════════════════════════════════════════════════════
    // Pattern 1 — Rollback cascade
    //   DEPLOY_FAILED -> DEPLOY_FAILED -> ROLLBACK_STARTED, within 10 min
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testRollbackCascade_fullSequence_emitsAlert() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-rb-1", "DEPLOY_FAILED",     "FAILURE", base),
                event("pipe-rb-1", "DEPLOY_FAILED",     "FAILURE", base.plusMinutes(2)),
                event("pipe-rb-1", "ROLLBACK_STARTED",  "FAILURE", base.plusMinutes(4))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectRollbackCascade(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertFalse("Full DEPLOY_FAILED->DEPLOY_FAILED->ROLLBACK_STARTED should emit an alert",
                alerts.isEmpty());
        String alert = alerts.get(0);
        assertTrue(alert.contains("pipe-rb-1"));
        assertTrue(alert.contains("DEPLOY_ROLLBACK_CASCADE"));
        assertTrue(alert.contains("cascade_duration_minutes"));
    }

    @Test
    public void testRollbackCascade_missingRollback_noAlert() throws Exception {
        // Two failures but no rollback — should not produce a complete match.
        List<CicdEvent> events = List.of(
                event("pipe-rb-2", "DEPLOY_FAILED", "FAILURE", base),
                event("pipe-rb-2", "DEPLOY_FAILED", "FAILURE", base.plusMinutes(2))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectRollbackCascade(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertTrue("Missing rollback step should not emit a complete-match alert",
                alerts.isEmpty());
    }

    @Test
    public void testRollbackCascade_outsideWindow_noAlert() throws Exception {
        // Rollback arrives 12 minutes after the first failure — exceeds the 10-min window.
        List<CicdEvent> events = List.of(
                event("pipe-rb-3", "DEPLOY_FAILED",    "FAILURE", base),
                event("pipe-rb-3", "DEPLOY_FAILED",    "FAILURE", base.plusMinutes(2)),
                event("pipe-rb-3", "ROLLBACK_STARTED", "FAILURE", base.plusMinutes(12))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectRollbackCascade(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertTrue("Sequence spanning more than 10 minutes should not emit a complete-match alert",
                alerts.isEmpty());
    }

    @Test
    public void testRollbackCascade_withInterveningNoiseEvents_stillMatches() throws Exception {
        // .followedBy() uses relaxed contiguity — unrelated events between
        // the pattern's steps must not block the match.
        List<CicdEvent> events = List.of(
                event("pipe-rb-4", "DEPLOY_FAILED",    "FAILURE", base),
                event("pipe-rb-4", "BUILD_STARTED",    "SUCCESS", base.plusSeconds(30)), // noise
                event("pipe-rb-4", "DEPLOY_FAILED",    "FAILURE", base.plusMinutes(2)),
                event("pipe-rb-4", "BUILD_SUCCESS",    "SUCCESS", base.plusMinutes(3)),  // noise
                event("pipe-rb-4", "ROLLBACK_STARTED", "FAILURE", base.plusMinutes(4))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectRollbackCascade(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertFalse("Intervening noise events should not block relaxed-contiguity matching",
                alerts.isEmpty());
    }

    @Test
    public void testRollbackCascade_isolatedPerPipeline() throws Exception {
        List<CicdEvent> pipeA = List.of(
                event("pipe-rb-A", "DEPLOY_FAILED",    "FAILURE", base),
                event("pipe-rb-A", "DEPLOY_FAILED",    "FAILURE", base.plusMinutes(2)),
                event("pipe-rb-A", "ROLLBACK_STARTED", "FAILURE", base.plusMinutes(4))
        );
        List<CicdEvent> pipeB = List.of(
                event("pipe-rb-B", "DEPLOY_FAILED", "FAILURE", base)
        );

        List<CicdEvent> allEvents = new ArrayList<>(pipeA);
        allEvents.addAll(pipeB);

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectRollbackCascade(keyedStream(allEvents))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertEquals("Only pipe-rb-A should produce a complete match", 1, alerts.size());
        assertTrue(alerts.get(0).contains("pipe-rb-A"));
        assertFalse(alerts.get(0).contains("pipe-rb-B"));
    }

    @Test
    public void testRollbackCascade_partialMatch_firesTimeoutSideOutput() throws Exception {
        // Only the first failure — the window will expire with a partial match.
        List<CicdEvent> events = List.of(
                event("pipe-rb-5", "DEPLOY_FAILED", "FAILURE", base)
        );

        List<String> timeouts = new ArrayList<>();
        FailurePatternOperator.detectRollbackCascade(keyedStream(events))
                .getSideOutput(FailurePatternOperator.ROLLBACK_CASCADE_TIMEOUT_TAG)
                .executeAndCollect()
                .forEachRemaining(timeouts::add);

        assertFalse("A single failure with no follow-up should time out, not vanish",
                timeouts.isEmpty());
        assertTrue(timeouts.get(0).contains("DEPLOY_ROLLBACK_CASCADE_TIMEOUT"));
        assertTrue(timeouts.get(0).contains("pipe-rb-5"));
    }

    // ══════════════════════════════════════════════════════════════════
    // Pattern 2 — Deployment instability
    //   DEPLOY_FAILED -> DEPLOY_STARTED -> DEPLOY_FAILED, within 10 min
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testInstability_fullSequence_emitsAlert() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-in-1", "DEPLOY_FAILED",  "FAILURE", base),
                event("pipe-in-1", "DEPLOY_STARTED", "SUCCESS", base.plusMinutes(2)),
                event("pipe-in-1", "DEPLOY_FAILED",  "FAILURE", base.plusMinutes(4))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectDeploymentInstability(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertFalse("Full fail->retry->fail sequence should emit an alert", alerts.isEmpty());
        String alert = alerts.get(0);
        assertTrue(alert.contains("pipe-in-1"));
        assertTrue(alert.contains("DEPLOY_INSTABILITY"));
        assertTrue(alert.contains("\"failures\":2"));
    }

    @Test
    public void testInstability_noRetryBetweenFailures_noAlert() throws Exception {
        // Two DEPLOY_FAILED events back-to-back, no DEPLOY_STARTED retry between
        // them — this is the rollback-cascade shape, not deployment instability.
        List<CicdEvent> events = List.of(
                event("pipe-in-2", "DEPLOY_FAILED", "FAILURE", base),
                event("pipe-in-2", "DEPLOY_FAILED", "FAILURE", base.plusMinutes(2))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectDeploymentInstability(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertTrue("Two failures with no retry between them should not match the instability pattern",
                alerts.isEmpty());
    }

    @Test
    public void testInstability_outsideWindow_noAlert() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-in-3", "DEPLOY_FAILED",  "FAILURE", base),
                event("pipe-in-3", "DEPLOY_STARTED", "SUCCESS", base.plusMinutes(5)),
                event("pipe-in-3", "DEPLOY_FAILED",  "FAILURE", base.plusMinutes(11))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectDeploymentInstability(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertTrue("Sequence spanning more than 10 minutes should not emit a complete-match alert",
                alerts.isEmpty());
    }

    // ══════════════════════════════════════════════════════════════════
    // Pattern 3 — Build OK, deploy broken
    //   BUILD_SUCCESS -> DEPLOY_FAILED -> DEPLOY_FAILED, within 15 min
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testBuildBroken_fullSequence_emitsAlert() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-bb-1", "BUILD_SUCCESS", "SUCCESS", base),
                event("pipe-bb-1", "DEPLOY_FAILED",  "FAILURE", base.plusMinutes(3)),
                event("pipe-bb-1", "DEPLOY_FAILED",  "FAILURE", base.plusMinutes(6))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectBuildOkDeployBroken(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertFalse("BUILD_SUCCESS followed by two deploy failures should emit an alert",
                alerts.isEmpty());
        String alert = alerts.get(0);
        assertTrue(alert.contains("pipe-bb-1"));
        assertTrue(alert.contains("BUILD_OK_DEPLOY_BROKEN"));
        assertTrue(alert.contains("infrastructure"));
    }

    @Test
    public void testBuildBroken_buildFailedInstead_noAlert() throws Exception {
        // BUILD_FAILED, not BUILD_SUCCESS — the pattern's anchor never matches.
        List<CicdEvent> events = List.of(
                event("pipe-bb-2", "BUILD_FAILED",  "FAILURE", base),
                event("pipe-bb-2", "DEPLOY_FAILED", "FAILURE", base.plusMinutes(3)),
                event("pipe-bb-2", "DEPLOY_FAILED", "FAILURE", base.plusMinutes(6))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectBuildOkDeployBroken(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertTrue("A failed build should never anchor the build-ok-deploy-broken pattern",
                alerts.isEmpty());
    }

    @Test
    public void testBuildBroken_outsideWindow_noAlert() throws Exception {
        List<CicdEvent> events = List.of(
                event("pipe-bb-3", "BUILD_SUCCESS", "SUCCESS", base),
                event("pipe-bb-3", "DEPLOY_FAILED",  "FAILURE", base.plusMinutes(5)),
                event("pipe-bb-3", "DEPLOY_FAILED",  "FAILURE", base.plusMinutes(16))
        );

        List<String> alerts = new ArrayList<>();
        FailurePatternOperator.detectBuildOkDeployBroken(keyedStream(events))
                .executeAndCollect()
                .forEachRemaining(alerts::add);

        assertTrue("Sequence spanning more than 15 minutes should not emit a complete-match alert",
                alerts.isEmpty());
    }

    // ══════════════════════════════════════════════════════════════════
    // Pattern structure sanity checks
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testPatternStructures_areNotNull() {
        Pattern<CicdEvent, ?> rollback    = FailurePatternOperator.buildRollbackCascadePattern();
        Pattern<CicdEvent, ?> instability = FailurePatternOperator.buildDeploymentInstabilityPattern();
        Pattern<CicdEvent, ?> buildBroken = FailurePatternOperator.buildBuildOkDeployBrokenPattern();

        assertNotNull(rollback);
        assertNotNull(instability);
        assertNotNull(buildBroken);
        assertEquals("rollback", rollback.getName());
        assertEquals("second_failure", instability.getName());
        assertEquals("second_failure", buildBroken.getName());
    }
}
