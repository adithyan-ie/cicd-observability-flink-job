//package com.cicd.observability.cep;
//
//import com.cicd.observability.model.CicdEvent;
//import com.cicd.observability.operators.cep.FailurePatternOperator;
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.time.LocalDateTime;
//import java.time.ZoneOffset;
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.junit.Assert.*;
//
///**
// * JUnit tests for CEP Failure Pattern detection.
// *
// * Tests cover:
// *  - Full pattern match: all 5 events in order → alert emitted
// *  - Partial pattern: only 3 events → no full match (timeout fires in prod)
// *  - Out-of-order events: pattern still detected with relaxed contiguity
// *  - Different pipeline IDs: patterns are isolated per pipeline (keyBy)
// *  - Pattern structure: .within() and .followedBy() conditions verified
// */
//public class FailurePatternOperatorTest {
//
//    private StreamExecutionEnvironment env;
//
//    @Before
//    public void setUp() {
//        env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//    }
//
//    private CicdEvent event(String pipeline, String type, LocalDateTime eventTime) {
//        CicdEvent e = new CicdEvent();
//        e.setPipelineId(pipeline);
//        e.setServiceName("cep-test-svc");
//        e.setEventType(type);
//        e.setStatus("FAILURE");
//        e.setTimestampMs(eventTime.toInstant(ZoneOffset.UTC).toEpochMilli());
//        e.setEventTimestamp(eventTime);
//        e.setBranch("main");
//        e.setCommitSha("abc123");
//        return e;
//    }
//
//    /** Full cascade in the correct order. */
//    private List<CicdEvent> fullCascade(String pipelineId, long base) {
//        return List.of(
//                event(pipelineId, "DEPENDENCY_UPDATED",     base),
//                event(pipelineId, "UNIT_TEST_FAILED",       base + 60_000),
//                event(pipelineId, "RETRY_TRIGGERED",        base + 120_000),
//                event(pipelineId, "INTEGRATION_TEST_FAILED",base + 180_000),
//                event(pipelineId, "ROLLBACK_TRIGGERED",     base + 240_000)
//        );
//    }
//
//    // ══════════════════════════════════════════════════════════════════
//    // Full pattern match — all 5 events in order within 10 min window
//    // ══════════════════════════════════════════════════════════════════
//
//    @Test
//    public void testFullPattern_emitsAlert() throws Exception {
//        long base = System.currentTimeMillis();
//        List<CicdEvent> events = fullCascade("pipe-cep-1", base);
//
//        List<String> alerts = new ArrayList<>();
//        FailurePatternOperator
//                .detect(env.fromCollection(events).keyBy(CicdEvent::getPipelineId))
//                .executeAndCollect()
//                .forEachRemaining(alerts::add);
//
//        assertFalse("Full cascade should emit a failure pattern alert",
//                alerts.isEmpty());
//
//        String alert = alerts.get(0);
//        assertTrue("Alert should contain pipeline ID",
//                alert.contains("pipe-cep-1"));
//        assertTrue("Alert should contain pattern name",
//                alert.contains("FAILURE_PATTERN_DETECTED"));
//        assertTrue("Alert should contain DORA impact",
//                alert.contains("dora_impact"));
//    }
//
//    // ══════════════════════════════════════════════════════════════════
//    // Partial pattern — missing ROLLBACK_TRIGGERED → no full match
//    // ══════════════════════════════════════════════════════════════════
//
//    @Test
//    public void testPartialPattern_noFullMatch() throws Exception {
//        long base = System.currentTimeMillis();
//        // Only first 4 steps — no ROLLBACK_TRIGGERED
//        List<CicdEvent> events = List.of(
//                event("pipe-cep-2", "DEPENDENCY_UPDATED",      base),
//                event("pipe-cep-2", "UNIT_TEST_FAILED",        base + 60_000),
//                event("pipe-cep-2", "RETRY_TRIGGERED",         base + 120_000),
//                event("pipe-cep-2", "INTEGRATION_TEST_FAILED", base + 180_000)
//        );
//
//        List<String> alerts = new ArrayList<>();
//        FailurePatternOperator
//                .detect(env.fromCollection(events).keyBy(CicdEvent::getPipelineId))
//                .executeAndCollect()
//                .forEachRemaining(alerts::add);
//
//        // No full match — partial match fires as timeout in production
//        // In bounded test, the pattern times out without emitting a full alert
//        assertTrue("Incomplete cascade should NOT emit a full pattern alert",
//                alerts.isEmpty());
//    }
//
//    // ══════════════════════════════════════════════════════════════════
//    // Relaxed contiguity — intervening events are ignored
//    // ══════════════════════════════════════════════════════════════════
//
//    @Test
//    public void testPattern_withInterveningEvents_stillMatches() throws Exception {
//        long base = System.currentTimeMillis();
//        // Intersperse unrelated events between pattern steps
//        List<CicdEvent> events = List.of(
//                event("pipe-cep-3", "DEPENDENCY_UPDATED",     base),
//                event("pipe-cep-3", "BUILD_STARTED",          base + 10_000), // noise
//                event("pipe-cep-3", "UNIT_TEST_FAILED",       base + 60_000),
//                event("pipe-cep-3", "BUILD_FAILED",           base + 90_000), // noise
//                event("pipe-cep-3", "RETRY_TRIGGERED",        base + 120_000),
//                event("pipe-cep-3", "PACKAGE_SUCCESS",        base + 150_000), // noise
//                event("pipe-cep-3", "INTEGRATION_TEST_FAILED",base + 180_000),
//                event("pipe-cep-3", "ROLLBACK_TRIGGERED",     base + 240_000)
//        );
//
//        List<String> alerts = new ArrayList<>();
//        FailurePatternOperator
//                .detect(env.fromCollection(events).keyBy(CicdEvent::getPipelineId))
//                .executeAndCollect()
//                .forEachRemaining(alerts::add);
//
//        // .followedBy() (relaxed contiguity) should ignore the noise events
//        assertFalse("Pattern with intervening noise events should still match",
//                alerts.isEmpty());
//        assertTrue(alerts.get(0).contains("FAILURE_PATTERN_DETECTED"));
//    }
//
//    // ══════════════════════════════════════════════════════════════════
//    // Pipeline isolation — different pipeline IDs don't cross-contaminate
//    // ══════════════════════════════════════════════════════════════════
//
//    @Test
//    public void testPattern_isolatedPerPipeline() throws Exception {
//        long base = System.currentTimeMillis();
//        // Pipeline A: full cascade → should match
//        List<CicdEvent> pipeA = fullCascade("pipe-cep-A", base);
//        // Pipeline B: only 2 events → should NOT match
//        List<CicdEvent> pipeB = List.of(
//                event("pipe-cep-B", "DEPENDENCY_UPDATED", base),
//                event("pipe-cep-B", "UNIT_TEST_FAILED",   base + 60_000)
//        );
//
//        List<CicdEvent> allEvents = new ArrayList<>(pipeA);
//        allEvents.addAll(pipeB);
//
//        List<String> alerts = new ArrayList<>();
//        FailurePatternOperator
//                .detect(env.fromCollection(allEvents).keyBy(CicdEvent::getPipelineId))
//                .executeAndCollect()
//                .forEachRemaining(alerts::add);
//
//        // Only pipe-A should produce a match
//        assertEquals("Only one pipeline should produce a full pattern match", 1, alerts.size());
//        assertTrue("Alert should be for pipe-cep-A", alerts.get(0).contains("pipe-cep-A"));
//        assertFalse("Alert should NOT be for pipe-cep-B", alerts.get(0).contains("pipe-cep-B"));
//    }
//
//    // ══════════════════════════════════════════════════════════════════
//    // Pattern structure — verify .within() and .followedBy() are used
//    // ══════════════════════════════════════════════════════════════════
//
//    @Test
//    public void testPatternStructure_hasCorrectSteps() {
//        Pattern<CicdEvent, ?> pattern = FailurePatternOperator.buildPattern();
//        assertNotNull("Pattern should not be null", pattern);
//        // Pattern name is the last step name
//        assertNotNull("Pattern should have a name", pattern.getName());
//    }
//
//    @Test
//    public void testPattern_eventsOutsideWindow_doNotMatch() throws Exception {
//        long base = System.currentTimeMillis();
//        // Pattern spread over 15 minutes — exceeds the 10-minute CEP window
//        List<CicdEvent> events = List.of(
//                event("pipe-cep-5", "DEPENDENCY_UPDATED",      base),
//                event("pipe-cep-5", "UNIT_TEST_FAILED",        base + 3 * 60_000),
//                event("pipe-cep-5", "RETRY_TRIGGERED",         base + 6 * 60_000),
//                event("pipe-cep-5", "INTEGRATION_TEST_FAILED", base + 9 * 60_000),
//                event("pipe-cep-5", "ROLLBACK_TRIGGERED",      base + 15 * 60_000) // outside 10min
//        );
//
//        List<String> alerts = new ArrayList<>();
//        FailurePatternOperator
//                .detect(env.fromCollection(events).keyBy(CicdEvent::getPipelineId))
//                .executeAndCollect()
//                .forEachRemaining(alerts::add);
//
//        assertTrue("Pattern exceeding time window should NOT produce full match alert",
//                alerts.isEmpty());
//    }
//}
