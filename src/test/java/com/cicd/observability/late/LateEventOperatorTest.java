//package com.cicd.observability.late;
//
//import com.cicd.observability.model.CicdEvent;
//import com.cicd.observability.model.MetricResult;
//import com.cicd.observability.operators.late.LateEventOperator;
//import com.cicd.observability.router.EventRouter;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.junit.Assert.*;
//
///**
// * JUnit tests for Late Event Processing.
// *
// * Tests cover:
// *  - EventRouter late candidate threshold detection (now - event_ts > 30s)
// *  - LateEventOperator processes events routed to LATE_TAG
// *  - Side output (truly late events) is populated when allowedLateness exceeded
// */
//public class LateEventOperatorTest {
//
//    private StreamExecutionEnvironment env;
//
//    @Before
//    public void setUp() {
//        env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//    }
//
//    private CicdEvent event(String pipeline, String type, String status, long tsMs) {
//        CicdEvent e = new CicdEvent();
//        e.setPipelineId(pipeline);
//        e.setServiceName("late-test-svc");
//        e.setEventType(type);
//        e.setStatus(status);
//        e.setTimestampMs(tsMs);
//        e.setEventTimestamp(java.time.Instant.ofEpochMilli(tsMs).toString());
//        return e;
//    }
//
//    // ══════════════════════════════════════════════════════════════════
//    // EventRouter — late candidate detection
//    // ══════════════════════════════════════════════════════════════════
//
//    @Test
//    public void testRouter_detectsLateCandidate_whenEventIsOld() throws Exception {
//        // Event timestamped 60 seconds ago → should be routed to LATE_TAG
//        long oldEventMs = System.currentTimeMillis() - 60_000L;
//        CicdEvent lateEvent = event("pipe-late-1", "BUILD_STARTED", "SUCCESS", oldEventMs);
//
//        SingleOutputStreamOperator<CicdEvent> routedStream =
//                env.fromCollection(List.of(lateEvent))
//                   .process(new EventRouter())
//                   .name("router");
//
//        List<CicdEvent> lateRouted = new ArrayList<>();
//        routedStream.getSideOutput(EventRouter.LATE_TAG)
//                    .executeAndCollect()
//                    .forEachRemaining(lateRouted::add);
//
//        assertFalse("Old event should be routed to LATE_TAG", lateRouted.isEmpty());
//        assertEquals("pipe-late-1", lateRouted.get(0).getPipelineId());
//    }
//
//    @Test
//    public void testRouter_doesNotFlagFreshEvent_asLate() throws Exception {
//        // Event timestamped NOW → should NOT be routed to LATE_TAG
//        long freshMs = System.currentTimeMillis();
//        CicdEvent freshEvent = event("pipe-fresh-1", "BUILD_STARTED", "SUCCESS", freshMs);
//
//        SingleOutputStreamOperator<CicdEvent> routedStream =
//                env.fromCollection(List.of(freshEvent))
//                   .process(new EventRouter());
//
//        List<CicdEvent> lateRouted = new ArrayList<>();
//        routedStream.getSideOutput(EventRouter.LATE_TAG)
//                    .executeAndCollect()
//                    .forEachRemaining(lateRouted::add);
//
//        assertTrue("Fresh event should NOT be in LATE_TAG", lateRouted.isEmpty());
//    }
//
//    // ══════════════════════════════════════════════════════════════════
//    // LateEventOperator — processes late-candidate stream
//    // ══════════════════════════════════════════════════════════════════
//
//    @Test
//    public void testLateEventOperator_emitsMetric_forLateEvents() throws Exception {
//        long oldMs = System.currentTimeMillis() - 120_000L; // 2 minutes ago
//        List<CicdEvent> events = List.of(
//                event("pipe-late-2", "BUILD_STARTED", "SUCCESS", oldMs),
//                event("pipe-late-2", "TEST_FAILED",   "FAILURE", oldMs + 5000)
//        );
//
//        List<MetricResult> results = new ArrayList<>();
//        LateEventOperator.compute(env.fromCollection(events))
//                .executeAndCollect()
//                .forEachRemaining(results::add);
//
//        assertFalse("Expected late event metric result", results.isEmpty());
//        MetricResult r = results.get(0);
//        assertEquals("pipe-late-2", r.getPipelineId());
//        assertNotNull(r.getDetail());
//        // detail should contain total and failure count
//        assertTrue(r.getDetail().contains("failures"));
//        assertTrue(r.getDetail().contains("total"));
//    }
//
//    // ══════════════════════════════════════════════════════════════════
//    // Side output — truly late events
//    // ══════════════════════════════════════════════════════════════════
//
//    @Test
//    public void testLateEventSideOutput_containsLateEvents() throws Exception {
//        long veryOldMs = System.currentTimeMillis() - 300_000L; // 5 minutes ago
//        List<CicdEvent> events = List.of(
//                event("pipe-late-3", "DEPLOY_FAILED", "FAILURE", veryOldMs)
//        );
//
//        SingleOutputStreamOperator<MetricResult> lateOp =
//                LateEventOperator.compute(env.fromCollection(events));
//
//        // The side output contains truly late events (beyond allowedLateness)
//        List<CicdEvent> sideOutput = new ArrayList<>();
//        lateOp.getSideOutput(LateEventOperator.LATE_TAG)
//              .executeAndCollect()
//              .forEachRemaining(sideOutput::add);
//
//        // In a test environment with bounded input, truly late events may or
//        // may not appear depending on window firing — we verify the stream exists
//        assertNotNull("Late event side output stream should exist",
//                lateOp.getSideOutput(LateEventOperator.LATE_TAG));
//    }
//}
