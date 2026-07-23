package com.cicd.observability.health;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.health.PipelineHealthOperator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * JUnit tests for Pipeline Health Score operator.
 *
 * The health score is a weighted composite:
 *   build  × 0.40
 *   test   × 0.30
 *   sonar  × 0.20
 *   package× 0.10
 *
 * All events successful → score = 100.0 (Elite)
 * All events failed     → score = 0.0   (Low)
 */
public class PipelineHealthOperatorTest {

    private StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }

    private CicdEvent event(String pipeline, String type, String status, long tsMs) {
        CicdEvent e = new CicdEvent();
        e.setPipelineId(pipeline);
        e.setServiceName("health-test-svc");
        e.setEventType(type);
        e.setStatus(status);
        e.setTimestampMs(tsMs);
        return e;
    }

    /** compute()/computeLive() are event-time based — every test needs a watermark assigned. */
    private DataStream<CicdEvent> withWatermarks(List<CicdEvent> events) {
        return env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((e, ts) -> e.getTimestampMs()));
    }

    // ══════════════════════════════════════════════════════════════════
    // All stages succeed → Elite score
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testHealthScore_allSuccess_isElite() throws Exception {
        long base = System.currentTimeMillis();
        List<CicdEvent> events = List.of(
                event("p1", "BUILD_SUCCESS",     "SUCCESS", base),
                event("p1", "TEST_SUCCESS",      "SUCCESS", base + 1000),
                event("p1", "SONARQUBE_SUCCESS", "SUCCESS", base + 2000),
                event("p1", "PACKAGE_SUCCESS",   "SUCCESS", base + 3000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.compute(withWatermarks(events))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse("Expected health metric", results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(MetricResult.MetricType.PIPELINE_HEALTH_SCORE, r.getMetricType());
        assertEquals(100.0, r.getValue(), 0.01);
        assertEquals("Elite", r.getPerformanceBand());
    }

    // ══════════════════════════════════════════════════════════════════
    // Build failures drag score below threshold
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testHealthScore_buildFailures_reducesScore() throws Exception {
        long base = System.currentTimeMillis();
        // 1 build success + 1 build failure = 50% build rate
        // test, sonar, package all success = 100%
        // score = (50 × 0.35) + (100 × 0.25) + (100 × 0.15) + (100 × 0.10) + (100 × 0.15) = 82.5
        List<CicdEvent> events = List.of(
                event("p2", "BUILD_SUCCESS",     "SUCCESS", base),
                event("p2", "BUILD_FAILED",      "FAILURE", base + 500),
                event("p2", "TEST_SUCCESS",      "SUCCESS", base + 1000),
                event("p2", "SONARQUBE_SUCCESS", "SUCCESS", base + 2000),
                event("p2", "PACKAGE_SUCCESS",   "SUCCESS", base + 3000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.compute(withWatermarks(events))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(82.5, r.getValue(), 1.0); // ±1 tolerance
        assertNotNull(r.getDetail());           // detail JSON should be populated
        assertTrue(r.getDetail().contains("build"));
    }

    // ══════════════════════════════════════════════════════════════════
    // All stages fail → Low band
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testHealthScore_allFailures_isLow() throws Exception {
        long base = System.currentTimeMillis();
        List<CicdEvent> events = List.of(
                event("p3", "BUILD_FAILED",      "FAILURE", base),
                event("p3", "TEST_FAILED",       "FAILURE", base + 1000),
                event("p3", "SONARQUBE_FAILED",  "FAILURE", base + 2000),
                event("p3", "PACKAGE_FAILED",    "FAILURE", base + 3000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.compute(withWatermarks(events))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        MetricResult r = results.get(0);
        // No DEPLOY_ events occur in this test, and rate() defaults an
        // unobserved category to 100% ("no news is good news"), so the
        // deploy component still contributes 100 × 0.15 = 15 even though
        // every observed category (build/test/sonar/package) is at 0%.
        assertEquals(15.0, r.getValue(), 0.01);
        assertEquals("Low", r.getPerformanceBand());
    }

    // ══════════════════════════════════════════════════════════════════
    // Pipeline ID is correctly set in output
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testHealthScore_pipelineIdPreserved() throws Exception {
        long base = System.currentTimeMillis();
        String pipelineId = "bloodpressure-pipeline-99";
        List<CicdEvent> events = List.of(
                event(pipelineId, "BUILD_SUCCESS", "SUCCESS", base)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.compute(withWatermarks(events))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        assertEquals(pipelineId, results.get(0).getPipelineId());
    }

    // ══════════════════════════════════════════════════════════════════
    // Live health score (same pattern as the live deploy/CFR counters)
    // ══════════════════════════════════════════════════════════════════

    @Test
    public void testLiveHealthScore_updatesPerEventWithinWindow() throws Exception {
        long base = System.currentTimeMillis();
        List<CicdEvent> events = List.of(
                event("p-live-1", "BUILD_SUCCESS", "SUCCESS", base),
                event("p-live-1", "BUILD_FAILED",  "FAILURE", base + 1000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.computeLive(withWatermarks(events), Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        // score = buildRate*0.35 + (100 for the other 4 categories, no events yet)
        assertEquals(2, results.size());
        assertEquals(MetricResult.MetricType.PIPELINE_HEALTH_SCORE_LIVE, results.get(0).getMetricType());
        assertEquals("1/1 build success = 100% build rate -> full score",
                100.0, results.get(0).getValue(), 0.01);
        assertEquals("1/2 build success = 50% build rate -> 82.5 composite",
                82.5, results.get(1).getValue(), 0.01);
    }

    @Test
    public void testLiveHealthScore_resetsWhenWindowCloses() throws Exception {
        long base = System.currentTimeMillis();
        // Window size 10s. First window: 1 success + 1 failure = 82.5.
        // Third event is 15s later — past the boundary — so the tally
        // must restart (1/1 success = 100), not accumulate to 2/3.
        List<CicdEvent> events = List.of(
                event("p-live-2", "BUILD_SUCCESS", "SUCCESS", base),
                event("p-live-2", "BUILD_FAILED",  "FAILURE", base + 1000),
                event("p-live-2", "BUILD_SUCCESS", "SUCCESS", base + 15000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.computeLive(withWatermarks(events), Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertEquals(3, results.size());
        assertEquals(82.5, results.get(1).getValue(), 0.01);
        assertEquals("New window should restart clean (100), not carry over the failure",
                100.0, results.get(2).getValue(), 0.01);
        assertEquals(1L, results.get(2).getSampleCount());
    }

    @Test
    public void testLiveHealthScore_dropsLateEventsInsteadOfMiscountingIntoNewWindow() throws Exception {
        long base = System.currentTimeMillis();
        List<CicdEvent> events = List.of(
                event("p-live-3", "BUILD_SUCCESS", "SUCCESS", base + 15000),
                event("p-live-3", "BUILD_FAILED",  "FAILURE", base + 5000),  // late — belongs to window 1
                event("p-live-3", "BUILD_SUCCESS", "SUCCESS", base + 16000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.computeLive(withWatermarks(events), Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertEquals("Late event should be dropped, not emitted at all", 2, results.size());
        assertEquals("Late failure must not be folded into window 2's score",
                100.0, results.get(1).getValue(), 0.01);
        assertEquals(2L, results.get(1).getSampleCount());
    }
}
