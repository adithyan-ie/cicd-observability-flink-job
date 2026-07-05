package com.cicd.observability.health;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.health.PipelineHealthOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

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
        PipelineHealthOperator.compute(env.fromCollection(events))
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
        // score = (50 × 0.40) + (100 × 0.30) + (100 × 0.20) + (100 × 0.10) = 80.0
        List<CicdEvent> events = List.of(
                event("p2", "BUILD_SUCCESS",     "SUCCESS", base),
                event("p2", "BUILD_FAILED",      "FAILURE", base + 500),
                event("p2", "TEST_SUCCESS",      "SUCCESS", base + 1000),
                event("p2", "SONARQUBE_SUCCESS", "SUCCESS", base + 2000),
                event("p2", "PACKAGE_SUCCESS",   "SUCCESS", base + 3000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.compute(env.fromCollection(events))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(80.0, r.getValue(), 1.0); // ±1 tolerance
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
        PipelineHealthOperator.compute(env.fromCollection(events))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(0.0, r.getValue(), 0.01);
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
        PipelineHealthOperator.compute(env.fromCollection(events))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        assertEquals(pipelineId, results.get(0).getPipelineId());
    }
}
