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

public class PipelineHealthOperatorTest {

    private StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }

    private long alignedBase(long windowMs) {
        return (System.currentTimeMillis() / windowMs) * windowMs + 1000;
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

    private DataStream<CicdEvent> withWatermarks(List<CicdEvent> events) {
        return env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CicdEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((e, ts) -> e.getTimestampMs()));
    }

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
        PipelineHealthOperator.compute(withWatermarks(events), Time.minutes(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse("Expected health metric", results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(MetricResult.MetricType.PIPELINE_HEALTH_SCORE, r.getMetricType());
        assertEquals(100.0, r.getValue(), 0.01);
        assertEquals("Elite", r.getPerformanceBand());
    }

    @Test
    public void testHealthScore_buildFailures_reducesScore() throws Exception {
        long base = System.currentTimeMillis();

        List<CicdEvent> events = List.of(
                event("p2", "BUILD_SUCCESS",     "SUCCESS", base),
                event("p2", "BUILD_FAILED",      "FAILURE", base + 500),
                event("p2", "TEST_SUCCESS",      "SUCCESS", base + 1000),
                event("p2", "SONARQUBE_SUCCESS", "SUCCESS", base + 2000),
                event("p2", "PACKAGE_SUCCESS",   "SUCCESS", base + 3000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.compute(withWatermarks(events), Time.minutes(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(82.5, r.getValue(), 1.0);
        assertNotNull(r.getDetail());
        assertTrue(r.getDetail().contains("build"));
    }

    @Test
    public void testHealthScore_deployFailures_reducesScore() throws Exception {
        long base = System.currentTimeMillis();

        List<CicdEvent> events = List.of(
                event("p6", "BUILD_SUCCESS",  "SUCCESS", base),
                event("p6", "DEPLOY_FAILED",  "FAILURE", base + 500),
                event("p6", "BUILD_SUCCESS",  "SUCCESS", base + 1000),
                event("p6", "DEPLOY_SUCCESS", "SUCCESS", base + 1500)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.compute(withWatermarks(events), Time.minutes(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        MetricResult r = results.get(0);
        assertEquals(92.5, r.getValue(), 1.0);
        assertTrue(r.getDetail().contains("deploy"));
    }

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
        PipelineHealthOperator.compute(withWatermarks(events), Time.minutes(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        MetricResult r = results.get(0);

        assertEquals(15.0, r.getValue(), 0.01);
        assertEquals("Low", r.getPerformanceBand());
    }

    @Test
    public void testHealthScore_pipelineIdPreserved() throws Exception {
        long base = System.currentTimeMillis();
        String pipelineId = "bloodpressure-pipeline-99";
        List<CicdEvent> events = List.of(
                event(pipelineId, "BUILD_SUCCESS", "SUCCESS", base)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.compute(withWatermarks(events), Time.minutes(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty());
        assertEquals(pipelineId, results.get(0).getPipelineId());
    }

    @Test
    public void testLiveHealthScore_updatesPerEventWithinWindow() throws Exception {
        long base = alignedBase(10_000);
        List<CicdEvent> events = List.of(
                event("p-live-1", "BUILD_SUCCESS", "SUCCESS", base),
                event("p-live-1", "BUILD_FAILED",  "FAILURE", base + 1000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.computeLive(withWatermarks(events), Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        List<MetricResult> increments = results.stream().filter(r -> r.getSampleCount() > 0).toList();
        assertEquals(2, increments.size());
        assertEquals(MetricResult.MetricType.PIPELINE_HEALTH_SCORE_LIVE, increments.get(0).getMetricType());
        assertEquals("1/1 build success = 100% build rate -> full score",
                100.0, increments.get(0).getValue(), 0.01);
        assertEquals("1/2 build success = 50% build rate -> 82.5 composite",
                82.5, increments.get(1).getValue(), 0.01);
        assertEquals(3, results.size());
    }

    @Test
    public void testLiveHealthScore_resetsWhenWindowCloses() throws Exception {
        long base = alignedBase(10_000);

        List<CicdEvent> events = List.of(
                event("p-live-2", "BUILD_SUCCESS", "SUCCESS", base),
                event("p-live-2", "BUILD_FAILED",  "FAILURE", base + 1000),
                event("p-live-2", "BUILD_SUCCESS", "SUCCESS", base + 15000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.computeLive(withWatermarks(events), Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        List<MetricResult> increments = results.stream().filter(r -> r.getSampleCount() > 0).toList();
        List<MetricResult> resets     = results.stream().filter(r -> r.getSampleCount() == 0).toList();

        assertEquals(3, increments.size());
        assertEquals(82.5, increments.get(1).getValue(), 0.01);
        assertEquals("New window should restart clean (100), not carry over the failure",
                100.0, increments.get(2).getValue(), 0.01);
        assertEquals(1L, increments.get(2).getSampleCount());
        assertEquals("One reset per window that closed", 2, resets.size());
    }

    @Test
    public void testLiveHealthScore_dropsLateEventsInsteadOfMiscountingIntoNewWindow() throws Exception {
        long base = alignedBase(10_000);
        List<CicdEvent> events = List.of(
                event("p-live-3", "BUILD_SUCCESS", "SUCCESS", base + 15000),
                event("p-live-3", "BUILD_FAILED",  "FAILURE", base + 5000),
                event("p-live-3", "BUILD_SUCCESS", "SUCCESS", base + 16000)
        );

        List<MetricResult> results = new ArrayList<>();
        PipelineHealthOperator.computeLive(withWatermarks(events), Time.seconds(10))
                .executeAndCollect()
                .forEachRemaining(results::add);

        List<MetricResult> increments = results.stream().filter(r -> r.getSampleCount() > 0).toList();
        assertEquals("Late event should be dropped, not emitted at all", 2, increments.size());
        assertEquals("Late failure must not be folded into window 2's score",
                100.0, increments.get(1).getValue(), 0.01);
        assertEquals(2L, increments.get(1).getSampleCount());
        assertEquals(3, results.size());
    }
}
