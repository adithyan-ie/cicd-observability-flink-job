package com.cicd.observability.operators.dora;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * DORA Metric #1 — Deployment Frequency
 *
 * Flink features used:
 *   - filter()                     → keep only DEPLOY_SUCCESS events
 *   - keyBy(pipelineId)            → one state machine per pipeline
 *   - TumblingEventTimeWindows(1d) → count per day, driven by watermark
 *   - AggregateFunction            → incremental count (memory-efficient)
 *   - ProcessWindowFunction        → access window metadata (start/end ms)
 *   - .allowedLateness(1h)         → a DEPLOY_SUCCESS event that arrives after
 *     the window has fired (but within the grace period) re-triggers THIS
 *     window and re-emits an updated count for the same window. Postgres
 *     upserts on (metric_type, pipeline_id, window_start_ms, window_end_ms),
 *     so the correction overwrites the earlier row instead of duplicating it.
 *   - .sideOutputLateData(TRULY_LATE_TAG) → events beyond the grace period,
 *     for audit instead of silent drop.
 */
public class DeploymentFrequencyOperator {

    /** Deploy events too late even for the allowed-lateness grace period. */
    public static final OutputTag<CicdEvent> TRULY_LATE_TAG =
            new OutputTag<CicdEvent>("dora-truly-late-events") {};

    private static final Time ALLOWED_LATENESS = Time.hours(1);

    public static SingleOutputStreamOperator<MetricResult> compute(
            DataStream<CicdEvent> events, Time windowSize) {

        double windowDays = windowSize.toMilliseconds() / (double)(86_400_000L);
        Long slidingTime = Long.parseLong(String.valueOf(windowDays));
        return events
                .filter(e -> "DEPLOY_SUCCESS".equals(e.getEventType()))
                .map(e -> {
                            System.out.println(e.getEventType());
                            System.out.println("event time:"+ e.getEventTimestamp());


                            return e;})
                .keyBy(CicdEvent::getPipelineId)
                .window(SlidingEventTimeWindows.of(Time.days(slidingTime),Time.minutes(1)))
                .allowedLateness(ALLOWED_LATENESS)
                .sideOutputLateData(TRULY_LATE_TAG)
                .aggregate(new DeployCountAgg(), new DeployFreqWindowFn(windowDays));
    }

    // ── Accumulator ────────────────────────────────────────────────────

    public static class DeployCount {
        long count = 0;
        String serviceName = "";
    }

    // ── AggregateFunction ──────────────────────────────────────────────

    static class DeployCountAgg
            implements AggregateFunction<CicdEvent, DeployCount, DeployCount> {

        @Override public DeployCount createAccumulator() { return new DeployCount(); }

        @Override
        public DeployCount add(CicdEvent e, DeployCount acc) {
            acc.count++;
            acc.serviceName = e.getServiceName();
            return acc;
        }

        @Override public DeployCount getResult(DeployCount acc) { return acc; }

        @Override
        public DeployCount merge(DeployCount a, DeployCount b) {
            a.count += b.count;
            return a;
        }
    }

    // ── ProcessWindowFunction ──────────────────────────────────────────

    static class DeployFreqWindowFn
            extends ProcessWindowFunction<DeployCount, MetricResult, String, TimeWindow> {

        private final double windowDays;
        DeployFreqWindowFn(double windowDays) { this.windowDays = windowDays; }

        @Override
        public void process(String pipelineId, Context ctx,
                            Iterable<DeployCount> elements, Collector<MetricResult> out) {
            DeployCount acc = elements.iterator().next();
            double deploysPerDayRate = acc.count / windowDays;

            System.out.println(
                    "Window: " +
                            Instant.ofEpochMilli(ctx.window().getStart()) +
                            " -> " +
                            Instant.ofEpochMilli(ctx.window().getEnd()) +
                            " count=" + acc.count
            );
            MetricResult r = new MetricResult(
                    MetricResult.MetricType.DEPLOYMENT_FREQUENCY,
                    pipelineId, acc.serviceName,
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getStart()), ZoneOffset.UTC).toString(),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getEnd()), ZoneOffset.UTC).toString(),
                    acc.count, acc.count);
            out.collect(r);
        }
    }
}
