package com.cicd.observability.operators.dora;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * DORA Metric #1 — Deployment Frequency
 *
 * Two independent streams, computed and sunk separately:
 *   - {@link #compute}     → historical, DORA-accurate daily count (tumbling
 *     window, fires once per day + late corrections).
 *   - {@link #computeLive} → real-time counter for the live dashboard tile
 *     (no windowing — emits immediately on every deploy).
 *
 * Keeping these separate means the live tile updates instantly without
 * forcing the historical window to re-fire repeatedly (which previously
 * caused checkpoint-timeout instability when replaying a Kafka backlog).
 */
public class DeploymentFrequencyOperator {

    /** Deploy events too late even for the allowed-lateness grace period. */
    public static final OutputTag<CicdEvent> TRULY_LATE_TAG =
            new OutputTag<CicdEvent>("dora-truly-late-events") {};

    private static final Time ALLOWED_LATENESS = Time.hours(1);

    // ════════════════════════════════════════════════════════════════
    // Historical metric — 1-day tumbling window, default EventTimeTrigger
    // ════════════════════════════════════════════════════════════════

    public static SingleOutputStreamOperator<MetricResult> compute(
            DataStream<CicdEvent> events, Time windowSize) {

        double windowDays = windowSize.toMilliseconds() / (double)(86_400_000L);
        return events
                .filter(e -> "DEPLOY_SUCCESS".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .window(TumblingEventTimeWindows.of(windowSize))
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

            MetricResult r = new MetricResult(
                    MetricResult.MetricType.DEPLOYMENT_FREQUENCY,
                    pipelineId, acc.serviceName,
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getStart()), ZoneOffset.UTC).toString(),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getEnd()), ZoneOffset.UTC).toString(),
                    acc.count, acc.count);
            out.collect(r);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Live metric — no window, emits on every deploy event
    // ════════════════════════════════════════════════════════════════

    /**
     * Sentinel window_start/window_end for the live metric row, so every
     * emission for the same pipeline shares one Postgres row (upserted in
     * place via the same unique index used by the windowed metric) instead
     * of inserting a new row per deploy.
     */
    private static final String LIVE_WINDOW_MARKER = LocalDateTime.of(1970, 1, 1, 0, 0, 0).toString();

    /**
     * @param windowSize same bucket size as the historical {@link #compute}
     *                   window (e.g. 1 day in production) — the live counter
     *                   resets each time event-time crosses one of these
     *                   boundaries, so "live count" always means "count so
     *                   far in the window that's currently open."
     */
    public static SingleOutputStreamOperator<MetricResult> computeLive(
            DataStream<CicdEvent> events, Time windowSize) {
        return events
                .filter(e -> "DEPLOY_SUCCESS".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .process(new LiveDeployCounter(windowSize.toMilliseconds()));
    }

    static class LiveDeployCounter extends KeyedProcessFunction<String, CicdEvent, MetricResult> {

        private final long windowMs;
        private transient ValueState<Long> counter;
        private transient ValueState<Long> windowEnd;

        LiveDeployCounter(long windowMs) { this.windowMs = windowMs; }

        @Override
        public void open(Configuration parameters) {
            counter = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("live-deploy-count", Long.class));
            windowEnd = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("live-deploy-window-end", Long.class));
        }

        @Override
        public void processElement(CicdEvent event, Context ctx, Collector<MetricResult> out) throws Exception {
            long ts = event.getTimestampMs();
            Long currentWindowEnd = windowEnd.value();

            if (currentWindowEnd != null && ts < currentWindowEnd - windowMs) {
                // Late event belonging to a window that has already closed.
                // The live tile only reflects the window currently open —
                // counting this here would silently attribute it to the
                // wrong window. The historical stream (compute()), which
                // has allowedLateness, is the source of truth for any
                // retroactive correction.
                return;
            }

            if (currentWindowEnd == null || ts >= currentWindowEnd) {
                // First event for this key, or this event belongs to a later
                // window than the one currently being counted (covers the
                // case where the close-timer below hasn't fired yet, e.g.
                // several windows are skipped at once).
                counter.clear();
                long nextWindowEnd = ((ts / windowMs) + 1) * windowMs;
                windowEnd.update(nextWindowEnd);
                ctx.timerService().registerEventTimeTimer(nextWindowEnd - 1);
            }

            long count = (counter.value() == null ? 0L : counter.value()) + 1;
            counter.update(count);

            out.collect(new MetricResult(
                    MetricResult.MetricType.DEPLOYMENT_FREQUENCY_LIVE,
                    event.getPipelineId(), event.getServiceName(),
                    LIVE_WINDOW_MARKER, LIVE_WINDOW_MARKER,
                    count, count));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricResult> out) {
            // Window closed — next deploy starts a fresh count.
            counter.clear();
        }
    }
}
