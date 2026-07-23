package com.cicd.observability.operators.dora;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * DORA Metrics #2, #3, #4
 *
 * Lead Time for Changes — KeyedProcessFunction with MapState
 *   Stores BUILD_STARTED timestamp per pipeline, computes duration
 *   when DEPLOY_SUCCESS arrives for the same pipeline.
 *   Flink managed MapState survives crashes and restarts.
 *
 * Change Failure Rate — TumblingEventTimeWindows + AggregateFunction
 *   Counts total deploys and failed deploys in a 7-day window.
 *
 * Mean Time to Recovery — KeyedProcessFunction with MapState
 *   Stores INCIDENT_OPEN timestamp keyed by pipelineId,
 *   computes duration when BUILD_SUCCESS follows.
 */
public class DoraOperators {

    // ══════════════════════════════════════════════════════════════════
    // #2 — Lead Time for Changes
    // ══════════════════════════════════════════════════════════════════

    public static DataStream<MetricResult> leadTime(DataStream<CicdEvent> events) {
        return events
                .filter(e -> "BUILD_STARTED".equals(e.getEventType())
                          || "DEPLOY_SUCCESS".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .process(new LeadTimeProcessFn());
    }

    static class LeadTimeProcessFn
            extends KeyedProcessFunction<String, CicdEvent, MetricResult> {

        /**
         * Flink MapState — keyed by commitSha, stores build-start epoch-ms.
         * Lives in Flink's managed state (RocksDB in production) — not a
         * Java Map in memory, so it survives job restarts automatically.
         */
        private transient MapState<String, Long> buildStartTimes;
      //  private transient ValueState<List<Double>> leadTimeSamples;

        @Override
        public void open(Configuration cfg) {
            buildStartTimes = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("build-start-times", Types.STRING, Types.LONG));
//            leadTimeSamples = getRuntimeContext().getState(
//                    new ValueStateDescriptor<>("lead-time-samples",
//                            Types.LIST(Types.DOUBLE)));
        }

        @Override
        public void processElement(CicdEvent e, Context ctx,
                                   Collector<MetricResult> out) throws Exception {
            if ("BUILD_STARTED".equals(e.getEventType())) {
                // Store the timestamp keyed by commitSha so we can match
                // it when the deploy event for the same commit arrives.
                buildStartTimes.put(e.getCommitSha(), e.getTimestampMs());

            } else if ("DEPLOY_SUCCESS".equals(e.getEventType())) {
                Long startMs = buildStartTimes.get(e.getCommitSha());
                if (startMs != null) {
                    double leadMins = (e.getTimestampMs() - startMs) / 60_000.0;
//                    List<Double> samples = leadTimeSamples.value();
//                    if (samples == null) samples = new ArrayList<>();
//                    samples.add(leadMins);
//                    leadTimeSamples.update(samples);
                    buildStartTimes.remove(e.getCommitSha());

                    // Emit a rolling sample for Grafana dashboards
                    MetricResult leadTimeMetric = new MetricResult(
                            MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                            e.getPipelineId(), e.getServiceName(),
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(startMs), ZoneOffset.UTC).toString(),
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(e.getTimestampMs()), ZoneOffset.UTC).toString(),
                            leadMins, 1);
                    out.collect(leadTimeMetric);
                }
            }
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx,
                            Collector<MetricResult> out) throws Exception {
            // No timer needed — we emit on every deploy event
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // #3 — Change Failure Rate
    // ══════════════════════════════════════════════════════════════════

    private static final Time CFR_ALLOWED_LATENESS = Time.hours(1);

    /**
     * Truly-late events (beyond CFR_ALLOWED_LATENESS) are silently dropped —
     * no sideOutputLateData() here. Only DeploymentFrequencyOperator keeps
     * a truly-late audit trail; see its class comment for why.
     */
    public static SingleOutputStreamOperator<MetricResult> changeFailureRate(
            DataStream<CicdEvent> events, Time windowSize) {

        return events
                .filter(e -> "DEPLOY_SUCCESS".equals(e.getEventType())
                          || "DEPLOY_FAILED".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .window(TumblingEventTimeWindows.of(windowSize))
                .allowedLateness(CFR_ALLOWED_LATENESS)
                .aggregate(new CfrAgg(), new CfrWindowFn());
    }

    static class CfrAcc {
        long total = 0, failed = 0;
        String serviceName = "";
    }

    static class CfrAgg implements AggregateFunction<CicdEvent, CfrAcc, CfrAcc> {
        @Override public CfrAcc createAccumulator() { return new CfrAcc(); }

        @Override
        public CfrAcc add(CicdEvent e, CfrAcc acc) {
            acc.total++;
            if ("DEPLOY_FAILED".equals(e.getEventType()) || e.isFailure()) acc.failed++;
            acc.serviceName = e.getServiceName();
            return acc;
        }

        @Override public CfrAcc getResult(CfrAcc acc) { return acc; }

        @Override
        public CfrAcc merge(CfrAcc a, CfrAcc b) {
            a.total  += b.total;
            a.failed += b.failed;
            return a;
        }
    }

    static class CfrWindowFn
            extends ProcessWindowFunction<CfrAcc, MetricResult, String, TimeWindow> {
        @Override
        public void process(String pipelineId, Context ctx,
                            Iterable<CfrAcc> elems, Collector<MetricResult> out) {
            CfrAcc acc = elems.iterator().next();
            double cfr = acc.total == 0 ? 0 : (acc.failed * 100.0 / acc.total);
            out.collect(new MetricResult(
                    MetricResult.MetricType.CHANGE_FAILURE_RATE,
                    pipelineId, acc.serviceName,
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getStart()), ZoneOffset.UTC).toString(),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getEnd()), ZoneOffset.UTC).toString(), cfr, acc.total));
        }
    }

    // ── Live counter — same pattern as DeploymentFrequencyOperator's live
    //    stream: no window, resets on the window's close boundary, drops
    //    events that belong to an already-closed window instead of
    //    miscounting them into the current one. ─────────────────────────

    private static final String CFR_LIVE_WINDOW_MARKER =
            LocalDateTime.of(1970, 1, 1, 0, 0, 0).toString();

    /**
     * @param windowSize same bucket size as {@link #changeFailureRate}'s
     *                   window (e.g. 7 days in production).
     */
    public static SingleOutputStreamOperator<MetricResult> changeFailureRateLive(
            DataStream<CicdEvent> events, Time windowSize) {
        return events
                .filter(e -> "DEPLOY_SUCCESS".equals(e.getEventType())
                          || "DEPLOY_FAILED".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .process(new LiveCfrCounter(windowSize.toMilliseconds()));
    }

    static class LiveCfrCounter extends KeyedProcessFunction<String, CicdEvent, MetricResult> {

        private final long windowMs;
        private transient ValueState<Long> total;
        private transient ValueState<Long> failed;
        private transient ValueState<Long> windowEnd;
        private transient ValueState<String> serviceName;

        LiveCfrCounter(long windowMs) { this.windowMs = windowMs; }

        @Override
        public void open(Configuration cfg) {
            total     = getRuntimeContext().getState(new ValueStateDescriptor<>("live-cfr-total",  Types.LONG));
            failed    = getRuntimeContext().getState(new ValueStateDescriptor<>("live-cfr-failed", Types.LONG));
            windowEnd = getRuntimeContext().getState(new ValueStateDescriptor<>("live-cfr-window-end", Types.LONG));
            serviceName = getRuntimeContext().getState(new ValueStateDescriptor<>("live-cfr-service-name", Types.STRING));
        }

        @Override
        public void processElement(CicdEvent event, Context ctx, Collector<MetricResult> out) throws Exception {
            long ts = event.getTimestampMs();
            Long currentWindowEnd = windowEnd.value();

            if (currentWindowEnd != null && ts < currentWindowEnd - windowMs) {
                // Late event for an already-closed window — the live tile
                // only reflects the window currently open; changeFailureRate()
                // (with allowedLateness) is the source of truth for corrections.
                return;
            }

            if (currentWindowEnd == null || ts >= currentWindowEnd) {
                total.clear();
                failed.clear();
                long nextWindowEnd = ((ts / windowMs) + 1) * windowMs;
                windowEnd.update(nextWindowEnd);
                ctx.timerService().registerEventTimeTimer(nextWindowEnd - 1);
            }

            long totalCount = (total.value() == null ? 0L : total.value()) + 1;
            total.update(totalCount);

            long failedCount = failed.value() == null ? 0L : failed.value();
            if ("DEPLOY_FAILED".equals(event.getEventType()) || event.isFailure()) {
                failedCount++;
                failed.update(failedCount);
            }

            double cfr = failedCount * 100.0 / totalCount;
            serviceName.update(event.getServiceName());

            out.collect(new MetricResult(
                    MetricResult.MetricType.CHANGE_FAILURE_RATE_LIVE,
                    event.getPipelineId(), event.getServiceName(),
                    CFR_LIVE_WINDOW_MARKER, CFR_LIVE_WINDOW_MARKER,
                    cfr, totalCount));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricResult> out) throws Exception {
            // Window closed — emit the reset itself so Postgres/Grafana show
            // 0% immediately, instead of the last window's rate sitting
            // there stale until the next deploy/failure happens.
            total.clear();
            failed.clear();
            out.collect(new MetricResult(
                    MetricResult.MetricType.CHANGE_FAILURE_RATE_LIVE,
                    ctx.getCurrentKey(), serviceName.value(),
                    CFR_LIVE_WINDOW_MARKER, CFR_LIVE_WINDOW_MARKER,
                    0.0, 0));
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // #4 — Mean Time to Recovery (MTTR)
    // ══════════════════════════════════════════════════════════════════

    public static DataStream<MetricResult> mttr(DataStream<CicdEvent> events) {
        return events
                .filter(e -> "BUILD_FAILED".equals(e.getEventType())
                          || "BUILD_SUCCESS".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .process(new MttrProcessFn());
    }

    static class MttrProcessFn
            extends KeyedProcessFunction<String, CicdEvent, MetricResult> {

        /** Flink ValueState — stores the timestamp of the last BUILD_FAILED event. */
        private transient ValueState<Long> failureStartMs;

        @Override
        public void open(Configuration cfg) {
            failureStartMs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("failure-start-ms", Types.LONG));
        }

        @Override
        public void processElement(CicdEvent e, Context ctx,
                                   Collector<MetricResult> out) throws Exception {
            if ("BUILD_FAILED".equals(e.getEventType())) {
                // Record when the failure started
                failureStartMs.update(e.getTimestampMs());

            } else if ("BUILD_SUCCESS".equals(e.getEventType())) {
                Long startMs = failureStartMs.value();
                if (startMs != null) {
                    double mttrMins = (e.getTimestampMs() - startMs) / 60_000.0;
                    failureStartMs.clear();

                    out.collect(new MetricResult(
                            MetricResult.MetricType.MEAN_TIME_TO_RECOVERY,
                            e.getPipelineId(), e.getServiceName(),
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(startMs), ZoneOffset.UTC).toString(),
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(e.getTimestampMs()), ZoneOffset.UTC).toString(),
                            mttrMins, 1));
                }
            }
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx,
                            Collector<MetricResult> out) {}
    }
}
