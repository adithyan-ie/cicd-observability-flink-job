package com.cicd.observability.operators.health;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Pipeline Health Score
 *
 * Two independent streams, same shape as DeploymentFrequencyOperator /
 * DoraOperators' CFR:
 *   - {@link #compute}     → historical, once-per-day score (tumbling
 *     window, default trigger, allowedLateness for corrections).
 *   - {@link #computeLive} → real-time score for the live dashboard tile
 *     (no windowing — recomputes and emits on every event).
 *
 * A SlidingEventTimeWindows was used previously to get "frequent updates,"
 * but that fans every event into size/slide overlapping window instances
 * (e.g. 48 of them for a 1-day/30-min slide) which all fire independently —
 * expensive, and still needed filterChanged() downstream just to keep
 * Postgres from flooding. Splitting into a plain tumbling window (cheap,
 * fires once/day) plus a dedicated live counter (cheap, no window state)
 * gives the same "live" behaviour without the overlapping-window cost.
 *
 * Score formula (identical for both streams — see {@link #buildResult}):
 *   buildSuccessRate  (weight 0.35) × 100
 *   testSuccessRate   (weight 0.25) × 100
 *   sonarPassRate     (weight 0.15) × 100
 *   packageSuccessRate(weight 0.10) × 100
 *   deploySuccessRate (weight 0.15) × 100
 */
public class PipelineHealthOperator {

    private static final Time ALLOWED_LATENESS = Time.hours(1);

    // ════════════════════════════════════════════════════════════════
    // Historical metric — 1-day tumbling window, default EventTimeTrigger
    // ════════════════════════════════════════════════════════════════

    /**
     * Truly-late events (beyond ALLOWED_LATENESS) are silently dropped — no
     * sideOutputLateData() here. Only DeploymentFrequencyOperator keeps a
     * truly-late audit trail; see its class comment for why.
     */
    public static SingleOutputStreamOperator<MetricResult> compute(DataStream<CicdEvent> events) {
        return events
                .keyBy(CicdEvent::getPipelineId)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .allowedLateness(ALLOWED_LATENESS)
                .aggregate(new HealthAgg(), new HealthWindowFn());
    }

    /**
     * Keeps only the rows where the performance band (Elite/High/Medium/Low)
     * actually changed since this pipeline's last emission. Intended for the
     * live stream below, which emits on every event — the historical window
     * above only fires once/day (+ late corrections) so it doesn't need
     * this filter.
     */
    public static DataStream<MetricResult> filterChanged(DataStream<MetricResult> health) {
        return health
                .keyBy(MetricResult::getPipelineId)
                .filter(new BandChangeFilter());
    }

    static class BandChangeFilter extends RichFilterFunction<MetricResult> {
        private transient ValueState<String> lastBand;

        @Override
        public void open(Configuration parameters) {
            lastBand = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-health-band", String.class));
        }

        @Override
        public boolean filter(MetricResult r) throws Exception {
            String previous = lastBand.value();
            String current  = r.getPerformanceBand();
            if (current != null && current.equals(previous)) {
                return false;
            }
            lastBand.update(current);
            return true;
        }
    }

    // ── Accumulator ────────────────────────────────────────────────────

    static class HealthAcc {
        // Build
        long buildTotal = 0, buildSuccess = 0;
        // Test
        long testTotal  = 0, testSuccess  = 0;
        // SonarQube
        long sonarTotal = 0, sonarSuccess = 0;
        // Package
        long pkgTotal   = 0, pkgSuccess   = 0;
        // Deploy
        long deployTotal = 0, deploySuccess = 0;
        String serviceName = "";
    }

    /** Shared by both streams so the live and historical scores can never drift apart. */
    private static void accumulate(HealthAcc acc, CicdEvent e) {
        acc.serviceName = e.getServiceName();
        String et = e.getEventType();

        if (et != null) {
            if (et.startsWith("BUILD_")) {
                acc.buildTotal++;
                if (e.isSuccess()) acc.buildSuccess++;
            } else if (et.startsWith("TEST_")) {
                acc.testTotal++;
                if (e.isSuccess()) acc.testSuccess++;
            } else if (et.startsWith("SONARQUBE_")) {
                acc.sonarTotal++;
                if (e.isSuccess()) acc.sonarSuccess++;
            } else if (et.startsWith("PACKAGE_")) {
                acc.pkgTotal++;
                if (e.isSuccess()) acc.pkgSuccess++;
            } else if (et.startsWith("DEPLOY_")) {
                acc.deployTotal++;
                if (e.isSuccess()) acc.deploySuccess++;
            }
        }
    }

    private static double rate(long success, long total) {
        return total == 0 ? 100.0 : (success * 100.0 / total);
    }

    /** Shared by both streams — builds the weighted composite score + detail JSON. */
    private static MetricResult buildResult(String pipelineId, HealthAcc acc,
                                             MetricResult.MetricType type,
                                             String windowStart, String windowEnd) {
        double buildRate  = rate(acc.buildSuccess,  acc.buildTotal);
        double testRate   = rate(acc.testSuccess,   acc.testTotal);
        double sonarRate  = rate(acc.sonarSuccess,  acc.sonarTotal);
        double pkgRate    = rate(acc.pkgSuccess,    acc.pkgTotal);
        double deployRate = rate(acc.deploySuccess, acc.deployTotal);

        double score = (buildRate  * 0.35)
                     + (testRate   * 0.25)
                     + (sonarRate  * 0.15)
                     + (pkgRate    * 0.10)
                     + (deployRate * 0.15);

        long totalEvents = acc.buildTotal + acc.testTotal
                         + acc.sonarTotal + acc.pkgTotal + acc.deployTotal;

        MetricResult r = new MetricResult(type, pipelineId, acc.serviceName,
                windowStart, windowEnd, score, totalEvents);

        r.setDetail(String.format(
                "{\"build\":%.1f,\"test\":%.1f,\"sonar\":%.1f,\"package\":%.1f,\"deploy\":%.1f}",
                buildRate, testRate, sonarRate, pkgRate, deployRate));
        return r;
    }

    // ── AggregateFunction ──────────────────────────────────────────────

    static class HealthAgg
            implements AggregateFunction<CicdEvent, HealthAcc, HealthAcc> {

        @Override public HealthAcc createAccumulator() { return new HealthAcc(); }

        @Override
        public HealthAcc add(CicdEvent e, HealthAcc acc) {
            accumulate(acc, e);
            return acc;
        }

        @Override public HealthAcc getResult(HealthAcc acc) { return acc; }

        @Override
        public HealthAcc merge(HealthAcc a, HealthAcc b) {
            a.buildTotal   += b.buildTotal;   a.buildSuccess  += b.buildSuccess;
            a.testTotal    += b.testTotal;    a.testSuccess   += b.testSuccess;
            a.sonarTotal   += b.sonarTotal;   a.sonarSuccess  += b.sonarSuccess;
            a.pkgTotal     += b.pkgTotal;     a.pkgSuccess    += b.pkgSuccess;
            a.deployTotal  += b.deployTotal;  a.deploySuccess += b.deploySuccess;
            return a;
        }
    }

    // ── ProcessWindowFunction ──────────────────────────────────────────

    static class HealthWindowFn
            extends ProcessWindowFunction<HealthAcc, MetricResult, String, TimeWindow> {

        @Override
        public void process(String pipelineId, Context ctx,
                            Iterable<HealthAcc> elems, Collector<MetricResult> out) {
            HealthAcc acc = elems.iterator().next();
            out.collect(buildResult(pipelineId, acc, MetricResult.MetricType.PIPELINE_HEALTH_SCORE,
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getStart()), ZoneOffset.UTC).toString(),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getEnd()), ZoneOffset.UTC).toString()));
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Live metric — no window, emits on every stage event
    // ════════════════════════════════════════════════════════════════

    private static final String LIVE_WINDOW_MARKER = LocalDateTime.of(1970, 1, 1, 0, 0, 0).toString();

    /**
     * @param windowSize same bucket size as {@link #compute}'s window (e.g.
     *                   1 day in production) — the live tally resets each
     *                   time event-time crosses one of these boundaries.
     */
    public static DataStream<MetricResult> computeLive(DataStream<CicdEvent> events, Time windowSize) {
        return events
                .keyBy(CicdEvent::getPipelineId)
                .process(new LiveHealthCounter(windowSize.toMilliseconds()));
    }

    static class LiveHealthCounter extends KeyedProcessFunction<String, CicdEvent, MetricResult> {

        private final long windowMs;
        private transient ValueState<HealthAcc> acc;
        private transient ValueState<Long> windowEnd;

        LiveHealthCounter(long windowMs) { this.windowMs = windowMs; }

        @Override
        public void open(Configuration cfg) {
            acc = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("live-health-acc", HealthAcc.class));
            windowEnd = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("live-health-window-end", Long.class));
        }

        @Override
        public void processElement(CicdEvent event, Context ctx, Collector<MetricResult> out) throws Exception {
            long ts = event.getTimestampMs();
            Long currentWindowEnd = windowEnd.value();

            if (currentWindowEnd != null && ts < currentWindowEnd - windowMs) {
                // Late event for an already-closed window — the live tile
                // only reflects the window currently open; compute() (with
                // allowedLateness) is the source of truth for corrections.
                return;
            }

            if (currentWindowEnd == null || ts >= currentWindowEnd) {
                acc.clear();
                long nextWindowEnd = ((ts / windowMs) + 1) * windowMs;
                windowEnd.update(nextWindowEnd);
                ctx.timerService().registerEventTimeTimer(nextWindowEnd - 1);
            }

            HealthAcc a = acc.value();
            if (a == null) a = new HealthAcc();
            accumulate(a, event);
            acc.update(a);

            out.collect(buildResult(event.getPipelineId(), a, MetricResult.MetricType.PIPELINE_HEALTH_SCORE_LIVE,
                    LIVE_WINDOW_MARKER, LIVE_WINDOW_MARKER));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricResult> out) throws Exception {
            // Window closed — emit the reset itself so Postgres/Grafana show
            // a fresh score immediately, instead of the last window's score
            // sitting there stale until the next event happens.
            HealthAcc previous = acc.value();
            String lastServiceName = previous == null ? "" : previous.serviceName;
            acc.clear();

            HealthAcc fresh = new HealthAcc();
            fresh.serviceName = lastServiceName;
            out.collect(buildResult(ctx.getCurrentKey(), fresh, MetricResult.MetricType.PIPELINE_HEALTH_SCORE_LIVE,
                    LIVE_WINDOW_MARKER, LIVE_WINDOW_MARKER));
        }
    }
}
