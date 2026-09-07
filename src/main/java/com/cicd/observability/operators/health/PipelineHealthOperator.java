package com.cicd.observability.operators.health;

import com.cicd.observability.config.FlinkConfig;
import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.SourceTiming;
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
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class PipelineHealthOperator {

    private static final Time ALLOWED_LATENESS = Time.hours(5);

    public static final OutputTag<CicdEvent> TRULY_LATE_TAG =
            new OutputTag<CicdEvent>("health-truly-late-events") {};

    public static SingleOutputStreamOperator<MetricResult> compute(
            DataStream<CicdEvent> events, Time windowSize) {
        return events
                .keyBy(CicdEvent::getPipelineId)
                .window(TumblingEventTimeWindows.of(windowSize))
                .allowedLateness(ALLOWED_LATENESS)
                .sideOutputLateData(TRULY_LATE_TAG)
                .aggregate(new HealthAgg(), new HealthWindowFn());
    }

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

    static class HealthAcc {

        long buildTotal = 0, buildSuccess = 0;

        long testTotal  = 0, testSuccess  = 0;

        long sonarTotal = 0, sonarSuccess = 0;

        long pkgTotal   = 0, pkgSuccess   = 0;

        long deployTotal = 0, deploySuccess = 0;
        String serviceName = "";

        long minFlinkReceivedAtMs = 0;
    }

    private static boolean isTerminalOutcome(String eventType) {
        return eventType != null
                && (eventType.endsWith("_SUCCESS")
                 || eventType.endsWith("_FAILED")
                 || eventType.endsWith("_FAILURE"));
    }

    private static void accumulate(HealthAcc acc, CicdEvent e) {
        String et = e.getEventType();
        if (!isTerminalOutcome(et)) {
            return;
        }

        acc.serviceName = e.getServiceName();
        acc.minFlinkReceivedAtMs = SourceTiming.earliest(acc.minFlinkReceivedAtMs, e.getFlinkReceivedAtMs());

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

    private static double rate(long success, long total) {
        return total == 0 ? 100.0 : (success * 100.0 / total);
    }

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

        r.setFlinkReceivedAtMs(acc.minFlinkReceivedAtMs);
        r.setDetail(String.format(
                "{\"build\":%.1f,\"test\":%.1f,\"sonar\":%.1f,\"package\":%.1f,\"deploy\":%.1f}",
                buildRate, testRate, sonarRate, pkgRate, deployRate));
        return r;
    }

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
            a.minFlinkReceivedAtMs = SourceTiming.earliest(a.minFlinkReceivedAtMs, b.minFlinkReceivedAtMs);
            return a;
        }
    }

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

    private static final String LIVE_WINDOW_MARKER = LocalDateTime.of(1970, 1, 1, 0, 0, 0).toString();

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

            if (FlinkConfig.LIVE_COUNTER_WATERMARK_GATE) {
                long eventWindowEnd = ((ts / windowMs) + 1) * windowMs;
                if (ctx.timerService().currentWatermark() >= eventWindowEnd) {

                    return;
                }
            }

            if (currentWindowEnd != null && ts < currentWindowEnd - windowMs) {

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
