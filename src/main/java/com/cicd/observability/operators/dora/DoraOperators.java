package com.cicd.observability.operators.dora;

import com.cicd.observability.config.FlinkConfig;
import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.SourceTiming;
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
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class DoraOperators {

    public static DataStream<MetricResult> leadTime(DataStream<CicdEvent> events) {
        return events
                .filter(e -> "BUILD_STARTED".equals(e.getEventType())
                          || "DEPLOY_SUCCESS".equals(e.getEventType())
                          || "DEPLOY_FAILED".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .process(new LeadTimeProcessFn());
    }

    static class LeadTimeProcessFn
            extends KeyedProcessFunction<String, CicdEvent, MetricResult> {

        private transient MapState<String, Long> buildStartTimes;

        private transient MapState<String, Long> buildStartFlinkReceivedAt;

        @Override
        public void open(Configuration cfg) {
            buildStartTimes = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("build-start-times", Types.STRING, Types.LONG));
            buildStartFlinkReceivedAt = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("build-start-flink-received-at", Types.STRING, Types.LONG));

        }

        @Override
        public void processElement(CicdEvent e, Context ctx,
                                   Collector<MetricResult> out) throws Exception {
            if ("BUILD_STARTED".equals(e.getEventType())) {

                buildStartTimes.put(e.getCommitSha(), e.getTimestampMs());
                buildStartFlinkReceivedAt.put(e.getCommitSha(), e.getFlinkReceivedAtMs());

            } else if ("DEPLOY_SUCCESS".equals(e.getEventType())) {
                Long startMs = buildStartTimes.get(e.getCommitSha());
                if (startMs != null) {
                    double leadMins = (e.getTimestampMs() - startMs) / 60_000.0;

                    Long startFlinkReceivedAt = buildStartFlinkReceivedAt.get(e.getCommitSha());
                    buildStartTimes.remove(e.getCommitSha());
                    buildStartFlinkReceivedAt.remove(e.getCommitSha());

                    MetricResult leadTimeMetric = new MetricResult(
                            MetricResult.MetricType.LEAD_TIME_FOR_CHANGES,
                            e.getPipelineId(), e.getServiceName(),
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(startMs), ZoneOffset.UTC).toString(),
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(e.getTimestampMs()), ZoneOffset.UTC).toString(),
                            leadMins, 1);

                    leadTimeMetric.setFlinkReceivedAtMs(startFlinkReceivedAt != null ? startFlinkReceivedAt : 0);
                    out.collect(leadTimeMetric);
                }

            } else if ("DEPLOY_FAILED".equals(e.getEventType())) {

                buildStartTimes.remove(e.getCommitSha());
                buildStartFlinkReceivedAt.remove(e.getCommitSha());
            }
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx,
                            Collector<MetricResult> out) throws Exception {

        }
    }

    private static final Time CFR_ALLOWED_LATENESS = Time.hours(5);

    public static final OutputTag<CicdEvent> CFR_TRULY_LATE_TAG =
            new OutputTag<CicdEvent>("cfr-truly-late-events") {};

    public static SingleOutputStreamOperator<MetricResult> changeFailureRate(
            DataStream<CicdEvent> events, Time windowSize) {

        return events
                .filter(e -> "DEPLOY_SUCCESS".equals(e.getEventType())
                          || "DEPLOY_FAILED".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .window(TumblingEventTimeWindows.of(windowSize))
                .allowedLateness(CFR_ALLOWED_LATENESS)
                .sideOutputLateData(CFR_TRULY_LATE_TAG)
                .aggregate(new CfrAgg(), new CfrWindowFn());
    }

    static class CfrAcc {
        long total = 0, failed = 0;
        String serviceName = "";

        long minFlinkReceivedAtMs = 0;
    }

    static class CfrAgg implements AggregateFunction<CicdEvent, CfrAcc, CfrAcc> {
        @Override public CfrAcc createAccumulator() { return new CfrAcc(); }

        @Override
        public CfrAcc add(CicdEvent e, CfrAcc acc) {
            acc.total++;
            if ("DEPLOY_FAILED".equals(e.getEventType()) || e.isFailure()) acc.failed++;
            acc.serviceName = e.getServiceName();
            acc.minFlinkReceivedAtMs = SourceTiming.earliest(acc.minFlinkReceivedAtMs, e.getFlinkReceivedAtMs());
            return acc;
        }

        @Override public CfrAcc getResult(CfrAcc acc) { return acc; }

        @Override
        public CfrAcc merge(CfrAcc a, CfrAcc b) {
            a.total  += b.total;
            a.failed += b.failed;
            a.minFlinkReceivedAtMs = SourceTiming.earliest(a.minFlinkReceivedAtMs, b.minFlinkReceivedAtMs);
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
            MetricResult r = new MetricResult(
                    MetricResult.MetricType.CHANGE_FAILURE_RATE,
                    pipelineId, acc.serviceName,
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getStart()), ZoneOffset.UTC).toString(),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getEnd()), ZoneOffset.UTC).toString(), cfr, acc.total);
            r.setFlinkReceivedAtMs(acc.minFlinkReceivedAtMs);
            out.collect(r);
        }
    }

    private static final String CFR_LIVE_WINDOW_MARKER =
            LocalDateTime.of(1970, 1, 1, 0, 0, 0).toString();

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
        private transient ValueState<Long> lastFlinkReceivedAtMs;

        LiveCfrCounter(long windowMs) { this.windowMs = windowMs; }

        @Override
        public void open(Configuration cfg) {
            total     = getRuntimeContext().getState(new ValueStateDescriptor<>("live-cfr-total",  Types.LONG));
            failed    = getRuntimeContext().getState(new ValueStateDescriptor<>("live-cfr-failed", Types.LONG));
            windowEnd = getRuntimeContext().getState(new ValueStateDescriptor<>("live-cfr-window-end", Types.LONG));
            serviceName = getRuntimeContext().getState(new ValueStateDescriptor<>("live-cfr-service-name", Types.STRING));
            lastFlinkReceivedAtMs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("live-cfr-flink-received-at", Types.LONG));
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
                total.clear();
                failed.clear();
                if (currentWindowEnd != null) {
                    ctx.timerService().deleteEventTimeTimer(currentWindowEnd - 1);
                }
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
            lastFlinkReceivedAtMs.update(event.getFlinkReceivedAtMs());

            MetricResult r = new MetricResult(
                    MetricResult.MetricType.CHANGE_FAILURE_RATE_LIVE,
                    event.getPipelineId(), event.getServiceName(),
                    CFR_LIVE_WINDOW_MARKER, CFR_LIVE_WINDOW_MARKER,
                    cfr, totalCount);
            r.setFlinkReceivedAtMs(event.getFlinkReceivedAtMs());
            out.collect(r);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricResult> out) throws Exception {

            Long currentWindowEnd = windowEnd.value();
            if (currentWindowEnd == null || timestamp != currentWindowEnd - 1) {
                return;
            }

            total.clear();
            failed.clear();
            out.collect(new MetricResult(
                    MetricResult.MetricType.CHANGE_FAILURE_RATE_LIVE,
                    ctx.getCurrentKey(), serviceName.value(),
                    CFR_LIVE_WINDOW_MARKER, CFR_LIVE_WINDOW_MARKER,
                    0.0, 0));
        }
    }

    public static DataStream<MetricResult> mttr(DataStream<CicdEvent> events) {
        return events
                .filter(e -> "BUILD_FAILED".equals(e.getEventType())
                          || "BUILD_SUCCESS".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .process(new MttrProcessFn());
    }

    static class MttrProcessFn
            extends KeyedProcessFunction<String, CicdEvent, MetricResult> {

        private transient ValueState<Long> failureStartMs;

        private transient ValueState<Long> failureStartFlinkReceivedAtMs;

        @Override
        public void open(Configuration cfg) {
            failureStartMs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("failure-start-ms", Types.LONG));
            failureStartFlinkReceivedAtMs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("failure-start-flink-received-at-ms", Types.LONG));
        }

        @Override
        public void processElement(CicdEvent e, Context ctx,
                                   Collector<MetricResult> out) throws Exception {
            if ("BUILD_FAILED".equals(e.getEventType())) {

                failureStartMs.update(e.getTimestampMs());
                failureStartFlinkReceivedAtMs.update(e.getFlinkReceivedAtMs());

            } else if ("BUILD_SUCCESS".equals(e.getEventType())) {
                Long startMs = failureStartMs.value();
                if (startMs != null) {
                    double mttrMins = (e.getTimestampMs() - startMs) / 60_000.0;
                    Long startFlinkReceivedAt = failureStartFlinkReceivedAtMs.value();
                    failureStartMs.clear();
                    failureStartFlinkReceivedAtMs.clear();

                    MetricResult r = new MetricResult(
                            MetricResult.MetricType.MEAN_TIME_TO_RECOVERY,
                            e.getPipelineId(), e.getServiceName(),
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(startMs), ZoneOffset.UTC).toString(),
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(e.getTimestampMs()), ZoneOffset.UTC).toString(),
                            mttrMins, 1);

                    r.setFlinkReceivedAtMs(startFlinkReceivedAt != null ? startFlinkReceivedAt : 0);
                    out.collect(r);
                }
            }
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx,
                            Collector<MetricResult> out) {}
    }
}
