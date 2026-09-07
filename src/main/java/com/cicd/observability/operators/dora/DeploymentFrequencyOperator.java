package com.cicd.observability.operators.dora;

import com.cicd.observability.config.FlinkConfig;
import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.cicd.observability.operators.SourceTiming;
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

public class DeploymentFrequencyOperator {

    public static final OutputTag<CicdEvent> TRULY_LATE_TAG =
            new OutputTag<CicdEvent>("dora-truly-late-events") {};

    private static final Time ALLOWED_LATENESS = Time.hours(5);

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

    public static class DeployCount {
        long count = 0;
        String serviceName = "";

        long minFlinkReceivedAtMs = 0;
    }

    static class DeployCountAgg
            implements AggregateFunction<CicdEvent, DeployCount, DeployCount> {

        @Override public DeployCount createAccumulator() { return new DeployCount(); }

        @Override
        public DeployCount add(CicdEvent e, DeployCount acc) {
            acc.count++;
            acc.serviceName = e.getServiceName();
            acc.minFlinkReceivedAtMs = SourceTiming.earliest(acc.minFlinkReceivedAtMs, e.getFlinkReceivedAtMs());
            return acc;
        }

        @Override public DeployCount getResult(DeployCount acc) { return acc; }

        @Override
        public DeployCount merge(DeployCount a, DeployCount b) {
            a.count += b.count;
            a.minFlinkReceivedAtMs = SourceTiming.earliest(a.minFlinkReceivedAtMs, b.minFlinkReceivedAtMs);
            return a;
        }
    }

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
            r.setFlinkReceivedAtMs(acc.minFlinkReceivedAtMs);
            out.collect(r);
        }
    }

    private static final String LIVE_WINDOW_MARKER = LocalDateTime.of(1970, 1, 1, 0, 0, 0).toString();

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
        private transient ValueState<String> serviceName;
        private transient ValueState<Long> lastFlinkReceivedAtMs;

        LiveDeployCounter(long windowMs) { this.windowMs = windowMs; }

        @Override
        public void open(Configuration parameters) {
            counter = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("live-deploy-count", Long.class));
            windowEnd = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("live-deploy-window-end", Long.class));
            serviceName = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("live-deploy-service-name", String.class));
            lastFlinkReceivedAtMs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("live-deploy-flink-received-at", Long.class));
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

                counter.clear();
                if (currentWindowEnd != null) {
                    ctx.timerService().deleteEventTimeTimer(currentWindowEnd - 1);
                }
                long nextWindowEnd = ((ts / windowMs) + 1) * windowMs;
                windowEnd.update(nextWindowEnd);
                ctx.timerService().registerEventTimeTimer(nextWindowEnd - 1);
            }

            long count = (counter.value() == null ? 0L : counter.value()) + 1;
            counter.update(count);
            serviceName.update(event.getServiceName());
            lastFlinkReceivedAtMs.update(event.getFlinkReceivedAtMs());

            MetricResult r = new MetricResult(
                    MetricResult.MetricType.DEPLOYMENT_FREQUENCY_LIVE,
                    event.getPipelineId(), event.getServiceName(),
                    LIVE_WINDOW_MARKER, LIVE_WINDOW_MARKER,
                    count, count);
            r.setFlinkReceivedAtMs(event.getFlinkReceivedAtMs());
            out.collect(r);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricResult> out) throws Exception {

            Long currentWindowEnd = windowEnd.value();
            if (currentWindowEnd == null || timestamp != currentWindowEnd - 1) {
                return;
            }

            counter.clear();
            out.collect(new MetricResult(
                    MetricResult.MetricType.DEPLOYMENT_FREQUENCY_LIVE,
                    ctx.getCurrentKey(), serviceName.value(),
                    LIVE_WINDOW_MARKER, LIVE_WINDOW_MARKER,
                    0, 0));
        }
    }
}
