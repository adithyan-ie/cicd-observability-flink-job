package com.cicd.observability.operators.dora;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

/**
 * DORA Metric #1 — Deployment Frequency
 *
 * Flink features used:
 *   - filter()                     → keep only DEPLOY_SUCCESS events
 *   - keyBy(pipelineId)            → one state machine per pipeline
 *   - TumblingEventTimeWindows(1d) → count per day, driven by watermark
 *   - AggregateFunction            → incremental count (memory-efficient)
 *   - ProcessWindowFunction        → access window metadata (start/end ms)
 */
public class DeploymentFrequencyOperator {

    public static DataStream<MetricResult> compute(
            DataStream<CicdEvent> events, Time windowSize) {

        double windowDays = windowSize.toMilliseconds() / (double)(86_400_000L);

        return events
                .filter(e -> "DEPLOY_SUCCESS".equals(e.getEventType()))
                .keyBy(CicdEvent::getPipelineId)
                .window(TumblingEventTimeWindows.of(windowSize))
                .aggregate(new DeployCountAgg(), new DeployFreqWindowFn(windowDays));
    }

    // ── Accumulator ────────────────────────────────────────────────────

    static class DeployCount {
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
            double deploysPerDay = acc.count / windowDays;

            MetricResult r = new MetricResult(
                    MetricResult.MetricType.DEPLOYMENT_FREQUENCY,
                    pipelineId, acc.serviceName,
                    ctx.window().getStart(), ctx.window().getEnd(),
                    deploysPerDay, acc.count);
            out.collect(r);
        }
    }
}
