package com.cicd.observability.operators.late;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Late Event Processing
 *
 * Flink features used (that the Python version was NOT using):
 *
 *   1. OutputTag<CicdEvent>
 *      Typed side output tag — late events are routed here instead of dropped.
 *
 *   2. .allowedLateness(Time)
 *      Events arriving after the window closes but within this grace period
 *      are still included in the window (which fires again as an update).
 *      Events arriving after this grace period are truly late and go to
 *      the LATE_TAG side output.
 *
 *   3. .sideOutputLateData(LATE_TAG)
 *      Registers the side output — Flink routes truly late events here.
 *      The main stream and side output are independent DataStreams that
 *      can be sunk to different Kafka topics.
 *
 *   4. .getSideOutput(LATE_TAG)
 *      Retrieves the late-event stream from the operator for sinking.
 *
 * The WatermarkStrategy (configured in FlinkConfig) drives all of this:
 * forBoundedOutOfOrderness(30s) means events up to 30s late are not late.
 * allowedLateness(30s) adds another 30s grace after the window closes.
 * Anything beyond that goes to the side output.
 */
public class LateEventOperator {

    /**
     * Side output tag for truly late events.
     * Typed as CicdEvent so the late stream is fully typed and sinkable.
     */
    public static final OutputTag<CicdEvent> LATE_TAG =
            new OutputTag<CicdEvent>("late-cicd-events") {};

    /**
     * @return  operator result — call .getSideOutput(LATE_TAG) on the
     *          returned stream to get the late events side stream.
     */
    public static SingleOutputStreamOperator<MetricResult> compute(
            DataStream<CicdEvent> events) {

        return events
                .keyBy(CicdEvent::getPipelineId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // Events arriving within 30s after window close still counted
                .allowedLateness(Time.seconds(30))
                // Events beyond that → LATE_TAG side output (not dropped)
                .sideOutputLateData(LATE_TAG)
                .aggregate(new StageCountAgg(), new StageCountWindowFn());
    }

    // ── Accumulator ────────────────────────────────────────────────────

    static class StageAcc {
        int  total = 0, failures = 0;
        long windowStart = 0;
        String serviceName = "";
    }

    // ── AggregateFunction ──────────────────────────────────────────────

    static class StageCountAgg
            implements AggregateFunction<CicdEvent, StageAcc, StageAcc> {

        @Override public StageAcc createAccumulator() { return new StageAcc(); }

        @Override
        public StageAcc add(CicdEvent e, StageAcc acc) {
            acc.total++;
            if (e.isFailure()) acc.failures++;
            acc.serviceName = e.getServiceName();
            return acc;
        }

        @Override public StageAcc getResult(StageAcc acc) { return acc; }

        @Override
        public StageAcc merge(StageAcc a, StageAcc b) {
            a.total    += b.total;
            a.failures += b.failures;
            return a;
        }
    }

    // ── ProcessWindowFunction ──────────────────────────────────────────

    static class StageCountWindowFn
            extends ProcessWindowFunction<StageAcc, MetricResult, String, TimeWindow> {

        @Override
        public void process(String pipelineId, Context ctx,
                            Iterable<StageAcc> elems, Collector<MetricResult> out) {
            StageAcc acc = elems.iterator().next();

            MetricResult r = new MetricResult(
                    MetricResult.MetricType.LATE_EVENT_CORRECTED,
                    pipelineId, acc.serviceName,
                    ctx.window().getStart(), ctx.window().getEnd(),
                    acc.total, acc.total);
            r.setDetail("{\"failures\":" + acc.failures + ",\"total\":" + acc.total + "}");
            out.collect(r);
        }
    }
}
