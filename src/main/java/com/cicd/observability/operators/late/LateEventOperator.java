package com.cicd.observability.operators.late;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Late Event Auditing
 *
 * This operator no longer corrects any metric. Each windowed DORA/health
 * operator (DeploymentFrequencyOperator, PipelineHealthOperator,
 * DoraOperators.changeFailureRate) now owns its own .allowedLateness() /
 * .sideOutputLateData() and republishes an updated window itself when a
 * late event arrives within the grace period.
 *
 * What lands here is only the "truly late" side output of those operators —
 * events that missed even the grace period, so they are permanently absent
 * from the metric. This operator's only job is to count how many of those
 * real truly-late events arrive, per pipeline, per processing-time window —
 * an audit signal, not a correction.
 *
 * Processing-time windows (not event-time) are used deliberately: by
 * definition these events' timestamps are already behind the watermark,
 * so an event-time window would just mark them late again and drop them.
 *
 * The emitted MetricResult uses `lateMetricType` directly as its own
 * metric_type (e.g. DEPLOYMENT_FREQUENCY_LATE_EVENTS) rather than a single
 * generic type plus a "which metric" column — metric_type alone then keeps
 * these rows from colliding with each other or with the real metric on the
 * existing Postgres unique index, no schema change required.
 */
public class LateEventOperator {

    private static final Time AUDIT_WINDOW = Time.minutes(10);

    /**
     * @param trulyLateEvents a metric operator's TRULY_LATE_TAG side output
     * @param lateMetricType  the specific *_LATE_EVENTS type to emit —
     *                        identifies which metric's window these missed
     */
    public static DataStream<MetricResult> auditTrulyLate(
            DataStream<CicdEvent> trulyLateEvents,
            MetricResult.MetricType lateMetricType) {

        return trulyLateEvents
                .keyBy(CicdEvent::getPipelineId)
                .window(TumblingProcessingTimeWindows.of(AUDIT_WINDOW))
                .aggregate(new StageCountAgg(), new TrulyLateCountWindowFn(lateMetricType));
    }

    // ── Accumulator ────────────────────────────────────────────────────

    static class StageAcc {
        int  total = 0, failures = 0;
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

    static class TrulyLateCountWindowFn
            extends ProcessWindowFunction<StageAcc, MetricResult, String, TimeWindow> {

        private final MetricResult.MetricType lateMetricType;

        TrulyLateCountWindowFn(MetricResult.MetricType lateMetricType) {
            this.lateMetricType = lateMetricType;
        }

        @Override
        public void process(String pipelineId, Context ctx,
                            Iterable<StageAcc> elems, Collector<MetricResult> out) {
            StageAcc acc = elems.iterator().next();

            MetricResult r = new MetricResult(
                    lateMetricType,
                    pipelineId, acc.serviceName,
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getStart()), ZoneOffset.UTC).toString(),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.window().getEnd()), ZoneOffset.UTC).toString(),
                    acc.total, acc.total);
            out.collect(r);
        }
    }
}
