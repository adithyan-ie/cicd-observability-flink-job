package com.cicd.observability.operators.watermark;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

/**
 * Reports the job's current event-time watermark as a MetricResult, so it
 * can be read off the dashboard when constructing "truly late" test events
 * (see DeploymentFrequencyOperator.ALLOWED_LATENESS / TRULY_LATE_TAG).
 *
 * Applied directly on the post-source stream (before keyBy/routing), so the
 * watermark read here is the same event-time frontier that flows into every
 * downstream windowed operator. One row is upserted in Postgres (pipeline_id
 * "GLOBAL", sentinel window) each time the watermark advances — no timers or
 * keying needed, since a new watermark can only appear on the back of an
 * incoming element.
 *
 * Pinned to parallelism 1 deliberately. With source parallelism N > 1, a
 * forward/chained reporter would get N instances each wired 1:1 to one
 * source subtask, and Flink only computes an operator's watermark as the
 * min of ITS OWN input channels — so each instance would report its own
 * subtask's local watermark, not the job-wide minimum, and whichever
 * instance last upserted the shared "GLOBAL" row would win, possibly
 * showing a watermark ahead of what the keyed windowed operators (which
 * take the min across all their inputs post-keyBy) are actually using.
 * Dropping to parallelism 1 forces Flink to fan in all N source channels
 * into this single instance, so the min-of-inputs rule gives it the true
 * global minimum for free. The extra shuffle is cheap here — this stream
 * only emits when the watermark actually advances.
 */
public class WatermarkReporterOperator {

    private static final String GLOBAL_PIPELINE_ID = "GLOBAL";
    private static final String WINDOW_MARKER = LocalDateTime.of(1970, 1, 1, 0, 0, 0).toString();

    public static DataStream<MetricResult> report(DataStream<CicdEvent> events) {
        return events
                .process(new WatermarkReporterFn())
                .name("watermark-reporter")
                .setParallelism(1);
    }

    static class WatermarkReporterFn extends ProcessFunction<CicdEvent, MetricResult> {

        private transient long lastReportedWatermark;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            lastReportedWatermark = Long.MIN_VALUE;
        }

        @Override
        public void processElement(CicdEvent event, Context ctx, Collector<MetricResult> out) {
            long watermark = ctx.timerService().currentWatermark();
            if (watermark == Long.MIN_VALUE || watermark == lastReportedWatermark) {
                return;
            }
            lastReportedWatermark = watermark;

            out.collect(new MetricResult(
                    MetricResult.MetricType.WATERMARK,
                    GLOBAL_PIPELINE_ID, "",
                    WINDOW_MARKER, WINDOW_MARKER,
                    watermark, 0));
        }
    }
}
