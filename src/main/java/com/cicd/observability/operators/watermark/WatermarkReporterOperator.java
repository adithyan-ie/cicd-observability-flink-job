package com.cicd.observability.operators.watermark;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
 * "GLOBAL", sentinel window) whenever the watermark advances — checked both
 * on every incoming element AND on a recurring processing-time timer (see
 * WatermarkReporterFn). The timer is required: this operator otherwise only
 * gets a chance to notice the watermark moving when a new CicdEvent happens
 * to flow through it, so during any gap between events (or once the source
 * backlog is fully drained) the reported value — and the "Current Watermark"
 * dashboard panel — would sit frozen at whatever it was last time an element
 * arrived, even though the watermark had actually kept advancing underneath.
 *
 * Keyed on a constant string rather than left unkeyed: Flink only allows
 * registerProcessingTimeTimer on keyed streams. Every element still hashes
 * to the same key, so — combined with parallelism 1 below — this remains a
 * single instance receiving every element, same as before.
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

    // How often (processing time) to re-check the watermark even if no new
    // element has arrived — see class javadoc.
    private static final long POLL_INTERVAL_MS = 10_000;

    public static DataStream<MetricResult> report(DataStream<CicdEvent> events) {
        return events
                .keyBy(e -> GLOBAL_PIPELINE_ID)
                .process(new WatermarkReporterFn())
                .name("watermark-reporter")
                .setParallelism(1);
    }

    static class WatermarkReporterFn extends KeyedProcessFunction<String, CicdEvent, MetricResult> {

        private transient long lastReportedWatermark;
        private transient boolean pollTimerScheduled;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            lastReportedWatermark = Long.MIN_VALUE;
            pollTimerScheduled = false;
        }

        @Override
        public void processElement(CicdEvent event, Context ctx, Collector<MetricResult> out) {
            maybeReport(ctx.timerService().currentWatermark(), out);
            scheduleNextPoll(ctx.timerService());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricResult> out) {
            maybeReport(ctx.timerService().currentWatermark(), out);
            ctx.timerService().registerProcessingTimeTimer(timestamp + POLL_INTERVAL_MS);
        }

        private void scheduleNextPoll(TimerService timerService) {
            if (pollTimerScheduled) {
                return;
            }
            pollTimerScheduled = true;
            timerService.registerProcessingTimeTimer(
                    timerService.currentProcessingTime() + POLL_INTERVAL_MS);
        }

        private void maybeReport(long watermark, Collector<MetricResult> out) {
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
