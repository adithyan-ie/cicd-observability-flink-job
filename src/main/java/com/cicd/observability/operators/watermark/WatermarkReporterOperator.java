package com.cicd.observability.operators.watermark;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

public class WatermarkReporterOperator {

    private static final String GLOBAL_PIPELINE_ID = "GLOBAL";
    private static final String WINDOW_MARKER = LocalDateTime.of(1970, 1, 1, 0, 0, 0).toString();

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
