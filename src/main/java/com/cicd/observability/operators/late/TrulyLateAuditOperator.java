package com.cicd.observability.operators.late;

import com.cicd.observability.model.CicdEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class TrulyLateAuditOperator {

    public static SingleOutputStreamOperator<String> auditTrulyLate(
            DataStream<CicdEvent> trulyLateEvents, String sourceMetric) {

        return trulyLateEvents
                .keyBy(CicdEvent::getPipelineId)
                .process(new TrulyLateAuditFn(sourceMetric))
                .name("format-truly-late-" + sourceMetric.toLowerCase());
    }

    static class TrulyLateAuditFn extends KeyedProcessFunction<String, CicdEvent, String> {

        private final String sourceMetric;

        TrulyLateAuditFn(String sourceMetric) {
            this.sourceMetric = sourceMetric;
        }

        @Override
        public void processElement(CicdEvent e, Context ctx, Collector<String> out) {
            long watermarkMs = ctx.timerService().currentWatermark();
            String eventTime = Instant.ofEpochMilli(e.getTimestampMs()).toString();
            String watermarkTime = watermarkMs == Long.MIN_VALUE
                    ? null
                    : Instant.ofEpochMilli(watermarkMs).toString();

            out.collect(String.format(
                    "{\"source_metric\":\"%s\",\"event_id\":\"%s\",\"pipeline_id\":\"%s\",\"service_name\":\"%s\","
                    + "\"event_type\":\"%s\",\"event_time\":\"%s\",\"watermark_time\":%s,\"late\":true}",
                    sourceMetric, e.getEventId(), e.getPipelineId(), e.getServiceName(), e.getEventType(),
                    eventTime, watermarkTime == null ? "null" : "\"" + watermarkTime + "\""));
        }
    }
}
