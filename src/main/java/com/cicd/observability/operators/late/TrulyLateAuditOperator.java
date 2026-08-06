package com.cicd.observability.operators.late;

import com.cicd.observability.model.CicdEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * Shared truly-late audit trail — turns any windowed operator's
 * TRULY_LATE_TAG side output into a JSON record, tagged with which metric's
 * window it missed. Reused across DeploymentFrequencyOperator,
 * DoraOperators.changeFailureRate, and PipelineHealthOperator so all three
 * land in the same Postgres LATE_EVENT audit trail — one generic "unable to
 * process as truly late" panel, distinguishing rows by `source_metric`
 * rather than needing a separate panel per metric.
 */
public class TrulyLateAuditOperator {

    /**
     * @param trulyLateEvents a metric operator's TRULY_LATE_TAG side output
     * @param sourceMetric    identifies which metric's window this event
     *                        missed (e.g. "DEPLOYMENT_FREQUENCY",
     *                        "CHANGE_FAILURE_RATE", "PIPELINE_HEALTH_SCORE")
     */
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
