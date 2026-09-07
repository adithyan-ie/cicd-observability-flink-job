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

public class LateEventOperator {

    private static final Time AUDIT_WINDOW = Time.minutes(10);

    public static DataStream<MetricResult> auditTrulyLate(
            DataStream<CicdEvent> trulyLateEvents,
            MetricResult.MetricType lateMetricType) {

        return trulyLateEvents
                .keyBy(CicdEvent::getPipelineId)
                .window(TumblingProcessingTimeWindows.of(AUDIT_WINDOW))
                .aggregate(new StageCountAgg(), new TrulyLateCountWindowFn(lateMetricType));
    }

    static class StageAcc {
        int  total = 0, failures = 0;
        String serviceName = "";
    }

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
