package com.cicd.observability.operators.health;

import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

/**
 * Pipeline Health Score
 *
 * Computes a 0–100 composite score per pipeline using a 7-day sliding window
 * (slides every 1 minute so dashboards update frequently).
 *
 * Score formula:
 *   buildSuccessRate  (weight 0.40) × 100
 *   testSuccessRate   (weight 0.30) × 100
 *   sonarPassRate     (weight 0.20) × 100
 *   packageSuccessRate(weight 0.10) × 100
 *
 * Flink features used:
 *   - SlidingEventTimeWindows  — overlapping windows for near-real-time updates
 *   - AggregateFunction        — incremental per-event counting
 *   - ProcessWindowFunction    — final score calculation with window metadata
 */
public class PipelineHealthOperator {

    public static DataStream<MetricResult> compute(DataStream<CicdEvent> events) {
        return events
                .keyBy(CicdEvent::getPipelineId)
                // 7-day window sliding every 1 minute — frequent dashboard updates
                .window(SlidingEventTimeWindows.of(Time.days(7), Time.minutes(1)))
                .aggregate(new HealthAgg(), new HealthWindowFn());
    }

    // ── Accumulator ────────────────────────────────────────────────────

    static class HealthAcc {
        // Build
        long buildTotal = 0, buildSuccess = 0;
        // Test
        long testTotal  = 0, testSuccess  = 0;
        // SonarQube
        long sonarTotal = 0, sonarSuccess = 0;
        // Package
        long pkgTotal   = 0, pkgSuccess   = 0;
        String serviceName = "";
    }

    // ── AggregateFunction ──────────────────────────────────────────────

    static class HealthAgg
            implements AggregateFunction<CicdEvent, HealthAcc, HealthAcc> {

        @Override public HealthAcc createAccumulator() { return new HealthAcc(); }

        @Override
        public HealthAcc add(CicdEvent e, HealthAcc acc) {
            acc.serviceName = e.getServiceName();
            String et = e.getEventType();

            if (et != null) {
                if (et.startsWith("BUILD_")) {
                    acc.buildTotal++;
                    if (e.isSuccess()) acc.buildSuccess++;
                } else if (et.startsWith("TEST_")) {
                    acc.testTotal++;
                    if (e.isSuccess()) acc.testSuccess++;
                } else if (et.startsWith("SONARQUBE_")) {
                    acc.sonarTotal++;
                    if (e.isSuccess()) acc.sonarSuccess++;
                } else if (et.startsWith("PACKAGE_")) {
                    acc.pkgTotal++;
                    if (e.isSuccess()) acc.pkgSuccess++;
                }
            }
            return acc;
        }

        @Override public HealthAcc getResult(HealthAcc acc) { return acc; }

        @Override
        public HealthAcc merge(HealthAcc a, HealthAcc b) {
            a.buildTotal   += b.buildTotal;   a.buildSuccess  += b.buildSuccess;
            a.testTotal    += b.testTotal;    a.testSuccess   += b.testSuccess;
            a.sonarTotal   += b.sonarTotal;   a.sonarSuccess  += b.sonarSuccess;
            a.pkgTotal     += b.pkgTotal;     a.pkgSuccess    += b.pkgSuccess;
            return a;
        }
    }

    // ── ProcessWindowFunction ──────────────────────────────────────────

    static class HealthWindowFn
            extends ProcessWindowFunction<HealthAcc, MetricResult, String, TimeWindow> {

        @Override
        public void process(String pipelineId, Context ctx,
                            Iterable<HealthAcc> elems, Collector<MetricResult> out) {
            HealthAcc acc = elems.iterator().next();

            double buildRate = rate(acc.buildSuccess, acc.buildTotal);
            double testRate  = rate(acc.testSuccess,  acc.testTotal);
            double sonarRate = rate(acc.sonarSuccess, acc.sonarTotal);
            double pkgRate   = rate(acc.pkgSuccess,   acc.pkgTotal);

            // Weighted composite score
            double score = (buildRate * 0.40)
                         + (testRate  * 0.30)
                         + (sonarRate * 0.20)
                         + (pkgRate   * 0.10);

            long totalEvents = acc.buildTotal + acc.testTotal
                             + acc.sonarTotal + acc.pkgTotal;

            MetricResult r = new MetricResult(
                    MetricResult.MetricType.PIPELINE_HEALTH_SCORE,
                    pipelineId, acc.serviceName,
                    ctx.window().getStart(), ctx.window().getEnd(),
                    score, totalEvents);

            // Attach breakdown as detail JSON for Grafana
            r.setDetail(String.format(
                    "{\"build\":%.1f,\"test\":%.1f,\"sonar\":%.1f,\"package\":%.1f}",
                    buildRate, testRate, sonarRate, pkgRate));
            out.collect(r);
        }

        private double rate(long success, long total) {
            return total == 0 ? 100.0 : (success * 100.0 / total);
        }
    }
}
