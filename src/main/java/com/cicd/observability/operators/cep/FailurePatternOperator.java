package com.cicd.observability.operators.cep;

import com.cicd.observability.model.CicdEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Failure Pattern Discovery using Flink CEP (Complex Event Processing).
 *
 * Flink CEP replaces the Python hand-rolled FSM with a proper NFA
 * (Non-deterministic Finite Automaton) that runs distributed across
 * Flink task managers with managed state and fault tolerance.
 *
 * The pattern detected:
 *   DEPENDENCY_UPDATED → UNIT_TEST_FAILED → RETRY_TRIGGERED
 *       → INTEGRATION_TEST_FAILED → ROLLBACK_TRIGGERED
 *   All within 10 minutes for the same pipeline.
 *
 * Flink CEP features used:
 *   Pattern.begin()         → anchor the first event
 *   .followedBy()           → relaxed contiguity (other events can appear between)
 *   .where(SimpleCondition) → filter condition on each pattern step
 *   .within(Time)           → time constraint — pattern must complete within N minutes
 *   CEP.pattern()           → wraps KeyedStream with Flink's NFA engine
 *   PatternStream.select()  → fires when complete match found
 *   PatternTimeoutFunction  → fires when pattern times out (incomplete match)
 *   OutputTag               → routes timeout alerts to a separate stream
 */
public class FailurePatternOperator {

    private static final Logger LOG = LoggerFactory.getLogger(FailurePatternOperator.class);

    /** Side output for partial pattern matches that timed out. */
    public static final OutputTag<String> TIMEOUT_TAG =
            new OutputTag<String>("pattern-timeout") {};

    // ── CEP Pattern definition ─────────────────────────────────────────

    /**
     * Defines the cascading failure sequence using Flink CEP Pattern API.
     *
     * .followedBy() uses RELAXED contiguity — other events may appear
     * between pattern steps.  This is correct for CI/CD because pipelines
     * emit many intermediate events (logs, metrics, heartbeats) between
     * the failure events we care about.
     *
     * .within() is the critical time constraint.  Without it, a pattern
     * could span days.  Flink CEP's NFA will fire PatternTimeoutFunction
     * for any partially-matched pattern that does not complete within this
     * window, giving early-warning alerts.
     */
    public static Pattern<CicdEvent, ?> buildPattern() {
        return Pattern.<CicdEvent>begin("dependency_update")
                .where(new SimpleCondition<CicdEvent>() {
                    @Override
                    public boolean filter(CicdEvent e) {
                        return "DEPENDENCY_UPDATED".equals(e.getEventType());
                    }
                })

                .followedBy("unit_test_failure")
                .where(new SimpleCondition<CicdEvent>() {
                    @Override
                    public boolean filter(CicdEvent e) {
                        return "UNIT_TEST_FAILED".equals(e.getEventType());
                    }
                })

                .followedBy("retry")
                .where(new SimpleCondition<CicdEvent>() {
                    @Override
                    public boolean filter(CicdEvent e) {
                        return "RETRY_TRIGGERED".equals(e.getEventType());
                    }
                })

                .followedBy("integration_test_failure")
                .where(new SimpleCondition<CicdEvent>() {
                    @Override
                    public boolean filter(CicdEvent e) {
                        return "INTEGRATION_TEST_FAILED".equals(e.getEventType());
                    }
                })

                .followedBy("rollback")
                .where(new SimpleCondition<CicdEvent>() {
                    @Override
                    public boolean filter(CicdEvent e) {
                        return "ROLLBACK_TRIGGERED".equals(e.getEventType());
                    }
                })

                // The entire sequence must complete within 10 minutes.
                // If not, PatternTimeoutFunction fires for the partial match.
                .within(Time.minutes(10));
    }

    // ── Apply CEP to KeyedStream ───────────────────────────────────────

    /**
     * Wraps the keyed event stream with Flink's NFA engine.
     *
     * The stream MUST be keyed by pipeline_id before calling this method.
     * CEP maintains one NFA state machine per key, distributed across
     * Flink task managers. This is what the Python FSM loop was trying to
     * do — but without distribution, state management, or fault tolerance.
     *
     * @return main stream of complete pattern match alerts (JSON strings)
     *         Call .getSideOutput(TIMEOUT_TAG) for partial timeout alerts.
     */
    public static SingleOutputStreamOperator<String> detect(
            DataStream<CicdEvent> keyedByPipeline) {

        Pattern<CicdEvent, ?> pattern = buildPattern();

        // CEP.pattern() registers the NFA with the Flink runtime.
        // Each Flink subtask maintains NFA state for the pipelines assigned to it.
        PatternStream<CicdEvent> patternStream =
                CEP.pattern(keyedByPipeline, pattern);

        // select() fires PatternSelectFunction for complete matches
        // and PatternTimeoutFunction for timed-out partial matches.
        return patternStream.select(
                TIMEOUT_TAG,                        // side output for timeouts
                new FailurePatternTimeoutFn(),       // partial match handler
                new FailurePatternSelectFn()         // complete match handler
        );
    }

    // ── Complete match handler ─────────────────────────────────────────

    static class FailurePatternSelectFn
            implements PatternSelectFunction<CicdEvent, String> {

        private transient ObjectMapper mapper;

        /**
         * Called exactly once per complete pattern match.
         *
         * patternMap keys correspond to the step names in buildPattern().
         * Each value is a List<CicdEvent> (usually one element per step,
         * unless .oneOrMore() or .times() is used in the pattern).
         */
        @Override
        public String select(Map<String, List<CicdEvent>> patternMap) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();

            CicdEvent dep  = patternMap.get("dependency_update").get(0);
            CicdEvent unit = patternMap.get("unit_test_failure").get(0);
            CicdEvent retry= patternMap.get("retry").get(0);
            CicdEvent integ= patternMap.get("integration_test_failure").get(0);
            CicdEvent rb   = patternMap.get("rollback").get(0);

            Map<String, Object> alert = new HashMap<>();
            alert.put("alert_type",       "FAILURE_PATTERN_DETECTED");
            alert.put("pattern_name",     "Dependency Update Failure Cascade");
            alert.put("pipeline_id",      dep.getPipelineId());
            alert.put("service_name",     dep.getServiceName());
            alert.put("branch",           dep.getBranch());
            alert.put("commit_sha",       dep.getCommitSha());
            alert.put("pattern_start_ts", dep.getEventTimestamp());
            alert.put("pattern_end_ts",   rb.getEventTimestamp());

            // Duration of the full cascade
            long durationMs = rb.getTimestampMs() - dep.getTimestampMs();
            alert.put("cascade_duration_minutes", durationMs / 60_000.0);

            // Sequence detail
            alert.put("matched_sequence", List.of(
                    stepDetail("dependency_update",        dep),
                    stepDetail("unit_test_failure",        unit),
                    stepDetail("retry",                    retry),
                    stepDetail("integration_test_failure", integ),
                    stepDetail("rollback",                 rb)
            ));

            alert.put("dora_impact", Map.of(
                    "change_failure_rate", "INCREASED",
                    "mttr_clock_started",  rb.getEventTimestamp()
            ));

            LOG.warn("🔴 FAILURE PATTERN MATCH: pipeline={} service={}",
                    dep.getPipelineId(), dep.getServiceName());

            return mapper.writeValueAsString(alert);
        }

        private Map<String, String> stepDetail(String step, CicdEvent e) {
            Map<String, String> m = new HashMap<>();
            m.put("step",  step);
            m.put("event", e.getEventType());
            m.put("ts",    e.getEventTimestamp());
            return m;
        }
    }

    // ── Partial match / timeout handler ───────────────────────────────

    static class FailurePatternTimeoutFn
            implements PatternTimeoutFunction<CicdEvent, String> {

        private transient ObjectMapper mapper;

        /**
         * Called when the pattern window (10 min) expires before all steps
         * are matched.  partialPatternMap contains the steps that DID match.
         *
         * This is an early-warning mechanism — the pipeline started the
         * failure cascade but hasn't completed it yet (or recovered mid-way).
         */
        @Override
        public String timeout(Map<String, List<CicdEvent>> partialPatternMap,
                              long timeoutTimestamp) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();

            CicdEvent first = partialPatternMap.values().iterator().next().get(0);
            List<String> matched = List.copyOf(partialPatternMap.keySet());

            Map<String, Object> timeout = new HashMap<>();
            timeout.put("alert_type",      "PATTERN_TIMEOUT");
            timeout.put("description",     "Failure pattern started but did not complete within 10 min window");
            timeout.put("pipeline_id",     first.getPipelineId());
            timeout.put("service_name",    first.getServiceName());
            timeout.put("matched_steps",   matched);
            timeout.put("expired_at_ms",   timeoutTimestamp);

            LOG.info("⚠️ Pattern timeout: pipeline={} matched steps={}",
                    first.getPipelineId(), matched);

            return mapper.writeValueAsString(timeout);
        }
    }
}
