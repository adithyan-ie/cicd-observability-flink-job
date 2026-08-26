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
 * Deployment Failure Pattern Discovery using Flink CEP (Complex Event Processing).
 *
 * Three independent NFAs run over the same pipeline_id-keyed stream. Each
 * models a realistic CI/CD failure narrative — the individual events
 * (a single DEPLOY_FAILED) are not exceptional on their own, but the
 * *sequence* is:
 *
 *  1. Rollback cascade      — DEPLOY_FAILED, DEPLOY_FAILED, ROLLBACK_STARTED
 *                              within 10 min. Two failures that end in an
 *                              automatic rollback.
 *  2. Deployment instability — DEPLOY_FAILED, DEPLOY_STARTED, DEPLOY_FAILED
 *                              within 10 min. A retry that fails again,
 *                              with no rollback (yet).
 *  3. Build OK, deploy broken — BUILD_SUCCESS, DEPLOY_FAILED, DEPLOY_FAILED
 *                              within 15 min. The app compiles fine but
 *                              deployment keeps failing — points at
 *                              infrastructure/config rather than code.
 *
 * Flink CEP features used (per pattern):
 *   Pattern.begin()         → anchor the first event
 *   .followedBy()           → relaxed contiguity (other events can appear between)
 *   .where(SimpleCondition) → filter condition on each pattern step
 *   .within(Time)           → time constraint — pattern must complete within N minutes
 *   CEP.pattern()           → wraps KeyedStream with Flink's NFA engine
 *   PatternStream.select()  → fires when complete match found
 *   PatternTimeoutFunction  → fires when pattern times out (incomplete match)
 *   OutputTag               → routes each pattern's timeout alerts to its own side output
 */
public class FailurePatternOperator {

    private static final Logger LOG = LoggerFactory.getLogger(FailurePatternOperator.class);

    /** Side outputs for partial matches that timed out — one per pattern. */
    public static final OutputTag<String> ROLLBACK_CASCADE_TIMEOUT_TAG =
            new OutputTag<String>("rollback-cascade-timeout") {};
    public static final OutputTag<String> DEPLOY_INSTABILITY_TIMEOUT_TAG =
            new OutputTag<String>("deploy-instability-timeout") {};
    public static final OutputTag<String> BUILD_OK_DEPLOY_BROKEN_TIMEOUT_TAG =
            new OutputTag<String>("build-ok-deploy-broken-timeout") {};

    /** Shared helper — matches events of a single event_type. */
    private static SimpleCondition<CicdEvent> eventType(String type) {
        return new SimpleCondition<CicdEvent>() {
            @Override
            public boolean filter(CicdEvent e) {
                return type.equals(e.getEventType());
            }
        };
    }

    // ════════════════════════════════════════════════════════════════════
    // Pattern 1 — Rollback cascade
    //   DEPLOY_FAILED -> DEPLOY_FAILED -> ROLLBACK_STARTED, within 10 min
    // ════════════════════════════════════════════════════════════════════

    public static Pattern<CicdEvent, ?> buildRollbackCascadePattern() {
        return Pattern.<CicdEvent>begin("first_failure")
                .where(eventType("DEPLOY_FAILED"))

                .followedBy("second_failure")
                .where(eventType("DEPLOY_FAILED"))

                .followedBy("rollback")
                .where(eventType("ROLLBACK_STARTED"))

                .within(Time.minutes(10));
    }

    public static SingleOutputStreamOperator<String> detectRollbackCascade(
            DataStream<CicdEvent> keyedByPipeline) {
        PatternStream<CicdEvent> patternStream =
                CEP.pattern(keyedByPipeline, buildRollbackCascadePattern());

        return patternStream.select(
                ROLLBACK_CASCADE_TIMEOUT_TAG,
                new RollbackCascadeTimeoutFn(),
                new RollbackCascadeSelectFn()
        );
    }

    static class RollbackCascadeSelectFn
            implements PatternSelectFunction<CicdEvent, String> {

        private transient ObjectMapper mapper;

        @Override
        public String select(Map<String, List<CicdEvent>> patternMap) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();

            CicdEvent first  = patternMap.get("first_failure").get(0);
            CicdEvent second = patternMap.get("second_failure").get(0);
            CicdEvent rb     = patternMap.get("rollback").get(0);

            Map<String, Object> alert = new HashMap<>();
            alert.put("alert_type",        "DEPLOY_ROLLBACK_CASCADE");
            alert.put("pattern_name",      "Repeated Deploy Failure -> Rollback");
            alert.put("pipeline_id",       first.getPipelineId());
            alert.put("service_name",      first.getServiceName());
            alert.put("branch",            first.getBranch());
            alert.put("commit_sha",        first.getCommitSha());
            alert.put("first_failure_ts",  first.getEventTimestamp());
            alert.put("second_failure_ts", second.getEventTimestamp());
            alert.put("rollback_ts",       rb.getEventTimestamp());
            alert.put("cascade_duration_minutes",
                    (rb.getTimestampMs() - first.getTimestampMs()) / 60_000.0);
            alert.put("matched_sequence", List.of(
                    stepDetail("first_failure",  first),
                    stepDetail("second_failure", second),
                    stepDetail("rollback",       rb)
            ));

            LOG.warn("🔴 DEPLOY ROLLBACK CASCADE: pipeline={} service={}",
                    first.getPipelineId(), first.getServiceName());

            return mapper.writeValueAsString(alert);
        }
    }

    static class RollbackCascadeTimeoutFn
            implements PatternTimeoutFunction<CicdEvent, String> {

        private transient ObjectMapper mapper;

        @Override
        public String timeout(Map<String, List<CicdEvent>> partialPatternMap,
                              long timeoutTimestamp) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();

            CicdEvent first = partialPatternMap.values().iterator().next().get(0);

            Map<String, Object> timeout = new HashMap<>();
            timeout.put("alert_type",    "DEPLOY_ROLLBACK_CASCADE_TIMEOUT");
            timeout.put("description",   "Repeated deploy failure did not roll back within the 10-minute window");
            timeout.put("pipeline_id",   first.getPipelineId());
            timeout.put("service_name",  first.getServiceName());
            timeout.put("matched_steps", List.copyOf(partialPatternMap.keySet()));
            timeout.put("expired_at_ms", timeoutTimestamp);

            LOG.info("⚠️ Rollback cascade timeout: pipeline={} matched steps={}",
                    first.getPipelineId(), partialPatternMap.keySet());

            return mapper.writeValueAsString(timeout);
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // Pattern 2 — Deployment instability
    //   DEPLOY_FAILED -> DEPLOY_STARTED -> DEPLOY_FAILED, within 10 min
    // ════════════════════════════════════════════════════════════════════

    public static Pattern<CicdEvent, ?> buildDeploymentInstabilityPattern() {
        return Pattern.<CicdEvent>begin("first_failure")
                .where(eventType("DEPLOY_FAILED"))

                .followedBy("retry_deploy")
                .where(eventType("DEPLOY_STARTED"))

                .followedBy("second_failure")
                .where(eventType("DEPLOY_FAILED"))

                .within(Time.minutes(10));
    }

    public static SingleOutputStreamOperator<String> detectDeploymentInstability(
            DataStream<CicdEvent> keyedByPipeline) {
        PatternStream<CicdEvent> patternStream =
                CEP.pattern(keyedByPipeline, buildDeploymentInstabilityPattern());

        return patternStream.select(
                DEPLOY_INSTABILITY_TIMEOUT_TAG,
                new DeploymentInstabilityTimeoutFn(),
                new DeploymentInstabilitySelectFn()
        );
    }

    static class DeploymentInstabilitySelectFn
            implements PatternSelectFunction<CicdEvent, String> {

        private transient ObjectMapper mapper;

        @Override
        public String select(Map<String, List<CicdEvent>> patternMap) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();

            CicdEvent first  = patternMap.get("first_failure").get(0);
            CicdEvent retry  = patternMap.get("retry_deploy").get(0);
            CicdEvent second = patternMap.get("second_failure").get(0);

            Map<String, Object> alert = new HashMap<>();
            alert.put("alert_type",        "DEPLOY_INSTABILITY");
            alert.put("pattern_name",      "Deployment Instability (fail -> retry -> fail)");
            alert.put("pipeline_id",       first.getPipelineId());
            alert.put("service_name",      first.getServiceName());
            alert.put("branch",            first.getBranch());
            alert.put("commit_sha",        first.getCommitSha());
            alert.put("first_failure_ts",  first.getEventTimestamp());
            alert.put("retry_deploy_ts",   retry.getEventTimestamp());
            alert.put("second_failure_ts", second.getEventTimestamp());
            alert.put("failures", 2);
            alert.put("matched_sequence", List.of(
                    stepDetail("first_failure",  first),
                    stepDetail("retry_deploy",   retry),
                    stepDetail("second_failure", second)
            ));

            LOG.warn("🟠 DEPLOYMENT INSTABILITY: pipeline={} service={}",
                    first.getPipelineId(), first.getServiceName());

            return mapper.writeValueAsString(alert);
        }
    }

    static class DeploymentInstabilityTimeoutFn
            implements PatternTimeoutFunction<CicdEvent, String> {

        private transient ObjectMapper mapper;

        @Override
        public String timeout(Map<String, List<CicdEvent>> partialPatternMap,
                              long timeoutTimestamp) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();

            CicdEvent first = partialPatternMap.values().iterator().next().get(0);

            Map<String, Object> timeout = new HashMap<>();
            timeout.put("alert_type",    "DEPLOY_INSTABILITY_TIMEOUT");
            timeout.put("description",   "Deploy failed and retried but the retry's outcome did not arrive within the 10-minute window");
            timeout.put("pipeline_id",   first.getPipelineId());
            timeout.put("service_name",  first.getServiceName());
            timeout.put("matched_steps", List.copyOf(partialPatternMap.keySet()));
            timeout.put("expired_at_ms", timeoutTimestamp);

            LOG.info("⚠️ Deployment instability timeout: pipeline={} matched steps={}",
                    first.getPipelineId(), partialPatternMap.keySet());

            return mapper.writeValueAsString(timeout);
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // Pattern 3 — Build OK, deploy broken
    //   BUILD_SUCCESS -> DEPLOY_FAILED -> DEPLOY_FAILED, within 15 min
    // ════════════════════════════════════════════════════════════════════

    public static Pattern<CicdEvent, ?> buildBuildOkDeployBrokenPattern() {
        return Pattern.<CicdEvent>begin("build_ok")
                .where(eventType("BUILD_SUCCESS"))

                .followedBy("first_failure")
                .where(eventType("DEPLOY_FAILED"))

                .followedBy("second_failure")
                .where(eventType("DEPLOY_FAILED"))

                .within(Time.minutes(15));
    }

    public static SingleOutputStreamOperator<String> detectBuildOkDeployBroken(
            DataStream<CicdEvent> keyedByPipeline) {
        PatternStream<CicdEvent> patternStream =
                CEP.pattern(keyedByPipeline, buildBuildOkDeployBrokenPattern());

        return patternStream.select(
                BUILD_OK_DEPLOY_BROKEN_TIMEOUT_TAG,
                new BuildOkDeployBrokenTimeoutFn(),
                new BuildOkDeployBrokenSelectFn()
        );
    }

    static class BuildOkDeployBrokenSelectFn
            implements PatternSelectFunction<CicdEvent, String> {

        private transient ObjectMapper mapper;

        @Override
        public String select(Map<String, List<CicdEvent>> patternMap) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();

            CicdEvent buildOk = patternMap.get("build_ok").get(0);
            CicdEvent first   = patternMap.get("first_failure").get(0);
            CicdEvent second  = patternMap.get("second_failure").get(0);

            Map<String, Object> alert = new HashMap<>();
            alert.put("alert_type",         "BUILD_OK_DEPLOY_BROKEN");
            alert.put("pattern_name",       "Build Succeeded but Deploy Repeatedly Failed");
            alert.put("pipeline_id",        buildOk.getPipelineId());
            alert.put("service_name",       buildOk.getServiceName());
            alert.put("branch",             buildOk.getBranch());
            alert.put("commit_sha",         buildOk.getCommitSha());
            alert.put("build_success_ts",   buildOk.getEventTimestamp());
            alert.put("first_failure_ts",   first.getEventTimestamp());
            alert.put("second_failure_ts",  second.getEventTimestamp());
            alert.put("likely_cause",
                    "Application build succeeded — repeated deploy failures point to "
                  + "infrastructure/configuration issues rather than code");
            alert.put("matched_sequence", List.of(
                    stepDetail("build_ok",       buildOk),
                    stepDetail("first_failure",  first),
                    stepDetail("second_failure", second)
            ));

            LOG.warn("🟡 BUILD OK, DEPLOY BROKEN: pipeline={} service={}",
                    buildOk.getPipelineId(), buildOk.getServiceName());

            return mapper.writeValueAsString(alert);
        }
    }

    static class BuildOkDeployBrokenTimeoutFn
            implements PatternTimeoutFunction<CicdEvent, String> {

        private transient ObjectMapper mapper;

        @Override
        public String timeout(Map<String, List<CicdEvent>> partialPatternMap,
                              long timeoutTimestamp) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();

            CicdEvent first = partialPatternMap.values().iterator().next().get(0);

            Map<String, Object> timeout = new HashMap<>();
            timeout.put("alert_type",    "BUILD_OK_DEPLOY_BROKEN_TIMEOUT");
            timeout.put("description",   "Build succeeded but deploy failures did not repeat a second time within the 15-minute window");
            timeout.put("pipeline_id",   first.getPipelineId());
            timeout.put("service_name",  first.getServiceName());
            timeout.put("matched_steps", List.copyOf(partialPatternMap.keySet()));
            timeout.put("expired_at_ms", timeoutTimestamp);

            LOG.info("⚠️ Build-ok-deploy-broken timeout: pipeline={} matched steps={}",
                    first.getPipelineId(), partialPatternMap.keySet());

            return mapper.writeValueAsString(timeout);
        }
    }

    // ── Shared helper ───────────────────────────────────────────────────

    private static Map<String, String> stepDetail(String step, CicdEvent e) {
        Map<String, String> m = new HashMap<>();
        m.put("step",  step);
        m.put("event", e.getEventType());
        m.put("ts",    e.getEventTimestamp());
        return m;
    }
}
