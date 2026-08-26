package com.cicd.observability.router;

import com.cicd.observability.model.CicdEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * EventRouter — classifies each incoming CicdEvent and routes it to the
 * correct use-case stream via typed OutputTags.
 *
 * Why this matters:
 *   Without routing, every event hits every operator chain on every arrival.
 *   A BUILD_STARTED event has no business going through the CEP failure
 *   pattern detector or the late-event handler unless it meets the criteria.
 *
 * How it works:
 *   - This is a ProcessFunction — Flink calls processElement() for every event.
 *   - Each use case has its own OutputTag<CicdEvent>.
 *   - processElement() decides which tags to emit to based on event_type + status.
 *   - An event can go to MULTIPLE tags (e.g. a DEPLOY_FAILED event goes to both
 *     DORA and CEP pattern streams).
 *   - Events that don't match ANY criteria are counted and dropped.
 *
 * Routing rules (what triggers each use case):
 *
 *   DORA_TAG      → BUILD_STARTED, DEPLOY_SUCCESS, DEPLOY_FAILED,
 *                   BUILD_FAILED, BUILD_SUCCESS (for lead-time, CFR, MTTR)
 *
 *   HEALTH_TAG    → ANY event with a stage (BUILD/TEST/SONARQUBE/PACKAGE/DEPLOY)
 *                   because health score is a composite of all stage outcomes
 *
 *   LATE_TAG      → Events where processing_time − event_time > threshold
 *                   i.e. events that arrived significantly after they happened.
 *                   Flink's watermark handles the exact boundary, but we
 *                   pre-screen here to avoid sending fast/on-time events
 *                   through the late-event operator chain unnecessarily.
 *
 *   CEP_TAG       → BUILD_SUCCESS, DEPLOY_STARTED, DEPLOY_FAILED, ROLLBACK_STARTED
 *                   (the event types that participate in FailurePatternOperator's
 *                   three deployment-failure patterns — rollback cascade,
 *                   deployment instability, and build-ok-deploy-broken)
 */
public class EventRouter extends ProcessFunction<CicdEvent, CicdEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(EventRouter.class);

    // ── Threshold: if event is older than this many seconds when it
    //    arrives at Flink, treat it as a potential late event candidate.
    private static final long LATE_CANDIDATE_THRESHOLD_MS = 30_000L; // 30 seconds

    // ── OutputTag declarations ─────────────────────────────────────────
    // Each tag is typed as CicdEvent. The tag name is just for logging —
    // Flink uses object identity (the tag instance itself) for routing.

    /** DORA metrics: deployment frequency, lead time, CFR, MTTR */
    public static final OutputTag<CicdEvent> DORA_TAG =
            new OutputTag<CicdEvent>("dora-events") {};

    /** Pipeline health score: all stage events */
    public static final OutputTag<CicdEvent> HEALTH_TAG =
            new OutputTag<CicdEvent>("health-events") {};

    /** Late event processing: events that arrived late relative to event time */
    public static final OutputTag<CicdEvent> LATE_TAG =
            new OutputTag<CicdEvent>("late-candidate-events") {};

    /** CEP failure pattern: only the 5 event types in the cascade pattern */
    public static final OutputTag<CicdEvent> CEP_TAG =
            new OutputTag<CicdEvent>("cep-pattern-events") {};

    // ── Event types for each use case ──────────────────────────────────

    private static final Set<String> DORA_EVENT_TYPES = Set.of(
            "BUILD_STARTED",
            "BUILD_SUCCESS",
            "BUILD_FAILED",
            "DEPLOY_STARTED",
            "DEPLOY_SUCCESS",
            "DEPLOY_FAILED"
    );

    private static final Set<String> HEALTH_EVENT_TYPES = Set.of(
            "BUILD_STARTED",  "BUILD_SUCCESS",  "BUILD_FAILED",
            "TEST_STARTED",   "TEST_SUCCESS",   "TEST_FAILED",
            "SONARQUBE_STARTED", "SONARQUBE_SUCCESS", "SONARQUBE_FAILED",
            "PACKAGE_STARTED","PACKAGE_SUCCESS","PACKAGE_FAILED",
            "DEPLOY_STARTED", "DEPLOY_SUCCESS", "DEPLOY_FAILED"
    );

    /** Event types that participate in any of the three CEP deployment-failure patterns */
    private static final Set<String> CEP_EVENT_TYPES = Set.of(
            "BUILD_SUCCESS",
            "DEPLOY_STARTED",
            "DEPLOY_FAILED",
            "ROLLBACK_STARTED"
    );

    // ── Routing logic ──────────────────────────────────────────────────

    @Override
    public void processElement(CicdEvent event,
                               Context ctx,
                               Collector<CicdEvent> out) {

        if (event == null || event.getEventType() == null) return;

        String eventType  = event.getEventType();
        boolean routed    = false;
        long nowMs        = System.currentTimeMillis();
        long eventMs      = event.getTimestampMs();

        // ── Route 1: DORA ──────────────────────────────────────────────
        // Only events that directly measure deployment pipeline outcomes.
        if (DORA_EVENT_TYPES.contains(eventType)) {
            ctx.output(DORA_TAG, event);
            routed = true;
            LOG.debug("→ DORA: pipeline={} type={}", event.getPipelineId(), eventType);
        }

        // ── Route 2: Pipeline Health Score ────────────────────────────
        // All stage events contribute to the health composite score.
        if (HEALTH_EVENT_TYPES.contains(eventType)) {
            ctx.output(HEALTH_TAG, event);
            routed = true;
            LOG.debug("→ HEALTH: pipeline={} type={}", event.getPipelineId(), eventType);
        }

        // ── Route 3: Late Event Detection ─────────────────────────────
        // Pre-screen: only send to the late-event operator chain if the
        // event's own timestamp is significantly older than now.
        // Flink's watermark + allowedLateness handles the exact boundary;
        // this filter avoids passing every fast event through that chain.
        //
        // An event is a "late candidate" if it claims to have happened
        // more than LATE_CANDIDATE_THRESHOLD_MS ago relative to processing time.
        if (eventMs > 0 && (nowMs - eventMs) > LATE_CANDIDATE_THRESHOLD_MS) {
            ctx.output(LATE_TAG, event);
            routed = true;
            LOG.info("→ LATE: pipeline={} type={} delay={}s",
                    event.getPipelineId(), eventType,
                    (nowMs - eventMs) / 1000);
        }

        // ── Route 4: CEP Failure Pattern ──────────────────────────────
        // Only the exact event types that participate in the cascade pattern.
        // Sending BUILD_STARTED or PACKAGE_SUCCESS to the CEP NFA would add
        // noise without ever triggering a pattern match.
        if (CEP_EVENT_TYPES.contains(eventType)) {
            ctx.output(CEP_TAG, event);
            routed = true;
            LOG.debug("→ CEP: pipeline={} type={}", event.getPipelineId(), eventType);
        }

        // ── Emit to main output (all events — for general logging/audit) ──
        // The main output can be sunk to a raw-events Kafka topic for replay.
        out.collect(event);

        if (!routed) {
            LOG.debug("Event not routed to any use case: type={}", eventType);
        }
    }
}
